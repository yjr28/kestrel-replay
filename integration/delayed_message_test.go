package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/yjr28/kestrel-replay/internal/experiment"
	"github.com/yjr28/kestrel-replay/internal/fault"
	"github.com/yjr28/kestrel-replay/internal/graph"
	"github.com/yjr28/kestrel-replay/internal/model"
	"github.com/yjr28/kestrel-replay/internal/orchestrator"
	"github.com/yjr28/kestrel-replay/internal/replay"
)

type delayedArtifactReplayReport struct {
	RecordedOutcome      replay.OutcomeSignature         `json:"recorded_outcome"`
	ReplayedOutcome      replay.OutcomeSignature         `json:"replayed_outcome"`
	RecordedMessages     replay.MessageDeliverySignature `json:"recorded_message_delivery"`
	ReplayedMessages     replay.MessageDeliverySignature `json:"replayed_message_delivery"`
	MessageDeliveryMatch bool                            `json:"message_delivery_match"`
	RecordedMessageDelay replay.MessageDelaySignature    `json:"recorded_message_delay"`
	ReplayedMessageDelay replay.MessageDelaySignature    `json:"replayed_message_delay"`
	MessageDelayEligible bool                            `json:"message_delay_eligible"`
	MessageDelayMatch    bool                            `json:"message_delay_match"`
	ReplayMatch          bool                            `json:"replay_match"`
}

func TestMultiProcessDelayedMessageReplay(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-process integration test")
	}
	if runtime.GOOS == "windows" {
		t.Skip("signal-based process lifecycle is currently Unix-only")
	}

	root, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	binDir := t.TempDir()
	nodeBin := filepath.Join(binDir, "kestrel-node")
	replayBin := filepath.Join(binDir, "kestrel-artifact-replay")
	buildBinary(t, root, nodeBin, "./cmd/kestrel-node")
	buildBinary(t, root, replayBin, "./cmd/kestrel-artifact-replay")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	spec := fault.Spec{
		Kind: fault.DelayedMessage, TargetService: "broker", Operation: ordersCompletedTopic,
		TriggerOnMatch: 1, Delay: 120 * time.Millisecond, Seed: 20260907,
	}

	failing, err := orchestrator.RunScenario(ctx, nodeBin, &spec, "req-delay-failing")
	if err != nil {
		t.Fatal(err)
	}
	if failing.Outcome.HTTPStatus != 201 || failing.Outcome.Classification != "success" {
		t.Fatalf("delayed-message fault should preserve synchronous success, got %+v", failing.Outcome)
	}

	delivery := replay.MessageDelivery(failing.Events, ordersCompletedTopic)
	if delivery.PublishCount != 1 || delivery.ConsumeCounts["notification"] != 1 || delivery.ConsumeCounts["audit"] != 1 || delivery.ConsumeCounts["analytics"] != 1 {
		t.Fatalf("delayed-message fault changed delivery multiplicity: %+v", delivery)
	}
	delay := replay.MessageDelay(failing.Events, ordersCompletedTopic)
	if !replay.MeetsMinimumMessageDelay(delay, spec.Delay) {
		t.Fatalf("recorded message delay did not meet configured threshold %v: %+v", spec.Delay, delay)
	}

	var sawFault bool
	for _, event := range failing.Events {
		if event.Kind != model.KindFault || event.Attributes["fault.kind"] != string(fault.DelayedMessage) {
			continue
		}
		if event.Attributes["target.service"] != "broker" || event.Attributes["target.operation"] != ordersCompletedTopic || event.Attributes["schedule.phase"] != "before_delivery" {
			t.Fatalf("incomplete delayed-message fault evidence: %+v", event)
		}
		if event.Attributes["delay_us"] != fmt.Sprintf("%d", spec.Delay.Microseconds()) || event.Attributes["message.id"] == "" {
			t.Fatalf("delayed-message evidence does not preserve schedule/message identity: %+v", event)
		}
		sawFault = true
	}
	if !sawFault {
		t.Fatal("missing recorded delayed_message injector evidence")
	}

	artifactDir, err := experiment.Save(filepath.Join(binDir, "experiments"), experiment.Record{
		ExperimentID:     "seeded-order-delay",
		Workload:         "single-create-order",
		Topology:         []string{"gateway", "auth", "account", "order", "inventory", "pricing", "payment", "broker", "notification", "audit", "analytics", "collector"},
		Fault:            &spec,
		ExpectedBehavior: "broker holds the orders.completed envelope before delivering one copy to each worker while the synchronous order remains successful",
		ObservedBehavior: fmt.Sprintf("http=%d publish=%d correlated_consumes=%d min_delay_us=%d", failing.Outcome.HTTPStatus, delay.PublishCount, delay.CorrelatedConsumeCount, delay.MinConsumeDelayMicros),
		Outcome:          failing.Outcome,
		Events:           failing.Events,
	})
	if err != nil {
		t.Fatal(err)
	}

	recorded, err := experiment.Load(artifactDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(recorded.Events) < 15 {
		t.Fatalf("unexpected delayed-message evidence count: %d", len(recorded.Events))
	}
	recordedDelay := replay.MessageDelay(recorded.Events, ordersCompletedTopic)
	if !replay.MeetsMinimumMessageDelay(recordedDelay, spec.Delay) {
		t.Fatalf("persisted delayed-message evidence lost timing threshold: %+v", recordedDelay)
	}
	failing = orchestrator.Result{}

	report := runDelayedArtifactReplay(t, ctx, replayBin, artifactDir, nodeBin, "req-delay-replay")
	if !report.ReplayMatch || !report.MessageDeliveryMatch || !report.MessageDelayEligible || !report.MessageDelayMatch {
		t.Fatalf("delayed-message artifact replay mismatch: %+v", report)
	}
	if !replay.Equivalent(recorded.Manifest.Outcome, report.ReplayedOutcome) {
		t.Fatalf("delayed-message replay changed external outcome: %+v", report)
	}
	if !replay.MeetsMinimumMessageDelay(report.RecordedMessageDelay, spec.Delay) || !replay.MeetsMinimumMessageDelay(report.ReplayedMessageDelay, spec.Delay) {
		t.Fatalf("recorded/replayed delay threshold mismatch: recorded=%+v replayed=%+v", report.RecordedMessageDelay, report.ReplayedMessageDelay)
	}

	g, err := graph.Build(recorded.Events)
	if err != nil {
		t.Fatal(err)
	}
	messageEdges := 0
	for _, edge := range g.Edges {
		if edge.Kind == graph.EdgeMessage {
			messageEdges++
		}
	}
	if messageEdges != 3 {
		t.Fatalf("delayed delivery should preserve one publish-to-consume edge per worker, got %d", messageEdges)
	}
}

func runDelayedArtifactReplay(t *testing.T, ctx context.Context, replayBin, artifactDir, nodeBin, requestID string) delayedArtifactReplayReport {
	t.Helper()
	cmd := exec.CommandContext(ctx, replayBin, "-artifact", artifactDir, "-node", nodeBin, "-request-id", requestID, "-json")
	raw, err := cmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			t.Fatalf("delayed artifact replay failed: %v\n%s", err, exitErr.Stderr)
		}
		t.Fatal(err)
	}
	var report delayedArtifactReplayReport
	if err := json.Unmarshal(raw, &report); err != nil {
		t.Fatalf("decode delayed artifact replay report: %v\n%s", err, raw)
	}
	return report
}
