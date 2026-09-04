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

const ordersCompletedTopic = "orders.completed"

type artifactReplayReport struct {
	ExperimentID         string                          `json:"experiment_id"`
	RecordedEventCount   int                             `json:"recorded_event_count"`
	RecordedGraphNodes   int                             `json:"recorded_graph_nodes"`
	RecordedGraphEdges   int                             `json:"recorded_graph_edges"`
	RecordedOutcome      replay.OutcomeSignature         `json:"recorded_outcome"`
	ReplayedOutcome      replay.OutcomeSignature         `json:"replayed_outcome"`
	RecordedMessages     replay.MessageDeliverySignature `json:"recorded_message_delivery"`
	ReplayedMessages     replay.MessageDeliverySignature `json:"replayed_message_delivery"`
	MessageDeliveryMatch bool                            `json:"message_delivery_match"`
	ReplayMatch          bool                            `json:"replay_match"`
}

func TestMultiProcessFailureReplay(t *testing.T) {
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

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	spec := fault.Spec{Kind: fault.Latency, TargetService: "inventory", Operation: "check", TriggerOnMatch: 1, Delay: 75 * time.Millisecond, Seed: 20260903}

	healthy, err := orchestrator.RunScenario(ctx, nodeBin, nil, "req-healthy")
	if err != nil {
		t.Fatal(err)
	}
	failing, err := orchestrator.RunScenario(ctx, nodeBin, &spec, "req-failing")
	if err != nil {
		t.Fatal(err)
	}
	if failing.Outcome.HTTPStatus != 504 || failing.Outcome.TerminalService != "inventory" {
		t.Fatalf("unexpected failing outcome: %+v", failing.Outcome)
	}

	artifactDir, err := experiment.Save(filepath.Join(binDir, "experiments"), experiment.Record{
		ExperimentID:     "seeded-inventory-timeout",
		Workload:         "single-create-order",
		Topology:         []string{"gateway", "auth", "account", "order", "inventory", "pricing", "payment", "broker", "notification", "audit", "analytics", "collector"},
		Fault:            &spec,
		ExpectedBehavior: "inventory latency exceeds the order service timeout and produces inventory_timeout",
		ObservedBehavior: fmt.Sprintf("http=%d terminal=%s error=%s", failing.Outcome.HTTPStatus, failing.Outcome.TerminalService, failing.Outcome.ErrorCode),
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
	failing = orchestrator.Result{}

	report := runArtifactReplay(t, ctx, replayBin, artifactDir, nodeBin, "req-replay")
	if !report.ReplayMatch || !replay.Equivalent(recorded.Manifest.Outcome, report.ReplayedOutcome) {
		t.Fatalf("artifact replay mismatch: %+v", report)
	}
	if report.ExperimentID != recorded.Manifest.ExperimentID || report.RecordedEventCount != len(recorded.Events) {
		t.Fatalf("artifact replay loaded unexpected evidence: %+v", report)
	}
	if len(healthy.Events) < 14 || len(recorded.Events) < 6 {
		t.Fatalf("unexpected event counts healthy=%d recorded=%d", len(healthy.Events), len(recorded.Events))
	}

	g, err := graph.Build(recorded.Events)
	if err != nil {
		t.Fatal(err)
	}
	if len(g.Nodes) < 6 || len(g.Edges) < 4 || report.RecordedGraphNodes != len(g.Nodes) || report.RecordedGraphEdges != len(g.Edges) {
		t.Fatalf("graph mismatch local=%d/%d report=%d/%d", len(g.Nodes), len(g.Edges), report.RecordedGraphNodes, report.RecordedGraphEdges)
	}
	divergence, ok := graph.EarliestMeaningfulDivergence(healthy.Events, recorded.Events, 20*time.Millisecond)
	if !ok || divergence.Service != "inventory" || divergence.Operation != "check" {
		t.Fatalf("unexpected divergence ok=%t value=%+v", ok, divergence)
	}
}

func TestMultiProcessConnectionResetReplay(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-process integration test")
	}
	if runtime.GOOS == "windows" {
		t.Skip("TCP reset injection and signal-based process lifecycle are currently Unix-only")
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

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	spec := fault.Spec{Kind: fault.ConnectionReset, TargetService: "inventory", Operation: "check", TriggerOnMatch: 1, Seed: 20260904}

	failing, err := orchestrator.RunScenario(ctx, nodeBin, &spec, "req-reset-failing")
	if err != nil {
		t.Fatal(err)
	}
	if failing.Outcome.HTTPStatus != 502 || failing.Outcome.TerminalService != "inventory" || failing.Outcome.ErrorCode != "inventory_connection_reset" {
		t.Fatalf("unexpected reset outcome: %+v", failing.Outcome)
	}

	var sawFault, sawResetSpan bool
	for _, event := range failing.Events {
		if event.Kind == model.KindFault && event.Attributes["fault.kind"] == string(fault.ConnectionReset) {
			sawFault = true
		}
		if event.Kind == model.KindSpan && event.Service == "inventory" && event.Status == "error" && event.Attributes["transport.error"] == "connection_reset" {
			sawResetSpan = true
		}
	}
	if !sawFault || !sawResetSpan {
		t.Fatalf("missing reset evidence fault=%t span=%t", sawFault, sawResetSpan)
	}

	artifactDir, err := experiment.Save(filepath.Join(binDir, "experiments"), experiment.Record{
		ExperimentID:     "seeded-inventory-reset",
		Workload:         "single-create-order",
		Topology:         []string{"gateway", "auth", "account", "order", "inventory", "pricing", "payment", "broker", "notification", "audit", "analytics", "collector"},
		Fault:            &spec,
		ExpectedBehavior: "inventory forcibly resets the dependency TCP connection and order reports inventory_connection_reset",
		ObservedBehavior: fmt.Sprintf("http=%d terminal=%s error=%s", failing.Outcome.HTTPStatus, failing.Outcome.TerminalService, failing.Outcome.ErrorCode),
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
	failing = orchestrator.Result{}
	report := runArtifactReplay(t, ctx, replayBin, artifactDir, nodeBin, "req-reset-replay")
	if !report.ReplayMatch || !replay.Equivalent(recorded.Manifest.Outcome, report.ReplayedOutcome) {
		t.Fatalf("reset artifact replay mismatch: %+v", report)
	}
	if report.ReplayedOutcome.HTTPStatus != 502 || report.ReplayedOutcome.ErrorCode != "inventory_connection_reset" {
		t.Fatalf("unexpected reset replay outcome: %+v", report.ReplayedOutcome)
	}
}

func TestMultiProcessServiceCrashReplay(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-process integration test")
	}
	if runtime.GOOS == "windows" {
		t.Skip("process-kill replay is currently Unix-only")
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
	spec := fault.Spec{Kind: fault.ServiceCrash, TargetService: "inventory", TriggerOnMatch: 1, Seed: 20260905}

	healthy, err := orchestrator.RunScenario(ctx, nodeBin, nil, "req-crash-healthy")
	if err != nil {
		t.Fatal(err)
	}
	failing, err := orchestrator.RunScenario(ctx, nodeBin, &spec, "req-crash-failing")
	if err != nil {
		t.Fatal(err)
	}
	if failing.Outcome.HTTPStatus != 502 || failing.Outcome.TerminalService != "inventory" || failing.Outcome.ErrorCode != "inventory_connection_refused" {
		t.Fatalf("unexpected crash outcome: %+v", failing.Outcome)
	}

	var sawCrashFault, sawInventorySpan bool
	for _, event := range failing.Events {
		if event.Kind == model.KindFault && event.Attributes["fault.kind"] == string(fault.ServiceCrash) && event.Attributes["target.service"] == "inventory" && event.Attributes["schedule.phase"] == "before_request" {
			sawCrashFault = true
		}
		if event.Kind == model.KindSpan && event.Service == "inventory" {
			sawInventorySpan = true
		}
	}
	if !sawCrashFault {
		t.Fatal("missing recorded service_crash injector evidence")
	}
	if sawInventorySpan {
		t.Fatal("inventory emitted a request span even though the process was killed before the workload")
	}

	artifactDir, err := experiment.Save(filepath.Join(binDir, "experiments"), experiment.Record{
		ExperimentID:     "seeded-inventory-crash",
		Workload:         "single-create-order",
		Topology:         []string{"gateway", "auth", "account", "order", "inventory", "pricing", "payment", "broker", "notification", "audit", "analytics", "collector"},
		Fault:            &spec,
		ExpectedBehavior: "inventory is killed before the request and order observes inventory_connection_refused",
		ObservedBehavior: fmt.Sprintf("http=%d terminal=%s error=%s", failing.Outcome.HTTPStatus, failing.Outcome.TerminalService, failing.Outcome.ErrorCode),
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
	if len(recorded.Events) < 5 {
		t.Fatalf("unexpected crash evidence count: %d", len(recorded.Events))
	}
	failing = orchestrator.Result{}
	report := runArtifactReplay(t, ctx, replayBin, artifactDir, nodeBin, "req-crash-replay")
	if !report.ReplayMatch || !replay.Equivalent(recorded.Manifest.Outcome, report.ReplayedOutcome) {
		t.Fatalf("crash artifact replay mismatch: %+v", report)
	}
	if report.ReplayedOutcome.HTTPStatus != 502 || report.ReplayedOutcome.TerminalService != "inventory" || report.ReplayedOutcome.ErrorCode != "inventory_connection_refused" {
		t.Fatalf("unexpected crash replay outcome: %+v", report.ReplayedOutcome)
	}

	g, err := graph.Build(recorded.Events)
	if err != nil {
		t.Fatal(err)
	}
	if report.RecordedGraphNodes != len(g.Nodes) || report.RecordedGraphEdges != len(g.Edges) {
		t.Fatalf("crash graph mismatch local=%d/%d report=%d/%d", len(g.Nodes), len(g.Edges), report.RecordedGraphNodes, report.RecordedGraphEdges)
	}
	divergence, ok := graph.EarliestMeaningfulDivergenceForTerminalService(healthy.Events, recorded.Events, 20*time.Millisecond, recorded.Manifest.Outcome.TerminalService)
	if !ok || divergence.Service != "inventory" || divergence.Operation != "check" || divergence.Reason != "missing_span" {
		t.Fatalf("unexpected crash divergence ok=%t value=%+v", ok, divergence)
	}
}

func TestMultiProcessDuplicateMessageReplay(t *testing.T) {
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
	spec := fault.Spec{Kind: fault.DuplicateMessage, TargetService: "broker", Operation: ordersCompletedTopic, TriggerOnMatch: 1, Seed: 20260906}

	failing, err := orchestrator.RunScenario(ctx, nodeBin, &spec, "req-duplicate-failing")
	if err != nil {
		t.Fatal(err)
	}
	if failing.Outcome.HTTPStatus != 201 || failing.Outcome.Classification != "success" {
		t.Fatalf("duplicate fault should preserve synchronous success, got %+v", failing.Outcome)
	}

	sig := replay.MessageDelivery(failing.Events, ordersCompletedTopic)
	if sig.PublishCount != 1 || sig.ConsumeCounts["notification"] != 2 || sig.ConsumeCounts["audit"] != 2 || sig.ConsumeCounts["analytics"] != 2 {
		t.Fatalf("unexpected duplicate delivery signature: %+v", sig)
	}
	var sawFault bool
	for _, event := range failing.Events {
		if event.Kind == model.KindFault && event.Attributes["fault.kind"] == string(fault.DuplicateMessage) && event.Attributes["message.id"] != "" && event.Attributes["duplicate.extra_copies"] == "1" {
			sawFault = true
		}
	}
	if !sawFault {
		t.Fatal("missing recorded duplicate_message injector evidence")
	}

	artifactDir, err := experiment.Save(filepath.Join(binDir, "experiments"), experiment.Record{
		ExperimentID:     "seeded-order-duplicate",
		Workload:         "single-create-order",
		Topology:         []string{"gateway", "auth", "account", "order", "inventory", "pricing", "payment", "broker", "notification", "audit", "analytics", "collector"},
		Fault:            &spec,
		ExpectedBehavior: "broker delivers the orders.completed envelope twice to each worker while the synchronous order request remains successful",
		ObservedBehavior: fmt.Sprintf("http=%d publish=%d notification=%d audit=%d analytics=%d", failing.Outcome.HTTPStatus, sig.PublishCount, sig.ConsumeCounts["notification"], sig.ConsumeCounts["audit"], sig.ConsumeCounts["analytics"]),
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
	if len(recorded.Events) < 21 {
		t.Fatalf("unexpected duplicate evidence count: %d", len(recorded.Events))
	}
	recordedSig := replay.MessageDelivery(recorded.Events, ordersCompletedTopic)
	failing = orchestrator.Result{}

	report := runArtifactReplay(t, ctx, replayBin, artifactDir, nodeBin, "req-duplicate-replay")
	if !report.ReplayMatch || !report.MessageDeliveryMatch {
		t.Fatalf("duplicate artifact replay mismatch: %+v", report)
	}
	if !replay.Equivalent(recorded.Manifest.Outcome, report.ReplayedOutcome) || !replay.EquivalentMessageDelivery(recordedSig, report.ReplayedMessages) {
		t.Fatalf("duplicate replay semantic evidence mismatch: %+v", report)
	}
	if report.ReplayedMessages.PublishCount != 1 || report.ReplayedMessages.ConsumeCounts["notification"] != 2 || report.ReplayedMessages.ConsumeCounts["audit"] != 2 || report.ReplayedMessages.ConsumeCounts["analytics"] != 2 {
		t.Fatalf("unexpected replayed duplicate delivery signature: %+v", report.ReplayedMessages)
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
	if messageEdges != 6 {
		t.Fatalf("expected publisher to connect to six duplicate consume events, got %d", messageEdges)
	}
}

func runArtifactReplay(t *testing.T, ctx context.Context, replayBin, artifactDir, nodeBin, requestID string) artifactReplayReport {
	t.Helper()
	replayCmd := exec.CommandContext(ctx, replayBin, "-artifact", artifactDir, "-node", nodeBin, "-request-id", requestID, "-json")
	rawReport, err := replayCmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			t.Fatalf("artifact replay failed: %v\n%s", err, exitErr.Stderr)
		}
		t.Fatal(err)
	}
	var report artifactReplayReport
	if err := json.Unmarshal(rawReport, &report); err != nil {
		t.Fatalf("decode artifact replay report: %v\n%s", err, rawReport)
	}
	return report
}

func buildBinary(t *testing.T, root, output, pkg string) {
	t.Helper()
	build := exec.Command("go", "build", "-o", output, pkg)
	build.Dir = root
	if raw, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build %s: %v\n%s", pkg, err, raw)
	}
}
