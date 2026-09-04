package graph

import (
	"testing"
	"time"

	"github.com/yjr28/kestrel-replay/internal/model"
)

func TestMessageTopologyProfileFindsDuplicateConsumerMultiplicity(t *testing.T) {
	now := time.Now().UTC()
	healthyRuns := [][]model.Event{
		messageRun("h1", now, 1),
		messageRun("h2", now.Add(time.Second), 1),
		messageRun("h3", now.Add(2*time.Second), 1),
	}
	profile, err := BuildMessageTopologyProfile(healthyRuns)
	if err != nil {
		t.Fatal(err)
	}
	if profile.RunCount != 3 || len(profile.Baselines()) != 4 {
		t.Fatalf("unexpected message profile: runs=%d baselines=%#v", profile.RunCount, profile.Baselines())
	}
	for _, baseline := range profile.Baselines() {
		if baseline.MinCount != 1 || baseline.MaxCount != 1 || baseline.MedianCount != 1 || !baseline.Stable {
			t.Fatalf("unexpected healthy multiplicity: %+v", baseline)
		}
		if len(baseline.HealthyRuns) != 3 {
			t.Fatalf("missing healthy-run provenance: %+v", baseline)
		}
		for runIndex, evidence := range baseline.HealthyRuns {
			if evidence.RunIndex != runIndex || evidence.Count != 1 || len(evidence.EventIDs) != 1 {
				t.Fatalf("unexpected healthy-run evidence: %+v", evidence)
			}
		}
	}

	failing := messageRun("f", now.Add(3*time.Second), 2)
	divergences := CompareMessageTopology(profile, failing)
	if len(divergences) != 3 {
		t.Fatalf("expected three duplicated consumer divergences, got %#v", divergences)
	}
	seen := map[string]bool{}
	for _, divergence := range divergences {
		if divergence.Topic != "orders.completed" || divergence.Action != "consume" || divergence.Reason != "count_above_healthy_range" {
			t.Fatalf("unexpected divergence: %+v", divergence)
		}
		if divergence.HealthyMedian != 1 || divergence.HealthyMax != 1 || divergence.FailingCount != 2 || divergence.CountDelta != 1 || len(divergence.FailingEventIDs) != 2 {
			t.Fatalf("unexpected multiplicity evidence: %+v", divergence)
		}
		if len(divergence.HealthyRuns) != 3 {
			t.Fatalf("divergence lost healthy provenance: %+v", divergence)
		}
		seen[divergence.Service] = true
	}
	for _, service := range []string{"notification", "audit", "analytics"} {
		if !seen[service] {
			t.Fatalf("missing %s divergence: %#v", service, divergences)
		}
	}
}

func TestMessageTopologyProfileAcceptsHealthyEquivalentFlow(t *testing.T) {
	now := time.Now().UTC()
	profile, err := BuildMessageTopologyProfile([][]model.Event{
		messageRun("h1", now, 1),
		messageRun("h2", now.Add(time.Second), 1),
	})
	if err != nil {
		t.Fatal(err)
	}
	if divergences := CompareMessageTopology(profile, messageRun("f", now.Add(2*time.Second), 1)); len(divergences) != 0 {
		t.Fatalf("healthy-equivalent message flow diverged: %#v", divergences)
	}
}

func TestMessageTopologyProfileReportsMissingAndUnexpectedFlows(t *testing.T) {
	now := time.Now().UTC()
	profile, err := BuildMessageTopologyProfile([][]model.Event{
		messageRun("h1", now, 1),
		messageRun("h2", now.Add(time.Second), 1),
	})
	if err != nil {
		t.Fatal(err)
	}
	failing := []model.Event{
		messageEvent("f-pub", "order", "publish", now.Add(2*time.Second)),
		messageEvent("f-notification", "notification", "consume", now.Add(2*time.Second)),
		messageEvent("f-audit", "audit", "consume", now.Add(2*time.Second)),
		messageEvent("f-search", "search-index", "consume", now.Add(2*time.Second)),
	}
	divergences := CompareMessageTopology(profile, failing)
	var missingAnalytics, unexpectedSearch bool
	for _, divergence := range divergences {
		if divergence.Service == "analytics" && divergence.Reason == "count_below_healthy_range" && divergence.FailingCount == 0 {
			if len(divergence.HealthyRuns) != 2 || len(divergence.HealthyRuns[0].EventIDs) != 1 || len(divergence.HealthyRuns[1].EventIDs) != 1 {
				t.Fatalf("missing flow lost healthy evidence: %+v", divergence)
			}
			missingAnalytics = true
		}
		if divergence.Service == "search-index" && divergence.Reason == "unexpected_message_flow" && divergence.FailingCount == 1 {
			if len(divergence.HealthyRuns) != 2 || len(divergence.FailingEventIDs) != 1 {
				t.Fatalf("unexpected flow lost absence/failing evidence: %+v", divergence)
			}
			for runIndex, evidence := range divergence.HealthyRuns {
				if evidence.RunIndex != runIndex || evidence.Count != 0 || len(evidence.EventIDs) != 0 {
					t.Fatalf("unexpected flow must preserve explicit healthy absence: %+v", divergence)
				}
			}
			unexpectedSearch = true
		}
	}
	if !missingAnalytics || !unexpectedSearch {
		t.Fatalf("missing topology evidence: %#v", divergences)
	}
}

func TestMessageTopologyProfileKeepsOptionalHealthyEnvelope(t *testing.T) {
	now := time.Now().UTC()
	run1 := append(messageRun("h1", now, 1), messageEvent("h1-optional", "warehouse", "consume", now))
	run2 := messageRun("h2", now.Add(time.Second), 1)
	run3 := append(messageRun("h3", now.Add(2*time.Second), 1), messageEvent("h3-optional", "warehouse", "consume", now.Add(2*time.Second)))
	profile, err := BuildMessageTopologyProfile([][]model.Event{run1, run2, run3})
	if err != nil {
		t.Fatal(err)
	}
	var warehouse MessageFlowBaseline
	for _, baseline := range profile.Baselines() {
		if baseline.Service == "warehouse" {
			warehouse = baseline
			break
		}
	}
	if len(warehouse.HealthyRuns) != 3 || warehouse.HealthyRuns[0].Count != 1 || warehouse.HealthyRuns[1].Count != 0 || len(warehouse.HealthyRuns[1].EventIDs) != 0 || warehouse.HealthyRuns[2].Count != 1 {
		t.Fatalf("optional flow provenance must preserve explicit zero-count run: %+v", warehouse)
	}

	failing := append(messageRun("f", now.Add(3*time.Second), 1), messageEvent("f-optional", "warehouse", "consume", now.Add(3*time.Second)))
	if divergences := CompareMessageTopology(profile, failing); len(divergences) != 0 {
		t.Fatalf("optional healthy flow inside empirical 0..1 envelope diverged: %#v", divergences)
	}
}

func TestMessageTopologyProfileReturnsDefensiveHealthyEvidenceCopies(t *testing.T) {
	now := time.Now().UTC()
	profile, err := BuildMessageTopologyProfile([][]model.Event{
		messageRun("h1", now, 1),
		messageRun("h2", now.Add(time.Second), 1),
	})
	if err != nil {
		t.Fatal(err)
	}
	baselines := profile.Baselines()
	if len(baselines) == 0 || len(baselines[0].HealthyRuns) == 0 || len(baselines[0].HealthyRuns[0].EventIDs) == 0 {
		t.Fatalf("missing baseline provenance: %#v", baselines)
	}
	originalID := baselines[0].HealthyRuns[0].EventIDs[0]
	baselines[0].HealthyRuns[0].EventIDs[0] = "mutated"
	baselinesAgain := profile.Baselines()
	if baselinesAgain[0].HealthyRuns[0].EventIDs[0] != originalID {
		t.Fatalf("caller mutated profile provenance through returned baseline: %#v", baselinesAgain[0])
	}
}

func messageRun(prefix string, at time.Time, consumeCopies int) []model.Event {
	events := []model.Event{messageEvent(prefix+"-pub", "order", "publish", at)}
	for _, service := range []string{"notification", "audit", "analytics"} {
		for i := 0; i < consumeCopies; i++ {
			events = append(events, messageEvent(prefix+"-"+service+string(rune('a'+i)), service, "consume", at.Add(time.Duration(i+1)*time.Millisecond)))
		}
	}
	return events
}

func messageEvent(id, service, action string, at time.Time) model.Event {
	return model.Event{
		ID: id, Source: model.SourceApplication, Kind: model.KindMessage,
		TraceID: "trace-" + id, CorrelationID: "request-" + id,
		Service: service, Operation: "order_completed", Timestamp: at, Status: "ok",
		Attributes: map[string]string{"topic": "orders.completed", "message.id": "message-" + id, "message.action": action},
	}
}
