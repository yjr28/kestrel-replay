package corpus

import (
	"testing"

	"github.com/yjr28/kestrel-replay/internal/graph"
)

func TestExpectedMessageTopologyScopesDuplicateCase(t *testing.T) {
	for _, c := range Cases() {
		truth, ok := ExpectedMessageTopology(c)
		if c.ID != "orders-completed-duplicate" {
			if ok {
				t.Fatalf("unexpected topology truth for %s: %+v", c.ID, truth)
			}
			continue
		}
		if !ok || truth.Topic != Topic || truth.Action != "consume" || truth.FailingCount != 2 || len(truth.Services) != 3 {
			t.Fatalf("unexpected duplicate truth: %+v ok=%t", truth, ok)
		}
	}
}

func TestValidateMessageTopologyAcceptsExpectedDuplicateEvidence(t *testing.T) {
	var duplicate Case
	for _, c := range Cases() {
		if c.ID == "orders-completed-duplicate" {
			duplicate = c
			break
		}
	}
	divergences := []graph.MessageTopologyDivergence{
		{Topic: Topic, Action: "consume", Service: "notification", Reason: "count_above_healthy_range", HealthyMedian: 1, HealthyMin: 1, HealthyMax: 1, FailingCount: 2, CountDelta: 1, FailingEventIDs: []string{"n1", "n2"}},
		{Topic: Topic, Action: "consume", Service: "audit", Reason: "count_above_healthy_range", HealthyMedian: 1, HealthyMin: 1, HealthyMax: 1, FailingCount: 2, CountDelta: 1, FailingEventIDs: []string{"a1", "a2"}},
		{Topic: Topic, Action: "consume", Service: "analytics", Reason: "count_above_healthy_range", HealthyMedian: 1, HealthyMin: 1, HealthyMax: 1, FailingCount: 2, CountDelta: 1, FailingEventIDs: []string{"x1", "x2"}},
	}
	if err := ValidateMessageTopology(duplicate, divergences); err != nil {
		t.Fatal(err)
	}
}

func TestValidateMessageTopologyRejectsMissingConsumerEvidence(t *testing.T) {
	var duplicate Case
	for _, c := range Cases() {
		if c.ID == "orders-completed-duplicate" {
			duplicate = c
			break
		}
	}
	divergences := []graph.MessageTopologyDivergence{
		{Topic: Topic, Action: "consume", Service: "notification", Reason: "count_above_healthy_range", HealthyMax: 1, FailingCount: 2, CountDelta: 1, FailingEventIDs: []string{"n1", "n2"}},
		{Topic: Topic, Action: "consume", Service: "audit", Reason: "count_above_healthy_range", HealthyMax: 1, FailingCount: 2, CountDelta: 1, FailingEventIDs: []string{"a1", "a2"}},
	}
	if err := ValidateMessageTopology(duplicate, divergences); err == nil {
		t.Fatal("expected missing analytics topology evidence to fail")
	}
}
