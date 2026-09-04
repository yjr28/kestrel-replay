package demo

import (
	"testing"
	"time"

	"github.com/yjr28/kestrel-replay/internal/fault"
	"github.com/yjr28/kestrel-replay/internal/graph"
	"github.com/yjr28/kestrel-replay/internal/replay"
)

func TestFailureReplayMatchesRecordedOutcome(t *testing.T) {
	spec := fault.Spec{Kind: fault.Latency, TargetService: "inventory", Operation: "check", TriggerOnMatch: 1, Delay: 75 * time.Millisecond, Seed: 20260903}
	healthy, err := RunScenario(nil)
	if err != nil {
		t.Fatal(err)
	}
	failing, err := RunScenario([]fault.Spec{spec})
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := RunScenario([]fault.Spec{spec})
	if err != nil {
		t.Fatal(err)
	}

	if failing.Outcome.HTTPStatus != 504 || failing.Outcome.TerminalService != "inventory" {
		t.Fatalf("unexpected failure outcome: %+v", failing.Outcome)
	}
	if !replay.Equivalent(failing.Outcome, replayed.Outcome) {
		t.Fatalf("replay mismatch: failing=%+v replay=%+v", failing.Outcome, replayed.Outcome)
	}
	if _, err := graph.Build(failing.Events); err != nil {
		t.Fatalf("causal graph failed: %v", err)
	}
	d, ok := graph.EarliestMeaningfulDivergence(healthy.Events, failing.Events, 20*time.Millisecond)
	if !ok {
		t.Fatal("expected a divergence")
	}
	if d.Service == "" {
		t.Fatalf("divergence missing service: %+v", d)
	}
}
