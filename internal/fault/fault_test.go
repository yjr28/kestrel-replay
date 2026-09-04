package fault

import (
	"testing"
	"time"
)

func TestControllerDeterministic(t *testing.T) {
	spec := Spec{Kind: Latency, TargetService: "inventory", Operation: "check", TriggerOnMatch: 2, Delay: 50 * time.Millisecond, JitterFraction: .2, Seed: 42}
	a, err := NewController([]Spec{spec})
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewController([]Spec{spec})
	if err != nil {
		t.Fatal(err)
	}

	if a.Decide("inventory", "check").Inject {
		t.Fatal("first match should not inject")
	}
	da := a.Decide("inventory", "check")
	b.Decide("inventory", "check")
	db := b.Decide("inventory", "check")
	if !da.Inject || !db.Inject {
		t.Fatal("second match should inject")
	}
	if da.Delay != db.Delay {
		t.Fatalf("same seed produced different delays: %v vs %v", da.Delay, db.Delay)
	}
}
