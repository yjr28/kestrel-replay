package fault

import (
	"strings"
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

func TestConnectionResetSpecValidates(t *testing.T) {
	spec := Spec{Kind: ConnectionReset, TargetService: "inventory", Operation: "check", TriggerOnMatch: 1, Seed: 7}
	if err := spec.Validate(); err != nil {
		t.Fatalf("valid reset spec rejected: %v", err)
	}
	spec.Delay = time.Millisecond
	if err := spec.Validate(); err == nil || !strings.Contains(err.Error(), "does not accept delay") {
		t.Fatalf("expected reset delay rejection, got %v", err)
	}
}

func TestServiceCrashSpecValidatesButInServiceControllerRejectsIt(t *testing.T) {
	spec := Spec{Kind: ServiceCrash, TargetService: "inventory", TriggerOnMatch: 1, Seed: 9}
	if err := spec.Validate(); err != nil {
		t.Fatalf("valid crash spec rejected: %v", err)
	}
	if _, err := NewController([]Spec{spec}); err == nil || !strings.Contains(err.Error(), "not supported by the in-service controller") {
		t.Fatalf("expected controller rejection for orchestrator-owned crash, got %v", err)
	}
	spec.TriggerOnMatch = 2
	if err := spec.Validate(); err == nil || !strings.Contains(err.Error(), "trigger_on_match=1") {
		t.Fatalf("expected unsupported crash trigger rejection, got %v", err)
	}
}

func TestDuplicateMessageSpecValidatesButInServiceControllerRejectsIt(t *testing.T) {
	spec := Spec{Kind: DuplicateMessage, TargetService: "broker", Operation: "orders.completed", TriggerOnMatch: 1, Seed: 11}
	if err := spec.Validate(); err != nil {
		t.Fatalf("valid duplicate spec rejected: %v", err)
	}
	if _, err := NewController([]Spec{spec}); err == nil || !strings.Contains(err.Error(), "not supported by the in-service controller") {
		t.Fatalf("expected controller rejection for broker-owned duplicate fault, got %v", err)
	}
	spec.Operation = ""
	if err := spec.Validate(); err == nil || !strings.Contains(err.Error(), "requires a target operation") {
		t.Fatalf("expected missing duplicate operation rejection, got %v", err)
	}
	spec = Spec{Kind: DuplicateMessage, TargetService: "broker", Operation: "orders.completed", TriggerOnMatch: 1, Delay: time.Millisecond}
	if err := spec.Validate(); err == nil || !strings.Contains(err.Error(), "does not accept delay") {
		t.Fatalf("expected duplicate delay rejection, got %v", err)
	}
}

func TestDelayedMessageSpecValidatesButInServiceControllerRejectsIt(t *testing.T) {
	spec := Spec{Kind: DelayedMessage, TargetService: "broker", Operation: "orders.completed", TriggerOnMatch: 1, Delay: 90 * time.Millisecond, Seed: 12}
	if err := spec.Validate(); err != nil {
		t.Fatalf("valid delayed-message spec rejected: %v", err)
	}
	if _, err := NewController([]Spec{spec}); err == nil || !strings.Contains(err.Error(), "not supported by the in-service controller") {
		t.Fatalf("expected controller rejection for broker-owned delayed fault, got %v", err)
	}

	withoutDelay := spec
	withoutDelay.Delay = 0
	if err := withoutDelay.Validate(); err == nil || !strings.Contains(err.Error(), "positive delay") {
		t.Fatalf("expected missing delayed-message delay rejection, got %v", err)
	}
	withoutOperation := spec
	withoutOperation.Operation = ""
	if err := withoutOperation.Validate(); err == nil || !strings.Contains(err.Error(), "requires a target operation") {
		t.Fatalf("expected missing delayed-message operation rejection, got %v", err)
	}
	withJitter := spec
	withJitter.JitterFraction = .1
	if err := withJitter.Validate(); err == nil || !strings.Contains(err.Error(), "does not yet accept jitter") {
		t.Fatalf("expected delayed-message jitter rejection, got %v", err)
	}
}

func TestUnimplementedFaultKindRejected(t *testing.T) {
	spec := Spec{Kind: PacketLoss, TargetService: "inventory", TriggerOnMatch: 1, Seed: 7}
	if err := spec.Validate(); err == nil || !strings.Contains(err.Error(), "not implemented") {
		t.Fatalf("expected unimplemented fault rejection, got %v", err)
	}
}
