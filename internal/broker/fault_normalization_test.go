package broker

import (
	"context"
	"testing"
	"time"

	"github.com/yjr28/kestrel-replay/internal/fault"
)

func TestBrokerAsyncFaultNormalizesTargetKeys(t *testing.T) {
	spec := fault.Spec{
		Kind:           fault.DuplicateMessage,
		TargetService:  " broker ",
		Operation:      " orders.completed\t",
		TriggerOnMatch: 1,
		Seed:           101,
	}
	b, err := NewWithFault([]string{"http://worker"}, 1, "  http://collector/ \t", &spec)
	if err != nil {
		t.Fatalf("formatting-only whitespace should not reject supported broker configuration: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	defer func() {
		if err := b.Close(ctx); err != nil {
			t.Errorf("close broker: %v", err)
		}
	}()

	if b.faultSpec == nil {
		t.Fatal("expected broker fault spec")
	}
	if got := b.faultSpec.TargetService; got != "broker" {
		t.Fatalf("target service was not canonicalized: %q", got)
	}
	if got := b.faultSpec.Operation; got != ordersCompletedOperation {
		t.Fatalf("operation was not canonicalized: %q", got)
	}
	if got := b.collectorURL; got != "http://collector" {
		t.Fatalf("collector URL was not canonicalized: %q", got)
	}
	if spec.TargetService != " broker " || spec.Operation != " orders.completed\t" {
		t.Fatalf("constructor mutated caller-owned spec: %+v", spec)
	}
}
