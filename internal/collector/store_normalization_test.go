package collector

import (
	"testing"
	"time"

	"github.com/yjr28/kestrel-replay/internal/model"
)

func TestStoreCanonicalizesEvidenceIdentifiers(t *testing.T) {
	store := &Store{}
	input := model.Event{
		ID:            " event-1 ",
		Source:        model.SourceFault,
		Kind:          model.KindFault,
		TraceID:       " trace-a ",
		SpanID:        " span-a ",
		ParentSpanID:  " parent-a ",
		CorrelationID: " correlation-a ",
		Service:       " broker ",
		Operation:     " orders.completed ",
		Timestamp:     time.Now().UTC(),
		Status:        " injected ",
		Attributes: map[string]string{
			"fault.kind":       " delayed_message ",
			"target.service":   " broker ",
			"target.operation": " orders.completed ",
			"note":             " preserve surrounding whitespace ",
		},
	}

	if err := store.Add(input); err != nil {
		t.Fatal(err)
	}
	got := store.Snapshot(" trace-a ")
	if len(got) != 1 {
		t.Fatalf("expected trace query to match canonical identifier, got %+v", got)
	}
	e := got[0]
	if e.ID != "event-1" || e.TraceID != "trace-a" || e.SpanID != "span-a" || e.ParentSpanID != "parent-a" || e.CorrelationID != "correlation-a" {
		t.Fatalf("identity fields were not canonicalized: %+v", e)
	}
	if e.Service != "broker" || e.Operation != "orders.completed" || e.Status != "injected" {
		t.Fatalf("semantic keys were not canonicalized: %+v", e)
	}
	if e.Attributes["fault.kind"] != "delayed_message" || e.Attributes["target.service"] != "broker" || e.Attributes["target.operation"] != "orders.completed" {
		t.Fatalf("fault evidence keys were not canonicalized: %+v", e.Attributes)
	}
	if e.Attributes["note"] != " preserve surrounding whitespace " {
		t.Fatalf("unrecognized attribute was modified: %q", e.Attributes["note"])
	}
	if input.ID != " event-1 " || input.Attributes["fault.kind"] != " delayed_message " {
		t.Fatalf("caller-owned event was mutated: %+v", input)
	}
}
