package collector

import (
	"testing"
	"time"

	"github.com/yjr28/kestrel-replay/internal/model"
)

func TestStoreCanonicalizesMessageTopic(t *testing.T) {
	store := &Store{}
	input := model.Event{
		ID:        "event-1",
		Source:    model.SourceApplication,
		Kind:      model.KindMessage,
		TraceID:   "trace-a",
		Service:   "order",
		Timestamp: time.Now().UTC(),
		Attributes: map[string]string{
			"topic":          " orders.completed ",
			"message.id":     " message-1 ",
			"message.action": " publish ",
			"note":           " preserve surrounding whitespace ",
		},
	}

	if err := store.Add(input); err != nil {
		t.Fatal(err)
	}
	got := store.Snapshot("trace-a")
	if len(got) != 1 {
		t.Fatalf("unexpected snapshot: %+v", got)
	}
	attrs := got[0].Attributes
	if attrs["topic"] != "orders.completed" || attrs["message.id"] != "message-1" || attrs["message.action"] != "publish" {
		t.Fatalf("message evidence keys were not canonicalized: %+v", attrs)
	}
	if attrs["note"] != " preserve surrounding whitespace " {
		t.Fatalf("unrecognized attribute was modified: %q", attrs["note"])
	}
	if input.Attributes["topic"] != " orders.completed " {
		t.Fatalf("caller-owned event was mutated: %+v", input.Attributes)
	}
}
