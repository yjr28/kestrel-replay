package graph

import (
	"testing"
	"time"

	"github.com/yjr28/kestrel-replay/internal/model"
)

func TestApplicationSpanIndexTreatsFormattingOnlyEventIDReuseAsAmbiguous(t *testing.T) {
	events := []model.Event{
		{
			ID:        "span-event-1",
			Kind:      model.KindSpan,
			Source:    model.SourceApplication,
			TraceID:   "trace-1",
			SpanID:    "span-1",
			Service:   "orders",
			Operation: "create",
			Timestamp: time.Unix(1, 0),
		},
		{
			ID:        " span-event-1 ",
			Kind:      model.KindSpan,
			Source:    model.SourceApplication,
			TraceID:   "trace-1",
			SpanID:    "span-2",
			Service:   "inventory",
			Operation: "check",
			Timestamp: time.Unix(2, 0),
		},
	}

	spans, ambiguous := applicationSpanIndexWithAmbiguity(events)
	ordersKey := divergenceKey{service: "orders", operation: "create"}
	inventoryKey := divergenceKey{service: "inventory", operation: "check"}

	for _, key := range []divergenceKey{ordersKey, inventoryKey} {
		if _, ok := ambiguous[key]; !ok {
			t.Fatalf("expected formatting-only event ID reuse to make key %+v ambiguous", key)
		}
		if _, ok := spans[key]; ok {
			t.Fatalf("expected ambiguous key %+v to be withheld", key)
		}
	}
}
