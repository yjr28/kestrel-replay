package graph

import (
	"testing"
	"time"

	"github.com/yjr28/kestrel-replay/internal/model"
)

func TestMessageFlowCountsTreatsFormattingOnlyEventIDReuseAsAmbiguous(t *testing.T) {
	events := []model.Event{
		{
			ID:        "event-1",
			Kind:      model.KindMessage,
			Source:    model.SourceApplication,
			TraceID:   "trace-1",
			Service:   "notification",
			Timestamp: time.Unix(1, 0),
			Attributes: map[string]string{
				"message.id":     "message-1",
				"message.action": "consume",
				"topic":          "orders.completed",
			},
		},
		{
			ID:        " event-1 ",
			Kind:      model.KindMessage,
			Source:    model.SourceApplication,
			TraceID:   "trace-1",
			Service:   "notification",
			Timestamp: time.Unix(2, 0),
			Attributes: map[string]string{
				"message.id":     "message-2",
				"message.action": "consume",
				"topic":          "orders.completed",
			},
		},
	}

	counts, ids, ambiguous := messageFlowCountsWithAmbiguity(events)
	key := messageFlowKey{topic: "orders.completed", action: "consume", service: "notification"}
	if _, ok := ambiguous[key]; !ok {
		t.Fatal("expected formatting-only event ID reuse to make the flow ambiguous")
	}
	if _, ok := counts[key]; ok {
		t.Fatal("expected ambiguous flow count to be withheld")
	}
	if _, ok := ids[key]; ok {
		t.Fatal("expected ambiguous flow provenance IDs to be withheld")
	}
}
