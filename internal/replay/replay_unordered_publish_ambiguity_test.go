package replay

import (
	"testing"
	"time"

	"github.com/yjr28/kestrel-replay/internal/model"
)

func TestMessageCorrelationWithholdsWhenAnotherPublishCannotBeOrderedAgainstConsume(t *testing.T) {
	before := time.Unix(99, 0).UTC()
	tied := time.Unix(100, 0).UTC()
	events := []model.Event{
		{ID: "publish-earlier", Sequence: 1, Source: model.SourceApplication, Kind: model.KindMessage, TraceID: "trace", Service: "api", Timestamp: before, Attributes: map[string]string{"topic": "orders", "message.id": "m1", "message.action": "publish"}},
		{ID: "publish-tied", Source: model.SourceApplication, Kind: model.KindMessage, TraceID: "trace", Service: "api", Timestamp: tied, Attributes: map[string]string{"topic": "orders", "message.id": "m1", "message.action": "publish"}},
		{ID: "consume", Source: model.SourceApplication, Kind: model.KindMessage, TraceID: "trace", Service: "worker", Timestamp: tied, Attributes: map[string]string{"topic": "orders", "message.id": "m1", "message.action": "consume"}},
	}

	delivery := MessageDelivery(events, "orders")
	if got := delivery.ConsumeCounts["worker"]; got != 0 {
		t.Fatalf("consume must be withheld when another same-key publish cannot be ordered against it, got %d", got)
	}

	delay := MessageDelay(events, "orders")
	if delay.CorrelatedConsumeCount != 0 {
		t.Fatalf("ambiguous consume must not contribute delay evidence, got %d", delay.CorrelatedConsumeCount)
	}
}
