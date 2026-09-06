package replay

import (
	"testing"
	"time"

	"github.com/yjr28/kestrel-replay/internal/model"
)

func TestMessageDeliveryRequiresPublishToPrecedeEqualTimestampConsume(t *testing.T) {
	ts := time.Unix(100, 0).UTC()
	events := []model.Event{
		{ID: "consume", Sequence: 1, Source: model.SourceApplication, Kind: model.KindMessage, TraceID: "trace", Service: "worker", Timestamp: ts, Attributes: map[string]string{"topic": "orders", "message.id": "m1", "message.action": "consume"}},
		{ID: "publish", Sequence: 2, Source: model.SourceApplication, Kind: model.KindMessage, TraceID: "trace", Service: "api", Timestamp: ts, Attributes: map[string]string{"topic": "orders", "message.id": "m1", "message.action": "publish"}},
	}

	sig := MessageDelivery(events, "orders")
	if got := sig.ConsumeCounts["worker"]; got != 0 {
		t.Fatalf("equal-timestamp consume preceding publish by sequence must not correlate, got %d", got)
	}
}

func TestMessageDelayRequiresPublishToPrecedeEqualTimestampConsume(t *testing.T) {
	ts := time.Unix(100, 0).UTC()
	events := []model.Event{
		{ID: "consume", Sequence: 1, Source: model.SourceApplication, Kind: model.KindMessage, TraceID: "trace", Service: "worker", Timestamp: ts, Attributes: map[string]string{"topic": "orders", "message.id": "m1", "message.action": "consume"}},
		{ID: "publish", Sequence: 2, Source: model.SourceApplication, Kind: model.KindMessage, TraceID: "trace", Service: "api", Timestamp: ts, Attributes: map[string]string{"topic": "orders", "message.id": "m1", "message.action": "publish"}},
	}

	sig := MessageDelay(events, "orders")
	if sig.CorrelatedConsumeCount != 0 {
		t.Fatalf("equal-timestamp consume preceding publish by sequence must not contribute delay evidence, got %d", sig.CorrelatedConsumeCount)
	}
}
