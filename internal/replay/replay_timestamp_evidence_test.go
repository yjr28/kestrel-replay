package replay

import (
	"testing"
	"time"

	"github.com/yjr28/kestrel-replay/internal/model"
)

func TestMessageDeliveryRejectsZeroTimestampPublishEvidence(t *testing.T) {
	events := []model.Event{
		{Source: model.SourceApplication, Kind: model.KindMessage, TraceID: "trace", Service: "order", Attributes: map[string]string{"topic": "orders.completed", "message.id": "generated-a", "message.action": "publish"}},
		{Source: model.SourceApplication, Kind: model.KindMessage, TraceID: "trace", Service: "notification", Timestamp: time.Now().UTC(), Attributes: map[string]string{"topic": "orders.completed", "message.id": "generated-a", "message.action": "consume"}},
	}

	sig := MessageDelivery(events, "orders.completed")
	if len(sig.ConsumeCounts) != 0 {
		t.Fatalf("untimestamped publish must not establish delivery ordering evidence: %+v", sig)
	}
}

func TestMessageDelayRejectsZeroTimestampPublishEvidence(t *testing.T) {
	events := []model.Event{
		{Source: model.SourceApplication, Kind: model.KindMessage, TraceID: "trace", Service: "order", Attributes: map[string]string{"topic": "orders.completed", "message.id": "generated-a", "message.action": "publish"}},
		{Source: model.SourceApplication, Kind: model.KindMessage, TraceID: "trace", Service: "notification", Timestamp: time.Now().UTC(), Attributes: map[string]string{"topic": "orders.completed", "message.id": "generated-a", "message.action": "consume"}},
	}

	sig := MessageDelay(events, "orders.completed")
	if sig.CorrelatedConsumeCount != 0 || sig.MinConsumeDelayMicros != 0 {
		t.Fatalf("untimestamped publish must not establish delay evidence: %+v", sig)
	}
	if MeetsMinimumMessageDelay(sig, time.Millisecond) {
		t.Fatalf("untimestamped publish must not satisfy delayed-delivery evidence: %+v", sig)
	}
}

func TestMessageDeliveryRejectsZeroTimestampConsumeEvidence(t *testing.T) {
	events := []model.Event{
		{Source: model.SourceApplication, Kind: model.KindMessage, TraceID: "trace", Service: "order", Attributes: map[string]string{"topic": "orders.completed", "message.id": "generated-a", "message.action": "publish"}},
		{Source: model.SourceApplication, Kind: model.KindMessage, TraceID: "trace", Service: "notification", Attributes: map[string]string{"topic": "orders.completed", "message.id": "generated-a", "message.action": "consume"}},
	}

	sig := MessageDelivery(events, "orders.completed")
	if len(sig.ConsumeCounts) != 0 {
		t.Fatalf("zero timestamps must not establish equal-time delivery ordering evidence: %+v", sig)
	}
}
