package replay

import (
	"testing"
	"time"

	"github.com/yjr28/kestrel-replay/internal/model"
)

func TestMessageDelayRejectsConsumeBeforePublish(t *testing.T) {
	now := time.Now().UTC()
	events := []model.Event{
		{Source: model.SourceApplication, Kind: model.KindMessage, Service: "notification", Timestamp: now, Attributes: map[string]string{"topic": "orders.completed", "message.id": "generated-a", "message.action": "consume"}},
		{Source: model.SourceApplication, Kind: model.KindMessage, Service: "order", Timestamp: now.Add(100 * time.Millisecond), Attributes: map[string]string{"topic": "orders.completed", "message.id": "generated-a", "message.action": "publish"}},
	}

	sig := MessageDelay(events, "orders.completed")
	if sig.PublishCount != 1 {
		t.Fatalf("expected one publish, got %+v", sig)
	}
	if sig.CorrelatedConsumeCount != 0 || sig.MinConsumeDelayMicros != 0 {
		t.Fatalf("consume before publish must not count as correlated delay evidence: %+v", sig)
	}
	if MeetsMinimumMessageDelay(sig, time.Millisecond) {
		t.Fatalf("consume before publish must not satisfy delayed-delivery evidence: %+v", sig)
	}
}

func TestMessageDeliveryRejectsConsumeBeforePublish(t *testing.T) {
	now := time.Now().UTC()
	events := []model.Event{
		{Source: model.SourceApplication, Kind: model.KindMessage, Service: "notification", Timestamp: now, Attributes: map[string]string{"topic": "orders.completed", "message.id": "generated-a", "message.action": "consume"}},
		{Source: model.SourceApplication, Kind: model.KindMessage, Service: "order", Timestamp: now.Add(100 * time.Millisecond), Attributes: map[string]string{"topic": "orders.completed", "message.id": "generated-a", "message.action": "publish"}},
	}

	sig := MessageDelivery(events, "orders.completed")
	if sig.PublishCount != 1 {
		t.Fatalf("expected one publish, got %+v", sig)
	}
	if len(sig.ConsumeCounts) != 0 {
		t.Fatalf("consume before publish must not count as delivery evidence: %+v", sig)
	}
}

func TestMessageDeliveryWithholdsAmbiguousPublishCorrelation(t *testing.T) {
	now := time.Now().UTC()
	events := []model.Event{
		{Source: model.SourceApplication, Kind: model.KindMessage, Service: "order", Timestamp: now, Attributes: map[string]string{"topic": "orders.completed", "message.id": "generated-a", "message.action": "publish"}},
		{Source: model.SourceApplication, Kind: model.KindMessage, Service: "order", Timestamp: now.Add(time.Millisecond), Attributes: map[string]string{"topic": "orders.completed", "message.id": "generated-a", "message.action": "publish"}},
		{Source: model.SourceApplication, Kind: model.KindMessage, Service: "notification", Timestamp: now.Add(2 * time.Millisecond), Attributes: map[string]string{"topic": "orders.completed", "message.id": "generated-a", "message.action": "consume"}},
	}

	sig := MessageDelivery(events, "orders.completed")
	if sig.PublishCount != 2 {
		t.Fatalf("expected both publishes to remain observable, got %+v", sig)
	}
	if len(sig.ConsumeCounts) != 0 {
		t.Fatalf("duplicate publish identity must withhold ambiguous delivery correlation: %+v", sig)
	}
}
