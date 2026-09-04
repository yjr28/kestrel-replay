package replay

import (
	"testing"
	"time"

	"github.com/yjr28/kestrel-replay/internal/model"
)

func TestEquivalent(t *testing.T) {
	a := OutcomeSignature{Classification: "timeout", HTTPStatus: 504, TerminalService: "inventory", ErrorCode: "inventory_timeout", CausalPath: []string{"gateway", "order", "inventory"}}
	b := a
	if !Equivalent(a, b) {
		t.Fatal("identical signatures should match")
	}
	b.ErrorCode = "other"
	if Equivalent(a, b) {
		t.Fatal("different error code should not match")
	}
}

func TestMessageDeliveryCanonicalizesGeneratedIdentities(t *testing.T) {
	now := time.Now()
	events := []model.Event{
		{ID: "publish", Source: model.SourceApplication, Kind: model.KindMessage, TraceID: "trace-a", SpanID: "span-a", Service: "order", Timestamp: now, Attributes: map[string]string{"topic": "orders.completed", "message.id": "generated-a", "message.action": "publish"}},
		{ID: "n1", Source: model.SourceApplication, Kind: model.KindMessage, TraceID: "trace-a", SpanID: "span-n1", Service: "notification", Timestamp: now, Attributes: map[string]string{"topic": "orders.completed", "message.id": "generated-a", "message.action": "consume"}},
		{ID: "n2", Source: model.SourceApplication, Kind: model.KindMessage, TraceID: "trace-a", SpanID: "span-n2", Service: "notification", Timestamp: now, Attributes: map[string]string{"topic": "orders.completed", "message.id": "generated-a", "message.action": "consume"}},
		{ID: "audit", Source: model.SourceApplication, Kind: model.KindMessage, TraceID: "trace-a", SpanID: "span-au", Service: "audit", Timestamp: now, Attributes: map[string]string{"topic": "orders.completed", "message.id": "generated-a", "message.action": "consume"}},
	}
	sig := MessageDelivery(events, "orders.completed")
	if sig.PublishCount != 1 || sig.ConsumeCounts["notification"] != 2 || sig.ConsumeCounts["audit"] != 1 {
		t.Fatalf("unexpected signature: %+v", sig)
	}

	other := MessageDeliverySignature{Topic: "orders.completed", PublishCount: 1, ConsumeCounts: map[string]int{"notification": 2, "audit": 1}}
	if !EquivalentMessageDelivery(sig, other) {
		t.Fatalf("equivalent canonical signatures should match: %+v vs %+v", sig, other)
	}
	other.ConsumeCounts["notification"] = 1
	if EquivalentMessageDelivery(sig, other) {
		t.Fatal("missing duplicate delivery should not match")
	}
}

func TestMessageDelayUsesCorrelatedPublishConsumeTiming(t *testing.T) {
	now := time.Now().UTC()
	events := []model.Event{
		{ID: "publish", Source: model.SourceApplication, Kind: model.KindMessage, Service: "order", Timestamp: now, Attributes: map[string]string{"topic": "orders.completed", "message.id": "generated-a", "message.action": "publish"}},
		{ID: "unrelated", Source: model.SourceApplication, Kind: model.KindMessage, Service: "audit", Timestamp: now.Add(5 * time.Millisecond), Attributes: map[string]string{"topic": "orders.completed", "message.id": "other", "message.action": "consume"}},
		{ID: "notification", Source: model.SourceApplication, Kind: model.KindMessage, Service: "notification", Timestamp: now.Add(120 * time.Millisecond), Attributes: map[string]string{"topic": "orders.completed", "message.id": "generated-a", "message.action": "consume"}},
		{ID: "audit", Source: model.SourceApplication, Kind: model.KindMessage, Service: "audit", Timestamp: now.Add(135 * time.Millisecond), Attributes: map[string]string{"topic": "orders.completed", "message.id": "generated-a", "message.action": "consume"}},
	}

	sig := MessageDelay(events, "orders.completed")
	if sig.PublishCount != 1 || sig.CorrelatedConsumeCount != 2 || sig.MinConsumeDelayMicros != (120*time.Millisecond).Microseconds() {
		t.Fatalf("unexpected delay signature: %+v", sig)
	}
	if !MeetsMinimumMessageDelay(sig, 100*time.Millisecond) {
		t.Fatalf("expected delay signature to satisfy 100ms threshold: %+v", sig)
	}
	if MeetsMinimumMessageDelay(sig, 125*time.Millisecond) {
		t.Fatalf("delay signature must not satisfy a threshold above the earliest consume: %+v", sig)
	}
}

func TestMessageDelayRequiresCorrelatedConsume(t *testing.T) {
	now := time.Now().UTC()
	sig := MessageDelay([]model.Event{
		{Source: model.SourceApplication, Kind: model.KindMessage, Service: "order", Timestamp: now, Attributes: map[string]string{"topic": "orders.completed", "message.id": "a", "message.action": "publish"}},
		{Source: model.SourceApplication, Kind: model.KindMessage, Service: "audit", Timestamp: now.Add(time.Second), Attributes: map[string]string{"topic": "orders.completed", "message.id": "b", "message.action": "consume"}},
	}, "orders.completed")
	if MeetsMinimumMessageDelay(sig, time.Millisecond) {
		t.Fatalf("unrelated consume must not satisfy delayed-delivery evidence: %+v", sig)
	}
}
