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
