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
