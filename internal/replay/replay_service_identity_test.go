package replay

import (
	"testing"
	"time"

	"github.com/yjr28/kestrel-replay/internal/model"
)

func TestMessageDeliveryWithholdsConsumeWithoutServiceIdentity(t *testing.T) {
	now := time.Now().UTC()
	events := []model.Event{
		{Source: model.SourceApplication, Kind: model.KindMessage, TraceID: "trace", Service: "order", Timestamp: now, Sequence: 1, Attributes: map[string]string{"topic": "orders.completed", "message.id": "message", "message.action": "publish"}},
		{Source: model.SourceApplication, Kind: model.KindMessage, TraceID: "trace", Timestamp: now.Add(time.Millisecond), Sequence: 2, Attributes: map[string]string{"topic": "orders.completed", "message.id": "message", "message.action": "consume"}},
	}

	sig := MessageDelivery(events, "orders.completed")

	if sig.PublishCount != 1 {
		t.Fatalf("expected the observed publish to remain counted, got %d", sig.PublishCount)
	}
	if len(sig.ConsumeCounts) != 0 {
		t.Fatalf("expected consume without service identity to be withheld, got %#v", sig.ConsumeCounts)
	}
}
