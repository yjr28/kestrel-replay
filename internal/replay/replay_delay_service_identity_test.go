package replay

import (
	"testing"
	"time"

	"github.com/yjr28/kestrel-replay/internal/model"
)

func TestMessageDelayWithholdsConsumeWithoutServiceIdentity(t *testing.T) {
	now := time.Now().UTC()
	events := []model.Event{
		{Source: model.SourceApplication, Kind: model.KindMessage, TraceID: "trace", Service: "order", Timestamp: now, Sequence: 1, Attributes: map[string]string{"topic": "orders.completed", "message.id": "message", "message.action": "publish"}},
		{Source: model.SourceApplication, Kind: model.KindMessage, TraceID: "trace", Timestamp: now.Add(10 * time.Millisecond), Sequence: 2, Attributes: map[string]string{"topic": "orders.completed", "message.id": "message", "message.action": "consume"}},
	}

	sig := MessageDelay(events, "orders.completed")

	if sig.PublishCount != 1 {
		t.Fatalf("expected the observed publish to remain counted, got %d", sig.PublishCount)
	}
	if sig.CorrelatedConsumeCount != 0 || sig.MinConsumeDelayMicros != 0 {
		t.Fatalf("expected delay evidence without consume service identity to be withheld, got %#v", sig)
	}
}
