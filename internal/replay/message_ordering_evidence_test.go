package replay

import (
	"testing"
	"time"

	"github.com/yjr28/kestrel-replay/internal/model"
)

func TestMessageDeliveryWithholdsEqualTimestampWhenSequenceEvidenceIsIncomplete(t *testing.T) {
	now := time.Now().UTC()
	tests := []struct {
		name            string
		publishSequence uint64
		consumeSequence uint64
	}{
		{name: "publish sequence missing", publishSequence: 0, consumeSequence: 2},
		{name: "consume sequence missing", publishSequence: 1, consumeSequence: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sig := MessageDelivery([]model.Event{
				{Source: model.SourceApplication, Kind: model.KindMessage, Service: "order", Timestamp: now, Sequence: tt.publishSequence, Attributes: map[string]string{"topic": "orders.completed", "message.id": "same", "message.action": "publish"}},
				{Source: model.SourceApplication, Kind: model.KindMessage, Service: "notification", Timestamp: now, Sequence: tt.consumeSequence, Attributes: map[string]string{"topic": "orders.completed", "message.id": "same", "message.action": "consume"}},
			}, "orders.completed")

			if sig.PublishCount != 1 {
				t.Fatalf("publish evidence should remain visible: %+v", sig)
			}
			if len(sig.ConsumeCounts) != 0 {
				t.Fatalf("incomplete equal-timestamp sequence evidence must not establish publish-before-consume ordering: %+v", sig)
			}
		})
	}
}

func TestMessageDelayWithholdsEqualTimestampWhenSequenceEvidenceIsIncomplete(t *testing.T) {
	now := time.Now().UTC()
	sig := MessageDelay([]model.Event{
		{Source: model.SourceApplication, Kind: model.KindMessage, Service: "order", Timestamp: now, Sequence: 0, Attributes: map[string]string{"topic": "orders.completed", "message.id": "same", "message.action": "publish"}},
		{Source: model.SourceApplication, Kind: model.KindMessage, Service: "notification", Timestamp: now, Sequence: 2, Attributes: map[string]string{"topic": "orders.completed", "message.id": "same", "message.action": "consume"}},
	}, "orders.completed")

	if sig.CorrelatedConsumeCount != 0 || sig.MinConsumeDelayMicros != 0 {
		t.Fatalf("incomplete equal-timestamp sequence evidence must not produce delay evidence: %+v", sig)
	}
}
