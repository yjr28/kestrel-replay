package replay

import (
	"testing"
	"time"

	"github.com/yjr28/kestrel-replay/internal/model"
)

func TestMessageSignaturesWithholdEmptyTopicIdentity(t *testing.T) {
	publishedAt := time.Date(2026, 9, 6, 22, 0, 0, 0, time.UTC)
	events := []model.Event{
		{
			ID:        "publish-empty-topic",
			Sequence:  1,
			Source:    model.SourceApplication,
			Kind:      model.KindMessage,
			TraceID:   "trace-1",
			Service:   "orders",
			Timestamp: publishedAt,
			Attributes: map[string]string{
				"message.id":     "message-1",
				"message.action": "publish",
				"topic":          " ",
			},
		},
	}

	delivery := MessageDelivery(events, " ")
	if delivery.Topic != "" || delivery.PublishCount != 0 || len(delivery.ConsumeCounts) != 0 {
		t.Fatalf("delivery accepted empty topic identity: %+v", delivery)
	}

	delay := MessageDelay(events, " ")
	if delay.Topic != "" || delay.PublishCount != 0 || delay.CorrelatedConsumeCount != 0 || delay.MinConsumeDelayMicros != 0 {
		t.Fatalf("delay accepted empty topic identity: %+v", delay)
	}
}
