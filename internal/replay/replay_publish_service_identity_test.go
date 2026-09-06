package replay

import (
	"testing"
	"time"

	"github.com/yjr28/kestrel-replay/internal/model"
)

func TestMessageSignaturesWithholdPublishWithoutServiceIdentity(t *testing.T) {
	publishedAt := time.Date(2026, 9, 6, 21, 0, 0, 0, time.UTC)
	events := []model.Event{
		{
			ID:        "publish-missing-service",
			Sequence:  1,
			Source:    model.SourceApplication,
			Kind:      model.KindMessage,
			TraceID:   "trace-1",
			Timestamp: publishedAt,
			Attributes: map[string]string{
				"message.id":     "message-1",
				"message.action": "publish",
				"topic":          "orders.completed",
			},
		},
	}

	delivery := MessageDelivery(events, "orders.completed")
	if delivery.PublishCount != 0 {
		t.Fatalf("delivery publish count = %d, want 0 for publish without service identity", delivery.PublishCount)
	}

	delay := MessageDelay(events, "orders.completed")
	if delay.PublishCount != 0 {
		t.Fatalf("delay publish count = %d, want 0 for publish without service identity", delay.PublishCount)
	}
}
