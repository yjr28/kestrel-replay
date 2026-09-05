package replay

import (
	"testing"
	"time"

	"github.com/yjr28/kestrel-replay/internal/model"
)

func TestMessageReplayNormalizesFormattingKeys(t *testing.T) {
	now := time.Now().UTC()
	events := []model.Event{
		{Source: model.SourceApplication, Kind: model.KindMessage, Service: " order ", Timestamp: now, Attributes: map[string]string{"topic": " orders.completed ", "message.id": " generated-a ", "message.action": " publish "}},
		{Source: model.SourceApplication, Kind: model.KindMessage, Service: " notification ", Timestamp: now.Add(120 * time.Millisecond), Attributes: map[string]string{"topic": " orders.completed ", "message.id": " generated-a ", "message.action": " consume "}},
	}

	delivery := MessageDelivery(events, " orders.completed ")
	if delivery.Topic != "orders.completed" || delivery.PublishCount != 1 || delivery.ConsumeCounts["notification"] != 1 {
		t.Fatalf("formatting-only whitespace changed delivery evidence: %+v", delivery)
	}

	delay := MessageDelay(events, " orders.completed ")
	if delay.Topic != "orders.completed" || delay.PublishCount != 1 || delay.CorrelatedConsumeCount != 1 || delay.MinConsumeDelayMicros != (120*time.Millisecond).Microseconds() {
		t.Fatalf("formatting-only whitespace changed delay evidence: %+v", delay)
	}
}
