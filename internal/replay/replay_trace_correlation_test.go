package replay

import (
	"testing"
	"time"

	"github.com/yjr28/kestrel-replay/internal/model"
)

func TestMessageDeliveryDoesNotCorrelateAcrossTraceBoundary(t *testing.T) {
	now := time.Now().UTC()
	events := []model.Event{
		{Source: model.SourceApplication, Kind: model.KindMessage, TraceID: "trace-other", Service: "order", Timestamp: now, Attributes: map[string]string{"topic": "orders.completed", "message.id": "reused-id", "message.action": "publish"}},
		{Source: model.SourceApplication, Kind: model.KindMessage, TraceID: "trace-target", Service: "notification", Timestamp: now.Add(time.Millisecond), Attributes: map[string]string{"topic": "orders.completed", "message.id": "reused-id", "message.action": "consume"}},
	}

	sig := MessageDelivery(events, "orders.completed")
	if sig.PublishCount != 1 {
		t.Fatalf("expected publish to remain observable, got %+v", sig)
	}
	if len(sig.ConsumeCounts) != 0 {
		t.Fatalf("same message id from another trace must not satisfy delivery correlation: %+v", sig)
	}
}

func TestMessageDelayScopesReusedMessageIDByTrace(t *testing.T) {
	now := time.Now().UTC()
	events := []model.Event{
		{Source: model.SourceApplication, Kind: model.KindMessage, TraceID: "trace-target", Service: "order", Timestamp: now, Attributes: map[string]string{"topic": "orders.completed", "message.id": "reused-id", "message.action": "publish"}},
		{Source: model.SourceApplication, Kind: model.KindMessage, TraceID: "trace-other", Service: "order", Timestamp: now.Add(time.Millisecond), Attributes: map[string]string{"topic": "orders.completed", "message.id": "reused-id", "message.action": "publish"}},
		{Source: model.SourceApplication, Kind: model.KindMessage, TraceID: "trace-target", Service: "notification", Timestamp: now.Add(5 * time.Millisecond), Attributes: map[string]string{"topic": "orders.completed", "message.id": "reused-id", "message.action": "consume"}},
	}

	sig := MessageDelay(events, "orders.completed")
	if sig.PublishCount != 2 {
		t.Fatalf("expected both publishes to remain observable, got %+v", sig)
	}
	if sig.CorrelatedConsumeCount != 1 || sig.MinConsumeDelayMicros != (5*time.Millisecond).Microseconds() {
		t.Fatalf("reuse in another trace must not make target-trace delay evidence ambiguous: %+v", sig)
	}
}

func TestMessageDeliveryWithholdsCorrelationWithoutTraceEvidence(t *testing.T) {
	now := time.Now().UTC()
	events := []model.Event{
		{Source: model.SourceApplication, Kind: model.KindMessage, Service: "order", Timestamp: now, Attributes: map[string]string{"topic": "orders.completed", "message.id": "untraced-id", "message.action": "publish"}},
		{Source: model.SourceApplication, Kind: model.KindMessage, Service: "notification", Timestamp: now.Add(time.Millisecond), Attributes: map[string]string{"topic": "orders.completed", "message.id": "untraced-id", "message.action": "consume"}},
	}

	sig := MessageDelivery(events, "orders.completed")
	if sig.PublishCount != 1 {
		t.Fatalf("expected untraced publish to remain observable, got %+v", sig)
	}
	if len(sig.ConsumeCounts) != 0 {
		t.Fatalf("message id alone must not establish delivery correlation without trace evidence: %+v", sig)
	}
}

func TestMessageDelayWithholdsCorrelationWithoutTraceEvidence(t *testing.T) {
	now := time.Now().UTC()
	events := []model.Event{
		{Source: model.SourceApplication, Kind: model.KindMessage, Service: "order", Timestamp: now, Attributes: map[string]string{"topic": "orders.completed", "message.id": "untraced-id", "message.action": "publish"}},
		{Source: model.SourceApplication, Kind: model.KindMessage, Service: "notification", Timestamp: now.Add(5 * time.Millisecond), Attributes: map[string]string{"topic": "orders.completed", "message.id": "untraced-id", "message.action": "consume"}},
	}

	sig := MessageDelay(events, "orders.completed")
	if sig.PublishCount != 1 {
		t.Fatalf("expected untraced publish to remain observable, got %+v", sig)
	}
	if sig.CorrelatedConsumeCount != 0 || sig.MinConsumeDelayMicros != 0 {
		t.Fatalf("message id alone must not establish delay evidence without trace evidence: %+v", sig)
	}
}
