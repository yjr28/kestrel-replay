package graph

import (
	"testing"
	"time"

	"github.com/yjr28/kestrel-replay/internal/model"
)

func messageEvent(id, action, messageID, topic, service string, at time.Time) model.Event {
	return model.Event{
		ID: id, Source: model.SourceApplication, Kind: model.KindMessage,
		TraceID: "trace-1", Service: service, Timestamp: at,
		Attributes: map[string]string{
			"message.id":     messageID,
			"message.action": action,
			"topic":          topic,
		},
	}
}

func TestBuildDoesNotLinkSameMessageIDAcrossTopics(t *testing.T) {
	now := time.Now().UTC()
	events := []model.Event{
		messageEvent("publish-orders", "publish", "message-1", "orders", "producer", now),
		messageEvent("consume-refunds", "consume", "message-1", "refunds", "consumer", now.Add(time.Millisecond)),
	}

	g, err := Build(events)
	if err != nil {
		t.Fatalf("build graph: %v", err)
	}
	if len(g.Edges) != 0 {
		t.Fatalf("message id alone must not create cross-topic evidence edge: %#v", g.Edges)
	}
}

func TestBuildScopesPublisherAmbiguityToTopic(t *testing.T) {
	now := time.Now().UTC()
	events := []model.Event{
		messageEvent("publish-orders-a", "publish", "message-1", "orders", "producer-a", now),
		messageEvent("publish-orders-b", "publish", "message-1", "orders", "producer-b", now.Add(time.Millisecond)),
		messageEvent("publish-refunds", "publish", "message-1", "refunds", "producer-c", now.Add(2*time.Millisecond)),
		messageEvent("consume-orders", "consume", "message-1", "orders", "consumer-a", now.Add(3*time.Millisecond)),
		messageEvent("consume-refunds", "consume", "message-1", "refunds", "consumer-b", now.Add(4*time.Millisecond)),
	}

	g, err := Build(events)
	if err != nil {
		t.Fatalf("build graph: %v", err)
	}
	if len(g.Edges) != 1 || g.Edges[0] != (Edge{From: "publish-refunds", To: "consume-refunds", Kind: EdgeMessage}) {
		t.Fatalf("publisher ambiguity must remain scoped to its topic, got %#v", g.Edges)
	}
}
