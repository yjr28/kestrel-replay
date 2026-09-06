package graph

import (
	"strings"
	"testing"
	"time"

	"github.com/yjr28/kestrel-replay/internal/model"
)

func TestBuildRejectsFormattingOnlyDuplicateEventIDs(t *testing.T) {
	now := time.Now().UTC()
	events := []model.Event{
		{
			ID: "span-1", Source: model.SourceApplication, Kind: model.KindSpan,
			TraceID: "trace-1", SpanID: "parent", Service: "api", Operation: "handle", Timestamp: now,
		},
		{
			ID: " span-1 ", Source: model.SourceApplication, Kind: model.KindSpan,
			TraceID: "trace-1", SpanID: "child", Service: "worker", Operation: "work", Timestamp: now.Add(time.Millisecond),
		},
	}

	_, err := Build(events)
	if err == nil {
		t.Fatal("expected formatting-only duplicate event ids to be rejected")
	}
	if !strings.Contains(err.Error(), `duplicate event id "span-1"`) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestBuildCanonicalizesSpanIdentityForParentEdges(t *testing.T) {
	now := time.Now().UTC()
	events := []model.Event{
		{
			ID: " parent-event ", Source: model.SourceApplication, Kind: model.KindSpan,
			TraceID: " trace-1 ", SpanID: " parent-span ", Service: "api", Operation: "handle", Timestamp: now,
		},
		{
			ID: "child-event", Source: model.SourceApplication, Kind: model.KindSpan,
			TraceID: "trace-1", SpanID: "child-span", ParentSpanID: " parent-span ", Service: "worker", Operation: "work", Timestamp: now.Add(time.Millisecond),
		},
	}

	g, err := Build(events)
	if err != nil {
		t.Fatalf("build graph: %v", err)
	}
	if _, ok := g.Nodes["parent-event"]; !ok {
		t.Fatalf("expected canonical parent event id in graph nodes: %#v", g.Nodes)
	}
	if len(g.Edges) != 1 || g.Edges[0] != (Edge{From: "parent-event", To: "child-event", Kind: EdgeParentSpan}) {
		t.Fatalf("expected canonical parent span edge, got %#v", g.Edges)
	}
}

func TestBuildCanonicalizesMessageIdentityForEdges(t *testing.T) {
	now := time.Now().UTC()
	events := []model.Event{
		{
			ID: "publish-event", Source: model.SourceApplication, Kind: model.KindMessage,
			TraceID: "trace-1", Service: "producer", Timestamp: now,
			Attributes: map[string]string{"message.id": " message-1 ", "message.action": " publish ", "topic": "orders"},
		},
		{
			ID: "consume-event", Source: model.SourceApplication, Kind: model.KindMessage,
			TraceID: "trace-1", Service: "consumer", Timestamp: now.Add(time.Millisecond),
			Attributes: map[string]string{"message.id": "message-1", "message.action": "consume", "topic": "orders"},
		},
	}

	g, err := Build(events)
	if err != nil {
		t.Fatalf("build graph: %v", err)
	}
	if len(g.Edges) != 1 || g.Edges[0] != (Edge{From: "publish-event", To: "consume-event", Kind: EdgeMessage}) {
		t.Fatalf("expected canonical message edge, got %#v", g.Edges)
	}
}
