package graph

import (
	"testing"
	"time"

	"github.com/yjr28/kestrel-replay/internal/model"
)

func TestBuildRequiresPublishEvidenceBeforeConsume(t *testing.T) {
	base := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	publish := model.Event{
		ID: "publish", Sequence: 2, Source: model.SourceApplication, Kind: model.KindMessage,
		TraceID: "trace", Service: "producer", Timestamp: base.Add(time.Second),
		Attributes: map[string]string{"message.id": "m-1", "message.action": "publish", "topic": "orders"},
	}
	consume := model.Event{
		ID: "consume", Sequence: 1, Source: model.SourceApplication, Kind: model.KindMessage,
		TraceID: "trace", Service: "consumer", Timestamp: base,
		Attributes: map[string]string{"message.id": "m-1", "message.action": "consume", "topic": "orders"},
	}

	g, err := Build([]model.Event{publish, consume})
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	if hasMessageEdge(g, "publish", "consume") {
		t.Fatal("message edge used publish evidence that occurs after consume evidence")
	}
}

func TestBuildRequiresSequenceOrderingForEqualTimestampMessageEvidence(t *testing.T) {
	ts := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	publish := model.Event{
		ID: "publish", Sequence: 2, Source: model.SourceApplication, Kind: model.KindMessage,
		TraceID: "trace", Service: "producer", Timestamp: ts,
		Attributes: map[string]string{"message.id": "m-1", "message.action": "publish", "topic": "orders"},
	}
	consume := model.Event{
		ID: "consume", Sequence: 1, Source: model.SourceApplication, Kind: model.KindMessage,
		TraceID: "trace", Service: "consumer", Timestamp: ts,
		Attributes: map[string]string{"message.id": "m-1", "message.action": "consume", "topic": "orders"},
	}

	g, err := Build([]model.Event{publish, consume})
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	if hasMessageEdge(g, "publish", "consume") {
		t.Fatal("message edge used equal-timestamp publish evidence that is not earlier by sequence")
	}
}

func TestBuildLinksEqualTimestampMessageEvidenceWhenPublishSequenceIsEarlier(t *testing.T) {
	ts := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	publish := model.Event{
		ID: "publish", Sequence: 1, Source: model.SourceApplication, Kind: model.KindMessage,
		TraceID: "trace", Service: "producer", Timestamp: ts,
		Attributes: map[string]string{"message.id": "m-1", "message.action": "publish", "topic": "orders"},
	}
	consume := model.Event{
		ID: "consume", Sequence: 2, Source: model.SourceApplication, Kind: model.KindMessage,
		TraceID: "trace", Service: "consumer", Timestamp: ts,
		Attributes: map[string]string{"message.id": "m-1", "message.action": "consume", "topic": "orders"},
	}

	g, err := Build([]model.Event{publish, consume})
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	if !hasMessageEdge(g, "publish", "consume") {
		t.Fatal("expected message edge when equal-timestamp publish evidence is earlier by sequence")
	}
}

func TestBuildScopesPublisherAmbiguityToEvidenceBeforeConsume(t *testing.T) {
	base := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	earlyPublish := model.Event{
		ID: "publish-early", Sequence: 1, Source: model.SourceApplication, Kind: model.KindMessage,
		TraceID: "trace", Service: "producer-a", Timestamp: base,
		Attributes: map[string]string{"message.id": "m-1", "message.action": "publish", "topic": "orders"},
	}
	consume := model.Event{
		ID: "consume", Sequence: 2, Source: model.SourceApplication, Kind: model.KindMessage,
		TraceID: "trace", Service: "consumer", Timestamp: base.Add(time.Second),
		Attributes: map[string]string{"message.id": "m-1", "message.action": "consume", "topic": "orders"},
	}
	latePublish := model.Event{
		ID: "publish-late", Sequence: 3, Source: model.SourceApplication, Kind: model.KindMessage,
		TraceID: "trace", Service: "producer-b", Timestamp: base.Add(2 * time.Second),
		Attributes: map[string]string{"message.id": "m-1", "message.action": "publish", "topic": "orders"},
	}

	g, err := Build([]model.Event{latePublish, consume, earlyPublish})
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	if !hasMessageEdge(g, "publish-early", "consume") {
		t.Fatal("later duplicate publish evidence suppressed the unique preceding publisher")
	}
	if hasMessageEdge(g, "publish-late", "consume") {
		t.Fatal("consume linked to publish evidence that occurs after it")
	}
}

func TestBuildWithholdsMessageEdgeWhenMultiplePublishersPrecedeConsume(t *testing.T) {
	base := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	publishA := model.Event{
		ID: "publish-a", Sequence: 1, Source: model.SourceApplication, Kind: model.KindMessage,
		TraceID: "trace", Service: "producer-a", Timestamp: base,
		Attributes: map[string]string{"message.id": "m-1", "message.action": "publish", "topic": "orders"},
	}
	publishB := model.Event{
		ID: "publish-b", Sequence: 2, Source: model.SourceApplication, Kind: model.KindMessage,
		TraceID: "trace", Service: "producer-b", Timestamp: base.Add(time.Second),
		Attributes: map[string]string{"message.id": "m-1", "message.action": "publish", "topic": "orders"},
	}
	consume := model.Event{
		ID: "consume", Sequence: 3, Source: model.SourceApplication, Kind: model.KindMessage,
		TraceID: "trace", Service: "consumer", Timestamp: base.Add(2 * time.Second),
		Attributes: map[string]string{"message.id": "m-1", "message.action": "consume", "topic": "orders"},
	}

	g, err := Build([]model.Event{publishA, publishB, consume})
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	if hasMessageEdge(g, "publish-a", "consume") || hasMessageEdge(g, "publish-b", "consume") {
		t.Fatal("message edge should be withheld when multiple publishers precede the consume")
	}
}

func hasMessageEdge(g *Graph, from, to string) bool {
	for _, edge := range g.Edges {
		if edge.From == from && edge.To == to && edge.Kind == EdgeMessage {
			return true
		}
	}
	return false
}
