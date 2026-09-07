package graph

import (
	"testing"
	"time"

	"github.com/yjr28/kestrel-replay/internal/model"
)

func TestBuildWithholdsEqualTimestampMessageEdgeWithoutCompleteSequenceEvidence(t *testing.T) {
	ts := time.Date(2026, 9, 7, 3, 0, 0, 0, time.UTC)
	publish := model.Event{
		ID: "publish", Sequence: 0, Source: model.SourceApplication, Kind: model.KindMessage,
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
		t.Fatal("message edge used equal-timestamp evidence without complete nonzero sequence ordering")
	}
}
