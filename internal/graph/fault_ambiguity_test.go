package graph

import (
	"testing"
	"time"

	"github.com/yjr28/kestrel-replay/internal/model"
)

func TestBuildWithholdsFaultEdgeWhenMultipleFaultsCouldExplainSpan(t *testing.T) {
	now := time.Now().UTC()
	events := []model.Event{
		{ID: "fault-1", Sequence: 1, Source: model.SourceFault, Kind: model.KindFault, Timestamp: now, Attributes: map[string]string{"fault.kind": "latency", "target.service": "inventory", "target.operation": "check"}},
		{ID: "fault-2", Sequence: 2, Source: model.SourceFault, Kind: model.KindFault, Timestamp: now.Add(time.Millisecond), Attributes: map[string]string{"fault.kind": "connection_reset", "target.service": "inventory", "target.operation": "check"}},
		{ID: "span", Sequence: 3, Source: model.SourceApplication, Kind: model.KindSpan, TraceID: "trace", SpanID: "check", Service: "inventory", Operation: "check", Timestamp: now.Add(2 * time.Millisecond)},
	}

	g, err := Build(events)
	if err != nil {
		t.Fatal(err)
	}
	for _, edge := range g.Edges {
		if edge.Kind == EdgeFault && edge.To == "span" {
			t.Fatalf("multiple eligible fault events are ambiguous and must not be collapsed to one causal edge: %#v", g.Edges)
		}
	}
}
