package graph

import (
	"testing"
	"time"

	"github.com/yjr28/kestrel-replay/internal/model"
)

func TestBuildScopesOperationFaultEdgesToTargetOperation(t *testing.T) {
	now := time.Now()
	events := []model.Event{
		{ID: "fault", Sequence: 1, Source: model.SourceFault, Kind: model.KindFault, Timestamp: now, Attributes: map[string]string{"fault.kind": "latency", "target.service": "inventory", "target.operation": "check"}},
		{ID: "reserve", Sequence: 2, Source: model.SourceApplication, Kind: model.KindSpan, TraceID: "trace", SpanID: "reserve", Service: "inventory", Operation: "reserve", Timestamp: now.Add(time.Millisecond)},
		{ID: "check", Sequence: 3, Source: model.SourceApplication, Kind: model.KindSpan, TraceID: "trace", SpanID: "check", Service: "inventory", Operation: "check", Timestamp: now.Add(2 * time.Millisecond)},
	}

	g, err := Build(events)
	if err != nil {
		t.Fatal(err)
	}
	for _, edge := range g.Edges {
		if edge.Kind != EdgeFault {
			continue
		}
		if edge.From != "fault" || edge.To != "check" {
			t.Fatalf("operation-scoped fault must only link to its target operation: %#v", g.Edges)
		}
	}
	if !hasEdge(g.Edges, Edge{From: "fault", To: "check", Kind: EdgeFault}) {
		t.Fatalf("expected target operation fault edge, got %#v", g.Edges)
	}
}

func TestBuildKeepsServiceCrashFaultEdgesServiceScoped(t *testing.T) {
	now := time.Now()
	events := []model.Event{
		{ID: "fault", Sequence: 1, Source: model.SourceFault, Kind: model.KindFault, Timestamp: now, Attributes: map[string]string{"fault.kind": "service_crash", "target.service": "inventory"}},
		{ID: "reserve", Sequence: 2, Source: model.SourceApplication, Kind: model.KindSpan, TraceID: "trace", SpanID: "reserve", Service: "inventory", Operation: "reserve", Timestamp: now.Add(time.Millisecond)},
	}

	g, err := Build(events)
	if err != nil {
		t.Fatal(err)
	}
	if !hasEdge(g.Edges, Edge{From: "fault", To: "reserve", Kind: EdgeFault}) {
		t.Fatalf("service_crash remains service-scoped evidence, got %#v", g.Edges)
	}
}

func TestBuildRequiresFaultToPrecedeSpanAtEqualTimestamp(t *testing.T) {
	now := time.Now()
	events := []model.Event{
		{ID: "span", Sequence: 1, Source: model.SourceApplication, Kind: model.KindSpan, TraceID: "trace", SpanID: "check", Service: "inventory", Operation: "check", Timestamp: now},
		{ID: "fault", Sequence: 2, Source: model.SourceFault, Kind: model.KindFault, Timestamp: now, Attributes: map[string]string{"fault.kind": "latency", "target.service": "inventory", "target.operation": "check"}},
	}

	g, err := Build(events)
	if err != nil {
		t.Fatal(err)
	}
	if hasEdge(g.Edges, Edge{From: "fault", To: "span", Kind: EdgeFault}) {
		t.Fatalf("fault ordered after span at the same timestamp must not create a fault edge: %#v", g.Edges)
	}
}

func TestBuildAllowsFaultToPrecedeSpanBySequenceAtEqualTimestamp(t *testing.T) {
	now := time.Now()
	events := []model.Event{
		{ID: "fault", Sequence: 1, Source: model.SourceFault, Kind: model.KindFault, Timestamp: now, Attributes: map[string]string{"fault.kind": "latency", "target.service": "inventory", "target.operation": "check"}},
		{ID: "span", Sequence: 2, Source: model.SourceApplication, Kind: model.KindSpan, TraceID: "trace", SpanID: "check", Service: "inventory", Operation: "check", Timestamp: now},
	}

	g, err := Build(events)
	if err != nil {
		t.Fatal(err)
	}
	if !hasEdge(g.Edges, Edge{From: "fault", To: "span", Kind: EdgeFault}) {
		t.Fatalf("fault ordered before span by sequence should remain eligible: %#v", g.Edges)
	}
}

func hasEdge(edges []Edge, want Edge) bool {
	for _, edge := range edges {
		if edge == want {
			return true
		}
	}
	return false
}
