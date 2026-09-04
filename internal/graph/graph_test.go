package graph

import (
	"testing"
	"time"

	"github.com/yjr28/kestrel-replay/internal/model"
)

func TestBuildParentAndMessageEdges(t *testing.T) {
	now := time.Now()
	events := []model.Event{
		{ID: "p", Sequence: 1, Source: model.SourceApplication, Kind: model.KindSpan, TraceID: "t", SpanID: "s1", Service: "order", Operation: "create", Timestamp: now},
		{ID: "c", Sequence: 2, Source: model.SourceApplication, Kind: model.KindSpan, TraceID: "t", SpanID: "s2", ParentSpanID: "s1", Service: "inventory", Operation: "check", Timestamp: now.Add(time.Millisecond)},
		{ID: "m1", Sequence: 3, Source: model.SourceApplication, Kind: model.KindMessage, TraceID: "t", Service: "order", Operation: "publish", Timestamp: now.Add(2 * time.Millisecond), Attributes: map[string]string{"message.id": "x", "message.action": "publish"}},
		{ID: "m2", Sequence: 4, Source: model.SourceApplication, Kind: model.KindMessage, TraceID: "t", Service: "audit", Operation: "consume", Timestamp: now.Add(3 * time.Millisecond), Attributes: map[string]string{"message.id": "x", "message.action": "consume"}},
	}
	g, err := Build(events)
	if err != nil {
		t.Fatal(err)
	}
	if len(g.Edges) != 2 {
		t.Fatalf("expected 2 edges, got %d: %#v", len(g.Edges), g.Edges)
	}
}

func TestEarliestMeaningfulDivergenceLatency(t *testing.T) {
	now := time.Now()
	healthy := []model.Event{{ID: "h", Source: model.SourceApplication, Kind: model.KindSpan, TraceID: "t1", SpanID: "s1", Service: "inventory", Operation: "check", Timestamp: now, Status: "ok", Attributes: map[string]string{"duration_us": "1000"}}}
	failing := []model.Event{{ID: "f", Source: model.SourceApplication, Kind: model.KindSpan, TraceID: "t2", SpanID: "s2", Service: "inventory", Operation: "check", Timestamp: now, Status: "ok", Attributes: map[string]string{"duration_us": "80000"}}}
	d, ok := EarliestMeaningfulDivergence(healthy, failing, 20*time.Millisecond)
	if !ok || d.Service != "inventory" || d.Reason != "latency_delta" {
		t.Fatalf("unexpected divergence: ok=%v d=%+v", ok, d)
	}
}

func TestTerminalServiceAnchorFindsMissingCrashSpan(t *testing.T) {
	now := time.Now()
	healthy := []model.Event{
		{ID: "h-order", Source: model.SourceApplication, Kind: model.KindSpan, TraceID: "t1", SpanID: "o1", Service: "order", Operation: "create", Timestamp: now, Status: "ok", Attributes: map[string]string{"duration_us": "1000"}},
		{ID: "h-inv", Source: model.SourceApplication, Kind: model.KindSpan, TraceID: "t1", SpanID: "i1", Service: "inventory", Operation: "check", Timestamp: now.Add(time.Millisecond), Status: "ok", Attributes: map[string]string{"duration_us": "500"}},
	}
	failing := []model.Event{
		{ID: "f-order", Source: model.SourceApplication, Kind: model.KindSpan, TraceID: "t2", SpanID: "o2", Service: "order", Operation: "create", Timestamp: now, Status: "error", Attributes: map[string]string{"duration_us": "900"}},
		{ID: "fault", Source: model.SourceFault, Kind: model.KindFault, CorrelationID: "req", Service: "inventory", Operation: "process_exit", Timestamp: now.Add(-time.Millisecond), Status: "injected", Attributes: map[string]string{"fault.kind": "service_crash", "target.service": "inventory"}},
	}

	d, ok := EarliestMeaningfulDivergenceForTerminalService(healthy, failing, 20*time.Millisecond, "inventory")
	if !ok || d.Service != "inventory" || d.Operation != "check" || d.Reason != "missing_span" {
		t.Fatalf("unexpected crash divergence: ok=%v d=%+v", ok, d)
	}
}

func TestTerminalServiceAnchorPrefersTerminalStatusChange(t *testing.T) {
	now := time.Now()
	healthy := []model.Event{
		{ID: "h-gw", Source: model.SourceApplication, Kind: model.KindSpan, TraceID: "t1", SpanID: "g1", Service: "gateway", Operation: "create_order", Timestamp: now, Status: "ok", Attributes: map[string]string{"duration_us": "1000"}},
		{ID: "h-inv", Source: model.SourceApplication, Kind: model.KindSpan, TraceID: "t1", SpanID: "i1", Service: "inventory", Operation: "check", Timestamp: now.Add(time.Millisecond), Status: "ok", Attributes: map[string]string{"duration_us": "500"}},
	}
	failing := []model.Event{
		{ID: "f-gw", Source: model.SourceApplication, Kind: model.KindSpan, TraceID: "t2", SpanID: "g2", Service: "gateway", Operation: "create_order", Timestamp: now, Status: "error", Attributes: map[string]string{"duration_us": "900"}},
		{ID: "f-inv", Source: model.SourceApplication, Kind: model.KindSpan, TraceID: "t2", SpanID: "i2", Service: "inventory", Operation: "check", Timestamp: now.Add(time.Millisecond), Status: "error", Attributes: map[string]string{"duration_us": "600"}},
	}

	d, ok := EarliestMeaningfulDivergenceForTerminalService(healthy, failing, 20*time.Millisecond, "inventory")
	if !ok || d.Service != "inventory" || d.Operation != "check" || d.Reason != "terminal_status_change" {
		t.Fatalf("unexpected terminal divergence: ok=%v d=%+v", ok, d)
	}
}
