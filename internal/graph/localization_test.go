package graph

import (
	"testing"
	"time"

	"github.com/yjr28/kestrel-replay/internal/model"
)

func TestRankDivergencesLatencyPrefersTerminalLocalAnomaly(t *testing.T) {
	now := time.Now().UTC()
	healthy := []model.Event{
		{ID: "h-order", Source: model.SourceApplication, Kind: model.KindSpan, TraceID: "h", SpanID: "ho", Service: "order", Operation: "create", Timestamp: now, Status: "ok", Attributes: map[string]string{"duration_us": "2000"}},
		{ID: "h-inv", Source: model.SourceApplication, Kind: model.KindSpan, TraceID: "h", SpanID: "hi", Service: "inventory", Operation: "check", Timestamp: now.Add(time.Millisecond), Status: "ok", Attributes: map[string]string{"duration_us": "1000"}},
	}
	failing := []model.Event{
		{ID: "f-order", Source: model.SourceApplication, Kind: model.KindSpan, TraceID: "f", SpanID: "fo", Service: "order", Operation: "create", Timestamp: now, Status: "error", Attributes: map[string]string{"duration_us": "43000"}},
		{ID: "f-inv", Source: model.SourceApplication, Kind: model.KindSpan, TraceID: "f", SpanID: "fi", Service: "inventory", Operation: "check", Timestamp: now.Add(time.Millisecond), Status: "ok", Attributes: map[string]string{"duration_us": "76000"}},
	}

	got := RankDivergences(healthy, failing, 20*time.Millisecond, "inventory")
	if len(got) < 2 {
		t.Fatalf("expected ranked candidates, got %#v", got)
	}
	if got[0].Service != "inventory" || got[0].Operation != "check" || got[0].Reason != "latency_delta" {
		t.Fatalf("unexpected top candidate: %+v", got[0])
	}
	if got[0].ConfidenceModel != localizationConfidenceModel || got[0].ConfidenceScore <= got[1].ConfidenceScore {
		t.Fatalf("unexpected score metadata/order: top=%+v second=%+v", got[0], got[1])
	}
	if got[0].HealthyEventID != "h-inv" || got[0].FailingEventID != "f-inv" || got[0].Anchor != "outcome.terminal_service=inventory" {
		t.Fatalf("unexpected provenance: %+v", got[0])
	}
}

func TestRankDivergencesCrashPrefersMissingTerminalSpan(t *testing.T) {
	now := time.Now().UTC()
	healthy := []model.Event{
		{ID: "h-order", Source: model.SourceApplication, Kind: model.KindSpan, TraceID: "h", SpanID: "ho", Service: "order", Operation: "create", Timestamp: now, Status: "ok"},
		{ID: "h-inv", Source: model.SourceApplication, Kind: model.KindSpan, TraceID: "h", SpanID: "hi", Service: "inventory", Operation: "check", Timestamp: now.Add(time.Millisecond), Status: "ok"},
	}
	failing := []model.Event{
		{ID: "f-order", Source: model.SourceApplication, Kind: model.KindSpan, TraceID: "f", SpanID: "fo", Service: "order", Operation: "create", Timestamp: now, Status: "error"},
	}

	got := RankDivergences(healthy, failing, 20*time.Millisecond, "inventory")
	if len(got) == 0 || got[0].Service != "inventory" || got[0].Operation != "check" || got[0].Reason != "missing_span" {
		t.Fatalf("unexpected crash ranking: %#v", got)
	}
	if got[0].HealthyEventID != "h-inv" || got[0].FailingEventID != "" {
		t.Fatalf("unexpected crash provenance: %+v", got[0])
	}
}

func TestRankDivergencesResetPrefersTerminalStatusChange(t *testing.T) {
	now := time.Now().UTC()
	healthy := []model.Event{
		{ID: "h-inv", Source: model.SourceApplication, Kind: model.KindSpan, TraceID: "h", SpanID: "hi", Service: "inventory", Operation: "check", Timestamp: now, Status: "ok"},
		{ID: "h-order", Source: model.SourceApplication, Kind: model.KindSpan, TraceID: "h", SpanID: "ho", Service: "order", Operation: "create", Timestamp: now.Add(time.Millisecond), Status: "ok"},
	}
	failing := []model.Event{
		{ID: "f-inv", Source: model.SourceApplication, Kind: model.KindSpan, TraceID: "f", SpanID: "fi", Service: "inventory", Operation: "check", Timestamp: now, Status: "error"},
		{ID: "f-order", Source: model.SourceApplication, Kind: model.KindSpan, TraceID: "f", SpanID: "fo", Service: "order", Operation: "create", Timestamp: now.Add(time.Millisecond), Status: "error"},
	}

	got := RankDivergences(healthy, failing, time.Hour, "inventory")
	if len(got) < 2 || got[0].Service != "inventory" || got[0].Reason != "terminal_status_change" {
		t.Fatalf("unexpected reset ranking: %#v", got)
	}
	if !TopKContains(got, "inventory", "check", 1) || !TopKContains(got, "inventory", "check", 3) {
		t.Fatalf("expected inventory/check in top-k: %#v", got)
	}
	if TopKContains(got, "payment", "authorize", 3) || TopKContains(got, "inventory", "check", 0) {
		t.Fatalf("unexpected top-k result: %#v", got)
	}
}

func TestConfidenceScoreIsDeterministicAndNotInjectorDriven(t *testing.T) {
	now := time.Now().UTC()
	healthy := []model.Event{{ID: "h", Source: model.SourceApplication, Kind: model.KindSpan, TraceID: "h", SpanID: "hs", Service: "inventory", Operation: "check", Timestamp: now, Status: "ok"}}
	failing := []model.Event{
		{ID: "injector", Source: model.SourceFault, Kind: model.KindFault, CorrelationID: "r", Service: "inventory", Timestamp: now.Add(-time.Second), Attributes: map[string]string{"fault.kind": "service_crash", "target.service": "inventory"}},
	}

	a := RankDivergences(healthy, failing, time.Second, "inventory")
	b := RankDivergences(healthy, failing, time.Second, "inventory")
	if len(a) != 1 || len(b) != 1 || a[0].ConfidenceScore != b[0].ConfidenceScore {
		t.Fatalf("ranking was not deterministic: a=%#v b=%#v", a, b)
	}
	if a[0].HealthyEventID != "h" || a[0].FailingEventID != "" || a[0].Reason != "missing_span" {
		t.Fatalf("injector evidence leaked into localization: %+v", a[0])
	}
}
