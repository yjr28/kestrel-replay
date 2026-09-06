package graph

import (
	"testing"
	"time"

	"github.com/yjr28/kestrel-replay/internal/model"
)

func TestEarliestMeaningfulDivergenceCanonicalizesUnexpectedSpanEventID(t *testing.T) {
	now := time.Now().UTC()
	failing := []model.Event{{
		ID: " failing-cache ", Source: model.SourceApplication, Kind: model.KindSpan,
		TraceID: "trace-1", SpanID: "span-1", Service: "cache", Operation: "lookup", Status: "error", Timestamp: now,
	}}

	divergence, ok := EarliestMeaningfulDivergence(nil, failing, 20*time.Millisecond)
	if !ok {
		t.Fatal("expected identified unexpected span evidence")
	}
	if divergence.Reason != "unexpected_span" || divergence.FailingEventID != "failing-cache" {
		t.Fatalf("expected canonical unexpected-span provenance, got %+v", divergence)
	}
}

func TestTerminalServiceAnchorCanonicalizesHealthySpanEventID(t *testing.T) {
	now := time.Now().UTC()
	healthy := []model.Event{{
		ID: " healthy-inventory ", Source: model.SourceApplication, Kind: model.KindSpan,
		TraceID: "trace-1", SpanID: "span-1", Service: "inventory", Operation: "check", Status: "ok", Timestamp: now,
	}}

	divergence, ok := EarliestMeaningfulDivergenceForTerminalService(healthy, nil, 20*time.Millisecond, "inventory")
	if !ok {
		t.Fatal("expected terminal-service missing span evidence")
	}
	if divergence.Reason != "missing_span" || divergence.HealthyEventID != "healthy-inventory" {
		t.Fatalf("expected canonical terminal-anchor provenance, got %+v", divergence)
	}
}
