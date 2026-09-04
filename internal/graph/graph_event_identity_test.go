package graph

import (
	"testing"
	"time"

	"github.com/yjr28/kestrel-replay/internal/model"
)

func TestEarliestMeaningfulDivergenceIgnoresUnidentifiedFailingSpan(t *testing.T) {
	now := time.Now().UTC()
	healthy := []model.Event{
		identitySpan("healthy-inventory", "inventory", "check", "ok", now),
	}
	failing := []model.Event{
		identitySpan("", "inventory", "check", "error", now.Add(time.Second)),
	}

	divergence, ok := EarliestMeaningfulDivergence(healthy, failing, 20*time.Millisecond)
	if !ok {
		t.Fatal("expected identified healthy span to remain missing")
	}
	if divergence.Reason != "missing_span" || divergence.Service != "inventory" || divergence.Operation != "check" {
		t.Fatalf("unidentified failing span must not satisfy healthy evidence: %+v", divergence)
	}
	if divergence.HealthyEventID != "healthy-inventory" || divergence.FailingEventID != "" {
		t.Fatalf("unexpected provenance: %+v", divergence)
	}
}

func TestEarliestMeaningfulDivergenceIgnoresUnidentifiedUnexpectedSpan(t *testing.T) {
	now := time.Now().UTC()
	healthy := []model.Event{
		identitySpan("healthy-order", "order", "create", "ok", now),
	}
	failing := []model.Event{
		identitySpan("failing-order", "order", "create", "ok", now.Add(time.Second)),
		identitySpan("", "cache", "lookup", "error", now.Add(2*time.Second)),
	}

	if divergence, ok := EarliestMeaningfulDivergence(healthy, failing, 20*time.Millisecond); ok {
		t.Fatalf("unidentified span must not establish unexpected evidence: %+v", divergence)
	}
}

func TestTerminalServiceAnchorDoesNotUseUnidentifiedHealthySpan(t *testing.T) {
	now := time.Now().UTC()
	healthy := []model.Event{
		identitySpan("healthy-order", "order", "create", "ok", now),
		identitySpan("", "inventory", "check", "ok", now.Add(time.Millisecond)),
	}
	failing := []model.Event{
		identitySpan("failing-order", "order", "create", "ok", now.Add(time.Second)),
	}

	if divergence, ok := EarliestMeaningfulDivergenceForTerminalService(healthy, failing, 20*time.Millisecond, "inventory"); ok {
		t.Fatalf("terminal anchor must not promote unnameable evidence: %+v", divergence)
	}
}

func identitySpan(id, service, operation, status string, at time.Time) model.Event {
	return model.Event{
		ID: id,
		Source: model.SourceApplication,
		Kind: model.KindSpan,
		Service: service,
		Operation: operation,
		Status: status,
		Timestamp: at,
		Attributes: map[string]string{"duration_us": "1000"},
	}
}
