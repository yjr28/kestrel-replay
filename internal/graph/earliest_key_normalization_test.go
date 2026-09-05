package graph

import (
	"testing"
	"time"

	"github.com/yjr28/kestrel-replay/internal/model"
)

func TestEarliestDivergenceNormalizesSpanKeys(t *testing.T) {
	now := time.Now().UTC()
	healthy := []model.Event{profileSpan("h-order", "order", "create", "ok", 1000, now)}
	failing := []model.Event{profileSpan("f-order", "  order\t", " create ", "ok", 1000, now)}

	if divergence, ok := EarliestMeaningfulDivergence(healthy, failing, 20*time.Millisecond); ok {
		t.Fatalf("formatting-only span key differences must not create divergence, got %+v", divergence)
	}
}

func TestTerminalEarliestDivergenceNormalizesSpanKeyAndStatusEvidence(t *testing.T) {
	now := time.Now().UTC()
	healthy := []model.Event{profileSpan("h-inventory", " inventory ", " check ", " ok ", 1000, now)}
	failing := []model.Event{profileSpan("f-inventory", "inventory", "check", "ok", 1000, now)}

	if divergence, ok := EarliestMeaningfulDivergenceForTerminalService(healthy, failing, 20*time.Millisecond, " inventory "); ok {
		t.Fatalf("formatting-only terminal span/status differences must not create divergence, got %+v", divergence)
	}
}

func TestEarliestDivergenceRequiresNonblankStatusEvidence(t *testing.T) {
	now := time.Now().UTC()
	healthy := []model.Event{profileSpan("h-order", "order", "create", "   ", 1000, now)}
	failing := []model.Event{profileSpan("f-order", "order", "create", "error", 1000, now)}

	if divergence, ok := EarliestMeaningfulDivergence(healthy, failing, 20*time.Millisecond); ok {
		t.Fatalf("blank status is unavailable status evidence, got %+v", divergence)
	}
}
