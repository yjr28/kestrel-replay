package graph

import (
	"testing"
	"time"

	"github.com/yjr28/kestrel-replay/internal/model"
)

func TestEarliestDivergenceDoesNotReintroduceReusedEventIdentity(t *testing.T) {
	now := time.Now().UTC()
	healthy := []model.Event{profileSpan("h-order", "order", "create", "ok", 1000, now)}
	failing := []model.Event{
		profileSpan("f-order", "order", "create", "ok", 1000, now),
		profileSpan("shared", "cache", "lookup", "ok", 1000, now),
		profileSpan("shared", "inventory", "check", "ok", 1000, now),
	}
	if divergence, ok := EarliestMeaningfulDivergence(healthy, failing, 20*time.Millisecond); ok {
		t.Fatalf("reused event identity is ineligible evidence, got %+v", divergence)
	}
}

func TestEarliestDivergenceDoesNotReintroduceUnkeyedFailingSpan(t *testing.T) {
	now := time.Now().UTC()
	healthy := []model.Event{profileSpan("h-order", "order", "create", "ok", 1000, now)}
	failing := []model.Event{
		profileSpan("f-order", "order", "create", "ok", 1000, now),
		profileSpan("f-unkeyed", "inventory", "   ", "error", 1000, now),
	}
	if divergence, ok := EarliestMeaningfulDivergence(healthy, failing, 20*time.Millisecond); ok {
		t.Fatalf("unkeyed failing span is ineligible evidence, got %+v", divergence)
	}
}

func TestEarliestDivergenceDoesNotReintroduceUnkeyedHealthySpan(t *testing.T) {
	now := time.Now().UTC()
	healthy := []model.Event{
		profileSpan("h-order", "order", "create", "ok", 1000, now),
		profileSpan("h-unkeyed", "inventory", "   ", "ok", 1000, now),
	}
	failing := []model.Event{profileSpan("f-order", "order", "create", "ok", 1000, now)}
	if divergence, ok := EarliestMeaningfulDivergence(healthy, failing, 20*time.Millisecond); ok {
		t.Fatalf("unkeyed healthy span is ineligible evidence, got %+v", divergence)
	}
}

func TestTerminalEarliestDivergenceRequiresIndexedHealthyEvidence(t *testing.T) {
	now := time.Now().UTC()
	healthy := []model.Event{
		profileSpan("h-order", "order", "create", "ok", 1000, now),
		profileSpan("h-unkeyed", "inventory", "   ", "ok", 1000, now),
	}
	failing := []model.Event{profileSpan("f-order", "order", "create", "ok", 1000, now)}
	if divergence, ok := EarliestMeaningfulDivergenceForTerminalService(healthy, failing, 20*time.Millisecond, "inventory"); ok {
		t.Fatalf("terminal anchor must not revive ineligible healthy evidence, got %+v", divergence)
	}
}
