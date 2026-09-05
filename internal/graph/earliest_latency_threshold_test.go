package graph

import (
	"testing"
	"time"

	"github.com/yjr28/kestrel-replay/internal/model"
)

func TestEarliestDivergenceRequiresPositiveLatencyThreshold(t *testing.T) {
	now := time.Now().UTC()
	healthy := []model.Event{profileSpan("h-order", "order", "create", "ok", 1000, now)}
	failing := []model.Event{profileSpan("f-order", "order", "create", "ok", 1000, now)}

	for _, threshold := range []time.Duration{0, -time.Millisecond} {
		if divergence, ok := EarliestMeaningfulDivergence(healthy, failing, threshold); ok {
			t.Fatalf("non-positive latency threshold must not create latency evidence (threshold=%v): %+v", threshold, divergence)
		}
	}
}
