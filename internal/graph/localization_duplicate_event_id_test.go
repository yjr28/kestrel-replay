package graph

import (
	"testing"
	"time"

	"github.com/yjr28/kestrel-replay/internal/model"
)

func TestRankDivergencesExcludesReusedSpanEventIdentity(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	healthy := []model.Event{{
		ID: "healthy-orders", Source: model.SourceApplication, Kind: model.KindSpan,
		Service: "orders", Operation: "POST /orders", Status: "ok", Timestamp: now,
	}}
	failing := []model.Event{
		{
			ID: "reused", Source: model.SourceApplication, Kind: model.KindSpan,
			Service: "orders", Operation: "POST /orders", Status: "error", Timestamp: now.Add(time.Second),
		},
		{
			ID: "reused", Source: model.SourceApplication, Kind: model.KindSpan,
			Service: "inventory", Operation: "GET /stock", Status: "error", Timestamp: now.Add(2 * time.Second),
		},
	}

	candidates := RankDivergences(healthy, failing, 0, "")
	if len(candidates) != 1 {
		t.Fatalf("got %d candidates, want only the healthy span reported missing: %#v", len(candidates), candidates)
	}
	if got := candidates[0]; got.Reason != "missing_span" || got.Service != "orders" || got.HealthyEventID != "healthy-orders" || got.FailingEventID != "" {
		t.Fatalf("reused event identity must not establish failing span evidence: %#v", got)
	}
}
