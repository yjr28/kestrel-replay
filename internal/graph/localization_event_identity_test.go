package graph

import (
	"testing"
	"time"

	"github.com/yjr28/kestrel-replay/internal/model"
)

func TestRankDivergencesRequiresEventIdentityForSpanEvidence(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	healthy := []model.Event{{
		ID: "healthy-orders", Source: model.SourceApplication, Kind: model.KindSpan,
		Service: "orders", Operation: "POST /orders", Status: "ok", Timestamp: now,
	}}
	failing := []model.Event{{
		ID: "", Source: model.SourceApplication, Kind: model.KindSpan,
		Service: "orders", Operation: "POST /orders", Status: "error", Timestamp: now.Add(time.Second),
	}}

	candidates := RankDivergences(healthy, failing, 0, "")
	if len(candidates) != 1 {
		t.Fatalf("got %d candidates, want 1: %#v", len(candidates), candidates)
	}
	if got := candidates[0]; got.Reason != "missing_span" || got.HealthyEventID != "healthy-orders" || got.FailingEventID != "" {
		t.Fatalf("id-less failing span must not establish paired evidence, got %#v", got)
	}

	unexpectedOnly := RankDivergences(nil, []model.Event{{
		ID: " ", Source: model.SourceApplication, Kind: model.KindSpan,
		Service: "inventory", Operation: "GET /stock", Status: "error", Timestamp: now,
	}}, 0, "")
	if len(unexpectedOnly) != 0 {
		t.Fatalf("id-less span must not establish unexpected-span evidence: %#v", unexpectedOnly)
	}
}
