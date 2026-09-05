package graph

import (
	"testing"
	"time"

	"github.com/yjr28/kestrel-replay/internal/model"
)

func TestRankDivergencesAbstainsWhenFailingSpanEventIdentityIsReused(t *testing.T) {
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
	if len(candidates) != 0 {
		t.Fatalf("reused failing event identity makes affected keys ambiguous; got candidates %#v", candidates)
	}
}

func TestRankDivergencesAbstainsWhenHealthySpanEventIdentityIsReused(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	healthy := []model.Event{
		{
			ID: "reused", Source: model.SourceApplication, Kind: model.KindSpan,
			Service: "orders", Operation: "POST /orders", Status: "ok", Timestamp: now,
		},
		{
			ID: "reused", Source: model.SourceApplication, Kind: model.KindSpan,
			Service: "inventory", Operation: "GET /stock", Status: "ok", Timestamp: now.Add(time.Second),
		},
	}
	failing := []model.Event{{
		ID: "failing-orders", Source: model.SourceApplication, Kind: model.KindSpan,
		Service: "orders", Operation: "POST /orders", Status: "error", Timestamp: now.Add(2 * time.Second),
	}}

	candidates := RankDivergences(healthy, failing, 0, "")
	if len(candidates) != 0 {
		t.Fatalf("reused healthy event identity makes affected keys ambiguous; got candidates %#v", candidates)
	}
}

func TestRankDivergencesAbstainsWhenFailingEventIDIsReusedByMalformedSpan(t *testing.T) {
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
			Service: "", Operation: "GET /stock", Status: "error", Timestamp: now.Add(2 * time.Second),
		},
	}

	candidates := RankDivergences(healthy, failing, 0, "")
	if len(candidates) != 0 {
		t.Fatalf("event identity reused by malformed application span must make eligible key ambiguous; got candidates %#v", candidates)
	}
}

func TestRankDivergencesAbstainsWhenHealthyEventIDIsReusedByMalformedSpan(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	healthy := []model.Event{
		{
			ID: "reused", Source: model.SourceApplication, Kind: model.KindSpan,
			Service: "orders", Operation: "POST /orders", Status: "ok", Timestamp: now,
		},
		{
			ID: "reused", Source: model.SourceApplication, Kind: model.KindSpan,
			Service: "inventory", Operation: "", Status: "ok", Timestamp: now.Add(time.Second),
		},
	}
	failing := []model.Event{{
		ID: "failing-orders", Source: model.SourceApplication, Kind: model.KindSpan,
		Service: "orders", Operation: "POST /orders", Status: "error", Timestamp: now.Add(2 * time.Second),
	}}

	candidates := RankDivergences(healthy, failing, 0, "")
	if len(candidates) != 0 {
		t.Fatalf("event identity reused by malformed application span must make eligible key ambiguous; got candidates %#v", candidates)
	}
}
