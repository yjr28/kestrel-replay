package graph

import (
	"testing"
	"time"

	"github.com/yjr28/kestrel-replay/internal/model"
)

func TestRankDivergencesAgainstProfileAbstainsWhenFailingSpanEventIdentityIsReused(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	healthyRuns := [][]model.Event{
		{{
			ID: "healthy-orders-1", Source: model.SourceApplication, Kind: model.KindSpan,
			Service: "orders", Operation: "POST /orders", Status: "ok", Timestamp: now,
		}},
		{{
			ID: "healthy-orders-2", Source: model.SourceApplication, Kind: model.KindSpan,
			Service: "orders", Operation: "POST /orders", Status: "ok", Timestamp: now.Add(time.Second),
		}},
	}
	profile, err := BuildHealthyProfile(healthyRuns)
	if err != nil {
		t.Fatalf("BuildHealthyProfile: %v", err)
	}

	failing := []model.Event{
		{
			ID: "reused", Source: model.SourceApplication, Kind: model.KindSpan,
			Service: "orders", Operation: "POST /orders", Status: "error", Timestamp: now.Add(2 * time.Second),
		},
		{
			ID: "reused", Source: model.SourceApplication, Kind: model.KindSpan,
			Service: "inventory", Operation: "GET /stock", Status: "error", Timestamp: now.Add(3 * time.Second),
		},
	}

	candidates := RankDivergencesAgainstProfile(profile, failing, 0, "")
	if len(candidates) != 0 {
		t.Fatalf("reused failing event identity makes the profiled key ambiguous; got candidates %#v", candidates)
	}
}

func TestBuildHealthyProfileRejectsEventIDReusedByMalformedSpan(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	runs := [][]model.Event{
		{
			{
				ID: "reused", Source: model.SourceApplication, Kind: model.KindSpan,
				Service: "orders", Operation: "POST /orders", Status: "ok", Timestamp: now,
			},
			{
				ID: "reused", Source: model.SourceApplication, Kind: model.KindSpan,
				Service: "", Operation: "GET /stock", Status: "ok", Timestamp: now.Add(time.Millisecond),
			},
		},
		{{
			ID: "healthy-orders-2", Source: model.SourceApplication, Kind: model.KindSpan,
			Service: "orders", Operation: "POST /orders", Status: "ok", Timestamp: now.Add(time.Second),
		}},
	}

	if _, err := BuildHealthyProfile(runs); err == nil {
		t.Fatal("healthy profile must reject an eligible span whose event ID is reused by an unkeyed application span")
	}
}
