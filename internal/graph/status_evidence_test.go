package graph

import (
	"testing"
	"time"

	"github.com/yjr28/kestrel-replay/internal/model"
)

func TestRankDivergencesRequiresNonblankStatusEvidence(t *testing.T) {
	now := time.Now().UTC()
	for _, healthyStatus := range []string{"", "   "} {
		healthy := []model.Event{{
			ID: "h", Source: model.SourceApplication, Kind: model.KindSpan,
			Service: "inventory", Operation: "check", Timestamp: now, Status: healthyStatus,
		}}
		failing := []model.Event{{
			ID: "f", Source: model.SourceApplication, Kind: model.KindSpan,
			Service: "inventory", Operation: "check", Timestamp: now, Status: "error",
		}}

		if got := RankDivergences(healthy, failing, time.Second, "inventory"); len(got) != 0 {
			t.Fatalf("blank healthy status %q produced status evidence: %#v", healthyStatus, got)
		}
	}

	for _, failingStatus := range []string{"", "   "} {
		healthy := []model.Event{{
			ID: "h", Source: model.SourceApplication, Kind: model.KindSpan,
			Service: "inventory", Operation: "check", Timestamp: now, Status: "ok",
		}}
		failing := []model.Event{{
			ID: "f", Source: model.SourceApplication, Kind: model.KindSpan,
			Service: "inventory", Operation: "check", Timestamp: now, Status: failingStatus,
		}}

		if got := RankDivergences(healthy, failing, time.Second, "inventory"); len(got) != 0 {
			t.Fatalf("blank failing status %q produced status evidence: %#v", failingStatus, got)
		}
	}
}

func TestRankDivergencesNormalizesStatusWhitespace(t *testing.T) {
	now := time.Now().UTC()
	healthy := []model.Event{{
		ID: "h", Source: model.SourceApplication, Kind: model.KindSpan,
		Service: "inventory", Operation: "check", Timestamp: now, Status: "  ok\t",
	}}
	failing := []model.Event{{
		ID: "f", Source: model.SourceApplication, Kind: model.KindSpan,
		Service: "inventory", Operation: "check", Timestamp: now, Status: "ok",
	}}

	if got := RankDivergences(healthy, failing, time.Second, "inventory"); len(got) != 0 {
		t.Fatalf("status whitespace manufactured divergence evidence: %#v", got)
	}
}
