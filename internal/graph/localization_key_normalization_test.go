package graph

import (
	"testing"
	"time"

	"github.com/yjr28/kestrel-replay/internal/model"
)

func TestRankDivergencesNormalizesSpanLocalizationKeys(t *testing.T) {
	now := time.Now().UTC()
	healthy := []model.Event{{
		ID: "h-inv", Source: model.SourceApplication, Kind: model.KindSpan,
		Service: "  inventory\t", Operation: " check ", Timestamp: now, Status: "ok",
	}}
	failing := []model.Event{{
		ID: "f-inv", Source: model.SourceApplication, Kind: model.KindSpan,
		Service: "inventory", Operation: "check", Timestamp: now, Status: "ok",
	}}

	if got := RankDivergences(healthy, failing, time.Second, "inventory"); len(got) != 0 {
		t.Fatalf("formatting-only localization key whitespace created divergence: %#v", got)
	}
}

func TestRankDivergencesUsesNormalizedKeyForTerminalAnchor(t *testing.T) {
	now := time.Now().UTC()
	healthy := []model.Event{{
		ID: "h-inv", Source: model.SourceApplication, Kind: model.KindSpan,
		Service: " inventory ", Operation: " check ", Timestamp: now, Status: "ok",
	}}
	failing := []model.Event{{
		ID: "f-inv", Source: model.SourceApplication, Kind: model.KindSpan,
		Service: "inventory", Operation: "check", Timestamp: now, Status: "error",
	}}

	got := RankDivergences(healthy, failing, time.Hour, " inventory ")
	if len(got) != 1 {
		t.Fatalf("expected one status divergence, got %#v", got)
	}
	if got[0].Service != "inventory" || got[0].Operation != "check" || got[0].Reason != "terminal_status_change" {
		t.Fatalf("unexpected normalized candidate: %+v", got[0])
	}
	if got[0].Anchor != "outcome.terminal_service=inventory" {
		t.Fatalf("unexpected normalized anchor: %+v", got[0])
	}
}

func TestTopKContainsNormalizesExpectedLocalizationKey(t *testing.T) {
	candidates := []LocalizationCandidate{{Divergence: Divergence{
		Service: "inventory", Operation: "check", Reason: "status_change",
	}}}

	if !TopKContains(candidates, "  inventory\t", " check ", 1) {
		t.Fatal("formatting-only whitespace in expected localization key must not change top-k membership")
	}
}
