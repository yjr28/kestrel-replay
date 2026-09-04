package graph

import (
	"testing"
	"time"

	"github.com/yjr28/kestrel-replay/internal/model"
)

func TestProfileRankingNormalizesTerminalServiceAnchor(t *testing.T) {
	now := time.Now().UTC()
	profile, err := BuildHealthyProfile([][]model.Event{
		{profileSpan("h1-order", "order", "create", "ok", 1000, now), profileSpan("h1-inventory", "inventory", "check", "ok", 1000, now)},
		{profileSpan("h2-order", "order", "create", "ok", 1000, now), profileSpan("h2-inventory", "inventory", "check", "ok", 1000, now)},
	})
	if err != nil {
		t.Fatal(err)
	}
	failing := []model.Event{profileSpan("f-order", "order", "create", "error", 1000, now.Add(time.Second))}
	candidates := RankDivergencesAgainstProfile(profile, failing, 20*time.Millisecond, "  inventory  ")
	if len(candidates) < 2 || candidates[0].Service != "inventory" || candidates[0].Reason != "missing_span" {
		t.Fatalf("trimmed terminal anchor should rank the missing terminal span first: %#v", candidates)
	}
	if candidates[0].Anchor != "outcome.terminal_service=inventory" {
		t.Fatalf("terminal anchor should contain the normalized service name: %+v", candidates[0])
	}
}
