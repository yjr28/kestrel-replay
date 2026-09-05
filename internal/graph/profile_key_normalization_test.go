package graph

import (
	"testing"
	"time"

	"github.com/yjr28/kestrel-replay/internal/model"
)

func TestHealthyProfileNormalizesSpanLocalizationKeys(t *testing.T) {
	now := time.Now().UTC()
	profile, err := BuildHealthyProfile([][]model.Event{
		{profileSpan("h1", " inventory ", " check\t", "ok", 1000, now)},
		{profileSpan("h2", "inventory", "check", "ok", 1000, now.Add(time.Second))},
	})
	if err != nil {
		t.Fatal(err)
	}

	baselines := profile.Baselines()
	if len(baselines) != 1 {
		t.Fatalf("expected one normalized baseline, got %#v", baselines)
	}
	if baselines[0].Service != "inventory" || baselines[0].Operation != "check" {
		t.Fatalf("expected normalized localization key, got %+v", baselines[0])
	}

	failing := []model.Event{profileSpan("f", " inventory\t", " check ", "ok", 1000, now.Add(2*time.Second))}
	if candidates := RankDivergencesAgainstProfile(profile, failing, 20*time.Millisecond, " inventory "); len(candidates) != 0 {
		t.Fatalf("formatting-only key whitespace must not establish a divergence: %#v", candidates)
	}
}
