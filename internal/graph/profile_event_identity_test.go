package graph

import (
	"strings"
	"testing"
	"time"

	"github.com/yjr28/kestrel-replay/internal/model"
)

func TestHealthyProfileRequiresIdentifiedEvidenceInEveryRun(t *testing.T) {
	now := time.Now().UTC()
	_, err := BuildHealthyProfile([][]model.Event{
		{profileSpan("h1", "inventory", "check", "ok", 1000, now)},
		{profileSpan("", "inventory", "check", "ok", 1000, now.Add(time.Second))},
	})
	if err == nil || !strings.Contains(err.Error(), "no service/operation present in every run") {
		t.Fatalf("unidentified span must not satisfy a stable baseline: %v", err)
	}
}

func TestHealthyProfileDoesNotTreatUnidentifiedSpanAsObservedTopology(t *testing.T) {
	now := time.Now().UTC()
	profile, err := BuildHealthyProfile([][]model.Event{
		{
			profileSpan("h1-core", "order", "create", "ok", 1000, now),
			profileSpan("", "cache", "lookup", "ok", 100, now.Add(time.Millisecond)),
		},
		{
			profileSpan("h2-core", "order", "create", "ok", 1000, now.Add(time.Second)),
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	failing := []model.Event{
		profileSpan("f-core", "order", "create", "ok", 1000, now.Add(2*time.Second)),
		profileSpan("f-cache", "cache", "lookup", "ok", 100, now.Add(2*time.Second+time.Millisecond)),
	}
	candidates := RankDivergencesAgainstProfile(profile, failing, 20*time.Millisecond, "")
	if len(candidates) != 1 || candidates[0].Reason != "unexpected_span" || candidates[0].Service != "cache" || candidates[0].FailingEventID != "f-cache" {
		t.Fatalf("unidentified healthy span must not suppress identified failing-only evidence: %#v", candidates)
	}
}

func TestHealthyProfileIgnoresUnidentifiedDuplicateOfEligibleSpan(t *testing.T) {
	now := time.Now().UTC()
	profile, err := BuildHealthyProfile([][]model.Event{
		{
			profileSpan("h1", "inventory", "check", "ok", 1000, now),
			profileSpan("", "inventory", "check", "error", 9000, now.Add(time.Millisecond)),
		},
		{profileSpan("h2", "inventory", "check", "ok", 1000, now.Add(time.Second))},
	})
	if err != nil {
		t.Fatalf("unidentified duplicate is ineligible evidence and must not create ambiguity: %v", err)
	}
	baselines := profile.Baselines()
	if len(baselines) != 1 || baselines[0].RepresentativeEventID == "" || !baselines[0].StatusStable || baselines[0].Status != "ok" {
		t.Fatalf("unexpected identified baseline: %#v", baselines)
	}
}
