package graph

import (
	"reflect"
	"testing"
	"time"

	"github.com/yjr28/kestrel-replay/internal/model"
)

func TestHealthyProfileRetainsEveryEligibleHealthyEventID(t *testing.T) {
	now := time.Now().UTC()
	profile, err := BuildHealthyProfile([][]model.Event{
		{profileSpan("h1-inv", "inventory", "check", "ok", 800, now)},
		{profileSpan("h2-inv", "inventory", "check", "ok", 1000, now.Add(time.Second))},
		{profileSpan("h3-inv", "inventory", "check", "ok", 1200, now.Add(2*time.Second))},
	})
	if err != nil {
		t.Fatal(err)
	}

	baselines := profile.Baselines()
	if len(baselines) != 1 {
		t.Fatalf("expected one baseline, got %#v", baselines)
	}
	want := []string{"h1-inv", "h2-inv", "h3-inv"}
	if !reflect.DeepEqual(baselines[0].HealthyEventIDs, want) {
		t.Fatalf("healthy provenance mismatch: got %v want %v", baselines[0].HealthyEventIDs, want)
	}
	if baselines[0].RepresentativeEventID != "h2-inv" {
		t.Fatalf("representative event must remain the median-nearest sample, got %q", baselines[0].RepresentativeEventID)
	}
}

func TestHealthyProfileBaselineProvenanceIsDefensivelyCopied(t *testing.T) {
	now := time.Now().UTC()
	profile, err := BuildHealthyProfile([][]model.Event{
		{profileSpan("h1", "inventory", "check", "ok", 1000, now)},
		{profileSpan("h2", "inventory", "check", "ok", 1100, now.Add(time.Second))},
	})
	if err != nil {
		t.Fatal(err)
	}

	first := profile.Baselines()
	first[0].HealthyEventIDs[0] = "mutated"
	second := profile.Baselines()
	if second[0].HealthyEventIDs[0] != "h1" {
		t.Fatalf("caller mutation leaked into profile: %#v", second[0].HealthyEventIDs)
	}
}
