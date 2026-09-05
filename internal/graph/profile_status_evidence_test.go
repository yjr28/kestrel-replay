package graph

import (
	"testing"
	"time"

	"github.com/yjr28/kestrel-replay/internal/model"
)

func TestHealthyProfileRequiresNonblankStatusEvidence(t *testing.T) {
	now := time.Now().UTC()
	runs := [][]model.Event{
		{{ID: "h1", Source: model.SourceApplication, Kind: model.KindSpan, Service: "inventory", Operation: "check", Timestamp: now, Status: ""}},
		{{ID: "h2", Source: model.SourceApplication, Kind: model.KindSpan, Service: "inventory", Operation: "check", Timestamp: now.Add(time.Second), Status: "   "}},
	}

	profile, err := BuildHealthyProfile(runs)
	if err != nil {
		t.Fatalf("build profile: %v", err)
	}
	baselines := profile.Baselines()
	if len(baselines) != 1 {
		t.Fatalf("expected one baseline, got %#v", baselines)
	}
	if baselines[0].StatusStable || baselines[0].Status != "" {
		t.Fatalf("blank healthy statuses became stable evidence: %+v", baselines[0])
	}

	failing := []model.Event{{ID: "f", Source: model.SourceApplication, Kind: model.KindSpan, Service: "inventory", Operation: "check", Timestamp: now, Status: "error"}}
	if got := RankDivergencesAgainstProfile(profile, failing, time.Second, "inventory"); len(got) != 0 {
		t.Fatalf("blank healthy profile status produced candidate: %#v", got)
	}
}

func TestProfileRankingRequiresNonblankFailingStatusEvidence(t *testing.T) {
	now := time.Now().UTC()
	runs := [][]model.Event{
		{{ID: "h1", Source: model.SourceApplication, Kind: model.KindSpan, Service: "inventory", Operation: "check", Timestamp: now, Status: "ok"}},
		{{ID: "h2", Source: model.SourceApplication, Kind: model.KindSpan, Service: "inventory", Operation: "check", Timestamp: now.Add(time.Second), Status: "ok"}},
	}

	profile, err := BuildHealthyProfile(runs)
	if err != nil {
		t.Fatalf("build profile: %v", err)
	}
	for _, status := range []string{"", "   "} {
		failing := []model.Event{{ID: "f", Source: model.SourceApplication, Kind: model.KindSpan, Service: "inventory", Operation: "check", Timestamp: now, Status: status}}
		if got := RankDivergencesAgainstProfile(profile, failing, time.Second, "inventory"); len(got) != 0 {
			t.Fatalf("blank failing status %q produced candidate: %#v", status, got)
		}
	}
}
