package graph

import (
	"math"
	"testing"
	"time"
)

func TestScoreCandidateLatencySeverityDoesNotOverflowDurationAddition(t *testing.T) {
	candidate := LocalizationCandidate{Divergence: Divergence{
		Reason: "latency_delta",
		Delta:  time.Duration(math.MaxInt64),
	}}
	threshold := time.Duration(math.MaxInt64)

	scoreCandidate(&candidate, "", threshold)

	if candidate.ConfidenceScore != 0.73 {
		t.Fatalf("expected overflow-safe severity score 0.73, got %.3f", candidate.ConfidenceScore)
	}
	wantBasis := "latency_severity:+0.030"
	found := false
	for _, basis := range candidate.ScoreBasis {
		if basis == wantBasis {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected score basis %q, got %v", wantBasis, candidate.ScoreBasis)
	}
}
