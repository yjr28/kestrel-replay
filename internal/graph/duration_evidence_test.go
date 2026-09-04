package graph

import (
	"testing"
	"time"

	"github.com/yjr28/kestrel-replay/internal/model"
)

func TestDurationOfRejectsMalformedLatencyEvidence(t *testing.T) {
	for _, raw := range []string{"-1", "9223372036854775807"} {
		event := model.Event{Attributes: map[string]string{"duration_us": raw}}
		if duration, ok := durationOf(event); ok {
			t.Fatalf("duration_us=%q must be ineligible latency evidence, got %s", raw, duration)
		}
	}

	boundary := model.Event{Attributes: map[string]string{"duration_us": "9223372036854775"}}
	if _, ok := durationOf(boundary); !ok {
		t.Fatal("largest whole-microsecond value representable by time.Duration should remain eligible")
	}
}

func TestRankDivergencesIgnoresMalformedFailingDuration(t *testing.T) {
	now := time.Now().UTC()
	healthy := profileSpan("h", "inventory", "check", "ok", 1000, now)
	failing := profileSpan("f", "inventory", "check", "ok", 1000, now.Add(time.Second))
	failing.Attributes["duration_us"] = "-5000"
	if candidates := RankDivergences([]model.Event{healthy}, []model.Event{failing}, time.Millisecond, ""); len(candidates) != 0 {
		t.Fatalf("malformed duration must not create latency evidence: %#v", candidates)
	}
}
