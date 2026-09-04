package graph

import (
	"testing"
	"time"

	"github.com/yjr28/kestrel-replay/internal/model"
)

func TestRankDivergencesAgainstProfileRejectsOverflowingMADThreshold(t *testing.T) {
	now := time.Now().UTC()
	const maxDurationUS int64 = 9223372036854775

	profile, err := BuildHealthyProfile([][]model.Event{
		{profileSpan("h-low", "inventory", "check", "ok", 0, now)},
		{profileSpan("h-high", "inventory", "check", "ok", maxDurationUS, now.Add(time.Second))},
	})
	if err != nil {
		t.Fatal(err)
	}
	baseline := profile.Baselines()[0]
	if baseline.MedianAbsDeviation <= time.Duration((1<<63-1)/6) {
		t.Fatalf("test setup must exercise 6*MAD overflow, got MAD=%s", baseline.MedianAbsDeviation)
	}

	failing := []model.Event{profileSpan("f", "inventory", "check", "ok", 1, now.Add(2*time.Second))}
	if candidates := RankDivergencesAgainstProfile(profile, failing, time.Nanosecond, ""); len(candidates) != 0 {
		t.Fatalf("overflowing adaptive threshold must make latency evidence unavailable, got %#v", candidates)
	}
}

func TestMultiplyDurationRejectsOverflow(t *testing.T) {
	boundary := time.Duration((1 << 63) - 1)
	if _, ok := multiplyDuration(boundary, 6); ok {
		t.Fatal("overflowing duration product must be rejected")
	}
	if got, ok := multiplyDuration(time.Second, 6); !ok || got != 6*time.Second {
		t.Fatalf("ordinary duration product should remain available: got %s ok=%v", got, ok)
	}
}
