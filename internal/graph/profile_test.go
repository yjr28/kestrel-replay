package graph

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/yjr28/kestrel-replay/internal/model"
)

func TestBuildHealthyProfileMedianMADAndLatencyRanking(t *testing.T) {
	now := time.Now().UTC()
	runs := [][]model.Event{
		{profileSpan("h1-inv", "inventory", "check", "ok", 800, now)},
		{profileSpan("h2-inv", "inventory", "check", "ok", 1000, now.Add(time.Second))},
		{profileSpan("h3-inv", "inventory", "check", "ok", 1200, now.Add(2*time.Second))},
	}
	profile, err := BuildHealthyProfile(runs)
	if err != nil {
		t.Fatal(err)
	}
	if profile.RunCount != 3 {
		t.Fatalf("unexpected run count %d", profile.RunCount)
	}
	baselines := profile.Baselines()
	if len(baselines) != 1 {
		t.Fatalf("expected one baseline, got %#v", baselines)
	}
	baseline := baselines[0]
	if baseline.MedianDuration != time.Millisecond || baseline.MedianAbsDeviation != 200*time.Microsecond {
		t.Fatalf("unexpected robust stats: %+v", baseline)
	}
	if baseline.RepresentativeEventID != "h2-inv" || baseline.DurationSampleCount != 3 || !baseline.StatusStable {
		t.Fatalf("unexpected baseline evidence: %+v", baseline)
	}

	failing := []model.Event{profileSpan("f-inv", "inventory", "check", "ok", 75000, now.Add(3*time.Second))}
	candidates := RankDivergencesAgainstProfile(profile, failing, 20*time.Millisecond, "inventory")
	if len(candidates) == 0 || candidates[0].Service != "inventory" || candidates[0].Operation != "check" || candidates[0].Reason != "latency_delta" {
		t.Fatalf("unexpected ranking: %#v", candidates)
	}
	if !strings.Contains(candidates[0].HealthyValue, "median=1ms") || !containsBasis(candidates[0].ScoreBasis, "healthy_profile_runs=3") || !containsBasis(candidates[0].ScoreBasis, "latency_threshold=20ms") {
		t.Fatalf("missing distribution provenance: %+v", candidates[0])
	}
}

func TestHealthyProfileIgnoresNormalJitterBelowMinimumThreshold(t *testing.T) {
	now := time.Now().UTC()
	profile, err := BuildHealthyProfile([][]model.Event{
		{profileSpan("h1", "inventory", "check", "ok", 900, now)},
		{profileSpan("h2", "inventory", "check", "ok", 1000, now.Add(time.Second))},
		{profileSpan("h3", "inventory", "check", "ok", 1100, now.Add(2*time.Second))},
	})
	if err != nil {
		t.Fatal(err)
	}
	failing := []model.Event{profileSpan("f", "inventory", "check", "ok", 15000, now.Add(3*time.Second))}
	candidates := RankDivergencesAgainstProfile(profile, failing, 20*time.Millisecond, "inventory")
	if len(candidates) != 0 {
		t.Fatalf("15ms healthy-relative jitter should not cross 20ms floor: %#v", candidates)
	}
}

func TestHealthyProfileMADCanRaiseLatencyThreshold(t *testing.T) {
	now := time.Now().UTC()
	profile, err := BuildHealthyProfile([][]model.Event{
		{profileSpan("h1", "inventory", "check", "ok", 1000, now)},
		{profileSpan("h2", "inventory", "check", "ok", 6000, now.Add(time.Second))},
		{profileSpan("h3", "inventory", "check", "ok", 11000, now.Add(2*time.Second))},
	})
	if err != nil {
		t.Fatal(err)
	}
	// median=6ms, MAD=5ms, so 6*MAD=30ms exceeds the configured 20ms floor.
	belowAdaptive := []model.Event{profileSpan("f1", "inventory", "check", "ok", 35000, now.Add(3*time.Second))}
	if candidates := RankDivergencesAgainstProfile(profile, belowAdaptive, 20*time.Millisecond, "inventory"); len(candidates) != 0 {
		t.Fatalf("29ms delta should remain below adaptive 30ms threshold: %#v", candidates)
	}
	aboveAdaptive := []model.Event{profileSpan("f2", "inventory", "check", "ok", 37000, now.Add(4*time.Second))}
	candidates := RankDivergencesAgainstProfile(profile, aboveAdaptive, 20*time.Millisecond, "inventory")
	if len(candidates) == 0 || !containsBasis(candidates[0].ScoreBasis, "latency_threshold=30ms") {
		t.Fatalf("expected adaptive 30ms threshold evidence: %#v", candidates)
	}
}

func TestHealthyProfileCrashRanksMissingTerminalSpan(t *testing.T) {
	now := time.Now().UTC()
	profile, err := BuildHealthyProfile([][]model.Event{
		{profileSpan("h1-order", "order", "create", "ok", 2000, now), profileSpan("h1-inv", "inventory", "check", "ok", 1000, now)},
		{profileSpan("h2-order", "order", "create", "ok", 2100, now), profileSpan("h2-inv", "inventory", "check", "ok", 1100, now)},
		{profileSpan("h3-order", "order", "create", "ok", 1900, now), profileSpan("h3-inv", "inventory", "check", "ok", 900, now)},
	})
	if err != nil {
		t.Fatal(err)
	}
	failing := []model.Event{profileSpan("f-order", "order", "create", "error", 3000, now.Add(time.Second))}
	candidates := RankDivergencesAgainstProfile(profile, failing, 20*time.Millisecond, "inventory")
	if len(candidates) < 2 || candidates[0].Service != "inventory" || candidates[0].Operation != "check" || candidates[0].Reason != "missing_span" {
		t.Fatalf("missing terminal span should rank first: %#v", candidates)
	}
	if candidates[0].Anchor != "outcome.terminal_service=inventory" {
		t.Fatalf("missing outcome anchor: %+v", candidates[0])
	}
}

func TestHealthyProfileRejectsDuplicateServiceOperationInRun(t *testing.T) {
	now := time.Now().UTC()
	_, err := BuildHealthyProfile([][]model.Event{
		{profileSpan("a", "inventory", "check", "ok", 1000, now), profileSpan("b", "inventory", "check", "ok", 1100, now)},
		{profileSpan("c", "inventory", "check", "ok", 1000, now)},
	})
	if err == nil || !strings.Contains(err.Error(), "duplicate application span") {
		t.Fatalf("expected duplicate-span rejection, got %v", err)
	}
}

func TestHealthyProfileRejectsReusedEventIdentityInRun(t *testing.T) {
	now := time.Now().UTC()
	_, err := BuildHealthyProfile([][]model.Event{
		{profileSpan("shared", "order", "create", "ok", 1000, now), profileSpan("shared", "inventory", "check", "ok", 1100, now)},
		{profileSpan("h2-order", "order", "create", "ok", 1000, now), profileSpan("h2-inventory", "inventory", "check", "ok", 1100, now)},
	})
	if err == nil || !strings.Contains(err.Error(), "reuses application span event id") {
		t.Fatalf("expected reused-event-id rejection, got %v", err)
	}
}

func TestUnstableHealthyTopologyIsNotCalledUnexpected(t *testing.T) {
	now := time.Now().UTC()
	profile, err := BuildHealthyProfile([][]model.Event{
		{profileSpan("h1-core", "order", "create", "ok", 1000, now), profileSpan("h1-optional", "cache", "lookup", "ok", 100, now)},
		{profileSpan("h2-core", "order", "create", "ok", 1000, now)},
		{profileSpan("h3-core", "order", "create", "ok", 1000, now), profileSpan("h3-optional", "cache", "lookup", "ok", 100, now)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if baselines := profile.Baselines(); len(baselines) != 1 || baselines[0].Service != "order" {
		t.Fatalf("optional healthy key should not become stable baseline: %#v", baselines)
	}
	failing := []model.Event{
		profileSpan("f-core", "order", "create", "ok", 1000, now.Add(time.Second)),
		profileSpan("f-optional", "cache", "lookup", "ok", 100, now.Add(time.Second)),
	}
	if candidates := RankDivergencesAgainstProfile(profile, failing, 20*time.Millisecond, ""); len(candidates) != 0 {
		t.Fatalf("healthy-observed optional key must not be unexpected: %#v", candidates)
	}
}

func profileSpan(id, service, operation, status string, durationUS int64, at time.Time) model.Event {
	return model.Event{
		ID: id, Source: model.SourceApplication, Kind: model.KindSpan,
		TraceID: "trace-" + id, SpanID: "span-" + id,
		Service: service, Operation: operation, Timestamp: at, Status: status,
		Attributes: map[string]string{"duration_us": strconv.FormatInt(durationUS, 10)},
	}
}

func containsBasis(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
