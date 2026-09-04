package graph

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/yjr28/kestrel-replay/internal/model"
)

// SpanBaseline summarizes a service/operation observed in every healthy run.
// Median/MAD are robust descriptive statistics; they are not a probabilistic
// model of latency.
type SpanBaseline struct {
	Service               string        `json:"service"`
	Operation             string        `json:"operation"`
	SampleCount           int           `json:"sample_count"`
	Status                string        `json:"status,omitempty"`
	StatusStable          bool          `json:"status_stable"`
	DurationSampleCount   int           `json:"duration_sample_count"`
	MedianDuration        time.Duration `json:"median_duration,omitempty"`
	MedianAbsDeviation    time.Duration `json:"median_abs_deviation,omitempty"`
	RepresentativeEventID string        `json:"representative_event_id,omitempty"`
	HealthyEventIDs       []string      `json:"healthy_event_ids,omitempty"`
}

// HealthyProfile contains only topology keys present in every supplied healthy
// run, while separately remembering every key observed at least once so an
// unstable healthy key is not mislabeled as an unexpected failing span.
type HealthyProfile struct {
	RunCount  int
	baselines map[divergenceKey]SpanBaseline
	observed  map[divergenceKey]struct{}
}

type healthySpanSample struct {
	event    model.Event
	duration time.Duration
	hasDur   bool
}

// BuildHealthyProfile builds a deterministic profile from at least two healthy
// executions. Application spans without event identity are ineligible evidence.
// Duplicate eligible application spans for the same service/operation in one
// run are rejected because the current v1 profile does not model retries yet.
func BuildHealthyProfile(runs [][]model.Event) (HealthyProfile, error) {
	if len(runs) < 2 {
		return HealthyProfile{}, fmt.Errorf("healthy profile requires at least two runs")
	}
	acc := make(map[divergenceKey][]healthySpanSample)
	observed := make(map[divergenceKey]struct{})
	for runIndex, run := range runs {
		seen := make(map[divergenceKey]struct{})
		for _, event := range model.Sorted(run) {
			if event.Kind != model.KindSpan || event.Source != model.SourceApplication || strings.TrimSpace(event.ID) == "" {
				continue
			}
			key := divergenceKey{service: event.Service, operation: event.Operation}
			if _, ok := seen[key]; ok {
				return HealthyProfile{}, fmt.Errorf("healthy run %d contains duplicate application span %s/%s", runIndex, key.service, key.operation)
			}
			seen[key] = struct{}{}
			observed[key] = struct{}{}
			duration, hasDur := durationOf(event)
			acc[key] = append(acc[key], healthySpanSample{event: event, duration: duration, hasDur: hasDur})
		}
	}

	profile := HealthyProfile{RunCount: len(runs), baselines: make(map[divergenceKey]SpanBaseline), observed: observed}
	for key, samples := range acc {
		if len(samples) != len(runs) {
			continue
		}
		baseline := SpanBaseline{Service: key.service, Operation: key.operation, SampleCount: len(samples)}
		baseline.HealthyEventIDs = make([]string, 0, len(samples))
		for _, sample := range samples {
			baseline.HealthyEventIDs = append(baseline.HealthyEventIDs, sample.event.ID)
		}
		baseline.Status = samples[0].event.Status
		baseline.StatusStable = true
		for _, sample := range samples[1:] {
			if sample.event.Status != baseline.Status {
				baseline.StatusStable = false
				baseline.Status = ""
				break
			}
		}

		durations := make([]time.Duration, 0, len(samples))
		for _, sample := range samples {
			if sample.hasDur {
				durations = append(durations, sample.duration)
			}
		}
		baseline.DurationSampleCount = len(durations)
		if len(durations) == len(samples) {
			baseline.MedianDuration = medianDuration(durations)
			deviations := make([]time.Duration, 0, len(durations))
			for _, duration := range durations {
				delta := duration - baseline.MedianDuration
				if delta < 0 {
					delta = -delta
				}
				deviations = append(deviations, delta)
			}
			baseline.MedianAbsDeviation = medianDuration(deviations)
			baseline.RepresentativeEventID = representativeEventID(samples, baseline.MedianDuration)
		} else {
			baseline.RepresentativeEventID = samples[0].event.ID
		}
		profile.baselines[key] = baseline
	}
	if len(profile.baselines) == 0 {
		return HealthyProfile{}, fmt.Errorf("healthy profile has no service/operation present in every run")
	}
	return profile, nil
}

// Baselines returns deterministic public profile summaries.
func (p HealthyProfile) Baselines() []SpanBaseline {
	out := make([]SpanBaseline, 0, len(p.baselines))
	for _, baseline := range p.baselines {
		baseline.HealthyEventIDs = append([]string(nil), baseline.HealthyEventIDs...)
		out = append(out, baseline)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Service == out[j].Service {
			return out[i].Operation < out[j].Operation
		}
		return out[i].Service < out[j].Service
	})
	return out
}

// RankDivergencesAgainstProfile ranks a failing run against a healthy-run
// distribution. Latency evidence uses an adaptive threshold of the configured
// minimum or six times the healthy MAD, whichever is larger.
func RankDivergencesAgainstProfile(profile HealthyProfile, failing []model.Event, minimumLatencyThreshold time.Duration, terminalService string) []LocalizationCandidate {
	failingSpans, failingAmbiguous := applicationSpanIndexWithAmbiguity(failing)
	keys := make([]divergenceKey, 0, len(profile.baselines))
	for key := range profile.baselines {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].service == keys[j].service {
			return keys[i].operation < keys[j].operation
		}
		return keys[i].service < keys[j].service
	})

	var candidates []LocalizationCandidate
	for _, key := range keys {
		if _, ambiguous := failingAmbiguous[key]; ambiguous {
			continue
		}
		baseline := profile.baselines[key]
		failingEvent, ok := failingSpans[key]
		if !ok {
			candidate := LocalizationCandidate{Divergence: Divergence{
				Service: key.service, Operation: key.operation, Reason: "missing_span",
				HealthyValue: baseline.Status, FailingValue: "missing", HealthyEventID: baseline.RepresentativeEventID,
			}}
			scoreCandidate(&candidate, terminalService, minimumLatencyThreshold)
			candidate.ScoreBasis = append(candidate.ScoreBasis, fmt.Sprintf("healthy_profile_runs=%d", profile.RunCount))
			candidates = append(candidates, candidate)
			continue
		}

		if baseline.StatusStable && baseline.Status != failingEvent.Status {
			reason := "status_change"
			if terminalService != "" && key.service == terminalService {
				reason = "terminal_status_change"
			}
			candidate := LocalizationCandidate{Divergence: Divergence{
				Service: key.service, Operation: key.operation, Reason: reason,
				HealthyValue: baseline.Status, FailingValue: failingEvent.Status,
				HealthyEventID: baseline.RepresentativeEventID, FailingEventID: failingEvent.ID,
			}}
			scoreCandidate(&candidate, terminalService, minimumLatencyThreshold)
			candidate.ScoreBasis = append(candidate.ScoreBasis, fmt.Sprintf("healthy_profile_runs=%d", profile.RunCount))
			candidates = append(candidates, candidate)
		}

		if baseline.DurationSampleCount == profile.RunCount {
			if failingDuration, ok := durationOf(failingEvent); ok {
				delta := failingDuration - baseline.MedianDuration
				if delta < 0 {
					delta = -delta
				}
				threshold := minimumLatencyThreshold
				madThreshold := 6 * baseline.MedianAbsDeviation
				if madThreshold > threshold {
					threshold = madThreshold
				}
				if threshold > 0 && delta >= threshold {
					candidate := LocalizationCandidate{Divergence: Divergence{
						Service: key.service, Operation: key.operation, Reason: "latency_delta",
						HealthyValue: fmt.Sprintf("median=%s mad=%s n=%d", baseline.MedianDuration, baseline.MedianAbsDeviation, baseline.DurationSampleCount),
						FailingValue: failingDuration.String(), Delta: delta,
						HealthyEventID: baseline.RepresentativeEventID, FailingEventID: failingEvent.ID,
					}}
					scoreCandidate(&candidate, terminalService, threshold)
					candidate.ScoreBasis = append(candidate.ScoreBasis,
						fmt.Sprintf("healthy_profile_runs=%d", profile.RunCount),
						fmt.Sprintf("latency_threshold=%s", threshold),
					)
					candidates = append(candidates, candidate)
				}
			}
		}
	}

	for key, event := range failingSpans {
		if _, known := profile.observed[key]; known {
			continue
		}
		candidate := LocalizationCandidate{Divergence: Divergence{
			Service: key.service, Operation: key.operation, Reason: "unexpected_span",
			FailingValue: event.Status, FailingEventID: event.ID,
		}}
		scoreCandidate(&candidate, terminalService, minimumLatencyThreshold)
		candidate.ScoreBasis = append(candidate.ScoreBasis, fmt.Sprintf("healthy_profile_runs=%d", profile.RunCount))
		candidates = append(candidates, candidate)
	}

	sort.SliceStable(candidates, func(i, j int) bool {
		if candidates[i].ConfidenceScore != candidates[j].ConfidenceScore {
			return candidates[i].ConfidenceScore > candidates[j].ConfidenceScore
		}
		if candidates[i].Service != candidates[j].Service {
			return candidates[i].Service < candidates[j].Service
		}
		if candidates[i].Operation != candidates[j].Operation {
			return candidates[i].Operation < candidates[j].Operation
		}
		return candidates[i].Reason < candidates[j].Reason
	})
	return candidates
}

func medianDuration(values []time.Duration) time.Duration {
	if len(values) == 0 {
		return 0
	}
	sorted := append([]time.Duration(nil), values...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	mid := len(sorted) / 2
	if len(sorted)%2 == 1 {
		return sorted[mid]
	}
	return sorted[mid-1] + (sorted[mid]-sorted[mid-1])/2
}

func representativeEventID(samples []healthySpanSample, median time.Duration) string {
	bestID := ""
	var bestDelta time.Duration
	for _, sample := range samples {
		if !sample.hasDur {
			continue
		}
		delta := sample.duration - median
		if delta < 0 {
			delta = -delta
		}
		if bestID == "" || delta < bestDelta || (delta == bestDelta && sample.event.ID < bestID) {
			bestID = sample.event.ID
			bestDelta = delta
		}
	}
	return bestID
}
