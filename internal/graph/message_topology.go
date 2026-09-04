package graph

import (
	"fmt"
	"sort"
	"strings"

	"github.com/yjr28/kestrel-replay/internal/model"
)

type messageFlowKey struct {
	topic   string
	action  string
	service string
}

// MessageFlowRunEvidence records the exact application events observed for one
// message-flow key in a healthy run. A zero Count with no EventIDs is explicit
// evidence that the flow was absent from that run.
type MessageFlowRunEvidence struct {
	RunIndex int      `json:"run_index"`
	Count    int      `json:"count"`
	EventIDs []string `json:"event_ids,omitempty"`
}

// MessageFlowBaseline is the empirical count envelope for one application
// message-flow key across healthy runs. It is descriptive, not probabilistic.
type MessageFlowBaseline struct {
	Topic       string                   `json:"topic"`
	Action      string                   `json:"action"`
	Service     string                   `json:"service"`
	SampleCount int                      `json:"sample_count"`
	MedianCount int                      `json:"median_count"`
	MinCount    int                      `json:"min_count"`
	MaxCount    int                      `json:"max_count"`
	Stable      bool                     `json:"stable"`
	HealthyRuns []MessageFlowRunEvidence `json:"healthy_runs"`
}

// MessageTopologyProfile describes observed publish/consume multiplicity across
// healthy runs. Missing events in a run contribute a count of zero so optional
// flows remain represented by a wider healthy envelope instead of disappearing.
type MessageTopologyProfile struct {
	RunCount  int
	baselines map[messageFlowKey]MessageFlowBaseline
}

// MessageTopologyDivergence reports a count outside the empirical healthy
// envelope. It identifies the observable flow whose multiplicity changed; it
// does not by itself claim which infrastructure component caused the change.
type MessageTopologyDivergence struct {
	Topic           string                   `json:"topic"`
	Action          string                   `json:"action"`
	Service         string                   `json:"service"`
	Reason          string                   `json:"reason"`
	HealthyMedian   int                      `json:"healthy_median_count"`
	HealthyMin      int                      `json:"healthy_min_count"`
	HealthyMax      int                      `json:"healthy_max_count"`
	FailingCount    int                      `json:"failing_count"`
	CountDelta      int                      `json:"count_delta"`
	HealthyRuns     []MessageFlowRunEvidence `json:"healthy_runs,omitempty"`
	FailingEventIDs []string                 `json:"failing_event_ids,omitempty"`
}

func BuildMessageTopologyProfile(runs [][]model.Event) (MessageTopologyProfile, error) {
	if len(runs) < 2 {
		return MessageTopologyProfile{}, fmt.Errorf("message topology profile requires at least two healthy runs")
	}
	perRunCounts := make([]map[messageFlowKey]int, 0, len(runs))
	perRunEventIDs := make([]map[messageFlowKey][]string, 0, len(runs))
	allKeys := make(map[messageFlowKey]struct{})
	for _, run := range runs {
		counts, eventIDs := messageFlowCounts(run)
		perRunCounts = append(perRunCounts, counts)
		perRunEventIDs = append(perRunEventIDs, eventIDs)
		for key := range counts {
			allKeys[key] = struct{}{}
		}
	}
	if len(allKeys) == 0 {
		return MessageTopologyProfile{}, fmt.Errorf("healthy runs contain no application message flows")
	}

	profile := MessageTopologyProfile{RunCount: len(runs), baselines: make(map[messageFlowKey]MessageFlowBaseline, len(allKeys))}
	for key := range allKeys {
		values := make([]int, 0, len(runs))
		healthyRuns := make([]MessageFlowRunEvidence, 0, len(runs))
		for runIndex, counts := range perRunCounts {
			count := counts[key]
			values = append(values, count)
			healthyRuns = append(healthyRuns, MessageFlowRunEvidence{
				RunIndex: runIndex,
				Count:    count,
				EventIDs: append([]string(nil), perRunEventIDs[runIndex][key]...),
			})
		}
		sorted := append([]int(nil), values...)
		sort.Ints(sorted)
		baseline := MessageFlowBaseline{
			Topic: key.topic, Action: key.action, Service: key.service,
			SampleCount: len(runs), MedianCount: medianInt(sorted),
			MinCount: sorted[0], MaxCount: sorted[len(sorted)-1],
			HealthyRuns: healthyRuns,
		}
		baseline.Stable = baseline.MinCount == baseline.MaxCount
		profile.baselines[key] = baseline
	}
	return profile, nil
}

func (p MessageTopologyProfile) Baselines() []MessageFlowBaseline {
	out := make([]MessageFlowBaseline, 0, len(p.baselines))
	for _, baseline := range p.baselines {
		baseline.HealthyRuns = cloneMessageFlowRunEvidence(baseline.HealthyRuns)
		out = append(out, baseline)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Topic != out[j].Topic {
			return out[i].Topic < out[j].Topic
		}
		if out[i].Action != out[j].Action {
			return out[i].Action < out[j].Action
		}
		return out[i].Service < out[j].Service
	})
	return out
}

func CompareMessageTopology(profile MessageTopologyProfile, failing []model.Event) []MessageTopologyDivergence {
	counts, eventIDs := messageFlowCounts(failing)
	var divergences []MessageTopologyDivergence
	for key, baseline := range profile.baselines {
		failingCount := counts[key]
		reason := ""
		if failingCount > baseline.MaxCount {
			reason = "count_above_healthy_range"
		} else if failingCount < baseline.MinCount {
			reason = "count_below_healthy_range"
		}
		if reason == "" {
			continue
		}
		divergences = append(divergences, MessageTopologyDivergence{
			Topic: key.topic, Action: key.action, Service: key.service, Reason: reason,
			HealthyMedian: baseline.MedianCount, HealthyMin: baseline.MinCount, HealthyMax: baseline.MaxCount,
			FailingCount: failingCount, CountDelta: failingCount - baseline.MedianCount,
			HealthyRuns: cloneMessageFlowRunEvidence(baseline.HealthyRuns),
			FailingEventIDs: append([]string(nil), eventIDs[key]...),
		})
	}
	for key, failingCount := range counts {
		if _, known := profile.baselines[key]; known {
			continue
		}
		divergences = append(divergences, MessageTopologyDivergence{
			Topic: key.topic, Action: key.action, Service: key.service, Reason: "unexpected_message_flow",
			FailingCount: failingCount, CountDelta: failingCount,
			HealthyRuns: absentMessageFlowRunEvidence(profile.RunCount),
			FailingEventIDs: append([]string(nil), eventIDs[key]...),
		})
	}
	sort.SliceStable(divergences, func(i, j int) bool {
		ai := absInt(divergences[i].CountDelta)
		aj := absInt(divergences[j].CountDelta)
		if ai != aj {
			return ai > aj
		}
		if divergences[i].Topic != divergences[j].Topic {
			return divergences[i].Topic < divergences[j].Topic
		}
		if divergences[i].Action != divergences[j].Action {
			return divergences[i].Action < divergences[j].Action
		}
		return divergences[i].Service < divergences[j].Service
	})
	return divergences
}

func absentMessageFlowRunEvidence(runCount int) []MessageFlowRunEvidence {
	runs := make([]MessageFlowRunEvidence, runCount)
	for runIndex := range runs {
		runs[runIndex] = MessageFlowRunEvidence{RunIndex: runIndex, Count: 0}
	}
	return runs
}

func cloneMessageFlowRunEvidence(in []MessageFlowRunEvidence) []MessageFlowRunEvidence {
	out := make([]MessageFlowRunEvidence, len(in))
	for i, evidence := range in {
		out[i] = evidence
		out[i].EventIDs = append([]string(nil), evidence.EventIDs...)
	}
	return out
}

func messageFlowCounts(events []model.Event) (map[messageFlowKey]int, map[messageFlowKey][]string) {
	counts := make(map[messageFlowKey]int)
	ids := make(map[messageFlowKey][]string)
	sortedEvents := model.Sorted(events)
	eventIDCounts := make(map[string]int)
	for _, event := range sortedEvents {
		if event.Source == model.SourceApplication && event.Kind == model.KindMessage && strings.TrimSpace(event.ID) != "" {
			eventIDCounts[event.ID]++
		}
	}
	for _, event := range sortedEvents {
		if event.Source != model.SourceApplication || event.Kind != model.KindMessage || strings.TrimSpace(event.ID) == "" || eventIDCounts[event.ID] != 1 {
			continue
		}
		topic := event.Attributes["topic"]
		messageID := event.Attributes["message.id"]
		action := event.Attributes["message.action"]
		if topic == "" || messageID == "" || (action != "publish" && action != "consume") {
			continue
		}
		key := messageFlowKey{topic: topic, action: action, service: event.Service}
		counts[key]++
		ids[key] = append(ids[key], event.ID)
	}
	return counts, ids
}

func medianInt(sorted []int) int {
	if len(sorted) == 0 {
		return 0
	}
	mid := len(sorted) / 2
	if len(sorted)%2 == 1 {
		return sorted[mid]
	}
	return sorted[mid-1] + (sorted[mid]-sorted[mid-1])/2
}

func absInt(value int) int {
	if value < 0 {
		return -value
	}
	return value
}
