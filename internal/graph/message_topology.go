package graph

import (
	"fmt"
	"sort"

	"github.com/yjr28/kestrel-replay/internal/model"
)

type messageFlowKey struct {
	topic   string
	action  string
	service string
}

// MessageFlowBaseline is the empirical count envelope for one application
// message-flow key across healthy runs. It is descriptive, not probabilistic.
type MessageFlowBaseline struct {
	Topic       string `json:"topic"`
	Action      string `json:"action"`
	Service     string `json:"service"`
	SampleCount int    `json:"sample_count"`
	MedianCount int    `json:"median_count"`
	MinCount    int    `json:"min_count"`
	MaxCount    int    `json:"max_count"`
	Stable      bool   `json:"stable"`
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
	Topic           string   `json:"topic"`
	Action          string   `json:"action"`
	Service         string   `json:"service"`
	Reason          string   `json:"reason"`
	HealthyMedian   int      `json:"healthy_median_count"`
	HealthyMin      int      `json:"healthy_min_count"`
	HealthyMax      int      `json:"healthy_max_count"`
	FailingCount    int      `json:"failing_count"`
	CountDelta      int      `json:"count_delta"`
	FailingEventIDs []string `json:"failing_event_ids,omitempty"`
}

func BuildMessageTopologyProfile(runs [][]model.Event) (MessageTopologyProfile, error) {
	if len(runs) < 2 {
		return MessageTopologyProfile{}, fmt.Errorf("message topology profile requires at least two healthy runs")
	}
	perRun := make([]map[messageFlowKey]int, 0, len(runs))
	allKeys := make(map[messageFlowKey]struct{})
	for _, run := range runs {
		counts, _ := messageFlowCounts(run)
		perRun = append(perRun, counts)
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
		for _, counts := range perRun {
			values = append(values, counts[key])
		}
		sorted := append([]int(nil), values...)
		sort.Ints(sorted)
		baseline := MessageFlowBaseline{
			Topic: key.topic, Action: key.action, Service: key.service,
			SampleCount: len(runs), MedianCount: medianInt(sorted),
			MinCount: sorted[0], MaxCount: sorted[len(sorted)-1],
		}
		baseline.Stable = baseline.MinCount == baseline.MaxCount
		profile.baselines[key] = baseline
	}
	return profile, nil
}

func (p MessageTopologyProfile) Baselines() []MessageFlowBaseline {
	out := make([]MessageFlowBaseline, 0, len(p.baselines))
	for _, baseline := range p.baselines {
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

func messageFlowCounts(events []model.Event) (map[messageFlowKey]int, map[messageFlowKey][]string) {
	counts := make(map[messageFlowKey]int)
	ids := make(map[messageFlowKey][]string)
	for _, event := range model.Sorted(events) {
		if event.Source != model.SourceApplication || event.Kind != model.KindMessage {
			continue
		}
		topic := event.Attributes["topic"]
		action := event.Attributes["message.action"]
		if topic == "" || (action != "publish" && action != "consume") {
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
