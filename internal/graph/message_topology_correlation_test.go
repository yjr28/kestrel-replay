package graph

import (
	"testing"
	"time"

	"github.com/yjr28/kestrel-replay/internal/model"
)

func TestMessageTopologyIgnoresUncorrelatableMessageEvents(t *testing.T) {
	for _, test := range []struct {
		name      string
		attribute string
		value     string
	}{
		{name: "missing message id", attribute: "message.id", value: ""},
		{name: "blank message id", attribute: "message.id", value: " \t "},
		{name: "missing topic", attribute: "topic", value: ""},
		{name: "blank topic", attribute: "topic", value: " \t "},
	} {
		t.Run(test.name, func(t *testing.T) {
			now := time.Now().UTC()
			profile, err := BuildMessageTopologyProfile([][]model.Event{
				messageRun("h1", now, 1),
				messageRun("h2", now.Add(time.Second), 1),
			})
			if err != nil {
				t.Fatal(err)
			}

			failing := messageRun("f", now.Add(2*time.Second), 1)
			uncorrelated := messageEvent("f-search", "search-index", "consume", now.Add(2*time.Second))
			if test.value == "" {
				delete(uncorrelated.Attributes, test.attribute)
			} else {
				uncorrelated.Attributes[test.attribute] = test.value
			}
			failing = append(failing, uncorrelated)
			if divergences := CompareMessageTopology(profile, failing); len(divergences) != 0 {
				t.Fatalf("uncorrelatable message event must not establish a topology divergence: %#v", divergences)
			}

			withoutAnalytics := make([]model.Event, 0, len(failing))
			for _, event := range failing {
				if event.Service == "analytics" {
					continue
				}
				withoutAnalytics = append(withoutAnalytics, event)
			}
			uncorrelatedAnalytics := messageEvent("f-analytics-uncorrelated", "analytics", "consume", now.Add(2*time.Second))
			if test.value == "" {
				delete(uncorrelatedAnalytics.Attributes, test.attribute)
			} else {
				uncorrelatedAnalytics.Attributes[test.attribute] = test.value
			}
			withoutAnalytics = append(withoutAnalytics, uncorrelatedAnalytics)

			divergences := CompareMessageTopology(profile, withoutAnalytics)
			if len(divergences) != 1 {
				t.Fatalf("uncorrelatable replacement must not satisfy healthy analytics flow: %#v", divergences)
			}
			divergence := divergences[0]
			if divergence.Service != "analytics" || divergence.Action != "consume" || divergence.Reason != "count_below_healthy_range" || divergence.FailingCount != 0 || len(divergence.FailingEventIDs) != 0 {
				t.Fatalf("unexpected missing-flow evidence: %+v", divergence)
			}
		})
	}
}
