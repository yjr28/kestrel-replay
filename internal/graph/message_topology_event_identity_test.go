package graph

import (
	"testing"
	"time"

	"github.com/yjr28/kestrel-replay/internal/model"
)

func TestMessageTopologyRequiresEventIdentityForEvidence(t *testing.T) {
	for _, test := range []struct {
		name string
		id   string
	}{
		{name: "empty", id: ""},
		{name: "whitespace", id: " \t "},
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
			for i := range failing {
				if failing[i].Service == "analytics" && failing[i].Attributes["message.action"] == "consume" {
					failing[i].ID = test.id
					break
				}
			}

			divergences := CompareMessageTopology(profile, failing)
			if len(divergences) != 1 {
				t.Fatalf("unidentifiable event must not satisfy topology evidence: %#v", divergences)
			}
			divergence := divergences[0]
			if divergence.Service != "analytics" || divergence.Action != "consume" || divergence.Reason != "count_below_healthy_range" || divergence.FailingCount != 0 {
				t.Fatalf("unexpected divergence for unidentifiable event: %+v", divergence)
			}
			if len(divergence.FailingEventIDs) != 0 {
				t.Fatalf("missing-flow evidence must not contain an invalid event identity: %+v", divergence)
			}
		})
	}
}

func TestMessageTopologyExcludesDuplicateEventIdentityFromEvidence(t *testing.T) {
	now := time.Now().UTC()
	profile, err := BuildMessageTopologyProfile([][]model.Event{
		messageRun("h1", now, 1),
		messageRun("h2", now.Add(time.Second), 1),
	})
	if err != nil {
		t.Fatal(err)
	}

	failing := messageRun("f", now.Add(2*time.Second), 1)
	var duplicate model.Event
	for i := range failing {
		if failing[i].Service == "analytics" && failing[i].Attributes["message.action"] == "consume" {
			duplicate = failing[i]
			break
		}
	}
	duplicate.Timestamp = duplicate.Timestamp.Add(time.Millisecond)
	failing = append(failing, duplicate)

	divergences := CompareMessageTopology(profile, failing)
	if len(divergences) != 1 {
		t.Fatalf("ambiguous duplicate identity must not count as topology evidence: %#v", divergences)
	}
	divergence := divergences[0]
	if divergence.Service != "analytics" || divergence.Action != "consume" || divergence.Reason != "count_below_healthy_range" || divergence.FailingCount != 0 {
		t.Fatalf("unexpected divergence for duplicate event identity: %+v", divergence)
	}
	if len(divergence.FailingEventIDs) != 0 {
		t.Fatalf("ambiguous duplicate identity must not be reported as exact provenance: %+v", divergence)
	}
}
