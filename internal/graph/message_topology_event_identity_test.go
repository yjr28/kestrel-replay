package graph

import (
	"testing"
	"time"

	"github.com/yjr28/kestrel-replay/internal/model"
)

func TestMessageTopologyRequiresEventIdentityForEvidence(t *testing.T) {
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
			failing[i].ID = ""
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
		t.Fatalf("missing-flow evidence must not contain an empty event identity: %+v", divergence)
	}
}
