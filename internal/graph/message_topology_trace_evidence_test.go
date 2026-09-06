package graph

import (
	"testing"
	"time"

	"github.com/yjr28/kestrel-replay/internal/model"
)

func TestMessageTopologyAbstainsOnUntracedFailingFlowEvidence(t *testing.T) {
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
			failing[i].TraceID = "  "
			break
		}
	}

	if divergences := CompareMessageTopology(profile, failing); len(divergences) != 0 {
		t.Fatalf("untraced flow evidence must make the key uncertain rather than prove presence or absence: %#v", divergences)
	}
}

func TestMessageTopologyWithholdsBaselineWhenHealthyFlowEvidenceIsUntraced(t *testing.T) {
	now := time.Now().UTC()
	run1 := messageRun("h1", now, 1)
	for i := range run1 {
		if run1[i].Service == "analytics" && run1[i].Attributes["message.action"] == "consume" {
			run1[i].TraceID = ""
			break
		}
	}

	profile, err := BuildMessageTopologyProfile([][]model.Event{
		run1,
		messageRun("h2", now.Add(time.Second), 1),
	})
	if err != nil {
		t.Fatal(err)
	}

	for _, baseline := range profile.Baselines() {
		if baseline.Service == "analytics" && baseline.Action == "consume" && baseline.Topic == "orders.completed" {
			t.Fatalf("healthy baseline must be withheld when any run has untraced evidence for the flow: %+v", baseline)
		}
	}
}
