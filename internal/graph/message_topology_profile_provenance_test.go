package graph

import (
	"testing"
	"time"

	"github.com/yjr28/kestrel-replay/internal/model"
)

func TestMessageTopologyProfileWithholdsAmbiguousHealthyFlow(t *testing.T) {
	now := time.Now().UTC()
	healthy1 := messageRun("h1", now, 1)
	healthy2 := messageRun("h2", now.Add(time.Second), 1)
	var duplicate model.Event
	for i := range healthy2 {
		if healthy2[i].Service == "analytics" && healthy2[i].Attributes["message.action"] == "consume" {
			duplicate = healthy2[i]
			break
		}
	}
	duplicate.Timestamp = duplicate.Timestamp.Add(time.Millisecond)
	healthy2 = append(healthy2, duplicate)

	profile, err := BuildMessageTopologyProfile([][]model.Event{healthy1, healthy2})
	if err != nil {
		t.Fatal(err)
	}
	for _, baseline := range profile.Baselines() {
		if baseline.Service == "analytics" && baseline.Action == "consume" {
			t.Fatalf("ambiguous healthy multiplicity must not become an empirical baseline: %+v", baseline)
		}
	}

	failing := messageRun("f", now.Add(2*time.Second), 1)
	if divergences := CompareMessageTopology(profile, failing); len(divergences) != 0 {
		t.Fatalf("healthy-observed ambiguous flow must not be relabeled unexpected: %#v", divergences)
	}
}

func TestMessageTopologyProfileWithholdsKeyWhoseIDIsReusedByUnkeyedHealthyMessage(t *testing.T) {
	now := time.Now().UTC()
	healthy1 := messageRun("h1", now, 1)
	healthy2 := messageRun("h2", now.Add(time.Second), 1)
	var analyticsID string
	for i := range healthy2 {
		if healthy2[i].Service == "analytics" && healthy2[i].Attributes["message.action"] == "consume" {
			analyticsID = healthy2[i].ID
			break
		}
	}
	unkeyed := messageEvent(analyticsID, "", "consume", now.Add(2*time.Second))
	unkeyed.Attributes["topic"] = "   "
	healthy2 = append(healthy2, unkeyed)

	profile, err := BuildMessageTopologyProfile([][]model.Event{healthy1, healthy2})
	if err != nil {
		t.Fatal(err)
	}
	for _, baseline := range profile.Baselines() {
		if baseline.Service == "analytics" && baseline.Action == "consume" {
			t.Fatalf("malformed reused provenance must withhold the affected healthy baseline: %+v", baseline)
		}
	}

	if divergences := CompareMessageTopology(profile, messageRun("f", now.Add(3*time.Second), 1)); len(divergences) != 0 {
		t.Fatalf("withheld healthy flow must remain observed rather than unexpected: %#v", divergences)
	}
}
