package graph

import (
	"testing"
	"time"

	"github.com/yjr28/kestrel-replay/internal/model"
)

func TestMessageTopologyNormalizesFlowKeys(t *testing.T) {
	now := time.Now().UTC()
	healthy1 := messageRun("h1", now, 1)
	healthy2 := withMessageFlowWhitespace(messageRun("h2", now.Add(time.Second), 1))

	profile, err := BuildMessageTopologyProfile([][]model.Event{healthy1, healthy2})
	if err != nil {
		t.Fatal(err)
	}
	baselines := profile.Baselines()
	if len(baselines) != 4 {
		t.Fatalf("formatting-only flow-key whitespace fragmented healthy baselines: %#v", baselines)
	}
	for _, baseline := range baselines {
		if baseline.Topic != "orders.completed" || (baseline.Action != "publish" && baseline.Action != "consume") {
			t.Fatalf("baseline retained unnormalized flow key: %+v", baseline)
		}
		if baseline.Service == "" || baseline.Service[0] == ' ' {
			t.Fatalf("baseline retained unnormalized service: %+v", baseline)
		}
		if baseline.MinCount != 1 || baseline.MaxCount != 1 {
			t.Fatalf("normalized healthy runs should describe one stable flow each: %+v", baseline)
		}
	}

	failing := withMessageFlowWhitespace(messageRun("f", now.Add(2*time.Second), 1))
	if divergences := CompareMessageTopology(profile, failing); len(divergences) != 0 {
		t.Fatalf("formatting-only flow-key whitespace manufactured topology evidence: %#v", divergences)
	}
}

func TestMessageTopologyRequiresNonblankServiceEvidence(t *testing.T) {
	now := time.Now().UTC()
	profile, err := BuildMessageTopologyProfile([][]model.Event{
		messageRun("h1", now, 1),
		messageRun("h2", now.Add(time.Second), 1),
	})
	if err != nil {
		t.Fatal(err)
	}

	failing := messageRun("f", now.Add(2*time.Second), 1)
	unkeyed := messageEvent("f-unkeyed", "   ", "consume", now.Add(2*time.Second))
	failing = append(failing, unkeyed)
	if divergences := CompareMessageTopology(profile, failing); len(divergences) != 0 {
		t.Fatalf("blank service must not establish an unexpected message-flow key: %#v", divergences)
	}
}

func withMessageFlowWhitespace(events []model.Event) []model.Event {
	out := append([]model.Event(nil), events...)
	for i := range out {
		out[i].Service = "  " + out[i].Service + "\t"
		attrs := make(map[string]string, len(out[i].Attributes))
		for key, value := range out[i].Attributes {
			attrs[key] = value
		}
		attrs["topic"] = "\t" + attrs["topic"] + "  "
		attrs["message.id"] = " " + attrs["message.id"] + "\t"
		attrs["message.action"] = "  " + attrs["message.action"] + " "
		out[i].Attributes = attrs
	}
	return out
}
