package graph

import (
	"testing"
	"time"

	"github.com/yjr28/kestrel-replay/internal/model"
)

func TestMessageTopologyDoesNotClaimUnexpectedFlowPreviouslySeenWithoutCorrelationIdentity(t *testing.T) {
	now := time.Now().UTC()
	healthy1 := append(messageRun("h1", now, 1), uncorrelatableObservedFlow("h1-search", now))
	healthy2 := append(messageRun("h2", now.Add(time.Second), 1), uncorrelatableObservedFlow("h2-search", now.Add(time.Second)))

	profile, err := BuildMessageTopologyProfile([][]model.Event{healthy1, healthy2})
	if err != nil {
		t.Fatal(err)
	}

	failing := append(messageRun("f", now.Add(2*time.Second), 1), messageEvent("f-search", "search-index", "consume", now.Add(2*time.Second)))
	for _, divergence := range CompareMessageTopology(profile, failing) {
		if divergence.Service == "search-index" && divergence.Action == "consume" && divergence.Topic == "orders.completed" && divergence.Reason == "unexpected_message_flow" {
			t.Fatalf("recognizable healthy flow without correlation identity cannot prove the flow was absent: %+v", divergence)
		}
	}
}

func uncorrelatableObservedFlow(id string, at time.Time) model.Event {
	event := messageEvent(id, "search-index", "consume", at)
	delete(event.Attributes, "message.id")
	return event
}
