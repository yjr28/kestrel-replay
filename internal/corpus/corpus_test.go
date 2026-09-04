package corpus

import (
	"testing"
	"time"

	"github.com/yjr28/kestrel-replay/internal/fault"
	"github.com/yjr28/kestrel-replay/internal/model"
	"github.com/yjr28/kestrel-replay/internal/replay"
)

func TestV1Definitions(t *testing.T) {
	if err := ValidateDefinitions(); err != nil {
		t.Fatal(err)
	}
	cases := Cases()
	if len(cases) != 4 {
		t.Fatalf("expected four v1 cases, got %d", len(cases))
	}
	want := map[fault.Kind]string{
		fault.Latency:          "inventory-timeout",
		fault.ConnectionReset:  "inventory-connection-reset",
		fault.ServiceCrash:     "inventory-pre-request-crash",
		fault.DuplicateMessage: "orders-completed-duplicate",
	}
	for _, c := range cases {
		if want[c.Fault.Kind] != c.ID {
			t.Fatalf("unexpected case mapping kind=%s id=%s", c.Fault.Kind, c.ID)
		}
	}
}

func TestDuplicateObservedValidationRequiresMultiplicity(t *testing.T) {
	c := Case{ID: "duplicate", Fault: fault.Spec{Kind: fault.DuplicateMessage, TargetService: "broker", Operation: Topic, TriggerOnMatch: 1}}
	outcome := replay.OutcomeSignature{Classification: "success", HTTPStatus: 201}
	now := time.Now().UTC()
	events := []model.Event{
		{ID: "fault", Source: model.SourceFault, Kind: model.KindFault, Timestamp: now, Attributes: map[string]string{"fault.kind": string(fault.DuplicateMessage), "target.service": "broker"}},
		{ID: "pub", Source: model.SourceApplication, Kind: model.KindMessage, TraceID: "t", Timestamp: now, Service: "order", Attributes: map[string]string{"topic": Topic, "message.id": "m", "message.action": "publish"}},
	}
	for _, service := range []string{"notification", "audit", "analytics"} {
		for i := 0; i < 2; i++ {
			events = append(events, model.Event{ID: service + string(rune('a'+i)), Source: model.SourceApplication, Kind: model.KindMessage, TraceID: "t", Timestamp: now, Service: service, Attributes: map[string]string{"topic": Topic, "message.id": "m", "message.action": "consume"}})
		}
	}
	if err := ValidateObserved(c, outcome, events); err != nil {
		t.Fatalf("valid duplicate evidence rejected: %v", err)
	}
	events = events[:len(events)-1]
	if err := ValidateObserved(c, outcome, events); err == nil {
		t.Fatal("missing duplicate consume should be rejected")
	}
}
