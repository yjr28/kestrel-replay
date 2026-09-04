package corpus

import (
	"testing"
	"time"

	"github.com/yjr28/kestrel-replay/internal/fault"
	"github.com/yjr28/kestrel-replay/internal/model"
	"github.com/yjr28/kestrel-replay/internal/replay"
)

func TestV1DefinitionsRemainImmutable(t *testing.T) {
	cases := CasesV1()
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
			t.Fatalf("unexpected v1 case mapping kind=%s id=%s", c.Fault.Kind, c.ID)
		}
		if c.Fault.Kind == fault.DelayedMessage {
			t.Fatal("v1 must not be retroactively extended with delayed_message")
		}
	}
}

func TestV2DefinitionsExtendV1WithDelayedMessage(t *testing.T) {
	if Version != "v2" || V1Version != "v1" {
		t.Fatalf("unexpected corpus versions current=%s v1=%s", Version, V1Version)
	}
	if err := ValidateDefinitions(); err != nil {
		t.Fatal(err)
	}
	cases := Cases()
	if len(cases) != 5 {
		t.Fatalf("expected five v2 cases, got %d", len(cases))
	}
	for i, old := range CasesV1() {
		if cases[i] != old {
			t.Fatalf("v2 changed v1 case at index %d: old=%+v new=%+v", i, old, cases[i])
		}
	}
	delayed := cases[len(cases)-1]
	if delayed.ID != "orders-completed-delay" || delayed.Fault.Kind != fault.DelayedMessage || delayed.Fault.Delay != 120*time.Millisecond {
		t.Fatalf("unexpected v2 delayed case: %+v", delayed)
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

func TestDelayedObservedValidationRequiresThresholdAndSingleDelivery(t *testing.T) {
	delay := 120 * time.Millisecond
	c := Case{ID: "delay", Fault: fault.Spec{Kind: fault.DelayedMessage, TargetService: "broker", Operation: Topic, TriggerOnMatch: 1, Delay: delay}}
	outcome := replay.OutcomeSignature{Classification: "success", HTTPStatus: 201}
	now := time.Now().UTC()
	events := []model.Event{
		{ID: "fault", Source: model.SourceFault, Kind: model.KindFault, Timestamp: now.Add(time.Millisecond), Attributes: map[string]string{"fault.kind": string(fault.DelayedMessage), "target.service": "broker", "delay_us": "120000"}},
		{ID: "pub", Source: model.SourceApplication, Kind: model.KindMessage, TraceID: "t", Timestamp: now, Service: "order", Attributes: map[string]string{"topic": Topic, "message.id": "m", "message.action": "publish"}},
	}
	for i, service := range []string{"notification", "audit", "analytics"} {
		events = append(events, model.Event{ID: service, Source: model.SourceApplication, Kind: model.KindMessage, TraceID: "t", Timestamp: now.Add(delay + time.Duration(i)*time.Millisecond), Service: service, Attributes: map[string]string{"topic": Topic, "message.id": "m", "message.action": "consume"}})
	}
	if err := ValidateObserved(c, outcome, events); err != nil {
		t.Fatalf("valid delayed-message evidence rejected: %v", err)
	}

	early := append([]model.Event(nil), events...)
	early[2].Timestamp = now.Add(100 * time.Millisecond)
	if err := ValidateObserved(c, outcome, early); err == nil {
		t.Fatal("consume before configured delay threshold should be rejected")
	}

	duplicated := append([]model.Event(nil), events...)
	duplicated = append(duplicated, model.Event{ID: "notification-extra", Source: model.SourceApplication, Kind: model.KindMessage, TraceID: "t", Timestamp: now.Add(delay + 2*time.Millisecond), Service: "notification", Attributes: map[string]string{"topic": Topic, "message.id": "m", "message.action": "consume"}})
	if err := ValidateObserved(c, outcome, duplicated); err == nil {
		t.Fatal("delayed-message case must reject duplicate delivery")
	}
}
