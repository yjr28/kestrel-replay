package model

import (
	"testing"
	"time"
)

func TestEventValidateSpan(t *testing.T) {
	e := Event{ID: "e1", Kind: KindSpan, Source: SourceApplication, TraceID: "t", SpanID: "s", Service: "order", Operation: "create", Timestamp: time.Now()}
	if err := e.Validate(); err != nil {
		t.Fatalf("expected valid span: %v", err)
	}

	e.Service = ""
	if err := e.Validate(); err == nil {
		t.Fatal("expected missing service to fail validation")
	}
}

func TestEventValidateRejectsUnsupportedKind(t *testing.T) {
	e := Event{ID: "e1", Kind: Kind("unknown"), Source: SourceApplication, Timestamp: time.Now()}
	if err := e.Validate(); err == nil {
		t.Fatal("expected unsupported event kind to fail validation")
	}
}

func TestEventValidateRejectsUnsupportedSource(t *testing.T) {
	e := Event{ID: "e1", Kind: KindNetwork, Source: Source("unknown"), Timestamp: time.Now()}
	if err := e.Validate(); err == nil {
		t.Fatal("expected unsupported event source to fail validation")
	}
}

func TestEventValidateMessageActions(t *testing.T) {
	base := Event{
		ID:        "e1",
		Kind:      KindMessage,
		Source:    SourceApplication,
		TraceID:   "trace",
		Service:   "order",
		Timestamp: time.Now(),
		Attributes: map[string]string{
			"message.id": "m1",
			"topic":      "orders.completed",
		},
	}

	for _, action := range []string{"publish", "consume", " publish ", "\tconsume\n"} {
		e := base
		e.Attributes = map[string]string{"message.id": "m1", "message.action": action, "topic": "orders.completed"}
		if err := e.Validate(); err != nil {
			t.Fatalf("expected supported message action %q to validate: %v", action, err)
		}
	}

	e := base
	e.Attributes = map[string]string{"message.id": "m1", "message.action": "ack", "topic": "orders.completed"}
	if err := e.Validate(); err == nil {
		t.Fatal("expected unsupported message action to fail validation")
	}
}

func TestEventValidateMessageRequiresTopologyIdentity(t *testing.T) {
	base := Event{
		ID:        "e1",
		Kind:      KindMessage,
		Source:    SourceApplication,
		TraceID:   "trace",
		Service:   "order",
		Timestamp: time.Now(),
		Attributes: map[string]string{
			"message.id":     "m1",
			"message.action": "publish",
			"topic":          "orders.completed",
		},
	}

	missingService := base
	missingService.Service = " \t "
	if err := missingService.Validate(); err == nil {
		t.Fatal("expected message event without service identity to fail validation")
	}

	missingTopic := base
	missingTopic.Attributes = map[string]string{
		"message.id":     "m1",
		"message.action": "publish",
		"topic":          " \n ",
	}
	if err := missingTopic.Validate(); err == nil {
		t.Fatal("expected message event without topic identity to fail validation")
	}
}

func TestEventValidateFaultKinds(t *testing.T) {
	base := Event{
		ID:        "e1",
		Kind:      KindFault,
		Source:    SourceFault,
		Timestamp: time.Now(),
	}

	cases := []map[string]string{
		{"target.service": "orders", "target.operation": "create", "fault.kind": "latency"},
		{"target.service": "orders", "target.operation": "create", "fault.kind": "connection_reset"},
		{"target.service": "orders", "fault.kind": "service_crash"},
		{"target.service": "broker", "target.operation": "orders.completed", "fault.kind": "duplicate_message"},
		{"target.service": "broker", "target.operation": "orders.completed", "fault.kind": "delayed_message"},
		{"target.service": "orders", "target.operation": " create ", "fault.kind": " latency "},
		{"target.service": "broker", "target.operation": " orders.completed ", "fault.kind": "\tdelayed_message\n"},
	}
	for _, attributes := range cases {
		e := base
		e.Attributes = attributes
		if err := e.Validate(); err != nil {
			t.Fatalf("expected implemented fault event kind %q to validate: %v", attributes["fault.kind"], err)
		}
	}

	for _, kind := range []string{"packet_loss", "service_restart", "rpc_timeout", "reordered_message", "unknown"} {
		e := base
		e.Attributes = map[string]string{"target.service": "orders", "fault.kind": kind}
		if err := e.Validate(); err == nil {
			t.Fatalf("expected unsupported fault event kind %q to fail validation", kind)
		}
	}
}

func TestEventValidateAsyncFaultRequiresTargetOperation(t *testing.T) {
	for _, kind := range []string{"duplicate_message", "delayed_message"} {
		e := Event{
			ID:        "e1",
			Kind:      KindFault,
			Source:    SourceFault,
			Timestamp: time.Now(),
			Attributes: map[string]string{
				"target.service":   "broker",
				"target.operation": " \t ",
				"fault.kind":       kind,
			},
		}
		if err := e.Validate(); err == nil {
			t.Fatalf("expected %s fault event without a target operation to fail validation", kind)
		}
	}
}

func TestEventCanonicalKeyNormalizesFormattingWhitespace(t *testing.T) {
	base := Event{Kind: KindSpan, Service: "order", Operation: "create", Status: "error"}
	formatted := Event{Kind: KindSpan, Service: " order ", Operation: "\tcreate\n", Status: " error "}

	if got, want := formatted.CanonicalKey(), base.CanonicalKey(); got != want {
		t.Fatalf("expected formatting-only whitespace to preserve canonical identity: got %q want %q", got, want)
	}
}

func TestEventCanonicalKeyFramesFieldsUnambiguously(t *testing.T) {
	a := Event{Kind: KindSpan, Service: "order|create", Operation: "checkout", Status: "error"}
	b := Event{Kind: KindSpan, Service: "order", Operation: "create|checkout", Status: "error"}

	if a.CanonicalKey() == b.CanonicalKey() {
		t.Fatalf("different semantic fields must not collapse to the same canonical key: %q", a.CanonicalKey())
	}
}

func TestEventCanonicalKeyPreservesSourceProvenance(t *testing.T) {
	application := Event{Source: SourceApplication, Kind: KindSpan, Service: "order", Operation: "create", Status: "error"}
	replayed := Event{Source: SourceReplay, Kind: KindSpan, Service: "order", Operation: "create", Status: "error"}

	if application.CanonicalKey() == replayed.CanonicalKey() {
		t.Fatalf("events from different evidence sources must not collapse to the same canonical key: %q", application.CanonicalKey())
	}
}
