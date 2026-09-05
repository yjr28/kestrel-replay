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
		Timestamp: time.Now(),
		Attributes: map[string]string{
			"message.id": "m1",
		},
	}

	for _, action := range []string{"publish", "consume", " publish ", "\tconsume\n"} {
		e := base
		e.Attributes = map[string]string{"message.id": "m1", "message.action": action}
		if err := e.Validate(); err != nil {
			t.Fatalf("expected supported message action %q to validate: %v", action, err)
		}
	}

	e := base
	e.Attributes = map[string]string{"message.id": "m1", "message.action": "ack"}
	if err := e.Validate(); err == nil {
		t.Fatal("expected unsupported message action to fail validation")
	}
}

func TestEventCanonicalKeyNormalizesFormattingWhitespace(t *testing.T) {
	base := Event{Kind: KindSpan, Service: "order", Operation: "create", Status: "error"}
	formatted := Event{Kind: KindSpan, Service: " order ", Operation: "\tcreate\n", Status: " error "}

	if got, want := formatted.CanonicalKey(), base.CanonicalKey(); got != want {
		t.Fatalf("expected formatting-only whitespace to preserve canonical identity: got %q want %q", got, want)
	}
}
