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
