package model

import (
	"testing"
	"time"
)

func TestValidateRejectsWhitespaceOnlyRequiredEvidenceFields(t *testing.T) {
	now := time.Now().UTC()
	tests := []struct {
		name  string
		event Event
	}{
		{
			name: "span service",
			event: Event{ID: "span-service", Kind: KindSpan, TraceID: "trace", SpanID: "span", Service: " \t ", Operation: "op", Timestamp: now},
		},
		{
			name: "span operation",
			event: Event{ID: "span-operation", Kind: KindSpan, TraceID: "trace", SpanID: "span", Service: "svc", Operation: " \n ", Timestamp: now},
		},
		{
			name: "message id",
			event: Event{ID: "message-id", Kind: KindMessage, TraceID: "trace", Timestamp: now, Attributes: map[string]string{"message.id": "   ", "message.action": "publish"}},
		},
		{
			name: "message action",
			event: Event{ID: "message-action", Kind: KindMessage, TraceID: "trace", Timestamp: now, Attributes: map[string]string{"message.id": "message-1", "message.action": "\t"}},
		},
		{
			name: "fault kind",
			event: Event{ID: "fault-kind", Kind: KindFault, Timestamp: now, Attributes: map[string]string{"fault.kind": " ", "target.service": "svc"}},
		},
		{
			name: "fault target service",
			event: Event{ID: "fault-service", Kind: KindFault, Timestamp: now, Attributes: map[string]string{"fault.kind": "delay", "target.service": " \r\n"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.event.Validate(); err == nil {
				t.Fatal("expected whitespace-only required field to be rejected")
			}
		})
	}
}
