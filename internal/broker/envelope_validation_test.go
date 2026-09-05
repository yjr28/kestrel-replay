package broker

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestBrokerRejectsWhitespaceOnlyEnvelopeIdentifiers(t *testing.T) {
	b := New(nil, 1)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := b.Close(ctx); err != nil {
			t.Errorf("close broker: %v", err)
		}
	}()

	bodies := []string{
		`{"trace_id":" ","parent_span_id":"s","request_id":"r","message_id":"m"}`,
		`{"trace_id":"t","parent_span_id":"\t","request_id":"r","message_id":"m"}`,
		`{"trace_id":"t","parent_span_id":"s","request_id":"\n","message_id":"m"}`,
		`{"trace_id":"t","parent_span_id":"s","request_id":"r","message_id":"  \t"}`,
	}
	for _, body := range bodies {
		req := httptest.NewRequest(http.MethodPost, "/publish", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		b.Handler().ServeHTTP(rec, req)
		if rec.Code != http.StatusUnprocessableEntity {
			t.Fatalf("expected whitespace-only identifier rejection for %s: status=%d body=%q", body, rec.Code, rec.Body.String())
		}
	}
}

func TestBrokerNormalizesEnvelopeIdentifiersBeforeQueueing(t *testing.T) {
	const (
		traceID   = "0123456789abcdef0123456789abcdef"
		spanID    = "0123456789abcdef"
		requestID = "request-1"
		messageID = "message-1"
	)

	b := &Broker{queue: make(chan delivery, 1)}
	body := `{"trace_id":"  ` + traceID + `  ","parent_span_id":"\t` + spanID + `\t","request_id":" ` + requestID + ` ","message_id":"\n` + messageID + `\n"}`
	req := httptest.NewRequest(http.MethodPost, "/publish", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	b.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected normalized envelope acceptance: status=%d body=%q", rec.Code, rec.Body.String())
	}

	item := <-b.queue
	if item.envelope.TraceID != traceID {
		t.Fatalf("trace id = %q, want %q", item.envelope.TraceID, traceID)
	}
	if item.envelope.ParentSpanID != spanID {
		t.Fatalf("parent span id = %q, want %q", item.envelope.ParentSpanID, spanID)
	}
	if item.envelope.RequestID != requestID {
		t.Fatalf("request id = %q, want %q", item.envelope.RequestID, requestID)
	}
	if item.envelope.MessageID != messageID {
		t.Fatalf("message id = %q, want %q", item.envelope.MessageID, messageID)
	}
}
