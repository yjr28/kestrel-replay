package broker

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/yjr28/kestrel-replay/internal/transport"
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

func TestBrokerNormalizesEnvelopeIdentifiersBeforeDelivery(t *testing.T) {
	const (
		traceID   = "0123456789abcdef0123456789abcdef"
		spanID    = "0123456789abcdef"
		requestID = "request-1"
		messageID = "message-1"
	)

	delivered := make(chan http.Header, 1)
	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		delivered <- r.Header.Clone()
		w.WriteHeader(http.StatusNoContent)
	}))
	defer worker.Close()

	b := New([]string{worker.URL}, 1)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := b.Close(ctx); err != nil {
			t.Errorf("close broker: %v", err)
		}
	}()

	body := `{"trace_id":"  ` + traceID + `  ","parent_span_id":"\t` + spanID + `\t","request_id":" ` + requestID + ` ","message_id":"\n` + messageID + `\n"}`
	req := httptest.NewRequest(http.MethodPost, "/publish", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	b.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected normalized envelope acceptance: status=%d body=%q", rec.Code, rec.Body.String())
	}

	select {
	case headers := <-delivered:
		wantTraceParent := "00-" + traceID + "-" + spanID + "-01"
		if got := headers.Get(transport.TraceParentHeader); got != wantTraceParent {
			t.Fatalf("traceparent = %q, want %q", got, wantTraceParent)
		}
		if got := headers.Get(transport.RequestIDHeader); got != requestID {
			t.Fatalf("request id = %q, want %q", got, requestID)
		}
		if got := headers.Get(transport.MessageIDHeader); got != messageID {
			t.Fatalf("message id = %q, want %q", got, messageID)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for broker delivery")
	}
}
