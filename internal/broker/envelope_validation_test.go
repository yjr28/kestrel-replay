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
