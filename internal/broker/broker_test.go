package broker

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestBrokerFansOut(t *testing.T) {
	var hits atomic.Int64
	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits.Add(1); w.WriteHeader(http.StatusNoContent) }))
	defer worker.Close()
	b := New([]string{worker.URL, worker.URL}, 4)
	srv := httptest.NewServer(b.Handler())
	resp, err := http.Post(srv.URL+"/publish", "application/json", strings.NewReader(`{"trace_id":"t","parent_span_id":"s","request_id":"r","message_id":"m"}`))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	deadline := time.Now().Add(time.Second)
	for hits.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if hits.Load() != 2 {
		t.Fatalf("expected 2 deliveries, got %d", hits.Load())
	}
	srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := b.Close(ctx); err != nil {
		t.Fatal(err)
	}
}
