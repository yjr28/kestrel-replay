package broker

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yjr28/kestrel-replay/internal/fault"
	"github.com/yjr28/kestrel-replay/internal/model"
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

func TestBrokerDuplicateMessageRecordsFaultBeforeDuplicateDelivery(t *testing.T) {
	var hits atomic.Int64
	var mu sync.Mutex
	messageIDs := []string{}
	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		messageIDs = append(messageIDs, r.Header.Get("X-Kestrel-Message-ID"))
		mu.Unlock()
		hits.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer worker.Close()

	faultEvents := make(chan model.Event, 1)
	collector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/events" {
			http.NotFound(w, r)
			return
		}
		var event model.Event
		if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		faultEvents <- event
		w.WriteHeader(http.StatusAccepted)
	}))
	defer collector.Close()

	spec := fault.Spec{Kind: fault.DuplicateMessage, TargetService: "broker", Operation: ordersCompletedOperation, TriggerOnMatch: 1, Seed: 91}
	b, err := NewWithFault([]string{worker.URL, worker.URL}, 4, collector.URL, &spec)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(b.Handler())
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/publish", "application/json", strings.NewReader(`{"trace_id":"t","parent_span_id":"s","request_id":"r","message_id":"m"}`))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("publish status=%d", resp.StatusCode)
	}

	deadline := time.Now().Add(time.Second)
	for hits.Load() < 4 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if hits.Load() != 4 {
		t.Fatalf("expected original + duplicate fan-out (4 deliveries), got %d", hits.Load())
	}
	mu.Lock()
	for _, id := range messageIDs {
		if id != "m" {
			t.Fatalf("duplicate changed message identity: %q", id)
		}
	}
	mu.Unlock()

	select {
	case event := <-faultEvents:
		if event.Kind != model.KindFault || event.Attributes["fault.kind"] != string(fault.DuplicateMessage) || event.Attributes["message.id"] != "m" {
			t.Fatalf("unexpected fault event: %+v", event)
		}
		if event.Attributes["duplicate.extra_copies"] != "1" || event.CorrelationID != "r" {
			t.Fatalf("incomplete duplicate evidence: %+v", event)
		}
	default:
		t.Fatal("duplicate delivery occurred without recorded fault evidence")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := b.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestBrokerDuplicateFaultRequiresBrokerTargetAndCollector(t *testing.T) {
	spec := fault.Spec{Kind: fault.DuplicateMessage, TargetService: "inventory", Operation: ordersCompletedOperation, TriggerOnMatch: 1}
	if _, err := NewWithFault([]string{"http://worker"}, 1, "http://collector", &spec); err == nil {
		t.Fatal("expected non-broker duplicate target rejection")
	}
	spec.TargetService = "broker"
	if _, err := NewWithFault([]string{"http://worker"}, 1, "", &spec); err == nil {
		t.Fatal("expected missing collector rejection")
	}
}
