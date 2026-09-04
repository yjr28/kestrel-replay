package broker

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
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

func TestBrokerDelayedMessageRecordsFaultBeforeDelayedDelivery(t *testing.T) {
	delay := 90 * time.Millisecond
	workerTimes := make(chan time.Time, 2)
	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("X-Kestrel-Message-ID"); got != "m" {
			t.Errorf("delayed delivery changed message identity: %q", got)
		}
		workerTimes <- time.Now().UTC()
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

	spec := fault.Spec{Kind: fault.DelayedMessage, TargetService: "broker", Operation: ordersCompletedOperation, TriggerOnMatch: 1, Delay: delay, Seed: 92}
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

	var injected model.Event
	select {
	case injected = <-faultEvents:
	case <-time.After(time.Second):
		t.Fatal("missing delayed-message fault evidence")
	}
	if injected.Kind != model.KindFault || injected.Attributes["fault.kind"] != string(fault.DelayedMessage) || injected.Attributes["message.id"] != "m" {
		t.Fatalf("unexpected delayed-message fault event: %+v", injected)
	}
	if injected.Attributes["delay_us"] != strconv.FormatInt(delay.Microseconds(), 10) || injected.Attributes["schedule.phase"] != "before_delivery" {
		t.Fatalf("incomplete delayed-message evidence: %+v", injected)
	}

	firstDelivery := time.Time{}
	for i := 0; i < 2; i++ {
		select {
		case at := <-workerTimes:
			if firstDelivery.IsZero() || at.Before(firstDelivery) {
				firstDelivery = at
			}
		case <-time.After(time.Second):
			t.Fatalf("expected two delayed fan-out deliveries, got %d", i)
		}
	}
	if elapsed := firstDelivery.Sub(injected.Timestamp); elapsed < 70*time.Millisecond {
		t.Fatalf("delivery was not meaningfully delayed: elapsed=%v configured=%v", elapsed, delay)
	}
	select {
	case at := <-workerTimes:
		t.Fatalf("delayed fault duplicated delivery unexpectedly at %v", at)
	case <-time.After(20 * time.Millisecond):
	}

	statsResp, err := http.Get(srv.URL + "/stats")
	if err != nil {
		t.Fatal(err)
	}
	defer statsResp.Body.Close()
	var stats map[string]uint64
	if err := json.NewDecoder(statsResp.Body).Decode(&stats); err != nil {
		t.Fatal(err)
	}
	if stats["delayed_envelopes"] != 1 || stats["duplicated_envelopes"] != 0 || stats["faults_injected"] != 1 {
		t.Fatalf("unexpected delayed broker stats: %+v", stats)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := b.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestBrokerAsyncFaultRequiresBrokerTargetAndCollector(t *testing.T) {
	for _, kind := range []fault.Kind{fault.DuplicateMessage, fault.DelayedMessage} {
		spec := fault.Spec{Kind: kind, TargetService: "inventory", Operation: ordersCompletedOperation, TriggerOnMatch: 1, Seed: 7}
		if kind == fault.DelayedMessage {
			spec.Delay = 10 * time.Millisecond
		}
		if _, err := NewWithFault([]string{"http://worker"}, 1, "http://collector", &spec); err == nil {
			t.Fatalf("expected non-broker %s target rejection", kind)
		}
		spec.TargetService = "broker"
		if _, err := NewWithFault([]string{"http://worker"}, 1, "", &spec); err == nil {
			t.Fatalf("expected missing collector rejection for %s", kind)
		}
	}
}
