package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yjr28/kestrel-replay/internal/fault"
	"github.com/yjr28/kestrel-replay/internal/model"
	"github.com/yjr28/kestrel-replay/internal/tracecontext"
	"github.com/yjr28/kestrel-replay/internal/transport"
)

const ordersCompletedOperation = "orders.completed"

type Envelope struct {
	TraceID      string `json:"trace_id"`
	ParentSpanID string `json:"parent_span_id"`
	RequestID    string `json:"request_id"`
	MessageID    string `json:"message_id"`
}

type delivery struct {
	envelope  Envelope
	duplicate bool
}

type Broker struct {
	workers      []string
	queue        chan delivery
	client       *http.Client
	collectorURL string
	faultSpec    *fault.Spec
	faultMu      sync.Mutex
	faultMatches int
	wg           sync.WaitGroup
	inflight     atomic.Int64
	delivered    atomic.Uint64
	errors       atomic.Uint64
	duplicated   atomic.Uint64
	injected     atomic.Uint64
	closed       atomic.Bool
}

func New(workers []string, capacity int) *Broker {
	b, err := NewWithFault(workers, capacity, "", nil)
	if err != nil {
		panic(err)
	}
	return b
}

// NewWithFault configures the broker-owned asynchronous fault path. The current
// implementation supports duplicate_message for the orders.completed fan-out.
// Fault evidence is synchronously accepted by the collector before a duplicated
// delivery is allowed into the broker queue.
func NewWithFault(workers []string, capacity int, collectorURL string, spec *fault.Spec) (*Broker, error) {
	if capacity < 1 {
		capacity = 1
	}
	var copied *fault.Spec
	if spec != nil {
		if err := spec.Validate(); err != nil {
			return nil, err
		}
		if spec.Kind != fault.DuplicateMessage {
			return nil, fmt.Errorf("broker fault kind %q is not supported", spec.Kind)
		}
		if spec.TargetService != "broker" {
			return nil, fmt.Errorf("duplicate message broker fault requires target service broker")
		}
		if spec.Operation != ordersCompletedOperation {
			return nil, fmt.Errorf("duplicate message broker fault requires operation %s", ordersCompletedOperation)
		}
		if strings.TrimSpace(collectorURL) == "" {
			return nil, fmt.Errorf("duplicate message broker fault requires collector URL")
		}
		v := *spec
		copied = &v
	}
	b := &Broker{
		workers:      append([]string(nil), workers...),
		queue:        make(chan delivery, capacity),
		client:       &http.Client{Timeout: 500 * time.Millisecond},
		collectorURL: strings.TrimRight(collectorURL, "/"),
		faultSpec:    copied,
	}
	b.wg.Add(1)
	go b.run()
	return b, nil
}

func (b *Broker) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) })
	mux.HandleFunc("POST /publish", func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var env Envelope
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&env); err != nil {
			http.Error(w, "invalid envelope", http.StatusBadRequest)
			return
		}
		if env.TraceID == "" || env.ParentSpanID == "" || env.RequestID == "" || env.MessageID == "" {
			http.Error(w, "missing envelope identifiers", http.StatusUnprocessableEntity)
			return
		}
		if b.closed.Load() {
			http.Error(w, "broker closed", http.StatusServiceUnavailable)
			return
		}

		duplicate := b.decideDuplicate()
		if duplicate {
			if err := b.recordDuplicateFault(r.Context(), env); err != nil {
				b.errors.Add(1)
				http.Error(w, "record duplicate fault: "+err.Error(), http.StatusServiceUnavailable)
				return
			}
			b.injected.Add(1)
		}

		select {
		case b.queue <- delivery{envelope: env, duplicate: duplicate}:
			w.WriteHeader(http.StatusAccepted)
		default:
			http.Error(w, "broker queue full", http.StatusServiceUnavailable)
		}
	})
	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"queued": len(b.queue), "inflight": b.inflight.Load(), "delivered": b.delivered.Load(), "errors": b.errors.Load(),
			"duplicated_envelopes": b.duplicated.Load(), "faults_injected": b.injected.Load(),
		})
	})
	return mux
}

func (b *Broker) decideDuplicate() bool {
	b.faultMu.Lock()
	defer b.faultMu.Unlock()
	if b.faultSpec == nil {
		return false
	}
	b.faultMatches++
	return b.faultMatches == b.faultSpec.TriggerOnMatch
}

func (b *Broker) recordDuplicateFault(ctx context.Context, env Envelope) error {
	spec := b.faultSpec
	if spec == nil {
		return fmt.Errorf("duplicate fault is not configured")
	}
	event := model.Event{
		ID:            "broker-fault-" + env.MessageID,
		Source:        model.SourceFault,
		Kind:          model.KindFault,
		TraceID:       env.TraceID,
		CorrelationID: env.RequestID,
		Service:       "broker",
		Operation:     ordersCompletedOperation,
		Timestamp:     time.Now().UTC(),
		Status:        "injected",
		Attributes: map[string]string{
			"fault.kind":             string(spec.Kind),
			"target.service":         spec.TargetService,
			"target.operation":       spec.Operation,
			"seed":                   strconv.FormatInt(spec.Seed, 10),
			"trigger_on_match":       strconv.Itoa(spec.TriggerOnMatch),
			"message.id":             env.MessageID,
			"duplicate.extra_copies": "1",
		},
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, b.collectorURL+"/v1/events", bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := b.client.Do(req)
	if err != nil {
		return err
	}
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, 8<<10))
	resp.Body.Close()
	if readErr != nil {
		return readErr
	}
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("collector status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

func (b *Broker) run() {
	defer b.wg.Done()
	for item := range b.queue {
		b.inflight.Add(1)
		rounds := 1
		if item.duplicate {
			rounds = 2
			b.duplicated.Add(1)
		}
		for round := 0; round < rounds; round++ {
			for _, worker := range b.workers {
				if err := b.deliver(worker, item.envelope); err != nil {
					b.errors.Add(1)
				} else {
					b.delivered.Add(1)
				}
			}
		}
		b.inflight.Add(-1)
	}
}

func (b *Broker) deliver(worker string, env Envelope) error {
	req, err := http.NewRequest(http.MethodPost, worker, bytes.NewReader([]byte(`{}`)))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(transport.TraceParentHeader, tracecontext.Context{TraceID: env.TraceID, SpanID: env.ParentSpanID, Flags: 1}.String())
	req.Header.Set(transport.RequestIDHeader, env.RequestID)
	req.Header.Set(transport.MessageIDHeader, env.MessageID)
	resp, err := b.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("worker returned %d", resp.StatusCode)
	}
	return nil
}

func (b *Broker) Close(ctx context.Context) error {
	if b.closed.Swap(true) {
		return nil
	}
	close(b.queue)
	done := make(chan struct{})
	go func() { b.wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
