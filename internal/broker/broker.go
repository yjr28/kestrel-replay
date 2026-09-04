package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yjr28/kestrel-replay/internal/tracecontext"
	"github.com/yjr28/kestrel-replay/internal/transport"
)

type Envelope struct {
	TraceID      string `json:"trace_id"`
	ParentSpanID string `json:"parent_span_id"`
	RequestID    string `json:"request_id"`
	MessageID    string `json:"message_id"`
}

type Broker struct {
	workers   []string
	queue     chan Envelope
	client    *http.Client
	wg        sync.WaitGroup
	inflight  atomic.Int64
	delivered atomic.Uint64
	errors    atomic.Uint64
	closed    atomic.Bool
}

func New(workers []string, capacity int) *Broker {
	if capacity < 1 {
		capacity = 1
	}
	b := &Broker{workers: append([]string(nil), workers...), queue: make(chan Envelope, capacity), client: &http.Client{Timeout: 500 * time.Millisecond}}
	b.wg.Add(1)
	go b.run()
	return b
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
		select {
		case b.queue <- env:
			w.WriteHeader(http.StatusAccepted)
		default:
			http.Error(w, "broker queue full", http.StatusServiceUnavailable)
		}
	})
	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"queued": len(b.queue), "inflight": b.inflight.Load(), "delivered": b.delivered.Load(), "errors": b.errors.Load(),
		})
	})
	return mux
}

func (b *Broker) run() {
	defer b.wg.Done()
	for env := range b.queue {
		b.inflight.Add(1)
		for _, worker := range b.workers {
			if err := b.deliver(worker, env); err != nil {
				b.errors.Add(1)
			} else {
				b.delivered.Add(1)
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
