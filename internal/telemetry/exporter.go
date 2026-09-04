package telemetry

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yjr28/kestrel-replay/internal/model"
)

type Exporter struct {
	endpoint string
	client   *http.Client
	queue    chan model.Event
	wg       sync.WaitGroup
	closed   atomic.Bool
	dropped  atomic.Uint64
	errors   atomic.Uint64
	sent     atomic.Uint64
}

func NewExporter(collectorBaseURL string, capacity int) *Exporter {
	if capacity < 1 {
		capacity = 1
	}
	e := &Exporter{
		endpoint: collectorBaseURL + "/v1/events",
		client:   &http.Client{Timeout: 500 * time.Millisecond},
		queue:    make(chan model.Event, capacity),
	}
	e.wg.Add(1)
	go e.run()
	return e
}

func (e *Exporter) Emit(event model.Event) bool {
	if e.closed.Load() {
		return false
	}
	select {
	case e.queue <- event:
		return true
	default:
		e.dropped.Add(1)
		return false
	}
}

func (e *Exporter) run() {
	defer e.wg.Done()
	for event := range e.queue {
		payload, err := json.Marshal(event)
		if err != nil {
			e.errors.Add(1)
			continue
		}
		req, err := http.NewRequest(http.MethodPost, e.endpoint, bytes.NewReader(payload))
		if err != nil {
			e.errors.Add(1)
			continue
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := e.client.Do(req)
		if err != nil {
			e.errors.Add(1)
			continue
		}
		resp.Body.Close()
		if resp.StatusCode/100 != 2 {
			e.errors.Add(1)
			continue
		}
		e.sent.Add(1)
	}
}

func (e *Exporter) Close(ctx context.Context) error {
	if e.closed.Swap(true) {
		return nil
	}
	close(e.queue)
	done := make(chan struct{})
	go func() { e.wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (e *Exporter) Stats() (sent, dropped, errors uint64, queued int) {
	return e.sent.Load(), e.dropped.Load(), e.errors.Load(), len(e.queue)
}
