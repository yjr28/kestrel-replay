package telemetry

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yjr28/kestrel-replay/internal/model"
)

const maxDeliveryAttempts = 5

type Exporter struct {
	endpoint string
	client   *http.Client
	queue    chan model.Event
	wg       sync.WaitGroup

	stateMu sync.RWMutex
	closed  bool

	pending atomic.Int64
	dropped atomic.Uint64
	errors  atomic.Uint64
	sent    atomic.Uint64
	retries atomic.Uint64
}

func NewExporter(collectorBaseURL string, capacity int) *Exporter {
	if capacity < 1 {
		capacity = 1
	}
	e := &Exporter{
		endpoint: collectorBaseURL + "/v1/events",
		client:   &http.Client{Timeout: 750 * time.Millisecond},
		queue:    make(chan model.Event, capacity),
	}
	e.wg.Add(1)
	go e.run()
	return e
}

func (e *Exporter) Emit(event model.Event) bool {
	e.stateMu.RLock()
	defer e.stateMu.RUnlock()
	if e.closed {
		return false
	}
	e.pending.Add(1)
	select {
	case e.queue <- event:
		return true
	default:
		e.pending.Add(-1)
		e.dropped.Add(1)
		return false
	}
}

func (e *Exporter) run() {
	defer e.wg.Done()
	for event := range e.queue {
		if e.deliver(event) {
			e.sent.Add(1)
		} else {
			e.errors.Add(1)
		}
		e.pending.Add(-1)
	}
}

func (e *Exporter) deliver(event model.Event) bool {
	payload, err := json.Marshal(event)
	if err != nil {
		return false
	}
	for attempt := 0; attempt < maxDeliveryAttempts; attempt++ {
		if attempt > 0 {
			e.retries.Add(1)
			time.Sleep(time.Duration(1<<(attempt-1)) * 10 * time.Millisecond)
		}
		req, err := http.NewRequest(http.MethodPost, e.endpoint, bytes.NewReader(payload))
		if err != nil {
			return false
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := e.client.Do(req)
		if err != nil {
			continue
		}
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
		resp.Body.Close()
		if resp.StatusCode/100 == 2 {
			return true
		}
		if resp.StatusCode != http.StatusTooManyRequests && resp.StatusCode/100 != 5 {
			return false
		}
	}
	return false
}

// Flush waits until every event accepted by Emit has reached a terminal
// delivery state. It does not close the exporter and is safe to use as an
// experiment evidence barrier while the process remains running.
func (e *Exporter) Flush(ctx context.Context) error {
	ticker := time.NewTicker(2 * time.Millisecond)
	defer ticker.Stop()
	for {
		if e.pending.Load() == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (e *Exporter) Close(ctx context.Context) error {
	e.stateMu.Lock()
	if e.closed {
		e.stateMu.Unlock()
		return nil
	}
	e.closed = true
	close(e.queue)
	e.stateMu.Unlock()

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

func (e *Exporter) Pending() int64 { return e.pending.Load() }
func (e *Exporter) Retries() uint64 { return e.retries.Load() }
