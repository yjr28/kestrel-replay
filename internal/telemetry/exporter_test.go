package telemetry

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yjr28/kestrel-replay/internal/collector"
	"github.com/yjr28/kestrel-replay/internal/model"
)

func TestExporterFlushesOnClose(t *testing.T) {
	server := collector.New(collector.Config{QueueCapacity: 16})
	defer server.Close()
	srv := httptest.NewServer(server.Handler())
	defer srv.Close()
	e := NewExporter(srv.URL, 8)
	e.Emit(testEvent("e1"))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := e.Close(ctx); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for server.Stats().Stored < 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if server.Stats().Stored != 1 {
		t.Fatalf("event was not flushed")
	}
}

func TestExporterFlushWaitsForAcceptedEvents(t *testing.T) {
	server := collector.New(collector.Config{QueueCapacity: 16, ProcessDelay: 10 * time.Millisecond})
	defer server.Close()
	srv := httptest.NewServer(server.Handler())
	defer srv.Close()
	e := NewExporter(srv.URL, 8)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = e.Close(ctx)
	}()
	for i := 0; i < 4; i++ {
		if !e.Emit(testEvent(string(rune('a' + i)))) {
			t.Fatalf("emit %d was not accepted", i)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := e.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if e.Pending() != 0 {
		t.Fatalf("pending=%d after flush", e.Pending())
	}
	sent, dropped, errs, _ := e.Stats()
	if sent != 4 || dropped != 0 || errs != 0 {
		t.Fatalf("unexpected exporter stats sent=%d dropped=%d errors=%d", sent, dropped, errs)
	}
}

func TestExporterRetriesTransientCollectorFailure(t *testing.T) {
	var attempts atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if attempts.Add(1) < 3 {
			http.Error(w, "temporary", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()
	e := NewExporter(srv.URL, 4)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = e.Close(ctx)
	}()
	if !e.Emit(testEvent("retry")) {
		t.Fatal("event was not accepted")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := e.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	sent, dropped, errs, _ := e.Stats()
	if sent != 1 || dropped != 0 || errs != 0 || attempts.Load() != 3 || e.Retries() != 2 {
		t.Fatalf("unexpected retry result sent=%d dropped=%d errors=%d attempts=%d retries=%d", sent, dropped, errs, attempts.Load(), e.Retries())
	}
}

func testEvent(id string) model.Event {
	return model.Event{ID: id, Source: model.SourceApplication, Kind: model.KindSpan, TraceID: "trace", SpanID: "span-" + id, Service: "svc", Operation: "op", Timestamp: time.Now().UTC()}
}
