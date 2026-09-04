package telemetry

import (
	"context"
	"net/http/httptest"
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
	e.Emit(model.Event{ID: "e1", Source: model.SourceApplication, Kind: model.KindSpan, TraceID: "t", SpanID: "s", Service: "svc", Operation: "op", Timestamp: time.Now().UTC()})
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
