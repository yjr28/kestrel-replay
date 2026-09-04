package serviceapp

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/yjr28/kestrel-replay/internal/collector"
	"github.com/yjr28/kestrel-replay/internal/model"
	"github.com/yjr28/kestrel-replay/internal/telemetry"
)

func TestLeafServiceExportsSpan(t *testing.T) {
	server := collector.New(collector.Config{QueueCapacity: 16})
	defer server.Close()
	collectorServer := httptest.NewServer(server.Handler())
	defer collectorServer.Close()
	exporter := telemetry.NewExporter(collectorServer.URL, 16)
	app, err := New(Config{Role: "pricing"}, exporter)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(app.Handler())
	resp, err := http.Post(srv.URL, "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := exporter.Close(ctx); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(time.Second)
	for server.Stats().Stored < 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	resp2, err := http.Get(collectorServer.URL + "/v1/events")
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	var events []model.Event
	if err := json.NewDecoder(resp2.Body).Decode(&events); err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].Service != "pricing" {
		t.Fatalf("unexpected events: %+v", events)
	}
}
