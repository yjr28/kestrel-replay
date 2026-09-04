package serviceapp

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"syscall"
	"testing"
	"time"

	"github.com/yjr28/kestrel-replay/internal/collector"
	"github.com/yjr28/kestrel-replay/internal/fault"
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

	events := loadCollectorEvents(t, server, collectorServer.URL, 1)
	if len(events) != 1 || events[0].Service != "pricing" {
		t.Fatalf("unexpected events: %+v", events)
	}
}

func TestInventoryConnectionResetIsRecordedAsTransportFailure(t *testing.T) {
	server := collector.New(collector.Config{QueueCapacity: 16})
	defer server.Close()
	collectorServer := httptest.NewServer(server.Handler())
	defer collectorServer.Close()
	exporter := telemetry.NewExporter(collectorServer.URL, 16)
	app, err := New(Config{
		Role: "inventory",
		Faults: []fault.Spec{{Kind: fault.ConnectionReset, TargetService: "inventory", Operation: "check", TriggerOnMatch: 1, Seed: 17}},
	}, exporter)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(app.Handler())
	client := &http.Client{Timeout: time.Second}
	resp, requestErr := client.Post(srv.URL, "application/json", nil)
	if resp != nil {
		resp.Body.Close()
	}
	if requestErr == nil {
		t.Fatal("expected connection reset to abort the HTTP exchange")
	}
	srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := exporter.Close(ctx); err != nil {
		t.Fatal(err)
	}

	events := loadCollectorEvents(t, server, collectorServer.URL, 2)
	var sawFault, sawResetSpan bool
	for _, event := range events {
		if event.Kind == model.KindFault && event.Attributes["fault.kind"] == string(fault.ConnectionReset) {
			sawFault = true
		}
		if event.Kind == model.KindSpan && event.Service == "inventory" && event.Status == "error" && event.Attributes["transport.error"] == "connection_reset" {
			sawResetSpan = true
		}
	}
	if !sawFault || !sawResetSpan {
		t.Fatalf("missing reset evidence fault=%t span=%t events=%+v", sawFault, sawResetSpan, events)
	}
}

func TestDependencyTransportFailureClassification(t *testing.T) {
	if code, status := dependencyTransportFailure("inventory", context.DeadlineExceeded); code != "inventory_timeout" || status != http.StatusGatewayTimeout {
		t.Fatalf("deadline classification=%s/%d", code, status)
	}
	if code, status := dependencyTransportFailure("inventory", syscall.ECONNRESET); code != "inventory_connection_reset" || status != http.StatusBadGateway {
		t.Fatalf("reset classification=%s/%d", code, status)
	}
	if code, status := dependencyTransportFailure("inventory", errors.New("boom")); code != "inventory_transport_error" || status != http.StatusBadGateway {
		t.Fatalf("generic classification=%s/%d", code, status)
	}
}

func loadCollectorEvents(t *testing.T, server *collector.Server, collectorURL string, minimum uint64) []model.Event {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for server.Stats().Stored < minimum && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	resp, err := http.Get(collectorURL + "/v1/events")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var events []model.Event
	if err := json.NewDecoder(resp.Body).Decode(&events); err != nil {
		t.Fatal(err)
	}
	return events
}
