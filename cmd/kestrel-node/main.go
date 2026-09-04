package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/yjr28/kestrel-replay/internal/broker"
	"github.com/yjr28/kestrel-replay/internal/collector"
	"github.com/yjr28/kestrel-replay/internal/fault"
	"github.com/yjr28/kestrel-replay/internal/serviceapp"
	"github.com/yjr28/kestrel-replay/internal/telemetry"
)

func main() {
	mode := flag.String("mode", "", "node mode: collector, broker, or service")
	listen := flag.String("listen", "127.0.0.1:0", "HTTP listen address")
	collectorURL := flag.String("collector", "", "collector base URL (service/broker fault mode)")
	role := flag.String("role", "", "service role")
	nextURL := flag.String("next", "", "next synchronous service URL")
	inventoryURL := flag.String("inventory", "", "inventory service URL")
	pricingURL := flag.String("pricing", "", "pricing service URL")
	paymentURL := flag.String("payment", "", "payment service URL")
	brokerURL := flag.String("broker", "", "broker base URL")
	workers := flag.String("workers", "", "comma-separated worker URLs (broker mode)")
	queueCapacity := flag.Int("queue-capacity", 4096, "bounded queue capacity")
	faultKind := flag.String("fault-kind", "", "optional fault kind")
	faultTarget := flag.String("fault-target", "", "fault target service")
	faultOperation := flag.String("fault-operation", "", "fault target operation")
	faultDelay := flag.Duration("fault-delay", 0, "fault delay")
	faultSeed := flag.Int64("fault-seed", 0, "fault seed")
	faultTrigger := flag.Int("fault-trigger", 1, "matching occurrence on which to inject")
	flag.Parse()

	var configuredFault *fault.Spec
	if *faultKind != "" {
		configuredFault = &fault.Spec{
			Kind: fault.Kind(*faultKind), TargetService: *faultTarget, Operation: *faultOperation,
			TriggerOnMatch: *faultTrigger, Delay: *faultDelay, Seed: *faultSeed,
		}
	}

	var handler http.Handler
	var cleanup func(context.Context) error

	switch *mode {
	case "collector":
		c := collector.New(collector.Config{QueueCapacity: *queueCapacity})
		handler = c.Handler()
		cleanup = func(context.Context) error { c.Close(); return nil }
	case "broker":
		urls := nonEmpty(strings.Split(*workers, ","))
		if len(urls) == 0 {
			log.Fatal("broker mode requires -workers")
		}
		b, err := broker.NewWithFault(urls, *queueCapacity, *collectorURL, configuredFault)
		if err != nil {
			log.Fatal(err)
		}
		handler = b.Handler()
		cleanup = b.Close
	case "service":
		if *collectorURL == "" || *role == "" {
			log.Fatal("service mode requires -collector and -role")
		}
		exporter := telemetry.NewExporter(*collectorURL, *queueCapacity)
		specs := []fault.Spec(nil)
		if configuredFault != nil {
			specs = append(specs, *configuredFault)
		}
		app, err := serviceapp.New(serviceapp.Config{Role: *role, NextURL: *nextURL, InventoryURL: *inventoryURL, PricingURL: *pricingURL, PaymentURL: *paymentURL, BrokerURL: *brokerURL, Faults: specs}, exporter)
		if err != nil {
			log.Fatal(err)
		}
		handler = withTelemetryBarrier(app.Handler(), exporter)
		cleanup = exporter.Close
	default:
		log.Fatal("-mode must be collector, broker, or service")
	}

	srv := &http.Server{Addr: *listen, Handler: handler, ReadHeaderTimeout: 2 * time.Second}
	errCh := make(chan error, 1)
	go func() {
		log.Printf("kestrel node mode=%s listening=%s", *mode, *listen)
		errCh <- srv.ListenAndServe()
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	select {
	case sig := <-sigCh:
		log.Printf("received %s", sig)
	case err := <-errCh:
		if err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("http shutdown: %v", err)
	}
	if cleanup != nil {
		if err := cleanup(ctx); err != nil {
			log.Printf("cleanup: %v", err)
		}
	}
	fmt.Println("kestrel node stopped")
}

// withTelemetryBarrier keeps the application handler asynchronous during normal
// requests while providing the experiment harness a deterministic evidence
// boundary. The flush endpoint itself is intentionally outside application
// instrumentation and waits until all application handlers have returned before
// draining the exporter's accepted-event set.
func withTelemetryBarrier(app http.Handler, exporter *telemetry.Exporter) http.Handler {
	var active atomic.Int64
	mux := http.NewServeMux()
	mux.HandleFunc("POST /telemetry/flush", func(w http.ResponseWriter, r *http.Request) {
		ticker := time.NewTicker(2 * time.Millisecond)
		defer ticker.Stop()
		for active.Load() != 0 {
			select {
			case <-r.Context().Done():
				http.Error(w, "active request drain: "+r.Context().Err().Error(), http.StatusGatewayTimeout)
				return
			case <-ticker.C:
			}
		}
		if err := exporter.Flush(r.Context()); err != nil {
			http.Error(w, "telemetry drain: "+err.Error(), http.StatusGatewayTimeout)
			return
		}
		sent, dropped, errs, queued := exporter.Stats()
		if dropped != 0 || errs != 0 || queued != 0 || exporter.Pending() != 0 {
			http.Error(w, fmt.Sprintf("telemetry incomplete sent=%d dropped=%d errors=%d queued=%d pending=%d", sent, dropped, errs, queued, exporter.Pending()), http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("X-Kestrel-Telemetry-Sent", fmt.Sprintf("%d", sent))
		w.Header().Set("X-Kestrel-Telemetry-Retries", fmt.Sprintf("%d", exporter.Retries()))
		w.WriteHeader(http.StatusNoContent)
	})
	mux.Handle("/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		active.Add(1)
		defer active.Add(-1)
		app.ServeHTTP(w, r)
	}))
	return mux
}

func nonEmpty(values []string) []string {
	out := values[:0]
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			out = append(out, strings.TrimSpace(v))
		}
	}
	return out
}
