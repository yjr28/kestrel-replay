package serviceapp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/yjr28/kestrel-replay/internal/broker"
	"github.com/yjr28/kestrel-replay/internal/fault"
	"github.com/yjr28/kestrel-replay/internal/model"
	"github.com/yjr28/kestrel-replay/internal/telemetry"
	"github.com/yjr28/kestrel-replay/internal/tracecontext"
	"github.com/yjr28/kestrel-replay/internal/transport"
)

type Config struct {
	Role         string
	NextURL      string
	InventoryURL string
	PricingURL   string
	PaymentURL   string
	BrokerURL    string
	Faults       []fault.Spec
}

type App struct {
	cfg      Config
	exporter *telemetry.Exporter
	faults   *fault.Controller
	client   *http.Client
	seq      atomic.Uint64
	prefix   string
}

func New(cfg Config, exporter *telemetry.Exporter) (*App, error) {
	if cfg.Role == "" {
		return nil, fmt.Errorf("role is required")
	}
	controller, err := fault.NewController(cfg.Faults)
	if err != nil {
		return nil, err
	}
	return &App{
		cfg:      cfg,
		exporter: exporter,
		faults:   controller,
		client:   &http.Client{Timeout: 500 * time.Millisecond},
		prefix:   fmt.Sprintf("%s-%d", cfg.Role, os.Getpid()),
	}, nil
}

func (a *App) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) })
	mux.Handle("/", a.instrument(http.HandlerFunc(a.serveRole)))
	return mux
}

func (a *App) instrument(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now().UTC()
		incoming, err := tracecontext.Parse(r.Header.Get(transport.TraceParentHeader))
		traceID := tracecontext.NewTraceID()
		parentSpan := ""
		flags := byte(1)
		if err == nil {
			traceID = incoming.TraceID
			parentSpan = incoming.SpanID
			flags = incoming.Flags
		}
		requestID := r.Header.Get(transport.RequestIDHeader)
		if requestID == "" {
			requestID = "req-" + traceID[:12]
			r.Header.Set(transport.RequestIDHeader, requestID)
		}
		spanID := tracecontext.NewSpanID()
		current := spanContext{TraceID: traceID, SpanID: spanID, RequestID: requestID, Flags: flags}

		if msgID := r.Header.Get(transport.MessageIDHeader); msgID != "" {
			a.emit(model.Event{
				ID: a.nextID("event"), Source: model.SourceApplication, Kind: model.KindMessage,
				TraceID: traceID, SpanID: spanID, ParentSpanID: parentSpan, CorrelationID: requestID,
				Service: a.cfg.Role, Operation: "order_completed", Timestamp: start, Status: "ok",
				Attributes: map[string]string{"message.id": msgID, "message.action": "consume", "topic": "orders.completed"},
			})
		}

		sw := &statusWriter{ResponseWriter: w}
		ctx := context.WithValue(r.Context(), spanKey{}, current)
		next.ServeHTTP(sw, r.WithContext(ctx))

		status := "ok"
		attributes := map[string]string{"duration_us": strconv.FormatInt(time.Since(start).Microseconds(), 10)}
		if sw.hijacked {
			status = "error"
			attributes["http.status_code"] = "0"
			attributes["transport.error"] = "connection_reset"
		} else {
			if sw.status == 0 {
				sw.status = http.StatusOK
			}
			attributes["http.status_code"] = strconv.Itoa(sw.status)
			if sw.status >= 400 {
				status = "error"
			}
		}
		a.emit(model.Event{
			ID: a.nextID("event"), Source: model.SourceApplication, Kind: model.KindSpan,
			TraceID: traceID, SpanID: spanID, ParentSpanID: parentSpan, CorrelationID: requestID,
			Service: a.cfg.Role, Operation: operationFor(a.cfg.Role), Timestamp: start, Status: status,
			Attributes: attributes,
		})
	})
}

type spanKey struct{}
type spanContext struct {
	TraceID   string
	SpanID    string
	RequestID string
	Flags     byte
}

func (a *App) serveRole(w http.ResponseWriter, r *http.Request) {
	current, _ := r.Context().Value(spanKey{}).(spanContext)
	spanID := current.SpanID
	traceID := current.TraceID
	requestID := current.RequestID

	switch a.cfg.Role {
	case "gateway", "auth", "account":
		a.proxy(w, r, a.cfg.NextURL, spanID, 500*time.Millisecond)
	case "order":
		for _, dep := range []struct {
			name    string
			url     string
			timeout time.Duration
		}{{"inventory", a.cfg.InventoryURL, 40 * time.Millisecond}, {"pricing", a.cfg.PricingURL, 200 * time.Millisecond}, {"payment", a.cfg.PaymentURL, 200 * time.Millisecond}} {
			status, body, hdr, err := a.call(r.Context(), dep.url, traceID, spanID, requestID, "", dep.timeout, nil)
			if err != nil {
				code, httpStatus := dependencyTransportFailure(dep.name, err)
				w.Header().Set(transport.FailureServiceHeader, dep.name)
				w.Header().Set(transport.ErrorCodeHeader, code)
				http.Error(w, code, httpStatus)
				return
			}
			if status >= 400 {
				copyFailureHeaders(w.Header(), hdr)
				w.WriteHeader(status)
				_, _ = w.Write(body)
				return
			}
		}

		msgID := a.nextID("msg")
		a.emit(model.Event{
			ID: a.nextID("event"), Source: model.SourceApplication, Kind: model.KindMessage,
			TraceID: traceID, SpanID: spanID, CorrelationID: requestID, Service: "order",
			Operation: "order_completed", Timestamp: time.Now().UTC(), Status: "ok",
			Attributes: map[string]string{"message.id": msgID, "message.action": "publish", "topic": "orders.completed"},
		})
		env := broker.Envelope{TraceID: traceID, ParentSpanID: spanID, RequestID: requestID, MessageID: msgID}
		payload, _ := json.Marshal(env)
		status, _, _, err := a.call(r.Context(), a.cfg.BrokerURL+"/publish", traceID, spanID, requestID, "", 200*time.Millisecond, payload)
		if err != nil || status >= 400 {
			w.Header().Set(transport.FailureServiceHeader, "broker")
			w.Header().Set(transport.ErrorCodeHeader, "broker_publish_failed")
			http.Error(w, "broker_publish_failed", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(w, `{"order_id":"o-1"}`)
	case "inventory":
		d := a.faults.Decide("inventory", "check")
		if d.Inject {
			attributes := map[string]string{
				"fault.kind":       string(d.Spec.Kind),
				"target.service":   d.Spec.TargetService,
				"target.operation": d.Spec.Operation,
				"seed":             strconv.FormatInt(d.Spec.Seed, 10),
			}
			if d.Delay > 0 {
				attributes["delay_us"] = strconv.FormatInt(d.Delay.Microseconds(), 10)
			}
			a.emit(model.Event{
				ID: a.nextID("event"), Source: model.SourceFault, Kind: model.KindFault,
				TraceID: traceID, CorrelationID: requestID, Service: "inventory", Operation: "check", Timestamp: time.Now().UTC(),
				Attributes: attributes,
			})
			switch d.Spec.Kind {
			case fault.Latency:
				time.Sleep(d.Delay)
			case fault.ConnectionReset:
				if err := resetTCPConnection(w); err != nil {
					w.Header().Set(transport.FailureServiceHeader, "inventory")
					w.Header().Set(transport.ErrorCodeHeader, "inventory_reset_injector_failed")
					http.Error(w, "inventory_reset_injector_failed", http.StatusInternalServerError)
				}
				return
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"available":true}`)
	case "pricing":
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"total_cents":4200}`)
	case "payment":
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"authorized":true}`)
	case "notification", "audit", "analytics":
		w.WriteHeader(http.StatusNoContent)
	default:
		http.Error(w, "unknown role", http.StatusInternalServerError)
	}
}

func dependencyTransportFailure(service string, err error) (string, int) {
	if errors.Is(err, context.DeadlineExceeded) {
		return service + "_timeout", http.StatusGatewayTimeout
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return service + "_timeout", http.StatusGatewayTimeout
	}
	if errors.Is(err, syscall.ECONNRESET) {
		return service + "_connection_reset", http.StatusBadGateway
	}
	return service + "_transport_error", http.StatusBadGateway
}

func resetTCPConnection(w http.ResponseWriter) error {
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		return errors.New("response writer does not support connection hijacking")
	}
	conn, _, err := hijacker.Hijack()
	if err != nil {
		return fmt.Errorf("hijack connection: %w", err)
	}
	if tcp, ok := conn.(*net.TCPConn); ok {
		if err := tcp.SetLinger(0); err != nil {
			_ = conn.Close()
			return fmt.Errorf("set TCP linger: %w", err)
		}
	}
	if err := conn.Close(); err != nil {
		return fmt.Errorf("close reset connection: %w", err)
	}
	return nil
}

func (a *App) proxy(w http.ResponseWriter, r *http.Request, target, spanID string, timeout time.Duration) {
	current, _ := r.Context().Value(spanKey{}).(spanContext)
	status, body, hdr, err := a.call(r.Context(), target, current.TraceID, spanID, current.RequestID, "", timeout, nil)
	if err != nil {
		code, httpStatus := dependencyTransportFailure(a.cfg.Role, err)
		w.Header().Set(transport.FailureServiceHeader, a.cfg.Role)
		w.Header().Set(transport.ErrorCodeHeader, code)
		http.Error(w, code, httpStatus)
		return
	}
	copyFailureHeaders(w.Header(), hdr)
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

func (a *App) call(ctx context.Context, target, traceID, parentSpanID, requestID, messageID string, timeout time.Duration, payload []byte) (int, []byte, http.Header, error) {
	if target == "" {
		return 0, nil, nil, fmt.Errorf("empty target")
	}
	if payload == nil {
		payload = []byte(`{}`)
	}
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, target, bytes.NewReader(payload))
	if err != nil {
		return 0, nil, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(transport.TraceParentHeader, tracecontext.Context{TraceID: traceID, SpanID: parentSpanID, Flags: 1}.String())
	req.Header.Set(transport.RequestIDHeader, requestID)
	if messageID != "" {
		req.Header.Set(transport.MessageIDHeader, messageID)
	}
	resp, err := a.client.Do(req)
	if err != nil {
		return 0, nil, nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, nil, resp.Header.Clone(), err
	}
	return resp.StatusCode, body, resp.Header.Clone(), nil
}

func (a *App) emit(e model.Event) {
	if a.exporter != nil {
		a.exporter.Emit(e)
	}
}

func (a *App) nextID(kind string) string {
	return fmt.Sprintf("%s-%s-%06d", a.prefix, kind, a.seq.Add(1))
}

func operationFor(role string) string {
	switch role {
	case "gateway":
		return "create_order"
	case "auth":
		return "authorize"
	case "account":
		return "load_account"
	case "order":
		return "create"
	case "inventory":
		return "check"
	case "pricing":
		return "quote"
	case "payment":
		return "authorize"
	case "notification", "audit", "analytics":
		return "consume_order_event"
	default:
		return "unknown"
	}
}

type statusWriter struct {
	http.ResponseWriter
	status   int
	hijacked bool
}

func (w *statusWriter) WriteHeader(code int) {
	if w.status != 0 {
		return
	}
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}
func (w *statusWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(p)
}
func (w *statusWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hijacker, ok := w.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, errors.New("underlying response writer does not support connection hijacking")
	}
	conn, rw, err := hijacker.Hijack()
	if err == nil {
		w.hijacked = true
	}
	return conn, rw, err
}

func copyFailureHeaders(dst, src http.Header) {
	if src == nil {
		return
	}
	for _, key := range []string{transport.FailureServiceHeader, transport.ErrorCodeHeader} {
		if v := src.Get(key); v != "" {
			dst.Set(key, v)
		}
	}
}
