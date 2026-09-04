package demo

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yjr28/kestrel-replay/internal/fault"
	"github.com/yjr28/kestrel-replay/internal/model"
	"github.com/yjr28/kestrel-replay/internal/replay"
)

const (
	traceHeader      = "X-Kestrel-Trace-ID"
	parentSpanHeader = "X-Kestrel-Parent-Span-ID"
	failureSvcHeader = "X-Kestrel-Failure-Service"
	errorCodeHeader  = "X-Kestrel-Error-Code"
	requestIDHeader  = "X-Kestrel-Request-ID"
)

type Recorder struct {
	mu       sync.Mutex
	events   []model.Event
	seq      atomic.Uint64
	spanSeq  atomic.Uint64
	traceSeq atomic.Uint64
	msgSeq   atomic.Uint64
}

func (r *Recorder) TraceID() string   { return fmt.Sprintf("trace-%06d", r.traceSeq.Add(1)) }
func (r *Recorder) SpanID() string    { return fmt.Sprintf("span-%06d", r.spanSeq.Add(1)) }
func (r *Recorder) MessageID() string { return fmt.Sprintf("msg-%06d", r.msgSeq.Add(1)) }

func (r *Recorder) Add(e model.Event) {
	e.Sequence = r.seq.Add(1)
	if e.ID == "" {
		e.ID = fmt.Sprintf("event-%06d", e.Sequence)
	}
	if e.Timestamp.IsZero() {
		e.Timestamp = time.Now().UTC()
	}
	r.mu.Lock()
	r.events = append(r.events, e)
	r.mu.Unlock()
}

func (r *Recorder) Events() []model.Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]model.Event(nil), r.events...)
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	if w.status == 0 {
		w.status = code
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(p)
}

type System struct {
	recorder *Recorder
	faults   *fault.Controller
	servers  []*httptest.Server
	client   *http.Client
	asyncWG  sync.WaitGroup

	gatewayURL string
	workerURLs map[string]string
}

func NewSystem(specs []fault.Spec) (*System, error) {
	controller, err := fault.NewController(specs)
	if err != nil {
		return nil, err
	}
	s := &System{
		recorder:   &Recorder{},
		faults:     controller,
		client:     &http.Client{Timeout: 250 * time.Millisecond},
		workerURLs: map[string]string{},
	}

	// Leaf asynchronous workers.
	for _, name := range []string{"notification", "audit", "analytics"} {
		srv := s.newServer(name, "consume_order_event", func(w http.ResponseWriter, r *http.Request, spanID string) {
			w.WriteHeader(http.StatusNoContent)
		})
		s.workerURLs[name] = srv.URL
	}

	// Leaf synchronous services.
	inventory := s.newServer("inventory", "check", func(w http.ResponseWriter, r *http.Request, spanID string) {
		d := s.faults.Decide("inventory", "check")
		if d.Inject {
			s.recorder.Add(model.Event{
				Source: model.SourceFault, Kind: model.KindFault, TraceID: r.Header.Get(traceHeader),
				CorrelationID: r.Header.Get(requestIDHeader), Service: "inventory", Operation: "check",
				Attributes: map[string]string{
					"fault.kind": string(d.Spec.Kind), "target.service": d.Spec.TargetService,
					"target.operation": d.Spec.Operation, "seed": strconv.FormatInt(d.Spec.Seed, 10),
					"delay_us": strconv.FormatInt(d.Delay.Microseconds(), 10),
				},
			})
			if d.Spec.Kind == fault.Latency {
				time.Sleep(d.Delay)
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"available":true}`)
	})
	pricing := s.newServer("pricing", "quote", func(w http.ResponseWriter, r *http.Request, spanID string) {
		_, _ = io.WriteString(w, `{"total_cents":4200}`)
	})
	payment := s.newServer("payment", "authorize", func(w http.ResponseWriter, r *http.Request, spanID string) {
		_, _ = io.WriteString(w, `{"authorized":true}`)
	})

	order := s.newServer("order", "create", func(w http.ResponseWriter, r *http.Request, spanID string) {
		traceID := r.Header.Get(traceHeader)
		reqID := r.Header.Get(requestIDHeader)
		for _, dep := range []struct{ name, url string }{{"inventory", inventory.URL}, {"pricing", pricing.URL}, {"payment", payment.URL}} {
			status, body, hdr, err := s.callWithTimeout(r.Context(), dep.url, traceID, spanID, reqID, 30*time.Millisecond)
			if err != nil {
				w.Header().Set(failureSvcHeader, dep.name)
				w.Header().Set(errorCodeHeader, dep.name+"_timeout")
				http.Error(w, dep.name+"_timeout", http.StatusGatewayTimeout)
				return
			}
			if status >= 400 {
				copyFailureHeaders(w.Header(), hdr)
				w.WriteHeader(status)
				_, _ = w.Write(body)
				return
			}
		}

		msgID := s.recorder.MessageID()
		s.recorder.Add(model.Event{
			Source: model.SourceApplication, Kind: model.KindMessage, TraceID: traceID, SpanID: spanID,
			CorrelationID: reqID, Service: "order", Operation: "order_completed", Status: "ok",
			Attributes: map[string]string{"message.id": msgID, "message.action": "publish", "topic": "orders.completed"},
		})
		s.asyncWG.Add(1)
		go func() {
			defer s.asyncWG.Done()
			for _, worker := range []string{"notification", "audit", "analytics"} {
				s.recorder.Add(model.Event{
					Source: model.SourceApplication, Kind: model.KindMessage, TraceID: traceID, ParentSpanID: spanID,
					CorrelationID: reqID, Service: worker, Operation: "order_completed", Status: "ok",
					Attributes: map[string]string{"message.id": msgID, "message.action": "consume", "topic": "orders.completed"},
				})
				_, _, _, _ = s.call(context.Background(), s.workerURLs[worker], traceID, spanID, reqID)
			}
		}()
		w.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(w, `{"order_id":"o-1"}`)
	})

	account := s.newProxyServer("account", "load_account", order.URL)
	auth := s.newProxyServer("auth", "authorize", account.URL)
	gateway := s.newProxyServer("gateway", "create_order", auth.URL)
	s.gatewayURL = gateway.URL
	return s, nil
}

func (s *System) newProxyServer(service, operation, next string) *httptest.Server {
	return s.newServer(service, operation, func(w http.ResponseWriter, r *http.Request, spanID string) {
		status, body, hdr, err := s.call(r.Context(), next, r.Header.Get(traceHeader), spanID, r.Header.Get(requestIDHeader))
		if err != nil {
			w.Header().Set(failureSvcHeader, service)
			w.Header().Set(errorCodeHeader, service+"_downstream_timeout")
			http.Error(w, service+"_downstream_timeout", http.StatusGatewayTimeout)
			return
		}
		copyFailureHeaders(w.Header(), hdr)
		w.WriteHeader(status)
		_, _ = w.Write(body)
	})
}

func (s *System) newServer(service, operation string, fn func(http.ResponseWriter, *http.Request, string)) *httptest.Server {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now().UTC()
		traceID := r.Header.Get(traceHeader)
		if traceID == "" {
			traceID = s.recorder.TraceID()
			r.Header.Set(traceHeader, traceID)
		}
		reqID := r.Header.Get(requestIDHeader)
		if reqID == "" {
			reqID = "req-1"
			r.Header.Set(requestIDHeader, reqID)
		}
		parent := r.Header.Get(parentSpanHeader)
		spanID := s.recorder.SpanID()
		sw := &statusWriter{ResponseWriter: w}
		fn(sw, r, spanID)
		if sw.status == 0 {
			sw.status = http.StatusOK
		}
		status := "ok"
		if sw.status >= 400 {
			status = "error"
		}
		s.recorder.Add(model.Event{
			Source: model.SourceApplication, Kind: model.KindSpan, TraceID: traceID, SpanID: spanID,
			ParentSpanID: parent, CorrelationID: reqID, Service: service, Operation: operation,
			Timestamp: start, Status: status,
			Attributes: map[string]string{"http.status_code": strconv.Itoa(sw.status), "duration_us": strconv.FormatInt(time.Since(start).Microseconds(), 10)},
		})
	})
	srv := httptest.NewServer(h)
	s.servers = append(s.servers, srv)
	return srv
}

func (s *System) callWithTimeout(ctx context.Context, url, traceID, parentSpanID, requestID string, timeout time.Duration) (int, []byte, http.Header, error) {
	client := &http.Client{Timeout: timeout}
	return s.callWithClient(ctx, client, url, traceID, parentSpanID, requestID)
}

func (s *System) call(ctx context.Context, url, traceID, parentSpanID, requestID string) (int, []byte, http.Header, error) {
	return s.callWithClient(ctx, s.client, url, traceID, parentSpanID, requestID)
}

func (s *System) callWithClient(ctx context.Context, client *http.Client, url, traceID, parentSpanID, requestID string) (int, []byte, http.Header, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader([]byte(`{}`)))
	if err != nil {
		return 0, nil, nil, err
	}
	req.Header.Set(traceHeader, traceID)
	req.Header.Set(parentSpanHeader, parentSpanID)
	req.Header.Set(requestIDHeader, requestID)
	resp, err := client.Do(req)
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

func copyFailureHeaders(dst, src http.Header) {
	if src == nil {
		return
	}
	for _, key := range []string{failureSvcHeader, errorCodeHeader} {
		if v := src.Get(key); v != "" {
			dst.Set(key, v)
		}
	}
}

func (s *System) Close() {
	s.asyncWG.Wait()
	for i := len(s.servers) - 1; i >= 0; i-- {
		s.servers[i].Close()
	}
}

type Result struct {
	Events  []model.Event
	Outcome replay.OutcomeSignature
}

func RunScenario(specs []fault.Spec) (Result, error) {
	s, err := NewSystem(specs)
	if err != nil {
		return Result{}, err
	}
	defer s.Close()

	req, err := http.NewRequest(http.MethodPost, s.gatewayURL, strings.NewReader(`{"sku":"demo"}`))
	if err != nil {
		return Result{}, err
	}
	req.Header.Set(requestIDHeader, "req-demo-1")
	resp, err := (&http.Client{Timeout: 2 * time.Second}).Do(req)
	if err != nil {
		return Result{}, err
	}
	body, readErr := io.ReadAll(resp.Body)
	resp.Body.Close()
	if readErr != nil {
		return Result{}, readErr
	}

	s.asyncWG.Wait()
	// A timed-out inventory handler may still be completing after the caller returned.
	time.Sleep(90 * time.Millisecond)

	outcome := replay.OutcomeSignature{HTTPStatus: resp.StatusCode}
	if resp.StatusCode < 400 {
		outcome.Classification = "success"
		outcome.CausalPath = []string{"gateway", "auth", "account", "order", "inventory", "pricing", "payment"}
	} else {
		outcome.Classification = "distributed_failure"
		outcome.TerminalService = resp.Header.Get(failureSvcHeader)
		outcome.ErrorCode = resp.Header.Get(errorCodeHeader)
		if outcome.ErrorCode == "" {
			outcome.ErrorCode = strings.TrimSpace(string(body))
		}
		outcome.CausalPath = []string{"gateway", "auth", "account", "order", outcome.TerminalService}
	}
	return Result{Events: s.recorder.Events(), Outcome: outcome}, nil
}
