package collector

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yjr28/kestrel-replay/internal/model"
)

const maxEventBodyBytes = 1 << 20

type Config struct {
	QueueCapacity int
	ProcessDelay  time.Duration // test/debug hook; production should leave zero.
}

func (c Config) normalized() Config {
	if c.QueueCapacity <= 0 {
		c.QueueCapacity = 4096
	}
	return c
}

type Stats struct {
	Accepted          uint64 `json:"accepted"`
	Stored            uint64 `json:"stored"`
	Dropped           uint64 `json:"dropped"`
	Invalid           uint64 `json:"invalid"`
	QueueDepth        int    `json:"queue_depth"`
	QueueCapacity     int    `json:"queue_capacity"`
	StoreLatencyTotal uint64 `json:"store_latency_us_total"`
}

type Server struct {
	cfg   Config
	store *Store
	queue chan model.Event

	accepted  atomic.Uint64
	stored    atomic.Uint64
	dropped   atomic.Uint64
	invalid   atomic.Uint64
	latencyUS atomic.Uint64

	done      chan struct{}
	closeOnce sync.Once
	workerWG  sync.WaitGroup
}

func New(cfg Config) *Server {
	cfg = cfg.normalized()
	s := &Server{
		cfg: cfg, store: &Store{}, queue: make(chan model.Event, cfg.QueueCapacity), done: make(chan struct{}),
	}
	s.workerWG.Add(1)
	go s.worker()
	return s
}

func (s *Server) Close() {
	s.closeOnce.Do(func() {
		close(s.done)
		s.workerWG.Wait()
	})
}

func (s *Server) worker() {
	defer s.workerWG.Done()
	for {
		select {
		case e := <-s.queue:
			started := time.Now()
			if s.cfg.ProcessDelay > 0 {
				time.Sleep(s.cfg.ProcessDelay)
			}
			if err := s.store.Add(e); err == nil {
				s.stored.Add(1)
			} else {
				s.invalid.Add(1)
			}
			s.latencyUS.Add(uint64(time.Since(started).Microseconds()))
		case <-s.done:
			for {
				select {
				case e := <-s.queue:
					if err := s.store.Add(e); err == nil {
						s.stored.Add(1)
					} else {
						s.invalid.Add(1)
					}
				default:
					return
				}
			}
		}
	}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/events", s.handleEvents)
	mux.HandleFunc("/v1/stats", s.handleStats)
	mux.HandleFunc("/metrics", s.handleMetrics)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	return mux
}

func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		s.ingest(w, r)
	case http.MethodGet:
		s.readEvents(w, r)
	default:
		w.Header().Set("Allow", "GET, POST")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) ingest(w http.ResponseWriter, r *http.Request) {
	body := http.MaxBytesReader(w, r.Body, maxEventBodyBytes)
	dec := json.NewDecoder(body)
	dec.DisallowUnknownFields()
	var e model.Event
	if err := dec.Decode(&e); err != nil {
		s.invalid.Add(1)
		http.Error(w, "invalid event: "+err.Error(), http.StatusBadRequest)
		return
	}
	if err := ensureEOF(dec); err != nil {
		s.invalid.Add(1)
		http.Error(w, "invalid event: "+err.Error(), http.StatusBadRequest)
		return
	}
	if err := e.Validate(); err != nil {
		s.invalid.Add(1)
		http.Error(w, "invalid event: "+err.Error(), http.StatusBadRequest)
		return
	}

	select {
	case s.queue <- e:
		s.accepted.Add(1)
		w.WriteHeader(http.StatusAccepted)
	default:
		s.dropped.Add(1)
		w.Header().Set("Retry-After", "1")
		http.Error(w, "collector queue full", http.StatusTooManyRequests)
	}
}

func ensureEOF(dec *json.Decoder) error {
	var extra any
	err := dec.Decode(&extra)
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err == nil {
		return errors.New("multiple JSON values")
	}
	return err
}

func (s *Server) readEvents(w http.ResponseWriter, r *http.Request) {
	events := s.store.Snapshot(r.URL.Query().Get("trace_id"))
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(events)
}

func (s *Server) Stats() Stats {
	return Stats{
		Accepted: s.accepted.Load(), Stored: s.stored.Load(), Dropped: s.dropped.Load(), Invalid: s.invalid.Load(),
		QueueDepth: len(s.queue), QueueCapacity: cap(s.queue), StoreLatencyTotal: s.latencyUS.Load(),
	}
}

func (s *Server) handleStats(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(s.Stats())
}

func (s *Server) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	stats := s.Stats()
	metrics := map[string]uint64{
		"kestrel_collector_events_accepted_total":  stats.Accepted,
		"kestrel_collector_events_stored_total":    stats.Stored,
		"kestrel_collector_events_dropped_total":   stats.Dropped,
		"kestrel_collector_events_invalid_total":   stats.Invalid,
		"kestrel_collector_queue_depth":            uint64(stats.QueueDepth),
		"kestrel_collector_queue_capacity":         uint64(stats.QueueCapacity),
		"kestrel_collector_store_latency_us_total": stats.StoreLatencyTotal,
	}
	keys := make([]string, 0, len(metrics))
	for k := range metrics {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	for _, k := range keys {
		_, _ = fmt.Fprintf(w, "%s %s\n", k, strconv.FormatUint(metrics[k], 10))
	}
}
