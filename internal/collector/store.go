package collector

import (
	"strings"
	"sync"
	"sync/atomic"

	"github.com/yjr28/kestrel-replay/internal/model"
)

type Store struct {
	mu       sync.RWMutex
	events   []model.Event
	sequence atomic.Uint64
}

func (s *Store) Add(e model.Event) error {
	if err := e.Validate(); err != nil {
		return err
	}
	e = canonicalizeEvidence(e)
	e.Sequence = s.sequence.Add(1)
	s.mu.Lock()
	s.events = append(s.events, e)
	s.mu.Unlock()
	return nil
}

func canonicalizeEvidence(e model.Event) model.Event {
	e.ID = strings.TrimSpace(e.ID)
	e.TraceID = strings.TrimSpace(e.TraceID)
	e.SpanID = strings.TrimSpace(e.SpanID)
	e.ParentSpanID = strings.TrimSpace(e.ParentSpanID)
	e.CorrelationID = strings.TrimSpace(e.CorrelationID)
	e.Service = strings.TrimSpace(e.Service)
	e.Operation = strings.TrimSpace(e.Operation)
	e.Status = strings.TrimSpace(e.Status)

	if e.Attributes != nil {
		attrs := make(map[string]string, len(e.Attributes))
		for key, value := range e.Attributes {
			attrs[key] = value
		}
		for _, key := range []string{"message.id", "message.action", "fault.kind", "target.service", "target.operation"} {
			if value, ok := attrs[key]; ok {
				attrs[key] = strings.TrimSpace(value)
			}
		}
		e.Attributes = attrs
	}
	return e
}

func cloneEvent(e model.Event) model.Event {
	if e.Attributes == nil {
		return e
	}
	attrs := make(map[string]string, len(e.Attributes))
	for key, value := range e.Attributes {
		attrs[key] = value
	}
	e.Attributes = attrs
	return e
}

func (s *Store) Snapshot(traceID string) []model.Event {
	traceID = strings.TrimSpace(traceID)
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]model.Event, 0, len(s.events))
	for _, e := range s.events {
		if traceID == "" || e.TraceID == traceID {
			out = append(out, cloneEvent(e))
		}
	}
	return out
}
