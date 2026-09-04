package collector

import (
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
	e.Sequence = s.sequence.Add(1)
	s.mu.Lock()
	s.events = append(s.events, e)
	s.mu.Unlock()
	return nil
}

func (s *Store) Snapshot(traceID string) []model.Event {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]model.Event, 0, len(s.events))
	for _, e := range s.events {
		if traceID == "" || e.TraceID == traceID {
			out = append(out, e)
		}
	}
	return out
}
