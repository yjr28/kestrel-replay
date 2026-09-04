package model

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

type Source string

const (
	SourceApplication Source = "application"
	SourceKernel      Source = "kernel"
	SourceFault       Source = "failure_injector"
	SourceReplay      Source = "replay"
)

type Kind string

const (
	KindSpan    Kind = "span"
	KindMessage Kind = "message"
	KindFault   Kind = "fault"
	KindNetwork Kind = "network"
	KindProcess Kind = "process"
)

type Event struct {
	ID            string            `json:"id"`
	Sequence      uint64            `json:"sequence"`
	Source        Source            `json:"source"`
	Kind          Kind              `json:"kind"`
	TraceID       string            `json:"trace_id,omitempty"`
	SpanID        string            `json:"span_id,omitempty"`
	ParentSpanID  string            `json:"parent_span_id,omitempty"`
	CorrelationID string            `json:"correlation_id,omitempty"`
	Service       string            `json:"service,omitempty"`
	Operation     string            `json:"operation,omitempty"`
	Timestamp     time.Time         `json:"timestamp"`
	Status        string            `json:"status,omitempty"`
	Attributes    map[string]string `json:"attributes,omitempty"`
}

func (e Event) Validate() error {
	if strings.TrimSpace(e.ID) == "" {
		return errors.New("event id is required")
	}
	if e.Timestamp.IsZero() {
		return errors.New("event timestamp is required")
	}
	switch e.Kind {
	case KindSpan:
		if e.TraceID == "" || e.SpanID == "" || e.Service == "" || e.Operation == "" {
			return errors.New("span events require trace_id, span_id, service, and operation")
		}
	case KindMessage:
		if e.TraceID == "" || e.Attributes["message.id"] == "" || e.Attributes["message.action"] == "" {
			return errors.New("message events require trace_id, message.id, and message.action")
		}
	case KindFault:
		if e.Attributes["fault.kind"] == "" || e.Attributes["target.service"] == "" {
			return errors.New("fault events require fault.kind and target.service")
		}
	}
	return nil
}

// CanonicalKey deliberately excludes timestamps and generated IDs so executions can be compared.
func (e Event) CanonicalKey() string {
	return fmt.Sprintf("%s|%s|%s|%s", e.Kind, e.Service, e.Operation, e.Status)
}

func Sorted(events []Event) []Event {
	out := append([]Event(nil), events...)
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Timestamp.Equal(out[j].Timestamp) {
			if out[i].Sequence == out[j].Sequence {
				return out[i].ID < out[j].ID
			}
			return out[i].Sequence < out[j].Sequence
		}
		return out[i].Timestamp.Before(out[j].Timestamp)
	})
	return out
}
