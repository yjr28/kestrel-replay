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
	switch e.Source {
	case SourceApplication, SourceKernel, SourceFault, SourceReplay:
	default:
		return fmt.Errorf("unsupported event source %q", e.Source)
	}
	switch e.Kind {
	case KindSpan:
		if strings.TrimSpace(e.TraceID) == "" || strings.TrimSpace(e.SpanID) == "" || strings.TrimSpace(e.Service) == "" || strings.TrimSpace(e.Operation) == "" {
			return errors.New("span events require trace_id, span_id, service, and operation")
		}
	case KindMessage:
		if strings.TrimSpace(e.TraceID) == "" || strings.TrimSpace(e.Attributes["message.id"]) == "" || strings.TrimSpace(e.Attributes["message.action"]) == "" {
			return errors.New("message events require trace_id, message.id, and message.action")
		}
		switch strings.TrimSpace(e.Attributes["message.action"]) {
		case "publish", "consume":
		default:
			return fmt.Errorf("unsupported message action %q", e.Attributes["message.action"])
		}
	case KindFault:
		if e.Source != SourceFault {
			return fmt.Errorf("fault events require source %q", SourceFault)
		}
		faultKind := strings.TrimSpace(e.Attributes["fault.kind"])
		targetService := strings.TrimSpace(e.Attributes["target.service"])
		targetOperation := strings.TrimSpace(e.Attributes["target.operation"])
		if faultKind == "" || targetService == "" {
			return errors.New("fault events require fault.kind and target.service")
		}
		if service := strings.TrimSpace(e.Service); service != "" && service != targetService {
			return fmt.Errorf("fault event service %q conflicts with target.service %q", e.Service, e.Attributes["target.service"])
		}
		if operation := strings.TrimSpace(e.Operation); operation != "" && targetOperation != "" && operation != targetOperation {
			return fmt.Errorf("fault event operation %q conflicts with target.operation %q", e.Operation, e.Attributes["target.operation"])
		}
		switch faultKind {
		case "latency", "connection_reset", "duplicate_message", "delayed_message":
			if targetOperation == "" {
				return fmt.Errorf("%s fault events require target.operation", faultKind)
			}
		case "service_crash":
		default:
			return fmt.Errorf("unsupported fault event kind %q", e.Attributes["fault.kind"])
		}
	case KindNetwork, KindProcess:
		// These event kinds currently have no kind-specific required fields.
	default:
		return fmt.Errorf("unsupported event kind %q", e.Kind)
	}
	return nil
}

// CanonicalKey deliberately excludes timestamps and generated IDs so executions can be compared.
func (e Event) CanonicalKey() string {
	return fmt.Sprintf("%s|%s|%s|%s", e.Kind, strings.TrimSpace(e.Service), strings.TrimSpace(e.Operation), strings.TrimSpace(e.Status))
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
