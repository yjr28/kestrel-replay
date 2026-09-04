package graph

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/yjr28/kestrel-replay/internal/model"
)

type EdgeKind string

const (
	EdgeParentSpan EdgeKind = "parent_span"
	EdgeMessage    EdgeKind = "message"
	EdgeFault      EdgeKind = "fault"
)

type Edge struct {
	From string   `json:"from"`
	To   string   `json:"to"`
	Kind EdgeKind `json:"kind"`
}

type Graph struct {
	Nodes map[string]model.Event `json:"nodes"`
	Edges []Edge                 `json:"edges"`
	Order []string               `json:"order"`
}

func Build(events []model.Event) (*Graph, error) {
	g := &Graph{Nodes: make(map[string]model.Event, len(events))}
	spans := map[string]string{}
	publishers := map[string]string{}
	faultsByService := map[string][]string{}

	for _, e := range model.Sorted(events) {
		if err := e.Validate(); err != nil {
			return nil, fmt.Errorf("event %q: %w", e.ID, err)
		}
		if _, exists := g.Nodes[e.ID]; exists {
			return nil, fmt.Errorf("duplicate event id %q", e.ID)
		}
		g.Nodes[e.ID] = e
		g.Order = append(g.Order, e.ID)
		if e.Kind == model.KindSpan {
			spans[e.SpanID] = e.ID
		}
		if e.Kind == model.KindMessage && e.Attributes["message.action"] == "publish" {
			publishers[e.Attributes["message.id"]] = e.ID
		}
		if e.Kind == model.KindFault {
			faultsByService[e.Attributes["target.service"]] = append(faultsByService[e.Attributes["target.service"]], e.ID)
		}
	}

	for _, id := range g.Order {
		e := g.Nodes[id]
		if e.Kind == model.KindSpan && e.ParentSpanID != "" {
			if parent, ok := spans[e.ParentSpanID]; ok {
				g.Edges = append(g.Edges, Edge{From: parent, To: id, Kind: EdgeParentSpan})
			}
		}
		if e.Kind == model.KindMessage && e.Attributes["message.action"] == "consume" {
			if publisher, ok := publishers[e.Attributes["message.id"]]; ok {
				g.Edges = append(g.Edges, Edge{From: publisher, To: id, Kind: EdgeMessage})
			}
		}
		if e.Kind == model.KindSpan {
			for _, faultID := range faultsByService[e.Service] {
				faultEvent := g.Nodes[faultID]
				if !faultEvent.Timestamp.After(e.Timestamp) {
					g.Edges = append(g.Edges, Edge{From: faultID, To: id, Kind: EdgeFault})
					break
				}
			}
		}
	}
	return g, nil
}

type Divergence struct {
	Service        string        `json:"service"`
	Operation      string        `json:"operation"`
	Reason         string        `json:"reason"`
	HealthyValue   string        `json:"healthy_value"`
	FailingValue   string        `json:"failing_value"`
	Delta          time.Duration `json:"delta,omitempty"`
	HealthyEventID string        `json:"healthy_event_id,omitempty"`
	FailingEventID string        `json:"failing_event_id,omitempty"`
	Anchor         string        `json:"anchor,omitempty"`
}

type divergenceKey struct {
	service   string
	operation string
}

// EarliestMeaningfulDivergence compares application spans while deliberately
// ignoring explicit injector events. It first looks for a local latency anomaly
// because propagated parent errors are usually consequences, then falls back to
// status/topology changes. Returned event IDs make the selected evidence
// auditable without changing the conservative selection semantics.
func EarliestMeaningfulDivergence(healthy, failing []model.Event, latencyThreshold time.Duration) (Divergence, bool) {
	return earliestMeaningfulDivergence(healthy, failing, latencyThreshold, "")
}

// EarliestMeaningfulDivergenceForTerminalService uses the externally observed
// terminal service as an application-evidence anchor after checking for a local
// latency anomaly. This is useful when a crash removes the target service span
// entirely. It does not inspect failure-injector events.
func EarliestMeaningfulDivergenceForTerminalService(healthy, failing []model.Event, latencyThreshold time.Duration, terminalService string) (Divergence, bool) {
	return earliestMeaningfulDivergence(healthy, failing, latencyThreshold, strings.TrimSpace(terminalService))
}

func earliestMeaningfulDivergence(healthy, failing []model.Event, latencyThreshold time.Duration, terminalService string) (Divergence, bool) {
	healthySpans := make(map[divergenceKey]model.Event)
	failingSpans := make(map[divergenceKey]model.Event)
	for _, e := range healthy {
		if e.Kind == model.KindSpan && e.Source == model.SourceApplication {
			healthySpans[divergenceKey{service: e.Service, operation: e.Operation}] = e
		}
	}
	for _, e := range failing {
		if e.Kind == model.KindSpan && e.Source == model.SourceApplication {
			failingSpans[divergenceKey{service: e.Service, operation: e.Operation}] = e
		}
	}

	var bestLatency Divergence
	var haveLatency bool
	for key, e := range failingSpans {
		h, ok := healthySpans[key]
		if !ok {
			continue
		}
		hd, hok := durationOf(h)
		fd, fok := durationOf(e)
		if !hok || !fok {
			continue
		}
		delta := fd - hd
		if delta < 0 {
			delta = -delta
		}
		if delta >= latencyThreshold && (!haveLatency || delta > bestLatency.Delta) {
			bestLatency = Divergence{
				Service: e.Service, Operation: e.Operation, Reason: "latency_delta",
				HealthyValue: hd.String(), FailingValue: fd.String(), Delta: delta,
				HealthyEventID: h.ID, FailingEventID: e.ID,
			}
			haveLatency = true
		}
	}
	if haveLatency {
		return bestLatency, true
	}

	if terminalService != "" {
		anchor := "outcome.terminal_service=" + terminalService
		for _, h := range model.Sorted(healthy) {
			if h.Kind != model.KindSpan || h.Source != model.SourceApplication || h.Service != terminalService {
				continue
			}
			key := divergenceKey{service: h.Service, operation: h.Operation}
			f, ok := failingSpans[key]
			if !ok {
				return Divergence{
					Service: h.Service, Operation: h.Operation, Reason: "missing_span",
					HealthyValue: h.Status, FailingValue: "missing", HealthyEventID: h.ID, Anchor: anchor,
				}, true
			}
			if h.Status != f.Status {
				return Divergence{
					Service: h.Service, Operation: h.Operation, Reason: "terminal_status_change",
					HealthyValue: h.Status, FailingValue: f.Status,
					HealthyEventID: h.ID, FailingEventID: f.ID, Anchor: anchor,
				}, true
			}
		}
	}

	for _, e := range model.Sorted(failing) {
		if e.Kind != model.KindSpan || e.Source != model.SourceApplication {
			continue
		}
		key := divergenceKey{service: e.Service, operation: e.Operation}
		h, ok := healthySpans[key]
		if !ok {
			return Divergence{
				Service: e.Service, Operation: e.Operation, Reason: "unexpected_span",
				FailingValue: e.Status, FailingEventID: e.ID,
			}, true
		}
		if h.Status != e.Status {
			return Divergence{
				Service: e.Service, Operation: e.Operation, Reason: "status_change",
				HealthyValue: h.Status, FailingValue: e.Status,
				HealthyEventID: h.ID, FailingEventID: e.ID,
			}, true
		}
	}

	for _, h := range model.Sorted(healthy) {
		if h.Kind != model.KindSpan || h.Source != model.SourceApplication {
			continue
		}
		key := divergenceKey{service: h.Service, operation: h.Operation}
		if _, ok := failingSpans[key]; !ok {
			return Divergence{
				Service: h.Service, Operation: h.Operation, Reason: "missing_span",
				HealthyValue: h.Status, FailingValue: "missing", HealthyEventID: h.ID,
			}, true
		}
	}
	return Divergence{}, false
}

func durationOf(e model.Event) (time.Duration, bool) {
	raw := e.Attributes["duration_us"]
	if raw == "" {
		return 0, false
	}
	us, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, false
	}
	return time.Duration(us) * time.Microsecond, true
}
