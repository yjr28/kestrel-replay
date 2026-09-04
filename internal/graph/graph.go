package graph

import (
	"fmt"
	"strconv"
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
	Service      string        `json:"service"`
	Operation    string        `json:"operation"`
	Reason       string        `json:"reason"`
	HealthyValue string        `json:"healthy_value"`
	FailingValue string        `json:"failing_value"`
	Delta        time.Duration `json:"delta,omitempty"`
}

// EarliestMeaningfulDivergence compares application spans while deliberately
// ignoring explicit injector events. It first looks for a local latency anomaly
// because propagated parent errors are usually consequences, then falls back to
// the first status/topology change. This is intentionally conservative: it
// returns evidence, not a claim of perfect causal certainty.
func EarliestMeaningfulDivergence(healthy, failing []model.Event, latencyThreshold time.Duration) (Divergence, bool) {
	type key struct{ service, operation string }
	healthySpans := map[key]model.Event{}
	for _, e := range healthy {
		if e.Kind == model.KindSpan && e.Source == model.SourceApplication {
			healthySpans[key{e.Service, e.Operation}] = e
		}
	}

	var bestLatency Divergence
	var haveLatency bool
	for _, e := range failing {
		if e.Kind != model.KindSpan || e.Source != model.SourceApplication {
			continue
		}
		h, ok := healthySpans[key{e.Service, e.Operation}]
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
			bestLatency = Divergence{Service: e.Service, Operation: e.Operation, Reason: "latency_delta", HealthyValue: hd.String(), FailingValue: fd.String(), Delta: delta}
			haveLatency = true
		}
	}
	if haveLatency {
		return bestLatency, true
	}

	for _, e := range model.Sorted(failing) {
		if e.Kind != model.KindSpan || e.Source != model.SourceApplication {
			continue
		}
		h, ok := healthySpans[key{e.Service, e.Operation}]
		if !ok {
			return Divergence{Service: e.Service, Operation: e.Operation, Reason: "unexpected_span", FailingValue: e.Status}, true
		}
		if h.Status != e.Status {
			return Divergence{Service: e.Service, Operation: e.Operation, Reason: "status_change", HealthyValue: h.Status, FailingValue: e.Status}, true
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
