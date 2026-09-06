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

type spanIdentity struct {
	traceID string
	spanID  string
}

func Build(events []model.Event) (*Graph, error) {
	g := &Graph{Nodes: make(map[string]model.Event, len(events))}
	spans := map[spanIdentity]string{}
	publishers := map[string]string{}
	ambiguousPublishers := map[string]struct{}{}
	faultsByService := map[string][]string{}

	for _, e := range model.Sorted(events) {
		if err := e.Validate(); err != nil {
			return nil, fmt.Errorf("event %q: %w", e.ID, err)
		}
		e.ID = strings.TrimSpace(e.ID)
		if _, exists := g.Nodes[e.ID]; exists {
			return nil, fmt.Errorf("duplicate event id %q", e.ID)
		}
		g.Nodes[e.ID] = e
		g.Order = append(g.Order, e.ID)
		if e.Kind == model.KindSpan {
			identity := spanIdentity{traceID: strings.TrimSpace(e.TraceID), spanID: strings.TrimSpace(e.SpanID)}
			if existing, exists := spans[identity]; exists {
				return nil, fmt.Errorf("duplicate span identity trace_id=%q span_id=%q in events %q and %q", identity.traceID, identity.spanID, existing, e.ID)
			}
			spans[identity] = e.ID
		}
		if e.Kind == model.KindMessage && strings.TrimSpace(e.Attributes["message.action"]) == "publish" {
			messageID := strings.TrimSpace(e.Attributes["message.id"])
			if _, ambiguous := ambiguousPublishers[messageID]; ambiguous {
				continue
			}
			if _, exists := publishers[messageID]; exists {
				delete(publishers, messageID)
				ambiguousPublishers[messageID] = struct{}{}
				continue
			}
			publishers[messageID] = e.ID
		}
		if e.Kind == model.KindFault {
			targetService := strings.TrimSpace(e.Attributes["target.service"])
			faultsByService[targetService] = append(faultsByService[targetService], e.ID)
		}
	}

	for _, id := range g.Order {
		e := g.Nodes[id]
		if e.Kind == model.KindSpan && strings.TrimSpace(e.ParentSpanID) != "" {
			parentIdentity := spanIdentity{traceID: strings.TrimSpace(e.TraceID), spanID: strings.TrimSpace(e.ParentSpanID)}
			if parent, ok := spans[parentIdentity]; ok {
				g.Edges = append(g.Edges, Edge{From: parent, To: id, Kind: EdgeParentSpan})
			}
		}
		if e.Kind == model.KindMessage && strings.TrimSpace(e.Attributes["message.action"]) == "consume" {
			messageID := strings.TrimSpace(e.Attributes["message.id"])
			if publisher, ok := publishers[messageID]; ok {
				g.Edges = append(g.Edges, Edge{From: publisher, To: id, Kind: EdgeMessage})
			}
		}
		if e.Kind == model.KindSpan {
			for _, faultID := range faultsByService[strings.TrimSpace(e.Service)] {
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
	healthySpans, healthyAmbiguous := applicationSpanIndexWithAmbiguity(healthy)
	failingSpans, failingAmbiguous := applicationSpanIndexWithAmbiguity(failing)

	var bestLatency Divergence
	var haveLatency bool
	for key, e := range failingSpans {
		if _, ambiguous := healthyAmbiguous[key]; ambiguous {
			continue
		}
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
		if latencyThreshold > 0 && delta >= latencyThreshold && (!haveLatency || delta > bestLatency.Delta) {
			bestLatency = Divergence{
				Service: key.service, Operation: key.operation, Reason: "latency_delta",
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
			if h.Kind != model.KindSpan || h.Source != model.SourceApplication {
				continue
			}
			key := divergenceKey{service: strings.TrimSpace(h.Service), operation: strings.TrimSpace(h.Operation)}
			if key.service != terminalService {
				continue
			}
			indexedHealthy, eligible := healthySpans[key]
			if !eligible || indexedHealthy.ID != h.ID {
				continue
			}
			if _, ambiguous := healthyAmbiguous[key]; ambiguous {
				continue
			}
			if _, ambiguous := failingAmbiguous[key]; ambiguous {
				continue
			}
			f, ok := failingSpans[key]
			if !ok {
				return Divergence{
					Service: key.service, Operation: key.operation, Reason: "missing_span",
					HealthyValue: h.Status, FailingValue: "missing", HealthyEventID: h.ID, Anchor: anchor,
				}, true
			}
			healthyStatus := strings.TrimSpace(h.Status)
			failingStatus := strings.TrimSpace(f.Status)
			if healthyStatus != "" && failingStatus != "" && healthyStatus != failingStatus {
				return Divergence{
					Service: key.service, Operation: key.operation, Reason: "terminal_status_change",
					HealthyValue: healthyStatus, FailingValue: failingStatus,
					HealthyEventID: h.ID, FailingEventID: f.ID, Anchor: anchor,
				}, true
			}
		}
	}

	for _, e := range model.Sorted(failing) {
		if e.Kind != model.KindSpan || e.Source != model.SourceApplication {
			continue
		}
		key := divergenceKey{service: strings.TrimSpace(e.Service), operation: strings.TrimSpace(e.Operation)}
		indexedFailing, eligible := failingSpans[key]
		if !eligible || indexedFailing.ID != e.ID {
			continue
		}
		if _, ambiguous := failingAmbiguous[key]; ambiguous {
			continue
		}
		if _, ambiguous := healthyAmbiguous[key]; ambiguous {
			continue
		}
		h, ok := healthySpans[key]
		if !ok {
			return Divergence{
				Service: key.service, Operation: key.operation, Reason: "unexpected_span",
				FailingValue: e.Status, FailingEventID: e.ID,
			}, true
		}
		healthyStatus := strings.TrimSpace(h.Status)
		failingStatus := strings.TrimSpace(e.Status)
		if healthyStatus != "" && failingStatus != "" && healthyStatus != failingStatus {
			return Divergence{
				Service: key.service, Operation: key.operation, Reason: "status_change",
				HealthyValue: healthyStatus, FailingValue: failingStatus,
				HealthyEventID: h.ID, FailingEventID: e.ID,
			}, true
		}
	}

	for _, h := range model.Sorted(healthy) {
		if h.Kind != model.KindSpan || h.Source != model.SourceApplication {
			continue
		}
		key := divergenceKey{service: strings.TrimSpace(h.Service), operation: strings.TrimSpace(h.Operation)}
		indexedHealthy, eligible := healthySpans[key]
		if !eligible || indexedHealthy.ID != h.ID {
			continue
		}
		if _, ambiguous := healthyAmbiguous[key]; ambiguous {
			continue
		}
		if _, ambiguous := failingAmbiguous[key]; ambiguous {
			continue
		}
		if _, ok := failingSpans[key]; !ok {
			return Divergence{
				Service: key.service, Operation: key.operation, Reason: "missing_span",
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
	if err != nil || us < 0 {
		return 0, false
	}
	maxDurationUS := int64(^uint64(0)>>1) / int64(time.Microsecond)
	if us > maxDurationUS {
		return 0, false
	}
	return time.Duration(us) * time.Microsecond, true
}
