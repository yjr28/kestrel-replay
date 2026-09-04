package graph

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/yjr28/kestrel-replay/internal/model"
)

const localizationConfidenceModel = "heuristic_v1"

// LocalizationCandidate is a ranked divergence hypothesis. ConfidenceScore is
// a deterministic ranking heuristic in [0,1], not a calibrated probability.
// ScoreBasis records every additive component so the ranking is auditable.
type LocalizationCandidate struct {
	Divergence
	ConfidenceScore float64  `json:"confidence_score"`
	ConfidenceModel string   `json:"confidence_model"`
	ScoreBasis      []string `json:"score_basis,omitempty"`
}

// RankDivergences compares application spans and returns deterministic,
// evidence-backed localization candidates. Failure-injector events are never
// consulted. terminalService, when present, is an externally observed outcome
// anchor and only boosts candidates for that service.
func RankDivergences(healthy, failing []model.Event, latencyThreshold time.Duration, terminalService string) []LocalizationCandidate {
	terminalService = strings.TrimSpace(terminalService)
	healthySpans, healthyAmbiguous := applicationSpanIndexWithAmbiguity(healthy)
	failingSpans, failingAmbiguous := applicationSpanIndexWithAmbiguity(failing)
	keys := make([]divergenceKey, 0, len(healthySpans)+len(failingSpans))
	seen := make(map[divergenceKey]struct{}, len(healthySpans)+len(failingSpans))
	for key := range healthySpans {
		seen[key] = struct{}{}
		keys = append(keys, key)
	}
	for key := range failingSpans {
		if _, ok := seen[key]; ok {
			continue
		}
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].service == keys[j].service {
			return keys[i].operation < keys[j].operation
		}
		return keys[i].service < keys[j].service
	})

	var candidates []LocalizationCandidate
	for _, key := range keys {
		if _, ambiguous := healthyAmbiguous[key]; ambiguous {
			continue
		}
		if _, ambiguous := failingAmbiguous[key]; ambiguous {
			continue
		}
		h, hok := healthySpans[key]
		f, fok := failingSpans[key]
		switch {
		case hok && fok:
			if hd, okH := durationOf(h); okH {
				if fd, okF := durationOf(f); okF {
					delta := fd - hd
					if delta < 0 {
						delta = -delta
					}
					if latencyThreshold > 0 && delta >= latencyThreshold {
						c := LocalizationCandidate{Divergence: Divergence{
							Service: key.service, Operation: key.operation, Reason: "latency_delta",
							HealthyValue: hd.String(), FailingValue: fd.String(), Delta: delta,
							HealthyEventID: h.ID, FailingEventID: f.ID,
						}}
						scoreCandidate(&c, terminalService, latencyThreshold)
						candidates = append(candidates, c)
					}
				}
			}
			if h.Status != f.Status {
				reason := "status_change"
				if terminalService != "" && key.service == terminalService {
					reason = "terminal_status_change"
				}
				c := LocalizationCandidate{Divergence: Divergence{
					Service: key.service, Operation: key.operation, Reason: reason,
					HealthyValue: h.Status, FailingValue: f.Status,
					HealthyEventID: h.ID, FailingEventID: f.ID,
				}}
				scoreCandidate(&c, terminalService, latencyThreshold)
				candidates = append(candidates, c)
			}
		case hok:
			c := LocalizationCandidate{Divergence: Divergence{
				Service: key.service, Operation: key.operation, Reason: "missing_span",
				HealthyValue: h.Status, FailingValue: "missing", HealthyEventID: h.ID,
			}}
			scoreCandidate(&c, terminalService, latencyThreshold)
			candidates = append(candidates, c)
		case fok:
			c := LocalizationCandidate{Divergence: Divergence{
				Service: key.service, Operation: key.operation, Reason: "unexpected_span",
				FailingValue: f.Status, FailingEventID: f.ID,
			}}
			scoreCandidate(&c, terminalService, latencyThreshold)
			candidates = append(candidates, c)
		}
	}

	sort.SliceStable(candidates, func(i, j int) bool {
		if candidates[i].ConfidenceScore != candidates[j].ConfidenceScore {
			return candidates[i].ConfidenceScore > candidates[j].ConfidenceScore
		}
		if candidates[i].Service != candidates[j].Service {
			return candidates[i].Service < candidates[j].Service
		}
		if candidates[i].Operation != candidates[j].Operation {
			return candidates[i].Operation < candidates[j].Operation
		}
		return candidates[i].Reason < candidates[j].Reason
	})
	return candidates
}

// TopKContains reports whether the expected service/operation pair appears in
// the first k ranked candidates. It is intended for deterministic corpus
// evaluation; it does not infer seeded truth from fault events.
func TopKContains(candidates []LocalizationCandidate, service, operation string, k int) bool {
	if k <= 0 {
		return false
	}
	if k > len(candidates) {
		k = len(candidates)
	}
	for i := 0; i < k; i++ {
		if candidates[i].Service == service && candidates[i].Operation == operation {
			return true
		}
	}
	return false
}

func applicationSpanIndex(events []model.Event) map[divergenceKey]model.Event {
	spans, _ := applicationSpanIndexWithAmbiguity(events)
	return spans
}

// applicationSpanIndexWithAmbiguity returns only application span keys backed
// by exactly one identified event. Duplicate service/operation keys are kept
// separately so callers can avoid treating an arbitrary retry/duplicate as
// authoritative evidence while retry semantics remain unmodeled. Event IDs
// reused by multiple otherwise eligible spans are excluded because they cannot
// name one exact supporting event.
func applicationSpanIndexWithAmbiguity(events []model.Event) (map[divergenceKey]model.Event, map[divergenceKey]struct{}) {
	spans := make(map[divergenceKey]model.Event)
	ambiguous := make(map[divergenceKey]struct{})
	eventIDCounts := make(map[string]int)
	for _, event := range events {
		if event.Kind == model.KindSpan && event.Source == model.SourceApplication && strings.TrimSpace(event.ID) != "" {
			eventIDCounts[event.ID]++
		}
	}
	for _, e := range model.Sorted(events) {
		if e.Kind != model.KindSpan || e.Source != model.SourceApplication || strings.TrimSpace(e.ID) == "" || eventIDCounts[e.ID] != 1 {
			continue
		}
		key := divergenceKey{service: e.Service, operation: e.Operation}
		if _, alreadyAmbiguous := ambiguous[key]; alreadyAmbiguous {
			continue
		}
		if _, exists := spans[key]; exists {
			delete(spans, key)
			ambiguous[key] = struct{}{}
			continue
		}
		spans[key] = e
	}
	return spans, ambiguous
}

func scoreCandidate(c *LocalizationCandidate, terminalService string, latencyThreshold time.Duration) {
	c.ConfidenceModel = localizationConfidenceModel
	base := 0.0
	switch c.Reason {
	case "latency_delta":
		base = 0.70
	case "missing_span":
		base = 0.66
	case "terminal_status_change":
		base = 0.64
	case "status_change":
		base = 0.58
	case "unexpected_span":
		base = 0.52
	default:
		base = 0.40
	}
	c.ConfidenceScore = base
	c.ScoreBasis = append(c.ScoreBasis, fmt.Sprintf("reason=%s:+%.2f", c.Reason, base))

	if terminalService != "" && c.Service == terminalService {
		c.ConfidenceScore += 0.20
		c.Anchor = "outcome.terminal_service=" + terminalService
		c.ScoreBasis = append(c.ScoreBasis, "terminal_service_anchor:+0.20")
	}
	if c.HealthyEventID != "" && c.FailingEventID != "" {
		c.ConfidenceScore += 0.03
		c.ScoreBasis = append(c.ScoreBasis, "paired_event_provenance:+0.03")
	} else if c.HealthyEventID != "" || c.FailingEventID != "" {
		c.ConfidenceScore += 0.01
		c.ScoreBasis = append(c.ScoreBasis, "single_event_provenance:+0.01")
	}
	if c.Reason == "latency_delta" && latencyThreshold > 0 && c.Delta > 0 {
		severity := float64(c.Delta) / float64(c.Delta+latencyThreshold)
		bonus := math.Min(0.06, 0.06*severity)
		c.ConfidenceScore += bonus
		c.ScoreBasis = append(c.ScoreBasis, fmt.Sprintf("latency_severity:+%.3f", bonus))
	}
	if c.ConfidenceScore > 0.99 {
		c.ConfidenceScore = 0.99
	}
	c.ConfidenceScore = math.Round(c.ConfidenceScore*1000) / 1000
}
