package corpus

import (
	"fmt"
	"sort"

	"github.com/yjr28/kestrel-replay/internal/graph"
)

type MessageTopologyTruth struct {
	Topic        string   `json:"topic"`
	Action       string   `json:"action"`
	Services     []string `json:"services"`
	FailingCount int      `json:"failing_count"`
}

func ExpectedMessageTopology(c Case) (MessageTopologyTruth, bool) {
	if c.ID != "orders-completed-duplicate" {
		return MessageTopologyTruth{}, false
	}
	return MessageTopologyTruth{
		Topic: Topic, Action: "consume",
		Services: []string{"analytics", "audit", "notification"},
		FailingCount: 2,
	}, true
}

// ValidateMessageTopology evaluates already-produced structural divergences
// against seeded corpus truth. The graph comparator does not receive this truth.
func ValidateMessageTopology(c Case, divergences []graph.MessageTopologyDivergence) error {
	truth, ok := ExpectedMessageTopology(c)
	if !ok {
		return nil
	}
	matched := make(map[string]bool, len(truth.Services))
	for _, divergence := range divergences {
		if divergence.Topic != truth.Topic || divergence.Action != truth.Action {
			continue
		}
		for _, service := range truth.Services {
			if divergence.Service != service {
				continue
			}
			if divergence.Reason != "count_above_healthy_range" {
				return fmt.Errorf("%s expected count_above_healthy_range, got %s", service, divergence.Reason)
			}
			if divergence.HealthyMax != 1 || divergence.FailingCount != truth.FailingCount || divergence.CountDelta != 1 {
				return fmt.Errorf("%s multiplicity mismatch healthy_max=%d failing=%d delta=%d", service, divergence.HealthyMax, divergence.FailingCount, divergence.CountDelta)
			}
			if len(divergence.FailingEventIDs) != truth.FailingCount {
				return fmt.Errorf("%s expected %d failing event ids, got %d", service, truth.FailingCount, len(divergence.FailingEventIDs))
			}
			matched[service] = true
		}
	}
	var missing []string
	for _, service := range truth.Services {
		if !matched[service] {
			missing = append(missing, service)
		}
	}
	if len(missing) != 0 {
		sort.Strings(missing)
		return fmt.Errorf("missing expected message topology divergences for %v", missing)
	}
	return nil
}
