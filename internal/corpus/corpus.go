package corpus

import (
	"fmt"
	"net/http"
	"sort"
	"time"

	"github.com/yjr28/kestrel-replay/internal/fault"
	"github.com/yjr28/kestrel-replay/internal/model"
	"github.com/yjr28/kestrel-replay/internal/replay"
)

const (
	Version   = "v2"
	V1Version = "v1"
	Workload  = "single-create-order"
	Topic     = "orders.completed"
)

var topology = []string{"gateway", "auth", "account", "order", "inventory", "pricing", "payment", "broker", "notification", "audit", "analytics", "collector"}

type Case struct {
	ID               string
	Fault            fault.Spec
	ExpectedBehavior string
}

// CasesV1 preserves the original four-case replay corpus verbatim. New corpus
// versions are additive and must not silently redefine an older version.
func CasesV1() []Case {
	return []Case{
		{
			ID: "inventory-timeout",
			Fault: fault.Spec{Kind: fault.Latency, TargetService: "inventory", Operation: "check", TriggerOnMatch: 1, Delay: 75 * time.Millisecond, Seed: 20260903},
			ExpectedBehavior: "inventory latency exceeds the order dependency timeout and produces inventory_timeout",
		},
		{
			ID: "inventory-connection-reset",
			Fault: fault.Spec{Kind: fault.ConnectionReset, TargetService: "inventory", Operation: "check", TriggerOnMatch: 1, Seed: 20260904},
			ExpectedBehavior: "inventory resets the accepted TCP connection and order reports inventory_connection_reset",
		},
		{
			ID: "inventory-pre-request-crash",
			Fault: fault.Spec{Kind: fault.ServiceCrash, TargetService: "inventory", TriggerOnMatch: 1, Seed: 20260905},
			ExpectedBehavior: "inventory is killed after health checks but before workload execution and order reports inventory_connection_refused",
		},
		{
			ID: "orders-completed-duplicate",
			Fault: fault.Spec{Kind: fault.DuplicateMessage, TargetService: "broker", Operation: Topic, TriggerOnMatch: 1, Seed: 20260906},
			ExpectedBehavior: "broker delivers the orders.completed envelope twice to each worker while the synchronous request remains successful",
		},
	}
}

// Cases returns the current corpus. v2 extends the immutable v1 definitions
// with delayed async delivery and leaves all v1 fault parameters unchanged.
func Cases() []Case {
	cases := append([]Case(nil), CasesV1()...)
	cases = append(cases, Case{
		ID: "orders-completed-delay",
		Fault: fault.Spec{Kind: fault.DelayedMessage, TargetService: "broker", Operation: Topic, TriggerOnMatch: 1, Delay: 120 * time.Millisecond, Seed: 20260907},
		ExpectedBehavior: "broker delays the orders.completed envelope before delivering one copy to each worker while the synchronous request remains successful",
	})
	return cases
}

func Topology() []string {
	return append([]string(nil), topology...)
}

func ValidateDefinitions() error {
	cases := Cases()
	if len(cases) == 0 {
		return fmt.Errorf("corpus %s has no cases", Version)
	}
	seen := map[string]struct{}{}
	for _, c := range cases {
		if c.ID == "" {
			return fmt.Errorf("corpus case id is required")
		}
		if _, ok := seen[c.ID]; ok {
			return fmt.Errorf("duplicate corpus case id %q", c.ID)
		}
		seen[c.ID] = struct{}{}
		if c.ExpectedBehavior == "" {
			return fmt.Errorf("corpus case %q expected behavior is required", c.ID)
		}
		if err := c.Fault.Validate(); err != nil {
			return fmt.Errorf("corpus case %q: %w", c.ID, err)
		}
	}
	return nil
}

func ValidateObserved(c Case, outcome replay.OutcomeSignature, events []model.Event) error {
	switch c.Fault.Kind {
	case fault.Latency:
		if outcome.HTTPStatus != http.StatusGatewayTimeout || outcome.TerminalService != "inventory" || outcome.ErrorCode != "inventory_timeout" {
			return fmt.Errorf("expected HTTP 504 inventory_timeout, got %+v", outcome)
		}
		if countFault(events, fault.Latency) != 1 {
			return fmt.Errorf("expected exactly one latency fault event")
		}
	case fault.ConnectionReset:
		if outcome.HTTPStatus != http.StatusBadGateway || outcome.TerminalService != "inventory" || outcome.ErrorCode != "inventory_connection_reset" {
			return fmt.Errorf("expected HTTP 502 inventory_connection_reset, got %+v", outcome)
		}
		if countFault(events, fault.ConnectionReset) != 1 {
			return fmt.Errorf("expected exactly one connection_reset fault event")
		}
		if !hasTransportError(events, "inventory", "connection_reset") {
			return fmt.Errorf("missing inventory connection_reset span evidence")
		}
	case fault.ServiceCrash:
		if outcome.HTTPStatus != http.StatusBadGateway || outcome.TerminalService != "inventory" || outcome.ErrorCode != "inventory_connection_refused" {
			return fmt.Errorf("expected HTTP 502 inventory_connection_refused, got %+v", outcome)
		}
		if countFault(events, fault.ServiceCrash) != 1 {
			return fmt.Errorf("expected exactly one service_crash fault event")
		}
		if hasServiceSpan(events, "inventory") {
			return fmt.Errorf("inventory emitted a request span despite pre-request process crash")
		}
	case fault.DuplicateMessage:
		if outcome.HTTPStatus != http.StatusCreated || outcome.Classification != "success" {
			return fmt.Errorf("expected successful HTTP 201 outcome, got %+v", outcome)
		}
		if countFault(events, fault.DuplicateMessage) != 1 {
			return fmt.Errorf("expected exactly one duplicate_message fault event")
		}
		sig := replay.MessageDelivery(events, Topic)
		if sig.PublishCount != 1 || sig.ConsumeCounts["notification"] != 2 || sig.ConsumeCounts["audit"] != 2 || sig.ConsumeCounts["analytics"] != 2 {
			return fmt.Errorf("unexpected duplicate delivery signature: %+v", sig)
		}
	case fault.DelayedMessage:
		if outcome.HTTPStatus != http.StatusCreated || outcome.Classification != "success" {
			return fmt.Errorf("expected successful HTTP 201 outcome, got %+v", outcome)
		}
		if countFault(events, fault.DelayedMessage) != 1 {
			return fmt.Errorf("expected exactly one delayed_message fault event")
		}
		delivery := replay.MessageDelivery(events, Topic)
		if delivery.PublishCount != 1 || delivery.ConsumeCounts["notification"] != 1 || delivery.ConsumeCounts["audit"] != 1 || delivery.ConsumeCounts["analytics"] != 1 {
			return fmt.Errorf("delayed message changed delivery multiplicity: %+v", delivery)
		}
		delay := replay.MessageDelay(events, Topic)
		if delay.CorrelatedConsumeCount != 3 {
			return fmt.Errorf("delayed message has uncorrelated consume evidence: %+v", delay)
		}
		if !replay.MeetsMinimumMessageDelay(delay, c.Fault.Delay) {
			return fmt.Errorf("delayed message did not meet %v threshold: %+v", c.Fault.Delay, delay)
		}
	default:
		return fmt.Errorf("corpus case %q uses unsupported validation kind %q", c.ID, c.Fault.Kind)
	}
	return nil
}

func ObservedBehavior(outcome replay.OutcomeSignature, events []model.Event) string {
	messages := replay.MessageDelivery(events, Topic)
	delay := replay.MessageDelay(events, Topic)
	services := make([]string, 0, len(messages.ConsumeCounts))
	for service := range messages.ConsumeCounts {
		services = append(services, service)
	}
	sort.Strings(services)
	consumes := ""
	for i, service := range services {
		if i > 0 {
			consumes += ","
		}
		consumes += fmt.Sprintf("%s=%d", service, messages.ConsumeCounts[service])
	}
	return fmt.Sprintf("classification=%s http=%d terminal=%s error=%s message_publish=%d message_consumes=%s message_min_delay_us=%d", outcome.Classification, outcome.HTTPStatus, outcome.TerminalService, outcome.ErrorCode, messages.PublishCount, consumes, delay.MinConsumeDelayMicros)
}

func countFault(events []model.Event, kind fault.Kind) int {
	count := 0
	for _, event := range events {
		if event.Kind == model.KindFault && event.Attributes["fault.kind"] == string(kind) {
			count++
		}
	}
	return count
}

func hasTransportError(events []model.Event, service, transportError string) bool {
	for _, event := range events {
		if event.Kind == model.KindSpan && event.Service == service && event.Status == "error" && event.Attributes["transport.error"] == transportError {
			return true
		}
	}
	return false
}

func hasServiceSpan(events []model.Event, service string) bool {
	for _, event := range events {
		if event.Kind == model.KindSpan && event.Service == service {
			return true
		}
	}
	return false
}
