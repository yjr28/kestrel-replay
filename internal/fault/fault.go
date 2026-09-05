package fault

import (
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"
)

type Kind string

const (
	Latency          Kind = "latency"
	PacketLoss       Kind = "packet_loss"
	ConnectionReset  Kind = "connection_reset"
	ServiceCrash     Kind = "service_crash"
	ServiceRestart   Kind = "service_restart"
	RPCTimeout       Kind = "rpc_timeout"
	DuplicateMessage Kind = "duplicate_message"
	DelayedMessage   Kind = "delayed_message"
	ReorderedMessage Kind = "reordered_message"
)

type Spec struct {
	Kind           Kind          `json:"kind"`
	TargetService  string        `json:"target_service"`
	Operation      string        `json:"operation,omitempty"`
	TriggerOnMatch int           `json:"trigger_on_match"`
	Delay          time.Duration `json:"delay,omitempty"`
	JitterFraction float64       `json:"jitter_fraction,omitempty"`
	Seed           int64         `json:"seed"`
}

func (s Spec) Validate() error {
	if strings.TrimSpace(s.TargetService) == "" {
		return fmt.Errorf("target service is required")
	}
	if s.TriggerOnMatch < 1 {
		return fmt.Errorf("trigger_on_match must be >= 1")
	}
	if s.JitterFraction < 0 || s.JitterFraction > 1 {
		return fmt.Errorf("jitter_fraction must be between 0 and 1")
	}
	switch s.Kind {
	case Latency:
		if s.Delay <= 0 {
			return fmt.Errorf("latency fault requires positive delay")
		}
	case ConnectionReset:
		if s.Delay != 0 || s.JitterFraction != 0 {
			return fmt.Errorf("connection reset does not accept delay or jitter parameters")
		}
	case ServiceCrash:
		if s.Delay != 0 || s.JitterFraction != 0 {
			return fmt.Errorf("service crash does not accept delay or jitter parameters")
		}
		if s.Operation != "" {
			return fmt.Errorf("service crash is process-scoped and does not accept an operation")
		}
		if s.TriggerOnMatch != 1 {
			return fmt.Errorf("service crash currently supports only trigger_on_match=1")
		}
	case DuplicateMessage:
		if s.Delay != 0 || s.JitterFraction != 0 {
			return fmt.Errorf("duplicate message does not accept delay or jitter parameters")
		}
		if strings.TrimSpace(s.Operation) == "" {
			return fmt.Errorf("duplicate message requires a target operation/topic")
		}
	case DelayedMessage:
		if s.Delay <= 0 {
			return fmt.Errorf("delayed message requires positive delay")
		}
		if s.JitterFraction != 0 {
			return fmt.Errorf("delayed message does not yet accept jitter")
		}
		if strings.TrimSpace(s.Operation) == "" {
			return fmt.Errorf("delayed message requires a target operation/topic")
		}
	default:
		return fmt.Errorf("fault kind %q is not implemented", s.Kind)
	}
	return nil
}

type Decision struct {
	Inject bool
	Delay  time.Duration
	Spec   Spec
}

type Controller struct {
	mu      sync.Mutex
	specs   []Spec
	matches []int
	rngs    []*rand.Rand
}

func NewController(specs []Spec) (*Controller, error) {
	c := &Controller{specs: append([]Spec(nil), specs...), matches: make([]int, len(specs)), rngs: make([]*rand.Rand, len(specs))}
	for i, s := range c.specs {
		if err := s.Validate(); err != nil {
			return nil, fmt.Errorf("fault %d: %w", i, err)
		}
		if s.Kind != Latency && s.Kind != ConnectionReset {
			return nil, fmt.Errorf("fault %d: kind %q is not supported by the in-service controller", i, s.Kind)
		}
		c.rngs[i] = rand.New(rand.NewSource(s.Seed))
	}
	return c, nil
}

func (c *Controller) Decide(service, operation string) Decision {
	c.mu.Lock()
	defer c.mu.Unlock()

	service = strings.TrimSpace(service)
	operation = strings.TrimSpace(operation)
	for i, s := range c.specs {
		if strings.TrimSpace(s.TargetService) != service || (strings.TrimSpace(s.Operation) != "" && strings.TrimSpace(s.Operation) != operation) {
			continue
		}
		c.matches[i]++
		if c.matches[i] != s.TriggerOnMatch {
			continue
		}
		delay := s.Delay
		if s.JitterFraction > 0 && delay > 0 {
			spread := float64(delay) * s.JitterFraction
			delta := (c.rngs[i].Float64()*2 - 1) * spread
			delay = time.Duration(float64(delay) + delta)
		}
		return Decision{Inject: true, Delay: delay, Spec: s}
	}
	return Decision{}
}
