package fault

import (
	"fmt"
	"math/rand"
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
	if s.TargetService == "" {
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
		c.rngs[i] = rand.New(rand.NewSource(s.Seed))
	}
	return c, nil
}

func (c *Controller) Decide(service, operation string) Decision {
	c.mu.Lock()
	defer c.mu.Unlock()

	for i, s := range c.specs {
		if s.TargetService != service || (s.Operation != "" && s.Operation != operation) {
			continue
		}
		c.matches[i]++
		if c.matches[i] != s.TriggerOnMatch {
			continue
		}
		delay := s.Delay
		if s.JitterFraction > 0 && delay > 0 {
			// One deterministic sample per injected occurrence, centered around the configured delay.
			spread := float64(delay) * s.JitterFraction
			delta := (c.rngs[i].Float64()*2 - 1) * spread
			delay = time.Duration(float64(delay) + delta)
		}
		return Decision{Inject: true, Delay: delay, Spec: s}
	}
	return Decision{}
}
