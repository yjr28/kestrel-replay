package browserdemo

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/yjr28/kestrel-replay/internal/fault"
	"github.com/yjr28/kestrel-replay/internal/graph"
	"github.com/yjr28/kestrel-replay/internal/model"
	"github.com/yjr28/kestrel-replay/internal/replay"
)

type Config struct {
	DelayMS int     `json:"delay_ms"`
	Seed    int64   `json:"seed"`
	Jitter  float64 `json:"jitter"`
}

type Run struct {
	Events  []model.Event           `json:"events"`
	Outcome replay.OutcomeSignature `json:"outcome"`
}

type Result struct {
	Healthy       Run              `json:"healthy"`
	Failing       Run              `json:"failing"`
	Replayed      Run              `json:"replayed"`
	Graph         *graph.Graph     `json:"graph"`
	Divergence    graph.Divergence `json:"divergence"`
	HasDivergence bool             `json:"has_divergence"`
	ReplayMatch   bool             `json:"replay_match"`
	FaultDelayMS  float64          `json:"fault_delay_ms"`
}

func RunDemo(cfg Config) (Result, error) {
	if cfg.DelayMS <= 0 {
		cfg.DelayMS = 75
	}
	if cfg.Seed == 0 {
		cfg.Seed = 20260903
	}
	if cfg.Jitter < 0 || cfg.Jitter > 1 {
		return Result{}, fmt.Errorf("jitter must be between 0 and 1")
	}
	spec := fault.Spec{Kind: fault.Latency, TargetService: "inventory", Operation: "check", TriggerOnMatch: 1, Delay: time.Duration(cfg.DelayMS) * time.Millisecond, JitterFraction: cfg.Jitter, Seed: cfg.Seed}
	healthy, _, err := makeRun(nil)
	if err != nil {
		return Result{}, err
	}
	failing, delay, err := makeRun(&spec)
	if err != nil {
		return Result{}, err
	}
	replayed, _, err := makeRun(&spec)
	if err != nil {
		return Result{}, err
	}
	g, err := graph.Build(failing.Events)
	if err != nil {
		return Result{}, err
	}
	div, found := graph.EarliestMeaningfulDivergence(healthy.Events, failing.Events, 20*time.Millisecond)
	return Result{Healthy: healthy, Failing: failing, Replayed: replayed, Graph: g, Divergence: div, HasDivergence: found, ReplayMatch: replay.Equivalent(failing.Outcome, replayed.Outcome), FaultDelayMS: float64(delay) / float64(time.Millisecond)}, nil
}

func JSON(input string) string {
	var cfg Config
	if err := json.Unmarshal([]byte(input), &cfg); err != nil {
		return `{"error":"invalid config"}`
	}
	result, err := RunDemo(cfg)
	if err != nil {
		b, _ := json.Marshal(map[string]string{"error": err.Error()})
		return string(b)
	}
	b, _ := json.Marshal(result)
	return string(b)
}

func makeRun(spec *fault.Spec) (Run, time.Duration, error) {
	base := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
	trace := "trace-browser-001"
	req := "req-browser-001"
	services := []struct {
		name, op string
		baseMS   int
	}{{"gateway", "create_order", 6}, {"auth", "authorize", 4}, {"account", "load_account", 5}, {"order", "create", 10}, {"inventory", "check", 8}, {"pricing", "quote", 6}, {"payment", "authorize", 5}}
	var controller *fault.Controller
	var err error
	if spec != nil {
		controller, err = fault.NewController([]fault.Spec{*spec})
		if err != nil {
			return Run{}, 0, err
		}
	}
	events := make([]model.Event, 0, 12)
	parent := ""
	elapsed := 0
	seq := uint64(0)
	faultDelay := time.Duration(0)
	failed := false
	for i, s := range services {
		seq++
		spanID := fmt.Sprintf("span-%03d", i+1)
		duration := time.Duration(s.baseMS) * time.Millisecond
		status := "ok"
		if controller != nil && s.name == "inventory" {
			d := controller.Decide(s.name, s.op)
			if d.Inject {
				faultDelay = d.Delay
				seq++
				events = append(events, model.Event{ID: fmt.Sprintf("event-%03d", seq), Sequence: seq, Source: model.SourceFault, Kind: model.KindFault, TraceID: trace, CorrelationID: req, Service: s.name, Operation: s.op, Timestamp: base.Add(time.Duration(elapsed) * time.Millisecond), Status: "injected", Attributes: map[string]string{"fault.kind": string(d.Spec.Kind), "target.service": d.Spec.TargetService, "target.operation": d.Spec.Operation, "seed": strconv.FormatInt(d.Spec.Seed, 10), "delay_us": strconv.FormatInt(d.Delay.Microseconds(), 10)}})
				duration += d.Delay
				if d.Delay > 30*time.Millisecond {
					status = "error"
					failed = true
				}
			}
		}
		seq++
		events = append(events, model.Event{ID: fmt.Sprintf("event-%03d", seq), Sequence: seq, Source: model.SourceApplication, Kind: model.KindSpan, TraceID: trace, SpanID: spanID, ParentSpanID: parent, CorrelationID: req, Service: s.name, Operation: s.op, Timestamp: base.Add(time.Duration(elapsed) * time.Millisecond), Status: status, Attributes: map[string]string{"duration_us": strconv.FormatInt(duration.Microseconds(), 10)}})
		parent = spanID
		elapsed += s.baseMS
		if failed {
			break
		}
	}
	if !failed {
		seq++
		messageID := "msg-browser-001"
		events = append(events, model.Event{ID: fmt.Sprintf("event-%03d", seq), Sequence: seq, Source: model.SourceApplication, Kind: model.KindMessage, TraceID: trace, SpanID: "span-004", CorrelationID: req, Service: "order", Operation: "order_completed", Timestamp: base.Add(time.Duration(elapsed) * time.Millisecond), Status: "ok", Attributes: map[string]string{"message.id": messageID, "message.action": "publish", "topic": "orders.completed"}})
		for _, worker := range []string{"notification", "audit", "analytics"} {
			seq++
			events = append(events, model.Event{ID: fmt.Sprintf("event-%03d", seq), Sequence: seq, Source: model.SourceApplication, Kind: model.KindMessage, TraceID: trace, ParentSpanID: "span-004", CorrelationID: req, Service: worker, Operation: "order_completed", Timestamp: base.Add(time.Duration(elapsed+int(seq)) * time.Millisecond), Status: "ok", Attributes: map[string]string{"message.id": messageID, "message.action": "consume", "topic": "orders.completed"}})
		}
	}

	outcome := replay.OutcomeSignature{}
	if failed {
		outcome = replay.OutcomeSignature{Classification: "distributed_failure", HTTPStatus: 504, TerminalService: "inventory", ErrorCode: "inventory_timeout", CausalPath: []string{"gateway", "auth", "account", "order", "inventory"}}
	} else {
		outcome = replay.OutcomeSignature{Classification: "success", HTTPStatus: 201, CausalPath: []string{"gateway", "auth", "account", "order", "inventory", "pricing", "payment"}}
	}
	return Run{Events: events, Outcome: outcome}, faultDelay, nil
}
