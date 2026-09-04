package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/yjr28/kestrel-replay/internal/fault"
	"github.com/yjr28/kestrel-replay/internal/model"
	"github.com/yjr28/kestrel-replay/internal/replay"
	"github.com/yjr28/kestrel-replay/internal/transport"
)

type Result struct {
	Events  []model.Event
	Outcome replay.OutcomeSignature
}

type child struct {
	name string
	cmd  *exec.Cmd
	logs bytes.Buffer
	done chan error
}

// RunScenario launches a complete Kestrel demo topology as separate OS
// processes, executes one request, waits for telemetry to drain, and then
// tears the topology down. The caller supplies a built kestrel-node binary.
func RunScenario(ctx context.Context, nodeBinary string, spec *fault.Spec, requestID string) (Result, error) {
	if strings.TrimSpace(nodeBinary) == "" {
		return Result{}, fmt.Errorf("node binary is required")
	}
	if spec != nil {
		if err := spec.Validate(); err != nil {
			return Result{}, fmt.Errorf("fault spec: %w", err)
		}
		if spec.Kind == fault.ServiceCrash && spec.TargetService != "inventory" {
			return Result{}, fmt.Errorf("service crash replay currently supports only target service inventory")
		}
	}
	if requestID == "" {
		requestID = fmt.Sprintf("req-%d", time.Now().UnixNano())
	}

	names := []string{"collector", "broker", "gateway", "auth", "account", "order", "inventory", "pricing", "payment", "notification", "audit", "analytics"}
	addresses, err := allocateAddresses(names)
	if err != nil {
		return Result{}, err
	}
	url := func(name string) string { return "http://" + addresses[name] }

	var children []*child
	childrenByName := make(map[string]*child, len(names))
	stopAll := func() {
		for i := len(children) - 1; i >= 0; i-- {
			children[i].stop()
		}
	}
	defer stopAll()

	start := func(name string, args ...string) error {
		args = append(args, "-listen="+addresses[name])
		c, err := startChild(ctx, name, nodeBinary, args)
		if err != nil {
			return err
		}
		children = append(children, c)
		childrenByName[name] = c
		if err := waitHealth(ctx, url(name)+"/healthz", 4*time.Second); err != nil {
			return fmt.Errorf("%s failed health check: %w\n%s", name, err, c.logs.String())
		}
		return nil
	}

	if err := start("collector", "-mode=collector", "-queue-capacity=4096"); err != nil {
		return Result{}, err
	}
	for _, role := range []string{"notification", "audit", "analytics"} {
		if err := start(role, "-mode=service", "-role="+role, "-collector="+url("collector")); err != nil {
			return Result{}, err
		}
	}
	if err := start("broker", "-mode=broker", "-workers="+strings.Join([]string{url("notification"), url("audit"), url("analytics")}, ","), "-queue-capacity=1024"); err != nil {
		return Result{}, err
	}

	inventoryArgs := []string{"-mode=service", "-role=inventory", "-collector=" + url("collector")}
	if spec != nil && spec.Kind != fault.ServiceCrash {
		inventoryArgs = append(inventoryArgs,
			"-fault-kind="+string(spec.Kind),
			"-fault-target="+spec.TargetService,
			"-fault-operation="+spec.Operation,
			"-fault-delay="+spec.Delay.String(),
			"-fault-seed="+strconv.FormatInt(spec.Seed, 10),
			"-fault-trigger="+strconv.Itoa(spec.TriggerOnMatch),
		)
	}
	if err := start("inventory", inventoryArgs...); err != nil {
		return Result{}, err
	}
	if err := start("pricing", "-mode=service", "-role=pricing", "-collector="+url("collector")); err != nil {
		return Result{}, err
	}
	if err := start("payment", "-mode=service", "-role=payment", "-collector="+url("collector")); err != nil {
		return Result{}, err
	}
	if err := start("order", "-mode=service", "-role=order", "-collector="+url("collector"), "-inventory="+url("inventory"), "-pricing="+url("pricing"), "-payment="+url("payment"), "-broker="+url("broker")); err != nil {
		return Result{}, err
	}
	if err := start("account", "-mode=service", "-role=account", "-collector="+url("collector"), "-next="+url("order")); err != nil {
		return Result{}, err
	}
	if err := start("auth", "-mode=service", "-role=auth", "-collector="+url("collector"), "-next="+url("account")); err != nil {
		return Result{}, err
	}
	if err := start("gateway", "-mode=service", "-role=gateway", "-collector="+url("collector"), "-next="+url("auth")); err != nil {
		return Result{}, err
	}

	if spec != nil && spec.Kind == fault.ServiceCrash {
		if err := recordServiceCrash(ctx, url("collector"), requestID, *spec); err != nil {
			return Result{}, err
		}
		target := childrenByName[spec.TargetService]
		if target == nil {
			return Result{}, fmt.Errorf("service crash target %q is not running", spec.TargetService)
		}
		if err := target.crash(); err != nil {
			return Result{}, fmt.Errorf("crash %s: %w", spec.TargetService, err)
		}
		if err := waitUnavailable(ctx, url(spec.TargetService)+"/healthz", 2*time.Second); err != nil {
			return Result{}, fmt.Errorf("verify %s crash: %w", spec.TargetService, err)
		}
	}

	outcome, err := request(ctx, url("gateway"), requestID)
	if err != nil {
		return Result{}, err
	}
	if outcome.HTTPStatus < 400 {
		if err := waitBrokerIdle(ctx, url("broker"), 3, 3*time.Second); err != nil {
			return Result{}, err
		}
	}
	serviceNames := []string{"gateway", "auth", "account", "order", "inventory", "pricing", "payment", "notification", "audit", "analytics"}
	if spec != nil && spec.Kind == fault.ServiceCrash {
		serviceNames = without(serviceNames, spec.TargetService)
	}
	if err := flushTelemetry(ctx, serviceNames, url, 4*time.Second); err != nil {
		return Result{}, err
	}

	minimum := 14
	if spec != nil {
		minimum = 6
		if spec.Kind == fault.ServiceCrash {
			minimum = 5
		}
	}
	events, err := waitEvents(ctx, url("collector"), requestID, minimum, 4*time.Second)
	if err != nil {
		return Result{}, err
	}
	return Result{Events: events, Outcome: outcome}, nil
}

func recordServiceCrash(ctx context.Context, collectorURL, requestID string, spec fault.Spec) error {
	event := model.Event{
		ID:            "injector-" + requestID + "-service-crash",
		Source:        model.SourceFault,
		Kind:          model.KindFault,
		CorrelationID: requestID,
		Service:       spec.TargetService,
		Operation:     "process_exit",
		Timestamp:     time.Now().UTC(),
		Status:        "injected",
		Attributes: map[string]string{
			"fault.kind":       string(spec.Kind),
			"target.service":   spec.TargetService,
			"seed":             strconv.FormatInt(spec.Seed, 10),
			"schedule.phase":   "before_request",
			"trigger_on_match": strconv.Itoa(spec.TriggerOnMatch),
		},
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, collectorURL+"/v1/events", bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := (&http.Client{Timeout: time.Second}).Do(req)
	if err != nil {
		return fmt.Errorf("record service crash event: %w", err)
	}
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, 8<<10))
	resp.Body.Close()
	if readErr != nil {
		return fmt.Errorf("read service crash event response: %w", readErr)
	}
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("record service crash event: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

func request(ctx context.Context, gatewayURL, requestID string) (replay.OutcomeSignature, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, gatewayURL, strings.NewReader(`{"sku":"demo"}`))
	if err != nil {
		return replay.OutcomeSignature{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(transport.RequestIDHeader, requestID)
	resp, err := (&http.Client{Timeout: 2 * time.Second}).Do(req)
	if err != nil {
		return replay.OutcomeSignature{}, fmt.Errorf("gateway request: %w", err)
	}
	body, readErr := io.ReadAll(resp.Body)
	resp.Body.Close()
	if readErr != nil {
		return replay.OutcomeSignature{}, readErr
	}

	outcome := replay.OutcomeSignature{HTTPStatus: resp.StatusCode}
	if resp.StatusCode < 400 {
		outcome.Classification = "success"
		outcome.CausalPath = []string{"gateway", "auth", "account", "order", "inventory", "pricing", "payment"}
	} else {
		outcome.Classification = "distributed_failure"
		outcome.TerminalService = resp.Header.Get(transport.FailureServiceHeader)
		outcome.ErrorCode = resp.Header.Get(transport.ErrorCodeHeader)
		if outcome.ErrorCode == "" {
			outcome.ErrorCode = strings.TrimSpace(string(body))
		}
		outcome.CausalPath = []string{"gateway", "auth", "account", "order", outcome.TerminalService}
	}
	return outcome, nil
}

func allocateAddresses(names []string) (map[string]string, error) {
	out := make(map[string]string, len(names))
	for _, name := range names {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return nil, err
		}
		out[name] = ln.Addr().String()
		_ = ln.Close()
	}
	return out, nil
}

func startChild(ctx context.Context, name, binary string, args []string) (*child, error) {
	cmd := exec.CommandContext(ctx, binary, args...)
	c := &child{name: name, cmd: cmd, done: make(chan error, 1)}
	cmd.Stdout = &c.logs
	cmd.Stderr = &c.logs
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start %s: %w", name, err)
	}
	go func() { c.done <- cmd.Wait() }()
	return c, nil
}

func (c *child) crash() error {
	if c == nil || c.cmd == nil || c.cmd.Process == nil {
		return fmt.Errorf("process is not running")
	}
	return c.cmd.Process.Kill()
}

func (c *child) stop() {
	if c == nil || c.cmd == nil || c.cmd.Process == nil {
		return
	}
	_ = c.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-c.done:
	case <-time.After(2 * time.Second):
		_ = c.cmd.Process.Kill()
		<-c.done
	}
}

func waitHealth(ctx context.Context, endpoint string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	client := &http.Client{Timeout: 200 * time.Millisecond}
	for time.Now().Before(deadline) {
		if err := ctx.Err(); err != nil {
			return err
		}
		resp, err := client.Get(endpoint)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode/100 == 2 {
				return nil
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for %s", endpoint)
}

func waitUnavailable(ctx context.Context, endpoint string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	client := &http.Client{Timeout: 100 * time.Millisecond}
	for time.Now().Before(deadline) {
		if err := ctx.Err(); err != nil {
			return err
		}
		resp, err := client.Get(endpoint)
		if err != nil {
			return nil
		}
		resp.Body.Close()
		if resp.StatusCode/100 != 2 {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for %s to become unavailable", endpoint)
}

func waitBrokerIdle(ctx context.Context, baseURL string, minDelivered uint64, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	client := &http.Client{Timeout: 300 * time.Millisecond}
	for time.Now().Before(deadline) {
		if err := ctx.Err(); err != nil {
			return err
		}
		resp, err := client.Get(baseURL + "/stats")
		if err == nil {
			var stats struct {
				Queued    int    `json:"queued"`
				Inflight  int64  `json:"inflight"`
				Delivered uint64 `json:"delivered"`
				Errors    uint64 `json:"errors"`
			}
			decodeErr := json.NewDecoder(resp.Body).Decode(&stats)
			resp.Body.Close()
			if decodeErr == nil && stats.Queued == 0 && stats.Inflight == 0 && stats.Delivered >= minDelivered && stats.Errors == 0 {
				return nil
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	return fmt.Errorf("broker did not drain")
}

func flushTelemetry(ctx context.Context, names []string, url func(string) string, timeout time.Duration) error {
	drainCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	client := &http.Client{Timeout: timeout}
	for _, name := range names {
		req, err := http.NewRequestWithContext(drainCtx, http.MethodPost, url(name)+"/telemetry/flush", nil)
		if err != nil {
			return fmt.Errorf("build %s telemetry flush: %w", name, err)
		}
		resp, err := client.Do(req)
		if err != nil {
			return fmt.Errorf("flush %s telemetry: %w", name, err)
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 8<<10))
		resp.Body.Close()
		if readErr != nil {
			return fmt.Errorf("read %s telemetry flush response: %w", name, readErr)
		}
		if resp.StatusCode/100 != 2 {
			return fmt.Errorf("flush %s telemetry: status=%d body=%s", name, resp.StatusCode, strings.TrimSpace(string(body)))
		}
	}
	return nil
}

func waitEvents(ctx context.Context, baseURL, requestID string, minimum int, timeout time.Duration) ([]model.Event, error) {
	deadline := time.Now().Add(timeout)
	client := &http.Client{Timeout: 300 * time.Millisecond}
	var last []model.Event
	for time.Now().Before(deadline) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		resp, err := client.Get(baseURL + "/v1/events")
		if err == nil {
			var all []model.Event
			decodeErr := json.NewDecoder(resp.Body).Decode(&all)
			resp.Body.Close()
			if decodeErr == nil {
				last = last[:0]
				for _, e := range all {
					if e.CorrelationID == requestID {
						last = append(last, e)
					}
				}
				if len(last) >= minimum {
					return append([]model.Event(nil), last...), nil
				}
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	return nil, fmt.Errorf("collector captured %d events for %s; expected at least %d", len(last), requestID, minimum)
}

func without(values []string, excluded string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		if value != excluded {
			out = append(out, value)
		}
	}
	return out
}
