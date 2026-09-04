package integration_test

import (
	"context"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/yjr28/kestrel-replay/internal/fault"
	"github.com/yjr28/kestrel-replay/internal/graph"
	"github.com/yjr28/kestrel-replay/internal/orchestrator"
	"github.com/yjr28/kestrel-replay/internal/replay"
)

func TestMultiProcessFailureReplay(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-process integration test")
	}
	if runtime.GOOS == "windows" {
		t.Skip("signal-based process lifecycle is currently Unix-only")
	}

	root, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	bin := filepath.Join(t.TempDir(), "kestrel-node")
	build := exec.Command("go", "build", "-o", bin, "./cmd/kestrel-node")
	build.Dir = root
	if raw, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build kestrel-node: %v\n%s", err, raw)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	spec := fault.Spec{Kind: fault.Latency, TargetService: "inventory", Operation: "check", TriggerOnMatch: 1, Delay: 75 * time.Millisecond, Seed: 20260903}

	healthy, err := orchestrator.RunScenario(ctx, bin, nil, "req-healthy")
	if err != nil {
		t.Fatal(err)
	}
	failing, err := orchestrator.RunScenario(ctx, bin, &spec, "req-failing")
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := orchestrator.RunScenario(ctx, bin, &spec, "req-replay")
	if err != nil {
		t.Fatal(err)
	}

	if failing.Outcome.HTTPStatus != 504 || failing.Outcome.TerminalService != "inventory" {
		t.Fatalf("unexpected failing outcome: %+v", failing.Outcome)
	}
	if !replay.Equivalent(failing.Outcome, replayed.Outcome) {
		t.Fatalf("replay mismatch failing=%+v replay=%+v", failing.Outcome, replayed.Outcome)
	}
	if len(healthy.Events) < 14 || len(failing.Events) < 6 || len(replayed.Events) < 6 {
		t.Fatalf("unexpected event counts healthy=%d failing=%d replay=%d", len(healthy.Events), len(failing.Events), len(replayed.Events))
	}

	g, err := graph.Build(failing.Events)
	if err != nil {
		t.Fatal(err)
	}
	if len(g.Nodes) < 6 || len(g.Edges) < 4 {
		t.Fatalf("graph too small nodes=%d edges=%d", len(g.Nodes), len(g.Edges))
	}
	divergence, ok := graph.EarliestMeaningfulDivergence(healthy.Events, failing.Events, 20*time.Millisecond)
	if !ok || divergence.Service != "inventory" || divergence.Operation != "check" {
		t.Fatalf("unexpected divergence ok=%t value=%+v", ok, divergence)
	}
}
