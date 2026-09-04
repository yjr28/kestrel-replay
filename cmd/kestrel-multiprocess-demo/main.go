package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/yjr28/kestrel-replay/internal/experiment"
	"github.com/yjr28/kestrel-replay/internal/fault"
	"github.com/yjr28/kestrel-replay/internal/graph"
	"github.com/yjr28/kestrel-replay/internal/orchestrator"
	"github.com/yjr28/kestrel-replay/internal/replay"
)

func main() {
	node := flag.String("node", ".kestrel/bin/kestrel-node", "path to built kestrel-node binary")
	experimentRoot := flag.String("experiments", ".kestrel/experiments", "directory for immutable experiment artifacts")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	spec := fault.Spec{Kind: fault.Latency, TargetService: "inventory", Operation: "check", TriggerOnMatch: 1, Delay: 75 * time.Millisecond, Seed: 20260903}
	healthy, err := orchestrator.RunScenario(ctx, *node, nil, "req-healthy")
	if err != nil {
		log.Fatal(err)
	}
	failing, err := orchestrator.RunScenario(ctx, *node, &spec, "req-failing")
	if err != nil {
		log.Fatal(err)
	}

	experimentID := "demo-" + time.Now().UTC().Format("20060102T150405000000000")
	artifactDir, err := experiment.Save(*experimentRoot, experiment.Record{
		ExperimentID:     experimentID,
		Workload:         "single-create-order",
		Topology:         []string{"gateway", "auth", "account", "order", "inventory", "pricing", "payment", "broker", "notification", "audit", "analytics", "collector"},
		Fault:            &spec,
		ExpectedBehavior: "inventory latency exceeds the order service timeout and produces inventory_timeout",
		ObservedBehavior: fmt.Sprintf("http=%d terminal=%s error=%s", failing.Outcome.HTTPStatus, failing.Outcome.TerminalService, failing.Outcome.ErrorCode),
		Outcome:          failing.Outcome,
		Events:           failing.Events,
	})
	if err != nil {
		log.Fatal(err)
	}
	// Deliberately discard the in-memory failing execution. Everything below is
	// driven by the immutable artifact that was just committed to disk.
	failing = orchestrator.Result{}
	recorded, err := experiment.Load(artifactDir)
	if err != nil {
		log.Fatal(err)
	}

	replayed, err := orchestrator.RunScenario(ctx, *node, recorded.Manifest.Fault, "req-replay")
	if err != nil {
		log.Fatal(err)
	}
	g, err := graph.Build(recorded.Events)
	if err != nil {
		log.Fatal(err)
	}
	divergence, found := graph.EarliestMeaningfulDivergence(healthy.Events, recorded.Events, 20*time.Millisecond)

	fmt.Println("Kestrel multi-process demo")
	fmt.Println("==========================")
	fmt.Println("topology: 10 service processes + broker + collector")
	fmt.Printf("healthy outcome: %s (signature=%s, events=%d)\n", healthy.Outcome.Classification, healthy.Outcome.Digest(), len(healthy.Events))
	fmt.Printf("recorded failure: %s/%s (signature=%s, events=%d)\n", recorded.Manifest.Outcome.TerminalService, recorded.Manifest.Outcome.ErrorCode, recorded.Manifest.Outcome.Digest(), len(recorded.Events))
	fmt.Printf("artifact: %s (sha256=%s)\n", artifactDir, recorded.Manifest.EventsSHA256)
	fmt.Printf("causal graph: nodes=%d edges=%d\n", len(g.Nodes), len(g.Edges))
	if found {
		raw, _ := json.Marshal(divergence)
		fmt.Printf("divergence evidence: %s\n", raw)
	} else {
		fmt.Println("divergence evidence: none")
	}
	fmt.Printf("replay outcome: %s/%s (signature=%s)\n", replayed.Outcome.TerminalService, replayed.Outcome.ErrorCode, replayed.Outcome.Digest())
	fmt.Printf("replay_match=%t\n", replay.Equivalent(recorded.Manifest.Outcome, replayed.Outcome))
	if !replay.Equivalent(recorded.Manifest.Outcome, replayed.Outcome) {
		log.Fatal("replay outcome did not match recorded failure")
	}
}
