package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/yjr28/kestrel-replay/internal/fault"
	"github.com/yjr28/kestrel-replay/internal/graph"
	"github.com/yjr28/kestrel-replay/internal/orchestrator"
	"github.com/yjr28/kestrel-replay/internal/replay"
)

func main() {
	node := flag.String("node", ".kestrel/bin/kestrel-node", "path to built kestrel-node binary")
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
	replayed, err := orchestrator.RunScenario(ctx, *node, &spec, "req-replay")
	if err != nil {
		log.Fatal(err)
	}

	g, err := graph.Build(failing.Events)
	if err != nil {
		log.Fatal(err)
	}
	divergence, found := graph.EarliestMeaningfulDivergence(healthy.Events, failing.Events, 20*time.Millisecond)

	fmt.Println("Kestrel multi-process demo")
	fmt.Println("==========================")
	fmt.Println("topology: 10 service processes + broker + collector")
	fmt.Printf("healthy outcome: %s (signature=%s, events=%d)\n", healthy.Outcome.Classification, healthy.Outcome.Digest(), len(healthy.Events))
	fmt.Printf("failing outcome: %s/%s (signature=%s, events=%d)\n", failing.Outcome.TerminalService, failing.Outcome.ErrorCode, failing.Outcome.Digest(), len(failing.Events))
	fmt.Printf("causal graph: nodes=%d edges=%d\n", len(g.Nodes), len(g.Edges))
	if found {
		raw, _ := json.Marshal(divergence)
		fmt.Printf("divergence evidence: %s\n", raw)
	} else {
		fmt.Println("divergence evidence: none")
	}
	fmt.Printf("replay outcome: %s/%s (signature=%s)\n", replayed.Outcome.TerminalService, replayed.Outcome.ErrorCode, replayed.Outcome.Digest())
	fmt.Printf("replay_match=%t\n", replay.Equivalent(failing.Outcome, replayed.Outcome))
	if !replay.Equivalent(failing.Outcome, replayed.Outcome) {
		log.Fatal("replay outcome did not match recorded failure")
	}
}
