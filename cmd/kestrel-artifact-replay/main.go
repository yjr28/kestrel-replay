package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/yjr28/kestrel-replay/internal/experiment"
	"github.com/yjr28/kestrel-replay/internal/graph"
	"github.com/yjr28/kestrel-replay/internal/orchestrator"
	"github.com/yjr28/kestrel-replay/internal/replay"
)

const replayMessageTopic = "orders.completed"

type report struct {
	ExperimentID         string                          `json:"experiment_id"`
	RecordedEventCount   int                             `json:"recorded_event_count"`
	RecordedGraphNodes   int                             `json:"recorded_graph_nodes"`
	RecordedGraphEdges   int                             `json:"recorded_graph_edges"`
	RecordedOutcome      replay.OutcomeSignature         `json:"recorded_outcome"`
	ReplayedOutcome      replay.OutcomeSignature         `json:"replayed_outcome"`
	RecordedMessages     replay.MessageDeliverySignature `json:"recorded_message_delivery"`
	ReplayedMessages     replay.MessageDeliverySignature `json:"replayed_message_delivery"`
	MessageDeliveryMatch bool                            `json:"message_delivery_match"`
	ReplayMatch          bool                            `json:"replay_match"`
}

func main() {
	artifactDir := flag.String("artifact", "", "experiment artifact directory")
	node := flag.String("node", ".kestrel/bin/kestrel-node", "path to built kestrel-node binary")
	requestID := flag.String("request-id", "req-artifact-replay", "request correlation id for the fresh replay")
	jsonOutput := flag.Bool("json", false, "emit a machine-readable JSON report")
	flag.Parse()

	if *artifactDir == "" {
		log.Fatal("-artifact is required")
	}
	artifact, err := experiment.Load(*artifactDir)
	if err != nil {
		log.Fatalf("load artifact: %v", err)
	}
	g, err := graph.Build(artifact.Events)
	if err != nil {
		log.Fatalf("build recorded graph: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	result, err := orchestrator.RunScenario(ctx, *node, artifact.Manifest.Fault, *requestID)
	if err != nil {
		log.Fatalf("run replay: %v", err)
	}

	recordedMessages := replay.MessageDelivery(artifact.Events, replayMessageTopic)
	replayedMessages := replay.MessageDelivery(result.Events, replayMessageTopic)
	messageMatch := replay.EquivalentMessageDelivery(recordedMessages, replayedMessages)
	outcomeMatch := replay.Equivalent(artifact.Manifest.Outcome, result.Outcome)

	r := report{
		ExperimentID: artifact.Manifest.ExperimentID, RecordedEventCount: len(artifact.Events),
		RecordedGraphNodes: len(g.Nodes), RecordedGraphEdges: len(g.Edges),
		RecordedOutcome: artifact.Manifest.Outcome, ReplayedOutcome: result.Outcome,
		RecordedMessages: recordedMessages, ReplayedMessages: replayedMessages,
		MessageDeliveryMatch: messageMatch,
		ReplayMatch: outcomeMatch && messageMatch,
	}
	if *jsonOutput {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(r); err != nil {
			log.Fatal(err)
		}
	} else {
		fmt.Printf("experiment=%s recorded_events=%d graph_nodes=%d graph_edges=%d\n", r.ExperimentID, r.RecordedEventCount, r.RecordedGraphNodes, r.RecordedGraphEdges)
		fmt.Printf("recorded=%s/%s replayed=%s/%s message_delivery_match=%t replay_match=%t\n", r.RecordedOutcome.TerminalService, r.RecordedOutcome.ErrorCode, r.ReplayedOutcome.TerminalService, r.ReplayedOutcome.ErrorCode, r.MessageDeliveryMatch, r.ReplayMatch)
	}
	if !r.ReplayMatch {
		os.Exit(2)
	}
}
