package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/yjr28/kestrel-replay/internal/corpus"
	"github.com/yjr28/kestrel-replay/internal/experiment"
	"github.com/yjr28/kestrel-replay/internal/graph"
	"github.com/yjr28/kestrel-replay/internal/model"
	"github.com/yjr28/kestrel-replay/internal/orchestrator"
	"github.com/yjr28/kestrel-replay/internal/replay"
)

const reportSchemaVersion = 2

type artifactReplayReport struct {
	RecordedOutcome      replay.OutcomeSignature         `json:"recorded_outcome"`
	ReplayedOutcome      replay.OutcomeSignature         `json:"replayed_outcome"`
	RecordedMessages     replay.MessageDeliverySignature `json:"recorded_message_delivery"`
	ReplayedMessages     replay.MessageDeliverySignature `json:"replayed_message_delivery"`
	MessageDeliveryMatch bool                            `json:"message_delivery_match"`
	ReplayMatch          bool                            `json:"replay_match"`
}

type caseReport struct {
	CaseID                 string                          `json:"case_id"`
	FaultKind              string                          `json:"fault_kind"`
	ArtifactDir            string                          `json:"artifact_dir,omitempty"`
	EventCount             int                             `json:"event_count,omitempty"`
	EventsSHA256           string                          `json:"events_sha256,omitempty"`
	RecordedOutcome        replay.OutcomeSignature         `json:"recorded_outcome"`
	RecordedMessages       replay.MessageDeliverySignature `json:"recorded_message_delivery"`
	ReplayedOutcome        replay.OutcomeSignature         `json:"replayed_outcome"`
	ReplayedMessages       replay.MessageDeliverySignature `json:"replayed_message_delivery"`
	OutcomeMatch           bool                            `json:"outcome_match"`
	MessageMatch           bool                            `json:"message_delivery_match"`
	ReplayMatch            bool                            `json:"replay_match"`
	LocalizationEligible   bool                            `json:"localization_eligible"`
	ExpectedLocalization   *corpus.LocalizationTruth       `json:"expected_localization,omitempty"`
	LocalizationTop1       bool                            `json:"localization_top1"`
	LocalizationTop3       bool                            `json:"localization_top3"`
	LocalizationCandidates []graph.LocalizationCandidate  `json:"localization_candidates,omitempty"`
	RegressionPass         bool                            `json:"regression_pass"`
	Error                  string                          `json:"error,omitempty"`
}

type corpusReport struct {
	SchemaVersion             int          `json:"schema_version"`
	CorpusVersion             string       `json:"corpus_version"`
	RunID                     string       `json:"run_id"`
	CreatedAt                 time.Time    `json:"created_at"`
	CompletedAt               time.Time    `json:"completed_at"`
	CaseCount                 int          `json:"case_count"`
	PassedCount               int          `json:"passed_count"`
	FailedCount               int          `json:"failed_count"`
	ReplayPassedCount         int          `json:"replay_passed_count"`
	ReplayFailedCount         int          `json:"replay_failed_count"`
	LocalizationEligibleCount int          `json:"localization_eligible_count"`
	LocalizationTop1Count     int          `json:"localization_top1_count"`
	LocalizationTop3Count     int          `json:"localization_top3_count"`
	HealthyArtifactDir        string       `json:"healthy_artifact_dir"`
	HealthyEventCount         int          `json:"healthy_event_count"`
	HealthyEventsSHA256       string       `json:"healthy_events_sha256"`
	ReportPath                string       `json:"report_path"`
	Cases                     []caseReport `json:"cases"`
}

func main() {
	node := flag.String("node", ".kestrel/bin/kestrel-node", "path to built kestrel-node binary")
	replayBin := flag.String("replay", ".kestrel/bin/kestrel-artifact-replay", "path to built kestrel-artifact-replay binary")
	root := flag.String("root", ".kestrel/corpus-runs", "root directory for immutable corpus run artifacts")
	jsonOutput := flag.Bool("json", false, "emit the corpus report as JSON")
	flag.Parse()

	if err := corpus.ValidateDefinitions(); err != nil {
		log.Fatalf("validate corpus: %v", err)
	}
	if strings.TrimSpace(*node) == "" || strings.TrimSpace(*replayBin) == "" || strings.TrimSpace(*root) == "" {
		log.Fatal("-node, -replay, and -root must be non-empty")
	}

	started := time.Now().UTC()
	runID := corpus.Version + "-" + started.Format("20060102T150405.000000000Z")
	runDir := filepath.Join(filepath.Clean(*root), runID)
	artifactRoot := filepath.Join(runDir, "artifacts")
	if err := os.MkdirAll(artifactRoot, 0o750); err != nil {
		log.Fatalf("create corpus run directory: %v", err)
	}
	reportPath := filepath.Join(runDir, "report.json")

	healthyArtifactDir, healthyArtifact, err := recordHealthyBaseline(*node, artifactRoot, runID)
	if err != nil {
		log.Fatalf("record healthy baseline: %v", err)
	}

	report := corpusReport{
		SchemaVersion:       reportSchemaVersion,
		CorpusVersion:       corpus.Version,
		RunID:               runID,
		CreatedAt:           started,
		CaseCount:           len(corpus.Cases()),
		HealthyArtifactDir:  healthyArtifactDir,
		HealthyEventCount:   len(healthyArtifact.Events),
		HealthyEventsSHA256: healthyArtifact.Manifest.EventsSHA256,
		ReportPath:          reportPath,
	}

	for _, c := range corpus.Cases() {
		cr := runCase(*node, *replayBin, artifactRoot, runID, healthyArtifact.Events, c)
		report.Cases = append(report.Cases, cr)
		if cr.ReplayMatch {
			report.ReplayPassedCount++
		} else {
			report.ReplayFailedCount++
		}
		if cr.LocalizationEligible {
			report.LocalizationEligibleCount++
			if cr.LocalizationTop1 {
				report.LocalizationTop1Count++
			}
			if cr.LocalizationTop3 {
				report.LocalizationTop3Count++
			}
		}
		if cr.RegressionPass {
			report.PassedCount++
		} else {
			report.FailedCount++
		}
	}
	report.CompletedAt = time.Now().UTC()

	if err := writeReport(reportPath, report); err != nil {
		log.Fatalf("write corpus report: %v", err)
	}
	if *jsonOutput {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(report); err != nil {
			log.Fatal(err)
		}
	} else {
		fmt.Printf("corpus=%s run=%s cases=%d passed=%d failed=%d replay=%d/%d localization_top1=%d/%d localization_top3=%d/%d report=%s\n",
			report.CorpusVersion, report.RunID, report.CaseCount, report.PassedCount, report.FailedCount,
			report.ReplayPassedCount, report.CaseCount,
			report.LocalizationTop1Count, report.LocalizationEligibleCount,
			report.LocalizationTop3Count, report.LocalizationEligibleCount,
			report.ReportPath,
		)
		for _, c := range report.Cases {
			fmt.Printf("case=%s kind=%s replay_match=%t regression_pass=%t", c.CaseID, c.FaultKind, c.ReplayMatch, c.RegressionPass)
			if c.LocalizationEligible {
				fmt.Printf(" localization_top1=%t localization_top3=%t", c.LocalizationTop1, c.LocalizationTop3)
			}
			if c.Error != "" {
				fmt.Printf(" error=%q", c.Error)
			}
			fmt.Println()
		}
	}
	if report.FailedCount != 0 {
		os.Exit(2)
	}
}

func recordHealthyBaseline(node, artifactRoot, runID string) (string, experiment.Artifact, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	result, err := orchestrator.RunScenario(ctx, node, nil, "corpus-"+runID+"-healthy")
	cancel()
	if err != nil {
		return "", experiment.Artifact{}, err
	}
	if result.Outcome.Classification != "success" || result.Outcome.HTTPStatus != 201 {
		return "", experiment.Artifact{}, fmt.Errorf("unexpected healthy outcome: %+v", result.Outcome)
	}
	if len(result.Events) < 14 {
		return "", experiment.Artifact{}, fmt.Errorf("healthy baseline captured %d events; expected at least 14", len(result.Events))
	}
	artifactDir, err := experiment.Save(artifactRoot, experiment.Record{
		ExperimentID:     corpus.Version + "-healthy-baseline",
		Workload:         corpus.Workload,
		Topology:         corpus.Topology(),
		ExpectedBehavior: "healthy single-create-order execution completes without injected faults",
		ObservedBehavior: corpus.ObservedBehavior(result.Outcome, result.Events),
		Outcome:          result.Outcome,
		Events:           result.Events,
	})
	if err != nil {
		return "", experiment.Artifact{}, err
	}
	artifact, err := experiment.Load(artifactDir)
	if err != nil {
		return "", experiment.Artifact{}, err
	}
	return artifactDir, artifact, nil
}

func runCase(node, replayBin, artifactRoot, runID string, healthyEvents []model.Event, c corpus.Case) caseReport {
	cr := caseReport{CaseID: c.ID, FaultKind: string(c.Fault.Kind)}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	result, err := orchestrator.RunScenario(ctx, node, &c.Fault, "corpus-"+runID+"-"+c.ID)
	cancel()
	if err != nil {
		cr.Error = "record scenario: " + err.Error()
		return cr
	}
	cr.RecordedOutcome = result.Outcome
	cr.RecordedMessages = replay.MessageDelivery(result.Events, corpus.Topic)
	if err := corpus.ValidateObserved(c, result.Outcome, result.Events); err != nil {
		cr.Error = "validate recorded evidence: " + err.Error()
		return cr
	}

	experimentID := corpus.Version + "-" + c.ID
	artifactDir, err := experiment.Save(artifactRoot, experiment.Record{
		ExperimentID:     experimentID,
		Workload:         corpus.Workload,
		Topology:         corpus.Topology(),
		Fault:            &c.Fault,
		ExpectedBehavior: c.ExpectedBehavior,
		ObservedBehavior: corpus.ObservedBehavior(result.Outcome, result.Events),
		Outcome:          result.Outcome,
		Events:           result.Events,
	})
	if err != nil {
		cr.Error = "persist artifact: " + err.Error()
		return cr
	}
	cr.ArtifactDir = artifactDir

	artifact, err := experiment.Load(artifactDir)
	if err != nil {
		cr.Error = "reload artifact: " + err.Error()
		return cr
	}
	cr.EventCount = len(artifact.Events)
	cr.EventsSHA256 = artifact.Manifest.EventsSHA256
	cr.RecordedOutcome = artifact.Manifest.Outcome
	cr.RecordedMessages = replay.MessageDelivery(artifact.Events, corpus.Topic)

	if truth, ok := corpus.ExpectedLocalization(c); ok {
		cr.LocalizationEligible = true
		cr.ExpectedLocalization = &truth
		candidates := graph.RankDivergences(healthyEvents, artifact.Events, corpus.LocalizationLatencyThreshold, artifact.Manifest.Outcome.TerminalService)
		cr.LocalizationCandidates = firstCandidates(candidates, 5)
		cr.LocalizationTop1 = graph.TopKContains(candidates, truth.Service, truth.Operation, 1)
		cr.LocalizationTop3 = graph.TopKContains(candidates, truth.Service, truth.Operation, 3)
	}

	replayCtx, replayCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer replayCancel()
	cmd := exec.CommandContext(replayCtx, replayBin,
		"-artifact", artifactDir,
		"-node", node,
		"-request-id", "corpus-replay-"+runID+"-"+c.ID,
		"-json",
	)
	raw, err := cmd.CombinedOutput()
	if err != nil {
		cr.Error = fmt.Sprintf("artifact replay: %v output=%s", err, strings.TrimSpace(string(raw)))
		return cr
	}
	var rr artifactReplayReport
	if err := json.Unmarshal(raw, &rr); err != nil {
		cr.Error = fmt.Sprintf("decode artifact replay report: %v output=%s", err, strings.TrimSpace(string(raw)))
		return cr
	}
	cr.ReplayedOutcome = rr.ReplayedOutcome
	cr.ReplayedMessages = rr.ReplayedMessages
	cr.OutcomeMatch = replay.Equivalent(cr.RecordedOutcome, cr.ReplayedOutcome)
	cr.MessageMatch = replay.EquivalentMessageDelivery(cr.RecordedMessages, cr.ReplayedMessages)
	cr.ReplayMatch = rr.ReplayMatch && rr.MessageDeliveryMatch && cr.OutcomeMatch && cr.MessageMatch
	if !cr.ReplayMatch {
		cr.Error = "replayed evidence did not match recorded artifact"
		return cr
	}
	cr.RegressionPass = !cr.LocalizationEligible || cr.LocalizationTop1
	return cr
}

func firstCandidates(candidates []graph.LocalizationCandidate, limit int) []graph.LocalizationCandidate {
	if limit <= 0 || len(candidates) == 0 {
		return nil
	}
	if len(candidates) < limit {
		limit = len(candidates)
	}
	return append([]graph.LocalizationCandidate(nil), candidates[:limit]...)
}

func writeReport(path string, report corpusReport) error {
	raw, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o640); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}
