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

const (
	reportSchemaVersion = 3
	healthyProfileRuns  = 3
)

type artifactReplayReport struct {
	RecordedOutcome      replay.OutcomeSignature         `json:"recorded_outcome"`
	ReplayedOutcome      replay.OutcomeSignature         `json:"replayed_outcome"`
	RecordedMessages     replay.MessageDeliverySignature `json:"recorded_message_delivery"`
	ReplayedMessages     replay.MessageDeliverySignature `json:"replayed_message_delivery"`
	MessageDeliveryMatch bool                            `json:"message_delivery_match"`
	ReplayMatch          bool                            `json:"replay_match"`
}

type healthyBaselineReport struct {
	Index        int    `json:"index"`
	ArtifactDir  string `json:"artifact_dir"`
	EventCount   int    `json:"event_count"`
	EventsSHA256 string `json:"events_sha256"`
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
	SchemaVersion             int                     `json:"schema_version"`
	CorpusVersion             string                  `json:"corpus_version"`
	RunID                     string                  `json:"run_id"`
	CreatedAt                 time.Time               `json:"created_at"`
	CompletedAt               time.Time               `json:"completed_at"`
	CaseCount                 int                     `json:"case_count"`
	PassedCount               int                     `json:"passed_count"`
	FailedCount               int                     `json:"failed_count"`
	ReplayPassedCount         int                     `json:"replay_passed_count"`
	ReplayFailedCount         int                     `json:"replay_failed_count"`
	LocalizationEligibleCount int                     `json:"localization_eligible_count"`
	LocalizationTop1Count     int                     `json:"localization_top1_count"`
	LocalizationTop3Count     int                     `json:"localization_top3_count"`
	HealthyProfileRunCount    int                     `json:"healthy_profile_run_count"`
	HealthyBaselines          []healthyBaselineReport `json:"healthy_baselines"`
	HealthyProfile            []graph.SpanBaseline    `json:"healthy_profile"`
	ReportPath                string                  `json:"report_path"`
	Cases                     []caseReport            `json:"cases"`
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

	healthyReports, healthyArtifacts, err := recordHealthyBaselines(*node, artifactRoot, runID, healthyProfileRuns)
	if err != nil {
		log.Fatalf("record healthy baselines: %v", err)
	}
	healthyRuns := make([][]model.Event, 0, len(healthyArtifacts))
	for _, artifact := range healthyArtifacts {
		healthyRuns = append(healthyRuns, artifact.Events)
	}
	profile, err := graph.BuildHealthyProfile(healthyRuns)
	if err != nil {
		log.Fatalf("build healthy profile: %v", err)
	}

	report := corpusReport{
		SchemaVersion:          reportSchemaVersion,
		CorpusVersion:          corpus.Version,
		RunID:                  runID,
		CreatedAt:              started,
		CaseCount:              len(corpus.Cases()),
		HealthyProfileRunCount: profile.RunCount,
		HealthyBaselines:       healthyReports,
		HealthyProfile:         profile.Baselines(),
		ReportPath:             reportPath,
	}

	for _, c := range corpus.Cases() {
		cr := runCase(*node, *replayBin, artifactRoot, runID, profile, c)
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
		fmt.Printf("corpus=%s run=%s cases=%d passed=%d failed=%d replay=%d/%d healthy_profile_runs=%d localization_top1=%d/%d localization_top3=%d/%d report=%s\n",
			report.CorpusVersion, report.RunID, report.CaseCount, report.PassedCount, report.FailedCount,
			report.ReplayPassedCount, report.CaseCount, report.HealthyProfileRunCount,
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

func recordHealthyBaselines(node, artifactRoot, runID string, count int) ([]healthyBaselineReport, []experiment.Artifact, error) {
	if count < 2 {
		return nil, nil, fmt.Errorf("healthy baseline count must be at least two")
	}
	reports := make([]healthyBaselineReport, 0, count)
	artifacts := make([]experiment.Artifact, 0, count)
	for i := 1; i <= count; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		requestID := fmt.Sprintf("corpus-%s-healthy-%02d", runID, i)
		result, err := orchestrator.RunScenario(ctx, node, nil, requestID)
		cancel()
		if err != nil {
			return nil, nil, fmt.Errorf("healthy run %d: %w", i, err)
		}
		if result.Outcome.Classification != "success" || result.Outcome.HTTPStatus != 201 {
			return nil, nil, fmt.Errorf("healthy run %d unexpected outcome: %+v", i, result.Outcome)
		}
		if len(result.Events) < 14 {
			return nil, nil, fmt.Errorf("healthy run %d captured %d events; expected at least 14", i, len(result.Events))
		}
		experimentID := fmt.Sprintf("%s-healthy-baseline-%02d", corpus.Version, i)
		artifactDir, err := experiment.Save(artifactRoot, experiment.Record{
			ExperimentID:     experimentID,
			Workload:         corpus.Workload,
			Topology:         corpus.Topology(),
			ExpectedBehavior: "healthy single-create-order execution completes without injected faults",
			ObservedBehavior: corpus.ObservedBehavior(result.Outcome, result.Events),
			Outcome:          result.Outcome,
			Events:           result.Events,
		})
		if err != nil {
			return nil, nil, fmt.Errorf("healthy run %d persist artifact: %w", i, err)
		}
		artifact, err := experiment.Load(artifactDir)
		if err != nil {
			return nil, nil, fmt.Errorf("healthy run %d reload artifact: %w", i, err)
		}
		reports = append(reports, healthyBaselineReport{Index: i, ArtifactDir: artifactDir, EventCount: len(artifact.Events), EventsSHA256: artifact.Manifest.EventsSHA256})
		artifacts = append(artifacts, artifact)
	}
	return reports, artifacts, nil
}

func runCase(node, replayBin, artifactRoot, runID string, profile graph.HealthyProfile, c corpus.Case) caseReport {
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
		candidates := graph.RankDivergencesAgainstProfile(profile, artifact.Events, corpus.LocalizationLatencyThreshold, artifact.Manifest.Outcome.TerminalService)
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
