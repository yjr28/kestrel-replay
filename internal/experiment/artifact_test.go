package experiment

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yjr28/kestrel-replay/internal/fault"
	"github.com/yjr28/kestrel-replay/internal/model"
	"github.com/yjr28/kestrel-replay/internal/replay"
)

func sampleRecord() Record {
	now := time.Date(2026, 9, 4, 2, 30, 0, 0, time.UTC)
	spec := fault.Spec{Kind: fault.Latency, TargetService: "inventory", Operation: "check", TriggerOnMatch: 1, Delay: 75 * time.Millisecond, Seed: 7}
	return Record{
		ExperimentID: "incident-0001", CreatedAt: now, Workload: "single-create-order",
		Topology: []string{"gateway", "order", "inventory"}, Fault: &spec,
		ExpectedBehavior: "request times out at inventory", ObservedBehavior: "HTTP 504 inventory_timeout",
		Outcome: replay.OutcomeSignature{Classification: "distributed_failure", HTTPStatus: 504, TerminalService: "inventory", ErrorCode: "inventory_timeout", CausalPath: []string{"gateway", "order", "inventory"}},
		Events: []model.Event{{
			ID: "event-1", Sequence: 1, Source: model.SourceApplication, Kind: model.KindSpan,
			TraceID: "00000000000000000000000000000001", SpanID: "0000000000000001", Service: "inventory", Operation: "check",
			Timestamp: now, Status: "error", Attributes: map[string]string{"duration_us": "75000"},
		}},
	}
}

func TestSaveLoadRoundTrip(t *testing.T) {
	root := t.TempDir()
	record := sampleRecord()
	dir, err := Save(root, record)
	if err != nil {
		t.Fatal(err)
	}
	artifact, err := Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	if artifact.Manifest.SchemaVersion != SchemaVersion || artifact.Manifest.ExperimentID != record.ExperimentID {
		t.Fatalf("unexpected manifest: %+v", artifact.Manifest)
	}
	if artifact.Manifest.EventCount != 1 || len(artifact.Events) != 1 || artifact.Events[0].ID != "event-1" {
		t.Fatalf("unexpected events: %+v", artifact.Events)
	}
	if !replay.Equivalent(artifact.Manifest.Outcome, record.Outcome) {
		t.Fatalf("outcome mismatch: %+v", artifact.Manifest.Outcome)
	}
	if _, err := Save(root, record); err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("expected immutable destination error, got %v", err)
	}
}

func TestLoadDetectsEventCorruption(t *testing.T) {
	dir, err := Save(t.TempDir(), sampleRecord())
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, eventsName)
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.WriteString(" \n")
	_ = f.Close()
	if _, err := Load(dir); err == nil || !strings.Contains(err.Error(), "digest mismatch") {
		t.Fatalf("expected digest mismatch, got %v", err)
	}
}

func TestLoadDetectsManifestCorruption(t *testing.T) {
	dir, err := Save(t.TempDir(), sampleRecord())
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, manifestName)
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.WriteString(" \n")
	_ = f.Close()
	if _, err := Load(dir); err == nil || !strings.Contains(err.Error(), "manifest digest mismatch") {
		t.Fatalf("expected manifest digest mismatch, got %v", err)
	}
}

func TestRejectsUnsupportedSchema(t *testing.T) {
	err := validateManifest(Manifest{SchemaVersion: 99})
	if err == nil || !strings.Contains(err.Error(), "unsupported experiment schema") {
		t.Fatalf("expected schema error, got %v", err)
	}
}

func TestSaveRejectsConcurrentWriterReservation(t *testing.T) {
	root := t.TempDir()
	record := sampleRecord()
	lockPath := filepath.Join(root, record.ExperimentID+".lock")
	if err := os.WriteFile(lockPath, []byte("reserved"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Save(root, record); err == nil || !strings.Contains(err.Error(), "is being written") {
		t.Fatalf("expected writer reservation error, got %v", err)
	}
}

func TestSaveRejectsUnsafeExperimentID(t *testing.T) {
	record := sampleRecord()
	record.ExperimentID = "../escape"
	if _, err := Save(t.TempDir(), record); err == nil {
		t.Fatal("expected unsafe id rejection")
	}
}
