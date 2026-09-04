package experiment

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func writeTestReservation(t *testing.T, root, experimentID string, pid int, hostname string, createdAt time.Time, withTemp bool) {
	t.Helper()
	tmpDir := filepath.Join(root, experimentID+".tmp")
	if withTemp {
		if err := os.Mkdir(tmpDir, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(tmpDir, "partial"), []byte("incomplete"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	raw, err := json.Marshal(reservation{PID: pid, Hostname: hostname, CreatedAt: createdAt, TempDir: filepath.Base(tmpDir)})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, experimentID+".lock"), append(raw, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestRecoverStaleDeadReservation(t *testing.T) {
	root := t.TempDir()
	record := sampleRecord()
	hostname, err := os.Hostname()
	if err != nil {
		t.Fatal(err)
	}
	writeTestReservation(t, root, record.ExperimentID, 1<<30, hostname, time.Now().Add(-2*time.Hour), true)

	report, err := Recover(root, record.ExperimentID, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if !report.RemovedLock || !report.RemovedTempDir || report.CommittedExists {
		t.Fatalf("unexpected recovery report: %+v", report)
	}
	if _, err := Save(root, record); err != nil {
		t.Fatalf("save after recovery: %v", err)
	}
}

func TestRecoverRefusesLiveReservation(t *testing.T) {
	root := t.TempDir()
	record := sampleRecord()
	hostname, err := os.Hostname()
	if err != nil {
		t.Fatal(err)
	}
	writeTestReservation(t, root, record.ExperimentID, os.Getpid(), hostname, time.Now().Add(-2*time.Hour), true)

	if _, err := Recover(root, record.ExperimentID, time.Hour); err == nil || !strings.Contains(err.Error(), "still alive") {
		t.Fatalf("expected live-owner refusal, got %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, record.ExperimentID+".lock")); err != nil {
		t.Fatalf("live reservation lock was modified: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, record.ExperimentID+".tmp")); err != nil {
		t.Fatalf("live reservation temp dir was modified: %v", err)
	}
}

func TestRecoverRefusesYoungDeadReservation(t *testing.T) {
	root := t.TempDir()
	record := sampleRecord()
	hostname, err := os.Hostname()
	if err != nil {
		t.Fatal(err)
	}
	writeTestReservation(t, root, record.ExperimentID, 1<<30, hostname, time.Now().Add(-time.Minute), true)

	if _, err := Recover(root, record.ExperimentID, time.Hour); err == nil || !strings.Contains(err.Error(), "stale threshold") {
		t.Fatalf("expected young-reservation refusal, got %v", err)
	}
}

func TestRecoverCommittedArtifactOnlyCleansAuxiliaryPaths(t *testing.T) {
	root := t.TempDir()
	record := sampleRecord()
	dir, err := Save(root, record)
	if err != nil {
		t.Fatal(err)
	}
	hostname, err := os.Hostname()
	if err != nil {
		t.Fatal(err)
	}
	writeTestReservation(t, root, record.ExperimentID, os.Getpid(), hostname, time.Now(), true)

	report, err := Recover(root, record.ExperimentID, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if !report.CommittedExists || !report.RemovedLock || !report.RemovedTempDir {
		t.Fatalf("unexpected committed recovery report: %+v", report)
	}
	if _, err := Load(dir); err != nil {
		t.Fatalf("committed artifact was damaged: %v", err)
	}
}
