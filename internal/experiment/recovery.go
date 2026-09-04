package experiment

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

type reservation struct {
	PID       int       `json:"pid"`
	Hostname  string    `json:"hostname"`
	CreatedAt time.Time `json:"created_at"`
	TempDir   string    `json:"temp_dir"`
}

type RecoveryReport struct {
	ExperimentID    string `json:"experiment_id"`
	RemovedLock     bool   `json:"removed_lock"`
	RemovedTempDir  bool   `json:"removed_temp_dir"`
	CommittedExists bool   `json:"committed_exists"`
}

func reserveExperiment(lockPath, tmpDir string) error {
	hostname, err := os.Hostname()
	if err != nil {
		return fmt.Errorf("resolve hostname: %w", err)
	}
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = os.Remove(lockPath)
		}
	}()
	value := reservation{
		PID:       os.Getpid(),
		Hostname:  hostname,
		CreatedAt: time.Now().UTC(),
		TempDir:   filepath.Base(tmpDir),
	}
	enc := json.NewEncoder(f)
	if err := enc.Encode(value); err != nil {
		_ = f.Close()
		return fmt.Errorf("encode reservation: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("sync reservation: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close reservation: %w", err)
	}
	committed = true
	return nil
}

// Recover removes an abandoned writer reservation for one experiment ID.
// It never mutates a committed experiment directory. A reservation is eligible
// only when it is older than staleAfter and its recorded process is confirmed
// dead on the same host. If the committed directory already exists, a leftover
// reservation/temp path is safe to remove because Save never overwrites the
// committed destination.
func Recover(root, experimentID string, staleAfter time.Duration) (RecoveryReport, error) {
	if strings.TrimSpace(root) == "" {
		return RecoveryReport{}, errors.New("experiment root is required")
	}
	if !experimentIDPattern.MatchString(experimentID) {
		return RecoveryReport{}, errors.New("experiment id must match [A-Za-z0-9][A-Za-z0-9_-]{0,127}")
	}
	if staleAfter <= 0 {
		return RecoveryReport{}, errors.New("stale duration must be positive")
	}
	root = filepath.Clean(root)
	finalDir := filepath.Join(root, experimentID)
	lockPath := finalDir + ".lock"
	tmpDir := finalDir + ".tmp"
	report := RecoveryReport{ExperimentID: experimentID}

	if _, err := os.Stat(finalDir); err == nil {
		report.CommittedExists = true
		if existed, err := removePathIfExists(lockPath, false); err != nil {
			return report, fmt.Errorf("remove committed reservation: %w", err)
		} else {
			report.RemovedLock = existed
		}
		if existed, err := removePathIfExists(tmpDir, true); err != nil {
			return report, fmt.Errorf("remove committed temp directory: %w", err)
		} else {
			report.RemovedTempDir = existed
		}
		if report.RemovedLock || report.RemovedTempDir {
			if err := syncDir(root); err != nil {
				return report, fmt.Errorf("sync experiment root: %w", err)
			}
		}
		return report, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return report, fmt.Errorf("stat committed experiment: %w", err)
	}

	res, err := readReservation(lockPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return report, fmt.Errorf("no writer reservation exists for experiment %q", experimentID)
		}
		return report, err
	}
	if res.TempDir != filepath.Base(tmpDir) {
		return report, errors.New("reservation temp directory does not match experiment id")
	}
	age := time.Since(res.CreatedAt)
	if age < staleAfter {
		return report, fmt.Errorf("reservation is only %s old; stale threshold is %s", age.Round(time.Second), staleAfter)
	}
	hostname, err := os.Hostname()
	if err != nil {
		return report, fmt.Errorf("resolve hostname: %w", err)
	}
	if res.Hostname != hostname {
		return report, fmt.Errorf("reservation belongs to host %q; current host is %q", res.Hostname, hostname)
	}
	if processAlive(res.PID) {
		return report, fmt.Errorf("reservation owner pid %d is still alive", res.PID)
	}

	if existed, err := removePathIfExists(tmpDir, true); err != nil {
		return report, fmt.Errorf("remove stale temp directory: %w", err)
	} else {
		report.RemovedTempDir = existed
	}
	if existed, err := removePathIfExists(lockPath, false); err != nil {
		return report, fmt.Errorf("remove stale reservation: %w", err)
	} else {
		report.RemovedLock = existed
	}
	if err := syncDir(root); err != nil {
		return report, fmt.Errorf("sync experiment root: %w", err)
	}
	return report, nil
}

func readReservation(path string) (reservation, error) {
	raw, err := readLimitedFile(path, 16<<10)
	if err != nil {
		return reservation{}, err
	}
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.DisallowUnknownFields()
	var value reservation
	if err := dec.Decode(&value); err != nil {
		return reservation{}, fmt.Errorf("decode reservation: %w", err)
	}
	if err := ensureJSONEOF(dec); err != nil {
		return reservation{}, fmt.Errorf("decode reservation: %w", err)
	}
	if value.PID <= 0 || strings.TrimSpace(value.Hostname) == "" || value.CreatedAt.IsZero() || strings.TrimSpace(value.TempDir) == "" {
		return reservation{}, errors.New("reservation metadata is incomplete")
	}
	return value, nil
}

func processAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	err := syscall.Kill(pid, 0)
	if err == nil || errors.Is(err, syscall.EPERM) {
		return true
	}
	return !errors.Is(err, syscall.ESRCH)
}

func removePathIfExists(path string, directory bool) (bool, error) {
	if _, err := os.Stat(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	if directory {
		return true, os.RemoveAll(path)
	}
	return true, os.Remove(path)
}
