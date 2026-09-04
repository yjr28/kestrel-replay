package experiment

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/yjr28/kestrel-replay/internal/fault"
	"github.com/yjr28/kestrel-replay/internal/model"
	"github.com/yjr28/kestrel-replay/internal/replay"
)

const (
	SchemaVersion = 1
	manifestName  = "manifest.json"
	eventsName    = "events.ndjson"
	checksumsName = "checksums.json"
)

var experimentIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$`)

type Manifest struct {
	SchemaVersion    int                     `json:"schema_version"`
	ExperimentID     string                  `json:"experiment_id"`
	CreatedAt        time.Time               `json:"created_at"`
	Workload         string                  `json:"workload"`
	Topology         []string                `json:"topology"`
	Fault            *fault.Spec             `json:"fault,omitempty"`
	ExpectedBehavior string                  `json:"expected_behavior"`
	ObservedBehavior string                  `json:"observed_behavior"`
	Outcome          replay.OutcomeSignature `json:"outcome"`
	EventsFile       string                  `json:"events_file"`
	EventCount       int                     `json:"event_count"`
	EventsSHA256     string                  `json:"events_sha256"`
}

type Record struct {
	ExperimentID     string
	CreatedAt        time.Time
	Workload         string
	Topology         []string
	Fault            *fault.Spec
	ExpectedBehavior string
	ObservedBehavior string
	Outcome          replay.OutcomeSignature
	Events           []model.Event
}

type Artifact struct {
	Manifest Manifest
	Events   []model.Event
}

type checksums struct {
	ManifestSHA256 string `json:"manifest_sha256"`
	EventsSHA256   string `json:"events_sha256"`
}

func Save(root string, record Record) (string, error) {
	if err := validateRecord(record); err != nil {
		return "", err
	}
	if strings.TrimSpace(root) == "" {
		return "", errors.New("experiment root is required")
	}
	root = filepath.Clean(root)
	if err := os.MkdirAll(root, 0o750); err != nil {
		return "", fmt.Errorf("create experiment root: %w", err)
	}
	finalDir := filepath.Join(root, record.ExperimentID)
	lockPath := finalDir + ".lock"
	lock, err := os.OpenFile(lockPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			return "", fmt.Errorf("experiment %q is being written", record.ExperimentID)
		}
		return "", fmt.Errorf("reserve experiment id: %w", err)
	}
	_ = lock.Close()
	defer os.Remove(lockPath)
	if _, err := os.Stat(finalDir); err == nil {
		return "", fmt.Errorf("experiment %q already exists", record.ExperimentID)
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("stat experiment destination: %w", err)
	}

	tmpDir, err := os.MkdirTemp(root, ".kestrel-tmp-")
	if err != nil {
		return "", fmt.Errorf("create experiment temp dir: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = os.RemoveAll(tmpDir)
		}
	}()

	eventCount, digest, err := writeEvents(filepath.Join(tmpDir, eventsName), record.Events)
	if err != nil {
		return "", err
	}
	createdAt := record.CreatedAt.UTC()
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}
	manifest := Manifest{
		SchemaVersion: SchemaVersion, ExperimentID: record.ExperimentID, CreatedAt: createdAt,
		Workload: record.Workload, Topology: append([]string(nil), record.Topology...), Fault: cloneFault(record.Fault),
		ExpectedBehavior: record.ExpectedBehavior, ObservedBehavior: record.ObservedBehavior, Outcome: record.Outcome,
		EventsFile: eventsName, EventCount: eventCount, EventsSHA256: digest,
	}
	manifestPath := filepath.Join(tmpDir, manifestName)
	if err := writeJSONFile(manifestPath, manifest); err != nil {
		return "", err
	}
	manifestDigest, err := hashFile(manifestPath, 1<<20)
	if err != nil {
		return "", fmt.Errorf("hash manifest: %w", err)
	}
	if err := writeJSONFile(filepath.Join(tmpDir, checksumsName), checksums{ManifestSHA256: manifestDigest, EventsSHA256: digest}); err != nil {
		return "", err
	}
	if err := syncDir(tmpDir); err != nil {
		return "", fmt.Errorf("sync experiment temp dir: %w", err)
	}
	if err := os.Rename(tmpDir, finalDir); err != nil {
		return "", fmt.Errorf("commit experiment: %w", err)
	}
	committed = true
	if err := syncDir(root); err != nil {
		return "", fmt.Errorf("sync experiment root: %w", err)
	}
	return finalDir, nil
}

func Load(dir string) (Artifact, error) {
	if strings.TrimSpace(dir) == "" {
		return Artifact{}, errors.New("experiment directory is required")
	}
	dir = filepath.Clean(dir)
	sums, err := readChecksums(filepath.Join(dir, checksumsName))
	if err != nil {
		return Artifact{}, err
	}
	manifestPath := filepath.Join(dir, manifestName)
	manifestRaw, err := readLimitedFile(manifestPath, 1<<20)
	if err != nil {
		return Artifact{}, fmt.Errorf("read manifest: %w", err)
	}
	manifestDigest := sha256.Sum256(manifestRaw)
	if hex.EncodeToString(manifestDigest[:]) != sums.ManifestSHA256 {
		return Artifact{}, errors.New("manifest digest mismatch")
	}
	dec := json.NewDecoder(strings.NewReader(string(manifestRaw)))
	dec.DisallowUnknownFields()
	var manifest Manifest
	if err := dec.Decode(&manifest); err != nil {
		return Artifact{}, fmt.Errorf("decode manifest: %w", err)
	}
	if err := ensureJSONEOF(dec); err != nil {
		return Artifact{}, fmt.Errorf("decode manifest: %w", err)
	}
	if err := validateManifest(manifest); err != nil {
		return Artifact{}, err
	}
	if manifest.EventsSHA256 != sums.EventsSHA256 {
		return Artifact{}, errors.New("event digest disagreement between manifest and checksums")
	}

	eventsPath := filepath.Join(dir, manifest.EventsFile)
	events, digest, err := readEvents(eventsPath)
	if err != nil {
		return Artifact{}, err
	}
	if len(events) != manifest.EventCount {
		return Artifact{}, fmt.Errorf("event count mismatch: manifest=%d actual=%d", manifest.EventCount, len(events))
	}
	if digest != manifest.EventsSHA256 || digest != sums.EventsSHA256 {
		return Artifact{}, fmt.Errorf("event digest mismatch: expected=%s actual=%s", manifest.EventsSHA256, digest)
	}
	return Artifact{Manifest: manifest, Events: events}, nil
}

func validateRecord(record Record) error {
	if !experimentIDPattern.MatchString(record.ExperimentID) {
		return errors.New("experiment id must match [A-Za-z0-9][A-Za-z0-9_-]{0,127}")
	}
	if strings.TrimSpace(record.Workload) == "" {
		return errors.New("workload is required")
	}
	if len(record.Topology) == 0 {
		return errors.New("topology is required")
	}
	if record.Fault != nil {
		if err := record.Fault.Validate(); err != nil {
			return fmt.Errorf("fault: %w", err)
		}
	}
	if strings.TrimSpace(record.ExpectedBehavior) == "" {
		return errors.New("expected behavior is required")
	}
	if strings.TrimSpace(record.ObservedBehavior) == "" {
		return errors.New("observed behavior is required")
	}
	if strings.TrimSpace(record.Outcome.Classification) == "" {
		return errors.New("outcome classification is required")
	}
	if len(record.Events) == 0 {
		return errors.New("at least one event is required")
	}
	for i, event := range record.Events {
		if err := event.Validate(); err != nil {
			return fmt.Errorf("event %d: %w", i, err)
		}
	}
	return nil
}

func validateManifest(manifest Manifest) error {
	if manifest.SchemaVersion != SchemaVersion {
		return fmt.Errorf("unsupported experiment schema version %d", manifest.SchemaVersion)
	}
	if !experimentIDPattern.MatchString(manifest.ExperimentID) {
		return errors.New("invalid experiment id in manifest")
	}
	if manifest.CreatedAt.IsZero() {
		return errors.New("manifest created_at is required")
	}
	if strings.TrimSpace(manifest.Workload) == "" || len(manifest.Topology) == 0 {
		return errors.New("manifest workload/topology are required")
	}
	if manifest.Fault != nil {
		if err := manifest.Fault.Validate(); err != nil {
			return fmt.Errorf("manifest fault: %w", err)
		}
	}
	if manifest.EventsFile != eventsName || filepath.Base(manifest.EventsFile) != manifest.EventsFile {
		return errors.New("manifest events_file must be events.ndjson")
	}
	if manifest.EventCount < 1 {
		return errors.New("manifest event_count must be positive")
	}
	if len(manifest.EventsSHA256) != sha256.Size*2 {
		return errors.New("manifest events_sha256 is invalid")
	}
	if _, err := hex.DecodeString(manifest.EventsSHA256); err != nil {
		return errors.New("manifest events_sha256 is invalid")
	}
	if strings.TrimSpace(manifest.Outcome.Classification) == "" {
		return errors.New("manifest outcome classification is required")
	}
	return nil
}

func writeEvents(path string, events []model.Event) (int, string, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return 0, "", fmt.Errorf("create events log: %w", err)
	}
	h := sha256.New()
	writer := bufio.NewWriterSize(io.MultiWriter(f, h), 64<<10)
	for i, event := range events {
		if err := event.Validate(); err != nil {
			_ = f.Close()
			return 0, "", fmt.Errorf("event %d: %w", i, err)
		}
		raw, err := json.Marshal(event)
		if err != nil {
			_ = f.Close()
			return 0, "", fmt.Errorf("encode event %d: %w", i, err)
		}
		if _, err := writer.Write(raw); err != nil {
			_ = f.Close()
			return 0, "", fmt.Errorf("write event %d: %w", i, err)
		}
		if err := writer.WriteByte('\n'); err != nil {
			_ = f.Close()
			return 0, "", fmt.Errorf("write event delimiter: %w", err)
		}
	}
	if err := writer.Flush(); err != nil {
		_ = f.Close()
		return 0, "", fmt.Errorf("flush events log: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return 0, "", fmt.Errorf("sync events log: %w", err)
	}
	if err := f.Close(); err != nil {
		return 0, "", fmt.Errorf("close events log: %w", err)
	}
	return len(events), hex.EncodeToString(h.Sum(nil)), nil
}

func readEvents(path string) ([]model.Event, string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, "", fmt.Errorf("open events log: %w", err)
	}
	defer f.Close()
	h := sha256.New()
	dec := json.NewDecoder(io.TeeReader(f, h))
	var events []model.Event
	for {
		var event model.Event
		err := dec.Decode(&event)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, "", fmt.Errorf("decode event %d: %w", len(events), err)
		}
		if err := event.Validate(); err != nil {
			return nil, "", fmt.Errorf("validate event %d: %w", len(events), err)
		}
		events = append(events, event)
	}
	return events, hex.EncodeToString(h.Sum(nil)), nil
}

func readChecksums(path string) (checksums, error) {
	raw, err := readLimitedFile(path, 16<<10)
	if err != nil {
		return checksums{}, fmt.Errorf("read checksums: %w", err)
	}
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.DisallowUnknownFields()
	var sums checksums
	if err := dec.Decode(&sums); err != nil {
		return checksums{}, fmt.Errorf("decode checksums: %w", err)
	}
	if err := ensureJSONEOF(dec); err != nil {
		return checksums{}, fmt.Errorf("decode checksums: %w", err)
	}
	for name, digest := range map[string]string{"manifest": sums.ManifestSHA256, "events": sums.EventsSHA256} {
		if len(digest) != sha256.Size*2 {
			return checksums{}, fmt.Errorf("invalid %s checksum", name)
		}
		if _, err := hex.DecodeString(digest); err != nil {
			return checksums{}, fmt.Errorf("invalid %s checksum", name)
		}
	}
	return sums, nil
}

func readLimitedFile(path string, limit int64) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	raw, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(raw)) > limit {
		return nil, fmt.Errorf("%s exceeds %d bytes", filepath.Base(path), limit)
	}
	return raw, nil
}

func hashFile(path string, limit int64) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, io.LimitReader(f, limit+1))
	if err != nil {
		return "", err
	}
	if n > limit {
		return "", fmt.Errorf("file exceeds %d bytes", limit)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func writeJSONFile(path string, value any) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("create %s: %w", filepath.Base(path), err)
	}
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(value); err != nil {
		_ = f.Close()
		return fmt.Errorf("encode %s: %w", filepath.Base(path), err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("sync %s: %w", filepath.Base(path), err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close %s: %w", filepath.Base(path), err)
	}
	return nil
}

func ensureJSONEOF(dec *json.Decoder) error {
	var extra any
	if err := dec.Decode(&extra); errors.Is(err, io.EOF) {
		return nil
	} else if err == nil {
		return errors.New("multiple JSON values")
	} else {
		return err
	}
}

func syncDir(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

func cloneFault(spec *fault.Spec) *fault.Spec {
	if spec == nil {
		return nil
	}
	copy := *spec
	return &copy
}
