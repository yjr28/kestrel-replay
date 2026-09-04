package orchestrator

import (
	"context"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestWaitHealthReportsChildExit(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("test uses a Unix shell to produce a deterministic child exit")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := startChild(ctx, "dead-child", "sh", []string{"-c", "echo startup-failed >&2; exit 17"})
	if err != nil {
		t.Fatal(err)
	}

	err = waitHealth(ctx, c, "http://127.0.0.1:1/healthz", time.Second)
	if err == nil {
		t.Fatal("expected child exit to fail health wait")
	}
	if !strings.Contains(err.Error(), "child exited") || !strings.Contains(err.Error(), "exit status 17") {
		t.Fatalf("health error did not preserve child exit evidence: %v", err)
	}
	if !strings.Contains(c.logs.String(), "startup-failed") {
		t.Fatalf("child logs did not preserve startup evidence: %q", c.logs.String())
	}

	// stop must remain safe after another observer has noticed process exit.
	c.stop()
}
