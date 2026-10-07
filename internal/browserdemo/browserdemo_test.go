package browserdemo

import "testing"

func TestRunDemoReproducesInventoryFailure(t *testing.T) {
	got, err := RunDemo(Config{DelayMS: 75, Seed: 20260903})
	if err != nil {
		t.Fatal(err)
	}
	if got.Failing.Outcome.TerminalService != "inventory" {
		t.Fatalf("terminal=%q", got.Failing.Outcome.TerminalService)
	}
	if got.Failing.Outcome.ErrorCode != "inventory_timeout" {
		t.Fatalf("error=%q", got.Failing.Outcome.ErrorCode)
	}
	if !got.ReplayMatch {
		t.Fatal("expected replay match")
	}
	if !got.HasDivergence || got.Divergence.Service != "inventory" {
		t.Fatalf("divergence=%+v", got.Divergence)
	}
}

func TestRunDemoCanStayHealthy(t *testing.T) {
	got, err := RunDemo(Config{DelayMS: 15, Seed: 20260903})
	if err != nil {
		t.Fatal(err)
	}
	if got.Failing.Outcome.Classification != "success" {
		t.Fatalf("classification=%q", got.Failing.Outcome.Classification)
	}
	if !got.ReplayMatch {
		t.Fatal("expected replay match")
	}
}
