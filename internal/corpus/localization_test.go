package corpus

import "testing"

func TestExpectedLocalizationScopesOnlySynchronousCases(t *testing.T) {
	eligible := 0
	synchronous := map[string]bool{
		"inventory-timeout":             true,
		"inventory-connection-reset":    true,
		"inventory-pre-request-crash":   true,
	}
	for _, c := range Cases() {
		truth, ok := ExpectedLocalization(c)
		if !synchronous[c.ID] {
			if ok {
				t.Fatalf("async case %s must remain excluded from span-only localization: %+v", c.ID, truth)
			}
			continue
		}
		if !ok {
			t.Fatalf("expected localization truth for synchronous case %s", c.ID)
		}
		eligible++
		if truth.Service != "inventory" || truth.Operation != "check" {
			t.Fatalf("unexpected truth for %s: %+v", c.ID, truth)
		}
	}
	if eligible != 3 {
		t.Fatalf("expected three localization-eligible synchronous cases, got %d", eligible)
	}
}
