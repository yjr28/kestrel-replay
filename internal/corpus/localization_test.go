package corpus

import "testing"

func TestExpectedLocalizationScopesOnlySynchronousV1Cases(t *testing.T) {
	eligible := 0
	for _, c := range Cases() {
		truth, ok := ExpectedLocalization(c)
		if c.ID == "orders-completed-duplicate" {
			if ok {
				t.Fatalf("duplicate-message case must remain excluded from span-only localization: %+v", truth)
			}
			continue
		}
		if !ok {
			t.Fatalf("expected localization truth for %s", c.ID)
		}
		eligible++
		if truth.Service != "inventory" || truth.Operation != "check" {
			t.Fatalf("unexpected truth for %s: %+v", c.ID, truth)
		}
	}
	if eligible != 3 {
		t.Fatalf("expected three localization-eligible v1 cases, got %d", eligible)
	}
}
