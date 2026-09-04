package replay

import "testing"

func TestEquivalent(t *testing.T) {
	a := OutcomeSignature{Classification: "timeout", HTTPStatus: 504, TerminalService: "inventory", ErrorCode: "inventory_timeout", CausalPath: []string{"gateway", "order", "inventory"}}
	b := a
	if !Equivalent(a, b) {
		t.Fatal("identical signatures should match")
	}
	b.ErrorCode = "other"
	if Equivalent(a, b) {
		t.Fatal("different error code should not match")
	}
}
