package replay

import "testing"

func TestOutcomeDigestDistinguishesCausalPathElementBoundaries(t *testing.T) {
	a := OutcomeSignature{
		Classification: "timeout",
		HTTPStatus:     504,
		CausalPath:     []string{"gateway\norder"},
	}
	b := OutcomeSignature{
		Classification: "timeout",
		HTTPStatus:     504,
		CausalPath:     []string{"gateway", "order"},
	}

	if Equivalent(a, b) {
		t.Fatal("structurally different causal paths must not be equivalent")
	}
	if a.Digest() == b.Digest() {
		t.Fatalf("structurally different causal paths must not share a digest: %q", a.Digest())
	}
}

func TestOutcomeDigestDistinguishesScalarFieldBoundaries(t *testing.T) {
	a := OutcomeSignature{
		Classification: "timeout\n504",
		HTTPStatus:     0,
		TerminalService: "",
		ErrorCode:      "",
	}
	b := OutcomeSignature{
		Classification:  "timeout",
		HTTPStatus:      504,
		TerminalService: "0",
		ErrorCode:       "",
	}

	if Equivalent(a, b) {
		t.Fatal("structurally different scalar fields must not be equivalent")
	}
	if a.Digest() == b.Digest() {
		t.Fatalf("structurally different scalar fields must not share a digest: %q", a.Digest())
	}
}
