package replay

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

type OutcomeSignature struct {
	Classification  string   `json:"classification"`
	HTTPStatus      int      `json:"http_status"`
	TerminalService string   `json:"terminal_service,omitempty"`
	ErrorCode       string   `json:"error_code,omitempty"`
	CausalPath      []string `json:"causal_path,omitempty"`
}

func (o OutcomeSignature) Digest() string {
	h := sha256.New()
	fmt.Fprintf(h, "%s\n%d\n%s\n%s\n", o.Classification, o.HTTPStatus, o.TerminalService, o.ErrorCode)
	for _, p := range o.CausalPath {
		fmt.Fprintln(h, p)
	}
	return hex.EncodeToString(h.Sum(nil))[:16]
}

func Equivalent(a, b OutcomeSignature) bool {
	if a.Classification != b.Classification || a.HTTPStatus != b.HTTPStatus || a.TerminalService != b.TerminalService || a.ErrorCode != b.ErrorCode {
		return false
	}
	if len(a.CausalPath) != len(b.CausalPath) {
		return false
	}
	for i := range a.CausalPath {
		if a.CausalPath[i] != b.CausalPath[i] {
			return false
		}
	}
	return true
}
