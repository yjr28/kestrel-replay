package fault

import (
	"strings"
	"testing"
	"time"
)

func TestOperationScopedSpecsRequireOperation(t *testing.T) {
	tests := []struct {
		name string
		spec Spec
	}{
		{
			name: "latency",
			spec: Spec{Kind: Latency, TargetService: "inventory", Operation: " \t ", TriggerOnMatch: 1, Delay: time.Millisecond},
		},
		{
			name: "connection reset",
			spec: Spec{Kind: ConnectionReset, TargetService: "inventory", Operation: "\n", TriggerOnMatch: 1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.spec.Validate()
			if err == nil || !strings.Contains(err.Error(), "target operation") {
				t.Fatalf("expected missing operation rejection, got %v", err)
			}
		})
	}
}
