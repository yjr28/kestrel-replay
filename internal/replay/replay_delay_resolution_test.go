package replay

import (
	"testing"
	"time"
)

func TestMeetsMinimumMessageDelayWithholdsUnrepresentableSubMicrosecondThreshold(t *testing.T) {
	sig := MessageDelaySignature{
		Topic:                  "orders.completed",
		PublishCount:           1,
		CorrelatedConsumeCount: 1,
		MinConsumeDelayMicros:  0,
	}

	if MeetsMinimumMessageDelay(sig, 500*time.Nanosecond) {
		t.Fatal("sub-microsecond threshold must not be satisfied by zero-microsecond evidence")
	}
}
