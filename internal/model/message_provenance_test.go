package model

import (
	"strings"
	"testing"
	"time"
)

func TestEventValidateMessageRequiresApplicationProvenance(t *testing.T) {
	base := Event{
		ID:        "e1",
		Kind:      KindMessage,
		TraceID:   "trace",
		Service:   "order",
		Timestamp: time.Now(),
		Attributes: map[string]string{
			"message.id":     "m1",
			"message.action": "publish",
			"topic":          "orders.completed",
		},
	}

	valid := base
	valid.Source = SourceApplication
	if err := valid.Validate(); err != nil {
		t.Fatalf("application message evidence should validate: %v", err)
	}

	for _, source := range []Source{SourceKernel, SourceFault, SourceReplay} {
		e := base
		e.Source = source
		err := e.Validate()
		if err == nil || !strings.Contains(err.Error(), string(SourceApplication)) {
			t.Fatalf("expected message source %q to be rejected, got %v", source, err)
		}
	}
}
