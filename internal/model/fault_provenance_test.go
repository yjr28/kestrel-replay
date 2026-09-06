package model

import (
	"testing"
	"time"
)

func TestEventValidateFaultRequiresFailureInjectorSource(t *testing.T) {
	for _, source := range []Source{SourceApplication, SourceKernel, SourceReplay} {
		e := Event{
			ID:        "e1",
			Kind:      KindFault,
			Source:    source,
			Timestamp: time.Now(),
			Attributes: map[string]string{
				"target.service": "inventory",
				"fault.kind":     "latency",
			},
		}
		if err := e.Validate(); err == nil {
			t.Fatalf("expected fault event from source %q to fail validation", source)
		}
	}

	e := Event{
		ID:        "e1",
		Kind:      KindFault,
		Source:    SourceFault,
		Timestamp: time.Now(),
		Attributes: map[string]string{
			"target.service": "inventory",
			"fault.kind":     "latency",
		},
	}
	if err := e.Validate(); err != nil {
		t.Fatalf("expected failure-injector fault evidence to validate: %v", err)
	}
}
