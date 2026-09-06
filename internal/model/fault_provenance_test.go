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
				"target.service":   "inventory",
				"target.operation": "check",
				"fault.kind":       "latency",
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
			"target.service":   "inventory",
			"target.operation": "check",
			"fault.kind":       "latency",
		},
	}
	if err := e.Validate(); err != nil {
		t.Fatalf("expected failure-injector fault evidence to validate: %v", err)
	}
}

func TestEventValidateOperationScopedFaultRequiresTargetOperation(t *testing.T) {
	for _, kind := range []string{"latency", "connection_reset", "duplicate_message", "delayed_message"} {
		e := Event{
			ID:        "e1",
			Kind:      KindFault,
			Source:    SourceFault,
			Timestamp: time.Now(),
			Attributes: map[string]string{
				"target.service":   "inventory",
				"target.operation": "   ",
				"fault.kind":       kind,
			},
		}
		if err := e.Validate(); err == nil {
			t.Fatalf("expected %s fault without target operation to fail validation", kind)
		}
	}
}

func TestEventValidateServiceCrashRemainsProcessScoped(t *testing.T) {
	e := Event{
		ID:        "e1",
		Kind:      KindFault,
		Source:    SourceFault,
		Timestamp: time.Now(),
		Attributes: map[string]string{
			"target.service": "inventory",
			"fault.kind":     "service_crash",
		},
	}
	if err := e.Validate(); err != nil {
		t.Fatalf("expected process-scoped service crash evidence to validate without target operation: %v", err)
	}
}
