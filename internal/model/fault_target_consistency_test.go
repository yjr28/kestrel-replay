package model

import (
	"testing"
	"time"
)

func TestEventValidateFaultRejectsConflictingTargetIdentity(t *testing.T) {
	base := Event{
		ID:        "e1",
		Kind:      KindFault,
		Source:    SourceFault,
		Timestamp: time.Now(),
		Service:   "orders",
		Operation: "create",
		Attributes: map[string]string{
			"fault.kind":       "latency",
			"target.service":   "orders",
			"target.operation": "create",
		},
	}

	if err := base.Validate(); err != nil {
		t.Fatalf("expected matching fault target identity to validate: %v", err)
	}

	formatted := base
	formatted.Service = " orders "
	formatted.Operation = "\tcreate\n"
	formatted.Attributes = map[string]string{
		"fault.kind":       " latency ",
		"target.service":   " orders ",
		"target.operation": " create ",
	}
	if err := formatted.Validate(); err != nil {
		t.Fatalf("expected formatting-only target differences to validate: %v", err)
	}

	serviceConflict := base
	serviceConflict.Service = "payments"
	if err := serviceConflict.Validate(); err == nil {
		t.Fatal("expected fault event service conflicting with target.service to fail validation")
	}

	operationConflict := base
	operationConflict.Operation = "cancel"
	if err := operationConflict.Validate(); err == nil {
		t.Fatal("expected fault event operation conflicting with target.operation to fail validation")
	}
}

func TestEventValidateFaultAllowsAbsentEnvelopeTargetFields(t *testing.T) {
	e := Event{
		ID:        "e1",
		Kind:      KindFault,
		Source:    SourceFault,
		Timestamp: time.Now(),
		Attributes: map[string]string{
			"fault.kind":       "connection_reset",
			"target.service":   "inventory",
			"target.operation": "check",
		},
	}
	if err := e.Validate(); err != nil {
		t.Fatalf("expected attribute-only target identity to remain valid: %v", err)
	}
}

func TestEventValidateServiceCrashRejectsOperationScope(t *testing.T) {
	base := Event{
		ID:        "e1",
		Kind:      KindFault,
		Source:    SourceFault,
		Timestamp: time.Now(),
		Attributes: map[string]string{
			"fault.kind":     "service_crash",
			"target.service": "orders",
		},
	}
	if err := base.Validate(); err != nil {
		t.Fatalf("expected process-scoped service crash evidence to validate: %v", err)
	}

	withTargetOperation := base
	withTargetOperation.Attributes = map[string]string{
		"fault.kind":       "service_crash",
		"target.service":   "orders",
		"target.operation": "create",
	}
	if err := withTargetOperation.Validate(); err == nil {
		t.Fatal("expected service crash target.operation to fail validation")
	}

	withEnvelopeOperation := base
	withEnvelopeOperation.Operation = "create"
	if err := withEnvelopeOperation.Validate(); err == nil {
		t.Fatal("expected service crash envelope operation to fail validation")
	}
}
