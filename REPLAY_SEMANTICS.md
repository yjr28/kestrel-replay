# Replay Semantics

"Replay" is intentionally scoped. Kestrel does not claim general deterministic replay of distributed systems.

## Levels

### Level A — input replay

Reissue recorded requests/events with equivalent input identity and configuration.

**Status:** partially present in the demo harness, but not yet exposed as a standalone recorded-input artifact.

### Level B — failure schedule replay

Reapply a recorded failure schedule: target, trigger, seed, and fault parameters are used in a fresh execution.

**Status:** implemented for the first latency-fault vertical slice.

### Level C — message-order replay

Control asynchronous delivery so recorded relative ordering can be reproduced for supported message paths.

**Status:** planned.

### Level D — selected deterministic execution replay

Control enough nondeterminism for a narrowly defined component/execution region to reproduce internal execution decisions.

**Status:** not implemented; no claim is made that arbitrary Go/Linux distributed execution can reach this level.

## Outcome signature

The current replay comparison uses a machine-readable signature containing:

- classification;
- HTTP status;
- terminal service;
- error code;
- causal path.

The signature is also hashed to a short digest for terminal output. Replay success requires semantic field equality, not digest equality alone.

## Current latency replay

The demo records/configures a latency fault at `inventory/check`, runs a failing request, then creates a fresh system with the same fault spec and executes again. The replay passes only if the outcome signature matches.

This proves a narrow but real statement:

> Kestrel can reproduce the tested timeout outcome by replaying the recorded latency failure schedule in the current controlled demo topology.

It does **not** prove:

- deterministic instruction scheduling;
- identical timestamps or durations;
- identical generated trace/span IDs;
- general reproduction of arbitrary network failures;
- deterministic replay across Kubernetes nodes.

## Future replay reports

Each incident report should include:

- supported replay level;
- exact manifest used;
- original signature;
- replay signature;
- pass/fail reason;
- number of attempts if retries are part of the benchmark protocol;
- known nondeterministic dimensions.
