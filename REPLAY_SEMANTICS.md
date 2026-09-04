# Replay Semantics

"Replay" is intentionally scoped. Kestrel does not claim general deterministic replay of distributed systems.

## Levels

### Level A — input replay

Reissue recorded requests/events with equivalent input identity and configuration.

**Status:** partially present in the demo harness, but not yet exposed as a standalone recorded-input artifact.

### Level B — failure schedule replay

Reapply a recorded failure schedule: target, trigger, seed, and fault parameters are used in a fresh execution.

**Status:** implemented for the tested latency and connection-reset fault slices.

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

The latency case records/configures a fault at `inventory/check`, runs a failing request, persists the experiment, then creates a fresh system from the recorded artifact and executes again. The replay passes only if the outcome signature matches.

The currently tested signature is a distributed failure with HTTP 504, terminal service `inventory`, and error code `inventory_timeout`.

## Current connection-reset replay

The reset case records a `connection_reset` fault at `inventory/check`. The inventory process terminates the accepted TCP connection with reset semantics instead of returning an HTTP failure response. The order process must observe and classify the transport failure as `inventory_connection_reset`; the recorded signature uses HTTP 502 and terminal service `inventory`.

The integration test persists the original reset evidence and launches `kestrel-artifact-replay` as a separate process. That replay process receives only the artifact directory and a fresh `kestrel-node` binary path. Replay passes only when the fresh topology produces the same semantic outcome signature.

These cases prove narrow but real statements:

- Kestrel can reproduce the tested timeout outcome by replaying a recorded latency failure schedule in the controlled topology.
- Kestrel can reproduce the tested TCP-reset outcome by replaying a recorded connection-reset schedule in the Unix/Linux multi-process topology used by CI.

They do **not** prove:

- deterministic instruction scheduling;
- identical timestamps or durations;
- identical generated trace/span IDs;
- general reproduction of arbitrary network failures;
- identical socket-error presentation across all operating systems or proxies;
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
