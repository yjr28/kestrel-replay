# Replay Semantics

"Replay" is intentionally scoped. Kestrel does not claim general deterministic replay of distributed systems.

## Levels

### Level A — input replay

Reissue recorded requests/events with equivalent input identity and configuration.

**Status:** partially present in the demo harness, but not yet exposed as a standalone recorded-input artifact.

### Level B — failure schedule replay

Reapply a recorded failure schedule: target, trigger, seed, and fault parameters are used in a fresh execution.

**Status:** implemented for the tested latency, connection-reset, orchestrated service-crash, broker duplicate-message, and broker delayed-message slices.

### Level C — message-order replay

Control asynchronous delivery so recorded relative ordering can be reproduced for supported message paths.

**Status:** not yet implemented. Duplicate or delayed delivery does not by itself claim ordering control.

### Level D — selected deterministic execution replay

Control enough nondeterminism for a narrowly defined component/execution region to reproduce internal execution decisions.

**Status:** not implemented; no claim is made that arbitrary Go/Linux distributed execution can reach this level.

## External outcome signature

The current external replay comparison uses a machine-readable signature containing:

- classification;
- HTTP status;
- terminal service;
- error code;
- causal path.

The signature is also hashed to a short digest for terminal output. Semantic field equality is required.

## Async message-delivery signature

An external HTTP outcome is insufficient for faults that alter asynchronous side effects while preserving request success. Kestrel therefore also derives a canonical message-delivery signature for `orders.completed` containing:

- publish count;
- consume count per service.

Generated message/span IDs and timestamps are deliberately excluded so a fresh replay process can be compared semantically. `kestrel-artifact-replay` currently requires both external outcome equality and message-delivery-signature equality. Synchronous failing incidents naturally compare with zero `orders.completed` deliveries.

## Async message-delay evidence

For the implemented delayed-message slice, matching HTTP outcome and delivery multiplicity are still insufficient. Kestrel correlates application publish and consume evidence by `message.id`, then derives the minimum observed publish-to-consume delay for the target topic. Both the recorded incident and the fresh replay must meet the configured positive delay threshold.

Delay comparison is deliberately threshold-based rather than exact wall-clock equality. Correlation is withheld when a consume predates its publish or when the same message ID has multiple observed publish events, because those records do not provide unambiguous publish provenance. This is evidence gating for the tested broker-delay slice, not a claim of deterministic queue timing or controlled message ordering.

## Current latency replay

The latency case records/configures a fault at `inventory/check`, persists the failure, then creates a fresh system from the recorded artifact. The tested signature is HTTP 504, terminal service `inventory`, error code `inventory_timeout`.

## Current connection-reset replay

The reset case records a `connection_reset` fault at `inventory/check`. The inventory process terminates the accepted TCP connection with reset semantics instead of returning an HTTP failure response. The tested signature is HTTP 502, terminal service `inventory`, error code `inventory_connection_reset`.

## Current service-crash replay

The crash case is an orchestrator-owned pre-request schedule for `inventory`. A fresh topology is made healthy, Kestrel records the crash schedule, kills the real inventory process, confirms unavailability, and then issues the workload. The tested Unix/Linux signature is HTTP 502, terminal service `inventory`, error code `inventory_connection_refused`.

The healthy execution contains an `inventory/check` span while the failing execution does not. Localization can use the persisted external terminal service as an application-evidence anchor to report that missing span without consulting injector events.

## Current duplicate-message replay

The duplicate case targets `broker/orders.completed`. Before duplicating a delivery, the broker requires the collector to accept explicit injector evidence tied to the original message ID. It then delivers the same envelope twice to each of notification, audit, and analytics.

The synchronous request remains successful (HTTP 201), so a healthy run would have the same external outcome. Replay therefore passes only if the canonical async signature also matches: one publish and two consumes per worker in the currently tested case. The graph correspondingly contains six publish→consume message edges.

A separate `kestrel-artifact-replay` process reconstructs the recorded evidence, starts a fresh topology, reapplies the broker schedule, and requires both outcome and async-delivery parity. This is Level-B duplicate-delivery schedule replay; it is **not** yet Level-C message-order replay.

## Current delayed-message replay

The delayed case also targets `broker/orders.completed`. The broker records explicit delayed-message injector evidence before applying the configured positive delay, while preserving one delivery per notification, audit, and analytics worker.

Artifact replay requires external-outcome equality, message-delivery-signature equality, and minimum-delay evidence in both the recorded incident and the fresh replay. Exact timestamps are not compared, delayed-message jitter remains outside the implemented replay semantics, and this slice does not claim Level-C ordering control.

These cases prove narrow but real statements:

- Kestrel can reproduce the tested timeout outcome by replaying a recorded latency schedule.
- Kestrel can reproduce the tested TCP-reset outcome by replaying a recorded reset schedule.
- Kestrel can reproduce the tested pre-request inventory crash outcome by replaying a recorded process-kill schedule.
- Kestrel can reproduce the tested duplicate async fan-out by replaying a recorded broker duplicate schedule and matching canonical per-consumer delivery counts.
- Kestrel can reproduce the tested delayed async fan-out by replaying a recorded broker delay schedule and requiring the configured minimum delay in both recorded and replayed evidence.

They do **not** prove deterministic instruction scheduling, identical timing/IDs, arbitrary network-failure replay, arbitrary process lifecycle replay, controlled async ordering, delayed-message jitter replay, or deterministic replay across Kubernetes nodes.

## Future replay reports

Each incident report should include:

- supported replay level;
- exact manifest used;
- original and replayed external outcome signatures;
- applicable asynchronous evidence signature;
- pass/fail reason;
- number of attempts if retries are part of the benchmark protocol;
- known nondeterministic dimensions.
