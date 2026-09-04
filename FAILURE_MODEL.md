# Failure Model

## Principles

Every injected failure must be reproducible enough to identify *what was requested*, even when the operating system or scheduler makes the exact resulting timing nondeterministic.

Each experiment record is expected to contain:

- seed;
- workload identity/version;
- topology/configuration;
- failure kind;
- target service/operation;
- trigger position;
- injected parameters;
- injection timestamp;
- expected behavior;
- observed outcome signature.

A fault kind is accepted by `fault.Spec.Validate` only when the runtime actually implements it. Enum values may exist ahead of implementation for design clarity, but unsupported kinds are rejected rather than silently behaving as no-ops.

## Fault classes

| Class | Status | Replay expectation |
|---|---|---|
| latency | implemented | Level B failure-schedule replay |
| packet loss | planned; rejected by current validator | controlled schedule; outcome may vary by TCP behavior |
| connection reset | implemented | Level B failure-schedule replay |
| service crash | implemented for orchestrated pre-request `inventory` crash | Level B failure-schedule replay |
| service restart | planned; rejected by current validator | Level B |
| RPC timeout | planned; rejected by current validator | Level B |
| duplicate message | implemented for broker `orders.completed` fan-out | Level B schedule replay with async-delivery evidence parity |
| delayed message | planned; rejected by current validator | Level B/C |
| reordered async messages | planned; rejected by current validator | Level C where broker/test harness permits ordering control |

## Implemented latency fault

`internal/fault` defines a fault spec with kind, target service/operation, trigger position, delay/jitter parameters where applicable, and seed. A controller counts matching execution points and fires on the configured match. Jitter, when enabled, is generated from the recorded seed so Kestrel's own randomized choice is repeatable.

The latency integration case injects delay into `inventory/check`; the order service has a tighter dependency timeout than the upstream proxy layers, preserving the observed terminal cause as `inventory_timeout` with HTTP 504.

## Implemented connection-reset fault

The connection-reset case targets `inventory/check` but does not simulate failure with an HTTP error. The inventory service hijacks the accepted HTTP connection, applies TCP linger zero when the underlying connection is TCP, and closes it so the Linux integration environment observes a real reset at the caller.

Kestrel records an explicit reset fault event plus an errored inventory span with `transport.error=connection_reset`. The tested outcome is HTTP 502, terminal service `inventory`, error code `inventory_connection_reset`. A separate artifact-replay process must reproduce the same semantic outcome.

## Implemented service-crash fault

The current service-crash slice is orchestrator-owned rather than service-local. The topology starts healthy, Kestrel records the pre-request crash schedule, kills the real inventory OS process, confirms its health endpoint is unavailable, and only then issues the workload.

The tested Unix/Linux loopback environment produces a real refused TCP connection. The caller classifies it as HTTP 502, terminal service `inventory`, error code `inventory_connection_refused`. There is no `inventory/check` request span because the process was already dead; localization can report that healthy span as `missing_span` using the external terminal-service outcome as an anchor without reading the injector event for the answer.

Current crash scope is explicit: `inventory`, before request, `trigger_on_match=1`, Unix-oriented child-process lifecycle. General mid-request crashes, restart sequences, and Kubernetes lifecycle replay are not claimed.

## Implemented duplicate-message fault

The duplicate-message slice is broker-owned. Its current supported target is `broker/orders.completed`; the single-request integration harness supports `trigger_on_match=1`.

When the trigger fires:

1. the broker synchronously records a fault event in the collector containing the request/trace correlation, original `message.id`, seed, trigger, target operation, and `duplicate.extra_copies=1`;
2. only after the collector accepts that evidence does the broker enqueue the faulty delivery;
3. the original envelope is delivered twice to each of notification, audit, and analytics while preserving the same message ID.

The synchronous create-order request still returns HTTP 201. Therefore HTTP outcome equality alone is insufficient to claim replay. Kestrel derives a canonical message-delivery signature for `orders.completed` that ignores generated trace/span/message identities and timestamps but counts application publishes plus consumes per service.

The tested duplicate signature is one publish and two consumes at each of notification, audit, and analytics. The persisted causal graph contains six message edges from the one publisher to six consume events. Artifact replay succeeds only when both the external outcome and canonical async-delivery signature match the recorded incident.

The in-service controller rejects duplicate-message specs because the broker owns async delivery scheduling. Delayed/reordered delivery is not yet implemented or implied by this slice.

## What the seed does not guarantee

A seed does not make Linux scheduling, TCP behavior, GC, or arbitrary concurrency deterministic. It guarantees that Kestrel's own randomized injector decisions and trigger schedule can be reproduced. Replay success is determined by supported semantic evidence, not by assuming bit-for-bit identical execution.

## Corpus policy

A future incident corpus should use immutable experiment manifests. A case is only counted in replay-rate metrics if:

1. the original failure was actually observed;
2. the replay was executed from its recorded manifest;
3. the replay result was compared by the documented outcome/evidence-signature rules;
4. pass/fail output is retained as an artifact.

Unsupported or flaky cases remain in the corpus with their limitations; they are not silently removed from the denominator after the fact.
