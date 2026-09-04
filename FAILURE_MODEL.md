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
| service crash | planned; rejected by current validator | Level B |
| service restart | planned; rejected by current validator | Level B |
| RPC timeout | planned; rejected by current validator | Level B |
| duplicate message | planned; rejected by current validator | Level B/C |
| delayed message | planned; rejected by current validator | Level B/C |
| reordered async messages | planned; rejected by current validator | Level C where broker/test harness permits ordering control |

## Implemented latency fault

`internal/fault` defines a fault spec with:

- `kind`;
- `target_service`;
- optional `operation`;
- `trigger_on_match`;
- base delay;
- optional jitter fraction;
- seed.

A controller counts matching execution points and fires on the configured match. Jitter, when enabled, is generated from the recorded seed so the chosen injected delay is repeatable.

The latency integration case injects delay into `inventory/check`; the order service has a tighter dependency timeout than the upstream proxy layers, preserving the observed terminal cause as `inventory_timeout` with HTTP 504.

## Implemented connection-reset fault

The connection-reset case targets the same `inventory/check` boundary but does not simulate failure with an HTTP error. The inventory service hijacks the accepted HTTP connection, applies TCP linger zero when the underlying connection is TCP, and closes it so the Linux integration environment observes a real reset at the caller.

The application instrumentation records two pieces of evidence:

- an explicit fault-injector event with `fault.kind=connection_reset`;
- the affected inventory span with `status=error`, `http.status_code=0`, and `transport.error=connection_reset`.

The order client classifies an actual `ECONNRESET` separately from deadline expiry. The tested outcome signature is HTTP 502, terminal service `inventory`, error code `inventory_connection_reset`. Other unrecognized transport errors remain `inventory_transport_error`; they are not relabeled as resets just because a reset fault was requested.

The multi-process integration test persists this failure, reloads the artifact, launches a separate replay process with the recorded fault schedule, and requires the replayed outcome signature to match.

## What the seed does not guarantee

A seed does not make Linux scheduling, TCP behavior, GC, or arbitrary concurrency deterministic. It guarantees that Kestrel's own randomized injector decisions and trigger schedule can be reproduced. Replay success is determined by the resulting outcome signature, not by assuming bit-for-bit identical execution.

For connection reset, the current proven environment is the Unix/Linux process topology used by CI and the demo architecture. This is not a claim that every operating system, proxy, service mesh, or Kubernetes network path will surface reset errors identically.

## Corpus policy

A future incident corpus should use immutable experiment manifests. A case is only counted in replay-rate metrics if:

1. the original failure was actually observed;
2. the replay was executed from its recorded manifest;
3. the replay result was compared by the documented outcome-signature rules;
4. pass/fail output is retained as an artifact.

Unsupported or flaky cases remain in the corpus with their limitations; they are not silently removed from the denominator after the fact.
