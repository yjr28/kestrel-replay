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

## Fault classes

Planned classes:

| Class | Status | Replay expectation |
|---|---|---|
| latency | implemented in first slice | Level B failure-schedule replay |
| packet loss | planned | controlled schedule; outcome may vary by TCP behavior |
| connection reset | planned | Level B where injector can target connection/RPC deterministically |
| service crash | planned | Level B |
| service restart | planned | Level B |
| RPC timeout | planned | Level B |
| duplicate message | planned | Level B/C |
| delayed message | planned | Level B/C |
| reordered async messages | planned | Level C where broker/test harness permits ordering control |

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

The demo currently injects latency into `inventory/check`; the order service has a tighter dependency timeout than the upstream proxy layers, preserving the observed terminal cause as `inventory_timeout`.

## What the seed does not guarantee

A seed does not make Linux scheduling, TCP behavior, GC, or arbitrary concurrency deterministic. It guarantees that Kestrel's own randomized injector decisions can be reproduced. Replay success is determined by the resulting outcome signature, not by assuming bit-for-bit identical execution.

## Corpus policy

A future incident corpus should use immutable experiment manifests. A case is only counted in replay-rate metrics if:

1. the original failure was actually observed;
2. the replay was executed from its recorded manifest;
3. the replay result was compared by the documented outcome-signature rules;
4. pass/fail output is retained as an artifact.

Unsupported or flaky cases remain in the corpus with their limitations; they are not silently removed from the denominator after the fact.
