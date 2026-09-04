# Development

## Requirements

Current slice:

- Go 1.23+
- Linux/macOS for core Go development

Future eBPF development will require Linux, a supported kernel/BTF setup, Rust/Cargo, clang/LLVM as appropriate, and explicit privileges/capabilities.

## Commands

```bash
make test
make vet
make check
make integration
make demo
# fast legacy in-process harness
make demo-inprocess
make benchmark
# replay a previously persisted incident
make artifact-replay ARTIFACT=.kestrel/experiments/<id>
# guarded cleanup of stale writer state after abrupt process death
make artifact-recover EXPERIMENT=<id> STALE_AFTER=15m
```

## Engineering workflow

Work in vertical slices. Every substantial change should:

1. define the observable behavior/invariant first;
2. implement the smallest coherent slice;
3. add unit/integration/replay tests;
4. run `make check`;
5. run the demo or relevant experiment;
6. inspect the output instead of assuming success;
7. commit logically.

Do not add untestable infrastructure just to populate directories.

## Testing layers

Current:

- normalized event validation unit tests;
- deterministic fault-controller unit tests;
- explicit rejection tests for fault kinds that are declared but not implemented, plus separation between service-local and orchestrator-owned fault classes;
- causal graph and divergence unit tests, including terminal-service status changes and missing-span crash localization;
- outcome-signature unit tests;
- collector queue/overload and exporter tests;
- exporter transient-delivery retry and deterministic telemetry-drain coverage;
- W3C trace-context tests;
- real TCP connection-reset injection and transport-classification tests;
- refused-connection transport classification for a killed dependency;
- immutable experiment artifact integrity/schema/immutability tests;
- guarded stale-writer recovery tests covering dead-owner cleanup, live-owner refusal, young-reservation refusal, and committed-artifact preservation;
- multi-process latency, TCP connection-reset, and pre-request inventory service-crash artifact replay tests across 10 service processes + broker + collector;
- service-crash evidence assertions proving an explicit injector record exists while the killed inventory process emits no request span;
- persisted crash localization asserting the missing healthy `inventory/check` span is found using the recorded terminal service rather than the injector event.

Planned:

- property tests for graph invariants and event normalization;
- broker/message-order fault tests;
- service restart and explicit RPC-timeout replay tests;
- retention and schema-migration tests for persisted experiments;
- eBPF integration tests on Linux CI runners;
- seeded corpus replay regression suite.

## CI

GitHub Actions runs tests and `go vet` on pushes and pull requests. Linux-specific eBPF CI will be added only when the agent exists and its privilege/runtime requirements are explicit.

The multi-process harness treats evidence completeness as a correctness property. Services expose a telemetry drain barrier that waits for active request handlers and pending exporter delivery; orchestration invokes that barrier before reading experiment evidence. Tests should not compensate for missing telemetry by lowering event-count expectations or adding arbitrary sleeps.

## Repository policy for measured claims

Never copy a target metric into README prose as if measured. Any numeric performance/replay/localization claim must have a reproducible benchmark entry in `BENCHMARKS.md` and retained raw artifacts.
