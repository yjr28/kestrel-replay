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
make demo-inprocess
make benchmark
make artifact-replay ARTIFACT=.kestrel/experiments/<id>
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
- deterministic service-local fault-controller tests;
- ownership/validation tests separating service-local, orchestrator-owned, and broker-owned fault classes;
- causal graph/divergence tests including missing-span crash localization;
- external outcome-signature tests;
- canonical async message-delivery signature tests that ignore generated IDs/timestamps while preserving publish/consume multiplicity;
- collector queue/overload tests;
- exporter transient-delivery retry and deterministic telemetry-drain coverage;
- W3C trace-context tests;
- real TCP reset and refused-connection classification tests;
- broker unit tests proving duplicate delivery preserves message identity and refuses to inject until collector evidence is accepted;
- immutable artifact integrity/schema/immutability and guarded stale-writer recovery tests;
- multi-process artifact replay for latency, TCP reset, pre-request inventory crash, and broker duplicate-message delivery across 10 service processes + broker + collector;
- duplicate integration assertions requiring one `orders.completed` publish, two consumes at each worker, six graph message edges, and separate-process async-signature replay parity.

Planned:

- property tests for graph invariants and event normalization;
- delayed/reordered broker fault tests and eventual Level-C ordering semantics;
- service restart and explicit RPC-timeout replay tests;
- retention/schema-migration tests for persisted experiments;
- eBPF integration tests on Linux CI runners;
- seeded corpus replay regression suite.

## CI

GitHub Actions runs tests and `go vet` on pushes and pull requests. Linux-specific eBPF CI will be added only when the agent exists and its privilege/runtime requirements are explicit.

The multi-process harness treats evidence completeness as a correctness property. Services expose a telemetry drain barrier that waits for active handlers and pending exporter delivery; orchestration invokes it before reading experiment evidence. Broker-owned duplicate injection separately requires collector acceptance of the injector event before enqueueing the faulty delivery. Tests should not compensate for missing telemetry by lowering evidence-count expectations or adding arbitrary sleeps.

## Repository policy for measured claims

Never copy a target metric into README prose as if measured. Any numeric performance/replay/localization claim must have a reproducible benchmark entry in `BENCHMARKS.md` and retained raw artifacts.
