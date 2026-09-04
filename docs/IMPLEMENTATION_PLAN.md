# Living Implementation Plan

This file records the current engineering sequence. It should change when measurements or implementation evidence invalidate assumptions.

## Milestone 0 — baseline inspection

**Status: complete**

- Repository began with one branch (`main`), one initial commit, and a one-line README.
- No existing issues, PRs, workflows, or implementation constraints existed.

## Milestone 1 — replayable vertical slice

**Status: complete for first implementation**

Delivered normalized events, explicit causal edges, conservative divergence evidence, seeded latency injection, a 10-service logical topology, async order-event fan-out, machine-readable outcomes, Level-B replay, tests/CI, and architecture/replay/benchmark/development documentation.

## Milestone 2 — real application telemetry + collector

**Status: in progress**

Completed:

- W3C `traceparent` propagation across synchronous HTTP and async broker fan-out;
- 10 application services as separate OS processes in the integration/demo path;
- standalone bounded collector with overload/error/drop observability;
- bounded service exporter with retry/pending accounting;
- deterministic service telemetry-drain barrier before evidence is judged;
- separate bounded broker process;
- multi-process integration/replay tests across fresh topologies.

Still required:

- OpenTelemetry SDK integration;
- protobuf/gRPC only where measurement justifies them;
- containerized topology;
- broader asynchronous correlation/order assertions.

## Milestone 3 — experiment persistence

**Status: core restart/replay and guarded stale-writer recovery complete; indexing/retention/migration remain**

Completed:

- versioned immutable manifests;
- NDJSON event log plus SHA-256 manifest/event verification;
- path-safe IDs and same-ID writer exclusion;
- deterministic temp staging plus writer reservation metadata;
- atomic publication and strict reload validation;
- separate artifact-replay CLI with no hidden failing-run memory;
- conservative stale-writer recovery that refuses young/live/cross-host state and never mutates committed artifacts.

Still required:

- PostgreSQL metadata/results index;
- explicit retention/redaction configuration;
- schema migration/compatibility policy.

## Milestone 4 — Rust/eBPF evidence

- add Rust toolchain and Linux-only agent;
- start with TCP connect/accept/failure and socket/process lifecycle evidence;
- correlate socket/process identifiers to service spans;
- quantify event loss and CPU overhead;
- demonstrate at least one debugging fact unavailable or ambiguous in application spans alone.

## Milestone 5 — fault corpus + richer replay

**Status: v1 replay-regression corpus complete; broader fault coverage remains in progress**

Completed:

- latency timeout with Level-B artifact replay;
- real TCP connection reset with distinct transport evidence/outcome and separate-process replay;
- orchestrator-owned pre-request inventory process crash with explicit injector evidence, verified unavailability, real `inventory_connection_refused` outcome, separate-process replay, and missing-span localization;
- broker-owned `duplicate_message` schedule for `orders.completed`;
- evidence-first duplicate injection: collector acceptance of the fault record is required before the broker enqueues the duplicated envelope;
- duplicate fan-out preserves the original message ID and delivers two copies to each notification/audit/analytics worker;
- canonical async message-delivery signature counting publishes and per-service consumes while ignoring generated IDs/timestamps;
- artifact replay requires both external outcome parity and async-delivery parity;
- duplicate integration proof of one publish, two consumes per worker, and six graph message edges;
- validation rejects unimplemented kinds and enforces ownership boundaries between service-local, orchestrator, and broker injectors;
- immutable versioned `v1` incident definitions for the four supported slices;
- corpus runner that records each case, validates observed evidence before persistence, reloads checksum-verified artifacts, invokes the separate replay executable, and reports per-case replay parity;
- CI replay-regression gate that executes the corpus and retains the complete corpus-run directory as a GitHub Actions artifact even when the corpus step fails.

Still required:

- service restart and broader crash timing;
- explicit RPC-timeout fault class;
- delayed async message;
- controlled message reordering and Level-C semantics;
- packet-loss mechanism if defensibly targetable.

The v1 corpus exit gate is met: the fixed four-case corpus has automated pass/fail execution and retained replay evidence. This is a regression gate, not a publishable replay-success benchmark.

## Milestone 6 — causal divergence v2

**Status: early improvements landed while building the fault corpus**

Completed so far:

- local latency-delta comparison;
- status/topology fallback while ignoring injector events;
- terminal-service outcome anchor for status change vs missing span;
- real crash integration proof for missing `inventory/check`;
- explicit one-to-many message graph evidence for duplicate async delivery;
- divergence provenance via exact healthy/failing application event IDs plus explicit external terminal-service anchors where used.

Still required:

- richer graph/topology diffs;
- healthy-run distributions instead of one healthy sample;
- retry and message-order changes;
- kernel anomaly features;
- confidence scoring for localization evidence;
- top-k localization evaluation against seeded truth.

## Milestone 7 — deployment and performance engineering

- Docker Compose developer environment;
- Kubernetes manifests/Helm only if justified by benchmark realism;
- steady-state load generator;
- pprof/perf/flamegraph workflow;
- instrumentation on/off paired runs;
- CPU/memory/storage/drop-rate metrics;
- optimize only measured bottlenecks.

## Milestone 8 — resume gate

Before producing final résumé bullets:

- end-to-end demo is multi-process and reproducible;
- supported replay classes have measured success rate;
- causal graph visualization exists;
- low-level telemetry is meaningful;
- CI is comprehensive;
- benchmark methodology is defensible;
- every numeric statement is traceable to `BENCHMARKS.md`.
