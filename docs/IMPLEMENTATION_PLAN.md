# Living Implementation Plan

This file records the current engineering sequence. It should change when measurements or implementation evidence invalidate assumptions.

## Milestone 0 — baseline inspection

**Status: complete**

- Repository began with one branch (`main`), one initial commit, and a one-line README.
- No existing issues, PRs, workflows, or implementation constraints existed.

## Milestone 1 — replayable vertical slice

**Status: complete for first implementation**

Delivered:

- normalized event envelope;
- parent-span/message/fault graph edges;
- conservative divergence evidence;
- seeded fault controller;
- latency fault at a real HTTP dependency boundary;
- 10-service logical topology using real local TCP listeners;
- asynchronous order-event fan-out to notification/audit/analytics workers;
- outcome signature + Level-B failure-schedule replay;
- healthy/failure/replay end-to-end test;
- CLI demo;
- unit tests, `go vet`, and CI;
- architecture/failure/replay/benchmark/development docs.

Limitations at the end of Milestone 1 (some have since been removed in later milestones):

- one process owned all logical services;
- custom tracing headers instead of OpenTelemetry;
- in-memory events only;
- in-memory asynchronous transport;
- only latency injection executed end-to-end;
- no kernel telemetry.

## Milestone 2 — real application telemetry + collector

**Status: in progress**

Completed in phase 2A/2B/2C:

- W3C `traceparent` propagation across synchronous HTTP and asynchronous broker fan-out;
- 10 application services run as separate OS processes in the integration/demo path;
- standalone collector process;
- bounded collector ingestion queue with explicit HTTP 429 overload behavior and dropped/invalid counters;
- collector self-metrics and event query endpoint;
- bounded telemetry exporter queue with sent/dropped/error counters;
- bounded retry of transient collector-delivery failures plus pending-delivery accounting;
- explicit service telemetry-drain barrier that waits for active handlers and exporter delivery before experiment evidence is judged;
- separate broker process with bounded queue and delivery/error stats;
- multi-process integration tests that validate healthy → failing → replay, causal graph construction, and divergence localization across fresh topologies.

Still required before milestone completion:

- OpenTelemetry SDK integration once the dependency is available in a buildable environment;
- protobuf/gRPC boundaries where measurement shows they are justified;
- containerized process topology;
- stronger asynchronous correlation assertions and broker fault modes.

Partial exit gate achieved: `make demo` now runs the multi-process topology and produces graph/replay evidence through the standalone collector. The remaining gate is replacing the current lightweight instrumentation with actual OpenTelemetry SDK instrumentation without regressing the behavior.

## Milestone 3 — experiment persistence

**Status: core restart/replay and guarded stale-writer recovery complete; indexing/retention/migration remain**

Completed in phase 3A/3B/3C:

- versioned immutable experiment manifest;
- NDJSON event log chosen as the initial append-friendly portable event format;
- separate checksum metadata with SHA-256 verification of manifest and event bytes;
- path-safe experiment identifiers and same-ID writer exclusion;
- deterministic `<experiment-id>.tmp` staging path and JSON writer reservation with PID/hostname/timestamp metadata;
- atomic temp-directory → committed-directory publication;
- strict reload validation for schema, checksums, and event contents;
- default demo persists the failing run before graph/replay analysis;
- standalone artifact-replay CLI consumes only a persisted artifact plus a fresh node binary;
- integration tests prove persisted multiprocess failures can be reloaded, graphed, and replayed by a separate process with no hidden failing-run memory;
- explicit artifact-recovery API/CLI that refuses young, live-owner, or cross-host reservations and removes only stale same-host dead-owner state;
- recovery tests proving committed artifacts are never mutated by cleanup.

Still required:

- PostgreSQL metadata/results index (artifact bytes remain the source evidence);
- explicit retention/redaction configuration;
- migration/compatibility policy for future manifest/event schema versions.

Exit gate: **achieved for the currently supported fault slices.** An experiment can be stopped, loaded from storage, graphed, and replayed without hidden in-memory state, and abandoned writer state can be recovered conservatively after abrupt process death. PostgreSQL is deliberately not claimed as implemented.

## Milestone 4 — Rust/eBPF evidence

- add Rust toolchain and Linux-only agent;
- start with a small evidence set such as TCP connect/accept/failure and socket lifecycle;
- correlate socket/process identifiers to service spans;
- quantify event loss and CPU overhead;
- build one incident where kernel evidence materially improves attribution over application tracing alone.

Exit gate: documentation demonstrates a real debugging fact visible in eBPF evidence that is absent or ambiguous in normal spans.

## Milestone 5 — fault corpus + richer replay

**Status: in progress**

Completed:

- latency timeout failure with Level-B artifact replay;
- real TCP connection-reset injection at `inventory/check` using a forced socket reset rather than an HTTP error;
- distinct reset transport evidence and `inventory_connection_reset` outcome classification;
- separate-process artifact replay test for the reset failure;
- orchestrator-owned real inventory-process crash after the topology has become healthy and before the workload request;
- explicit persisted crash injector evidence plus verified target unavailability before workload execution;
- real refused-connection outcome classification as HTTP 502 / `inventory_connection_refused` rather than a synthetic application error;
- separate-process artifact replay that repeats the recorded pre-request process-kill schedule in a fresh topology;
- terminal-service-anchored localization that identifies the missing healthy `inventory/check` span in the crash artifact without consulting injector events;
- validation that rejects declared-but-unimplemented fault kinds instead of silently no-oping them, while keeping process-lifecycle faults out of the in-service controller.

Still required:

- service restart and broader crash timing beyond the current pre-request inventory-only slice;
- RPC timeout as an explicit caller-side fault class;
- duplicate/delayed async message;
- controlled message reordering;
- packet-loss mechanism if the environment permits defensible targeting;
- immutable versioned incident corpus and replay-regression runner;
- Level-C message-order replay where supported.

Exit gate: fixed corpus with automated replay pass/fail artifacts.

## Milestone 6 — causal divergence v2

**Status: early improvements landed while building the fault corpus**

Completed so far:

- local latency-delta comparison across healthy/failing application spans;
- status/topology fallback while ignoring explicit injector events;
- terminal-service outcome anchor for distinguishing an affected-service status change from a missing healthy span;
- real crash integration proof that localizes a missing `inventory/check` span from persisted application evidence.

Still required:

- graph/topology diffs beyond span-presence heuristics;
- healthy-run distributions instead of a single healthy sample;
- retry and message-order changes;
- kernel anomaly features;
- evidence provenance/confidence;
- top-k localization evaluation against seeded incident truth.

Exit gate: localization metrics on a versioned corpus.

## Milestone 7 — deployment and performance engineering

- Docker Compose developer environment;
- Kubernetes manifests/Helm if justified by benchmark realism;
- steady-state load generator;
- pprof/perf/flamegraph workflow;
- instrumentation on/off paired runs;
- CPU/memory/storage/drop-rate metrics;
- optimize only measured bottlenecks.

Exit gate: reproducible benchmark artifacts and populated `BENCHMARKS.md` result table.

## Milestone 8 — resume gate

Before producing final résumé bullets:

- end-to-end demo is multi-process and reproducible;
- supported replay classes have measured success rate;
- causal graph visualization exists;
- low-level telemetry is meaningful;
- CI is comprehensive;
- benchmark methodology is defensible;
- every numeric statement is traceable to `BENCHMARKS.md`.
