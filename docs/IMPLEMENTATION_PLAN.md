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

Known limitations:

- one process owns all logical services;
- custom tracing headers instead of OpenTelemetry;
- in-memory events only;
- in-memory asynchronous transport;
- only latency injection is executed end-to-end;
- no kernel telemetry.

## Milestone 2 — real application telemetry + collector

**Next**

- introduce protobuf API definitions where justified;
- use OpenTelemetry SDKs and W3C trace context;
- run application services as separate processes/containers;
- standalone Kestrel collector;
- bounded ingestion queues and explicit drop accounting;
- collector self-metrics;
- preserve correlation across a real asynchronous broker;
- integration tests that assert parent/message correlations survive process boundaries.

Exit gate: `make demo` runs the multi-process topology and produces the same graph/replay evidence without relying on in-process recorder calls.

## Milestone 3 — experiment persistence

- versioned experiment manifest;
- PostgreSQL metadata/results schema;
- choose and justify high-volume event storage format;
- transaction/integrity model;
- retention/redaction configuration;
- crash/restart tests.

Exit gate: an experiment can be stopped, loaded from storage, graphed, and replayed without hidden in-memory state.

## Milestone 4 — Rust/eBPF evidence

- add Rust toolchain and Linux-only agent;
- start with a small evidence set such as TCP connect/accept/failure and socket lifecycle;
- correlate socket/process identifiers to service spans;
- quantify event loss and CPU overhead;
- build one incident where kernel evidence materially improves attribution over application tracing alone.

Exit gate: documentation demonstrates a real debugging fact visible in eBPF evidence that is absent or ambiguous in normal spans.

## Milestone 5 — fault corpus + richer replay

- connection reset;
- service crash/restart;
- RPC timeout;
- duplicate/delayed async message;
- controlled message reordering;
- packet-loss mechanism if environment permits defensible targeting;
- immutable seeded incident manifests;
- Level-C message-order replay where supported.

Exit gate: fixed corpus with automated replay pass/fail artifacts.

## Milestone 6 — causal divergence v2

- graph/topology diffs;
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
