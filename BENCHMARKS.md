# Benchmarks

## Status

No production benchmark result has been promoted yet. There are currently **no defensible public claims** for requests/sec, p95 overhead, CPU/memory overhead, storage rate, replay success percentage, or root-cause localization accuracy.

The existing Go benchmark is a development smoke microbenchmark that repeatedly creates and tears down the first vertical-slice topology. It is not representative of steady-state distributed throughput and must not be used in a résumé.

## Reproducibility gate

A number may appear in the README or résumé only when this file records:

- git commit SHA;
- hardware/VM/container environment;
- OS/kernel version;
- workload generator and parameters;
- deployment topology;
- Kestrel configuration/sampling;
- warmup duration;
- measured duration;
- repetitions;
- raw result artifact location;
- summary statistic computation;
- known caveats.

## Required benchmark families

### Throughput and latency

Compare the same application topology with:

1. baseline instrumentation disabled;
2. application tracing only;
3. full supported Kestrel capture.

Report requests/sec and p50/p95/p99 latency. Instrumentation overhead must be computed from paired runs under equivalent load.

### Resource overhead

Measure at minimum:

- application CPU;
- collector CPU;
- eBPF agent CPU;
- RSS/memory;
- event bytes/sec;
- persisted bytes/sec;
- event drop rate;
- collector/write latency.

### Replay success

For a fixed, versioned seeded incident corpus:

`replay_success = matching_outcome_signatures / attempted_supported_incidents`

The denominator must include every attempted incident in a declared supported class. Unsupported classes should be reported separately, not omitted silently.

### Root-cause localization

Preferred automated metrics:

- top-1 / top-k injected-cause localization accuracy;
- candidate-set reduction relative to all involved services/events;
- time-to-first-correct-cause in a scripted triage workflow.

A human-time claim such as "18.4 min → 2.7 min" will not be used unless a defensible blinded/repeated study is actually conducted.

## Development command

```bash
make benchmark
```

This command currently runs only the vertical-slice smoke benchmark. A dedicated steady-state load generator and artifact writer will replace it before any performance claim is published.

## Target hypotheses

These are goals to investigate, not current results:

- 25k+ req/s in the benchmark topology;
- <4.20% p95 latency overhead at the declared capture configuration;
- >95% outcome reproduction for explicitly supported fault classes;
- strong automated root-cause localization on a fixed seeded corpus.
