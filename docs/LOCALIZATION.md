# Localization semantics

Kestrel's localization layer ranks evidence-backed differences between healthy executions and a failing execution. It is deliberately separate from failure injection: candidate construction does not read `failure_injector` events or seeded corpus truth.

## Evidence used today

The current application-span localizer can use:

- empirical duration envelopes from multiple healthy executions for the same `(service, operation)`;
- application span status changes;
- spans present in healthy executions but missing from the failing execution;
- spans present only in the failing execution;
- the externally observed terminal service from the outcome signature as an optional anchor.

Each divergence carries the exact healthy/failing event IDs that support it when those events exist. When the external terminal service affects ranking, the candidate records an `outcome.terminal_service=<service>` anchor.

The corpus currently builds its healthy span profile from three separately recorded healthy executions. The profile stores descriptive timing/count statistics; these are empirical regression baselines, not calibrated probability models.

## Ranked candidates

`graph.RankDivergences` emits deterministic `LocalizationCandidate` values. Each candidate contains a `confidence_score`, `confidence_model`, and the additive `score_basis` used to produce the score.

The current model is `heuristic_v1`. Its score is **not a probability** and must not be described as statistical confidence. It exists only to make ranking rules explicit, deterministic, and reviewable. Terminal-service evidence boosts a candidate for the same service; paired event provenance contributes a small ranking bonus; latency candidates additionally include a bounded severity term relative to the configured healthy profile.

Tie-breaking is deterministic by service, operation, then reason.

## Async message-topology evidence

Kestrel also builds an empirical message-topology profile from the same healthy-run set. For each `(topic, action, service)` flow it records the observed count envelope and compares failing evidence against that envelope.

`graph.CompareMessageTopology` reports only application message evidence and includes the exact failing event IDs that support a multiplicity divergence. It can distinguish:

- count above the healthy range;
- count below the healthy range;
- an unexpected message flow.

The current duplicate-delivery corpus case is evaluated with seeded truth outside the graph comparator. The expected observation is one `orders.completed` publish and duplicated consumes at notification, audit, and analytics. This is a structural regression check; it does not claim that consumer-side multiplicity alone identifies an infrastructure root cause.

## Seeded-truth evaluation

The v1 incident corpus stores localization truth separately from the graph package. Candidate ranking completes before that truth is consulted. The corpus then evaluates whether the expected `(service, operation)` appears at top-1 and top-3.

Span-localization eligibility remains intentionally narrow:

- `inventory-timeout` → `inventory/check`;
- `inventory-connection-reset` → `inventory/check`;
- `inventory-pre-request-crash` → `inventory/check`.

`orders-completed-duplicate` is still excluded from span-localization top-k evaluation. It has its own message-topology regression gate because the currently observed divergence is a one-to-many consume multiplicity change rather than a single application-span culprit.

## Current regression observation

GitHub Actions at commit `43ad283` produced one retained v1 corpus run with:

- replay regression: 4/4 cases passed;
- localization top-1: 3/3 span-localization-eligible cases;
- localization top-3: 3/3 span-localization-eligible cases;
- message-topology validation: 1/1 eligible case.

The same run used three healthy executions to construct the span and message-topology profiles. These numbers describe one deterministic regression run over the current tiny seeded corpus. They are not a publishable replay-success rate, production root-cause accuracy estimate, or statistically meaningful benchmark.

## Next localization work

The next justified steps are:

1. enrich graph/topology comparison beyond current span and message-count envelopes;
2. add retry/order evidence only when those behaviors and fault classes are implemented end to end;
3. incorporate kernel evidence only after the eBPF agent can demonstrate incremental debugging value;
4. expand healthy-run samples and evaluation cases before treating empirical envelopes as representative;
5. consider calibrated confidence only after there is enough independent evaluation data to justify calibration.
