# Localization semantics

Kestrel's localization layer ranks evidence-backed differences between a healthy execution and a failing execution. It is deliberately separate from failure injection: candidate construction does not read `failure_injector` events or seeded corpus truth.

## Evidence used today

The current application-span localizer can use:

- local duration deltas for the same `(service, operation)`;
- application span status changes;
- spans present only in the healthy execution;
- spans present only in the failing execution;
- the externally observed terminal service from the outcome signature as an optional anchor.

Each divergence carries the exact healthy/failing event IDs that support it when those events exist. When the external terminal service affects ranking, the candidate records an `outcome.terminal_service=<service>` anchor.

## Ranked candidates

`graph.RankDivergences` emits deterministic `LocalizationCandidate` values. Each candidate contains a `confidence_score`, `confidence_model`, and the additive `score_basis` used to produce the score.

The current model is `heuristic_v1`. Its score is **not a probability** and must not be described as statistical confidence. It exists only to make ranking rules explicit, deterministic, and reviewable. Terminal-service evidence boosts a candidate for the same service; paired event provenance contributes a small ranking bonus; latency candidates additionally include a bounded severity term relative to the configured threshold.

Tie-breaking is deterministic by service, operation, then reason.

## Seeded-truth evaluation

The v1 incident corpus stores localization truth separately from the graph package. Candidate ranking completes before that truth is consulted. The corpus then evaluates whether the expected `(service, operation)` appears at top-1 and top-3.

Current eligibility is intentionally narrow:

- `inventory-timeout` → `inventory/check`;
- `inventory-connection-reset` → `inventory/check`;
- `inventory-pre-request-crash` → `inventory/check`.

`orders-completed-duplicate` is excluded from span-only localization evaluation. Its replay correctness is currently evaluated through the canonical asynchronous message-delivery signature. It should enter localization evaluation only after message-topology divergence becomes a ranked evidence source.

Each corpus run records one healthy execution as its own checksum-verified artifact and uses that persisted evidence as the baseline for the eligible failing artifacts. This is still a **single healthy sample**, not a healthy-run distribution.

## Current regression observation

GitHub Actions at commit `766db2c` produced one retained v1 corpus run with:

- replay regression: 4/4 cases passed;
- localization top-1: 3/3 eligible cases;
- localization top-3: 3/3 eligible cases.

These numbers describe one deterministic regression run over the current tiny seeded corpus. They are not a publishable replay-success rate, production root-cause accuracy estimate, or statistically meaningful benchmark.

## Next localization work

The next justified steps are:

1. build healthy timing distributions from multiple persisted baseline executions rather than one sample;
2. compare graph/topology and message-delivery structure directly;
3. add retry/order evidence when those fault classes exist;
4. incorporate kernel evidence only after the eBPF agent can demonstrate incremental debugging value;
5. consider calibrated confidence only after there is enough independent evaluation data to justify calibration.
