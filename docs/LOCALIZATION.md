# Localization semantics

Kestrel's localization layer ranks evidence-backed differences between healthy executions and a failing execution. It is deliberately separate from failure injection: candidate construction does not read `failure_injector` events or seeded corpus truth.

## Evidence used today

The current application-span localizer can use:

- empirical duration envelopes from multiple healthy executions for the same `(service, operation)`;
- application span status changes;
- spans present in healthy executions but missing from the failing execution;
- spans present only in the failing execution;
- the externally observed terminal service from the outcome signature as an optional anchor.

Application-span evidence must have a non-blank event ID before it can establish or satisfy a divergence. This eligibility rule applies to ranked single-run localization, the earlier single-divergence primitive, and multi-run healthy span profiles. It keeps provenance auditable: an unnamed span cannot silently replace a missing healthy/failing observation, create an `unexpected_span` candidate, or mark a healthy topology key as observed. Single-run divergences carry the exact healthy/failing event IDs that support the comparison when those events exist. Multi-run profile baselines retain the eligible event ID from every healthy run contributing to a stable baseline and also retain one representative healthy event ID for current profile-vs-failing ranking output. Profile-vs-failing candidates therefore still expose the representative healthy ID plus the exact failing event ID; the complete healthy provenance remains available from the profile baseline rather than being implied by that representative candidate field. When the external terminal service affects ranking, the candidate records an `outcome.terminal_service=<service>` anchor.

Retry semantics are not modeled by the current span localizers. For single-run comparisons, more than one identified application span with the same `(service, operation)` makes that key ambiguous, so ranked localization and the earlier single-divergence primitive abstain on that key rather than choosing an event by timestamp or input order. Profile-vs-failing localization applies the same rule to duplicate keys in the failing run, preventing ambiguity from being mislabeled as a missing span. Reusing the same identified span event ID within a run is also ambiguous provenance: failing-run localization excludes those spans from evidence, while healthy-profile construction rejects the healthy run rather than choosing one observation. Healthy-profile construction likewise rejects duplicate eligible spans for the same `(service, operation)` key. Unidentified spans remain ineligible evidence and therefore do not create either ambiguity.

The corpus currently builds its healthy span profile from three separately recorded healthy executions. The profile stores descriptive timing/count statistics; these are empirical regression baselines, not calibrated probability models.

## Ranked candidates

`graph.RankDivergences` emits deterministic `LocalizationCandidate` values. Each candidate contains a `confidence_score`, `confidence_model`, and the additive `score_basis` used to produce the score.

The current model is `heuristic_v1`. Its score is **not a probability** and must not be described as statistical confidence. It exists only to make ranking rules explicit, deterministic, and reviewable. Terminal-service evidence boosts a candidate for the same service; paired event provenance contributes a small ranking bonus; latency candidates additionally include a bounded severity term relative to the configured healthy profile.

Tie-breaking is deterministic by service, operation, then reason.

## Async message-topology evidence

Kestrel also builds an empirical message-topology profile from the same healthy-run set. Only application message events with a non-blank event ID, non-blank `topic`, supported `message.action`, and non-blank `message.id` are eligible to affect this profile or a failing-run topology comparison. The event ID is required because topology divergences expose exact event provenance; an event that cannot be named cannot establish or satisfy that evidence. The message ID remains a separate eligibility requirement for message correlation metadata. If the same non-blank event ID appears on more than one application message event in a run, all application message events bearing that ID are excluded from topology evidence rather than choosing one by timestamp or input order; otherwise the reported event ID would not identify a unique supporting observation.

For each eligible `(topic, action, service)` flow it records the observed count envelope plus per-run evidence: the count and exact application event IDs observed in each healthy run. An absent flow in a particular healthy run is represented explicitly as a zero count with no event IDs.

`graph.CompareMessageTopology` reports only eligible application message evidence. For flows that exceed or fall below a healthy envelope, the divergence carries the per-run healthy evidence and the exact failing event IDs. For a flow never seen in the healthy profile, the divergence carries an explicit zero-count record for every healthy run together with the failing events that establish the newly observed flow. It can distinguish:

- count above the healthy range;
- count below the healthy range;
- an unexpected message flow.

These records establish observed topology differences; they do not by themselves identify which infrastructure component caused the difference.

The current duplicate-delivery corpus case is evaluated with seeded truth outside the graph comparator. The expected observation is one `orders.completed` publish and duplicated consumes at notification, audit, and analytics. This is a structural regression check; it does not claim that consumer-side multiplicity alone identifies an infrastructure root cause.

The fixed-delay `delayed_message` corpus slice has a separate correlated timing-evidence gate. That evidence checks the implemented broker-owned fixed-delay behavior; it does not establish arbitrary scheduling determinism, message reordering support, or Level-C replay semantics.

## Seeded-truth evaluation

The versioned incident corpus stores localization truth separately from the graph package. Candidate ranking completes before that truth is consulted. The corpus then evaluates whether the expected `(service, operation)` appears at top-1 and top-3 for the span-localization-eligible cases.

Span-localization eligibility remains intentionally narrow:

- `inventory-timeout` → `inventory/check`;
- `inventory-connection-reset` → `inventory/check`;
- `inventory-pre-request-crash` → `inventory/check`.

`orders-completed-duplicate` is excluded from span-localization top-k evaluation. It has its own message-topology regression gate because the currently observed divergence is a one-to-many consume multiplicity change rather than a single application-span culprit. The fixed-delay delayed-message slice likewise uses its dedicated correlated timing evidence rather than being counted as span-localization truth.

GitHub Actions is the authoritative regression gate for this branch. It runs tests, `go vet`, the current versioned corpus, and retains corpus evidence. Passing that gate establishes only that the checked implementation and seeded regression cases agree at that exact commit; it is not a replay-success benchmark, a production root-cause accuracy estimate, or statistically meaningful evidence of general performance.

## Next localization work

The next justified steps are:

1. enrich graph/topology comparison beyond current span and message-count envelopes;
2. add retry/order evidence only when those behaviors and fault classes are implemented end to end;
3. incorporate kernel evidence only after the eBPF agent can demonstrate incremental debugging value;
4. expand healthy-run samples and evaluation cases before treating empirical envelopes as representative;
5. consider calibrated confidence only after there is enough independent evaluation data to justify calibration.
