# Experiment Artifact Format

Kestrel persists completed experiments as immutable directories so graph reconstruction and replay do not depend on the collector process that originally observed the failure.

## Layout

```text
<experiment-id>/
├── manifest.json
├── events.ndjson
└── checksums.json
```

`manifest.json` is versioned with `schema_version`. Version 1 records the experiment identifier, creation time, workload name, topology, optional seeded fault specification, expected and observed behavior, the recorded outcome signature, the event-log filename/count, and the event-log SHA-256 digest.

`events.ndjson` contains one normalized `model.Event` per line. Events are validated before persistence and again during load.

`checksums.json` contains SHA-256 digests for both the exact manifest bytes and the exact event-log bytes. This detects accidental corruption or modification. It is an **integrity check, not an authenticity mechanism**: an attacker able to rewrite the artifact can also rewrite its checksums. Signed artifacts are outside the current threat model.

## Commit semantics

`experiment.Save`:

1. validates the complete record and fault specification;
2. reserves the experiment ID with an exclusive writer lock;
3. writes the event log into a temporary directory;
4. flushes and `fsync`s the event log;
5. writes and `fsync`s the manifest;
6. writes and `fsync`s the checksum file;
7. `fsync`s the temporary directory;
8. atomically renames the temporary directory to the final experiment ID;
9. `fsync`s the experiment root.

Existing experiment IDs are never overwritten by the API. A failed write cleans its temporary directory during normal process execution. Abrupt process death can leave a stale temporary directory or reservation file; explicit crash-recovery cleanup is a remaining Milestone 3 task.

## Load semantics

`experiment.Load` accepts only the fixed `events.ndjson` event filename. It validates:

- supported schema version;
- safe experiment ID;
- required workload/topology/outcome metadata;
- fault specification validity;
- manifest checksum;
- event-log checksum agreement between manifest and checksum file;
- every normalized event;
- recorded event count.

Unknown manifest/checksum fields are rejected to prevent silently accepting a schema the current binary does not understand.

## Replay from an artifact

After running `make demo`, copy the printed artifact directory and run:

```bash
make artifact-replay ARTIFACT=.kestrel/experiments/<experiment-id>
```

The command starts a fresh topology from only the persisted fault schedule and compares its outcome signature with the recorded signature.

## Current storage boundary

This format is the first durable experiment boundary, not the final production storage design. PostgreSQL metadata/results indexing and a measured decision about long-term high-volume event storage remain pending. The NDJSON format is intentionally simple and inspectable while event volume is still small enough that storage benchmarks would otherwise be premature.
