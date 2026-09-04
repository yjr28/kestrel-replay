# Experiment Artifact Format

Kestrel persists completed experiments as immutable directories so graph reconstruction and replay do not depend on the collector process that originally observed the failure.

## Layout

Committed artifact:

```text
<experiment-id>/
├── manifest.json
├── events.ndjson
└── checksums.json
```

An in-progress write uses two sibling paths:

```text
<experiment-id>.lock
<experiment-id>.tmp/
```

The reservation file contains the writer PID, hostname, reservation timestamp, and expected temporary-directory name. The temporary directory is deterministic per experiment ID, so recovery never has to guess which partial write belongs to which reservation.

`manifest.json` is versioned with `schema_version`. Version 1 records the experiment identifier, creation time, workload name, topology, optional seeded fault specification, expected and observed behavior, the recorded outcome signature, the event-log filename/count, and the event-log SHA-256 digest.

`events.ndjson` contains one normalized `model.Event` per line. Events are validated before persistence and again during load.

`checksums.json` contains SHA-256 digests for both the exact manifest bytes and the exact event-log bytes. This detects accidental corruption or modification. It is an **integrity check, not an authenticity mechanism**: an attacker able to rewrite the artifact can also rewrite its checksums. Signed artifacts are outside the current threat model.

## Commit semantics

`experiment.Save`:

1. validates the complete record and fault specification;
2. reserves the experiment ID with an exclusive JSON writer reservation;
3. refuses an existing committed directory or leftover deterministic temp directory;
4. writes the event log into `<experiment-id>.tmp`;
5. flushes and `fsync`s the event log;
6. writes and `fsync`s the manifest;
7. writes and `fsync`s the checksum file;
8. `fsync`s the temporary directory;
9. atomically renames the temporary directory to the final experiment ID;
10. `fsync`s the experiment root;
11. removes the writer reservation on normal return.

Existing experiment IDs are never overwritten by the API. A failed write cleans its temporary directory during normal process execution.

## Crash recovery

Abrupt process death can leave `<experiment-id>.lock` and `<experiment-id>.tmp`. Recovery is explicit rather than automatic because deleting an active writer's state would violate the storage invariant.

```bash
make artifact-recover EXPERIMENT=<experiment-id>
# optional threshold override
make artifact-recover EXPERIMENT=<experiment-id> STALE_AFTER=30m
```

`experiment.Recover` follows conservative rules:

- the caller must provide a positive staleness threshold;
- a non-committed reservation younger than that threshold is refused;
- a reservation owned by a live PID on the same host is refused even when old;
- a reservation from another hostname is refused because process liveness cannot be proven locally;
- only a stale reservation whose same-host owner is confirmed dead may have its lock/temp paths removed;
- if the committed artifact directory already exists, only sibling lock/temp leftovers are removed; the committed directory is never mutated.

Recovery returns a structured report stating exactly which auxiliary paths were removed.

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
