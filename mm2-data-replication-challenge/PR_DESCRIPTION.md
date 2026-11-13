# Suggested PR description for apache/kafka#23198

## Summary

This change completes the Kafka Data Replication take-home around MirrorMaker 2 source-offset loss.

It handles the two failure modes differently:

- **Retention/truncation data loss:** fail fast with `DataLossException`; continuing would silently create a gap on the standby cluster.
- **Source topic reset/recreation:** automatically `seekToBeginning()` for reset partitions and resume replication on the next poll, with no MM2 service restart or manual connector-offset intervention.

`offset.validation.enabled=true` switches the replication consumer to `auto.offset.reset=none` so `OffsetOutOfRangeException` is surfaced to `MirrorSourceTask`. Classification is done per partition with `beginningOffsets()`: earliest `0` means reset, earliest `> 0` means retention loss. Mixed batches fail safely as data loss if any affected partition represents retention loss or cannot be classified safely.

Successful reset recovery emits the distinct marker:

```text
SOURCE_TOPIC_RESET_RECOVERED
```

## Runnable project

The complete environment is under `mm2-data-replication-challenge/`:

- `docker-compose.yml` — isolated single-node primary and standby Kafka KRaft clusters, required 1-partition/1-replica topics and 60-second retention, one-command startup
- `Dockerfile.mm2` — builds the MM2 runtime from this checkout so the demo exercises this PR's code
- `mm2.properties` — one-way `primary -> standby` replication with offset validation enabled
- `producer.py` — CLI `--count` producer generating exactly N JSON events (`event_id`, UTC `timestamp`, `op_type`, `key`, `value`) with production error handling
- `run_challenge.sh` — repeatable normal replication, retention data-loss, and topic-reset recovery scenarios
- `README.md` — setup/run instructions, log analysis, design rationale, and AI usage disclosure

Start everything:

```bash
docker compose -f mm2-data-replication-challenge/docker-compose.yml up -d --build
```

Run all scenarios:

```bash
bash mm2-data-replication-challenge/run_challenge.sh
```

### Scenario 1 — normal replication

Produces events to `commit-log` and verifies the standby `primary.commit-log` end offset advances.

### Scenario 2 — retention / data loss

Pauses MM2, produces source records, waits until retention actually advances the source log-start offset, resumes MM2, and verifies the `Source data loss detected while replicating` log plus fail-fast behavior.

### Scenario 3 — topic reset / automatic recovery

Deletes and recreates `commit-log` while MM2 stays running, produces records into the recreated topic, verifies `SOURCE_TOPIC_RESET_RECOVERED`, and verifies new records reach the standby without restarting MM2.

## Tests

Unit/config coverage includes:

- retention -> `DataLossException`
- reset -> automatic seek/recovery and successful next poll
- mixed reset + retention batch -> `DataLossException`
- validation-disabled backward-compatible behavior
- first-start handling and config propagation

Embedded-cluster integration coverage includes:

- deterministic retention data-loss failure using `Admin#deleteRecords`
- delete/recreate while the connector remains running, followed by automatic reset recovery and resumed replication

Recommended checks:

```bash
./gradlew :connect:mirror:test
./gradlew :connect:mirror:checkstyleMain :connect:mirror:checkstyleTest spotlessCheck
```

## Metrics note

The reviewer suggested considering a dedicated metric. This revision intentionally keeps the existing `MirrorSourceMetrics` contract unchanged to avoid broadening the patch and altering existing metric-count/cardinality assumptions. Both outcomes use explicit, grep-friendly log markers; a dedicated metric can be introduced as a follow-up compatibility change.

## Design rationale

Fail-fast is intentional for true retention loss because the missing source records no longer exist and silently advancing would create an unreported DR gap.

Topic recreation is recoverable under the requested semantics: the old stored position belongs to the previous topic incarnation, so MM2 explicitly repositions the affected partitions to the new log's earliest offset and continues on the next poll.

## AI Usage

AI assistance was used. ChatGPT helped inspect the assignment/reviewer feedback, navigate the MM2 source/test code, reason about Kafka consumer offset semantics and Connect task lifecycle behavior, implement source/tests and the Docker automation harness, and draft documentation. Changes were grounded against current Kafka repository APIs. No test or command is claimed as executed unless explicitly reported as run.
