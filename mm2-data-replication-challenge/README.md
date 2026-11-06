# Kafka Data Replication Challenge

This directory contains the runnable environment and automation for the MirrorMaker 2 source-offset-loss enhancement in this fork.

- Fork: https://github.com/Shivani1502/kafka
- Upstream PR: https://github.com/apache/kafka/pull/23198

## What this demonstrates

The project runs two isolated single-node Kafka KRaft clusters and a MirrorMaker 2 process built from this checkout:

- `primary` cluster, containing `commit-log`
- `standby` cluster, containing `primary.commit-log`
- one-way replication from `primary` to `standby`
- 1 partition / 1 replica for both topics
- broker `log.retention.ms=60000`
- topic `retention.ms=60000`
- topic `segment.ms=1000` to make the retention scenario deterministic in a short-running demo
- `offset.validation.enabled=true` for the MirrorSourceTask

The MM2 image is built locally from this repository so the demo uses the source changes from PR #23198. No Docker Hub image is required.

## Prerequisites

- Docker with Docker Compose v2
- Python 3
- enough Docker resources to build Kafka and run three Java processes

## Start the complete environment

From the repository root:

```bash
docker compose -f mm2-data-replication-challenge/docker-compose.yml up -d --build
```

That single command builds the modified Kafka distribution for MM2, starts both Kafka clusters, creates the two topics with the requested settings, and starts MirrorMaker 2.

To stop and delete the environment:

```bash
docker compose -f mm2-data-replication-challenge/docker-compose.yml down -v
```

## CLI commit-log producer

`producer.py` produces exactly the number of events requested by `--count` and exits non-zero if the underlying Kafka producer fails.

Example:

```bash
python3 mm2-data-replication-challenge/producer.py --count 10
```

Every event is valid JSON with this shape:

```json
{
  "event_id": "unique-uuid",
  "timestamp": "2026-08-22T04:30:00+00:00",
  "op_type": "UPSERT",
  "key": "key-0",
  "value": "value-0"
}
```

`event_id` is generated with UUID v4 and `timestamp` is UTC ISO-8601.

## Run all three scenarios

From the repository root:

```bash
bash mm2-data-replication-challenge/run_challenge.sh
```

The script is repeatable and resets Docker volumes between scenarios.

### Scenario 1 — normal replication

The script produces 10 events to `primary.commit-log` and waits until the standby topic has at least 10 records.

Expected result:

```text
PASS: 10 events replicated from commit-log to primary.commit-log.
```

### Scenario 2 — retention / data loss

The script starts with a healthy replicated position, pauses the MM2 container, produces records while MM2 cannot read them, waits for the 60-second retention window to expire, and then resumes MM2.

The next source poll requests an offset below the new log start. `MirrorSourceTask` classifies the event as data loss and throws `DataLossException` rather than silently skipping the missing records.

Expected log text includes:

```text
Source data loss detected while replicating
```

Expected script result:

```text
PASS: retention-driven offset loss was detected and the MM2 task failed fast.
```

### Scenario 3 — topic reset and automatic recovery

The script starts with normal replication, then deletes and recreates `commit-log` while MM2 remains running. The stored MM2 offset is now invalid and the recreated log starts from offset 0.

`MirrorSourceTask` detects this as a reset, calls `seekToBeginning()` for the affected partitions, emits a distinct reset/recovery log marker, and returns from that poll. The next poll consumes the recreated topic from its earliest offset. No MM2 service restart or manual connector-offset intervention is required.

Expected log text includes:

```text
SOURCE_TOPIC_RESET_RECOVERED
```

Expected script result:

```text
PASS: topic reset was detected, MM2 recovered automatically, and replication resumed without an MM2 restart.
```

## Log analysis

Use:

```bash
docker logs mm2-replicator
```

Useful markers:

| Scenario | What to look for |
| --- | --- |
| Normal replication | normal `MirrorSourceTask` poll/replication logs and increasing standby end offset |
| Retention data loss | `Source data loss detected while replicating` followed by `DataLossException` / failed task |
| Topic reset | `SOURCE_TOPIC_RESET_RECOVERED` followed by continued replication without restarting the MM2 container |

## Design rationale

### Why data loss fails fast

If the source log start offset has advanced above MM2's requested offset, records that should have been replicated no longer exist. Continuing from the new earliest offset would create an undetected gap on the standby cluster. There is no lossless automatic recovery, so the safe behavior is to fail the task with `DataLossException` and require an operator to explicitly accept/remediate the gap.

### Why topic reset recovers automatically

A recreated topic is different: when the new log starts at offset 0, the old source position belongs to the previous incarnation of the topic. The requested behavior is to recognize that reset and begin replication from the new topic's earliest offset automatically. The task therefore seeks reset partitions to the beginning and resumes on the next poll without restarting the MM2 service.

### Mixed-partition batches

`OffsetOutOfRangeException` can contain multiple partitions. Classification is performed per partition rather than with one `allMatch()` decision across the batch. If any partition shows retention-based data loss (or cannot be safely classified), the batch fails with `DataLossException`; reset-only batches recover automatically. This avoids hiding a real data-loss partition inside a mixed batch.

### Metrics note

The reviewer suggested considering a dedicated metric. This revision intentionally keeps the existing `MirrorSourceMetrics` contract unchanged to avoid broadening the patch and changing existing metric cardinality/count assumptions. The two conditions are surfaced through explicit, grep-friendly log markers; a metric can be added as a follow-up with a dedicated compatibility discussion.

## Source changes

The core implementation lives in:

- `connect/mirror/src/main/java/org/apache/kafka/connect/mirror/MirrorSourceTask.java`
- `connect/mirror/src/main/java/org/apache/kafka/connect/mirror/DataLossException.java`
- `connect/mirror/src/main/java/org/apache/kafka/connect/mirror/TopicResetException.java`

Tests:

- `MirrorSourceTaskOffsetLossTest`
- `MirrorSourceOffsetValidationConfigTest`
- `MirrorConnectorsIntegrationOffsetValidationTest`

Recommended repository checks:

```bash
./gradlew :connect:mirror:test
./gradlew :connect:mirror:checkstyleMain :connect:mirror:checkstyleTest spotlessCheck
```

## AI Usage

AI assistance was used in this take-home. ChatGPT was used to inspect the assignment, navigate the Kafka MirrorMaker 2 code paths, reason about `auto.offset.reset`, `OffsetOutOfRangeException`, Connect task failure behavior, propose and implement source/test changes, prepare the Docker/automation harness, and draft/update PR documentation. The implementation was grounded against the actual Kafka trunk source and APIs; design decisions such as fail-fast for unrecoverable retention loss versus automatic recovery for recreated topics were made to match the assignment and reviewer feedback. No claim is made that AI-generated changes were independently validated unless a test or command is explicitly reported as run.
