# Requirement → Implementation Mapping Report

Legend: **Implemented** | **Partially Implemented** | **Missing** | **External** (action you must take outside the codebase)

---

## Task 1 — Commit Log Producer

| Requirement | Status | Modified / Added Files | Explanation |
|---|---|---|---|
| CLI generating JSON events to `commit-log` | **Implemented** | `producer/commit_log_producer.py` | `KafkaProducer` writes JSON to the configurable `--topic` (default `commit-log`) on `--bootstrap-servers`. |
| `--count N` then exit | **Implemented** | `producer/commit_log_producer.py` | `argparse --count` loops exactly N times, `flush()`, then exits 0. |
| Valid JSON, unique UUIDs, current timestamps | **Implemented** | `producer/commit_log_producer.py` | `uuid.uuid4()` per event, `int(time.time())` timestamp, schema validated to match spec exactly (`event_id, timestamp, op_type, key, value.status`). |

## Task 2 — Log Truncation Detection (fail-fast)

| Requirement | Status | Modified / Added Files | Explanation |
|---|---|---|---|
| Detect offset gaps indicating truncated data | **Implemented** | `MirrorSourceTask.java` (mod), `LogTruncationException.java` (new) | `detectTruncationAndReset()` compares per-partition `expectedOffsets` against live `beginningOffsets()`; `logStartOffset > expected` (with `logEndOffset >= expected`) ⇒ truncation. Explicit detection is required because MM2's consumer uses `auto.offset.reset=earliest` and would otherwise skip silently. |
| Log detailed error + throw to fail-fast | **Implemented** | `MirrorSourceTask.java`, `LogTruncationException.java` | Throws `LogTruncationException` (subclass of `KafkaException`) with topic-partition, expected vs log-start offset, and purged count. A dedicated `catch (LogTruncationException)` placed **before** the existing `catch (KafkaException)` re-throws so Connect fails the task. SLF4J ERROR logged. |
| Minimal disruption to MM2 logic | **Implemented** | `MirrorSourceTask.java` | One method + one call site in `poll()` + per-partition map; ~98 real LOC. No other classes touched. |

## Task 3 — Graceful Topic Reset Handling

| Requirement | Status | Modified / Added Files | Explanation |
|---|---|---|---|
| Detect source topic reset (delete + recreate) | **Implemented** | `MirrorSourceTask.java` | In `detectTruncationAndReset()`, `logEndOffset < expected` ⇒ the log was rewound to a fresh smaller topic ⇒ reset. End-offset comparison distinguishes reset from truncation. |
| Log reset events with timestamp + topic details | **Implemented** | `MirrorSourceTask.java` | SLF4J WARN includes topic-partition, `System.currentTimeMillis()` epoch, expected/end/begin offsets. |
| Automatically resubscribe from beginning offset | **Implemented** | `MirrorSourceTask.java` | `consumer.seek(tp, logStartOffset)`, resets `expectedOffsets`, logs INFO confirmation; task continues running (recovers, does not fail). |

## Architecture / Cluster Setup

| Requirement | Status | Modified / Added Files | Explanation |
|---|---|---|---|
| Primary single-node cluster hosting `commit-log` | **Implemented** | `docker-compose.yml` | `primary-kafka` KRaft single-node, external `localhost:9092`. |
| Standby single-node cluster hosting `primary.commit-log` | **Implemented** | `docker-compose.yml` | `dr-kafka` KRaft single-node, external `localhost:9093`. DefaultReplicationPolicy renames `commit-log` → `primary.commit-log`. |
| Topics: 1 partition, 1 replica | **Implemented** | `scripts/run_challenge.sh` | `create_source_topic()` uses `--partitions 1 --replication-factor 1`. |
| `log.retention.ms=60000` on `commit-log` for truncation test | **Implemented** | `scripts/run_challenge.sh` | Truncation scenario creates the topic with `retention.ms=60000` (+ short `segment.ms` so segments roll and purge). |

## Deliverables

| Requirement | Status | Modified / Added Files | Explanation |
|---|---|---|---|
| Kafka fork + PR with MM2 mods | **External** | `kafka-fork-changes/` | Source files ready to drop into a fork. You must create the fork, commit, and open the PR (placeholders in README). |
| Docker Hub: Enhanced MM2 image | **Implemented (build recipe)** / **External (push)** | `mm2/Dockerfile`, `mm2/mm2.properties`, `mm2/.dockerignore.fork` | Multi-stage build of the fork onto `apache/kafka:4.0.0`. You run the build + push with your namespace. |
| Docker Hub: Producer image | **Implemented (build recipe)** / **External (push)** | `producer/Dockerfile` | Builds the CLI. You push. |
| `docker-compose.yml` (both clusters + MM2 + producer) | **Implemented** | `docker-compose.yml` | All four services defined with healthchecks + dependencies. |
| `run_challenge.sh` (3 scenarios) | **Implemented** | `scripts/run_challenge.sh` | Normal (1000 msgs + verify), truncation (pause→purge→resume→verify fail), reset (delete/recreate→verify recovery). |
| `README.md` (all sections) | **Implemented** | `README.md` | Repo links, image table, setup, test execution, log analysis, design rationale, AI-usage methodology, file manifest. |
| SLF4J comprehensive logging | **Implemented** | `MirrorSourceTask.java`, `LogTruncationException.java` | ERROR (truncation), WARN (reset detected), INFO (resubscribed). |
| Unit tests | **Implemented** | `MirrorSourceTaskTest.java` | 3 tests covering all branches, using existing Mockito pattern. |

## Known External Actions (cannot be done inside the codebase)

1. Replace placeholders: fork URL, PR URL, Docker Hub namespace (`README.md`, `docker-compose.yml`).
2. Create the GitHub fork, commit, push, open PR.
3. Build & push the two Docker images.
4. (Optional) Rebase onto the `4.0.0` tag — runtime image already pins `apache/kafka:4.0.0`; source applies cleanly.
