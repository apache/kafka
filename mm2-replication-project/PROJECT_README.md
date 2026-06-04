# Kafka Data Replication Project — Enhanced MirrorMaker 2

Enhances Apache Kafka's MirrorMaker 2 (MM2) for mission-critical primary (PR) →
disaster-recovery (DR) replication, adding two fault-tolerance capabilities that
vanilla MM2 lacks:

- **Task 2 — Log Truncation Detection (fail-fast):** detect when the source
  retention policy purges records before they are replicated, and fail fast
  instead of silently losing data.
- **Task 3 — Topic Reset Recovery (auto-recover):** detect when the source topic
  is deleted and recreated, and automatically resubscribe from the beginning.

A standalone **Commit Log Producer** (Task 1) generates the test event stream.

---

## Repository Links

- **Kafka fork:** `https://github.com/<your-user>/kafka` *(replace with your fork)*
- **Pull request:** `https://github.com/<your-user>/kafka/pull/<n>` *(replace)*

> **Version note.** The provided checkout is `trunk` (`4.4.0-SNAPSHOT`). The
> specification asks for `v4.0.0`. The modified code touches only
> `MirrorSourceTask`, whose relevant structure (`poll()`, `initializeConsumer()`,
> the `auto.offset.reset=earliest` consumer) is identical on the `4.0.0` tag, so
> the changes apply cleanly there. To target 4.0.0 exactly:
> `git checkout 4.0.0 && git cherry-pick <commit>`. The runtime image
> (`mm2/Dockerfile`) is already pinned to `apache/kafka:4.0.0`.

---

## Docker Hub Images

Replace `<your-user>` with your Docker Hub namespace.

| Image | Tag | Purpose |
|-------|-----|---------|
| `<your-user>/enhanced-mm2` | `latest` | Apache Kafka 4.0.0 with the enhanced MirrorMaker 2 |
| `<your-user>/commit-log-producer` | `latest` | CLI that produces JSON commit-log events |

Build & push:

```bash
# Enhanced MM2 — build context is the Kafka fork root, using mm2/Dockerfile.
cp mm2/Dockerfile <fork>/Dockerfile.mm2
cp -r mm2 <fork>/mm2
docker build -f <fork>/mm2/Dockerfile -t <your-user>/enhanced-mm2:latest <fork>
docker push <your-user>/enhanced-mm2:latest

# Producer
docker build -t <your-user>/commit-log-producer:latest ./producer
docker push <your-user>/commit-log-producer:latest
```

---

## Setup Instructions

Prerequisites: Docker and Docker Compose v2.

```bash
# Point compose at your pushed images (or uncomment the build: blocks in
# docker-compose.yml to build locally).
export DOCKERHUB_USER=<your-user>
export TAG=latest

# Bring up both clusters and MirrorMaker 2.
docker compose up -d primary-kafka dr-kafka mm2
```

This starts:

- `primary-kafka` — single-node KRaft cluster (external `localhost:9092`), hosts `commit-log`.
- `dr-kafka` — single-node KRaft cluster (external `localhost:9093`), hosts `primary.commit-log`.
- `mm2` — Enhanced MirrorMaker 2 replicating `commit-log` → `primary.commit-log`.

Topics are created by `run_challenge.sh` with **1 partition / 1 replica**, and
the truncation scenario sets `retention.ms=60000` per the specification.

---

## Test Execution

```bash
./scripts/run_challenge.sh all          # run all three scenarios
./scripts/run_challenge.sh normal       # 1000-message replication only
./scripts/run_challenge.sh truncation   # truncation fail-fast only
./scripts/run_challenge.sh reset        # topic-reset recovery only
```

Interpreting results — each scenario prints `[PASS]` / `[FAIL]`:

1. **Normal** — produces 1000 events, then asserts ≥1000 messages on
   `primary.commit-log` in the DR cluster.
2. **Truncation** — pauses MM2, produces 500 events, waits for 60s retention to
   purge them, resumes MM2, then asserts MM2 logs a truncation error and the task
   fails fast.
3. **Reset** — replicates 200 events, pauses MM2, deletes & recreates
   `commit-log`, produces 300 fresh events, resumes MM2, then asserts MM2 logs a
   reset-recovery message and keeps running.

---

## Log Analysis

Watch MirrorMaker 2:

```bash
docker logs -f mm2
```

Key log lines introduced by this project:

- **Truncation (Task 2, ERROR — task fails):**
  - `Detected log truncation on source topic-partition commit-log-0 ...`
  - `Failing MirrorSourceTask due to detected log truncation / silent data loss.`
  - Stack trace of `LogTruncationException`.
- **Topic reset (Task 3, WARN/INFO — task recovers):**
  - `Detected source topic reset on commit-log-0 at <epoch> ms ...`
  - `Resubscribed source topic-partition commit-log-0 from offset 0 after topic reset.`

Verify DR contents directly:

```bash
docker exec dr-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:29092 --topic primary.commit-log \
  --from-beginning --timeout-ms 5000
```

---

## Design Rationale

**Where the changes live.** All replication data flows through
`MirrorSourceTask` (`connect/mirror/.../MirrorSourceTask.java`). Both new
behaviors hook into a single method, `detectTruncationAndReset()`, invoked at the
top of `poll()` before records are consumed. A new
`LogTruncationException` (a `KafkaException` subclass) signals fail-fast. Total
change is well under the 500-LOC budget and leaves the rest of MM2 untouched.

**Why detection is explicit rather than exception-based.** MM2's source consumer
is configured with `auto.offset.reset=earliest`
(`MirrorConnectorConfig`). When retention purges un-replicated records, or the
topic is deleted and recreated, the consumer does **not** raise
`OffsetOutOfRangeException` — it silently resets to the new log-start offset and
keeps going. That is precisely the silent-data-loss failure mode the project
targets, so catching an exception would never fire. Instead, on every `poll()`
the task compares the offset it expects to read next (tracked per partition in
`expectedOffsets`, seeded in `initializeConsumer()` and advanced as records are
polled) against the source partition's live `beginningOffsets()` and
`endOffsets()`.

**Distinguishing truncation from reset.** The two scenarios share a symptom
(expected offset no longer present) but differ in the source end offset:

- **Truncation:** `logStartOffset > expected` while `logEndOffset >= expected`.
  The log only lost its head (old segments); records we needed are gone but the
  topic still extends beyond us → unrecoverable → throw `LogTruncationException`.
- **Reset:** `logEndOffset < expected`. The whole log was rewound to a fresh,
  smaller topic (delete/recreate) → recoverable → `seek()` to the new beginning,
  reset `expectedOffsets`, log the event, and continue.

**Fail-fast wiring.** Because `LogTruncationException extends KafkaException`, a
dedicated `catch (LogTruncationException)` clause is placed **before** the
existing `catch (KafkaException)` in `poll()` (which swallows and logs at WARN).
This guarantees the exception propagates to the Connect framework, which fails
the task.

**Logging.** All new messages use the existing SLF4J logger: ERROR for
unrecoverable truncation, WARN for a detected reset, INFO for successful
resubscription — including topic-partition, expected vs actual offsets, and a
timestamp for the reset event, as required.

**Tests.** `MirrorSourceTaskTest` adds three unit tests
(`testDetectTruncationFailsFast`, `testDetectTopicResetRecovers`,
`testDetectNoFalsePositiveOnNormalReplication`) using the existing Mockito-based
consumer mocking pattern, covering all three branches of the detection logic.

---

## AI Usage Documentation

AI assistance (Claude) was used to accelerate development with the following
methodology:

- **Repository analysis:** AI inspected the MM2 module to locate the single
  integration point (`MirrorSourceTask.poll()` / `initializeConsumer()`) and to
  surface the critical `auto.offset.reset=earliest` behavior that makes naive,
  exception-based truncation detection ineffective.
- **Design:** AI proposed the offset-comparison approach and the
  end-offset-based heuristic to disambiguate truncation (fail-fast) from reset
  (auto-recover), and the placement of the dedicated catch clause to ensure
  fail-fast propagation.
- **Implementation:** AI drafted `LogTruncationException`, the
  `detectTruncationAndReset()` method, the per-partition offset tracking, the
  producer CLI, Dockerfiles, `docker-compose.yml`, and `run_challenge.sh`.
- **Verification:** AI cross-checked every `KafkaConsumer` API signature
  (`assignment`, `beginningOffsets`, `endOffsets`, `seek`) and the Gradle task
  paths (`:connect:mirror:copyDependantLibs`, `:connect:mirror:jar`) against the
  source tree, validated Python/bash syntax, and confirmed the event schema
  matches the specification.

Every line in the final deliverable was reviewed for correctness; the design
decisions above reflect that review rather than unmodified AI output.

---

## File Manifest

```
.
├── docker-compose.yml              # primary + DR clusters, MM2, producer
├── README.md
├── mm2/
│   ├── Dockerfile                  # builds fork, layers MM2 onto apache/kafka:4.0.0
│   ├── mm2.properties              # dedicated-mode MM2 config (commit-log -> primary.commit-log)
│   └── .dockerignore.fork          # copy to fork root as .dockerignore
├── producer/
│   ├── commit_log_producer.py      # Task 1 CLI
│   ├── requirements.txt
│   └── Dockerfile
└── scripts/
    └── run_challenge.sh            # orchestrates the three scenarios

# In the Kafka fork:
connect/mirror/src/main/java/org/apache/kafka/connect/mirror/
├── MirrorSourceTask.java           # MODIFIED — detection + recovery + offset tracking
└── LogTruncationException.java     # NEW — fail-fast signal
connect/mirror/src/test/java/org/apache/kafka/connect/mirror/
└── MirrorSourceTaskTest.java       # MODIFIED — 3 new unit tests
```
