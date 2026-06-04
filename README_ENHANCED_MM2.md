# Apache Kafka (fork) — Enhanced MirrorMaker 2

This repository is a fork of Apache Kafka with **MirrorMaker 2 enhanced for
mission-critical PR → DR replication**. It is a complete, buildable Kafka source
tree: the upstream code plus three changed files and a self-contained project
folder (`mm2-replication-project/`) holding the producer, Docker setup, scripts,
and documentation.

## What was changed (vs upstream Kafka)

| File | Change |
|---|---|
| `connect/mirror/.../MirrorSourceTask.java` | Per-partition offset tracking + `detectTruncationAndReset()` called in `poll()`; dedicated fail-fast catch clause. |
| `connect/mirror/.../LogTruncationException.java` | **New** — fail-fast signal (subclass of `KafkaException`). |
| `connect/mirror/.../MirrorSourceTaskTest.java` | 3 new unit tests. |

- **Task 2 (truncation):** if source retention purges un-replicated records,
  MirrorMaker 2 throws `LogTruncationException` and fails fast (no silent loss).
- **Task 3 (topic reset):** if the source topic is deleted/recreated, MM2 detects
  the rewound log, resubscribes from the beginning, logs it, and keeps running.

Everything else under `mm2-replication-project/` (producer, compose, scripts,
config, reports) supports building and demonstrating the feature.

## Prerequisites

- **JDK 17+** (Kafka 4.x requires Java 17 for non-client modules).
- **Network access** on first build (Gradle 9.4.1 + dependencies download).
- **Docker + Docker Compose v2** for the runnable demo.

## One-time setup (required — Kafka does not commit the Gradle wrapper jar)

```bash
./setup.sh        # bootstraps gradle/wrapper/gradle-wrapper.jar
```

> Apache Kafka intentionally omits `gradle/wrapper/gradle-wrapper.jar` from its
> source tree, so it is not in this package either. `setup.sh` fetches it (via a
> system `gradle` if present, otherwise downloads the pinned 9.4.1 jar). This is
> the single unavoidable network step before the first build.

## Build

```bash
./gradlew :connect:mirror:jar
```

## Test

```bash
./gradlew :connect:mirror:test --tests "org.apache.kafka.connect.mirror.MirrorSourceTaskTest"
```

## Run the full demo (Docker)

```bash
cd mm2-replication-project

# Build the two images (MM2 build context is the repo root):
docker build -f mm2/Dockerfile -t <your-user>/enhanced-mm2:latest ..
docker build -t <your-user>/commit-log-producer:latest ./producer

export DOCKERHUB_USER=<your-user>
docker compose up -d primary-kafka dr-kafka mm2
./scripts/run_challenge.sh all          # normal + truncation + reset
```

## Documentation index

- `mm2-replication-project/PROJECT_README.md` — full project README (design, logs, AI usage).
- `mm2-replication-project/reports/REQUIREMENT_MAPPING.md` — requirement → implementation.
- `mm2-replication-project/reports/VERIFICATION_REPORTS.md` — build / test / run verification.
- `mm2-replication-project/reports/MODIFIED_FILES.md` — change summary.
- `mm2-replication-project/reports/COMMANDS.md` — exact commands.

## Before submitting

Replace placeholders (`<your-user>`, fork/PR URLs) in
`mm2-replication-project/PROJECT_README.md` and `docker-compose.yml`.
