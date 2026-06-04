# Build Verification Status

## What was statically verified (offline)
- Java brace balance in all modified/new files.
- All imports present, correctly ordered (Kafka checkstyle convention), and used
  (no unused-import checkstyle failures) in MirrorSourceTask.java,
  LogTruncationException.java, and MirrorSourceTaskTest.java.
- Every referenced KafkaConsumer API exists with a matching signature:
  assignment(), beginningOffsets(Collection), endOffsets(Collection),
  seek(TopicPartition, long).
- KafkaException(String) supertype constructor exists (used by LogTruncationException).
- LogTruncationException constructor call site arg types match (TopicPartition, long, long).
- String.format placeholder/arg count and types match.
- Mockito verify() calls are matcher-consistent (no mixed raw/matcher args).
- Gradle task + project paths used by mm2/Dockerfile exist:
  :connect:mirror project, :connect:mirror:copyDependantLibs, :connect:mirror:jar.
- Python producer compiles (py_compile) and emits the exact spec event schema.
- run_challenge.sh passes `bash -n` syntax check.

## What could NOT be run in this environment (and why)
- `./gradlew :connect:mirror:jar` — no JDK compiler (JRE only) and no network to
  download Kafka's build dependencies.
- `docker build` / `docker compose up` — Docker not available; network disabled.
- `run_challenge.sh` end-to-end — requires the running Docker environment.

## To actually build and run (on a machine with JDK 17+/Docker + network)
    # 1. Apply source changes to a fresh Kafka fork checkout, then:
    ./gradlew :connect:mirror:jar :connect:mirror:test
    # 2. Build images and run scenarios:
    docker build -f mm2/Dockerfile -t <user>/enhanced-mm2:latest <fork-root>
    docker build -t <user>/commit-log-producer:latest ./producer
    DOCKERHUB_USER=<user> docker compose up -d primary-kafka dr-kafka mm2
    ./scripts/run_challenge.sh all
