# Modified / Added Files — Change Summary

## Kafka fork source changes (apply to your fork → PR)

| File | Type | Real LOC | Change Summary |
|---|---|---|---|
| `connect/mirror/src/main/java/org/apache/kafka/connect/mirror/MirrorSourceTask.java` | Modified | +98 | Added `expectedOffsets` per-partition map (ConcurrentHashMap); seeded it in `initializeConsumer()`; advanced it per record in `poll()`; added `detectTruncationAndReset()` invoked at the top of `poll()`; added a dedicated `catch (LogTruncationException)` clause placed before the existing `catch (KafkaException)` so the fail-fast exception propagates to Connect. Added imports: `HashMap`, `ConcurrentHashMap`. |
| `connect/mirror/src/main/java/org/apache/kafka/connect/mirror/LogTruncationException.java` | **New** | 75 | New `KafkaException` subclass carrying topic-partition, expected offset, log-start offset, and purged-record count, with a detailed fail-fast message. |
| `connect/mirror/src/test/java/org/apache/kafka/connect/mirror/MirrorSourceTaskTest.java` | Modified | +66 | Added 3 unit tests (`testDetectTruncationFailsFast`, `testDetectTopicResetRecovers`, `testDetectNoFalsePositiveOnNormalReplication`) using the existing Mockito consumer-mocking pattern; added `assertThrows` static import. |

**Total new/changed application logic: ~239 lines — well under the 500-LOC budget.**

## Project deliverable files (repo root, outside the Kafka tree)

| File | Type | Change Summary |
|---|---|---|
| `producer/commit_log_producer.py` | New | Task 1 CLI; `--count`, `--bootstrap-servers`, `--topic`; spec-exact JSON schema; broker-retry; `acks=all`. |
| `producer/requirements.txt` | New | `kafka-python-ng` dependency. |
| `producer/Dockerfile` | New | python:3.12-slim image, entrypoint = producer. |
| `mm2/Dockerfile` | New | Multi-stage: build fork's `connect:mirror`, overlay onto `apache/kafka:4.0.0`. |
| `mm2/mm2.properties` | New | Dedicated-mode MM2 config: `commit-log` → `primary.commit-log`, RF=1, fast topic refresh. |
| `mm2/.dockerignore.fork` | New | Copy to fork root as `.dockerignore` to shrink build context. |
| `docker-compose.yml` | New | `primary-kafka`, `dr-kafka` (KRaft single-node), `mm2`, `producer` with healthchecks/deps. |
| `scripts/run_challenge.sh` | New | Orchestrates normal / truncation / reset scenarios with PASS/FAIL assertions. |
| `README.md` | New | All required sections. |
| `reports/REQUIREMENT_MAPPING.md` | New | Requirement → implementation table. |
| `reports/VERIFICATION_REPORTS.md` | New | Build / test / run verification. |
| `reports/MODIFIED_FILES.md` | New | This file. |
| `BUILD_VERIFICATION.md` | New | Concise build-status note. |

## Files explicitly checked and confirmed NOT to need changes

- `META-INF/services/org.apache.kafka.connect.source.SourceConnector` — lists
  connectors only; the new exception is not a connector.
- `checkstyle/*` — no import-control file governs `connect/mirror`.
- `build.gradle` `:connect:mirror` block — no per-class include list; new files
  are picked up automatically by the source set.
- `MirrorConnectorConfig.java` — `auto.offset.reset=earliest` is intentionally
  left as-is; the design depends on it.
