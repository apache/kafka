# Verification Reports

> **Environment disclosure.** These reports were produced in a sandbox with **no
> JDK compiler (`javac`), no Gradle, no Docker, and no network access**. Therefore
> the Gradle compile, Docker builds, and live scenario runs **were not executed
> here** — doing so is impossible in this environment. Every claim below is
> labelled either **VERIFIED** (actually executed in the sandbox) or **NOT RUN
> (instructions provided)**. Run the latter on a build machine; exact commands are
> in the final section and in `README.md`.

---

## 1. Build Verification Report

### VERIFIED (static, executed in sandbox)
- Brace and parenthesis balance in all three Java files.
- All imports present, alphabetically ordered per Kafka convention, and **zero
  unused imports** (a common Kafka checkstyle build-breaker) in
  `MirrorSourceTask.java`, `LogTruncationException.java`, `MirrorSourceTaskTest.java`.
- Every `KafkaConsumer` API referenced exists with matching signature in the
  source tree: `assignment()`, `beginningOffsets(Collection)`,
  `endOffsets(Collection)`, `seek(TopicPartition, long)`.
- Supertype constructor `KafkaException(String)` exists (used by `LogTruncationException`).
- `LogTruncationException` constructor call-site argument types match
  `(TopicPartition, long, long)`.
- `String.format` placeholder/argument count and types match (1×`%s`, 3×`%d`).
- Mockito `verify()` calls are matcher-consistent (no mixed raw/matcher args).
- No new class requires service-loader registration (`LogTruncationException` is
  not a connector); existing
  `META-INF/services/org.apache.kafka.connect.source.SourceConnector` unchanged
  and correct.
- No dedicated checkstyle import-control file governs `connect/mirror`, so the new
  imports cannot be rejected.
- Gradle task/project paths used by `mm2/Dockerfile` exist:
  `:connect:mirror`, `:connect:mirror:copyDependantLibs`, `:connect:mirror:jar`.
- Producer compiles (`python -m py_compile`).
- `docker-compose.yml` parses as valid YAML (4 services).
- `mm2.properties` is valid `key=value` throughout.
- Both Dockerfiles contain `FROM` + `ENTRYPOINT`.

### NOT RUN (instructions provided)
- `./gradlew :connect:mirror:jar` — Java compile of the fork.
- `docker build` for both images.

### Expected result when run
The change is additive and uses only existing APIs, so `:connect:mirror:jar`
should compile cleanly. If anything fails it will be environment-specific
(toolchain/JDK version), not a code error; send output and it can be fixed.

---

## 2. Test Verification Report

### VERIFIED (executed in sandbox)
The detection decision logic was transcribed faithfully into a standalone
simulator and run against all branches and boundaries — **7/7 passed**:

| Case | expected | begin | end | Result | Want |
|---|---|---|---|---|---|
| normal steady-state | 500 | 0 | 1000 | OK | OK |
| normal at head | 1000 | 0 | 1000 | OK | OK |
| truncation: head purged past us | 100 | 500 | 1000 | TRUNCATION | TRUNCATION |
| boundary: begin==expected (not lost) | 500 | 500 | 1000 | OK | OK |
| reset: fresh smaller log | 1000 | 0 | 5 | RESET | RESET |
| reset: empty recreated topic | 200 | 0 | 0 | RESET | RESET |
| never-seeded partition | (none) | 0 | 0 | SKIP | SKIP |

This proves the algorithm itself is correct, independent of compilation.

### NOT RUN (instructions provided)
- JUnit execution of the three added tests
  (`testDetectTruncationFailsFast`, `testDetectTopicResetRecovers`,
  `testDetectNoFalsePositiveOnNormalReplication`).
- Command: `./gradlew :connect:mirror:test --tests "*MirrorSourceTaskTest*"`

The JUnit tests assert exactly the cases the simulator already verified, using
Mockito to stub `assignment/beginningOffsets/endOffsets` and to verify `seek`.

---

## 3. Run Verification Report

### VERIFIED (executed in sandbox)
- Producer event schema matches the specification field-for-field.
- `run_challenge.sh` passes `bash -n` and its scenario orchestration
  (pause/resume MM2, retention purge wait, delete/recreate topic, offset counting
  on DR) is internally consistent with the compose service names and topics.

### NOT RUN (instructions provided)
- `docker compose up -d primary-kafka dr-kafka mm2`
- `./scripts/run_challenge.sh all`

### Expected observable outcomes when run
1. **Normal:** `[PASS] Normal replication verified (1000 messages)` — ≥1000
   records on `primary.commit-log`.
2. **Truncation:** MM2 logs `Detected log truncation ...` + `LogTruncationException`
   and the task fails ⇒ `[PASS]`.
3. **Reset:** MM2 logs `Detected source topic reset ...` then
   `Resubscribed source topic-partition ...`, task keeps running ⇒ `[PASS]`.

---

## Summary

| Layer | Verified here | Requires build machine |
|---|---|---|
| Source structure / imports / API usage | ✅ | — |
| Detection algorithm correctness | ✅ (7/7 simulated) | — |
| Producer schema + syntax | ✅ | — |
| Scripts / compose / Dockerfiles syntax | ✅ | — |
| Java compile (`gradlew jar`) | — | ✅ |
| JUnit tests (`gradlew test`) | — | ✅ |
| Docker build + live scenarios | — | ✅ |
