# Build / Run / Test Verification Reports (Full Package)

> **Authoring-environment disclosure.** The package was assembled in a sandbox
> with **no JDK compiler, no Gradle, no Docker, and no network**. The Gradle
> compile, JUnit run, Docker builds, and live scenarios therefore **were not
> executed here**. Each item is marked VERIFIED (run in sandbox) or NOT RUN
> (command provided). This package is the complete buildable Kafka source tree;
> run the NOT-RUN items on any machine with JDK 17+, Docker, and network.

## Package completeness — VERIFIED
- Complete Apache Kafka source tree included (upstream + 3 changed files),
  minus `.git` history and build caches (regenerated on build; not needed to compile).
- `gradlew`, `gradlew.bat`, `settings.gradle`, `build.gradle`, `gradle/` config present.
- `connect:mirror` registered in `settings.gradle`.
- Project folder `mm2-replication-project/` integrated at repo root with producer,
  compose, scripts, config, and all reports.
- 20/20 integrated consistency checks pass (1 expected note: see wrapper jar below).

## Known one-time setup — gradle-wrapper.jar
Apache Kafka does NOT commit `gradle/wrapper/gradle-wrapper.jar`; it is absent
from the original upload and therefore from this package. Run `./setup.sh` once
(needs network) to fetch it before the first `./gradlew` call. This is upstream
Kafka behavior, not a defect introduced here.

## Build Verification
### VERIFIED (static)
- 3 Java files: braces/parens balanced, zero unused imports, imports ordered per
  Kafka checkstyle convention.
- All `KafkaConsumer` APIs used exist with matching signatures.
- `KafkaException(String)` supertype exists; exception arg types + `String.format`
  placeholders match.
- No service-loader / import-control / build.gradle change required (verified).
- Gradle task paths used by the Dockerfile exist (`:connect:mirror:copyDependantLibs`, `:connect:mirror:jar`).
### NOT RUN (commands)
- `./setup.sh && ./gradlew :connect:mirror:jar`

## Test Verification
### VERIFIED
- Detection algorithm transcribed and run: **7/7** cases pass (normal, at-head,
  truncation, boundary, reset-small, reset-empty, unseeded).
### NOT RUN (commands)
- `./gradlew :connect:mirror:test --tests "*MirrorSourceTaskTest*"`
  (3 tests assert exactly the simulated cases via Mockito.)

## Run Verification
### VERIFIED
- Producer event schema matches spec; `python -m py_compile` passes.
- `run_challenge.sh` passes `bash -n`; orchestration consistent with compose
  service names/topics.
- `docker-compose.yml` parses; `mm2.properties` valid; both Dockerfiles complete;
  Dockerfile COPY paths corrected for the integrated layout.
### NOT RUN (commands)
- `docker compose up -d primary-kafka dr-kafka mm2 && ./scripts/run_challenge.sh all`

## Expected results when run
- Build: `:connect:mirror:jar` compiles (additive change, existing APIs only).
- Tests: 3 new tests green.
- Demo: normal `[PASS]` (≥1000 on DR); truncation `[PASS]` (LogTruncationException, task fails); reset `[PASS]` (resubscribe log, task survives).
