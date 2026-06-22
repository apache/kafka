<!--
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

# Agent Guide for Apache Kafka

This file is read by automated agents (security scanners, code analyzers,
AI assistants) operating on this repository.

## Project overview

Apache Kafka is a distributed event streaming platform. This repo is a **Gradle**
build in **Java** and **Scala 2.13**.

- **Supported JDK**: 11+ for `clients`, `generator`, and `streams`; 17+ for other modules.
- **Build/test JDK**: Java 17 and 25.
- **Build tool**: Gradle, wrapper `./gradlew` is recommended.

## Repository layout (high level)

| Path                                                                                          | Purpose                                                                                            |
|-----------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------|
| `clients/`                                                                                    | Producer, consumer, admin client APIs and protocol messages                                        |
| `core/`                                                                                       | Broker runtime, log, replication, request handling (gradually being removed along with Scala code) |
| `server/`, `server-common/`                                                                   | Broker/server components and shared server code                                                    |
| `metadata/`, `raft/`                                                                          | KRaft metadata layer and Raft implementation                                                       |
| `storage/`, `storage/api/`                                                                    | Log segments, checkpoints, tiered storage APIs                                                     |
| `group-coordinator/`, `share-coordinator/`, `transaction-coordinator/`, `coordinator-common/` | Coordinators                                                                                       |
| `connect/`                                                                                    | Kafka Connect (api, runtime, plugins, transforms)                                                  |
| `streams/`                                                                                    | Kafka Streams (+ Scala, examples, upgrade system tests)                                            |
| `generator/`                                                                                  | RPC/message code generation                                                                        |
| `tools/`, `shell/`                                                                            | CLI tools and kafka metadata shell                                                                 |
| `examples/`                                                                                   | Kafka producer and consumer examples                                                               |
| `jmh-benchmarks/`                                                                             | Kafka benchmarks tests                                                                             |
| `trogdor/`                                                                                    | test framework                                                                                     |
| `tests/`                                                                                      | System test harness (see `tests/README.md`)                                                        |
| `config/`                                                                                     | Sample broker/controller configuration                                                             |
| `checkstyle/`                                                                                 | Checkstyle rules and import-control XML                                                            |
| `docs/`                                                                                       | Documentation sources                                                                              |
| `docker/`                                                                                     | Docker images, build/test scripts, and examples for JVM and native Kafka           |


## Build commands

```bash
# Compile jars
./gradlew jar

# Release tarball (output under core/build/distributions/)
./gradlew clean releaseTarGz

# Publish to local Maven (skip signing for local dev)
./gradlew -PskipSigning=true publishToMavenLocal

# Regenerate RPC/message classes after branch switches
./gradlew processMessages processTestMessages

# List tasks
./gradlew tasks
```

Module-scoped build:

```bash
./gradlew core:jar
./gradlew clients:jar
./gradlew :streams:testAll
```

## Test commands

```bash
# All unit + integration tests
./gradlew test

# Split suites
./gradlew unitTest
./gradlew integrationTest

# Include flaky tests (marked @Flaky)
./gradlew test -Pkafka.test.run.flaky=true

# Re-run without considering up-to-date
./gradlew test --rerun-tasks

# Single test class
./gradlew clients:test --tests RequestResponseTest

# Single test method
./gradlew core:test --tests kafka.api.ProducerFailureHandlingTest.testCannotSendToInternalTopic

# Coverage (whole project or one module)
./gradlew reportCoverage -PenableTestCoverage=true -Dorg.gradle.parallel=false
./gradlew clients:reportCoverage -PenableTestCoverage=true -Dorg.gradle.parallel=false
```

**Test retries** (optional): `-PmaxTestRetries=1 -PmaxTestRetryFailures=3`

**Logging**: adjust `log4j2.yaml` under `<module>/src/test/resources/` for verbose test logs.

**System tests**: see [tests/README.md](tests/README.md).

## Code quality

Run before submitting Java changes:

```bash
./gradlew spotlessApply          # fix import order / formatting
./gradlew checkstyleMain checkstyleTest spotlessCheck
./gradlew spotbugsMain spotbugsTest -x test   # optional static analysis
```

Checkstyle reports: `<module>/build/reports/checkstyle/`.

## Coding conventions

- **Commits / PR titles**: Start with `KAFKA-XXXXX`, `MINOR`, or `HOTFIX`. Use `KAFKA-XXXXX` only when there is a valid Jira ticket for the change.
- **AI-generated contributions**: Follow the [AI-Generated Contributions](CONTRIBUTING.md#ai-generated-contributions) section in `CONTRIBUTING.md` — add a `Co-Authored-By` or `Generated-by` commit trailer for AI-assisted changes.
- **Public API / KIP**: Changes to public interfaces, wire protocol, configurations, or metrics generally require a [KIP](https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Improvement+Proposals). See the `javadoc` `include` list in [build.gradle](build.gradle) for packages treated as public API.
- **License header**: New source files need the standard ASF license header (see existing files in the same module).
- **Checkstyle**: Enforced on main and test sources; rules in `checkstyle/`. Import order is checked via Spotless.
- **Generated code**: Do not hand-edit generated RPC/message sources. Regenerate with `processMessages` / `processTestMessages`. See [clients/src/main/resources/common/message/README.md](clients/src/main/resources/common/message/README.md).
- **Scope**: Prefer targeted unit tests in the module you change; integration/system tests for cross-component behavior.
- **Compatibility**: clients, generator, and streams modules support Java 11+; all other modules require Java 17+. Avoid APIs that break those release levels.

## Security

See [SECURITY.md](./SECURITY.md) for how to report a vulnerability and for links
to the Apache Kafka security model under [docs/security/](./docs/security/).

Agents that scan this repository should consult `SECURITY.md` and the linked
security model before reporting issues. In particular, the core model defines
what is in and out of scope, how reports are classified, and a list of known
non-findings; each component page adds its own known non-findings.

## Reference docs

| Topic                          | Location |
|--------------------------------|----------|
| Build, test, broker quickstart | [README.md](README.md) |
| Contributing                   | [CONTRIBUTING.md](CONTRIBUTING.md), https://kafka.apache.org/contributing.html |
| Message protocol / codegen     | [clients/src/main/resources/common/message/README.md](clients/src/main/resources/common/message/README.md) |
| System tests                   | [tests/README.md](tests/README.md) |
| JMH benchmarks                 | [jmh-benchmarks/README.md](jmh-benchmarks/README.md) |
| Client examples                | [examples/README.md](examples/README.md) |
| Security                       | [SECURITY.md](SECURITY.md) |
