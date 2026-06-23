# Kafka API-checker plugins (buildSrc)

This module builds the KIP-1265 API-checker plugins from a single source tree:

| Plugin / Mojo | ID / artifactId | Audience |
|---|---|---|
| `KafkaPublicApiCheckerPlugin` | `org.apache.kafka.public-api-checker` (Gradle) | Internal — applied to Kafka's own build to validate that `@InterfaceAudience.Public` types only expose other public types, and that javadoc inclusion matches the audience annotations. |
| `KafkaInternalApiCheckerPlugin` | `org.apache.kafka.internal-api-checker` (Gradle) | External — published for plugin/connector/Streams-app developers to detect references from their bytecode to non-`@Public` Kafka classes. |
| `KafkaInternalApiCheckerMojo` | `org.apache.kafka:kafka-internal-api-checker-maven-plugin` | External — Maven equivalent of the Gradle internal-API checker. |

All three share the bytecode scanner and reporting code under `org.apache.kafka.apicheck`. ASM (9.6) does the scanning; nothing in the checker classloads scanned bytecode.

End-user documentation (Gradle/Maven snippets, `@SuppressKafkaInternalApiUsage`, audience-inheritance rules) lives at [`docs/apis/internal-api-checker.md`](../docs/apis/internal-api-checker.md). The notes below cover building, testing, and publishing the plugins themselves.

## Build

```bash
./gradlew :buildSrc:build
```

Produces both Gradle plugin jars, the Maven plugin jar (with `META-INF/maven/plugin.xml`), and runs the unit tests.

## Test

```bash
./gradlew :buildSrc:test
```

Includes a Gradle TestKit end-to-end test that applies the published `org.apache.kafka.internal-api-checker` plugin to a synthetic consumer project.

## Publish

Both publications inherit the release version from `-PkafkaPluginsVersion` and stage to the URL passed via `-PmavenUrl` (with `-PmavenUsername` / `-PmavenPassword` for credentials). With no overrides the version is `1.0.0-SNAPSHOT` and the publish URL is empty (so `publish` is a no-op suitable for local smoke-testing of the artifact layout).

```bash
# Stage to ASF Nexus alongside the rest of an AK release
./gradlew :buildSrc:publish \
  -PkafkaPluginsVersion=$KAFKA_VERSION \
  -PmavenUrl=$ASF_NEXUS_STAGING_URL \
  -PmavenUsername=$NEXUS_USER \
  -PmavenPassword=$NEXUS_PASS

# Local smoke-test
./gradlew :buildSrc:publish -PmavenUrl=file:///tmp/local-repo
```

The publish task produces three artifacts:

- `org.apache.kafka:kafka-internal-api-checker-maven-plugin:$KAFKA_VERSION` — Maven plugin (packaging `maven-plugin`).
- `org.apache.kafka.internal-api-checker:org.apache.kafka.internal-api-checker.gradle.plugin:$KAFKA_VERSION` — Gradle plugin marker that resolves `kafkaInternalApiChecker` to the implementation jar.
- `org.apache.kafka:buildSrc:$KAFKA_VERSION` — the implementation jar that backs the Gradle plugin marker. (`pluginMaven` publication; the jar name comes from this module's directory.)

Gradle Plugin Portal publishing is not wired here; the internal-API-checker is distributed through the same Maven coordinates as the rest of Kafka so existing AK consumers don't need a second repository.

## Layout

```
buildSrc/
├── build.gradle
├── src/main/java/org/apache/kafka/
│   ├── gradle/
│   │   ├── KafkaInternalApiCheckerPlugin.java       # External Gradle plugin
│   │   ├── KafkaInternalApiCheckerTask.java
│   │   ├── KafkaInternalApiCheckerExtension.java
│   │   ├── KafkaPublicApiCheckerPlugin.java         # Kafka-internal Gradle plugin
│   │   ├── KafkaPublicApiCheckerTask.java
│   │   └── KafkaPublicApiCheckerExtension.java
│   ├── maven/
│   │   └── KafkaInternalApiCheckerMojo.java         # External Maven mojo
│   └── apicheck/                                    # Shared scanner / validator / reporter
│       ├── ApiSurface.java
│       ├── ApiSurfaceScanner.java
│       ├── CascadeValidator.java
│       ├── JavadocConsistencyValidator.java
│       ├── PluginDeveloperApiUsageScanner.java
│       ├── PublicApiChecker.java
│       └── ViolationReporter.java
└── src/main/resources/META-INF/maven/org.apache.kafka/
    └── kafka-internal-api-checker-maven-plugin/plugin.xml
```