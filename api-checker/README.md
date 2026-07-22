# Kafka API-checker plugins

This is a separate Gradle build (composite-included from the root `settings.gradle` via
`pluginManagement { includeBuild 'api-checker' }`) that produces the KIP-1265 API checkers.
Living outside Kafka's main build keeps the Maven runtime dependencies off the main-build
classpath and lets each published artifact carry only the classes its consumers need.

| Subproject | Publishes | Audience |
|---|---|---|
| `:core` | `org.apache.kafka:kafka-api-checker-core` | Shared scanner + validator + reporter (ASM only). Both plugin jars depend on it. |
| `:gradle-plugins` | `org.apache.kafka:kafka-internal-api-checker-gradle-plugin` + plugin markers for `org.apache.kafka.public-api-checker` and `org.apache.kafka.internal-api-checker` | The Kafka-internal producer-side checker and the published consumer-side Gradle checker. |
| `:maven-plugin` | `org.apache.kafka:kafka-internal-api-checker-maven-plugin` | Maven equivalent of the consumer-side checker. |

End-user documentation (Gradle/Maven snippets, `@SuppressKafkaInternalApiUsage`,
audience-inheritance rules) lives at
[`docs/apis/internal-api-checker.md`](../docs/apis/internal-api-checker.md). The notes
below cover building, testing, and publishing the plugins themselves.

## Build

```bash
./gradlew :api-checker:core:build :api-checker:gradle-plugins:build :api-checker:maven-plugin:build
```

## Test

```bash
./gradlew :api-checker:core:test :api-checker:gradle-plugins:test
```

`:gradle-plugins:test` includes a Gradle TestKit end-to-end test that applies the
`org.apache.kafka.internal-api-checker` plugin to a synthetic consumer project.

## Publish

Each subproject's `publish` task stages to the URL passed via `-PmavenUrl` (with
`-PmavenUsername` / `-PmavenPassword` for credentials). The version is read from the
repo-root `gradle.properties`, so `release.py`'s existing `updateVersion` call sets it
automatically. `-PkafkaPluginsVersion=…` overrides for one-off out-of-band publishes.

```bash
# Stage to ASF Nexus alongside the rest of an AK release
./gradlew :api-checker:core:publish \
         :api-checker:gradle-plugins:publish \
         :api-checker:maven-plugin:publish \
  -PmavenUrl=$ASF_NEXUS_STAGING_URL \
  -PmavenUsername=$NEXUS_USER \
  -PmavenPassword=$NEXUS_PASS

# Local smoke-test
./gradlew :api-checker:core:publishToMavenLocal \
         :api-checker:gradle-plugins:publishToMavenLocal \
         :api-checker:maven-plugin:publishToMavenLocal
```

The five published coordinates:

- `org.apache.kafka:kafka-api-checker-core:$KAFKA_VERSION` — shared scanner library.
- `org.apache.kafka:kafka-internal-api-checker-gradle-plugin:$KAFKA_VERSION` — Gradle plugin
  implementation jar (consumed by the marker poms).
- `org.apache.kafka.internal-api-checker:org.apache.kafka.internal-api-checker.gradle.plugin:$KAFKA_VERSION` —
  marker pom for `plugins { id 'org.apache.kafka.internal-api-checker' }`.
- `org.apache.kafka.public-api-checker:org.apache.kafka.public-api-checker.gradle.plugin:$KAFKA_VERSION` —
  marker pom for the producer-side checker. (Kafka-internal use, but published from the
  same module for consistency.)
- `org.apache.kafka:kafka-internal-api-checker-maven-plugin:$KAFKA_VERSION` — Maven plugin
  (packaging `maven-plugin`).

## Layout

```
api-checker/
├── settings.gradle           # declares the three subprojects
├── build.gradle              # group / version / signing / common publishing config
├── core/
│   ├── build.gradle          # ASM dep; published as kafka-api-checker-core
│   └── src/
│       ├── main/java/.../apicheck/                # scanner, validators, reporter
│       ├── test/java/.../apicheck/                # unit tests
│       └── testFixtures/java/.../apicheck/        # AsmClassFactory, TempJarBuilder
│                                                  # — re-used by :gradle-plugins' tests
├── gradle-plugins/
│   ├── build.gradle          # java-gradle-plugin; depends on :core
│   └── src/{main,test}/java/.../gradle/           # Plugin/Task/Extension × 2
└── maven-plugin/
    ├── build.gradle          # Maven deps; templates plugin.xml at processResources
    └── src/main/
        ├── java/.../maven/KafkaInternalApiCheckerMojo.java
        └── resources/META-INF/maven/plugin.xml
```
