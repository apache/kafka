# Gradle Wrapper

This directory contains the Gradle wrapper files for the Apache Kafka project.

## Upgrading Gradle version

Run the wrapper task with the target version:

```shell
./gradlew wrapper --gradle-version <gradle-version>
```

That is the whole upgrade: it updates `gradle/wrapper/gradle-wrapper.properties` and `gradlew`, and no
checksum or commit has to be looked up by hand. `wrapper.gradle` fetches the published distribution
checksum and the published wrapper JAR checksum from https://services.gradle.org, and then finds the
commit in the gradle/gradle master history whose `gradle/wrapper/gradle-wrapper.jar` matches the published
wrapper checksum. Both checksums and the commit are printed by the task.

The wrapper JAR is pinned to a commit rather than to the version's release tag because a release tag may
contain the bootstrap JAR Gradle uses to build itself, which is from the previous Gradle version and then
doesn't match the published wrapper checksum, see https://issues.apache.org/jira/browse/KAFKA-21299.

## Trying a Gradle snapshot (nightly)

The same command works for a snapshot version listed at https://services.gradle.org/distributions-snapshots/
(the latest one is at https://services.gradle.org/versions/nightly), for example:

```shell
./gradlew wrapper --gradle-version 10.0.0-20261124194517+0000
```

A snapshot's wrapper JAR is never committed to gradle/gradle, and services.gradle.org publishes only its
checksum, not the JAR itself. For a snapshot the task therefore pins the newest wrapper JAR on gradle/gradle
master and prints a warning. The wrapper JAR only downloads and starts Gradle. The snapshot distribution is still
verified against `distributionSha256Sum`. Snapshots are meant for local testing only: don't commit one to trunk.

After upgrading, verify the Gradle version:

    ./gradlew --version
