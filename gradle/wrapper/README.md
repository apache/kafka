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

After upgrading, verify the Gradle version:

    ./gradlew --version
