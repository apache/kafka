# Gradle Wrapper

This directory contains the Gradle wrapper files for the Apache Kafka project.

## Upgrading Gradle version

When upgrading the Gradle version used by the Kafka project, update the following files in order:

1. **Update Gradle version** in `gradle/libs.versions.toml`:
   ```toml
   gradle = "9.2.0"
   ```

2. **You may have to update the wrapper JAR commit and checksum** in `wrapper.gradle`:
   - Find the ***Wrapper JAR Checksum*** at https://gradle.org/release-checksums/
   - if the Wrapper JAR checksum ***was not changed*** between Gradle versions (old and new) ***skip below to step 3.***
   - if the Wrapper JAR checksum was changed between Gradle versions (old and new) you will have to change two things:
   - `wrapperChecksum`to a new value
   - `wrapperJarCommit` to a specific commit from the Gradle master branch that is missing from the Gradle version’s git release tag:
     - https://github.com/gradle/gradle/commits/master/gradle/wrapper/gradle-wrapper.jar
     - https://github.com/gradle/gradle/commits/v<gradle-version>/gradle/wrapper/gradle-wrapper.jar
   - ***Do not use the release tag*** (`v<gradle-version>`) for `wrapperJarCommit`: it may contain the wrapper JAR of the previous
     Gradle version, see https://issues.apache.org/jira/browse/KAFKA-21299
   ```groovy
   task bootstrapWrapper() {
       // ... (other code)
       doLast {
           // ... (other code)
           String wrapperJarCommit = "<gradle-commit-sha>"
           String wrapperChecksum = "<wrapper-jar-sha256-checksum>"
           // ...
       }
   }
   ```
   - Verify the wrapper JAR at the commit matches the checksum:
   ```shell
   curl -sL https://raw.githubusercontent.com/gradle/gradle/<gradle-commit-sha>/gradle/wrapper/gradle-wrapper.jar | sha256sum
   ```


3. **Regenerate the `gradle/wrapper/gradle-wrapper.properties` and the wrapper script**:
   - Find the ***Binary-only (-bin) ZIP Checksum*** for the binary distribution at https://gradle.org/release-checksums/

      ```shell
      ./gradlew wrapper --gradle-version <gradle-version> \
      --distribution-type bin \
      --gradle-distribution-sha256-sum <binary-distribution-checksum>
      ```

After upgrading, verify the Gradle version:

    ./gradlew --version
