# Gradle Wrapper

This directory contains the Gradle wrapper files for the Apache Kafka project.

## Upgrading Gradle version

When upgrading the Gradle version used by the Kafka project, update the following files in order:

1. **Update Gradle version** in `gradle/dependencies.gradle`:
   ```groovy
   gradle: "9.2.0"
   ```

2. **Update wrapper JAR commit and checksum** in `wrapper.gradle`:
   - Find the ***Wrapper JAR Checksum*** at https://gradle.org/release-checksums/
   - Find the commit in the Gradle repository that updated the wrapper JAR to the new version
     (usually titled "Update to Gradle <gradle-version>"):
     https://api.github.com/repos/gradle/gradle/commits?path=gradle/wrapper/gradle-wrapper.jar

     Do not use the release tag (`v<gradle-version>`): it may contain the wrapper JAR of the previous
     Gradle version, see https://github.com/gradle/gradle/issues/39471.
   - Update the `wrapperJarCommit` and `wrapperChecksum` variables:
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
