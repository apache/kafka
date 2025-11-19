# Gradle Wrapper

This directory contains the Gradle wrapper files for the Apache Kafka project.

## Upgrading Gradle version

When upgrading the Gradle version used by the Kafka project, update the following files in order:

1. **Update Gradle version** in `gradle/dependencies.gradle`:
   ```groovy
   gradle: "9.2.0"
   ```

2. **Update wrapper JAR checksum** in `wrapper.gradle`:
   - Find the ***Wrapper JAR Checksum*** at https://gradle.org/release-checksums/
   - Update the `wrapperChecksum` variable:
   ```groovy
   task bootstrapWrapper() {
       // ... (other code)
       doLast {
           // ... (other code)
           String wrapperChecksum = "<wrapper-jar-sha256-checksum>"
           // ...
       }
   }
   ```
   - Verify the wrapper JAR URL is accessible at:
     `https://raw.githubusercontent.com/gradle/gradle/v<gradle-version>/gradle/wrapper/gradle-wrapper.jar`


3. **Regenerate the `gradle/wrapper/gradle-wrapper.properties` and the wrapper script**:
   - Find the ***Binary-only (-bin) ZIP Checksum*** for the binary distribution at https://gradle.org/release-checksums/

      ```shell
      ./gradlew wrapper --gradle-version <gradle-version> \
      --distribution-type bin \
      --gradle-distribution-sha256-sum <binary-distribution-checksum>
      ```

After upgrading, verify the Gradle version:

    ./gradlew --version
