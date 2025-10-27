# Gradle Wrapper

This directory contains the Gradle wrapper files for the Apache Kafka project.

## Upgrading Gradle version

When upgrading the Gradle version used by the Kafka project, update the following files in order:

1. **Update Gradle version** in `gradle/dependencies.gradle`:
   ```groovy
   gradle: "9.2.0"
   ```

2. **Update distribution checksums** in `gradle/wrapper/gradle-wrapper.properties`:
   - Find the SHA256 checksum for the binary distribution at https://gradle.org/release-checksums/
   - Update `distributionSha256Sum` with the Binary-only (-bin) ZIP Checksum
   - Update `distributionUrl` to use the latest version from `https://services.gradle.org/distributions/`:
     ```properties
     distributionSha256Sum=<sha256-checksum-from-release-page>
     distributionUrl=https\://services.gradle.org/distributions/gradle-9.2.0-bin.zip
     ```
   - Verify the distribution URL is accessible

3. **Update wrapper JAR checksum** in `wrapper.gradle`:
   - Find the Wrapper JAR Checksum at https://gradle.org/release-checksums/
   - Update the `wrapperChecksum` variable:
   ```groovy
   task bootstrapWrapper() {
    ...
    doLast {
        ...
        String wrapperChecksum = "<wrapper-jar-sha256-checksum>"
     ```
   - Verify the wrapper JAR URL is accessible at:
     `https://raw.githubusercontent.com/gradle/gradle/v<VERSION>/gradle/wrapper/gradle-wrapper.jar`

4. **Regenerate the wrapper script**:
   ```
   ./gradlew wrapper
   ```

After upgrading, verify the Gradle version:

    ./gradlew --version
