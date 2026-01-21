package org.apache.kafka.gradle;

import org.gradle.api.Project;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.provider.ListProperty;
import org.gradle.api.provider.Property;

import java.io.File;
import java.util.Arrays;

/**
 * Configuration extension for the KafkaInternalApiChecker plugin.
 * This plugin is used by external projects to ensure they don't use internal Kafka APIs.
 */
public class KafkaInternalApiCheckerExtension {
    private final Property<Boolean> enabled;
    private final Property<Boolean> failOnViolation;
    private final ConfigurableFileCollection sourceDirs;
    private final RegularFileProperty reportFile;

    public KafkaInternalApiCheckerExtension(Project project) {
        this.enabled = project.getObjects().property(Boolean.class);
        this.enabled.convention(true);

        this.failOnViolation = project.getObjects().property(Boolean.class);
        this.failOnViolation.convention(true);

        this.sourceDirs = project.getObjects().fileCollection();
        // Default to standard Java source directories
        this.sourceDirs.from(project.file("src/main/java"));

        this.reportFile = project.getObjects().fileProperty();
        this.reportFile.convention(project.getLayout().getBuildDirectory().file("reports/kafka-internal-api-usage.txt"));
    }

    public Property<Boolean> getEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled.set(enabled);
    }

    public Property<Boolean> getFailOnViolation() {
        return failOnViolation;
    }

    public void setFailOnViolation(boolean failOnViolation) {
        this.failOnViolation.set(failOnViolation);
    }

    public ConfigurableFileCollection getSourceDirs() {
        return sourceDirs;
    }

    public void setSourceDirs(Object... sourceDirs) {
        this.sourceDirs.setFrom(sourceDirs);
    }

    public RegularFileProperty getReportFile() {
        return reportFile;
    }

    public void setReportFile(File reportFile) {
        this.reportFile.set(reportFile);
    }
}