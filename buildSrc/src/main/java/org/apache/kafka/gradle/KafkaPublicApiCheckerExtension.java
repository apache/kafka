package org.apache.kafka.gradle;

import org.gradle.api.Project;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.provider.ListProperty;
import org.gradle.api.provider.Property;

import java.io.File;
import java.util.Arrays;

/**
 * Configuration extension for the KafkaPublicApiChecker plugin.
 */
public class KafkaPublicApiCheckerExtension {
    private final Property<Boolean> enabled;
    private final Property<Boolean> failOnViolation;
    private final RegularFileProperty javadocJarPath;
    private final ConfigurableFileCollection projectJarFiles;
    private final ConfigurableFileCollection referenceJarFiles;
    private final Property<Boolean> enforceJavadocConsistency;
    private final ListProperty<String> includePackages;
    private final ListProperty<String> excludePackages;
    private final RegularFileProperty reportFile;

    public KafkaPublicApiCheckerExtension(Project project) {
        this.enabled = project.getObjects().property(Boolean.class);
        this.enabled.convention(true);

        this.failOnViolation = project.getObjects().property(Boolean.class);
        this.failOnViolation.convention(true);

        this.javadocJarPath = project.getObjects().fileProperty();

        this.projectJarFiles = project.getObjects().fileCollection();

        this.referenceJarFiles = project.getObjects().fileCollection();

        this.enforceJavadocConsistency = project.getObjects().property(Boolean.class);
        this.enforceJavadocConsistency.convention(true);

        this.includePackages = project.getObjects().listProperty(String.class);
        this.includePackages.convention(Arrays.asList());

        this.excludePackages = project.getObjects().listProperty(String.class);
        this.excludePackages.convention(Arrays.asList("org.apache.kafka.common.internals"));

        this.reportFile = project.getObjects().fileProperty();
        this.reportFile.convention(project.getLayout().getBuildDirectory().file("reports/kafka-public-api-checker.txt"));
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

    public RegularFileProperty getJavadocJarPath() {
        return javadocJarPath;
    }

    public void setJavadocJarPath(File javadocJarPath) {
        this.javadocJarPath.set(javadocJarPath);
    }

    public ListProperty<String> getIncludePackages() {
        return includePackages;
    }

    public void setIncludePackages(String... packages) {
        this.includePackages.set(Arrays.asList(packages));
    }

    public ListProperty<String> getExcludePackages() {
        return excludePackages;
    }

    public void setExcludePackages(String... packages) {
        this.excludePackages.set(Arrays.asList(packages));
    }

    public ConfigurableFileCollection getProjectJarFiles() {
        return projectJarFiles;
    }

    /**
     * Jars of sibling Kafka modules this project depends on. Their classes are merged into the
     * scanned surface so cross-module {@code @InterfaceAudience.Public} references resolve, but
     * they don't contribute to this module's own javadoc-consistency or cascade iteration.
     */
    public ConfigurableFileCollection getReferenceJarFiles() {
        return referenceJarFiles;
    }

    public Property<Boolean> getEnforceJavadocConsistency() {
        return enforceJavadocConsistency;
    }

    public void setEnforceJavadocConsistency(boolean enforceJavadocConsistency) {
        this.enforceJavadocConsistency.set(enforceJavadocConsistency);
    }

    public RegularFileProperty getReportFile() {
        return reportFile;
    }

    public void setReportFile(File reportFile) {
        this.reportFile.set(reportFile);
    }
}