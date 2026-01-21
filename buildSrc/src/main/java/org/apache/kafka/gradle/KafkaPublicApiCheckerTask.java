package org.apache.kafka.gradle;

import org.apache.kafka.publicapi.PublicApiChecker;
import org.apache.kafka.publicapi.PublicApiViolation;
import org.apache.kafka.publicapi.ViolationReporter;
import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.provider.ListProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFile;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.Optional;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.TaskAction;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Gradle task for checking public API consistency in Kafka codebase.
 */
public class KafkaPublicApiCheckerTask extends DefaultTask {

    private final Property<Boolean> enabled = getProject().getObjects().property(Boolean.class);
    private final Property<Boolean> failOnViolation = getProject().getObjects().property(Boolean.class);
    private final RegularFileProperty javadocJarPath = getProject().getObjects().fileProperty();
    private final ConfigurableFileCollection projectJarFiles = getProject().getObjects().fileCollection();
    private final Property<Boolean> enforceJavadocConsistency = getProject().getObjects().property(Boolean.class);
    private final RegularFileProperty reportFile = getProject().getObjects().fileProperty();

    public KafkaPublicApiCheckerTask() {
        setGroup("verification");
        setDescription("Checks consistency between javadoc HTML files and @PublicApi annotations across project JARs");

        // Set default values
        enabled.convention(true);
        failOnViolation.convention(true);
        enforceJavadocConsistency.convention(true);
        reportFile.convention(getProject().getLayout().getBuildDirectory().file("reports/kafka-public-api-checker.txt"));
    }

    @TaskAction
    public void checkPublicApi() {
        if (!getCheckerEnabled().get()) {
            getLogger().info("KafkaPublicApiChecker is disabled, skipping...");
            return;
        }

        File jarFile = getJavadocJarFile();
        if (!jarFile.exists()) {
            throw new GradleException("Javadoc JAR file not found: " + jarFile.getAbsolutePath() +
                ". Make sure the javadoc task has run first.");
        }

        getLogger().info("Checking public API consistency in: {}", jarFile.getAbsolutePath());
        Configuration compileClasspath = getProject().getConfigurations().getByName("compileClasspath");

        try {

            // Collect all JAR files for class loader (javadoc JAR + project JARs)
            List<File> allJarFiles = new ArrayList<>();
            allJarFiles.add(jarFile);
            allJarFiles.addAll(projectJarFiles.getFiles());
            allJarFiles.addAll(compileClasspath.getFiles());

            // Create class loader with all JAR files
            ClassLoader classLoader = PublicApiChecker.createClassLoader(allJarFiles);

            PublicApiChecker checker = new PublicApiChecker(classLoader);

            // Use new dual validation approach
            List<PublicApiViolation> violations;
            if (projectJarFiles.getFiles().isEmpty()) {
                throw  new GradleException("Project JAR file not found: " + jarFile.getAbsolutePath());
            } else {
                // Use dual validation
                violations = checker.checkPublicApiConsistency(jarFile, new ArrayList<>(projectJarFiles.getFiles()));
            }

            // Generate report
            ViolationReporter reporter = new ViolationReporter();
            File report = reportFile.get().getAsFile();
            reporter.writeTextReport(violations, report);

            // Also write JSON report
            File jsonReport = new File(report.getParentFile(),
                report.getName().replace(".txt", ".json"));
            reporter.writeJsonReport(violations, jsonReport);

            // Print summary to console
            reporter.printToConsole(violations, true);

            getLogger().info("Public API check completed. Report written to: {}", report.getAbsolutePath());

            if (!violations.isEmpty()) {
                String message = String.format("Found %d public API violations. See report: %s",
                    violations.size(), report.getAbsolutePath());

                if (failOnViolation.get()) {
                    throw new GradleException(message);
                } else {
                    getLogger().warn(message);
                }
            } else {
                getLogger().info("✅ No public API violations found!");
            }

        } catch (IOException e) {
            throw new GradleException("Failed to check public API: " + e.getMessage(), e);
        }
    }

    private File getJavadocJarFile() {
        if (javadocJarPath.isPresent()) {
            return javadocJarPath.get().getAsFile();
        }

        File buildLibs = new File(getProject().getBuildDir(), "libs");

        // Look for any javadoc JAR
        if (buildLibs.exists()) {
            File[] files = buildLibs.listFiles((dir, name) ->
                name.contains("javadoc") && name.endsWith(".jar"));
            if (files != null && files.length > 0) {
                getLogger().warn("Using auto-detected javadoc JAR: {}", files[0].getName());
                return files[0];
            }
        }

        throw  new GradleException("Javadoc JAR file not found");
    }

    @Input
    public Property<Boolean> getCheckerEnabled() {
        return enabled;
    }

    @Input
    public Property<Boolean> getFailOnViolation() {
        return failOnViolation;
    }

    @InputFile
    @Optional
    public RegularFileProperty getJavadocJarPath() {
        return javadocJarPath;
    }

    @InputFiles
    @Optional
    public ConfigurableFileCollection getProjectJarFiles() {
        return projectJarFiles;
    }

    @Input
    public Property<Boolean> getEnforceJavadocConsistency() {
        return enforceJavadocConsistency;
    }

    @OutputFile
    public RegularFileProperty getReportFile() {
        return reportFile;
    }
}