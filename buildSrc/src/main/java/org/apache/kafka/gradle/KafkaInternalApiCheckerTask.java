package org.apache.kafka.gradle;

import org.apache.kafka.publicapi.PublicApiChecker;
import org.apache.kafka.publicapi.PublicApiViolation;
import org.apache.kafka.publicapi.ScanResult;
import org.apache.kafka.publicapi.ViolationReporter;
import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.ResolvedArtifact;
import org.gradle.api.file.FileCollection;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.TaskAction;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Gradle task for checking that external projects don't use internal Kafka APIs.
 *
 * <p>Scans compiled bytecode (.class files) under the project's class output directories, so it
 * works uniformly for Java, Scala, Kotlin and any other JVM-language consumer. The task
 * therefore runs after the project's {@code classes} task.
 */
public class KafkaInternalApiCheckerTask extends DefaultTask {

    private final Property<Boolean> enabled = getProject().getObjects().property(Boolean.class);
    private final Property<Boolean> failOnViolation = getProject().getObjects().property(Boolean.class);
    private final Property<FileCollection> classDirs = getProject().getObjects().property(FileCollection.class);
    private final RegularFileProperty reportFile = getProject().getObjects().fileProperty();

    public KafkaInternalApiCheckerTask() {
        setGroup("verification");
        setDescription("Checks that compiled bytecode doesn't reference internal Kafka APIs");

        // Set default values
        enabled.convention(true);
        failOnViolation.convention(true);
        classDirs.convention(getProject().files("build/classes"));
        reportFile.convention(getProject().getLayout().getBuildDirectory().file("reports/kafka-internal-api-usage.txt"));
    }

    @TaskAction
    public void checkInternalApiUsage() {
        if (!getCheckerEnabled().get()) {
            getLogger().info("KafkaInternalApiChecker is disabled, skipping...");
            return;
        }

        FileCollection classes = classDirs.get();
        if (classes.isEmpty()) {
            getLogger().info("No class directories configured, skipping internal API check");
            return;
        }

        getLogger().info("Checking for internal Kafka API usage in compiled bytecode...");

        try {

            // Get Kafka JARs from project dependencies
            List<File> kafkaJars = getKafkaJarsFromDependencies();

            if (kafkaJars.isEmpty()) {
                getLogger().warn("No Kafka dependencies found in project. Skipping internal API check.");
                return;
            }

            PublicApiChecker checker = new PublicApiChecker(kafkaJars);

            // Collect class file roots (directories and any explicitly-listed jars).
            List<File> classRoots = new ArrayList<>();
            for (File root : classes.getFiles()) {
                if (root.exists()) {
                    classRoots.add(root);
                }
            }

            if (classRoots.isEmpty()) {
                getLogger().info("No class files found, skipping internal API check");
                return;
            }

            getLogger().info("Scanning {} class file root(s) for internal API usage", classRoots.size());
            ScanResult result = checker.checkBytecode(classRoots);
            List<PublicApiViolation> violations = result.getViolations();
            List<PublicApiViolation> suppressions = result.getSuppressions();

            // Generate report
            ViolationReporter reporter = new ViolationReporter();
            File report = reportFile.get().getAsFile();
            reporter.writeTextReport(violations, suppressions, report);

            // Also write JSON report
            File jsonReport = new File(report.getParentFile(),
                report.getName().replace(".txt", ".json"));
            reporter.writeJsonReport(violations, suppressions, jsonReport);

            // Print summary to console
            reporter.printToConsole(violations, suppressions, true);

            getLogger().info("Internal API usage check completed. Report written to: {}", report.getAbsolutePath());

            if (!violations.isEmpty()) {
                String message = String.format("Found %d internal API usage violations. See report: %s",
                    violations.size(), report.getAbsolutePath());

                if (failOnViolation.get()) {
                    throw new GradleException(message);
                } else {
                    getLogger().warn(message);
                }
            } else {
                getLogger().info("✅ No internal API usage found!");
            }

        } catch (IOException e) {
            throw new GradleException("Failed to check internal API usage: " + e.getMessage(), e);
        }
    }

    private List<File> getKafkaJarsFromDependencies() {
        List<File> kafkaJars = new ArrayList<>();

        // Check all configurations for Kafka dependencies
        for (Configuration configuration : getProject().getConfigurations()) {
            if (configuration.isCanBeResolved()) {
                try {
                    for (ResolvedArtifact artifact : configuration.getResolvedConfiguration().getResolvedArtifacts()) {
                        String groupId = artifact.getModuleVersion().getId().getGroup();
                        if ("org.apache.kafka".equals(groupId)) {
                            kafkaJars.add(artifact.getFile());
                            getLogger().debug("Found Kafka dependency: {}", artifact.getFile().getName());
                        }
                    }
                } catch (Exception e) {
                    // Configuration might not be resolvable, skip it
                    getLogger().debug("Could not resolve configuration {}: {}", configuration.getName(), e.getMessage());
                }
            }
        }

        return kafkaJars;
    }

    @Input
    public Property<Boolean> getCheckerEnabled() {
        return enabled;
    }

    @Input
    public Property<Boolean> getFailOnViolation() {
        return failOnViolation;
    }

    @InputFiles
    public Property<FileCollection> getClassDirs() {
        return classDirs;
    }

    @OutputFile
    public RegularFileProperty getReportFile() {
        return reportFile;
    }
}