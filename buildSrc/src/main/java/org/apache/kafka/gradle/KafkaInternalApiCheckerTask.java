package org.apache.kafka.gradle;

import org.apache.kafka.publicapi.PublicApiChecker;
import org.apache.kafka.publicapi.PublicApiViolation;
import org.apache.kafka.publicapi.ViolationReporter;
import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.ResolvedArtifact;
import org.gradle.api.file.FileCollection;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.provider.ListProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.TaskAction;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Gradle task for checking that external projects don't use internal Kafka APIs.
 */
public class KafkaInternalApiCheckerTask extends DefaultTask {

    private final Property<Boolean> enabled = getProject().getObjects().property(Boolean.class);
    private final Property<Boolean> failOnViolation = getProject().getObjects().property(Boolean.class);
    private final Property<FileCollection> sourceDirs = getProject().getObjects().property(FileCollection.class);
    private final RegularFileProperty reportFile = getProject().getObjects().fileProperty();

    public KafkaInternalApiCheckerTask() {
        setGroup("verification");
        setDescription("Checks that source code doesn't use internal Kafka APIs");

        // Set default values
        enabled.convention(true);
        failOnViolation.convention(true);
        sourceDirs.convention(getProject().files("src/main/java"));
        reportFile.convention(getProject().getLayout().getBuildDirectory().file("reports/kafka-internal-api-usage.txt"));
    }

    @TaskAction
    public void checkInternalApiUsage() {
        if (!getCheckerEnabled().get()) {
            getLogger().info("KafkaInternalApiChecker is disabled, skipping...");
            return;
        }

        FileCollection sources = sourceDirs.get();
        if (sources.isEmpty()) {
            getLogger().info("No source directories configured, skipping internal API check");
            return;
        }

        getLogger().info("Checking for internal Kafka API usage in source directories...");

        try {

            // Get Kafka JARs from project dependencies
            List<File> kafkaJars = getKafkaJarsFromDependencies();

            if (kafkaJars.isEmpty()) {
                getLogger().warn("No Kafka dependencies found in project. Skipping internal API check.");
                return;
            }

            // Create class loader with Kafka JARs
            ClassLoader classLoader = PublicApiChecker.createClassLoader(kafkaJars);

            PublicApiChecker checker = new PublicApiChecker(classLoader);

            // Collect all source files
            List<File> sourceFiles = new ArrayList<>();
            for (File sourceDir : sources.getFiles()) {
                if (sourceDir.exists() && sourceDir.isDirectory()) {
                    collectJavaFiles(sourceDir, sourceFiles);
                }
            }

            if (sourceFiles.isEmpty()) {
                getLogger().info("No Java source files found, skipping internal API check");
                return;
            }

            getLogger().info("Checking {} Java source files for internal API usage", sourceFiles.size());
            List<PublicApiViolation> violations = checker.checkSourceFiles(sourceFiles);

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

    private void collectJavaFiles(File dir, List<File> javaFiles) {
        File[] files = dir.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    collectJavaFiles(file, javaFiles);
                } else if (file.getName().endsWith(".java")) {
                    javaFiles.add(file);
                }
            }
        }
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
    public Property<FileCollection> getSourceDirs() {
        return sourceDirs;
    }

    @OutputFile
    public RegularFileProperty getReportFile() {
        return reportFile;
    }
}