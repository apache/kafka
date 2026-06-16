/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.gradle;

import org.apache.kafka.publicapi.CheckResult;
import org.apache.kafka.publicapi.PublicApiChecker;
import org.apache.kafka.publicapi.PublicApiViolation;
import org.apache.kafka.publicapi.ViolationReporter;
import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
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
import java.util.List;

/**
 * Gradle task for checking public API consistency in Kafka codebase.
 */
public class KafkaPublicApiCheckerTask extends DefaultTask {

    private final Property<Boolean> enabled = getProject().getObjects().property(Boolean.class);
    private final Property<Boolean> failOnViolation = getProject().getObjects().property(Boolean.class);
    private final RegularFileProperty javadocJarPath = getProject().getObjects().fileProperty();
    private final ConfigurableFileCollection projectJarFiles = getProject().getObjects().fileCollection();
    private final ConfigurableFileCollection referenceJarFiles = getProject().getObjects().fileCollection();
    private final Property<Boolean> enforceJavadocConsistency = getProject().getObjects().property(Boolean.class);
    private final RegularFileProperty reportFile = getProject().getObjects().fileProperty();

    public KafkaPublicApiCheckerTask() {
        setGroup("verification");
        setDescription("Checks consistency between javadoc HTML files and @InterfaceAudience.Public annotations across project JARs");

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

        try {
            if (projectJarFiles.getFiles().isEmpty()) {
                throw new GradleException("Project JAR file not found: " + jarFile.getAbsolutePath());
            }
            PublicApiChecker checker = new PublicApiChecker(
                new ArrayList<>(projectJarFiles.getFiles()),
                new ArrayList<>(referenceJarFiles.getFiles()));
            CheckResult result = checker.checkPublicApiConsistency(jarFile);
            List<PublicApiViolation> violations = result.violations();
            List<PublicApiViolation> suppressions = result.suppressions();

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

            getLogger().info("Public API check completed. Report written to: {}", report.getAbsolutePath());

            if (!suppressions.isEmpty()) {
                getLogger().lifecycle("⚠️  {} suppression(s) honoured — see report for justifications.", suppressions.size());
            }
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

    @InputFiles
    @Optional
    public ConfigurableFileCollection getReferenceJarFiles() {
        return referenceJarFiles;
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