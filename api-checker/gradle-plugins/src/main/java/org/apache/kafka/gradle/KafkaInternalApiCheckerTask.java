/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import org.apache.kafka.apicheck.PublicApiChecker;
import org.apache.kafka.apicheck.PublicApiViolation;
import org.apache.kafka.apicheck.CheckResult;
import org.apache.kafka.apicheck.ViolationReporter;

import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.FileCollection;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.CacheableTask;

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
@CacheableTask
public class KafkaInternalApiCheckerTask extends DefaultTask {

    private final Property<Boolean> enabled = getProject().getObjects().property(Boolean.class);
    private final Property<Boolean> failOnViolation = getProject().getObjects().property(Boolean.class);
    private final Property<Boolean> failOnNoKafkaDependency = getProject().getObjects().property(Boolean.class);
    private final Property<FileCollection> classDirs = getProject().getObjects().property(FileCollection.class);
    private final ConfigurableFileCollection kafkaDependencyJars = getProject().getObjects().fileCollection();
    private final RegularFileProperty reportFile = getProject().getObjects().fileProperty();

    public KafkaInternalApiCheckerTask() {
        setGroup("verification");
        setDescription("Checks that compiled bytecode doesn't reference internal Kafka APIs");

        // Set default values
        enabled.convention(true);
        failOnViolation.convention(true);
        failOnNoKafkaDependency.convention(false);
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

        List<File> kafkaJars = new ArrayList<>(kafkaDependencyJars.getFiles());
        if (kafkaJars.isEmpty()) {
            handleNoKafkaDependency();
            return;
        }

        List<File> classRoots = PublicApiChecker.collectExistingRoots(classes.getFiles());
        if (classRoots.isEmpty()) {
            getLogger().info("No class files found, skipping internal API check");
            return;
        }

        runCheck(kafkaJars, classRoots);
    }

    private void handleNoKafkaDependency() {
        String msg = "No org.apache.kafka:* dependencies found on the configured "
                + "kafkaDependencyJars. The checker cannot derive an API surface and would "
                + "produce a meaningless '0 violations' report — likely a classpath or "
                + "configuration issue.";
        if (failOnNoKafkaDependency.get()) {
            throw new GradleException(msg);
        }
        getLogger().warn("{} Skipping internal API check. "
                + "Set kafkaInternalApiChecker.failOnNoKafkaDependency = true to make this fatal.", msg);
    }

    private void runCheck(List<File> kafkaJars, List<File> classRoots) {
        try {
            getLogger().info("Scanning {} class file root(s) for internal API usage", classRoots.size());
            CheckResult result = new PublicApiChecker(kafkaJars).checkBytecode(classRoots);
            reportResults(result);
        } catch (IOException e) {
            throw new GradleException("Failed to check internal API usage: " + e.getMessage(), e);
        }
    }

    private void reportResults(CheckResult result) throws IOException {
        List<PublicApiViolation> violations = result.violations();
        List<PublicApiViolation> suppressions = result.suppressions();

        ViolationReporter reporter = new ViolationReporter();
        File report = reportFile.get().getAsFile();
        reporter.writeTextReport(violations, suppressions, report);
        reporter.printToConsole(violations, suppressions);

        getLogger().info("Internal API usage check completed. Report written to: {}", report.getAbsolutePath());

        long unjustified = suppressions.stream().filter(PublicApiViolation::lacksReason).count();
        if (unjustified > 0) {
            getLogger().warn("{} suppression(s) carry no reason — KIP-1265 requires a justification on every @SuppressKafkaInternalApiUsage", unjustified);
        }

        if (violations.isEmpty()) {
            getLogger().info("No internal API usage found.");
            return;
        }

        String message = String.format("Found %d internal API usage violations. See report: %s",
                violations.size(), report.getAbsolutePath());
        if (failOnViolation.get()) {
            throw new GradleException(message);
        }
        getLogger().warn(message);
    }

    @Input
    public Property<Boolean> getCheckerEnabled() {
        return enabled;
    }

    @Input
    public Property<Boolean> getFailOnViolation() {
        return failOnViolation;
    }

    @Input
    public Property<Boolean> getFailOnNoKafkaDependency() {
        return failOnNoKafkaDependency;
    }

    @InputFiles
    @PathSensitive(PathSensitivity.RELATIVE)
    public Property<FileCollection> getClassDirs() {
        return classDirs;
    }

    /**
     * The Kafka jars whose {@code @InterfaceAudience.Public} annotations define the legal API
     * surface. Declared as an {@code @InputFiles} so a Kafka version bump (i.e. a different jar
     * path or content) invalidates the task and re-runs the scan. The plugin wires the default
     * from the project's compile classpath, filtered to {@code org.apache.kafka}.
     */
    @InputFiles
    @PathSensitive(PathSensitivity.RELATIVE)
    public ConfigurableFileCollection getKafkaDependencyJars() {
        return kafkaDependencyJars;
    }

    @OutputFile
    public RegularFileProperty getReportFile() {
        return reportFile;
    }
}