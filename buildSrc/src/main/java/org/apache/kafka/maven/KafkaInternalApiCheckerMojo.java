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
package org.apache.kafka.maven;

import org.apache.kafka.publicapi.PublicApiChecker;
import org.apache.kafka.publicapi.PublicApiViolation;
import org.apache.kafka.publicapi.ScanResult;
import org.apache.kafka.publicapi.ViolationReporter;

import org.apache.maven.artifact.Artifact;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.plugins.annotations.ResolutionScope;
import org.apache.maven.project.MavenProject;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Maven plugin for checking that external projects don't use internal Kafka APIs.
 *
 * <p>Scans compiled bytecode (.class files) under the project's build output directory, so it
 * works uniformly for Java, Scala, Kotlin and any other JVM-language consumer. Runs during the
 * verify phase after compilation has produced the bytecode it inspects.
 */
@Mojo(name = "verify",
      defaultPhase = LifecyclePhase.VERIFY,
      requiresDependencyResolution = ResolutionScope.COMPILE_PLUS_RUNTIME,
      threadSafe = true)
public class KafkaInternalApiCheckerMojo extends AbstractMojo {

    /**
     * The Maven project.
     */
    @Parameter(defaultValue = "${project}", required = true, readonly = true)
    private MavenProject project;

    /**
     * Enable/disable the checker.
     */
    @Parameter(property = "kafka.internal-api-checker.enabled", defaultValue = "true")
    private boolean enabled;

    /**
     * Fail build on violations.
     */
    @Parameter(property = "kafka.internal-api-checker.failOnViolation", defaultValue = "true")
    private boolean failOnViolation;

    /**
     * Compiled-class directories to scan. Defaults to the project's main and test build output
     * directories. Each entry may be a directory of {@code .class} files, an individual
     * {@code .class} file, or a {@code .jar} archive.
     */
    @Parameter
    private List<File> classesDirectories;


    /**
     * Report file location.
     */
    @Parameter(defaultValue = "${project.build.directory}/reports/kafka-internal-api-usage.txt")
    private File reportFile;

    @Override
    public void execute() throws MojoExecutionException, MojoFailureException {
        if (!enabled) {
            getLog().info("KafkaInternalApiChecker is disabled, skipping...");
            return;
        }

        // Set defaults
        if (classesDirectories == null || classesDirectories.isEmpty()) {
            classesDirectories = getDefaultClassesDirectories();
        }


        getLog().info("Checking for internal Kafka API usage in compiled bytecode...");

        try {

            // Get Kafka JARs from project dependencies
            List<File> kafkaJars = getKafkaJarsFromDependencies();

            if (kafkaJars.isEmpty()) {
                getLog().warn("No Kafka dependencies found in project. Skipping internal API check.");
                return;
            }

            PublicApiChecker checker = new PublicApiChecker(kafkaJars);

            // Collect class file roots (directories and any explicitly-listed jars).
            List<File> classRoots = new ArrayList<>();
            for (File root : classesDirectories) {
                if (root.exists()) {
                    classRoots.add(root);
                }
            }

            if (classRoots.isEmpty()) {
                getLog().info("No class files found, skipping internal API check");
                return;
            }

            getLog().info("Scanning " + classRoots.size() + " class file root(s) for internal API usage");
            ScanResult result = checker.checkBytecode(classRoots);
            List<PublicApiViolation> violations = result.getViolations();
            List<PublicApiViolation> suppressions = result.getSuppressions();

            // Generate report
            ViolationReporter reporter = new ViolationReporter();
            reporter.writeTextReport(violations, suppressions, reportFile);

            // Also write JSON report
            File jsonReport = ViolationReporter.jsonReportFor(reportFile);
            reporter.writeJsonReport(violations, suppressions, jsonReport);

            // Print summary to console
            reporter.printToConsole(violations, suppressions, true);

            getLog().info("Internal API usage check completed. Report written to: " + reportFile.getAbsolutePath());

            if (!violations.isEmpty()) {
                String message = String.format("Found %d internal API usage violations. See report: %s",
                    violations.size(), reportFile.getAbsolutePath());

                if (failOnViolation) {
                    throw new MojoFailureException(message);
                } else {
                    getLog().warn(message);
                }
            } else {
                getLog().info("No internal API usage found.");
            }

        } catch (IOException e) {
            throw new MojoExecutionException("Failed to check internal API usage: " + e.getMessage(), e);
        }
    }

    private List<File> getDefaultClassesDirectories() {
        List<File> dirs = new ArrayList<>();

        // Main compiled output
        File mainClasses = new File(project.getBuild().getOutputDirectory());
        if (mainClasses.exists()) {
            dirs.add(mainClasses);
        }

        // Test compiled output
        File testClasses = new File(project.getBuild().getTestOutputDirectory());
        if (testClasses.exists()) {
            dirs.add(testClasses);
        }

        return dirs;
    }

    private List<File> getKafkaJarsFromDependencies() {
        List<File> kafkaJars = new ArrayList<>();

        for (Artifact artifact : project.getArtifacts()) {
            if ("org.apache.kafka".equals(artifact.getGroupId())) {
                kafkaJars.add(artifact.getFile());
                getLog().debug("Found Kafka dependency: " + artifact.getFile().getName());
            }
        }

        return kafkaJars;
    }

    // Getters and setters for testing
    public void setProject(MavenProject project) {
        this.project = project;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public void setFailOnViolation(boolean failOnViolation) {
        this.failOnViolation = failOnViolation;
    }

    public void setClassesDirectories(List<File> classesDirectories) {
        this.classesDirectories = classesDirectories;
    }

    public void setReportFile(File reportFile) {
        this.reportFile = reportFile;
    }
}