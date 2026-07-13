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

import org.apache.kafka.apicheck.PublicApiChecker;
import org.apache.kafka.apicheck.PublicApiViolation;
import org.apache.kafka.apicheck.CheckResult;
import org.apache.kafka.apicheck.ViolationReporter;

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
     * Fail the build when no {@code org.apache.kafka:*} artifact is on the project's
     * classpath. The default is {@code false} for back-compat (the mojo warns and skips),
     * but on a project that's expected to depend on Kafka, turning this on catches
     * classpath/config mistakes that would otherwise produce a meaningless "0 violations"
     * report.
     */
    @Parameter(property = "kafka.internal-api-checker.failOnNoKafkaDependency", defaultValue = "false")
    private boolean failOnNoKafkaDependency;

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

        if (classesDirectories == null || classesDirectories.isEmpty()) {
            classesDirectories = getDefaultClassesDirectories();
        }

        getLog().info("Checking for internal Kafka API usage in compiled bytecode...");

        List<File> kafkaJars = getKafkaJarsFromDependencies();
        if (kafkaJars.isEmpty()) {
            handleNoKafkaDependency();
            return;
        }

        List<File> classRoots = PublicApiChecker.collectExistingRoots(classesDirectories);
        if (classRoots.isEmpty()) {
            getLog().info("No class files found, skipping internal API check");
            return;
        }

        runCheck(kafkaJars, classRoots);
    }

    private void handleNoKafkaDependency() throws MojoFailureException {
        String msg = "No org.apache.kafka:* dependencies found on the project classpath. "
                + "The checker cannot derive an API surface and would produce a meaningless "
                + "'0 violations' report — likely a classpath or configuration issue.";
        if (failOnNoKafkaDependency) {
            throw new MojoFailureException(msg);
        }
        getLog().warn(msg + " Skipping internal API check. "
                + "Set <failOnNoKafkaDependency>true</failOnNoKafkaDependency> to make this fatal.");
    }

    private void runCheck(List<File> kafkaJars, List<File> classRoots)
            throws MojoExecutionException, MojoFailureException {
        try {
            getLog().info("Scanning " + classRoots.size() + " class file root(s) for internal API usage");
            CheckResult result = new PublicApiChecker(kafkaJars).checkBytecode(classRoots);
            reportResults(result);
        } catch (IOException e) {
            throw new MojoExecutionException("Failed to check internal API usage: " + e.getMessage(), e);
        }
    }

    private void reportResults(CheckResult result) throws MojoFailureException, IOException {
        List<PublicApiViolation> violations = result.violations();
        List<PublicApiViolation> suppressions = result.suppressions();

        ViolationReporter reporter = new ViolationReporter();
        reporter.writeTextReport(violations, suppressions, reportFile);
        reporter.printToConsole(violations, suppressions);

        getLog().info("Internal API usage check completed. Report written to: " + reportFile.getAbsolutePath());

        long unjustified = suppressions.stream().filter(PublicApiViolation::lacksReason).count();
        if (unjustified > 0) {
            getLog().warn(unjustified + " suppression(s) carry no reason — KIP-1265 requires a justification on every @SuppressKafkaInternalApiUsage");
        }

        if (violations.isEmpty()) {
            getLog().info("No internal API usage found.");
            return;
        }

        String message = String.format("Found %d internal API usage violations. See report: %s",
                violations.size(), reportFile.getAbsolutePath());
        if (failOnViolation) {
            throw new MojoFailureException(message);
        }
        getLog().warn(message);
    }

    /**
     * Default to the project's main compiled output, matching the Gradle plugin's behaviour
     * (which feeds {@code sourceSets.main.output.classesDirs}). Test code legitimately uses
     * internal/test utilities, so including it by default would create noise that isn't a
     * real consumer-side concern. Users who want to scan test code can opt in by setting
     * {@code <classesDirectories>} explicitly.
     */
    private List<File> getDefaultClassesDirectories() {
        List<File> dirs = new ArrayList<>();
        File mainClasses = new File(project.getBuild().getOutputDirectory());
        if (mainClasses.exists()) {
            dirs.add(mainClasses);
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

    public void setFailOnNoKafkaDependency(boolean failOnNoKafkaDependency) {
        this.failOnNoKafkaDependency = failOnNoKafkaDependency;
    }

    public void setClassesDirectories(List<File> classesDirectories) {
        this.classesDirectories = classesDirectories;
    }

    public void setReportFile(File reportFile) {
        this.reportFile = reportFile;
    }
}