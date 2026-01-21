package org.apache.kafka.maven;

import org.apache.kafka.publicapi.PublicApiChecker;
import org.apache.kafka.publicapi.PublicApiViolation;
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Maven plugin for checking that external projects don't use internal Kafka APIs.
 *
 * This mojo runs during the validate phase to check source code for internal API usage.
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
     * Kafka version to validate against.
     */
    @Parameter(property = "kafka.internal-api-checker.kafkaVersion")
    private String kafkaVersion;

    /**
     * Source directories to scan.
     */
    @Parameter
    private List<File> sourceDirectories;


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
        if (sourceDirectories == null || sourceDirectories.isEmpty()) {
            sourceDirectories = getDefaultSourceDirectories();
        }


        getLog().info("Checking for internal Kafka API usage in source directories...");

        try {

            // Get Kafka JARs from project dependencies
            List<File> kafkaJars = getKafkaJarsFromDependencies();

            if (kafkaJars.isEmpty()) {
                getLog().warn("No Kafka dependencies found in project. Skipping internal API check.");
                return;
            }

            // Create class loader with Kafka JARs
            ClassLoader classLoader = PublicApiChecker.createClassLoader(kafkaJars);

            PublicApiChecker checker = new PublicApiChecker(classLoader);

            // Collect all source files
            List<File> sourceFiles = new ArrayList<>();
            for (File sourceDir : sourceDirectories) {
                if (sourceDir.exists() && sourceDir.isDirectory()) {
                    collectJavaFiles(sourceDir, sourceFiles);
                }
            }

            if (sourceFiles.isEmpty()) {
                getLog().info("No Java source files found, skipping internal API check");
                return;
            }

            getLog().info("Checking " + sourceFiles.size() + " Java source files for internal API usage");
            List<PublicApiViolation> violations = checker.checkSourceFiles(sourceFiles);

            // Generate report
            ViolationReporter reporter = new ViolationReporter();
            reporter.writeTextReport(violations, reportFile);

            // Also write JSON report
            File jsonReport = new File(reportFile.getParentFile(),
                reportFile.getName().replace(".txt", ".json"));
            reporter.writeJsonReport(violations, jsonReport);

            // Print summary to console
            reporter.printToConsole(violations, true);

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
                getLog().info("✅ No internal API usage found!");
            }

        } catch (IOException e) {
            throw new MojoExecutionException("Failed to check internal API usage: " + e.getMessage(), e);
        }
    }

    private List<File> getDefaultSourceDirectories() {
        List<File> dirs = new ArrayList<>();

        // Add main source directory
        File mainSrc = new File(project.getBasedir(), "src/main/java");
        if (mainSrc.exists()) {
            dirs.add(mainSrc);
        }

        // Add test source directory
        File testSrc = new File(project.getBasedir(), "src/test/java");
        if (testSrc.exists()) {
            dirs.add(testSrc);
        }

        return dirs;
    }

    private List<File> getKafkaJarsFromDependencies() {
        List<File> kafkaJars = new ArrayList<>();

        for (Artifact artifact : project.getArtifacts()) {
            if ("org.apache.kafka".equals(artifact.getGroupId())) {
                kafkaJars.add(artifact.getFile());
                getLog().debug("Found Kafka dependency: " + artifact.getFile().getName());

                // Auto-detect Kafka version if not set
                if (kafkaVersion == null) {
                    kafkaVersion = artifact.getVersion();
                    getLog().info("Auto-detected Kafka version: " + kafkaVersion);
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

    public void setKafkaVersion(String kafkaVersion) {
        this.kafkaVersion = kafkaVersion;
    }

    public void setSourceDirectories(List<File> sourceDirectories) {
        this.sourceDirectories = sourceDirectories;
    }

    public void setReportFile(File reportFile) {
        this.reportFile = reportFile;
    }
}