package org.apache.kafka.gradle;

import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.tasks.TaskProvider;

/**
 * Gradle plugin for checking that external projects don't use internal Kafka APIs.
 * This plugin is intended to be published and used by external Kafka plugin/application developers.
 */
public class KafkaInternalApiCheckerPlugin implements Plugin<Project> {

    @Override
    public void apply(Project project) {
        // Create the extension for configuration
        KafkaInternalApiCheckerExtension extension = project.getExtensions()
            .create("kafkaInternalApiChecker", KafkaInternalApiCheckerExtension.class, project);

        // Register the task
        TaskProvider<KafkaInternalApiCheckerTask> taskProvider = project.getTasks()
            .register("kafkaInternalApiChecker", KafkaInternalApiCheckerTask.class, task -> {
                task.getCheckerEnabled().set(extension.getEnabled());
                task.getFailOnViolation().set(extension.getFailOnViolation());
                task.getClassDirs().set(extension.getClassDirs());
                task.getReportFile().set(extension.getReportFile());
            });

        // Configure task to run as part of verification
        project.afterEvaluate(p -> {
            // Bytecode scan requires compiled output; ensure the project's classes task has run.
            taskProvider.configure(task -> task.dependsOn(project.getTasks().named("classes")));

            // Integrate into the standard build lifecycle - add to 'check' task
            project.getTasks().named("check").configure(checkTask -> {
                checkTask.dependsOn(taskProvider);
            });
        });

        // Add helpful task to skip checking
        project.getTasks().register("skipInternalApiCheck", task -> {
            task.setGroup("verification");
            task.setDescription("Disables internal API checking for this build");
            task.doLast(t -> {
                extension.getEnabled().set(false);
                project.getLogger().info("Internal API checking disabled for this build");
            });
        });

        project.getLogger().debug("Applied KafkaInternalApiChecker plugin to project: {}", project.getName());
    }
}