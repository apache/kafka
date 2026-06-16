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

import org.gradle.api.DefaultTask;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.api.tasks.javadoc.Javadoc;

/**
 * Gradle plugin for checking public API consistency in the Kafka codebase.
 * This is an internal plugin that runs as part of Kafka's own build process.
 */
public class KafkaPublicApiCheckerPlugin implements Plugin<Project> {

    @Override
    public void apply(Project project) {
        // Create the extension for configuration
        KafkaPublicApiCheckerExtension extension = project.getExtensions()
            .create("kafkaPublicApiChecker", KafkaPublicApiCheckerExtension.class, project);

        // Register the task
        TaskProvider<KafkaPublicApiCheckerTask> taskProvider = project.getTasks()
            .register("kafkaPublicApiChecker", KafkaPublicApiCheckerTask.class, task -> {
                task.getCheckerEnabled().set(extension.getEnabled());
                task.getFailOnViolation().set(extension.getFailOnViolation());
                task.getJavadocJarPath().set(extension.getJavadocJarPath());
                task.getProjectJarFiles().from(extension.getProjectJarFiles());
                task.getReferenceJarFiles().from(extension.getReferenceJarFiles());
                task.getEnforceJavadocConsistency().set(extension.getEnforceJavadocConsistency());
                task.getReportFile().set(extension.getReportFile());
            });

        // Configure task to run after javadoc
        project.afterEvaluate(p -> {
            TaskProvider<DefaultTask> javadocTask = project.getTasks().named("docsJar", DefaultTask.class);

            // Make sure our task runs after javadoc completes
            taskProvider.configure(task -> {
                task.mustRunAfter(javadocTask);
                task.dependsOn(javadocTask);

                // Auto-configure javadoc JAR path if not set
                task.getJavadocJarPath().convention(
                    p.getLayout().file(p.provider(() -> {
                        if (extension.getJavadocJarPath().isPresent()) {
                            return extension.getJavadocJarPath().get().getAsFile();
                        }
                        // Default location based on project name and version
                        String projectName = p.getName();
                        String version = p.getVersion().toString();
                        return new java.io.File(p.getBuildDir(), "libs/" + projectName + "-" + version + "-javadoc.jar");
                    }))
                );
            });

            // Make javadoc task finalize with our checker
            javadocTask.configure(javadoc -> javadoc.finalizedBy(taskProvider));

            // Add to check task dependencies if it exists
            project.getTasks().matching(task -> task.getName().equals("check")).configureEach(checkTask -> {
                checkTask.dependsOn(taskProvider);
            });
        });

        // Add helpful task to skip checking
        project.getTasks().register("skipPublicApiCheck", task -> {
            task.setGroup("verification");
            task.setDescription("Disables public API checking for this build");
            task.doLast(t -> {
                extension.getEnabled().set(false);
                project.getLogger().info("Public API checking disabled for this build");
            });
        });

        project.getLogger().debug("Applied KafkaPublicApiChecker plugin to project: {}", project.getName());
    }
}