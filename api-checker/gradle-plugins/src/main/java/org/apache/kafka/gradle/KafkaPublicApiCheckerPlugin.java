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

import org.gradle.api.DefaultTask;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.api.tasks.bundling.Jar;

/**
 * Gradle plugin for checking public API consistency in the Kafka codebase.
 * This is an internal plugin that runs as part of Kafka's own build process.
 */
public class KafkaPublicApiCheckerPlugin implements Plugin<Project> {

    /** Project property name that disables this checker for the current Gradle invocation. */
    static final String SKIP_PROPERTY = "kafkaPublicApiChecker.skip";

    @Override
    public void apply(Project project) {
        // Create the extension for configuration
        KafkaPublicApiCheckerExtension extension = project.getExtensions()
            .create("kafkaPublicApiChecker", KafkaPublicApiCheckerExtension.class, project);

        if (isPropertyTruthy(project, SKIP_PROPERTY)) {
            extension.getEnabled().set(false);
        }

        // Register the task
        TaskProvider<KafkaPublicApiCheckerTask> taskProvider = project.getTasks()
            .register("kafkaPublicApiChecker", KafkaPublicApiCheckerTask.class, task -> {
                task.getCheckerEnabled().set(extension.getEnabled());
                task.getFailOnViolation().set(extension.getFailOnViolation());
                // Intentionally NOT calling task.getJavadocJarPath().set(extension.getJavadocJarPath())
                // here — `set(Provider)` marks the property as explicitly assigned even if the upstream
                // is absent, which silently disables any later `.convention(...)` we install in
                // afterEvaluate. The convention block below is the single source of truth.
                task.getProjectJarFiles().from(extension.getProjectJarFiles());
                task.getReferenceJarFiles().from(extension.getReferenceJarFiles());
                task.getReportFile().set(extension.getReportFile());
            });

        // Configure task to run after javadoc
        project.afterEvaluate(p -> {
            TaskProvider<DefaultTask> docsJarTask = p.getTasks().named("docsJar", DefaultTask.class);

            // Wire the checker's `javadocJarPath` input to the `javadocJar` task's `archiveFile`
            // output where it exists. Reading the task's lazy output Provider — rather than
            // recomputing the path from project.name + version — guarantees we read the *exact*
            // file this Gradle run produced, which closes a stale-jar foot-gun:
            //
            //   build/libs/kafka-foo-4.3.0-SNAPSHOT-javadoc.jar   (left over from a months-old build)
            //   build/libs/kafka-foo-4.4.0-SNAPSHOT-javadoc.jar   (just produced this run)
            //
            // The pre-fix auto-detect listed the directory and picked files[0], which on APFS
            // returned the 4.3.0 (alphabetically earlier) jar. The checker then compared
            // months-old javadoc against current bytecode and reported "all clean" while the
            // module had real MISSING_PUBLICAPI_ANNOTATION violations against the fresh code.
            Task javadocJarRaw = p.getTasks().findByName("javadocJar");
            taskProvider.configure(task -> {
                task.mustRunAfter(docsJarTask);
                task.dependsOn(docsJarTask);

                if (javadocJarRaw instanceof Jar) {
                    Jar javadocJar = (Jar) javadocJarRaw;
                    task.getJavadocJarPath().convention(
                        extension.getJavadocJarPath().orElse(javadocJar.getArchiveFile())
                    );
                } else {
                    // No `javadocJar` task on this project — fall back to whatever the extension
                    // declared. The task's @TaskAction throws a clear error if neither is set,
                    // rather than silently scanning the wrong file.
                    task.getJavadocJarPath().convention(extension.getJavadocJarPath());
                }
            });

            // Make javadoc task finalize with our checker
            docsJarTask.configure(javadoc -> javadoc.finalizedBy(taskProvider));

            // Add to check task dependencies if it exists
            project.getTasks().matching(task -> task.getName().equals("check")).configureEach(checkTask -> {
                checkTask.dependsOn(taskProvider);
            });
        });

        if (isPropertyTruthy(project, SKIP_PROPERTY)) {
            project.getLogger().lifecycle("Public API checking disabled via -P{}", SKIP_PROPERTY);
        }

        project.getLogger().debug("Applied KafkaPublicApiChecker plugin to project: {}", project.getName());
    }

    /** Treat property values "true", "1", "yes" (case-insensitive) — or an empty value (just {@code -PsomeProp}) — as true. */
    static boolean isPropertyTruthy(Project project, String name) {
        if (!project.hasProperty(name)) return false;
        Object raw = project.findProperty(name);
        if (raw == null) return true;
        String s = raw.toString().trim();
        return s.isEmpty() || "true".equalsIgnoreCase(s) || "1".equals(s) || "yes".equalsIgnoreCase(s);
    }
}