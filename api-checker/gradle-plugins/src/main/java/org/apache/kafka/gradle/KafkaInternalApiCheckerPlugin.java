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

import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.artifacts.component.ModuleComponentIdentifier;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.tasks.SourceSetContainer;
import org.gradle.api.tasks.TaskProvider;

/**
 * Gradle plugin for checking that external projects don't use internal Kafka APIs.
 * This plugin is intended to be published and used by external Kafka plugin/application developers.
 *
 * <h2>Wiring strategy</h2>
 *
 * The plugin uses a reactive {@code Plugins.withType(JavaPlugin.class)} callback rather than
 * {@code project.afterEvaluate(...)} for the source-set + lifecycle wiring. Two reasons:
 *
 * <ul>
 *   <li><b>Order-insensitive</b>. {@code withType} fires whenever the Java plugin is applied,
 *       regardless of whether the user listed {@code id 'java'} before or after this plugin.
 *       Scala and Kotlin plugins apply JavaPlugin transitively, so the same callback also
 *       handles those.</li>
 *   <li><b>Configuration-cache friendly</b>. {@code afterEvaluate} forces eager evaluation
 *       at configuration time, which is at odds with the configuration cache and with
 *       Gradle's longer-term direction (isolated projects). The reactive form survives both.</li>
 * </ul>
 *
 * <h2>How {@code classDirs} is fed</h2>
 *
 * The extension's {@code classDirs} starts empty. When the Java plugin is applied, this plugin
 * <em>adds</em> {@code sourceSets.main.output.classesDirs} as a contributor. That FileCollection
 * carries producer-task info for the compile tasks, so Gradle's {@code @InputFiles} validation
 * can infer the {@code compileJava} / {@code compileScala} / {@code compileKotlin} ordering
 * automatically — no manual {@code dependsOn} required. Users can still call {@code .from(…)}
 * on the extension to extend, or {@code .setFrom(…)} to replace.
 *
 * <p>The task's {@code classDirs} is wired to the extension's {@code ConfigurableFileCollection}
 * by reference at task-registration time. Because {@code Property<FileCollection>.set(FileCollection)}
 * stores the same reference, later {@code .from(...)} calls on the extension (including the
 * main-source-set contribution made by the {@code withType} callback) are visible at task
 * execution time.
 */
public class KafkaInternalApiCheckerPlugin implements Plugin<Project> {

    /** Project property name that disables this checker for the current Gradle invocation. */
    static final String SKIP_PROPERTY = "kafkaInternalApiChecker.skip";

    @Override
    public void apply(Project project) {
        // Create the extension for configuration
        KafkaInternalApiCheckerExtension extension = project.getExtensions()
            .create("kafkaInternalApiChecker", KafkaInternalApiCheckerExtension.class, project);

        if (KafkaPublicApiCheckerPlugin.isPropertyTruthy(project, SKIP_PROPERTY)) {
            extension.getEnabled().set(false);
            project.getLogger().lifecycle("Internal API checking disabled via -P{}", SKIP_PROPERTY);
        }

        // Register the task. classDirs is wired directly to the extension's ConfigurableFileCollection
        // by reference, so anything added to extension.classDirs later (either by the JavaPlugin
        // hook below or by the user's build script) is visible to the task at execution time.
        TaskProvider<KafkaInternalApiCheckerTask> taskProvider = project.getTasks()
            .register("kafkaInternalApiChecker", KafkaInternalApiCheckerTask.class, task -> {
                task.getCheckerEnabled().set(extension.getEnabled());
                task.getFailOnViolation().set(extension.getFailOnViolation());
                task.getFailOnNoKafkaDependency().set(extension.getFailOnNoKafkaDependency());
                task.getClassDirs().set(extension.getClassDirs());
                task.getKafkaDependencyJars().from(extension.getKafkaDependencyJars());
                task.getReportFile().set(extension.getReportFile());
            });

        // Reactive hook: fires when the Java plugin (or any plugin that applies JavaPlugin
        // transitively, e.g. Scala or Kotlin) becomes part of this project. See the class-level
        // javadoc for why this is preferred over `project.afterEvaluate(...)`.
        project.getPlugins().withType(JavaPlugin.class, plugin -> {
            SourceSetContainer sourceSets = project.getExtensions().getByType(SourceSetContainer.class);

            // Contribute main source set output as the default scanning target. Wrapped in a
            // Provider so the underlying FileCollection (and its producer-task dependencies)
            // is resolved lazily — important for configuration cache compatibility.
            extension.getClassDirs().from(
                sourceSets.named("main").map(s -> s.getOutput().getClassesDirs())
            );

            // Default kafkaDependencyJars: the org.apache.kafka-filtered artifact view of the
            // project's compile classpaths. Wired through ArtifactView (not raw resolution) so
            // (a) it's a declared task input — a Kafka version bump invalidates the task — and
            // (b) resolution is deferred to execution time, which is configuration-cache safe.
            extension.getKafkaDependencyJars().from(
                kafkaArtifactsFromConfiguration(project, JavaPlugin.COMPILE_CLASSPATH_CONFIGURATION_NAME),
                kafkaArtifactsFromConfiguration(project, JavaPlugin.TEST_COMPILE_CLASSPATH_CONFIGURATION_NAME)
            );

            // Lifecycle wiring: make `check` depend on our task. The 'check' task itself comes
            // from LifecycleBasePlugin, which JavaPlugin pulls in.
            project.getTasks().named("check").configure(checkTask -> checkTask.dependsOn(taskProvider));
        });

        project.getLogger().debug("Applied KafkaInternalApiChecker plugin to project: {}", project.getName());
    }

    private static org.gradle.api.file.FileCollection kafkaArtifactsFromConfiguration(Project project, String configurationName) {
        return project.getConfigurations().getByName(configurationName)
                .getIncoming()
                .artifactView(view -> view.componentFilter(id ->
                        id instanceof ModuleComponentIdentifier
                                && "org.apache.kafka".equals(((ModuleComponentIdentifier) id).getGroup())))
                .getFiles();
    }
}