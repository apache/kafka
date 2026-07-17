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

import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.testfixtures.ProjectBuilder;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Plugin-wiring coverage for the consumer-facing {@code internal-api-checker}. The actual scan
 * logic is covered by {@code PluginDeveloperApiUsageScannerTest}; this class verifies that
 * applying the plugin produces a working extension/task and that the JavaPlugin reactive hook
 * contributes the expected defaults (classDirs + kafkaDependencyJars + `check` dependency).
 */
class KafkaInternalApiCheckerPluginTest {

    @Test
    void pluginApply_createsExtensionAndTask() {
        Project project = ProjectBuilder.builder().build();
        project.getPlugins().apply(KafkaInternalApiCheckerPlugin.class);

        assertNotNull(project.getExtensions().findByName("kafkaInternalApiChecker"),
                "expected the extension to be registered");
        assertNotNull(project.getTasks().findByName("kafkaInternalApiChecker"),
                "expected the kafkaInternalApiChecker task to be registered");
    }

    @Test
    void skipProperty_disablesTheChecker() {
        // -PkafkaInternalApiChecker.skip flips extension.enabled to false at configure time
        // (replacement for the old execution-phase skipInternalApiCheck task, whose doLast
        // mutation never reliably propagated).
        Project project = ProjectBuilder.builder().build();
        project.getExtensions().getExtraProperties().set("kafkaInternalApiChecker.skip", "true");
        project.getPlugins().apply(KafkaInternalApiCheckerPlugin.class);

        KafkaInternalApiCheckerExtension extension = (KafkaInternalApiCheckerExtension)
                project.getExtensions().getByName("kafkaInternalApiChecker");
        assertFalse(extension.getEnabled().get(),
                "extension.enabled must flip to false when the skip property is set");
    }

    @Test
    void pluginApplyWithJavaPlugin_wiresClassDirsKafkaJarsAndCheckLifecycle() {
        Project project = ProjectBuilder.builder().build();
        project.getPlugins().apply(KafkaInternalApiCheckerPlugin.class);
        project.getPlugins().apply(JavaPlugin.class);

        KafkaInternalApiCheckerExtension extension = (KafkaInternalApiCheckerExtension)
                project.getExtensions().getByName("kafkaInternalApiChecker");
        KafkaInternalApiCheckerTask task = (KafkaInternalApiCheckerTask)
                project.getTasks().getByName("kafkaInternalApiChecker");

        // classDirs default: sourceSets.main.output.classesDirs contributed by the reactive hook.
        // Resolving via getFiles() walks the contributors lazily.
        assertTrue(extension.getClassDirs().getFiles().stream()
                        .anyMatch(f -> f.getPath().contains("classes")),
                "JavaPlugin hook should contribute main source set output to classDirs, got: "
                        + extension.getClassDirs().getFiles());

        // The new declared input. With no Kafka deps on the test project's classpath the filter
        // produces an empty file collection; what matters is that the file collection itself is
        // wired (not null) so version bumps can later invalidate the task.
        assertNotNull(task.getKafkaDependencyJars(),
                "task.getKafkaDependencyJars() must be declared and wired by the plugin");

        // Lifecycle: kafkaInternalApiChecker must be a dependency of `check`.
        Task checkTask = project.getTasks().getByName("check");
        assertTrue(checkTask.getTaskDependencies().getDependencies(checkTask).stream()
                        .anyMatch(d -> "kafkaInternalApiChecker".equals(d.getName())),
                "`check` task should depend on `kafkaInternalApiChecker`");
    }
}