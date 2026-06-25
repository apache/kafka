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
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.provider.Property;

import java.io.File;

/**
 * Configuration extension for the KafkaInternalApiChecker plugin.
 * This plugin is used by external projects to ensure they don't use internal Kafka APIs.
 */
public class KafkaInternalApiCheckerExtension {
    private final Property<Boolean> enabled;
    private final Property<Boolean> failOnViolation;
    private final Property<Boolean> failOnNoKafkaDependency;
    private final ConfigurableFileCollection classDirs;
    private final ConfigurableFileCollection kafkaDependencyJars;
    private final RegularFileProperty reportFile;

    public KafkaInternalApiCheckerExtension(Project project) {
        this.enabled = project.getObjects().property(Boolean.class);
        this.enabled.convention(true);

        this.failOnViolation = project.getObjects().property(Boolean.class);
        this.failOnViolation.convention(true);

        // Safety net for misconfigured projects. By default the task warns when it can't find
        // any org.apache.kafka:* artifact on the classpath and proceeds with an empty surface
        // (back-compat). Setting this to true turns that warning into a hard failure so a
        // classpath or configuration mistake doesn't silently produce a "0 violations" report.
        this.failOnNoKafkaDependency = project.getObjects().property(Boolean.class);
        this.failOnNoKafkaDependency.convention(false);

        this.classDirs = project.getObjects().fileCollection();
        // Intentionally empty at construction. KafkaInternalApiCheckerPlugin reacts to the
        // Java plugin being applied — see project.getPlugins().withType(JavaPlugin.class) — and
        // adds sourceSets.main.output.classesDirs as a default contributor. That FileCollection
        // carries the producer-task info for compileJava / compileScala / compileKotlin / …, so
        // Gradle's @InputFiles validation can infer the compile-task dependency automatically.
        // A raw project.file("build/classes") would scan the same bytecode but with no producer
        // info attached, tripping "implicit dependency" validation errors on Gradle 9+.
        //
        // Users can still extend or replace this default from their build script:
        //   kafkaInternalApiChecker { classDirs.from(file("extra-classes")) }   // extend
        //   kafkaInternalApiChecker { classDirs.setFrom(file("only-this")) }    // replace

        this.kafkaDependencyJars = project.getObjects().fileCollection();

        this.reportFile = project.getObjects().fileProperty();
        this.reportFile.convention(project.getLayout().getBuildDirectory().file("reports/kafka-internal-api-usage.txt"));
    }

    public Property<Boolean> getEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled.set(enabled);
    }

    public Property<Boolean> getFailOnViolation() {
        return failOnViolation;
    }

    public void setFailOnViolation(boolean failOnViolation) {
        this.failOnViolation.set(failOnViolation);
    }

    public Property<Boolean> getFailOnNoKafkaDependency() {
        return failOnNoKafkaDependency;
    }

    public void setFailOnNoKafkaDependency(boolean failOnNoKafkaDependency) {
        this.failOnNoKafkaDependency.set(failOnNoKafkaDependency);
    }

    public ConfigurableFileCollection getClassDirs() {
        return classDirs;
    }

    public void setClassDirs(Object... classDirs) {
        this.classDirs.setFrom(classDirs);
    }

    public ConfigurableFileCollection getKafkaDependencyJars() {
        return kafkaDependencyJars;
    }

    public void setKafkaDependencyJars(Object... jars) {
        this.kafkaDependencyJars.setFrom(jars);
    }

    public RegularFileProperty getReportFile() {
        return reportFile;
    }

    public void setReportFile(File reportFile) {
        this.reportFile.set(reportFile);
    }
}