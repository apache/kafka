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
    private final ConfigurableFileCollection classDirs;
    private final RegularFileProperty reportFile;

    public KafkaInternalApiCheckerExtension(Project project) {
        this.enabled = project.getObjects().property(Boolean.class);
        this.enabled.convention(true);

        this.failOnViolation = project.getObjects().property(Boolean.class);
        this.failOnViolation.convention(true);

        this.classDirs = project.getObjects().fileCollection();
        // Default to standard compiled-class output directory; covers java/scala/kotlin subdirs.
        this.classDirs.from(project.file("build/classes"));

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

    public ConfigurableFileCollection getClassDirs() {
        return classDirs;
    }

    public void setClassDirs(Object... classDirs) {
        this.classDirs.setFrom(classDirs);
    }

    public RegularFileProperty getReportFile() {
        return reportFile;
    }

    public void setReportFile(File reportFile) {
        this.reportFile.set(reportFile);
    }
}