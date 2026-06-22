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
 * Configuration extension for the KafkaPublicApiChecker plugin.
 */
public class KafkaPublicApiCheckerExtension {
    private final Property<Boolean> enabled;
    private final Property<Boolean> failOnViolation;
    private final RegularFileProperty javadocJarPath;
    private final ConfigurableFileCollection projectJarFiles;
    private final ConfigurableFileCollection referenceJarFiles;
    private final RegularFileProperty reportFile;

    public KafkaPublicApiCheckerExtension(Project project) {
        this.enabled = project.getObjects().property(Boolean.class);
        this.enabled.convention(true);

        this.failOnViolation = project.getObjects().property(Boolean.class);
        this.failOnViolation.convention(true);

        this.javadocJarPath = project.getObjects().fileProperty();

        this.projectJarFiles = project.getObjects().fileCollection();

        this.referenceJarFiles = project.getObjects().fileCollection();

        this.reportFile = project.getObjects().fileProperty();
        this.reportFile.convention(project.getLayout().getBuildDirectory().file("reports/kafka-public-api-checker.txt"));
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

    public RegularFileProperty getJavadocJarPath() {
        return javadocJarPath;
    }

    public void setJavadocJarPath(File javadocJarPath) {
        this.javadocJarPath.set(javadocJarPath);
    }

    public ConfigurableFileCollection getProjectJarFiles() {
        return projectJarFiles;
    }

    /**
     * Jars of sibling Kafka modules this project depends on. Their classes are merged into the
     * scanned surface so cross-module {@code @InterfaceAudience.Public} references resolve, but
     * they don't contribute to this module's own javadoc-consistency or cascade iteration.
     */
    public ConfigurableFileCollection getReferenceJarFiles() {
        return referenceJarFiles;
    }

    public RegularFileProperty getReportFile() {
        return reportFile;
    }

    public void setReportFile(File reportFile) {
        this.reportFile.set(reportFile);
    }
}