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

import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.testfixtures.ProjectBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for the enhanced KafkaPublicApiCheckerTask with dual validation.
 */
public class KafkaPublicApiCheckerTaskTest {

    @TempDir
    Path tempDir;

    private Project project;
    private KafkaPublicApiCheckerTask task;

    @BeforeEach
    void setUp() {
        project = ProjectBuilder.builder().build();
        task = project.getTasks().create("checkPublicApi", KafkaPublicApiCheckerTask.class);
    }

    @Test
    void testTaskConfiguration_DefaultValues() {
        assertTrue(task.getCheckerEnabled().get());
        assertTrue(task.getFailOnViolation().get());
    }

    @Test
    void testTaskConfiguration_CustomValues() {
        task.getCheckerEnabled().set(false);
        task.getFailOnViolation().set(false);

        assertFalse(task.getCheckerEnabled().get());
        assertFalse(task.getFailOnViolation().get());
    }

    @Test
    void testTaskExecution_DisabledChecker() {
        task.getCheckerEnabled().set(false);

        // Should not throw exception when disabled
        assertDoesNotThrow(() -> task.checkPublicApi());
    }

    @Test
    void testTaskExecution_MissingJavadocJar() {
        task.getCheckerEnabled().set(true);
        task.getJavadocJarPath().set(new File("nonexistent.jar"));

        GradleException exception = assertThrows(GradleException.class, () -> task.checkPublicApi());
        assertTrue(exception.getMessage().contains("Javadoc JAR file not found"));
    }

    @Test
    void testTaskExecution_WithValidJarFiles() throws IOException {
        File javadocJar = createMockJavadocJar();
        File projectJar = createMockProjectJar();

        task.getCheckerEnabled().set(true);
        task.getJavadocJarPath().set(javadocJar);
        task.getProjectJarFiles().from(projectJar);
        task.getFailOnViolation().set(false); // Don't fail on violations for this test

        // Should not throw exception
        assertDoesNotThrow(() -> task.checkPublicApi());
    }

    @Test
    void testTaskExecution_FailsWhenProjectJarsMissing() throws IOException {
        // Project JAR files are mandatory after the SOLID refactor — without them the scanner has
        // nothing to read. The task must surface this as a configuration error rather than silently
        // running on an empty surface (which would let everything appear "missing javadoc").
        File javadocJar = createMockJavadocJar();

        task.getCheckerEnabled().set(true);
        task.getJavadocJarPath().set(javadocJar);
        task.getFailOnViolation().set(false);

        assertThrows(org.gradle.api.GradleException.class, () -> task.checkPublicApi());
    }

    @Test
    void testTaskInputsOutputs() throws IOException {
        File javadocJar = createMockJavadocJar();
        File projectJar = createMockProjectJar();

        task.getJavadocJarPath().set(javadocJar);
        task.getProjectJarFiles().from(projectJar);

        // Verify inputs are configured
        assertNotNull(task.getJavadocJarPath().getOrNull());
        assertFalse(task.getProjectJarFiles().isEmpty());

        // Verify outputs are configured
        assertNotNull(task.getReportFile().getOrNull());
    }

    @Test
    void testTaskDescription() {
        assertEquals("Checks consistency between javadoc HTML files and @InterfaceAudience.Public annotations across project JARs",
                    task.getDescription());
    }

    @Test
    void testTaskGroup() {
        assertEquals("verification", task.getGroup());
    }

    // Helper methods for creating mock JAR files

    private File createMockJavadocJar() throws IOException {
        File jarFile = tempDir.resolve("test-javadoc.jar").toFile();

        try (JarOutputStream jos = new JarOutputStream(new FileOutputStream(jarFile))) {
            // Add valid class HTML files
            addHtmlEntry(jos, "org/apache/kafka/common/Resource.html",
                "<html><body>Resource class documentation</body></html>");
            addHtmlEntry(jos, "org/apache/kafka/clients/producer/Producer.html",
                "<html><body>Producer interface documentation</body></html>");

            // Add structural HTML files
            addHtmlEntry(jos, "index.html", "<html><body>Index</body></html>");
            addHtmlEntry(jos, "overview-tree.html", "<html><body>Tree</body></html>");
        }

        return jarFile;
    }

    private File createMockProjectJar() throws IOException {
        File jarFile = tempDir.resolve("test-project.jar").toFile();

        try (JarOutputStream jos = new JarOutputStream(new FileOutputStream(jarFile))) {
            // Add some empty entries instead of invalid class files
            addEntry(jos, "META-INF/MANIFEST.MF", "Manifest-Version: 1.0\n".getBytes());
        }

        return jarFile;
    }

    private void addHtmlEntry(JarOutputStream jos, String path, String content) throws IOException {
        JarEntry entry = new JarEntry(path);
        jos.putNextEntry(entry);
        jos.write(content.getBytes());
        jos.closeEntry();
    }

    private void addEntry(JarOutputStream jos, String path, byte[] content) throws IOException {
        JarEntry entry = new JarEntry(path);
        jos.putNextEntry(entry);
        jos.write(content);
        jos.closeEntry();
    }
}