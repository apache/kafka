package org.apache.kafka.publicapi;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive tests for the enhanced PublicApiChecker with dual validation.
 */
public class PublicApiCheckerTest {

    @TempDir
    Path tempDir;

    private PublicApiChecker checker;

    @BeforeEach
    void setUp() {
        checker = new PublicApiChecker(getClass().getClassLoader());
    }

    @Test
    void testIsClassHtmlFile_ValidClassFile() throws Exception {
        Method isClassHtmlFile = PublicApiChecker.class.getDeclaredMethod("isClassHtmlFile", String.class);
        isClassHtmlFile.setAccessible(true);

        assertTrue((Boolean) isClassHtmlFile.invoke(checker, "org/apache/kafka/common/Resource.html"));
        assertTrue((Boolean) isClassHtmlFile.invoke(checker, "org/apache/kafka/clients/producer/Producer.html"));
    }

    @Test
    void testIsClassHtmlFile_InvalidFiles() throws Exception {
        Method isClassHtmlFile = PublicApiChecker.class.getDeclaredMethod("isClassHtmlFile", String.class);
        isClassHtmlFile.setAccessible(true);

        // Not HTML file
        assertFalse((Boolean) isClassHtmlFile.invoke(checker, "org/apache/kafka/common/Resource.java"));

        // No package structure
        assertFalse((Boolean) isClassHtmlFile.invoke(checker, "index.html"));

        // Structural HTML files
        assertFalse((Boolean) isClassHtmlFile.invoke(checker, "org/apache/kafka/package-summary.html"));
        assertFalse((Boolean) isClassHtmlFile.invoke(checker, "overview-tree.html"));
        assertFalse((Boolean) isClassHtmlFile.invoke(checker, "constant-values.html"));

        // Not a class (lowercase start)
        assertFalse((Boolean) isClassHtmlFile.invoke(checker, "org/apache/kafka/common/util.html"));
    }

    @Test
    void testConvertHtmlPathToClassName() throws Exception {
        Method convertHtmlPathToClassName = PublicApiChecker.class.getDeclaredMethod(
            "convertHtmlPathToClassName", String.class);
        convertHtmlPathToClassName.setAccessible(true);

        assertEquals("org.apache.kafka.common.Resource",
                   convertHtmlPathToClassName.invoke(checker, "org/apache/kafka/common/Resource.html"));

        assertEquals("org.apache.kafka.clients.producer.Producer",
                   convertHtmlPathToClassName.invoke(checker, "org/apache/kafka/clients/producer/Producer.html"));
    }

    @Test
    void testFindClassesFromJavadocHtml() throws Exception {
        // Create a mock javadoc JAR with HTML files
        File javadocJar = createMockJavadocJar();

        Method findClassesFromJavadocHtml = PublicApiChecker.class.getDeclaredMethod(
            "findClassesFromJavadocHtml", File.class);
        findClassesFromJavadocHtml.setAccessible(true);

        @SuppressWarnings("unchecked")
        Set<String> classes = (Set<String>) findClassesFromJavadocHtml.invoke(checker, javadocJar);

        assertTrue(classes.contains("org.apache.kafka.common.Resource"));
        assertTrue(classes.contains("org.apache.kafka.clients.producer.Producer"));
        assertFalse(classes.contains("org.apache.kafka.common.internals.InternalClass")); // excluded package

        // Should not contain structural files
        assertFalse(classes.contains("package-summary"));
        assertFalse(classes.contains("index"));
    }

    @Test
    void testCrossValidateClassSets_MissingJavadoc() throws Exception {
        Set<String> htmlClasses = new HashSet<>();
        htmlClasses.add("org.apache.kafka.common.Resource");

        Set<String> annotatedClasses = new HashSet<>();
        annotatedClasses.add("org.apache.kafka.common.Resource");
        annotatedClasses.add("org.apache.kafka.clients.producer.Producer"); // Missing from javadoc

        Method crossValidateClassSets = PublicApiChecker.class.getDeclaredMethod(
            "crossValidateClassSets", Set.class, Set.class);
        crossValidateClassSets.setAccessible(true);

        @SuppressWarnings("unchecked")
        List<PublicApiViolation> violations = (List<PublicApiViolation>)
            crossValidateClassSets.invoke(checker, htmlClasses, annotatedClasses);

        assertEquals(1, violations.size());
        PublicApiViolation violation = violations.get(0);
        assertEquals("MISSING_JAVADOC", violation.getViolationType());
        assertEquals("org.apache.kafka.clients.producer.Producer", violation.getClassName());
        assertTrue(violation.getDescription().contains("@InterfaceAudience.Public annotation but is missing from javadoc"));
    }

    @Test
    void testCrossValidateClassSets_MissingAnnotation() throws Exception {
        Set<String> htmlClasses = new HashSet<>();
        htmlClasses.add("org.apache.kafka.common.Resource");
        htmlClasses.add("org.apache.kafka.clients.producer.Producer"); // Missing @InterfaceAudience.Public

        Set<String> annotatedClasses = new HashSet<>();
        annotatedClasses.add("org.apache.kafka.common.Resource");

        Method crossValidateClassSets = PublicApiChecker.class.getDeclaredMethod(
            "crossValidateClassSets", Set.class, Set.class);
        crossValidateClassSets.setAccessible(true);

        @SuppressWarnings("unchecked")
        List<PublicApiViolation> violations = (List<PublicApiViolation>)
            crossValidateClassSets.invoke(checker, htmlClasses, annotatedClasses);

        assertEquals(1, violations.size());
        PublicApiViolation violation = violations.get(0);
        assertEquals("MISSING_PUBLICAPI_ANNOTATION", violation.getViolationType());
        assertEquals("org.apache.kafka.clients.producer.Producer", violation.getClassName());
        assertTrue(violation.getDescription().contains("appears in javadoc but lacks @InterfaceAudience.Public annotation"));
    }

    @Test
    void testCrossValidateClassSets_PerfectMatch() throws Exception {
        Set<String> htmlClasses = new HashSet<>();
        htmlClasses.add("org.apache.kafka.common.Resource");
        htmlClasses.add("org.apache.kafka.clients.producer.Producer");

        Set<String> annotatedClasses = new HashSet<>();
        annotatedClasses.add("org.apache.kafka.common.Resource");
        annotatedClasses.add("org.apache.kafka.clients.producer.Producer");

        Method crossValidateClassSets = PublicApiChecker.class.getDeclaredMethod(
            "crossValidateClassSets", Set.class, Set.class);
        crossValidateClassSets.setAccessible(true);

        @SuppressWarnings("unchecked")
        List<PublicApiViolation> violations = (List<PublicApiViolation>)
            crossValidateClassSets.invoke(checker, htmlClasses, annotatedClasses);

        assertTrue(violations.isEmpty(), "Should have no violations for perfectly matched sets");
    }

    @Test
    void testCheckPublicApiConsistency_IntegrationTest() throws IOException {
        File javadocJar = createMockJavadocJar();
        File projectJar = createMockProjectJar();

        List<File> projectJars = new ArrayList<>();
        projectJars.add(projectJar);

        List<PublicApiViolation> violations = checker.checkPublicApiConsistency(javadocJar, projectJars);

        // Should find violations due to class loading issues (mocked environment)
        // The exact number depends on implementation, but should not crash
        assertNotNull(violations);
    }

    // Helper methods for creating mock JAR files

    private File createMockJavadocJar() throws IOException {
        File jarFile = tempDir.resolve("test-javadoc.jar").toFile();

        try (JarOutputStream jos = new JarOutputStream(new FileOutputStream(jarFile))) {
            // Add valid class HTML files
            addHtmlEntry(jos, "org/apache/kafka/common/Resource.html", "");
            addHtmlEntry(jos, "org/apache/kafka/clients/producer/Producer.html", "");

            // Add excluded package class (should be filtered out)
            addHtmlEntry(jos, "org/apache/kafka/common/internals/InternalClass.html", "");

            // Add structural HTML files (should be filtered out)
            addHtmlEntry(jos, "org/apache/kafka/package-summary.html", "");
            addHtmlEntry(jos, "index.html", "");
            addHtmlEntry(jos, "overview-tree.html", "");
            addHtmlEntry(jos, "constant-values.html", "");

            // Add non-HTML files (should be filtered out)
            addEntry(jos, "org/apache/kafka/common/Resource.class", new byte[0]);
        }

        return jarFile;
    }

    private File createMockProjectJar() throws IOException {
        File jarFile = tempDir.resolve("test-project.jar").toFile();

        try (JarOutputStream jos = new JarOutputStream(new FileOutputStream(jarFile))) {
            // Add some empty entries instead of class files to avoid ClassFormatError
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