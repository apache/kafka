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
package org.apache.kafka.apicheck;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.objectweb.asm.Opcodes;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JavadocConsistencyValidatorTest {

    private static final File DUMMY_JAR = new File("ignored");

    @TempDir
    Path tempDir;

    @Test
    void directPublicMissingFromJavadoc_emitsMissingJavadoc() throws IOException {
        ClassFacts bar = factsPublic("org.apache.kafka.foo.Bar");
        ApiSurface surface = ApiSurface.builder()
                .recordClass(bar, DUMMY_JAR)
                .addDirectPublic(bar)
                .addEffectivePublic(bar)
                .build();
        File javadocJar = TempJarBuilder.jar().writeTo(tempDir, "javadoc.jar");

        CheckResult result = JavadocConsistencyValidator.validate(javadocJar, surface);

        assertEquals(1, result.violations().size());
        PublicApiViolation v = result.violations().get(0);
        assertEquals("MISSING_JAVADOC", v.getViolationType());
        assertEquals("org.apache.kafka.foo.Bar", v.getClassName());
        assertTrue(result.suppressions().isEmpty());
    }

    @Test
    void htmlClassNotEffectivelyPublic_emitsMissingAnnotation() throws IOException {
        // HTML claims a class is documented; the surface has nothing on it.
        ApiSurface surface = ApiSurface.builder().build();
        File javadocJar = TempJarBuilder.jar()
                .addHtml("org/apache/kafka/foo/Sneaky.html", "")
                .writeTo(tempDir, "javadoc.jar");

        CheckResult result = JavadocConsistencyValidator.validate(javadocJar, surface);

        assertEquals(1, result.violations().size());
        PublicApiViolation v = result.violations().get(0);
        assertEquals("MISSING_PUBLICAPI_ANNOTATION", v.getViolationType());
        assertEquals("org.apache.kafka.foo.Sneaky", v.getClassName());
    }

    @Test
    void effectivelyPublicViaInheritance_isNotFlagged() throws IOException {
        // Inner has no direct @Public but inherits it from Outer. Real javadoc emits nested-class
        // pages using the dotted form (Outer.Inner.html), which the validator must recognise.
        ClassFacts outer = factsPublic("org.apache.kafka.foo.Outer");
        ClassFacts inner = factsPlain("org.apache.kafka.foo.Outer$Inner");
        ApiSurface surface = ApiSurface.builder()
                .recordClass(outer, DUMMY_JAR).recordClass(inner, DUMMY_JAR)
                .addDirectPublic(outer)
                .addEffectivePublic(outer).addEffectivePublic(inner)
                .build();
        File javadocJar = TempJarBuilder.jar()
                .addHtml("org/apache/kafka/foo/Outer.html", "")
                .addHtml("org/apache/kafka/foo/Outer.Inner.html", "")
                .writeTo(tempDir, "javadoc.jar");

        CheckResult result = JavadocConsistencyValidator.validate(javadocJar, surface);

        assertTrue(result.violations().isEmpty(),
                "perfect match (direct + inherited) should yield no violations: " + result.violations());
    }

    @Test
    void deprecatedClassInJavadoc_isFilteredOut() throws IOException {
        // OldClass is deprecated and not in directPublic — would normally trip MISSING_PUBLICAPI_ANNOTATION,
        // but isDeprecated removes it from the HTML set first.
        ClassFacts deprecated = factsBuilder("org.apache.kafka.foo.OldClass")
                .addFlag(ClassFacts.Flag.DEPRECATED)
                .build();
        ApiSurface surface = ApiSurface.builder().recordClass(deprecated, DUMMY_JAR).build();
        File javadocJar = TempJarBuilder.jar()
                .addHtml("org/apache/kafka/foo/OldClass.html", "")
                .writeTo(tempDir, "javadoc.jar");

        CheckResult result = JavadocConsistencyValidator.validate(javadocJar, surface);
        assertTrue(result.violations().isEmpty(),
                "deprecated classes are out of scope on both validation sides");
    }

    @Test
    void perfectMatch_noViolations() throws IOException {
        ClassFacts bar = factsPublic("org.apache.kafka.foo.Bar");
        ApiSurface surface = ApiSurface.builder()
                .recordClass(bar, DUMMY_JAR)
                .addDirectPublic(bar)
                .addEffectivePublic(bar)
                .build();
        File javadocJar = TempJarBuilder.jar()
                .addHtml("org/apache/kafka/foo/Bar.html", "")
                .writeTo(tempDir, "javadoc.jar");

        CheckResult result = JavadocConsistencyValidator.validate(javadocJar, surface);
        assertTrue(result.violations().isEmpty());
        assertTrue(result.suppressions().isEmpty());
    }

    @Test
    void structuralHtmlPages_areIgnored() throws IOException {
        // Javadoc emits many non-class HTML files alongside class pages. The validator must skip
        // them; otherwise every javadoc jar trips bogus MISSING_PUBLICAPI_ANNOTATION violations.
        ApiSurface surface = ApiSurface.builder().build();
        File javadocJar = TempJarBuilder.jar()
                .addHtml("org/apache/kafka/foo/package-summary.html", "")
                .addHtml("org/apache/kafka/foo/overview-tree.html", "")
                .addHtml("org/apache/kafka/foo/index-all.html", "")
                .addHtml("index.html", "")
                .writeTo(tempDir, "javadoc.jar");

        CheckResult result = JavadocConsistencyValidator.validate(javadocJar, surface);
        assertTrue(result.violations().isEmpty(),
                "structural HTML must not be misread as class pages: " + result.violations());
    }

    @Test
    void nonKafkaHtml_isIgnored() throws IOException {
        ApiSurface surface = ApiSurface.builder().build();
        File javadocJar = TempJarBuilder.jar()
                .addHtml("com/example/External.html", "")
                .addHtml("java/util/HashMap.html", "")
                .writeTo(tempDir, "javadoc.jar");

        CheckResult result = JavadocConsistencyValidator.validate(javadocJar, surface);
        assertTrue(result.violations().isEmpty());
    }

    @Test
    void emptyJavadocJar_flagsAllDirectPublicClasses() throws IOException {
        ClassFacts a = factsPublic("org.apache.kafka.foo.A");
        ClassFacts b = factsPublic("org.apache.kafka.foo.B");
        ApiSurface surface = ApiSurface.builder()
                .recordClass(a, DUMMY_JAR).recordClass(b, DUMMY_JAR)
                .addDirectPublic(a).addDirectPublic(b)
                .addEffectivePublic(a).addEffectivePublic(b)
                .build();
        File javadocJar = TempJarBuilder.jar().writeTo(tempDir, "javadoc.jar");

        CheckResult result = JavadocConsistencyValidator.validate(javadocJar, surface);

        assertEquals(2, result.violations().size());
        assertTrue(result.violations().stream()
                .allMatch(v -> "MISSING_JAVADOC".equals(v.getViolationType())));
    }

    @Test
    void suppressionsListIsAlwaysEmpty_evenWithViolations() throws IOException {
        // JavadocConsistencyValidator doesn't carry a suppression mechanism — the CheckResult
        // always has an empty suppressions list. Callers compose validators uniformly, so the
        // shape matters even when no suppressions are possible.
        ClassFacts bar = factsPublic("org.apache.kafka.foo.Bar");
        ApiSurface surface = ApiSurface.builder()
                .recordClass(bar, DUMMY_JAR)
                .addDirectPublic(bar)
                .build();
        File javadocJar = TempJarBuilder.jar().writeTo(tempDir, "javadoc.jar");

        CheckResult result = JavadocConsistencyValidator.validate(javadocJar, surface);
        assertFalse(result.violations().isEmpty());
        assertNotNull(result.suppressions());
        assertTrue(result.suppressions().isEmpty());
    }

    private static ClassFacts factsPublic(String binaryName) {
        return factsBuilder(binaryName).addFlag(ClassFacts.Flag.PUBLIC_API).build();
    }

    private static ClassFacts factsPlain(String binaryName) {
        return factsBuilder(binaryName).build();
    }

    private static ClassFacts.Builder factsBuilder(String binaryName) {
        return ClassFacts.builder(binaryName).sourceAccess(Opcodes.ACC_PUBLIC);
    }
}
