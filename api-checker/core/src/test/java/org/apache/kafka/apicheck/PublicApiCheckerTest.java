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
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for the {@link PublicApiChecker} facade: surface construction, {@code isPublicApi}
 * predicate semantics, and merged results from {@link JavadocConsistencyValidator} +
 * {@link CascadeValidator} + {@link PluginDeveloperApiUsageScanner}.
 */
class PublicApiCheckerTest {

    @TempDir
    Path tempDir;

    // ----- isPublicApi -----

    @Test
    void isPublicApi_nonKafkaClass_returnsTrueOutOfScope() throws IOException {
        PublicApiChecker checker = checkerFor();
        assertTrue(checker.isPublicApi("java.util.Map"));
        assertTrue(checker.isPublicApi("com.example.Foo"));
    }

    @Test
    void isPublicApi_directPublic_returnsTrue() throws IOException {
        PublicApiChecker checker = checkerFor(
                AsmClassFactory.klass("org.apache.kafka.api.Pub").access(Opcodes.ACC_PUBLIC).publicApi());
        assertTrue(checker.isPublicApi("org.apache.kafka.api.Pub"));
    }

    @Test
    void isPublicApi_directPrivate_returnsFalse() throws IOException {
        PublicApiChecker checker = checkerFor(
                AsmClassFactory.klass("org.apache.kafka.api.Hidden").access(Opcodes.ACC_PUBLIC).privateApi());
        assertFalse(checker.isPublicApi("org.apache.kafka.api.Hidden"));
    }

    @Test
    void isPublicApi_deprecatedInternal_isStillFlagged() throws IOException {
        // Consumer side: @Deprecated does NOT make an internal class out-of-scope.
        // Deprecated-internal types are the most likely to be removed in the next release, so
        // a consumer reference to one is exactly the kind of break the checker exists to catch.
        // (The cascade validator still has its own deprecation bypass for the producer-side
        // leak check, which is a separate policy.)
        PublicApiChecker checker = checkerFor(
                AsmClassFactory.klass("org.apache.kafka.api.Old")
                        .access(Opcodes.ACC_PUBLIC).privateApi().deprecated());
        assertFalse(checker.isPublicApi("org.apache.kafka.api.Old"),
                "consumer predicate must flag @Deprecated @Private — those are the highest-risk references");
    }

    @Test
    void isPublicApi_nestedInheritsFromOuter() throws IOException {
        PublicApiChecker checker = checkerFor(
                AsmClassFactory.klass("org.apache.kafka.api.Outer").access(Opcodes.ACC_PUBLIC).publicApi(),
                AsmClassFactory.klass("org.apache.kafka.api.Outer$Inner").access(Opcodes.ACC_PUBLIC));
        assertTrue(checker.isPublicApi("org.apache.kafka.api.Outer$Inner"));
    }

    @Test
    void isPublicApi_nestedPrivateOverridesOuterPublic() throws IOException {
        PublicApiChecker checker = checkerFor(
                AsmClassFactory.klass("org.apache.kafka.api.Outer").access(Opcodes.ACC_PUBLIC).publicApi(),
                AsmClassFactory.klass("org.apache.kafka.api.Outer$Inner").access(Opcodes.ACC_PUBLIC).privateApi());
        assertFalse(checker.isPublicApi("org.apache.kafka.api.Outer$Inner"));
    }

    @Test
    void isPublicApi_unknownKafkaClass_returnsFalse() throws IOException {
        PublicApiChecker checker = checkerFor();
        assertFalse(checker.isPublicApi("org.apache.kafka.UnknownClass"));
    }

    @Test
    void isPublicApi_nestedWithoutOuterFacts_returnsFalse() throws IOException {
        // Inner is recorded but Outer isn't. The chain walk finds Inner (no annotation), looks up
        // Outer, gets null, exits with false — inheritance requires the outer to be in scope.
        PublicApiChecker checker = checkerFor(
                AsmClassFactory.klass("org.apache.kafka.api.Outer$Inner").access(Opcodes.ACC_PUBLIC));
        assertFalse(checker.isPublicApi("org.apache.kafka.api.Outer$Inner"));
    }

    // ----- checkPublicApiConsistency -----

    @Test
    void checkPublicApiConsistency_mergesJavadocAndCascadeViolations() throws IOException {
        // Bar is @Public with a method that leaks an internal type; javadoc jar is empty.
        // Expected: MISSING_JAVADOC from the javadoc validator + INVALID_RETURN_TYPE from cascade.
        AsmClassFactory.ClassBuilder bar = AsmClassFactory.klass("org.apache.kafka.api.Bar")
                .access(Opcodes.ACC_PUBLIC).publicApi()
                .method(AsmClassFactory.method("leak").returns("Lorg/apache/kafka/internals/Internal;"));
        File projectJar = TempJarBuilder.jar().addClass(bar).writeTo(tempDir, "proj.jar");
        File javadocJar = TempJarBuilder.jar().writeTo(tempDir, "javadoc.jar");

        CheckResult result = new PublicApiChecker(List.of(projectJar)).checkPublicApiConsistency(javadocJar);

        assertTrue(result.violations().stream().anyMatch(v -> "MISSING_JAVADOC".equals(v.getViolationType())),
                "expected MISSING_JAVADOC from javadoc validator: " + result.violations());
        assertTrue(result.violations().stream().anyMatch(v -> "INVALID_RETURN_TYPE".equals(v.getViolationType())),
                "expected INVALID_RETURN_TYPE from cascade validator: " + result.violations());
    }

    @Test
    void checkPublicApiConsistency_mergesSuppressionsFromCascade() throws IOException {
        // Class-level @SuppressKafkaInternalApiUsage routes the cascade leak into suppressions
        // instead of violations. Javadoc HTML present → no MISSING_JAVADOC.
        AsmClassFactory.ClassBuilder bar = AsmClassFactory.klass("org.apache.kafka.api.Bar")
                .access(Opcodes.ACC_PUBLIC).publicApi().suppress("legacy")
                .method(AsmClassFactory.method("leak").returns("Lorg/apache/kafka/internals/Internal;"));
        File projectJar = TempJarBuilder.jar().addClass(bar).writeTo(tempDir, "proj.jar");
        File javadocJar = TempJarBuilder.jar()
                .addHtml("org/apache/kafka/api/Bar.html", "")
                .writeTo(tempDir, "javadoc.jar");

        CheckResult result = new PublicApiChecker(List.of(projectJar)).checkPublicApiConsistency(javadocJar);

        assertTrue(result.violations().isEmpty(), "everything suppressed: " + result.violations());
        assertEquals(1, result.suppressions().size());
        assertTrue(result.suppressions().get(0).getDescription().contains("reason: legacy"),
                "suppression must carry the annotation reason: " + result.suppressions().get(0).getDescription());
    }

    // ----- checkBytecode -----

    @Test
    void checkBytecode_flagsConsumerReferenceToInternalClass() throws IOException {
        // Project surface defines Hidden as an in-scope Kafka class with no @Public annotation.
        // The consumer references it → checkBytecode should report INTERNAL_API_USAGE.
        File projectJar = TempJarBuilder.jar()
                .addClass(AsmClassFactory.klass("org.apache.kafka.internals.Hidden").access(Opcodes.ACC_PUBLIC))
                .writeTo(tempDir, "proj.jar");
        File consumerJar = TempJarBuilder.jar()
                .addClass(AsmClassFactory.klass("com.example.Consumer")
                        .access(Opcodes.ACC_PUBLIC)
                        .method(AsmClassFactory.method("hold").returns("Lorg/apache/kafka/internals/Hidden;")))
                .writeTo(tempDir, "consumer.jar");

        CheckResult result = new PublicApiChecker(List.of(projectJar)).checkBytecode(List.of(consumerJar));

        assertFalse(result.violations().isEmpty(),
                "unannotated Kafka class reference must be flagged: " + result.violations());
        assertEquals("INTERNAL_API_USAGE", result.violations().get(0).getViolationType());
        // className tracks the consumer (offender), so `Summary by Class` in the report groups
        // by the file a developer has to open. The leaked target lives in the description.
        assertEquals("com.example.Consumer", result.violations().get(0).getClassName());
        assertTrue(result.violations().get(0).getDescription().contains("org.apache.kafka.internals.Hidden"),
                "description must carry the leaked internal type: " + result.violations().get(0).getDescription());
    }

    @Test
    void checkBytecode_passesConsumerReferenceToPublicClass() throws IOException {
        File projectJar = TempJarBuilder.jar()
                .addClass(AsmClassFactory.klass("org.apache.kafka.api.Pub").access(Opcodes.ACC_PUBLIC).publicApi())
                .writeTo(tempDir, "proj.jar");
        File consumerJar = TempJarBuilder.jar()
                .addClass(AsmClassFactory.klass("com.example.Consumer")
                        .access(Opcodes.ACC_PUBLIC)
                        .method(AsmClassFactory.method("use").returns("Lorg/apache/kafka/api/Pub;")))
                .writeTo(tempDir, "consumer.jar");

        CheckResult result = new PublicApiChecker(List.of(projectJar)).checkBytecode(List.of(consumerJar));

        assertTrue(result.violations().isEmpty(),
                "reference to a @Public class must pass: " + result.violations());
    }

    // ----- constructor overload -----

    @Test
    void singleArgConstructor_equivalentToEmptyReferenceJars() throws IOException {
        File projectJar = TempJarBuilder.jar()
                .addClass(AsmClassFactory.klass("org.apache.kafka.api.Bar").access(Opcodes.ACC_PUBLIC).publicApi())
                .writeTo(tempDir, "proj.jar");

        PublicApiChecker single = new PublicApiChecker(List.of(projectJar));
        PublicApiChecker dual   = new PublicApiChecker(List.of(projectJar), Collections.emptyList());

        assertTrue(single.isPublicApi("org.apache.kafka.api.Bar"));
        assertTrue(dual.isPublicApi("org.apache.kafka.api.Bar"));
        assertFalse(single.isPublicApi("org.apache.kafka.UnknownClass"));
        assertFalse(dual.isPublicApi("org.apache.kafka.UnknownClass"));
    }

    // ----- helper -----

    private PublicApiChecker checkerFor(AsmClassFactory.ClassBuilder... classes) throws IOException {
        if (classes.length == 0) return new PublicApiChecker(List.of());
        TempJarBuilder jar = TempJarBuilder.jar();
        for (AsmClassFactory.ClassBuilder c : classes) jar.addClass(c);
        return new PublicApiChecker(List.of(jar.writeTo(tempDir, "proj.jar")));
    }
}
