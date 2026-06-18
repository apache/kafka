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
package org.apache.kafka.publicapi;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.objectweb.asm.Opcodes;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CascadeValidatorTest {

    private static final String OWNER_BIN = "org.apache.kafka.api.Owner";
    private static final String INTERNAL_BIN = "org.apache.kafka.internals.Internal";
    private static final String INTERNAL_DESC = "Lorg/apache/kafka/internals/Internal;";
    private static final String INTERNAL_INTERNAL_NAME = "org/apache/kafka/internals/Internal";

    @TempDir
    Path tempDir;

    @Test
    void publicMethodWithInternalReturnType_emitsInvalidReturnType() throws IOException {
        CheckResult r = run(owner()
                .method(AsmClassFactory.method("leak").returns(INTERNAL_DESC)));

        assertEquals(1, r.violations().size());
        PublicApiViolation v = r.violations().get(0);
        assertEquals("INVALID_RETURN_TYPE", v.getViolationType());
        assertEquals(OWNER_BIN, v.getClassName());
        assertEquals("leak", v.getMemberName());
        assertTrue(v.getDescription().contains(INTERNAL_BIN),
                "description should name the leaked type: " + v.getDescription());
    }

    @Test
    void publicMethodWithInternalParameter_emitsInvalidParameterType() throws IOException {
        CheckResult r = run(owner()
                .method(AsmClassFactory.method("take").param(INTERNAL_DESC)));

        assertEquals(1, r.violations().size());
        assertEquals("INVALID_PARAMETER_TYPE", r.violations().get(0).getViolationType());
    }

    @Test
    void publicMethodWithInternalException_emitsInvalidExceptionType() throws IOException {
        CheckResult r = run(owner()
                .method(AsmClassFactory.method("boom").throwsExc(INTERNAL_INTERNAL_NAME)));

        assertEquals(1, r.violations().size());
        assertEquals("INVALID_EXCEPTION_TYPE", r.violations().get(0).getViolationType());
    }

    @Test
    void arrayOfInternalType_recursesAndFlags() throws IOException {
        // Array descriptors prepend "[" to the element descriptor. The validator must recurse
        // through the array layer to reach the object element type.
        CheckResult r = run(owner()
                .method(AsmClassFactory.method("batch").returns("[" + INTERNAL_DESC)));

        assertEquals(1, r.violations().size());
        assertEquals("INVALID_RETURN_TYPE", r.violations().get(0).getViolationType());
    }

    @Test
    void deprecatedInternalType_isNotFlagged() throws IOException {
        // Internal is recorded with @Deprecated → out of scope on both sides.
        CheckResult r = runWithExtras(owner()
                        .method(AsmClassFactory.method("legacy").returns(INTERNAL_DESC)),
                facts(INTERNAL_BIN, ClassFacts.Flag.DEPRECATED));

        assertTrue(r.violations().isEmpty(),
                "deprecated referenced type must not trigger: " + r.violations());
    }

    @Test
    void referenceToEffectivelyPublicType_isNotFlagged() throws IOException {
        // Internal is in the membership set → counts as part of the public API surface.
        CheckResult r = runWithEffectivelyPublic(owner()
                        .method(AsmClassFactory.method("ok").returns(INTERNAL_DESC)),
                INTERNAL_BIN);

        assertTrue(r.violations().isEmpty());
    }

    @Test
    void referenceToNonKafkaType_isNotFlagged() throws IOException {
        // JDK types (java/util/Map) and third-party types are out of scope — the cascade rule
        // only constrains references inside org.apache.kafka.*.
        CheckResult r = run(owner()
                .method(AsmClassFactory.method("safe")
                        .returns("Ljava/util/Map;")
                        .param("Lcom/example/External;")));

        assertTrue(r.violations().isEmpty());
    }

    @Test
    void nonPublicMethod_isIgnored() throws IOException {
        // Cascade only checks ACC_PUBLIC methods. Private and protected leaks are tolerated
        // (per current policy — only the broadest source-level access is in scope).
        CheckResult r = run(owner()
                .method(AsmClassFactory.method("priv").access(Opcodes.ACC_PRIVATE).returns(INTERNAL_DESC))
                .method(AsmClassFactory.method("prot").access(Opcodes.ACC_PROTECTED).returns(INTERNAL_DESC)));

        assertTrue(r.violations().isEmpty());
    }

    @Test
    void syntheticMethod_isIgnored() throws IOException {
        // Bridge / ACC_SYNTHETIC methods are compiler-generated, not source-level API.
        CheckResult r = run(owner()
                .method(AsmClassFactory.method("bridge").bridge().returns(INTERNAL_DESC))
                .method(AsmClassFactory.method("synth").synthetic().returns(INTERNAL_DESC)));

        assertTrue(r.violations().isEmpty());
    }

    @Test
    void classLevelSuppress_movesAllViolationsToSuppressions() throws IOException {
        CheckResult r = run(owner()
                .suppress("legacy-api")
                .method(AsmClassFactory.method("leak").returns(INTERNAL_DESC)));

        assertTrue(r.violations().isEmpty(), "class-level suppress should silence every method");
        assertEquals(1, r.suppressions().size());
        PublicApiViolation s = r.suppressions().get(0);
        assertEquals("SUPPRESSED", s.getViolationType());
        assertTrue(s.getDescription().contains("reason: legacy-api"),
                "suppression must carry the annotation's reason: " + s.getDescription());
    }

    @Test
    void methodLevelSuppress_overridesClassLevelReason() throws IOException {
        // Class-level "class-reason" applies to methods without their own annotation;
        // a method-level annotation wins for that method.
        CheckResult r = run(owner()
                .suppress("class-reason")
                .method(AsmClassFactory.method("m1").returns(INTERNAL_DESC).suppress("method-reason"))
                .method(AsmClassFactory.method("m2").returns(INTERNAL_DESC)));

        assertTrue(r.violations().isEmpty());
        assertEquals(2, r.suppressions().size());
        assertTrue(r.suppressions().stream().anyMatch(v ->
                v.getMemberName().equals("m1") && v.getDescription().contains("reason: method-reason")));
        assertTrue(r.suppressions().stream().anyMatch(v ->
                v.getMemberName().equals("m2") && v.getDescription().contains("reason: class-reason")));
    }

    @Test
    void suppressWithNoValue_recordsNoReasonGiven() throws IOException {
        // @SuppressKafkaInternalApiUsage on its own (no value()) → ReasonCaptureVisitor records
        // an empty reason, which the reporter renders as "(no reason given)".
        CheckResult r = run(owner()
                .method(AsmClassFactory.method("leak").returns(INTERNAL_DESC).suppress(null)));

        assertTrue(r.violations().isEmpty());
        assertEquals(1, r.suppressions().size());
        assertTrue(r.suppressions().get(0).getDescription().contains("reason: (no reason given)"),
                "empty reason must render as '(no reason given)': "
                        + r.suppressions().get(0).getDescription());
    }

    @Test
    void jarOfReturnsNull_classIsSilentlySkipped() throws IOException {
        // Class is in the cascade iteration set but no jar is recorded for it. The validator
        // bails on the missing jar without throwing — defensive against scan/cascade desync.
        ClassFacts orphan = facts("org.apache.kafka.api.Orphan", ClassFacts.Flag.PUBLIC_API);
        ApiSurface surface = ApiSurface.builder()
                .addEffectivePublic(orphan)
                .addEffectivePublic(orphan)
                .build();

        CheckResult r = CascadeValidator.validate(surface);
        assertTrue(r.violations().isEmpty());
        assertTrue(r.suppressions().isEmpty());
    }

    // ----- helpers -----

    /** Owner class scaffolding shared by every cascade test: top-level public, audience @Public. */
    private static AsmClassFactory.ClassBuilder owner() {
        return AsmClassFactory.klass(OWNER_BIN).access(Opcodes.ACC_PUBLIC).publicApi();
    }

    private CheckResult run(AsmClassFactory.ClassBuilder owner) throws IOException {
        return runWithExtras(owner);
    }

    /** Validate against a surface that registers {@code extras} in addition to the owner class. */
    private CheckResult runWithExtras(AsmClassFactory.ClassBuilder owner, ClassFacts... extras) throws IOException {
        File jar = TempJarBuilder.jar().addClass(owner).writeTo(tempDir, "x.jar");
        ClassFacts ownerFacts = facts(owner.binaryName(), ClassFacts.Flag.PUBLIC_API);
        ApiSurface.Builder b = ApiSurface.builder()
                .recordClass(ownerFacts, jar)
                .addEffectivePublic(ownerFacts)
                .addEffectivePublic(ownerFacts);
        for (ClassFacts f : extras) b.recordClass(f, jar);
        return CascadeValidator.validate(b.build());
    }

    /** Validate against a surface where the named extras are also marked effectively public. */
    private CheckResult runWithEffectivelyPublic(AsmClassFactory.ClassBuilder owner,
                                                 String... effectivelyPublicBinaryNames) throws IOException {
        File jar = TempJarBuilder.jar().addClass(owner).writeTo(tempDir, "x.jar");
        ClassFacts ownerFacts = facts(owner.binaryName(), ClassFacts.Flag.PUBLIC_API);
        ApiSurface.Builder b = ApiSurface.builder()
                .recordClass(ownerFacts, jar)
                .addEffectivePublic(ownerFacts)
                .addEffectivePublic(ownerFacts);
        for (String name : effectivelyPublicBinaryNames) {
            ClassFacts f = facts(name, ClassFacts.Flag.PUBLIC_API);
            b.recordClass(f, jar).addEffectivePublic(f);
        }
        return CascadeValidator.validate(b.build());
    }

    private static ClassFacts facts(String binaryName, ClassFacts.Flag... flags) {
        ClassFacts.Builder b = ClassFacts.builder(binaryName).sourceAccess(Opcodes.ACC_PUBLIC);
        for (ClassFacts.Flag f : flags) b.addFlag(f);
        return b.build();
    }
}
