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
    void publicMethodWithInternalInGenericReturnSignature_emitsReturnTypeViolation() throws IOException {
        // The erased descriptor is `()Ljava/util/Map;` — no internal type. The generic signature
        // `()Ljava/util/Map<Ljava/lang/String;Lorg/apache/kafka/internals/Internal;>;` carries
        // the internal type as a Map value parameter on the *return* position. The signature
        // walker is position-aware so it surfaces this as INVALID_RETURN_TYPE rather than
        // mislabeling it as a parameter type.
        CheckResult r = run(owner()
                .method(AsmClassFactory.method("getStuff")
                        .returns("Ljava/util/Map;")
                        .signature("()Ljava/util/Map<Ljava/lang/String;Lorg/apache/kafka/internals/Internal;>;")));

        assertTrue(r.violations().stream().anyMatch(v -> "INVALID_RETURN_TYPE".equals(v.getViolationType())
                        && v.getDescription().contains(INTERNAL_BIN)),
                "generic return-type argument should surface as INVALID_RETURN_TYPE; got: " + r.violations());
    }

    @Test
    void publicMethodWithInternalInGenericParameterSignature_emitsParameterTypeViolation() throws IOException {
        // Generic on the parameter side: `void take(java.util.List<o.a.k.internals.Internal>)`.
        CheckResult r = run(owner()
                .method(AsmClassFactory.method("take")
                        .returns("V")
                        .param("Ljava/util/List;")
                        .signature("(Ljava/util/List<Lorg/apache/kafka/internals/Internal;>;)V")));

        assertTrue(r.violations().stream().anyMatch(v -> "INVALID_PARAMETER_TYPE".equals(v.getViolationType())
                        && v.getDescription().contains(INTERNAL_BIN)),
                "generic parameter-type argument should surface as INVALID_PARAMETER_TYPE; got: " + r.violations());
    }

    @Test
    void publicFieldWithInternalInGenericSignature_emitsFieldTypeViolation() throws IOException {
        // The erased descriptor is `Ljava/util/List;`. The signature
        // `Ljava/util/List<Lorg/apache/kafka/internals/Internal;>;` exposes Internal as a
        // List element parameter. Cascade must walk the signature.
        CheckResult r = run(owner()
                .field(AsmClassFactory.field("things")
                        .ofType("Ljava/util/List;")
                        .signature("Ljava/util/List<Lorg/apache/kafka/internals/Internal;>;")));

        assertTrue(r.violations().stream().anyMatch(v -> "INVALID_FIELD_TYPE".equals(v.getViolationType())
                        && v.getDescription().contains(INTERNAL_BIN)),
                "generic field signature should surface as an INVALID_FIELD_TYPE; got: " + r.violations());
    }

    @Test
    void publicClassExtendingInternalType_emitsSupertypeViolation() throws IOException {
        // A @Public class whose superclass is an internal Kafka type exposes the supertype
        // to consumers (it's part of the type's public contract — inherited methods, casts,
        // etc.). The cascade catches this from the class-header visit.
        CheckResult r = run(owner().superClass("org/apache/kafka/internals/Internal"));

        assertTrue(r.violations().stream().anyMatch(v -> "INVALID_SUPERTYPE".equals(v.getViolationType())
                        && v.getDescription().contains(INTERNAL_BIN)),
                "extending an internal type must trigger an INVALID_SUPERTYPE; got: " + r.violations());
    }

    @Test
    void publicClassImplementingInternalInterface_emitsSupertypeViolation() throws IOException {
        CheckResult r = run(owner().interfaces("org/apache/kafka/internals/Internal"));

        assertTrue(r.violations().stream().anyMatch(v -> "INVALID_SUPERTYPE".equals(v.getViolationType())
                        && v.getDescription().contains(INTERNAL_BIN)),
                "implementing an internal interface must trigger an INVALID_SUPERTYPE; got: " + r.violations());
    }

    @Test
    void classLevelSuppress_silencesSupertypeViolation() throws IOException {
        // Class-level @SuppressKafkaInternalApiUsage diverts the header leak to the
        // suppressions list, same as for method-level cascade leaks.
        CheckResult r = run(owner()
                .suppress("legacy-base-class")
                .superClass("org/apache/kafka/internals/Internal"));

        assertTrue(r.violations().isEmpty(), "class-level suppress should silence the header leak");
        assertTrue(r.suppressions().stream().anyMatch(s -> s.getDescription().contains("reason: legacy-base-class")),
                "suppression must carry the annotation reason; got: " + r.suppressions());
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
    void privateMethod_isIgnored() throws IOException {
        // Cascade only inspects externally-visible methods. Private leaks are tolerated
        // because they're invisible to consumers.
        CheckResult r = run(owner()
                .method(AsmClassFactory.method("priv").access(Opcodes.ACC_PRIVATE).returns(INTERNAL_DESC)));

        assertTrue(r.violations().isEmpty());
    }

    @Test
    void protectedMethod_emitsViolation() throws IOException {
        // KIP-1265: protected members on an extensible @Public class are reachable to
        // subclasses, so they count toward the public API surface and must not leak
        // non-public types.
        CheckResult r = run(owner()
                .method(AsmClassFactory.method("prot").access(Opcodes.ACC_PROTECTED).returns(INTERNAL_DESC)));

        assertEquals(1, r.violations().size());
        assertEquals("INVALID_RETURN_TYPE", r.violations().get(0).getViolationType());
    }

    @Test
    void publicFieldOfInternalType_emitsInvalidFieldType() throws IOException {
        // KIP-1265: field types are part of the cascade — a public field of an internal type
        // leaks the internal type just like a method signature does.
        CheckResult r = run(owner()
                .field(AsmClassFactory.field("leakField").ofType(INTERNAL_DESC)));

        assertEquals(1, r.violations().size());
        PublicApiViolation v = r.violations().get(0);
        assertEquals("INVALID_FIELD_TYPE", v.getViolationType());
        assertEquals(OWNER_BIN, v.getClassName());
        assertEquals("leakField", v.getMemberName());
        assertTrue(v.getDescription().contains(INTERNAL_BIN),
                "description should name the leaked type: " + v.getDescription());
    }

    @Test
    void protectedFieldOfInternalType_emitsInvalidFieldType() throws IOException {
        // protected fields on an extensible @Public class are also part of the API surface.
        CheckResult r = run(owner()
                .field(AsmClassFactory.field("protLeak").access(Opcodes.ACC_PROTECTED).ofType(INTERNAL_DESC)));

        assertEquals(1, r.violations().size());
        assertEquals("INVALID_FIELD_TYPE", r.violations().get(0).getViolationType());
    }

    @Test
    void privateFieldOfInternalType_isIgnored() throws IOException {
        CheckResult r = run(owner()
                .field(AsmClassFactory.field("hidden").access(Opcodes.ACC_PRIVATE).ofType(INTERNAL_DESC)));

        assertTrue(r.violations().isEmpty());
    }

    @Test
    void arrayFieldOfInternalType_recursesAndFlags() throws IOException {
        CheckResult r = run(owner()
                .field(AsmClassFactory.field("buf").ofType("[" + INTERNAL_DESC)));

        assertEquals(1, r.violations().size());
        assertEquals("INVALID_FIELD_TYPE", r.violations().get(0).getViolationType());
    }

    @Test
    void fieldLevelSuppress_movesFieldViolationToSuppressions() throws IOException {
        CheckResult r = run(owner()
                .field(AsmClassFactory.field("leak").ofType(INTERNAL_DESC).suppress("legacy-field")));

        assertTrue(r.violations().isEmpty());
        assertEquals(1, r.suppressions().size());
        assertTrue(r.suppressions().get(0).getDescription().contains("reason: legacy-field"));
    }

    @Test
    void classLevelSuppress_silencesFieldLeaks() throws IOException {
        CheckResult r = run(owner()
                .suppress("legacy-api")
                .field(AsmClassFactory.field("leak").ofType(INTERNAL_DESC)));

        assertTrue(r.violations().isEmpty());
        assertEquals(1, r.suppressions().size());
        assertTrue(r.suppressions().get(0).getDescription().contains("reason: legacy-api"));
    }

    @Test
    void syntheticField_isIgnored() throws IOException {
        // Compiler-generated synthetic fields (e.g. $assertionsDisabled) are not source-level API.
        CheckResult r = run(owner()
                .field(AsmClassFactory.field("synth")
                        .access(Opcodes.ACC_PUBLIC | Opcodes.ACC_SYNTHETIC)
                        .ofType(INTERNAL_DESC)));

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
    void descriptorAndSignatureWalks_dedupSameType() throws IOException {
        // When the type appears in both the erased descriptor AND the generic signature (this
        // happens for non-generic types and for the outer type in `Outer<...>`), the cascade
        // mustn't emit two violations for the same (member, type, position).
        CheckResult r = run(owner()
                .method(AsmClassFactory.method("doIt")
                        .returns(INTERNAL_DESC)
                        .signature("()L" + AsmClassFactory.toInternal(INTERNAL_BIN) + ";")));

        long internalHits = r.violations().stream()
                .filter(v -> v.getDescription().contains(INTERNAL_BIN))
                .count();
        assertEquals(1, internalHits,
                "descriptor and signature walks must dedup the same type/position: " + r.violations());
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
