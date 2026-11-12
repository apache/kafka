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
import org.objectweb.asm.Opcodes;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ApiSurfaceTest {

    private static final File JAR_A = new File("/tmp/a.jar");
    private static final File JAR_B = new File("/tmp/b.jar");

    @Test
    void factsOf_acceptsBothBinaryAndDottedNames() {
        ClassFacts inner = facts("org.apache.kafka.foo.Outer$Inner");
        ApiSurface surface = ApiSurface.builder().recordClass(inner, JAR_A).build();

        assertSame(inner, surface.factsOf("org.apache.kafka.foo.Outer$Inner"));
        assertSame(inner, surface.factsOf("org.apache.kafka.foo.Outer.Inner"));
    }

    @Test
    void factsOf_returnsNullForUnknownClass() {
        ApiSurface surface = ApiSurface.builder().build();
        assertNull(surface.factsOf("org.apache.kafka.NotThere"));
    }

    @Test
    void jarOf_returnsRecordedJar() {
        ClassFacts f = facts("org.apache.kafka.foo.Bar");
        ApiSurface surface = ApiSurface.builder().recordClass(f, JAR_A).build();
        assertEquals(JAR_A, surface.jarOf("org.apache.kafka.foo.Bar"));
        assertNull(surface.jarOf("org.apache.kafka.NotThere"));
    }

    @Test
    void recordClass_jarRegistrationIsFirstWins() {
        // The scanner already de-dupes by binary name before calling recordClass, but the builder
        // defends with putIfAbsent on the jar map so jarOf stays stable if a duplicate slips through.
        ClassFacts f = facts("org.apache.kafka.foo.Bar");
        ApiSurface surface = ApiSurface.builder()
                .recordClass(f, JAR_A)
                .recordClass(f, JAR_B)
                .build();
        assertEquals(JAR_A, surface.jarOf("org.apache.kafka.foo.Bar"));
    }

    @Test
    void isEffectivelyPublic_acceptsBothNameForms() {
        // isEffectivelyPublic walks the enclosing chain on facts. Inner inherits via Outer's @Public.
        ClassFacts outer = facts("org.apache.kafka.foo.Outer", ClassFacts.Flag.PUBLIC_API);
        ClassFacts inner = facts("org.apache.kafka.foo.Outer$Inner");
        ApiSurface surface = ApiSurface.builder()
                .recordClass(outer, JAR_A)
                .recordClass(inner, JAR_A)
                .build();

        assertTrue(surface.isEffectivelyPublic("org.apache.kafka.foo.Outer$Inner"));
        assertTrue(surface.isEffectivelyPublic("org.apache.kafka.foo.Outer.Inner"));
    }

    @Test
    void isEffectivelyPublic_skipsMissingIntermediates() {
        // Bug #5 regression: an anonymous/synthetic intermediate like Outer$1 is filtered out
        // of the surface by ApiSurfaceScanner#isSyntheticOrAnonymous, but a named nested class
        // *inside* the synthetic (Outer$1$Inner) should still inherit Outer's @Public. The walk
        // has to step past the missing intermediate lexically rather than stopping at the gap.
        ClassFacts outer = facts("org.apache.kafka.foo.Outer", ClassFacts.Flag.PUBLIC_API);
        ApiSurface surface = ApiSurface.builder()
                .recordClass(outer, JAR_A)
                .build();

        assertTrue(surface.isEffectivelyPublic("org.apache.kafka.foo.Outer$1$Inner"),
                "anonymous-enclosed nested class must still inherit Outer's @Public");
    }

    @Test
    void isEffectivelyPublic_falseWhenNotAnnotated() {
        ClassFacts f = facts("org.apache.kafka.foo.Bar");
        ApiSurface surface = ApiSurface.builder().recordClass(f, JAR_A).build();
        assertFalse(surface.isEffectivelyPublic("org.apache.kafka.foo.Bar"));
        assertFalse(surface.isEffectivelyPublic("org.apache.kafka.UnknownClass"));
    }

    @Test
    void isEffectivelyPublic_privateNestedOverridesInheritedPublic() {
        ClassFacts outer = facts("org.apache.kafka.foo.Outer", ClassFacts.Flag.PUBLIC_API);
        ClassFacts inner = facts("org.apache.kafka.foo.Outer$Inner", ClassFacts.Flag.PRIVATE_API);
        ApiSurface surface = ApiSurface.builder()
                .recordClass(outer, JAR_A).recordClass(inner, JAR_A)
                .build();

        assertTrue(surface.isEffectivelyPublic("org.apache.kafka.foo.Outer"));
        assertFalse(surface.isEffectivelyPublic("org.apache.kafka.foo.Outer$Inner"));
    }

    @Test
    void isDeprecated_trueWhenClassItselfIsDeprecated() {
        ClassFacts f = facts("org.apache.kafka.foo.Bar", ClassFacts.Flag.DEPRECATED);
        ApiSurface surface = ApiSurface.builder().recordClass(f, JAR_A).build();
        assertTrue(surface.isDeprecated("org.apache.kafka.foo.Bar"));
    }

    @Test
    void isDeprecated_inheritsFromEnclosingClass() {
        // Outer is @Deprecated; Inner is not directly annotated. The walk up the enclosing chain
        // (Outer$Inner → Outer) finds the deprecation and propagates it.
        ClassFacts outer = facts("org.apache.kafka.foo.Outer", ClassFacts.Flag.DEPRECATED);
        ClassFacts inner = facts("org.apache.kafka.foo.Outer$Inner");
        ApiSurface surface = ApiSurface.builder()
                .recordClass(outer, JAR_A)
                .recordClass(inner, JAR_A)
                .build();

        assertTrue(surface.isDeprecated("org.apache.kafka.foo.Outer$Inner"));
        assertTrue(surface.isDeprecated("org.apache.kafka.foo.Outer.Inner"));
    }

    @Test
    void isDeprecated_falseWhenNeitherSelfNorEnclosingIsDeprecated() {
        ClassFacts outer = facts("org.apache.kafka.foo.Outer");
        ClassFacts inner = facts("org.apache.kafka.foo.Outer$Inner");
        ApiSurface surface = ApiSurface.builder()
                .recordClass(outer, JAR_A)
                .recordClass(inner, JAR_A)
                .build();

        assertFalse(surface.isDeprecated("org.apache.kafka.foo.Outer$Inner"));
    }

    @Test
    void isDeprecated_falseWhenClassNotInSurface() {
        // factsOf returns null on the very first iteration; the walk exits cleanly with false.
        ApiSurface surface = ApiSurface.builder().build();
        assertFalse(surface.isDeprecated("org.apache.kafka.NotThere"));
    }

    @Test
    void isDeprecated_falseWhenEnclosingNotInSurface() {
        // Nested class is recorded but its outer isn't. The walk finds the inner (not deprecated),
        // then looks up the missing outer, gets null, and exits with false. No inheritance possible
        // without facts for the enclosing class.
        ClassFacts inner = facts("org.apache.kafka.foo.Outer$Inner");
        ApiSurface surface = ApiSurface.builder().recordClass(inner, JAR_A).build();
        assertFalse(surface.isDeprecated("org.apache.kafka.foo.Outer$Inner"));
    }

    @Test
    void iterationSets_containWhatWasAdded() {
        ClassFacts a = facts("org.apache.kafka.foo.A");
        ClassFacts b = facts("org.apache.kafka.foo.B");
        ApiSurface surface = ApiSurface.builder()
                .recordClass(a, JAR_A).recordClass(b, JAR_A)
                .addEffectivePublic(a)
                .addDirectPublic(b)
                .build();

        assertEquals(1, surface.effectivePublic().size());
        assertTrue(surface.effectivePublic().contains(a));
        assertEquals(1, surface.directPublic().size());
        assertTrue(surface.directPublic().contains(b));
    }

    @Test
    void iterationSets_areImmutable() {
        ApiSurface surface = ApiSurface.builder().build();
        ClassFacts f = facts("org.apache.kafka.foo.Bar");
        assertThrows(UnsupportedOperationException.class, () -> surface.effectivePublic().add(f));
        assertThrows(UnsupportedOperationException.class, () -> surface.directPublic().add(f));
    }

    private static ClassFacts facts(String binaryName, ClassFacts.Flag... flags) {
        ClassFacts.Builder b = ClassFacts.builder(binaryName).sourceAccess(Opcodes.ACC_PUBLIC);
        for (ClassFacts.Flag flag : flags) b.addFlag(flag);
        return b.build();
    }
}
