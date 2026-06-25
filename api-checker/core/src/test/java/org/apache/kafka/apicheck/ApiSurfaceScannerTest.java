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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ApiSurfaceScannerTest {

    @TempDir
    Path tempDir;

    @Test
    void publicTopLevelClass_isInDirectAndEffectiveSets() throws IOException {
        File jar = projectJar("proj.jar",
                AsmClassFactory.klass("org.apache.kafka.foo.Bar").access(Opcodes.ACC_PUBLIC).publicApi());
        ApiSurface s = scan(jar);

        assertTrue(containsDotted(s.directPublic(), "org.apache.kafka.foo.Bar"));
        assertTrue(containsDotted(s.effectivePublic(), "org.apache.kafka.foo.Bar"));
        assertTrue(s.isEffectivelyPublic("org.apache.kafka.foo.Bar"));
        assertEquals(jar, s.jarOf("org.apache.kafka.foo.Bar"));
    }

    @Test
    void privateNested_overridesInheritedPublic() throws IOException {
        File jar = projectJar("proj.jar",
                AsmClassFactory.klass("org.apache.kafka.foo.Outer").access(Opcodes.ACC_PUBLIC).publicApi(),
                AsmClassFactory.klass("org.apache.kafka.foo.Outer$Inner").access(Opcodes.ACC_PUBLIC).privateApi());
        ApiSurface s = scan(jar);

        assertTrue(s.isEffectivelyPublic("org.apache.kafka.foo.Outer"));
        assertFalse(s.isEffectivelyPublic("org.apache.kafka.foo.Outer$Inner"),
                "@Private must override inherited @Public");
        assertFalse(containsDotted(s.directPublic(), "org.apache.kafka.foo.Outer.Inner"));
        assertFalse(containsDotted(s.effectivePublic(), "org.apache.kafka.foo.Outer.Inner"));
    }

    @Test
    void unannotatedNested_inheritsPublicFromOuter() throws IOException {
        File jar = projectJar("proj.jar",
                AsmClassFactory.klass("org.apache.kafka.foo.Outer").access(Opcodes.ACC_PUBLIC).publicApi(),
                AsmClassFactory.klass("org.apache.kafka.foo.Outer$Inner").access(Opcodes.ACC_PUBLIC));
        ApiSurface s = scan(jar);

        assertTrue(s.isEffectivelyPublic("org.apache.kafka.foo.Outer$Inner"));
        // Inner has no direct @Public, so it's not in the MISSING_JAVADOC iteration set.
        assertFalse(containsDotted(s.directPublic(), "org.apache.kafka.foo.Outer.Inner"));
        // Externally visible + effective @Public + owned → in the cascade iteration set.
        assertTrue(containsDotted(s.effectivePublic(), "org.apache.kafka.foo.Outer.Inner"));
    }

    @Test
    void packagePrivateNested_inheritsPublicButNotInCascade() throws IOException {
        // Nested-class header is ACC_PUBLIC (compiler convention) but the InnerClasses entry says
        // package-private. The scanner must trust the InnerClasses entry over the header.
        File jar = projectJar("proj.jar",
                AsmClassFactory.klass("org.apache.kafka.foo.Outer").access(Opcodes.ACC_PUBLIC).publicApi(),
                AsmClassFactory.klass("org.apache.kafka.foo.Outer$Inner")
                        .access(Opcodes.ACC_PUBLIC)
                        .nestedAccess(0));
        ApiSurface s = scan(jar);

        assertTrue(s.isEffectivelyPublic("org.apache.kafka.foo.Outer$Inner"),
                "chain walk sees inherited @Public regardless of visibility");
        // The effectivePublic set contains all owned + effective-Public classes; CascadeValidator
        // filters on isExternallyVisible() at the iteration site rather than at the scanner.
        assertTrue(containsDotted(s.effectivePublic(), "org.apache.kafka.foo.Outer.Inner"));
        ClassFacts innerFacts = s.factsOf("org.apache.kafka.foo.Outer$Inner");
        assertFalse(innerFacts.isExternallyVisible(),
                "package-private nested classes are filtered out by CascadeValidator, not the scanner");
    }

    @Test
    void deprecatedClass_isExcludedFromIterationSets() throws IOException {
        File jar = projectJar("proj.jar",
                AsmClassFactory.klass("org.apache.kafka.foo.OldBar")
                        .access(Opcodes.ACC_PUBLIC)
                        .publicApi()
                        .deprecated());
        ApiSurface s = scan(jar);

        assertTrue(s.isDeprecated("org.apache.kafka.foo.OldBar"));
        assertTrue(s.directPublic().isEmpty(), "deprecated classes are out of scope on both validation sides");
        assertTrue(s.effectivePublic().isEmpty());
        // isEffectivelyPublic answers the audience question only — deprecation is handled
        // separately by callers (CascadeValidator skips deprecated refs before this check).
        assertTrue(s.isEffectivelyPublic("org.apache.kafka.foo.OldBar"));
    }

    @Test
    void anonymousAndLambdaClasses_areSkippedEntirely() throws IOException {
        File jar = TempJarBuilder.jar()
                .addClass(AsmClassFactory.klass("org.apache.kafka.foo.Outer").access(Opcodes.ACC_PUBLIC).publicApi())
                .addClass(AsmClassFactory.klass("org.apache.kafka.foo.Outer$1").access(Opcodes.ACC_PUBLIC))
                .addClass(AsmClassFactory.klass("org.apache.kafka.foo.Outer$$Lambda$0").access(Opcodes.ACC_PUBLIC))
                .writeTo(tempDir, "proj.jar");
        ApiSurface s = scan(jar);

        assertNotNull(s.factsOf("org.apache.kafka.foo.Outer"));
        assertNull(s.factsOf("org.apache.kafka.foo.Outer$1"),
                "anonymous classes (digit after $) are not part of the API surface");
        assertNull(s.factsOf("org.apache.kafka.foo.Outer$$Lambda$0"),
                "lambda / synthetic-accessor classes ($$) are skipped");
    }

    @Test
    void nonKafkaClasses_areSkipped() throws IOException {
        File jar = TempJarBuilder.jar()
                .addClass(AsmClassFactory.klass("com.example.External").access(Opcodes.ACC_PUBLIC).publicApi())
                .writeTo(tempDir, "proj.jar");
        ApiSurface s = scan(jar);

        assertNull(s.factsOf("com.example.External"));
        assertTrue(s.directPublic().isEmpty());
    }

    @Test
    void packageInfoAndModuleInfo_areSkipped() throws IOException {
        // The scanner short-circuits on the binary name before reading any bytes, so the content
        // of these entries is irrelevant — pass garbage to prove they aren't parsed.
        File jar = TempJarBuilder.jar()
                .addEntry("org/apache/kafka/foo/package-info.class", new byte[]{0})
                .addEntry("module-info.class", new byte[]{0})
                .addClass(AsmClassFactory.klass("org.apache.kafka.foo.Bar").access(Opcodes.ACC_PUBLIC).publicApi())
                .writeTo(tempDir, "proj.jar");
        ApiSurface s = scan(jar);

        assertNotNull(s.factsOf("org.apache.kafka.foo.Bar"));
        assertNull(s.factsOf("org.apache.kafka.foo.package-info"));
        assertNull(s.factsOf("module-info"));
    }

    @Test
    void referenceJar_contributesMembershipButNotIteration() throws IOException {
        File proj = projectJar("proj.jar",
                AsmClassFactory.klass("org.apache.kafka.proj.OwnedClass").access(Opcodes.ACC_PUBLIC).publicApi());
        File ref = TempJarBuilder.jar()
                .addClass(AsmClassFactory.klass("org.apache.kafka.ref.RefClass").access(Opcodes.ACC_PUBLIC).publicApi())
                .writeTo(tempDir, "ref.jar");
        ApiSurface s = ApiSurfaceScanner.scan(List.of(proj), List.of(ref));

        // Reference class is recorded and contributes to the membership set so cross-module
        // @Public references resolve in cascade checks…
        assertNotNull(s.factsOf("org.apache.kafka.ref.RefClass"));
        assertTrue(s.isEffectivelyPublic("org.apache.kafka.ref.RefClass"));
        // …but doesn't take part in this project's MISSING_JAVADOC / cascade iteration.
        assertFalse(containsDotted(s.directPublic(),    "org.apache.kafka.ref.RefClass"));
        assertFalse(containsDotted(s.effectivePublic(), "org.apache.kafka.ref.RefClass"));
        // Project-owned class participates in both iteration sets.
        assertTrue(containsDotted(s.directPublic(),    "org.apache.kafka.proj.OwnedClass"));
        assertTrue(containsDotted(s.effectivePublic(), "org.apache.kafka.proj.OwnedClass"));
    }

    @Test
    void crossJarDuplicate_projectJarWins() throws IOException {
        // Same binary name in both jars — the project jar is scanned first and keeps ownership.
        AsmClassFactory.ClassBuilder dup =
                AsmClassFactory.klass("org.apache.kafka.dup.Dup").access(Opcodes.ACC_PUBLIC).publicApi();
        File proj = TempJarBuilder.jar().addClass(dup).writeTo(tempDir, "proj.jar");
        // Re-build a new ClassBuilder for the ref jar to avoid sharing state with the above call.
        File ref = TempJarBuilder.jar()
                .addClass(AsmClassFactory.klass("org.apache.kafka.dup.Dup").access(Opcodes.ACC_PUBLIC).publicApi())
                .writeTo(tempDir, "ref.jar");

        ApiSurface s = ApiSurfaceScanner.scan(List.of(proj), List.of(ref));

        assertEquals(proj, s.jarOf("org.apache.kafka.dup.Dup"));
        assertTrue(containsDotted(s.directPublic(), "org.apache.kafka.dup.Dup"),
                "project-jar entry establishes ownership so the class participates in iteration");
    }

    // Helpers

    private File projectJar(String fileName, AsmClassFactory.ClassBuilder... builders) throws IOException {
        TempJarBuilder jar = TempJarBuilder.jar();
        for (AsmClassFactory.ClassBuilder b : builders) jar.addClass(b);
        return jar.writeTo(tempDir, fileName);
    }

    private static ApiSurface scan(File... projectJars) throws IOException {
        return ApiSurfaceScanner.scan(List.of(projectJars), Collections.emptyList());
    }

    private static boolean containsDotted(java.util.Collection<ClassFacts> set, String dottedName) {
        return set.stream().anyMatch(f -> f.dottedName().equals(dottedName));
    }
}
