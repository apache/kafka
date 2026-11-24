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

import org.objectweb.asm.AnnotationVisitor;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.Opcodes;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/**
 * Builds an {@link ApiSurface} from a set of project jars in a single two-pass scan:
 * <ol>
 *   <li>Read each class's direct bytecode facts (annotations, source-level access).</li>
 *   <li>Walk every class's enclosing-class chain (Hadoop-style inheritance) to resolve effective
 *       audience and assemble the derived dotted-name sets in the surface.</li>
 * </ol>
 *
 * <p>Reads bytecode metadata via ASM rather than the classloader, so a class with broken
 * transitive deps (gRPC stubs, telemetry shims, …) doesn't trip {@code LinkageError} —
 * annotation descriptors live in the constant pool, no linking required.
 */
final class ApiSurfaceScanner {

    // Bytecode descriptors used to identify class-level annotations the checker cares about.
    private static final String PUBLIC_API_DESCRIPTOR =
            "Lorg/apache/kafka/common/annotation/InterfaceAudience$Public;";
    private static final String PRIVATE_API_DESCRIPTOR =
            "Lorg/apache/kafka/common/annotation/InterfaceAudience$Private;";
    private static final String DEPRECATED_DESCRIPTOR = "Ljava/lang/Deprecated;";

    private ApiSurfaceScanner() {}

    /**
     * Scan the project's own jars and any reference jars (sibling Kafka modules it depends on)
     * and return an immutable surface.
     *
     * <p>Reference-jar classes contribute to {@link ApiSurface#factsOf} lookups and the
     * membership set behind {@link ApiSurface#isEffectivelyPublic} so cross-module
     * {@code @Public} references resolve correctly, but they do NOT take part in this
     * project's MISSING_JAVADOC iteration or cascade iteration — each module checks its
     * own surface.
     */
    static ApiSurface scan(List<File> projectJars, List<File> referenceJars) throws IOException {
        ApiSurface.Builder surface = ApiSurface.builder();
        Map<String, ClassFacts> byBinaryName = new HashMap<>();
        Set<String> projectJarDottedNames = new HashSet<>();

        // Pass 1 — read facts for every in-scope class. Project-owned classes get tracked so
        // pass 2 can keep them out of reference-only iteration sets.
        scanJars(projectJars, byBinaryName, surface, projectJarDottedNames::add);
        scanJars(referenceJars, byBinaryName, surface, name -> { });

        // Pass 2 — resolve inheritance and populate the surface's derived sets. Deprecated
        // classes are out of scope on both validation sides; the surface answers
        // {@link ApiSurface#isDeprecated} directly from the per-class facts so no separate
        // deprecated set is needed here.
        for (ClassFacts facts : byBinaryName.values()) {
            if (facts.isDeprecated() || !projectJarDottedNames.contains(facts.dottedName())) continue;
            if (facts.isPublic()) {
                surface.addDirectPublic(facts);
            }
            if (resolveEffectiveAudience(facts.binaryName(), byBinaryName) == DirectAudience.PUBLIC) {
                // Owned effective-Public classes all go into the surface — cross-module
                // @Public references resolve via {@link ApiSurface#isEffectivelyPublic} which
                // walks the enclosing chain. Reference-jar classes don't need to be added
                // here: they're validated by their own module's task.
                //
                // Iteration consumers (CascadeValidator) filter on
                // {@link ClassFacts#isExternallyVisible()} at the call site — private nested
                // classes inherit the audience but their methods/ctors aren't reachable to
                // consumers and shouldn't be cascade-walked.
                surface.addEffectivePublic(facts);
            }
        }

        return surface.build();
    }

    /** Backwards-compatible overload for callers that don't need reference jars (consumer-side scan). */
    static ApiSurface scan(List<File> projectJars) throws IOException {
        return scan(projectJars, java.util.Collections.emptyList());
    }

    private static void scanJars(List<File> jars, Map<String, ClassFacts> byBinaryName,
                                 ApiSurface.Builder surface,
                                 java.util.function.Consumer<String> markOwned) throws IOException {
        for (File jar : jars) {
            try (JarFile jarFile = new JarFile(jar)) {
                Enumeration<JarEntry> entries = jarFile.entries();
                while (entries.hasMoreElements()) {
                    JarEntry entry = entries.nextElement();
                    if (!entry.getName().endsWith(".class")) continue;

                    String binaryName = entry.getName()
                            .replace('/', '.')
                            .replaceAll(".class$", "");
                    if (binaryName.endsWith("package-info") || binaryName.endsWith("module-info")) continue;
                    if (!binaryName.startsWith("org.apache.kafka.")) continue;
                    // Anonymous / local / synthetic classes are never part of the public API surface,
                    // but would otherwise inherit @Public from an enclosing class under the
                    // Hadoop-style inheritance rule and trip cascade checks.
                    if (isSyntheticOrAnonymous(binaryName)) continue;
                    // First jar wins for cross-jar duplicates — project jars are scanned first
                    // so they keep ownership over a class also present in a reference jar.
                    if (byBinaryName.containsKey(binaryName)) continue;

                    ClassFacts facts = readClassFacts(jarFile, entry, binaryName);
                    byBinaryName.put(binaryName, facts);
                    surface.recordClass(facts, jar);
                    markOwned.accept(facts.dottedName());
                }
            }
        }
    }

    /** Read a class file's bytecode facts via ASM (jar-entry variant). */
    static ClassFacts readClassFacts(JarFile jar, JarEntry entry, String binaryName) throws IOException {
        try (InputStream in = jar.getInputStream(entry)) {
            return readClassFactsFromStream(in, binaryName);
        }
    }

    /** Read a class file's bytecode facts via ASM (stream variant — used for classpath lookups). */
    static ClassFacts readClassFactsFromStream(InputStream in, String binaryName) throws IOException {
        ClassFacts.Builder builder = ClassFacts.builder(binaryName);
        String internalName = binaryName.replace('.', '/');
        ClassReader reader = new ClassReader(in);
        reader.accept(new ClassVisitor(Opcodes.ASM9) {
            @Override
            public void visit(int version, int access, String name, String signature,
                              String superName, String[] interfaces) {
                builder.sourceAccess(access); // top-level access; overridden below for nested
            }

            @Override
            public AnnotationVisitor visitAnnotation(String descriptor, boolean visible) {
                if (PUBLIC_API_DESCRIPTOR.equals(descriptor)) {
                    builder.addFlag(ClassFacts.Flag.PUBLIC_API);
                } else if (PRIVATE_API_DESCRIPTOR.equals(descriptor)) {
                    builder.addFlag(ClassFacts.Flag.PRIVATE_API);
                } else if (DEPRECATED_DESCRIPTOR.equals(descriptor)) {
                    builder.addFlag(ClassFacts.Flag.DEPRECATED);
                }
                return null;
            }

            @Override
            public void visitInnerClass(String name, String outerName, String innerName, int access) {
                if (internalName.equals(name)) {
                    // For nested classes the InnerClasses entry holds the real source-level
                    // access; the class header's ACC_PUBLIC is a compiler artefact.
                    builder.sourceAccess(access);
                }
            }
        }, ClassReader.SKIP_CODE | ClassReader.SKIP_DEBUG | ClassReader.SKIP_FRAMES);
        return builder.build();
    }

    /**
     * @return true if the binary name encodes an anonymous, local, or compiler-synthetic class
     *         (e.g. {@code Outer$1}, {@code Outer$1$Inner}, {@code Outer$$Lambda$N}). Such classes
     *         are never part of the public API surface.
     */
    private static boolean isSyntheticOrAnonymous(String binaryName) {
        if (binaryName.contains("$$")) return true; // lambdas / synthetic accessor classes
        int dollar = binaryName.indexOf('$');
        while (dollar >= 0) {
            int nextDollar = binaryName.indexOf('$', dollar + 1);
            int end = nextDollar < 0 ? binaryName.length() : nextDollar;
            // A segment that starts with a digit is an anonymous or local class.
            if (end > dollar + 1 && Character.isDigit(binaryName.charAt(dollar + 1))) {
                return true;
            }
            dollar = nextDollar;
        }
        return false;
    }

    /**
     * Walk the enclosing-class chain (by stripping {@code $}-segments from the binary name) and
     * return the audience of the nearest class with an explicit annotation. Default is
     * {@code Private} per the KIP. Uses the same {@link ClassFacts#parentBinaryName} stepping
     * rule as {@link ApiSurface#findInChain} so the two walks agree on missing intermediates.
     */
    private static DirectAudience resolveEffectiveAudience(String binaryName, Map<String, ClassFacts> byBinaryName) {
        String name = binaryName;
        while (name != null) {
            ClassFacts facts = byBinaryName.get(name);
            if (facts != null) {
                if (facts.isPublic()) return DirectAudience.PUBLIC;
                if (facts.isPrivate()) return DirectAudience.PRIVATE;
            }
            name = ClassFacts.parentBinaryName(name);
        }
        return DirectAudience.PRIVATE;
    }

    private enum DirectAudience { PUBLIC, PRIVATE }
}