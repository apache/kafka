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

import org.objectweb.asm.AnnotationVisitor;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.Opcodes;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

    /** Scan the given jars and return an immutable surface. */
    static ApiSurface scan(List<File> projectJars) throws IOException {
        ApiSurface.Builder surface = ApiSurface.builder();
        Map<String, ClassFacts> byBinaryName = new HashMap<>();

        // Pass 1 — read facts for every in-scope class.
        for (File jar : projectJars) {
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

                    ClassFacts facts = readClassFacts(jarFile, entry, binaryName);
                    byBinaryName.put(binaryName, facts);
                    surface.recordClass(facts, jar);
                }
            }
        }

        // Pass 2 — resolve inheritance and populate the surface's derived sets. Deprecated
        // classes are out of scope on both validation sides; the surface answers
        // {@link ApiSurface#isDeprecated} directly from the per-class facts so no separate
        // deprecated set is needed here.
        for (ClassFacts facts : byBinaryName.values()) {
            if (facts.isDeprecated()) continue;
            if (facts.isPublic()) {
                surface.addDirectPublic(facts);
            }
            if (resolveEffectiveAudience(facts.binaryName(), byBinaryName) == DirectAudience.PUBLIC) {
                surface.markEffectivelyPublic(facts);
                // Cascade only runs on externally-visible classes — a private nested class
                // technically inherits @Public from its outer under the Hadoop model, but its
                // methods/ctors are unreachable to consumers and shouldn't be cascade-checked.
                if (facts.isExternallyVisible()) {
                    surface.addEffectivePublic(facts);
                }
            }
        }

        return surface.build();
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
     * {@code Private} per the KIP.
     */
    private static DirectAudience resolveEffectiveAudience(String binaryName, Map<String, ClassFacts> byBinaryName) {
        String name = binaryName;
        while (true) {
            ClassFacts facts = byBinaryName.get(name);
            if (facts != null) {
                if (facts.isPublic()) return DirectAudience.PUBLIC;
                if (facts.isPrivate()) return DirectAudience.PRIVATE;
            }
            int dollar = name.lastIndexOf('$');
            if (dollar < 0) return DirectAudience.PRIVATE;
            name = name.substring(0, dollar);
        }
    }

    private enum DirectAudience { PUBLIC, PRIVATE }
}