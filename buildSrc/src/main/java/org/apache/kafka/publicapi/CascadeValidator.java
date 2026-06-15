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
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/**
 * Checks that no public method of any effectively-{@code @Public} class leaks an internal Kafka
 * type through its return type, parameter types, or declared exceptions. Each finding either
 * lands in {@link CheckResult#violations()} (a real failure) or in
 * {@link CheckResult#suppressions()} (silenced by a class- or method-level
 * {@code @SuppressKafkaInternalApiUsage} — the reason is captured so reviewers can audit every
 * escape hatch on every build).
 *
 * <p>Reads bytecode directly via ASM rather than reflecting on a loaded {@code Class<?>}, which
 * sidesteps {@code LinkageError} / {@code NoClassDefFoundError} from broken transitive deps
 * (gRPC stubs, telemetry shims, etc.). The same robustness property as {@link ApiSurfaceScanner}.
 */
final class CascadeValidator {

    /** {@code @SuppressKafkaInternalApiUsage} — the escape hatch for known cascade leaks pending review. */
    private static final String SUPPRESS_DESCRIPTOR =
            "Lorg/apache/kafka/common/annotation/SuppressKafkaInternalApiUsage;";

    private CascadeValidator() {}

    static CheckResult validate(ApiSurface surface) throws IOException {
        List<PublicApiViolation> violations = new ArrayList<>();
        List<PublicApiViolation> suppressions = new ArrayList<>();
        for (ClassFacts cls : surface.effectivePublic()) {
            checkClass(cls, surface, violations, suppressions);
        }
        return new CheckResult(violations, suppressions);
    }

    private static void checkClass(ClassFacts cls, ApiSurface surface,
                                   List<PublicApiViolation> violations,
                                   List<PublicApiViolation> suppressions) throws IOException {
        File jarFile = surface.jarOf(cls.binaryName());
        if (jarFile == null) return; // class wasn't in any scanned jar
        String entryPath = cls.binaryName().replace('.', '/') + ".class";

        try (JarFile jar = new JarFile(jarFile)) {
            JarEntry entry = jar.getJarEntry(entryPath);
            if (entry == null) return;
            try (InputStream in = jar.getInputStream(entry)) {
                ClassReader reader = new ClassReader(in);
                reader.accept(new ClassVisitor(Opcodes.ASM9) {
                    /** Reason from a class-level {@code @SuppressKafkaInternalApiUsage}, or null. */
                    String classSuppressionReason;

                    @Override
                    public AnnotationVisitor visitAnnotation(String descriptor, boolean visible) {
                        if (SUPPRESS_DESCRIPTOR.equals(descriptor)) {
                            return new ReasonCaptureVisitor(r -> classSuppressionReason = r);
                        }
                        return null;
                    }

                    @Override
                    public MethodVisitor visitMethod(int access, String name, String descriptor,
                                                     String signature, String[] exceptions) {
                        if ((access & Opcodes.ACC_PUBLIC) == 0) return null;
                        // Bridge/synthetic methods are compiler-generated and never source-level API.
                        if ((access & (Opcodes.ACC_BRIDGE | Opcodes.ACC_SYNTHETIC)) != 0) return null;

                        // Buffer would-be violations and route them in visitEnd, because the method's
                        // own @SuppressKafkaInternalApiUsage is visited *after* visitMethod returns.
                        List<PublicApiViolation> buffered = new ArrayList<>();
                        checkAsmType(Type.getReturnType(descriptor), "INVALID_RETURN_TYPE",
                                "Public method returns non-public API type",
                                cls.binaryName(), name, surface, buffered);
                        for (Type argType : Type.getArgumentTypes(descriptor)) {
                            checkAsmType(argType, "INVALID_PARAMETER_TYPE",
                                    "Public method has non-public API parameter type",
                                    cls.binaryName(), name, surface, buffered);
                        }
                        if (exceptions != null) {
                            for (String excInternal : exceptions) {
                                checkBinaryReference(excInternal.replace('/', '.'),
                                        "INVALID_EXCEPTION_TYPE",
                                        "Public method declares non-public API exception type",
                                        cls.binaryName(), name, surface, buffered);
                            }
                        }

                        return new MethodVisitor(Opcodes.ASM9) {
                            /** Reason from a method-level {@code @SuppressKafkaInternalApiUsage}, or null. */
                            String methodSuppressionReason;

                            @Override
                            public AnnotationVisitor visitAnnotation(String d, boolean v) {
                                if (SUPPRESS_DESCRIPTOR.equals(d)) {
                                    return new ReasonCaptureVisitor(r -> methodSuppressionReason = r);
                                }
                                return null;
                            }

                            @Override
                            public void visitEnd() {
                                String reason = methodSuppressionReason != null ? methodSuppressionReason
                                        : classSuppressionReason;
                                if (reason != null) {
                                    for (PublicApiViolation original : buffered) {
                                        suppressions.add(asSuppression(original, reason));
                                    }
                                } else {
                                    violations.addAll(buffered);
                                }
                            }
                        };
                    }
                }, ClassReader.SKIP_CODE | ClassReader.SKIP_DEBUG | ClassReader.SKIP_FRAMES);
            }
        }
    }

    /** Recurse through array element types to find the concrete reference type, then check it. */
    private static void checkAsmType(Type type, String violationType, String message,
                                     String owner, String methodName, ApiSurface surface,
                                     List<PublicApiViolation> violations) {
        if (type.getSort() == Type.ARRAY) {
            checkAsmType(type.getElementType(), violationType, message, owner, methodName, surface, violations);
        } else if (type.getSort() == Type.OBJECT) {
            // Type.getClassName() returns the binary form (e.g. "org.apache.kafka.X$Y").
            checkBinaryReference(type.getClassName(), violationType, message, owner, methodName, surface, violations);
        }
    }

    /**
     * Apply the cascade rule to one referenced type. The reference is a violation iff it is
     * in {@code org.apache.kafka.*}, not deprecated, and not in the surface's
     * effective-Public-dotted set.
     */
    private static void checkBinaryReference(String binaryName, String violationType, String message,
                                             String owner, String methodName, ApiSurface surface,
                                             List<PublicApiViolation> violations) {
        if (!binaryName.startsWith("org.apache.kafka.")) return;
        if (surface.isDeprecated(binaryName)) return;
        if (surface.isEffectivelyPublic(binaryName)) return;
        violations.add(new PublicApiViolation(owner, violationType, message + ": " + binaryName, methodName));
    }

    /**
     * Render a would-be violation as a suppression entry that the reporter prints in the
     * "Suppressions" section. The reason from the {@code @SuppressKafkaInternalApiUsage}
     * annotation is appended so reviewers can audit every escape hatch on every build.
     */
    private static PublicApiViolation asSuppression(PublicApiViolation original, String reason) {
        String prettyReason = reason.isEmpty() ? "(no reason given)" : reason;
        String description = "Suppressed " + original.getViolationType() + " in "
                + original.getClassName() + "#" + original.getMemberName()
                + " — " + original.getDescription()
                + " — reason: " + prettyReason;
        return new PublicApiViolation(original.getClassName(), "SUPPRESSED", description, original.getMemberName());
    }

    /** Captures the {@code value()} of a {@code @SuppressKafkaInternalApiUsage} annotation. */
    private static final class ReasonCaptureVisitor extends AnnotationVisitor {
        private final Consumer<String> setter;
        private boolean assigned;

        ReasonCaptureVisitor(Consumer<String> setter) {
            super(Opcodes.ASM9);
            this.setter = setter;
        }

        @Override
        public void visit(String name, Object value) {
            if ("value".equals(name) && value instanceof String) {
                setter.accept((String) value);
                assigned = true;
            }
        }

        @Override
        public void visitEnd() {
            if (!assigned) setter.accept(""); // value() omitted — treat as suppressed with empty reason
        }
    }
}