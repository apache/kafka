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
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.signature.SignatureReader;
import org.objectweb.asm.signature.SignatureVisitor;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
        // Group by jar so each archive is opened once. Private/package-private nested classes
        // inherit the audience but their methods and ctors aren't reachable to consumers, so
        // cascade-walking them would just produce noise on internal helpers — filter them out
        // before grouping.
        Map<File, List<ClassFacts>> classesByJar = new LinkedHashMap<>();
        for (ClassFacts cls : surface.effectivePublic()) {
            if (!cls.isExternallyVisible()) continue;
            File jar = surface.jarOf(cls.binaryName());
            if (jar == null) continue;
            classesByJar.computeIfAbsent(jar, j -> new ArrayList<>()).add(cls);
        }
        for (Map.Entry<File, List<ClassFacts>> e : classesByJar.entrySet()) {
            try (JarFile jar = new JarFile(e.getKey())) {
                for (ClassFacts cls : e.getValue()) {
                    checkClass(cls, jar, surface, violations, suppressions);
                }
            }
        }
        return new CheckResult(violations, suppressions);
    }

    private static void checkClass(ClassFacts cls, JarFile jar, ApiSurface surface,
                                   List<PublicApiViolation> violations,
                                   List<PublicApiViolation> suppressions) throws IOException {
        String entryPath = cls.binaryName().replace('.', '/') + ".class";
        JarEntry entry = jar.getJarEntry(entryPath);
        if (entry == null) return;
        try (InputStream in = jar.getInputStream(entry)) {
            ClassReader reader = new ClassReader(in);
            reader.accept(new CascadeClassVisitor(cls, surface, violations, suppressions),
                    ClassReader.SKIP_CODE | ClassReader.SKIP_DEBUG | ClassReader.SKIP_FRAMES);
        }
    }

    /**
     * Drives the cascade check for a single class: validates the class header (extends/implements
     * + generic supertype args), then dispatches to per-method and per-field visitors that buffer
     * findings so a member-level {@code @SuppressKafkaInternalApiUsage} can divert them after the
     * fact.
     */
    private static final class CascadeClassVisitor extends ClassVisitor {
        private final ClassFacts cls;
        private final ApiSurface surface;
        private final List<PublicApiViolation> violations;
        private final List<PublicApiViolation> suppressions;
        /** Reason from a class-level {@code @SuppressKafkaInternalApiUsage}, or null. */
        private String classSuppressionReason;
        /**
         * Buffer extends/implements violations until {@link #visitEnd} so the
         * class-level {@code @SuppressKafkaInternalApiUsage} (visited after the class
         * header in ASM's event order) can divert them to suppressions if present.
         */
        private final List<PublicApiViolation> headerBuffered = new ArrayList<>();

        CascadeClassVisitor(ClassFacts cls, ApiSurface surface,
                            List<PublicApiViolation> violations,
                            List<PublicApiViolation> suppressions) {
            super(Opcodes.ASM9);
            this.cls = cls;
            this.surface = surface;
            this.violations = violations;
            this.suppressions = suppressions;
        }

        @Override
        public void visit(int version, int access, String name, String signature,
                          String superName, String[] interfaces) {
            // The class header itself leaks a non-public type if the @Public class
            // extends or implements an internal Kafka type the consumer can name.
            if (superName != null && !"java/lang/Object".equals(superName)) {
                checkSupertype(superName.replace('/', '.'),
                        "Public class extends non-public API type");
            }
            if (interfaces != null) {
                for (String iface : interfaces) {
                    checkSupertype(iface.replace('/', '.'),
                            "Public class implements non-public API type");
                }
            }
            // Generic supertype + interface type arguments live in the signature.
            // Walk via the supertype-aware path so package-private intermediates are
            // skipped just like the direct extends/implements check above.
            if (signature != null) {
                new SignatureReader(signature).accept(new SignatureVisitor(Opcodes.ASM9) {
                    @Override
                    public void visitClassType(String typeName) {
                        checkSupertype(typeName.replace('/', '.'),
                                "Public class header signature exposes non-public API type");
                    }
                });
            }
        }

        /**
         * Supertype cascade is stricter than the method/field cascade: package-private
         * supertypes aren't a real leak because a consumer can't even <em>name</em>
         * them from outside the package (no {@code instanceof}, no downcast). Skip
         * those; otherwise fall through to the same cascade rule as everywhere else.
         */
        private void checkSupertype(String binaryName, String message) {
            ClassFacts target = surface.factsOf(binaryName);
            if (target != null && !target.isExternallyVisible()) return;
            checkBinaryReference(binaryName, "INVALID_SUPERTYPE", message,
                    cls.binaryName(), null, surface, headerBuffered);
        }

        @Override
        public void visitEnd() {
            if (classSuppressionReason != null) {
                for (PublicApiViolation original : headerBuffered) {
                    suppressions.add(asSuppression(original, classSuppressionReason));
                }
            } else {
                violations.addAll(headerBuffered);
            }
        }

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
            // KIP-1265: a Public class's externally-visible methods (public + protected,
            // since protected members are reachable to subclasses of an extensible Public
            // class) must not leak non-public types.
            if ((access & (Opcodes.ACC_PUBLIC | Opcodes.ACC_PROTECTED)) == 0) return null;
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
            // Generic type arguments (e.g. Map<String, InternalFoo>) live in the
            // signature, not the erased descriptor — walk them too so the cascade
            // catches leaks the type-erasure layer would otherwise hide. Position-aware
            // so a return-type leak doesn't get mislabeled as a parameter.
            collectMethodSignatureRefs(signature, cls.binaryName(), name, surface, buffered);

            return new BufferedMemberVisitor(buffered);
        }

        @Override
        public FieldVisitor visitField(int access, String name, String descriptor,
                                       String signature, Object value) {
            // KIP-1265 names field types explicitly: a Public class's externally-visible
            // fields (public + protected) must not expose non-public types either.
            if ((access & (Opcodes.ACC_PUBLIC | Opcodes.ACC_PROTECTED)) == 0) return null;
            if ((access & Opcodes.ACC_SYNTHETIC) != 0) return null;

            // Buffer the would-be violation and route it in visitEnd, because the
            // field's own @SuppressKafkaInternalApiUsage is visited *after* visitField.
            List<PublicApiViolation> buffered = new ArrayList<>();
            checkAsmType(Type.getType(descriptor), "INVALID_FIELD_TYPE",
                    "Public field exposes non-public API type",
                    cls.binaryName(), name, surface, buffered);
            // Walk the generic field signature too — `List<InternalFoo>` etc. is
            // erased to plain List in the descriptor.
            collectSignatureRefs(signature, "INVALID_FIELD_TYPE",
                    "Public field signature exposes non-public API type",
                    cls.binaryName(), name, surface, buffered);

            return new BufferedFieldVisitor(buffered);
        }

        /**
         * Drains {@code buffered} into either {@link #violations} or {@link #suppressions} after
         * the member's own {@code @SuppressKafkaInternalApiUsage} (if any) has been seen. Falls
         * back to the class-level suppression reason when the member doesn't carry its own.
         */
        private void flush(List<PublicApiViolation> buffered, String memberReason) {
            String reason = memberReason != null ? memberReason : classSuppressionReason;
            if (reason != null) {
                for (PublicApiViolation original : buffered) {
                    suppressions.add(asSuppression(original, reason));
                }
            } else {
                violations.addAll(buffered);
            }
        }

        /** MethodVisitor that captures a method-level suppression reason and flushes on visitEnd. */
        private final class BufferedMemberVisitor extends MethodVisitor {
            private final List<PublicApiViolation> buffered;
            private String reason;

            BufferedMemberVisitor(List<PublicApiViolation> buffered) {
                super(Opcodes.ASM9);
                this.buffered = buffered;
            }

            @Override
            public AnnotationVisitor visitAnnotation(String d, boolean v) {
                if (SUPPRESS_DESCRIPTOR.equals(d)) {
                    return new ReasonCaptureVisitor(r -> reason = r);
                }
                return null;
            }

            @Override
            public void visitEnd() {
                flush(buffered, reason);
            }
        }

        /** FieldVisitor that captures a field-level suppression reason and flushes on visitEnd. */
        private final class BufferedFieldVisitor extends FieldVisitor {
            private final List<PublicApiViolation> buffered;
            private String reason;

            BufferedFieldVisitor(List<PublicApiViolation> buffered) {
                super(Opcodes.ASM9);
                this.buffered = buffered;
            }

            @Override
            public AnnotationVisitor visitAnnotation(String d, boolean v) {
                if (SUPPRESS_DESCRIPTOR.equals(d)) {
                    return new ReasonCaptureVisitor(r -> reason = r);
                }
                return null;
            }

            @Override
            public void visitEnd() {
                flush(buffered, reason);
            }
        }
    }

    /**
     * Walk a generic JVM signature and route each referenced class type through the cascade
     * check. {@code signature} is the optional generic descriptor ASM hands to
     * {@code visitMethod}/{@code visitField}; it's null for non-generic members.
     *
     * <p>Used for field signatures and the class-header signature, both of which have a
     * single cascade label; method signatures go through {@link #collectMethodSignatureRefs}
     * so the return-type / parameter-type / exception-type positions get distinct labels.
     */
    private static void collectSignatureRefs(String signature, String violationType, String message,
                                             String owner, String memberName, ApiSurface surface,
                                             List<PublicApiViolation> violations) {
        if (signature == null) return;
        new SignatureReader(signature).accept(new SignatureVisitor(Opcodes.ASM9) {
            @Override
            public void visitClassType(String name) {
                checkBinaryReference(name.replace('/', '.'),
                        violationType, message, owner, memberName, surface, violations);
            }
        });
    }

    /**
     * Walk a method signature using ASM's position-aware sub-visitor hooks so generic
     * leaks get labeled by where they actually sit: a leaking return-type
     * argument becomes {@code INVALID_RETURN_TYPE}, a parameter argument becomes
     * {@code INVALID_PARAMETER_TYPE}, and a generic throws-clause becomes
     * {@code INVALID_EXCEPTION_TYPE}. Type-parameter bounds
     * ({@code <T extends InternalFoo>}) fall under the parameter label since they constrain
     * caller-supplied types.
     */
    private static void collectMethodSignatureRefs(String signature, String owner, String memberName,
                                                   ApiSurface surface, List<PublicApiViolation> violations) {
        if (signature == null) return;
        new SignatureReader(signature).accept(new SignatureVisitor(Opcodes.ASM9) {
            @Override
            public SignatureVisitor visitReturnType() {
                return classTypeCollector("INVALID_RETURN_TYPE",
                        "Public method signature return type exposes non-public API type",
                        owner, memberName, surface, violations);
            }

            @Override
            public SignatureVisitor visitParameterType() {
                return classTypeCollector("INVALID_PARAMETER_TYPE",
                        "Public method signature parameter type exposes non-public API type",
                        owner, memberName, surface, violations);
            }

            @Override
            public SignatureVisitor visitExceptionType() {
                return classTypeCollector("INVALID_EXCEPTION_TYPE",
                        "Public method signature exception type exposes non-public API type",
                        owner, memberName, surface, violations);
            }

            @Override
            public SignatureVisitor visitClassBound() {
                return classTypeCollector("INVALID_PARAMETER_TYPE",
                        "Public method type parameter bound exposes non-public API type",
                        owner, memberName, surface, violations);
            }

            @Override
            public SignatureVisitor visitInterfaceBound() {
                return classTypeCollector("INVALID_PARAMETER_TYPE",
                        "Public method type parameter bound exposes non-public API type",
                        owner, memberName, surface, violations);
            }
        });
    }

    private static SignatureVisitor classTypeCollector(String violationType, String message, String owner,
                                                       String memberName, ApiSurface surface,
                                                       List<PublicApiViolation> violations) {
        return new SignatureVisitor(Opcodes.ASM9) {
            @Override
            public void visitClassType(String name) {
                checkBinaryReference(name.replace('/', '.'),
                        violationType, message, owner, memberName, surface, violations);
            }
        };
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
     *
     * <p>The descriptor walk and the generic-signature walk overlap (e.g. {@code Windows} in
     * {@code extends Windows<TimeWindow>} is seen by both), so dedup before adding to keep
     * one entry per leaked type per member.
     */
    private static void checkBinaryReference(String binaryName, String violationType, String message,
                                             String owner, String methodName, ApiSurface surface,
                                             List<PublicApiViolation> violations) {
        if (!binaryName.startsWith("org.apache.kafka.")) return;
        if (surface.isDeprecated(binaryName)) return;
        if (surface.isEffectivelyPublic(binaryName)) return;
        // Dedup on the semantic key (type + violation position + owner + member) rather than
        // the human-readable description, so the descriptor walk and the signature walk don't
        // produce two findings for the same leak just because they word the message differently.
        String descriptionSuffix = ": " + binaryName;
        for (PublicApiViolation existing : violations) {
            if (violationType.equals(existing.getViolationType())
                    && owner.equals(existing.getClassName())
                    && java.util.Objects.equals(methodName, existing.getMemberName())
                    && existing.getDescription().endsWith(descriptionSuffix)) {
                return;
            }
        }
        violations.add(new PublicApiViolation(owner, violationType, message + descriptionSuffix, methodName));
    }

    /**
     * Render a would-be violation as a suppression entry that the reporter prints in the
     * "Suppressions" section. The reason from the {@code @SuppressKafkaInternalApiUsage}
     * annotation is appended so reviewers can audit every escape hatch on every build.
     */
    private static PublicApiViolation asSuppression(PublicApiViolation original, String reason) {
        boolean noReason = reason.isEmpty();
        String prettyReason = noReason ? PublicApiViolation.NO_REASON_MARKER : reason;
        String description = "Suppressed " + original.getViolationType() + " in "
                + original.getClassName() + "#" + original.getMemberName()
                + " — " + original.getDescription()
                + " — reason: " + prettyReason;
        return new PublicApiViolation(original.getClassName(), "SUPPRESSED",
                description, original.getMemberName(), noReason);
    }

}