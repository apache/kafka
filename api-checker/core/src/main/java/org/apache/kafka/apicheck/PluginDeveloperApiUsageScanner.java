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
import org.objectweb.asm.Handle;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.signature.SignatureReader;
import org.objectweb.asm.signature.SignatureVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/**
 * Scans compiled JVM bytecode (.class files, packaged or loose) for references to Kafka classes
 * that are not annotated with {@code @InterfaceAudience.Public}. Catches Java, Scala, Kotlin and
 * any other JVM-language consumer uniformly — unlike a source-level scan, which is regex-bound
 * to .java imports.
 *
 * <h2>Known limitations</h2>
 * A handful of bytecode reference kinds are intentionally not walked. None of these are likely
 * in practice for plugin/connector code but they're worth knowing about when interpreting a
 * "0 violations" report:
 * <ul>
 *   <li><b>Parameter annotations</b> ({@code MethodVisitor#visitParameterAnnotation}) — the
 *       annotation TYPE on a parameter, e.g. {@code void foo(@InternalAnno String s)}. The
 *       method's own header (return / parameter / exception types) is walked, so any usage
 *       that survives erasure into the method descriptor is still caught.</li>
 *   <li><b>Type-use annotations</b> (JSR 308, {@code MethodVisitor#visitTypeAnnotation}) —
 *       annotations attached to type positions like {@code List<@InternalAnno String>}. Used
 *       almost exclusively by static-analysis tools; the underlying type is still recorded.</li>
 *   <li><b>Class literals inside annotation element values</b> — e.g.
 *       {@code @SomeAnnotation(impl = InternalClass.class)}. The annotation type itself is
 *       recorded but {@code AnnotationVisitor#visit} isn't traversed into for class-typed
 *       values. {@code InternalClass.class} loaded into a local also shows up via
 *       {@code visitLdcInsn} from the method body, so a typical "save it then return it"
 *       pattern is still caught — the gap is only when the class literal exists exclusively
 *       inside an annotation.</li>
 *   <li><b>Inlined compile-time constants</b> — Java inlines {@code public static final}
 *       primitive/String constants at the use site, so {@code InternalClass.SOME_CONSTANT}
 *       leaves no reference in the consumer's bytecode at all. This is documented in the KIP.</li>
 *   <li><b>Nested types referenced only through a generic type argument</b> — e.g.
 *       {@code Foo<Outer.Inner>} in a signature. The signature visitor handles the outer type
 *       via {@link SignatureVisitor#visitClassType} but doesn't override
 *       {@link SignatureVisitor#visitInnerClassType}, so the inner type is missed. Direct
 *       (non-generic) references like {@code Outer.Inner} as a return / parameter / field
 *       type carry {@code Outer$Inner} in the erased descriptor and ARE caught.</li>
 * </ul>
 */
public class PluginDeveloperApiUsageScanner {
    private static final Logger LOG = LoggerFactory.getLogger(PluginDeveloperApiUsageScanner.class);
    private static final int ASM_API = Opcodes.ASM9;

    /** Internal-form prefix (slashes) for any class we care about checking the audience of. */
    private static final String KAFKA_INTERNAL_PREFIX = "org/apache/kafka/";
    /** Descriptor of {@code @SuppressKafkaInternalApiUsage} — honoured when present on the enclosing class or member. */
    private static final String SUPPRESS_DESCRIPTOR =
            "Lorg/apache/kafka/common/annotation/SuppressKafkaInternalApiUsage;";
    private static final String NO_REASON_GIVEN = PublicApiViolation.NO_REASON_MARKER;

    private final Predicate<String> isPublicApi;

    /**
     * @param isPublicApi callback that returns {@code true} when the given binary class name
     *                    (e.g. {@code org.apache.kafka.clients.producer.KafkaProducer}) is part
     *                    of the public API surface
     */
    public PluginDeveloperApiUsageScanner(Predicate<String> isPublicApi) {
        this.isPublicApi = isPublicApi;
    }

    /**
     * Scan every {@code .class} entry reachable from the supplied roots. Each root may be a
     * directory of class files, an individual .class file, or a .jar archive.
     */
    public CheckResult scan(List<File> roots) throws IOException {
        // Use maps keyed by (consumer class, referenced internal class, member, line) so we
        // don't double-record the same call site reachable through multiple visitor callbacks.
        Map<String, PublicApiViolation> violations = new LinkedHashMap<>();
        Map<String, PublicApiViolation> suppressions = new LinkedHashMap<>();
        for (File root : roots) {
            if (root == null || !root.exists()) {
                continue;
            }
            if (root.isDirectory()) {
                scanDirectory(root, violations, suppressions);
            } else if (root.getName().endsWith(".jar")) {
                scanJar(root, violations, suppressions);
            } else if (root.getName().endsWith(".class")) {
                try (InputStream in = new BufferedInputStream(Files.newInputStream(root.toPath()))) {
                    scanClassStream(in, violations, suppressions);
                }
            }
        }
        return new CheckResult(new ArrayList<>(violations.values()),
                new ArrayList<>(suppressions.values()));
    }

    private void scanDirectory(File dir,
                               Map<String, PublicApiViolation> violations,
                               Map<String, PublicApiViolation> suppressions) throws IOException {
        File[] children = dir.listFiles();
        if (children == null) {
            return;
        }
        for (File child : children) {
            if (child.isDirectory()) {
                scanDirectory(child, violations, suppressions);
            } else if (child.getName().endsWith(".class")) {
                try (InputStream in = new BufferedInputStream(Files.newInputStream(child.toPath()))) {
                    scanClassStream(in, violations, suppressions);
                }
            }
        }
    }

    private void scanJar(File jar,
                         Map<String, PublicApiViolation> violations,
                         Map<String, PublicApiViolation> suppressions) throws IOException {
        try (JarFile jarFile = new JarFile(jar)) {
            Enumeration<JarEntry> entries = jarFile.entries();
            while (entries.hasMoreElements()) {
                JarEntry entry = entries.nextElement();
                if (!entry.getName().endsWith(".class")) {
                    continue;
                }
                try (InputStream in = new BufferedInputStream(jarFile.getInputStream(entry))) {
                    scanClassStream(in, violations, suppressions);
                }
            }
        }
    }

    private void scanClassStream(InputStream in,
                                 Map<String, PublicApiViolation> violations,
                                 Map<String, PublicApiViolation> suppressions) throws IOException {
        ClassReader reader = new ClassReader(in);
        reader.accept(new ReferenceCollectingClassVisitor(violations, suppressions), ClassReader.SKIP_FRAMES);
    }

    private void recordIfInternal(String internalName,
                                  String consumerClass,
                                  String memberName,
                                  int line,
                                  String suppressionReason,
                                  Map<String, PublicApiViolation> violations,
                                  Map<String, PublicApiViolation> suppressions) {
        String binaryName = resolveInternalKafkaReference(internalName);
        if (binaryName == null) {
            return;
        }
        // Test the full nested name. The predicate (backed by ApiSurface#isEffectivelyPublic)
        // walks the enclosing chain level-by-level, so an explicit @InterfaceAudience.Private
        // on a nested class correctly overrides an inherited @Public from its outer — collapsing
        // to the outer name here would silently allow exactly that override.
        if (isPublicApi.test(binaryName)) {
            return;
        }
        // Don't report a class flagging references to its own outermost compilation unit
        // (covers self-references and references between siblings nested under the same outer).
        if (ClassFacts.outermostBinaryName(binaryName).equals(ClassFacts.outermostBinaryName(consumerClass))) {
            return;
        }
        String location = formatLocation(consumerClass, memberName, line);
        String key = referenceKey(consumerClass, binaryName, memberName, line);
        if (suppressionReason != null) {
            recordSuppression(suppressions, key, consumerClass, binaryName, memberName, location, suppressionReason);
        } else {
            recordViolation(violations, key, consumerClass, binaryName, memberName, location);
        }
    }

    /**
     * @return the dotted binary name of an {@code org.apache.kafka.*} type referenced by
     *         {@code internalName} (an ASM internal name or descriptor), or {@code null} if the
     *         reference is to a non-Kafka type, a primitive, or unparseable.
     */
    private static String resolveInternalKafkaReference(String internalName) {
        if (internalName == null) {
            return null;
        }
        String trimmed = stripDescriptor(internalName);
        if (trimmed == null || !trimmed.startsWith(KAFKA_INTERNAL_PREFIX)) {
            return null;
        }
        return trimmed.replace('/', '.');
    }

    /** Render the consumer-side location as {@code Class#member (line N)}, omitting absent parts. */
    private static String formatLocation(String consumerClass, String memberName, int line) {
        StringBuilder sb = new StringBuilder(consumerClass);
        if (memberName != null) {
            sb.append('#').append(memberName);
        }
        if (line > 0) {
            sb.append(" (line ").append(line).append(')');
        }
        return sb.toString();
    }

    /** Stable de-dup key so the same call-site reported via multiple visitor callbacks collapses to one entry. */
    private static String referenceKey(String consumerClass, String binaryName, String memberName, int line) {
        return consumerClass + "|" + binaryName + "|" + (memberName == null ? "" : memberName) + "|" + line;
    }

    private static void recordSuppression(Map<String, PublicApiViolation> suppressions,
                                          String key, String consumerClass, String binaryName,
                                          String memberName, String location, String reason) {
        boolean noReason = reason.isEmpty();
        String prettyReason = noReason ? NO_REASON_GIVEN : reason;
        LOG.info("Suppressed internal-API reference to {} from {}: {}", binaryName, location, prettyReason);
        String description = String.format(
                "Suppressed reference to internal Kafka class %s from %s — reason: %s",
                binaryName, location, prettyReason);
        // className is the offender (consumer) so the report's `Summary by Class` groups by the
        // file a developer needs to open — the leaked target lives in the description.
        suppressions.putIfAbsent(key,
                new PublicApiViolation(consumerClass, "SUPPRESSED_INTERNAL_API_USAGE", description, memberName, noReason));
    }

    private static void recordViolation(Map<String, PublicApiViolation> violations,
                                        String key, String consumerClass, String binaryName,
                                        String memberName, String location) {
        String description = String.format(
                "Bytecode reference to internal Kafka class %s from %s",
                binaryName, location);
        violations.putIfAbsent(key,
                new PublicApiViolation(consumerClass, "INTERNAL_API_USAGE", description, memberName));
    }

    /**
     * Return the ASM internal name for a reference-typed {@link Type} (OBJECT or ARRAY), or
     * {@code null} for VOID/primitive types. {@link Type#getInternalName()} is only documented
     * for reference types — calling it on a primitive happens to produce a single-char descriptor
     * ("V"/"I"/...) today which the prefix gate happens to reject, but that's undefined behaviour
     * we shouldn't depend on.
     */
    private static String referenceInternalName(Type type) {
        int sort = type.getSort();
        return (sort == Type.OBJECT || sort == Type.ARRAY) ? type.getInternalName() : null;
    }

    private static String stripDescriptor(String name) {
        if (name == null || name.isEmpty()) {
            return null;
        }
        int i = 0;
        while (i < name.length() && name.charAt(i) == '[') {
            i++;
        }
        if (i >= name.length()) {
            return null;
        }
        char c = name.charAt(i);
        if (c == 'L' && name.endsWith(";")) {
            return name.substring(i + 1, name.length() - 1);
        }
        // Primitive descriptor (I, J, Z, ...) — nothing internal to record.
        if (i > 0 || "VZBSCIJFD".indexOf(c) >= 0) {
            return null;
        }
        return name;
    }

    /**
     * Buffered reference. Header references (class super/interfaces, method return/param types,
     * field type) are buffered because the {@code @SuppressKafkaInternalApiUsage} annotation that
     * may legitimise them is visited <em>after</em> the header. Body-instruction references can be
     * recorded immediately because annotations on a method/field are visited before the body.
     */
    private static final class PendingReference {
        final String internalName;
        final String memberName;
        final int line;
        PendingReference(String internalName, String memberName, int line) {
            this.internalName = internalName;
            this.memberName = memberName;
            this.line = line;
        }
    }

    /** Visits a class and records every referenced type, honouring {@code @SuppressKafkaInternalApiUsage}. */
    private final class ReferenceCollectingClassVisitor extends ClassVisitor {
        private final Map<String, PublicApiViolation> violations;
        private final Map<String, PublicApiViolation> suppressions;
        private String currentClass;
        private String classSuppression; // null = none; otherwise the reason (may be empty string)
        private final List<PendingReference> headerRefs = new ArrayList<>();

        ReferenceCollectingClassVisitor(Map<String, PublicApiViolation> violations,
                                        Map<String, PublicApiViolation> suppressions) {
            super(ASM_API);
            this.violations = violations;
            this.suppressions = suppressions;
        }

        /**
         * Class header — superclass + interface list + generic signature. Caught here so a consumer
         * that {@code extends} or {@code implements} an internal Kafka type is flagged even if its
         * body never names the type. Generics ({@code class C<T> extends Foo<Internal>}) hide
         * inside the signature string and are pulled out via {@link SignatureReader}.
         */
        @Override
        public void visit(int version, int access, String name, String signature,
                          String superName, String[] interfaces) {
            this.currentClass = name == null ? "<unknown>" : name.replace('/', '.');
            if (superName != null) {
                headerRefs.add(new PendingReference(superName, null, -1));
            }
            if (interfaces != null) {
                for (String iface : interfaces) {
                    headerRefs.add(new PendingReference(iface, null, -1));
                }
            }
            collectSignatureRefs(signature, null, -1, headerRefs);
        }

        /**
         * Class-level annotations. Two jobs: (a) capture the reason on
         * {@code @SuppressKafkaInternalApiUsage} so header refs can be silenced, and (b) treat the
         * annotation's own type as a reference — an {@code @InternalAnnotation} on a consumer class
         * is still a reference into Kafka internals.
         */
        @Override
        public AnnotationVisitor visitAnnotation(String descriptor, boolean visible) {
            if (SUPPRESS_DESCRIPTOR.equals(descriptor)) {
                return new ReasonCaptureVisitor(r -> classSuppression = r);
            }
            headerRefs.add(new PendingReference(stripDescriptor(descriptor), null, -1));
            return null;
        }

        /**
         * Field declarations. The field's declared type (descriptor) and its full generic form
         * (signature) can both name internal types — e.g. {@code private InternalCache cache} or
         * {@code Map<String, InternalFoo> data}. Refs are buffered so a field-level
         * {@code @SuppressKafkaInternalApiUsage} (visited after the declaration) can suppress them.
         */
        @Override
        public FieldVisitor visitField(int access, String name, String descriptor,
                                       String signature, Object value) {
            return new FieldVisitor(ASM_API) {
                private String fieldSuppression;
                private final List<PendingReference> fieldRefs = new ArrayList<>();
                {
                    fieldRefs.add(new PendingReference(stripDescriptor(descriptor), name, -1));
                    collectSignatureRefs(signature, name, -1, fieldRefs);
                }
                @Override
                public AnnotationVisitor visitAnnotation(String d, boolean v) {
                    if (SUPPRESS_DESCRIPTOR.equals(d)) {
                        return new ReasonCaptureVisitor(r -> fieldSuppression = r);
                    }
                    fieldRefs.add(new PendingReference(stripDescriptor(d), name, -1));
                    return null;
                }
                @Override
                public void visitEnd() {
                    String reason = effective(fieldSuppression);
                    flush(fieldRefs, reason);
                }
            };
        }

        /**
         * Per-method delegate. Each method's body is walked by a
         * {@link ReferenceCollectingMethodVisitor} that records every kind of bytecode-level
         * reference (see its own javadoc).
         */
        @Override
        public MethodVisitor visitMethod(int access, String name, String descriptor,
                                         String signature, String[] exceptions) {
            return new ReferenceCollectingMethodVisitor(name, descriptor, signature, exceptions);
        }

        /**
         * End of class. Flushes the buffered header refs (superclass / interfaces / generic
         * signature / non-suppress annotations) now that the class-level
         * {@code @SuppressKafkaInternalApiUsage} reason — which is visited <em>after</em> the
         * header in ASM's callback order — is finally known.
         */
        @Override
        public void visitEnd() {
            flush(headerRefs, classSuppression);
        }

        /** {@code memberReason} takes precedence; falls back to class-level. */
        String effective(String memberReason) {
            return memberReason != null ? memberReason : classSuppression;
        }

        void flush(List<PendingReference> refs, String reason) {
            for (PendingReference r : refs) {
                recordIfInternal(r.internalName, currentClass, r.memberName, r.line, reason, violations, suppressions);
            }
        }

        private void collectSignatureRefs(String signature, String member, int line, List<PendingReference> out) {
            if (signature == null) {
                return;
            }
            new SignatureReader(signature).accept(new SignatureVisitor(ASM_API) {
                @Override
                public void visitClassType(String name) {
                    out.add(new PendingReference(name, member, line));
                }
            });
        }

        /** Records type references encountered in method bodies; honours method-level + class-level suppression. */
        private final class ReferenceCollectingMethodVisitor extends MethodVisitor {
            private final String methodName;
            private final List<PendingReference> headerBuffer = new ArrayList<>();
            private int currentLine = -1;
            private String methodSuppression;
            private boolean codeStarted;

            ReferenceCollectingMethodVisitor(String name, String descriptor, String signature, String[] exceptions) {
                super(ASM_API);
                this.methodName = name;
                Type methodType = Type.getMethodType(descriptor);
                headerBuffer.add(new PendingReference(referenceInternalName(methodType.getReturnType()), name, -1));
                for (Type arg : methodType.getArgumentTypes()) {
                    headerBuffer.add(new PendingReference(referenceInternalName(arg), name, -1));
                }
                if (exceptions != null) {
                    for (String ex : exceptions) {
                        headerBuffer.add(new PendingReference(ex, name, -1));
                    }
                }
                collectSignatureRefs(signature, name, -1, headerBuffer);
            }

            /**
             * Method-level annotations. {@code @SuppressKafkaInternalApiUsage} captures the reason
             * to silence both header refs (return/param/exception types) and body refs in this
             * method. All other annotation types are themselves recorded as references — an
             * annotation IS a class reference, even if it's never used elsewhere.
             */
            @Override
            public AnnotationVisitor visitAnnotation(String d, boolean v) {
                if (SUPPRESS_DESCRIPTOR.equals(d)) {
                    return new ReasonCaptureVisitor(r -> methodSuppression = r);
                }
                headerBuffer.add(new PendingReference(stripDescriptor(d), methodName, -1));
                return null;
            }

            /**
             * Marker fired when ASM transitions from method header to method body. All method-level
             * annotations have already been visited by this point, so {@link #methodSuppression} is
             * stable — header refs (return type / params / declared exceptions / generic signature
             * / non-suppress annotations) are flushed now with the correct effective reason.
             */
            @Override
            public void visitCode() {
                if (!codeStarted) {
                    flush(headerBuffer, effective(methodSuppression));
                    headerBuffer.clear();
                    codeStarted = true;
                }
            }

            /**
             * Source line marker from the {@code LineNumberTable} debug attribute. Only fires for
             * classes compiled with {@code javac -g} (the default). Carries no class reference of
             * its own — we just track the current line so violations can be reported as
             * {@code ConsumerClass#method (line N)}. Stays at -1 when debug info is stripped.
             */
            @Override
            public void visitLineNumber(int line, org.objectweb.asm.Label start) {
                this.currentLine = line;
            }

            /**
             * Type-as-operand instructions: {@code NEW}, {@code ANEWARRAY}, {@code CHECKCAST},
             * {@code INSTANCEOF}. The operand is the class being instantiated, cast to, or tested —
             * exactly the cases where a consumer reaches an internal type without naming it via a
             * method call or field access.
             */
            @Override
            public void visitTypeInsn(int opcode, String type) {
                recordBody(type);
            }

            /**
             * Field-access instructions: {@code GETFIELD}, {@code PUTFIELD}, {@code GETSTATIC},
             * {@code PUTSTATIC}. Both the field's <em>owner</em> (the declaring class) and its
             * <em>type</em> (descriptor) can name internal Kafka classes — e.g. reading
             * {@code InternalClass.CONSTANT} (owner is internal) or writing to a field whose type
             * is internal.
             */
            @Override
            public void visitFieldInsn(int opcode, String owner, String name, String descriptor) {
                recordBody(owner);
                recordBody(stripDescriptor(descriptor));
            }

            /**
             * Method-invocation instructions: {@code INVOKEVIRTUAL}, {@code INVOKESPECIAL},
             * {@code INVOKESTATIC}, {@code INVOKEINTERFACE}. Three reference slots per call site:
             * the owner (declaring class), the return type, and each argument type. Catches both
             * "calls into an internal class" and "passes an internal type as an argument or
             * receives one as a return".
             */
            @Override
            public void visitMethodInsn(int opcode, String owner, String name,
                                        String descriptor, boolean isInterface) {
                recordBody(owner);
                Type methodType = Type.getMethodType(descriptor);
                recordBody(referenceInternalName(methodType.getReturnType()));
                for (Type arg : methodType.getArgumentTypes()) {
                    recordBody(referenceInternalName(arg));
                }
            }

            /**
             * {@code INVOKEDYNAMIC}. Emitted by {@code javac} for lambdas, method references, and
             * {@code String} concatenation since Java 9. Only the call-site descriptor — return
             * type and argument types — is walked. The bootstrap method handle itself is not
             * followed, because the LambdaMetafactory machinery is JDK-owned and the user-visible
             * references show up via the descriptor anyway.
             */
            @Override
            public void visitInvokeDynamicInsn(String name, String descriptor,
                                               Handle bootstrapMethodHandle, Object... bootstrapMethodArguments) {
                Type methodType = Type.getMethodType(descriptor);
                recordBody(referenceInternalName(methodType.getReturnType()));
                for (Type arg : methodType.getArgumentTypes()) {
                    recordBody(referenceInternalName(arg));
                }
            }

            /**
             * {@code LDC} loads a constant onto the operand stack — int / long / float / double /
             * String / and, relevantly here, {@code Class} literals such as
             * {@code InternalClass.class}. Only the {@link Type} case is interesting; primitive
             * and string constants carry no class reference.
             */
            @Override
            public void visitLdcInsn(Object value) {
                if (value instanceof Type) {
                    Type t = (Type) value;
                    if (t.getSort() == Type.OBJECT || t.getSort() == Type.ARRAY) {
                        recordBody(t.getInternalName());
                    }
                }
            }

            /**
             * {@code MULTIANEWARRAY} for multi-dimensional arrays
             * ({@code new InternalClass[3][3]}). Single-dimensional array allocation goes through
             * {@code ANEWARRAY} which is handled by {@link #visitTypeInsn}.
             */
            @Override
            public void visitMultiANewArrayInsn(String descriptor, int numDimensions) {
                recordBody(stripDescriptor(descriptor));
            }

            /**
             * Exception-handler table entries: {@code catch (InternalKafkaException e)}. The
             * {@code type} is the internal name of the caught exception (or {@code null} for a
             * {@code finally} block, which has no exception type to record).
             */
            @Override
            public void visitTryCatchBlock(org.objectweb.asm.Label start, org.objectweb.asm.Label end,
                                           org.objectweb.asm.Label handler, String type) {
                if (type != null) {
                    recordBody(type);
                }
            }

            /**
             * End of method. {@link #visitCode} is never called for abstract / native methods —
             * they have no body — so their header refs would otherwise leak unflushed. This safety
             * net guarantees every method's return/param/exception types are still audited.
             */
            @Override
            public void visitEnd() {
                if (!codeStarted) {
                    flush(headerBuffer, effective(methodSuppression));
                    headerBuffer.clear();
                }
            }

            private void recordBody(String internalName) {
                recordIfInternal(internalName, currentClass, methodName, currentLine,
                        effective(methodSuppression), violations, suppressions);
            }
        }
    }
}
