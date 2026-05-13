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
 */
public class BytecodeApiUsageScanner {
    private static final Logger logger = LoggerFactory.getLogger(BytecodeApiUsageScanner.class);
    private static final int ASM_API = Opcodes.ASM9;

    /** Internal-form prefix (slashes) for any class we care about checking the audience of. */
    private static final String KAFKA_INTERNAL_PREFIX = "org/apache/kafka/";
    /** Descriptor of {@code @SuppressKafkaInternalApiUsage} — honoured when present on the enclosing class or member. */
    private static final String SUPPRESS_DESCRIPTOR =
            "Lorg/apache/kafka/common/annotation/SuppressKafkaInternalApiUsage;";
    private static final String NO_REASON_GIVEN = "(no reason given)";

    private final Predicate<String> isPublicApi;

    /**
     * @param isPublicApi callback that returns {@code true} when the given binary class name
     *                    (e.g. {@code org.apache.kafka.clients.producer.KafkaProducer}) is part
     *                    of the public API surface
     */
    public BytecodeApiUsageScanner(Predicate<String> isPublicApi) {
        this.isPublicApi = isPublicApi;
    }

    /**
     * Scan every {@code .class} entry reachable from the supplied roots. Each root may be a
     * directory of class files, an individual .class file, or a .jar archive.
     */
    public ScanResult scan(List<File> roots) throws IOException {
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
        return new ScanResult(new ArrayList<>(violations.values()),
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
        if (internalName == null) {
            return;
        }
        // Strip array prefixes ('[' for arrays, 'L' / ';' for object descriptors).
        String trimmed = stripDescriptor(internalName);
        if (trimmed == null || !trimmed.startsWith(KAFKA_INTERNAL_PREFIX)) {
            return;
        }
        String binaryName = trimmed.replace('/', '.');
        // Skip references to inner/nested types -- the outer type covers them and has the audience marker.
        String outerName = binaryName.contains("$") ? binaryName.substring(0, binaryName.indexOf('$')) : binaryName;
        if (isPublicApi.test(outerName)) {
            return;
        }
        // Don't report classes flagging references to themselves.
        if (outerName.equals(consumerClass) || binaryName.equals(consumerClass)) {
            return;
        }
        String locationSuffix = (memberName != null ? "#" + memberName : "")
                + (line > 0 ? " (line " + line + ")" : "");
        if (suppressionReason != null) {
            String reasonForLog = suppressionReason.isEmpty() ? NO_REASON_GIVEN : suppressionReason;
            logger.info("Suppressed internal-API reference to {} from {}{}: {}",
                    binaryName, consumerClass, locationSuffix, reasonForLog);
            String description = String.format("Suppressed reference to internal Kafka class %s from %s%s — reason: %s",
                    binaryName, consumerClass, locationSuffix, reasonForLog);
            String key = consumerClass + "|" + binaryName + "|" + (memberName == null ? "" : memberName) + "|" + line;
            suppressions.putIfAbsent(key,
                    new PublicApiViolation(binaryName, "SUPPRESSED_INTERNAL_API_USAGE", description, memberName));
            return;
        }
        String description = String.format("Bytecode reference to internal Kafka class %s from %s%s",
                binaryName, consumerClass, locationSuffix);
        String key = consumerClass + "|" + binaryName + "|" + (memberName == null ? "" : memberName) + "|" + line;
        violations.putIfAbsent(key, new PublicApiViolation(binaryName, "INTERNAL_API_USAGE", description, memberName));
    }

    /** Convert any of: {@code Lorg/apache/kafka/Foo;}, {@code [Lorg/apache/kafka/Foo;}, {@code org/apache/kafka/Foo} to the bare internal form. */
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

    /** Captures the {@code value()} of a {@code @SuppressKafkaInternalApiUsage} annotation. */
    private static final class ReasonCaptureVisitor extends AnnotationVisitor {
        private final java.util.function.Consumer<String> setter;
        private boolean assigned;
        ReasonCaptureVisitor(java.util.function.Consumer<String> setter) {
            super(ASM_API);
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
            if (!assigned) {
                // Annotation is present but value() was omitted -- treat as suppressed with empty reason.
                setter.accept("");
            }
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

        @Override
        public AnnotationVisitor visitAnnotation(String descriptor, boolean visible) {
            if (SUPPRESS_DESCRIPTOR.equals(descriptor)) {
                return new ReasonCaptureVisitor(r -> classSuppression = r);
            }
            // The annotation's own type is a reference; record it (header refs aren't suppressed by other
            // annotations on this class, so the class-level suppression is the only thing in scope).
            headerRefs.add(new PendingReference(stripDescriptor(descriptor), null, -1));
            return null;
        }

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

        @Override
        public MethodVisitor visitMethod(int access, String name, String descriptor,
                                         String signature, String[] exceptions) {
            return new ReferenceCollectingMethodVisitor(name, descriptor, signature, exceptions);
        }

        @Override
        public void visitEnd() {
            // Header refs were buffered to allow class-level @SuppressKafkaInternalApiUsage (visited after
            // the class header) to suppress them.
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
                headerBuffer.add(new PendingReference(methodType.getReturnType().getInternalName(), name, -1));
                for (Type arg : methodType.getArgumentTypes()) {
                    headerBuffer.add(new PendingReference(arg.getInternalName(), name, -1));
                }
                if (exceptions != null) {
                    for (String ex : exceptions) {
                        headerBuffer.add(new PendingReference(ex, name, -1));
                    }
                }
                collectSignatureRefs(signature, name, -1, headerBuffer);
            }

            @Override
            public AnnotationVisitor visitAnnotation(String d, boolean v) {
                if (SUPPRESS_DESCRIPTOR.equals(d)) {
                    return new ReasonCaptureVisitor(r -> methodSuppression = r);
                }
                headerBuffer.add(new PendingReference(stripDescriptor(d), methodName, -1));
                return null;
            }

            @Override
            public void visitCode() {
                // Method-level annotations are visited before visitCode -- methodSuppression is now stable.
                // Flush header refs here so body instructions don't have to revisit them later.
                if (!codeStarted) {
                    flush(headerBuffer, effective(methodSuppression));
                    headerBuffer.clear();
                    codeStarted = true;
                }
            }

            @Override
            public void visitLineNumber(int line, org.objectweb.asm.Label start) {
                this.currentLine = line;
            }

            @Override
            public void visitTypeInsn(int opcode, String type) {
                recordBody(type);
            }

            @Override
            public void visitFieldInsn(int opcode, String owner, String name, String descriptor) {
                recordBody(owner);
                recordBody(stripDescriptor(descriptor));
            }

            @Override
            public void visitMethodInsn(int opcode, String owner, String name,
                                        String descriptor, boolean isInterface) {
                recordBody(owner);
                Type methodType = Type.getMethodType(descriptor);
                recordBody(methodType.getReturnType().getInternalName());
                for (Type arg : methodType.getArgumentTypes()) {
                    recordBody(arg.getInternalName());
                }
            }

            @Override
            public void visitInvokeDynamicInsn(String name, String descriptor,
                                               Handle bootstrapMethodHandle, Object... bootstrapMethodArguments) {
                Type methodType = Type.getMethodType(descriptor);
                recordBody(methodType.getReturnType().getInternalName());
                for (Type arg : methodType.getArgumentTypes()) {
                    recordBody(arg.getInternalName());
                }
            }

            @Override
            public void visitLdcInsn(Object value) {
                if (value instanceof Type) {
                    Type t = (Type) value;
                    if (t.getSort() == Type.OBJECT || t.getSort() == Type.ARRAY) {
                        recordBody(t.getInternalName());
                    }
                }
            }

            @Override
            public void visitMultiANewArrayInsn(String descriptor, int numDimensions) {
                recordBody(stripDescriptor(descriptor));
            }

            @Override
            public void visitEnd() {
                // Abstract / native methods have no body; visitCode is never called for them. Flush here as
                // a safety net so their header references still emit.
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
