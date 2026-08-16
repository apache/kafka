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
import org.objectweb.asm.AnnotationVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PluginDeveloperApiUsageScannerTest {

    @TempDir
    Path tempDir;

    @Test
    void scan_noRoots_returnsEmpty() throws IOException {
        PluginDeveloperApiUsageScanner scanner = new PluginDeveloperApiUsageScanner(always(true));
        CheckResult result = scanner.scan(Collections.emptyList());
        assertTrue(result.violations().isEmpty());
        assertTrue(result.suppressions().isEmpty());
    }

    @Test
    void scan_consumerReferencesPublicApiClass_returnsNoViolations() throws IOException {
        File classFile = writeClassFile("com/example/PublicConsumer",
                generateConsumerReferencing("com/example/PublicConsumer", "org/apache/kafka/clients/producer/KafkaProducer"));

        Predicate<String> isPublic = name -> "org.apache.kafka.clients.producer.KafkaProducer".equals(name);
        PluginDeveloperApiUsageScanner scanner = new PluginDeveloperApiUsageScanner(isPublic);
        List<PublicApiViolation> violations = scanner.scan(List.of(classFile.getParentFile())).violations();

        assertTrue(violations.isEmpty(),
                "Reference to an @InterfaceAudience.Public class must not be reported, but got: " + violations);
    }

    @Test
    void scan_consumerReferencesInternalApiClass_returnsViolation() throws IOException {
        File classFile = writeClassFile("com/example/InternalConsumer",
                generateConsumerReferencing("com/example/InternalConsumer", "org/apache/kafka/internals/SecretCabal"));

        PluginDeveloperApiUsageScanner scanner = new PluginDeveloperApiUsageScanner(always(false));
        List<PublicApiViolation> violations = scanner.scan(List.of(classFile.getParentFile())).violations();

        assertFalse(violations.isEmpty(), "Reference to an internal Kafka class must be reported");
        PublicApiViolation v = violations.get(0);
        assertEquals("INTERNAL_API_USAGE", v.getViolationType());
        // className is the consumer (offender) so the report's `Summary by Class` groups by the
        // file a developer needs to open; the leaked target lives in the description.
        assertEquals("com.example.InternalConsumer", v.getClassName());
        assertTrue(v.getDescription().contains("org.apache.kafka.internals.SecretCabal"),
                "violation description must name the leaked internal class. got: " + v.getDescription());
    }

    @Test
    void scan_ignoresNonKafkaReferences() throws IOException {
        File classFile = writeClassFile("com/example/JdkConsumer",
                generateConsumerReferencing("com/example/JdkConsumer", "java/util/HashMap"));

        PluginDeveloperApiUsageScanner scanner = new PluginDeveloperApiUsageScanner(always(false));
        List<PublicApiViolation> violations = scanner.scan(List.of(classFile.getParentFile())).violations();

        assertTrue(violations.isEmpty(),
                "References to non-Kafka classes (e.g. JDK) must not be reported, got: " + violations);
    }

    @Test
    void scan_classesPackagedInJar_areScanned() throws IOException {
        byte[] internalBytes = generateConsumerReferencing(
                "com/example/JarConsumer", "org/apache/kafka/internals/Hidden");
        File jar = tempDir.resolve("consumer.jar").toFile();
        try (JarOutputStream jos = new JarOutputStream(new FileOutputStream(jar))) {
            jos.putNextEntry(new JarEntry("com/example/JarConsumer.class"));
            jos.write(internalBytes);
            jos.closeEntry();
        }

        PluginDeveloperApiUsageScanner scanner = new PluginDeveloperApiUsageScanner(always(false));
        List<PublicApiViolation> violations = scanner.scan(List.of(jar)).violations();

        assertFalse(violations.isEmpty(), "scan of jar should find the internal reference");
        assertEquals("com.example.JarConsumer", violations.get(0).getClassName());
    }

    @Test
    void scan_classWithKafkaFieldOfInternalType_returnsViolation() throws IOException {
        // Different bytecode shape: a field whose type is an internal Kafka class.
        ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_FRAMES | ClassWriter.COMPUTE_MAXS);
        cw.visit(Opcodes.V11, Opcodes.ACC_PUBLIC, "com/example/FieldHolder", null, "java/lang/Object", null);
        cw.visitField(Opcodes.ACC_PRIVATE, "secret", "Lorg/apache/kafka/internals/Hidden;", null, null).visitEnd();
        // default ctor
        MethodVisitor mv = cw.visitMethod(Opcodes.ACC_PUBLIC, "<init>", "()V", null, null);
        mv.visitCode();
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        mv.visitMethodInsn(Opcodes.INVOKESPECIAL, "java/lang/Object", "<init>", "()V", false);
        mv.visitInsn(Opcodes.RETURN);
        mv.visitMaxs(0, 0);
        mv.visitEnd();
        cw.visitEnd();
        File classFile = writeClassFile("com/example/FieldHolder", cw.toByteArray());

        PluginDeveloperApiUsageScanner scanner = new PluginDeveloperApiUsageScanner(always(false));
        List<PublicApiViolation> violations = scanner.scan(List.of(classFile.getParentFile())).violations();

        assertTrue(violations.stream().anyMatch(v -> "com.example.FieldHolder".equals(v.getClassName())),
                "field-type reference must be reported, got: " + violations);
    }

    @Test
    void scan_classLevelSuppressionAnnotation_skipsViolations() throws IOException {
        byte[] bytes = generateConsumerReferencing(
                "com/example/SuppressedClass", "org/apache/kafka/internals/Hidden",
                ClassAnnotation.suppress("ports legacy adapter; tracked in JIRA-1234"),
                MethodAnnotations.none());
        File classFile = writeClassFile("com/example/SuppressedClass", bytes);

        PluginDeveloperApiUsageScanner scanner = new PluginDeveloperApiUsageScanner(always(false));
        CheckResult result = scanner.scan(List.of(classFile.getParentFile()));

        assertTrue(result.violations().isEmpty(),
                "Class-level @SuppressKafkaInternalApiUsage must suppress all violations; got: " + result.violations());
        assertFalse(result.suppressions().isEmpty(),
                "suppressions list must record the skipped reference so it shows in the report");
        PublicApiViolation s = result.suppressions().get(0);
        assertEquals("SUPPRESSED_INTERNAL_API_USAGE", s.getViolationType());
        assertTrue(s.getDescription().contains("ports legacy adapter; tracked in JIRA-1234"),
                "suppression description must carry the annotation's reason; got: " + s.getDescription());
    }

    @Test
    void scan_methodLevelSuppression_skipsOnlyThatMethod() throws IOException {
        byte[] bytes = generateConsumerWithTwoMethods(
                "com/example/PartiallySuppressed",
                "org/apache/kafka/internals/Hidden",
                MethodAnnotations.suppressOn("useIt", "intentional fallback"));
        File classFile = writeClassFile("com/example/PartiallySuppressed", bytes);

        PluginDeveloperApiUsageScanner scanner = new PluginDeveloperApiUsageScanner(always(false));
        List<PublicApiViolation> violations = scanner.scan(List.of(classFile.getParentFile())).violations();

        // The other method ("alsoUseIt") is not suppressed -- it must still report.
        assertEquals(1, violations.size(),
                "method-level suppression must only affect that method; got: " + violations);
        assertEquals("alsoUseIt", violations.get(0).getMemberName(),
                "unsuppressed method should be the one reported");
    }

    @Test
    void scan_suppressionWithoutReason_stillSuppresses() throws IOException {
        byte[] bytes = generateConsumerReferencing(
                "com/example/SuppressedNoReason", "org/apache/kafka/internals/Hidden",
                ClassAnnotation.suppress(null),  // annotation present, no value()
                MethodAnnotations.none());
        File classFile = writeClassFile("com/example/SuppressedNoReason", bytes);

        PluginDeveloperApiUsageScanner scanner = new PluginDeveloperApiUsageScanner(always(false));
        List<PublicApiViolation> violations = scanner.scan(List.of(classFile.getParentFile())).violations();

        assertTrue(violations.isEmpty(),
                "@SuppressKafkaInternalApiUsage with no reason must still suppress; got: " + violations);
    }

    @Test
    void scan_nestedPrivateOverride_isFlagged_evenWhenOuterIsPublic() throws IOException {
        // KIP-1265: a nested class explicitly marked @InterfaceAudience.Private must override an
        // inherited @Public from its outer. The predicate is given the full nested name; a Private
        // override must propagate through it, so a reference to org/apache/kafka/Outer$Inner is a
        // violation even though Outer alone is Public.
        File classFile = writeClassFile("com/example/NestedConsumer",
                generateConsumerReferencing("com/example/NestedConsumer", "org/apache/kafka/Outer$Inner"));

        // Outer is public, but the nested Outer$Inner is explicitly Private.
        Predicate<String> audience = name -> "org.apache.kafka.Outer".equals(name);
        PluginDeveloperApiUsageScanner scanner = new PluginDeveloperApiUsageScanner(audience);
        List<PublicApiViolation> violations = scanner.scan(List.of(classFile.getParentFile())).violations();

        assertFalse(violations.isEmpty(),
                "Reference to a Private-nested type must be flagged even when its outer is Public; got: " + violations);
        assertEquals("com.example.NestedConsumer", violations.get(0).getClassName());
        assertTrue(violations.get(0).getDescription().contains("org.apache.kafka.Outer$Inner"),
                "description must carry the leaked nested type; got: " + violations.get(0).getDescription());
    }

    @Test
    void scan_tryCatchOnInternalException_isFlagged() throws IOException {
        // `catch (InternalKafkaException ignored)` where the variable is never used leaves the
        // only reference to the exception type in the exception-handler table — visitTryCatchBlock.
        // Without that visitor override the scanner would miss it.
        ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_FRAMES | ClassWriter.COMPUTE_MAXS);
        cw.visit(Opcodes.V11, Opcodes.ACC_PUBLIC, "com/example/CatchConsumer", null, "java/lang/Object", null);
        MethodVisitor ctor = cw.visitMethod(Opcodes.ACC_PUBLIC, "<init>", "()V", null, null);
        ctor.visitCode();
        ctor.visitVarInsn(Opcodes.ALOAD, 0);
        ctor.visitMethodInsn(Opcodes.INVOKESPECIAL, "java/lang/Object", "<init>", "()V", false);
        ctor.visitInsn(Opcodes.RETURN);
        ctor.visitMaxs(0, 0);
        ctor.visitEnd();

        MethodVisitor mv = cw.visitMethod(Opcodes.ACC_PUBLIC, "doIt", "()V", null, null);
        mv.visitCode();
        org.objectweb.asm.Label start = new org.objectweb.asm.Label();
        org.objectweb.asm.Label end = new org.objectweb.asm.Label();
        org.objectweb.asm.Label handler = new org.objectweb.asm.Label();
        org.objectweb.asm.Label after = new org.objectweb.asm.Label();
        mv.visitTryCatchBlock(start, end, handler, "org/apache/kafka/internals/Boom");
        mv.visitLabel(start);
        // empty try body
        mv.visitLabel(end);
        mv.visitJumpInsn(Opcodes.GOTO, after);
        mv.visitLabel(handler);
        mv.visitInsn(Opcodes.POP);
        mv.visitLabel(after);
        mv.visitInsn(Opcodes.RETURN);
        mv.visitMaxs(0, 0);
        mv.visitEnd();
        cw.visitEnd();

        File classFile = writeClassFile("com/example/CatchConsumer", cw.toByteArray());
        PluginDeveloperApiUsageScanner scanner = new PluginDeveloperApiUsageScanner(always(false));
        List<PublicApiViolation> violations = scanner.scan(List.of(classFile.getParentFile())).violations();

        assertTrue(violations.stream().anyMatch(v -> "com.example.CatchConsumer".equals(v.getClassName())),
                "exception-table entry must be reported; got: " + violations);
    }

    @Test
    void scan_methodHeaderRefBuffering_returnTypeFlushedAtVisitCode() throws IOException {
        // The method-header ref (return type from the descriptor) is buffered until
        // visitCode fires — that's when the method-level @SuppressKafkaInternalApiUsage has
        // been visited and we know the effective reason. A non-suppressed method must still
        // produce the violation.
        byte[] bytes = generateMethodWithInternalReturnType("com/example/HeaderRef",
                "fetch", "Lorg/apache/kafka/internals/Hidden;", null);
        File classFile = writeClassFile("com/example/HeaderRef", bytes);

        PluginDeveloperApiUsageScanner scanner = new PluginDeveloperApiUsageScanner(always(false));
        List<PublicApiViolation> violations = scanner.scan(List.of(classFile.getParentFile())).violations();

        assertTrue(violations.stream().anyMatch(v -> "com.example.HeaderRef".equals(v.getClassName())),
                "method-header return-type ref must flush as a violation; got: " + violations);
    }

    @Test
    void scan_abstractMethodHeaderRef_flushedAtVisitEnd() throws IOException {
        // Abstract methods have no body, so ASM never fires visitCode. The scanner's visitEnd
        // safety net must flush the buffered header refs anyway, otherwise an abstract method's
        // internal return/param/exception types would silently slip through.
        ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_FRAMES | ClassWriter.COMPUTE_MAXS);
        cw.visit(Opcodes.V11, Opcodes.ACC_PUBLIC | Opcodes.ACC_ABSTRACT,
                "com/example/AbstractConsumer", null, "java/lang/Object", null);

        MethodVisitor ctor = cw.visitMethod(Opcodes.ACC_PUBLIC, "<init>", "()V", null, null);
        ctor.visitCode();
        ctor.visitVarInsn(Opcodes.ALOAD, 0);
        ctor.visitMethodInsn(Opcodes.INVOKESPECIAL, "java/lang/Object", "<init>", "()V", false);
        ctor.visitInsn(Opcodes.RETURN);
        ctor.visitMaxs(0, 0);
        ctor.visitEnd();

        // public abstract Hidden fetch();  — no body, no visitCode, no visitMaxs.
        MethodVisitor mv = cw.visitMethod(Opcodes.ACC_PUBLIC | Opcodes.ACC_ABSTRACT,
                "fetch", "()Lorg/apache/kafka/internals/Hidden;", null, null);
        mv.visitEnd();
        cw.visitEnd();

        File classFile = writeClassFile("com/example/AbstractConsumer", cw.toByteArray());
        PluginDeveloperApiUsageScanner scanner = new PluginDeveloperApiUsageScanner(always(false));
        List<PublicApiViolation> violations = scanner.scan(List.of(classFile.getParentFile())).violations();

        assertTrue(violations.stream().anyMatch(v -> "com.example.AbstractConsumer".equals(v.getClassName())),
                "abstract-method header ref must be flushed at visitEnd; got: " + violations);
    }

    @Test
    void scan_methodHeaderRefBuffering_methodLevelSuppressDivertsToSuppressions() throws IOException {
        // Same method header as the previous test, but the method carries
        // @SuppressKafkaInternalApiUsage. visitCode flushes the buffered header refs using the
        // effective reason (method-level wins over class-level), so they land in suppressions.
        byte[] bytes = generateMethodWithInternalReturnType("com/example/SuppressedHeader",
                "fetch", "Lorg/apache/kafka/internals/Hidden;", "header-ref reason");
        File classFile = writeClassFile("com/example/SuppressedHeader", bytes);

        PluginDeveloperApiUsageScanner scanner = new PluginDeveloperApiUsageScanner(always(false));
        CheckResult r = scanner.scan(List.of(classFile.getParentFile()));

        assertTrue(r.violations().isEmpty(),
                "method-level suppress must divert the buffered header ref; got violations: " + r.violations());
        assertTrue(r.suppressions().stream().anyMatch(s -> s.getDescription().contains("reason: header-ref reason")),
                "suppression list must carry the method's reason; got: " + r.suppressions());
    }

    /**
     * Generate a class with a single method whose return type descriptor names an internal
     * Kafka type. If {@code suppressReason} is non-null, the method carries
     * {@code @SuppressKafkaInternalApiUsage(suppressReason)} (or no value when "").
     */
    private static byte[] generateMethodWithInternalReturnType(String consumerInternalName,
                                                               String methodName,
                                                               String returnDescriptor,
                                                               String suppressReason) {
        ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_FRAMES | ClassWriter.COMPUTE_MAXS);
        cw.visit(Opcodes.V11, Opcodes.ACC_PUBLIC, consumerInternalName, null, "java/lang/Object", null);

        MethodVisitor ctor = cw.visitMethod(Opcodes.ACC_PUBLIC, "<init>", "()V", null, null);
        ctor.visitCode();
        ctor.visitVarInsn(Opcodes.ALOAD, 0);
        ctor.visitMethodInsn(Opcodes.INVOKESPECIAL, "java/lang/Object", "<init>", "()V", false);
        ctor.visitInsn(Opcodes.RETURN);
        ctor.visitMaxs(0, 0);
        ctor.visitEnd();

        MethodVisitor mv = cw.visitMethod(Opcodes.ACC_PUBLIC, methodName, "()" + returnDescriptor, null, null);
        if (suppressReason != null) {
            AnnotationVisitor av = mv.visitAnnotation(SUPPRESS_DESC, true);
            if (!suppressReason.isEmpty()) {
                av.visit("value", suppressReason);
            }
            av.visitEnd();
        }
        mv.visitCode();  // triggers the header-ref flush in the scanner's visitor
        mv.visitInsn(Opcodes.ACONST_NULL);
        mv.visitInsn(Opcodes.ARETURN);
        mv.visitMaxs(0, 0);
        mv.visitEnd();
        cw.visitEnd();
        return cw.toByteArray();
    }

    /** Build a class with a default ctor and a method that loads a class constant of {@code internalNameOfReferenced}. */
    private static byte[] generateConsumerReferencing(String consumerInternalName, String internalNameOfReferenced) {
        ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_FRAMES | ClassWriter.COMPUTE_MAXS);
        cw.visit(Opcodes.V11, Opcodes.ACC_PUBLIC, consumerInternalName, null, "java/lang/Object", null);

        // default ctor
        MethodVisitor ctor = cw.visitMethod(Opcodes.ACC_PUBLIC, "<init>", "()V", null, null);
        ctor.visitCode();
        ctor.visitVarInsn(Opcodes.ALOAD, 0);
        ctor.visitMethodInsn(Opcodes.INVOKESPECIAL, "java/lang/Object", "<init>", "()V", false);
        ctor.visitInsn(Opcodes.RETURN);
        ctor.visitMaxs(0, 0);
        ctor.visitEnd();

        // public void useIt() { Class<?> c = <referenced>.class; }
        MethodVisitor mv = cw.visitMethod(Opcodes.ACC_PUBLIC, "useIt", "()V", null, null);
        mv.visitCode();
        mv.visitLdcInsn(org.objectweb.asm.Type.getObjectType(internalNameOfReferenced));
        mv.visitInsn(Opcodes.POP);
        mv.visitInsn(Opcodes.RETURN);
        mv.visitMaxs(0, 0);
        mv.visitEnd();

        cw.visitEnd();
        return cw.toByteArray();
    }

    private File writeClassFile(String internalName, byte[] bytes) throws IOException {
        File classFile = tempDir.resolve(internalName.replace('/', '_') + ".class").toFile();
        Files.write(classFile.toPath(), bytes);
        return classFile;
    }

    private static Predicate<String> always(boolean value) {
        return s -> value;
    }

    // --- suppression-aware test helpers -------------------------------------------------

    private static final String SUPPRESS_DESC =
            "Lorg/apache/kafka/common/annotation/SuppressKafkaInternalApiUsage;";

    /** Class-level annotation descriptor — empty = no class annotation. */
    private static final class ClassAnnotation {
        final String reason; // null reason => annotation present with no value(); empty marker => no annotation
        final boolean present;
        private ClassAnnotation(boolean present, String reason) {
            this.present = present;
            this.reason = reason;
        }
        static ClassAnnotation none() {
            return new ClassAnnotation(false, null);
        }
        static ClassAnnotation suppress(String reason) {
            return new ClassAnnotation(true, reason);
        }
    }

    /** Per-method suppression directives. */
    private static final class MethodAnnotations {
        final String suppressedMethod;
        final String reason;
        private MethodAnnotations(String suppressedMethod, String reason) {
            this.suppressedMethod = suppressedMethod;
            this.reason = reason;
        }
        static MethodAnnotations none() {
            return new MethodAnnotations(null, null);
        }
        static MethodAnnotations suppressOn(String methodName, String reason) {
            return new MethodAnnotations(methodName, reason);
        }
    }

    /** Overload that also writes class-level + method-level suppression annotations. */
    private static byte[] generateConsumerReferencing(String consumerInternalName,
                                                     String internalNameOfReferenced,
                                                     ClassAnnotation classAnnotation,
                                                     MethodAnnotations methodAnnotations) {
        ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_FRAMES | ClassWriter.COMPUTE_MAXS);
        cw.visit(Opcodes.V11, Opcodes.ACC_PUBLIC, consumerInternalName, null, "java/lang/Object", null);

        if (classAnnotation.present) {
            AnnotationVisitor av = cw.visitAnnotation(SUPPRESS_DESC, true);
            if (classAnnotation.reason != null) {
                av.visit("value", classAnnotation.reason);
            }
            av.visitEnd();
        }

        // default ctor
        MethodVisitor ctor = cw.visitMethod(Opcodes.ACC_PUBLIC, "<init>", "()V", null, null);
        ctor.visitCode();
        ctor.visitVarInsn(Opcodes.ALOAD, 0);
        ctor.visitMethodInsn(Opcodes.INVOKESPECIAL, "java/lang/Object", "<init>", "()V", false);
        ctor.visitInsn(Opcodes.RETURN);
        ctor.visitMaxs(0, 0);
        ctor.visitEnd();

        MethodVisitor mv = cw.visitMethod(Opcodes.ACC_PUBLIC, "useIt", "()V", null, null);
        if ("useIt".equals(methodAnnotations.suppressedMethod)) {
            AnnotationVisitor mav = mv.visitAnnotation(SUPPRESS_DESC, true);
            if (methodAnnotations.reason != null) {
                mav.visit("value", methodAnnotations.reason);
            }
            mav.visitEnd();
        }
        mv.visitCode();
        mv.visitLdcInsn(org.objectweb.asm.Type.getObjectType(internalNameOfReferenced));
        mv.visitInsn(Opcodes.POP);
        mv.visitInsn(Opcodes.RETURN);
        mv.visitMaxs(0, 0);
        mv.visitEnd();

        cw.visitEnd();
        return cw.toByteArray();
    }

    /** Two methods both referencing the same internal class, with a suppression on one. */
    private static byte[] generateConsumerWithTwoMethods(String consumerInternalName,
                                                        String internalNameOfReferenced,
                                                        MethodAnnotations methodAnnotations) {
        ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_FRAMES | ClassWriter.COMPUTE_MAXS);
        cw.visit(Opcodes.V11, Opcodes.ACC_PUBLIC, consumerInternalName, null, "java/lang/Object", null);

        // default ctor
        MethodVisitor ctor = cw.visitMethod(Opcodes.ACC_PUBLIC, "<init>", "()V", null, null);
        ctor.visitCode();
        ctor.visitVarInsn(Opcodes.ALOAD, 0);
        ctor.visitMethodInsn(Opcodes.INVOKESPECIAL, "java/lang/Object", "<init>", "()V", false);
        ctor.visitInsn(Opcodes.RETURN);
        ctor.visitMaxs(0, 0);
        ctor.visitEnd();

        for (String methodName : List.of("useIt", "alsoUseIt")) {
            MethodVisitor mv = cw.visitMethod(Opcodes.ACC_PUBLIC, methodName, "()V", null, null);
            if (methodName.equals(methodAnnotations.suppressedMethod)) {
                AnnotationVisitor mav = mv.visitAnnotation(SUPPRESS_DESC, true);
                if (methodAnnotations.reason != null) {
                    mav.visit("value", methodAnnotations.reason);
                }
                mav.visitEnd();
            }
            mv.visitCode();
            mv.visitLdcInsn(org.objectweb.asm.Type.getObjectType(internalNameOfReferenced));
            mv.visitInsn(Opcodes.POP);
            mv.visitInsn(Opcodes.RETURN);
            mv.visitMaxs(0, 0);
            mv.visitEnd();
        }

        cw.visitEnd();
        return cw.toByteArray();
    }
}
