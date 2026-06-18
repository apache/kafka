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
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

import java.util.ArrayList;
import java.util.List;

/**
 * Synthesises {@code .class} bytes for KIP-1265 checker tests so they don't depend on real Kafka
 * classes on the classpath. Carries enough knobs to exercise the audience-annotation, nested-class
 * inner-access, and {@code @SuppressKafkaInternalApiUsage} paths the checker reads.
 */
final class AsmClassFactory {

    static final String PUBLIC_API_DESC =
            "Lorg/apache/kafka/common/annotation/InterfaceAudience$Public;";
    static final String PRIVATE_API_DESC =
            "Lorg/apache/kafka/common/annotation/InterfaceAudience$Private;";
    static final String SUPPRESS_DESC =
            "Lorg/apache/kafka/common/annotation/SuppressKafkaInternalApiUsage;";
    static final String DEPRECATED_DESC = "Ljava/lang/Deprecated;";

    private AsmClassFactory() {}

    static ClassBuilder klass(String binaryName) {
        return new ClassBuilder(binaryName);
    }

    static MethodSpec method(String name) {
        return new MethodSpec(name);
    }

    /** Wrap an internal name ("org/apache/kafka/X") as an object type descriptor. */
    static String objDesc(String internalName) {
        return "L" + internalName + ";";
    }

    static String toInternal(String binaryName) {
        return binaryName.replace('.', '/');
    }

    static final class ClassBuilder {
        private final String binaryName;
        private int headerAccess = Opcodes.ACC_PUBLIC;
        private Integer nestedAccess;
        private String superInternal = "java/lang/Object";
        private String[] interfaceInternals = new String[0];
        private boolean isInterface;
        private boolean publicApi;
        private boolean privateApi;
        private boolean deprecated;
        private boolean hasSuppress;
        private String suppressReason;
        private final List<MethodSpec> methods = new ArrayList<>();

        private ClassBuilder(String binaryName) {
            this.binaryName = binaryName;
        }

        String binaryName() {
            return binaryName;
        }

        ClassBuilder access(int access) {
            this.headerAccess = access;
            return this;
        }

        /**
         * Add an {@code InnerClasses} attribute entry for this class with the given source-level
         * access — the scanner reads this in preference to the (compiler-synthesised) header access
         * for nested classes. Binary name must contain {@code $}.
         */
        ClassBuilder nestedAccess(int access) {
            this.nestedAccess = access;
            return this;
        }

        ClassBuilder superClass(String internalName) {
            this.superInternal = internalName;
            return this;
        }

        ClassBuilder interfaces(String... internalNames) {
            this.interfaceInternals = internalNames;
            return this;
        }

        ClassBuilder asInterface() {
            this.isInterface = true;
            return this;
        }

        ClassBuilder publicApi() {
            this.publicApi = true;
            return this;
        }

        ClassBuilder privateApi() {
            this.privateApi = true;
            return this;
        }

        ClassBuilder deprecated() {
            this.deprecated = true;
            return this;
        }

        /** {@code @SuppressKafkaInternalApiUsage("reason")}; pass {@code null} to omit {@code value()}. */
        ClassBuilder suppress(String reason) {
            this.hasSuppress = true;
            this.suppressReason = reason;
            return this;
        }

        ClassBuilder method(MethodSpec method) {
            this.methods.add(method);
            return this;
        }

        byte[] toBytes() {
            ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_MAXS);
            String internalName = toInternal(binaryName);
            int access = headerAccess | (isInterface ? Opcodes.ACC_INTERFACE | Opcodes.ACC_ABSTRACT : 0);
            cw.visit(Opcodes.V11, access, internalName, null,
                    isInterface ? "java/lang/Object" : superInternal, interfaceInternals);
            writeClassAnnotations(cw);
            writeInnerClassEntry(cw, internalName);
            if (!isInterface) {
                writeDefaultCtor(cw, superInternal);
            }
            for (MethodSpec m : methods) {
                m.write(cw);
            }
            cw.visitEnd();
            return cw.toByteArray();
        }

        private void writeClassAnnotations(ClassWriter cw) {
            if (publicApi) {
                cw.visitAnnotation(PUBLIC_API_DESC, true).visitEnd();
            }
            if (privateApi) {
                cw.visitAnnotation(PRIVATE_API_DESC, true).visitEnd();
            }
            if (deprecated) {
                cw.visitAnnotation(DEPRECATED_DESC, true).visitEnd();
            }
            if (hasSuppress) {
                writeSuppress(cw.visitAnnotation(SUPPRESS_DESC, true), suppressReason);
            }
        }

        private void writeInnerClassEntry(ClassWriter cw, String internalName) {
            if (nestedAccess == null) {
                return;
            }
            int dollar = internalName.lastIndexOf('$');
            if (dollar < 0) {
                throw new IllegalStateException(
                        "nestedAccess requires a binaryName containing '$' (got " + binaryName + ")");
            }
            cw.visitInnerClass(internalName, internalName.substring(0, dollar),
                    internalName.substring(dollar + 1), nestedAccess);
        }
    }

    static final class MethodSpec {
        private final String name;
        private int access = Opcodes.ACC_PUBLIC;
        private String returnDesc = "V";
        private final List<String> paramDescs = new ArrayList<>();
        private final List<String> exceptionInternals = new ArrayList<>();
        private boolean hasSuppress;
        private String suppressReason;

        private MethodSpec(String name) {
            this.name = name;
        }

        MethodSpec access(int access) {
            this.access = access;
            return this;
        }

        MethodSpec returns(String desc) {
            this.returnDesc = desc;
            return this;
        }

        MethodSpec param(String desc) {
            this.paramDescs.add(desc);
            return this;
        }

        MethodSpec throwsExc(String internalName) {
            this.exceptionInternals.add(internalName);
            return this;
        }

        MethodSpec bridge() {
            this.access |= Opcodes.ACC_BRIDGE | Opcodes.ACC_SYNTHETIC;
            return this;
        }

        MethodSpec synthetic() {
            this.access |= Opcodes.ACC_SYNTHETIC;
            return this;
        }

        MethodSpec suppress(String reason) {
            this.hasSuppress = true;
            this.suppressReason = reason;
            return this;
        }

        void write(ClassWriter cw) {
            String desc = "(" + String.join("", paramDescs) + ")" + returnDesc;
            String[] excs = exceptionInternals.isEmpty()
                    ? null : exceptionInternals.toArray(new String[0]);
            MethodVisitor mv = cw.visitMethod(access, name, desc, null, excs);
            if (hasSuppress) {
                writeSuppress(mv.visitAnnotation(SUPPRESS_DESC, true), suppressReason);
            }
            mv.visitCode();
            emitZeroReturn(mv, returnDesc);
            mv.visitMaxs(0, 0);
            mv.visitEnd();
        }
    }

    private static void writeSuppress(AnnotationVisitor av, String reason) {
        if (reason != null) {
            av.visit("value", reason);
        }
        av.visitEnd();
    }

    private static void writeDefaultCtor(ClassWriter cw, String superInternal) {
        MethodVisitor mv = cw.visitMethod(Opcodes.ACC_PUBLIC, "<init>", "()V", null, null);
        mv.visitCode();
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        mv.visitMethodInsn(Opcodes.INVOKESPECIAL, superInternal, "<init>", "()V", false);
        mv.visitInsn(Opcodes.RETURN);
        mv.visitMaxs(1, 1);
        mv.visitEnd();
    }

    private static void emitZeroReturn(MethodVisitor mv, String desc) {
        switch (desc.charAt(0)) {
            case 'V':
                mv.visitInsn(Opcodes.RETURN);
                break;
            case 'I':
            case 'B':
            case 'S':
            case 'C':
            case 'Z':
                mv.visitInsn(Opcodes.ICONST_0);
                mv.visitInsn(Opcodes.IRETURN);
                break;
            case 'J':
                mv.visitInsn(Opcodes.LCONST_0);
                mv.visitInsn(Opcodes.LRETURN);
                break;
            case 'F':
                mv.visitInsn(Opcodes.FCONST_0);
                mv.visitInsn(Opcodes.FRETURN);
                break;
            case 'D':
                mv.visitInsn(Opcodes.DCONST_0);
                mv.visitInsn(Opcodes.DRETURN);
                break;
            case 'L':
            case '[':
                mv.visitInsn(Opcodes.ACONST_NULL);
                mv.visitInsn(Opcodes.ARETURN);
                break;
            default:
                throw new IllegalArgumentException("Unsupported return descriptor: " + desc);
        }
    }
}
