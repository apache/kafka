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
package org.apache.kafka.gradle;

import org.apache.kafka.apicheck.AsmClassFactory;
import org.apache.kafka.apicheck.TempJarBuilder;

import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.testfixtures.ProjectBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * End-to-end coverage of the consumer Gradle path: feeds a {@link KafkaInternalApiCheckerTask}
 * a synthetic Kafka surface jar + synthetic consumer bytecode and runs the task action directly.
 *
 * <p>The scanner itself is unit-tested in {@code PluginDeveloperApiUsageScannerTest}; what this
 * class adds is the wiring between the task's declared {@code kafkaDependencyJars} /
 * {@code classDirs} inputs, the {@code @TaskAction}, and the GradleException-vs-warning routing
 * via {@code failOnViolation}.
 */
class KafkaInternalApiCheckerTaskTest {

    @TempDir
    Path tempDir;

    private Project project;
    private KafkaInternalApiCheckerTask task;

    @BeforeEach
    void setUp() {
        project = ProjectBuilder.builder().build();
        task = project.getTasks().create("kafkaInternalApiChecker", KafkaInternalApiCheckerTask.class);
    }

    @Test
    void disabled_returnsWithoutScanning() {
        task.getCheckerEnabled().set(false);
        assertDoesNotThrow(() -> task.checkInternalApiUsage());
    }

    @Test
    void emptyKafkaDependencyJars_skipsCheckWithWarning() throws IOException {
        // classDirs configured, but no Kafka jars on the classpath — the scanner has no public
        // surface to validate against, so the task warns and returns rather than treating every
        // org.apache.kafka.* reference in the consumer bytecode as a violation.
        task.getClassDirs().set(project.files(consumerClassesDir("org/apache/kafka/internals/Hidden")));
        assertDoesNotThrow(() -> task.checkInternalApiUsage());
    }

    @Test
    void consumerReferencingInternalKafkaType_failsWhenFailOnViolation() throws IOException {
        File kafkaJar = synthesisedKafkaJarWithPublicClass();
        File classesDir = consumerClassesDir("org/apache/kafka/internals/Hidden");

        task.getKafkaDependencyJars().from(kafkaJar);
        task.getClassDirs().set(project.files(classesDir));
        task.getFailOnViolation().set(true);

        GradleException ex = assertThrows(GradleException.class, () -> task.checkInternalApiUsage());
        org.junit.jupiter.api.Assertions.assertTrue(ex.getMessage().contains("internal API usage violations"),
                "expected violations message, got: " + ex.getMessage());
    }

    @Test
    void consumerReferencingInternalKafkaType_warnsWhenFailOnViolationFalse() throws IOException {
        File kafkaJar = synthesisedKafkaJarWithPublicClass();
        File classesDir = consumerClassesDir("org/apache/kafka/internals/Hidden");

        task.getKafkaDependencyJars().from(kafkaJar);
        task.getClassDirs().set(project.files(classesDir));
        task.getFailOnViolation().set(false);

        // Logs a warning but does not throw — useful for adopt-as-warning rollouts.
        assertDoesNotThrow(() -> task.checkInternalApiUsage());
    }

    @Test
    void consumerReferencingPublicKafkaType_passes() throws IOException {
        File kafkaJar = synthesisedKafkaJarWithPublicClass();
        // Consumer references the @Public type rather than the internal one.
        File classesDir = consumerClassesDir("org/apache/kafka/clients/producer/KafkaProducer");

        task.getKafkaDependencyJars().from(kafkaJar);
        task.getClassDirs().set(project.files(classesDir));
        task.getFailOnViolation().set(true);

        assertDoesNotThrow(() -> task.checkInternalApiUsage());
    }

    // ----- helpers -----

    /**
     * Produce a "kafka-clients-like" jar containing one {@code @InterfaceAudience.Public}
     * class. References to that class from a consumer should pass; references to any other
     * {@code org.apache.kafka.*} class should be flagged as internal.
     */
    private File synthesisedKafkaJarWithPublicClass() throws IOException {
        return TempJarBuilder.jar()
                .addClass(AsmClassFactory.klass("org.apache.kafka.clients.producer.KafkaProducer")
                        .access(Opcodes.ACC_PUBLIC)
                        .publicApi())
                .writeTo(tempDir, "kafka-clients.jar");
    }

    /**
     * Write a single consumer .class to a fresh directory; the class loads a constant of the
     * given internal-name type, which the bytecode scanner picks up as a usage reference.
     */
    private File consumerClassesDir(String referencedInternalName) throws IOException {
        File dir = Files.createTempDirectory(tempDir, "consumer-classes").toFile();
        Files.write(new File(dir, "Consumer.class").toPath(),
                consumerBytes("com/example/Consumer", referencedInternalName));
        return dir;
    }

    /** Default ctor + one method that loads a class constant of {@code referencedInternalName}. */
    private static byte[] consumerBytes(String consumerInternalName, String referencedInternalName) {
        ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_FRAMES | ClassWriter.COMPUTE_MAXS);
        cw.visit(Opcodes.V11, Opcodes.ACC_PUBLIC, consumerInternalName, null, "java/lang/Object", null);

        MethodVisitor ctor = cw.visitMethod(Opcodes.ACC_PUBLIC, "<init>", "()V", null, null);
        ctor.visitCode();
        ctor.visitVarInsn(Opcodes.ALOAD, 0);
        ctor.visitMethodInsn(Opcodes.INVOKESPECIAL, "java/lang/Object", "<init>", "()V", false);
        ctor.visitInsn(Opcodes.RETURN);
        ctor.visitMaxs(0, 0);
        ctor.visitEnd();

        MethodVisitor mv = cw.visitMethod(Opcodes.ACC_PUBLIC, "useIt", "()V", null, null);
        mv.visitCode();
        mv.visitLdcInsn(Type.getObjectType(referencedInternalName));
        mv.visitInsn(Opcodes.POP);
        mv.visitInsn(Opcodes.RETURN);
        mv.visitMaxs(0, 0);
        mv.visitEnd();

        cw.visitEnd();
        return cw.toByteArray();
    }
}