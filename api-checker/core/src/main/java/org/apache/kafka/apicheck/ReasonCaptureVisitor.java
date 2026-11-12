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
import org.objectweb.asm.Opcodes;

import java.util.function.Consumer;

/**
 * Captures the {@code value()} string of a {@code @SuppressKafkaInternalApiUsage} annotation.
 * If the annotation is present but its {@code value()} was omitted, {@link #visitEnd()} routes
 * an empty string to the setter — the convention both checkers use to mean "suppressed without
 * a reason."
 */
final class ReasonCaptureVisitor extends AnnotationVisitor {

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
        if (!assigned) {
            setter.accept("");
        }
    }
}