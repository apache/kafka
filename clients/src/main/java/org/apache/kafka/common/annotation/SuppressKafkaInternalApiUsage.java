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
package org.apache.kafka.common.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a class, method, field, or constructor as intentionally permitted to reference Kafka
 * classes that are not part of the {@link InterfaceAudience.Public} surface. The
 * {@code KafkaInternalApiChecker} build plugin honours this annotation: a reference to an
 * internal Kafka class from an element (or its enclosing class) carrying this annotation is
 * skipped, and the supplied {@code value()} is printed by the checker so reviewers can see why
 * the exception was granted.
 *
 * <p>Use sparingly — every suppression is a place where the consumer accepts the risk of
 * Kafka-internal change.
 */
@Documented
@InterfaceAudience.Public
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.METHOD, ElementType.FIELD, ElementType.CONSTRUCTOR})
public @interface SuppressKafkaInternalApiUsage {
    /**
     * Human-readable justification for the suppression. Printed by the checker when the
     * suppression is honoured.
     */
    String value() default "";
}
