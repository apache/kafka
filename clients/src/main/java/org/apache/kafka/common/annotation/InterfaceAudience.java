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
 * Annotation to inform users of the intended audience of a particular class. The audience can
 * be {@link Public} or {@link Private}; the annotations target {@code TYPE} only — methods and
 * fields inherit the audience of their declaring class, and packages are intentionally not
 * a granularity the checker enforces. Audience is orthogonal to {@link InterfaceStability}:
 * a class may be public-audience and evolving-stability at the same time.
 *
 * <p>If no audience annotation is present on a class, it is assumed to be {@link Private}
 * (internal). External code must not depend on classes without an explicit {@link Public}
 * annotation.
 */
@InterfaceAudience.Public
public class InterfaceAudience {
    /**
     * Intended for end users of Apache Kafka. Classes and members marked {@code @Public} are
     * part of the Kafka public API surface; external code may depend on them subject to the
     * stability guarantees declared via {@link InterfaceStability}.
     */
    @Documented
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    public @interface Public { }

    /**
     * Intended for internal use within Apache Kafka. Classes and members marked {@code @Private}
     * carry no compatibility guarantees and may change or be removed at any time without notice.
     * This is also the assumed audience when no annotation is present.
     */
    @Documented
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    public @interface Private { }
}