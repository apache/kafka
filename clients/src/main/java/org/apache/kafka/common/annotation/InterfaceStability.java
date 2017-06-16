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
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Annotation to inform users of how much to rely on a particular package, class or method not changing over time.
 * Currently the stability can be {@link Stable}, {@link Evolving} or {@link Unstable}.
 */
@InterfaceStability.Evolving
public class InterfaceStability {
    /**
     * Compatibility is maintained in major, minor and patch releases with one exception: compatibility may be broken
     * in a major release (i.e. 0.m) for APIs that have been deprecated for at least one major/minor release cycle.
     * In cases where the impact of breaking compatibility is significant, there is also a minimum deprecation period
     * of one year.
     *
     * This is the default stability level for public APIs that are not annotated.
     */
    @Documented
    @Retention(RetentionPolicy.RUNTIME)
    public @interface Stable { }

    /**
     * Compatibility may be broken at minor release (i.e. m.x).
     */
    @Documented
    @Retention(RetentionPolicy.RUNTIME)
    public @interface Evolving { }

    /**
     * No guarantee is provided as to reliability or stability across any level of release granularity.
     */
    @Documented
    @Retention(RetentionPolicy.RUNTIME)
    public @interface Unstable { }
}
