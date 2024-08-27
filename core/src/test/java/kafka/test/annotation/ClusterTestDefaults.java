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

package kafka.test.annotation;

import kafka.test.junit.ClusterTestExtensions;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Used to set class level defaults for any test template methods annotated with {@link ClusterTest} or
 * {@link ClusterTests}. The default values here are also used as the source for defaults in
 * {@link ClusterTestExtensions}.
 */
@Documented
@Target({TYPE})
@Retention(RUNTIME)
public @interface ClusterTestDefaults {
    Type[] types() default {Type.ZK, Type.KRAFT, Type.CO_KRAFT};
    int brokers() default 1;
    int controllers() default 1;
    int disksPerBroker() default 1;
    boolean autoStart() default true;
    // Set default server properties for all @ClusterTest(s)
    ClusterConfigProperty[] serverProperties() default {};
}
