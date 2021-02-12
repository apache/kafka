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

import kafka.test.ClusterConfig;
import kafka.test.ClusterGenerator;
import org.junit.jupiter.api.TestTemplate;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Used to indicate that a test should call the method given by {@link #value()} to generate a number of
 * cluster configurations. The method specified by the value should accept a single argument of the type
 * {@link ClusterGenerator}. Any return value from the method is ignore. A test invocation
 * will be generated for each {@link ClusterConfig} provided to the ClusterGenerator instance.
 *
 * The method given here must be static since it is invoked before any tests are actually run. Each test generated
 * by this annotation will run as if it was defined as a separate test method with its own
 * {@link org.junit.jupiter.api.Test}. That is to say, each generated test invocation will have a separate lifecycle.
 *
 * This annotation may be used in conjunction with {@link ClusterTest} and {@link ClusterTests} which also yield
 * ClusterConfig instances.
 *
 * For Scala tests, the method should be defined in a companion object with the same name as the test class.
 */
@Documented
@Target({METHOD})
@Retention(RUNTIME)
@TestTemplate
public @interface ClusterTemplate {
    /**
     * Specify the static method used for generating cluster configs
     */
    String value();
}
