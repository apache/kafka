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

import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.junit.jupiter.api.TestTemplate;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

@Documented
@Target({METHOD})
@Retention(RUNTIME)
@TestTemplate
public @interface ClusterTest {
    Type clusterType() default Type.DEFAULT;
    int brokers() default 0;
    int controllers() default 0;
    AutoStart autoStart() default AutoStart.DEFAULT;

    String name() default "";
    SecurityProtocol securityProtocol() default SecurityProtocol.PLAINTEXT;
    String listener() default "";
    ClusterConfigProperty[] serverProperties() default {};
}
