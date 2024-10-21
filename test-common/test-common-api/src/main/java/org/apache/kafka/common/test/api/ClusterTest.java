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

package org.apache.kafka.common.test.api;

import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.server.common.MetadataVersion;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.Timeout;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static org.apache.kafka.common.test.TestKitNodes.DEFAULT_BROKER_LISTENER_NAME;
import static org.apache.kafka.common.test.TestKitNodes.DEFAULT_CONTROLLER_LISTENER_NAME;

@Documented
@Target({METHOD})
@Retention(RUNTIME)
@TestTemplate
@Timeout(60)
@Tag("integration")
public @interface ClusterTest {
    Type[] types() default {};
    int brokers() default 0;
    int controllers() default 0;
    int disksPerBroker() default 0;
    AutoStart autoStart() default AutoStart.DEFAULT;
    // The broker/controller listener name and SecurityProtocol configurations must
    // be kept in sync with the default values in TestKitNodes, as many tests
    // directly use TestKitNodes without relying on the ClusterTest annotation.
    SecurityProtocol brokerSecurityProtocol() default SecurityProtocol.PLAINTEXT;
    String brokerListener() default DEFAULT_BROKER_LISTENER_NAME;
    SecurityProtocol controllerSecurityProtocol() default SecurityProtocol.PLAINTEXT;
    String controllerListener() default DEFAULT_CONTROLLER_LISTENER_NAME;
    MetadataVersion metadataVersion() default MetadataVersion.IBP_4_0_IV3;
    ClusterConfigProperty[] serverProperties() default {};
    // users can add tags that they want to display in test
    String[] tags() default {};
    ClusterFeature[] features() default {};
}
