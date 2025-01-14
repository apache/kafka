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

package org.apache.kafka.common.test;

import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestKitNodeTest {

    @ParameterizedTest
    @EnumSource(SecurityProtocol.class)
    public void testSecurityProtocol(SecurityProtocol securityProtocol) {
        if (securityProtocol != SecurityProtocol.PLAINTEXT && securityProtocol != SecurityProtocol.SASL_PLAINTEXT) {
            assertEquals("Currently only support PLAINTEXT / SASL_PLAINTEXT security protocol",
                    assertThrows(IllegalArgumentException.class,
                            () -> new TestKitNodes.Builder().setBrokerSecurityProtocol(securityProtocol).build()).getMessage());
            assertEquals("Currently only support PLAINTEXT / SASL_PLAINTEXT security protocol",
                assertThrows(IllegalArgumentException.class,
                    () -> new TestKitNodes.Builder().setControllerSecurityProtocol(securityProtocol).build()).getMessage());
        }
    }

    @Test
    public void testListenerName() {
        ListenerName brokerListenerName = ListenerName.normalised("FOOBAR");
        ListenerName controllerListenerName = ListenerName.normalised("BAZQUX");
        TestKitNodes testKitNodes = new TestKitNodes.Builder()
                .setNumBrokerNodes(1)
                .setNumControllerNodes(1)
                .setBrokerListenerName(brokerListenerName)
                .setBrokerSecurityProtocol(SecurityProtocol.PLAINTEXT)
                .setControllerListenerName(controllerListenerName)
                .setControllerSecurityProtocol(SecurityProtocol.PLAINTEXT)
                .build();
        assertEquals(brokerListenerName, testKitNodes.brokerListenerName());
        assertEquals(controllerListenerName, testKitNodes.controllerListenerName());
    }
}
