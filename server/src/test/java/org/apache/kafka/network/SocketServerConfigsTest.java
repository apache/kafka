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
package org.apache.kafka.network;

import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class SocketServerConfigsTest {
    @Test
    public void testDefaultNameToSecurityProto() {
        assertEquals(Map.of(
            new ListenerName("PLAINTEXT"), SecurityProtocol.PLAINTEXT,
            new ListenerName("SSL"), SecurityProtocol.SSL,
            new ListenerName("SASL_PLAINTEXT"), SecurityProtocol.SASL_PLAINTEXT,
            new ListenerName("SASL_SSL"), SecurityProtocol.SASL_SSL
        ), SocketServerConfigs.DEFAULT_NAME_TO_SECURITY_PROTO);
    }

    @Test
    public void testListenerListToEndPointsWithEmptyString() {
        assertEquals(List.of(),
            SocketServerConfigs.listenerListToEndPoints(List.of(),
                SocketServerConfigs.DEFAULT_NAME_TO_SECURITY_PROTO));
    }

    @Test
    public void testListenerListToEndPointsWithBlankString() {
        KafkaException exception = assertThrows(KafkaException.class, () ->
                SocketServerConfigs.listenerListToEndPoints(List.of(" "), SocketServerConfigs.DEFAULT_NAME_TO_SECURITY_PROTO));
        assertEquals("Unable to parse   to a broker endpoint", exception.getMessage());
    }

    @Test
    public void testListenerListToEndPointsWithOneEndpoint() {
        assertEquals(List.of(new Endpoint("PLAINTEXT", SecurityProtocol.PLAINTEXT, "example.com", 8080)),
                SocketServerConfigs.listenerListToEndPoints(List.of("PLAINTEXT://example.com:8080"),
                    SocketServerConfigs.DEFAULT_NAME_TO_SECURITY_PROTO));
    }

    // Regression test for KAFKA-3719
    @Test
    public void testListenerListToEndPointsWithUnderscores() {
        assertEquals(List.of(
            new Endpoint("PLAINTEXT", SecurityProtocol.PLAINTEXT, "example.com", 8080),
            new Endpoint("SSL", SecurityProtocol.SSL, "local_host", 8081)),
                SocketServerConfigs.listenerListToEndPoints(List.of("PLAINTEXT://example.com:8080", "SSL://local_host:8081"),
                    SocketServerConfigs.DEFAULT_NAME_TO_SECURITY_PROTO));
    }

    @Test
    public void testListenerListToEndPointsWithWildcard() {
        assertEquals(List.of(new Endpoint("PLAINTEXT", SecurityProtocol.PLAINTEXT, null, 8080)),
                SocketServerConfigs.listenerListToEndPoints(List.of("PLAINTEXT://:8080"),
                    SocketServerConfigs.DEFAULT_NAME_TO_SECURITY_PROTO));
    }

    @Test
    public void testListenerListToEndPointsWithIpV6() {
        assertEquals(List.of(new Endpoint("PLAINTEXT", SecurityProtocol.PLAINTEXT, "::1", 9092)),
                SocketServerConfigs.listenerListToEndPoints(List.of("PLAINTEXT://[::1]:9092"),
                    SocketServerConfigs.DEFAULT_NAME_TO_SECURITY_PROTO));
    }

    @Test
    public void testAnotherListenerListToEndPointsWithIpV6() {
        assertEquals(List.of(new Endpoint("SASL_SSL", SecurityProtocol.SASL_SSL, "fe80::b1da:69ca:57f7:63d8%3", 9092)),
                SocketServerConfigs.listenerListToEndPoints(List.of("SASL_SSL://[fe80::b1da:69ca:57f7:63d8%3]:9092"),
                    SocketServerConfigs.DEFAULT_NAME_TO_SECURITY_PROTO));
    }

    @Test
    public void testAnotherListenerListToEndPointsWithNonDefaultProtoMap() {
        assertEquals(List.of(new Endpoint("CONTROLLER", SecurityProtocol.PLAINTEXT, "example.com", 9093)),
                SocketServerConfigs.listenerListToEndPoints(List.of("CONTROLLER://example.com:9093"),
                    Map.of(new ListenerName("CONTROLLER"), SecurityProtocol.PLAINTEXT)));
    }
}
