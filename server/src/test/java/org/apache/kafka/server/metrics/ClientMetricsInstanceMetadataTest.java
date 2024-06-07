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
package org.apache.kafka.server.metrics;

import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ClientMetricsInstanceMetadataTest {

    @Test
    public void testIsMatchValid() throws UnknownHostException {
        Uuid uuid = Uuid.randomUuid();
        ClientMetricsInstanceMetadata instanceMetadata = new ClientMetricsInstanceMetadata(uuid, ClientMetricsTestUtils.requestContext());
        // We consider empty/missing client matching patterns as valid
        assertTrue(instanceMetadata.isMatch(Collections.emptyMap()));

        assertTrue(instanceMetadata.isMatch(
            Collections.singletonMap(ClientMetricsConfigs.CLIENT_ID, Pattern.compile(".*"))));
        assertTrue(instanceMetadata.isMatch(
            Collections.singletonMap(ClientMetricsConfigs.CLIENT_ID, Pattern.compile("producer-1"))));
        assertTrue(instanceMetadata.isMatch(
            Collections.singletonMap(ClientMetricsConfigs.CLIENT_ID, Pattern.compile("producer.*"))));
        assertTrue(instanceMetadata.isMatch(
            Collections.singletonMap(ClientMetricsConfigs.CLIENT_INSTANCE_ID, Pattern.compile(uuid.toString()))));
        assertTrue(instanceMetadata.isMatch(
            Collections.singletonMap(ClientMetricsConfigs.CLIENT_SOFTWARE_NAME, Pattern.compile("apache-kafka-java"))));
        assertTrue(instanceMetadata.isMatch(
            Collections.singletonMap(ClientMetricsConfigs.CLIENT_SOFTWARE_VERSION, Pattern.compile("3.5.2"))));
        assertTrue(instanceMetadata.isMatch(
            Collections.singletonMap(ClientMetricsConfigs.CLIENT_SOURCE_ADDRESS, Pattern.compile(
                InetAddress.getLocalHost().getHostAddress()))));
        assertTrue(instanceMetadata.isMatch(
            Collections.singletonMap(ClientMetricsConfigs.CLIENT_SOURCE_PORT, Pattern.compile(
                String.valueOf(ClientMetricsTestUtils.CLIENT_PORT)))));
    }

    @Test
    public void testIsMatchMultiplePatternValid() throws UnknownHostException {
        Uuid uuid = Uuid.randomUuid();
        ClientMetricsInstanceMetadata instanceMetadata = new ClientMetricsInstanceMetadata(uuid,
            ClientMetricsTestUtils.requestContext());

        Map<String, Pattern> patternMap = new HashMap<>();
        patternMap.put(ClientMetricsConfigs.CLIENT_ID, Pattern.compile("producer-1"));
        patternMap.put(ClientMetricsConfigs.CLIENT_INSTANCE_ID, Pattern.compile(uuid.toString()));
        patternMap.put(ClientMetricsConfigs.CLIENT_SOFTWARE_NAME, Pattern.compile("apache-kafka-.*"));
        patternMap.put(ClientMetricsConfigs.CLIENT_SOFTWARE_VERSION, Pattern.compile("3.5.2"));
        patternMap.put(ClientMetricsConfigs.CLIENT_SOURCE_ADDRESS, Pattern.compile(InetAddress.getLocalHost().getHostAddress()));
        patternMap.put(ClientMetricsConfigs.CLIENT_SOURCE_PORT, Pattern.compile(String.valueOf(ClientMetricsTestUtils.CLIENT_PORT)));

        assertTrue(instanceMetadata.isMatch(patternMap));
    }

    @Test
    public void testIsMatchMismatchFail() throws UnknownHostException {
        Uuid uuid = Uuid.randomUuid();
        ClientMetricsInstanceMetadata instanceMetadata = new ClientMetricsInstanceMetadata(uuid,
            ClientMetricsTestUtils.requestContext());

        Map<String, Pattern> patternMap = new HashMap<>();
        patternMap.put(ClientMetricsConfigs.CLIENT_INSTANCE_ID, Pattern.compile(uuid.toString()));
        patternMap.put(ClientMetricsConfigs.CLIENT_SOFTWARE_NAME, Pattern.compile("apache-kafka-.*"));
        patternMap.put(ClientMetricsConfigs.CLIENT_SOFTWARE_VERSION, Pattern.compile("3.5.2"));
        patternMap.put(ClientMetricsConfigs.CLIENT_SOURCE_ADDRESS, Pattern.compile(InetAddress.getLocalHost().getHostAddress()));

        // Client id is different.
        patternMap.put(ClientMetricsConfigs.CLIENT_ID, Pattern.compile("producer-2"));
        assertFalse(instanceMetadata.isMatch(patternMap));

        // Client instance id is different.
        patternMap.put(ClientMetricsConfigs.CLIENT_ID, Pattern.compile("producer-1"));
        patternMap.put(ClientMetricsConfigs.CLIENT_INSTANCE_ID, Pattern.compile(uuid + "random"));
        assertFalse(instanceMetadata.isMatch(patternMap));

        // Software name is different.
        patternMap.put(ClientMetricsConfigs.CLIENT_INSTANCE_ID, Pattern.compile(uuid.toString()));
        patternMap.put(ClientMetricsConfigs.CLIENT_SOFTWARE_NAME, Pattern.compile("apache-kafka-java-1"));
        assertFalse(instanceMetadata.isMatch(patternMap));

        // Software version is different.
        patternMap.put(ClientMetricsConfigs.CLIENT_SOFTWARE_NAME, Pattern.compile("apache-kafka-java"));
        patternMap.put(ClientMetricsConfigs.CLIENT_SOFTWARE_VERSION, Pattern.compile("3.5.x"));
        assertFalse(instanceMetadata.isMatch(patternMap));

        // Source address is different.
        patternMap.put(ClientMetricsConfigs.CLIENT_SOFTWARE_VERSION, Pattern.compile("3.5.2"));
        patternMap.put(ClientMetricsConfigs.CLIENT_SOURCE_ADDRESS, Pattern.compile("1.2.3.4"));
        assertFalse(instanceMetadata.isMatch(patternMap));

        // Source port is different.
        patternMap.put(ClientMetricsConfigs.CLIENT_SOURCE_ADDRESS, Pattern.compile(InetAddress.getLocalHost().getHostAddress()));
        patternMap.put(ClientMetricsConfigs.CLIENT_SOURCE_PORT, Pattern.compile("8080"));
        assertFalse(instanceMetadata.isMatch(patternMap));
    }

    @Test
    public void testIsMatchWithInvalidKeyFail() throws UnknownHostException {
        Uuid uuid = Uuid.randomUuid();
        ClientMetricsInstanceMetadata instanceMetadata = new ClientMetricsInstanceMetadata(uuid,
            ClientMetricsTestUtils.requestContext());

        // Unknown key in pattern map
        assertFalse(instanceMetadata.isMatch(Collections.singletonMap("unknown", Pattern.compile(".*"))));
        // '*' key is considered as invalid regex pattern
        assertFalse(instanceMetadata.isMatch(Collections.singletonMap("*", Pattern.compile(".*"))));
    }

    @Test
    public void testIsMatchWithNullValueFail() throws UnknownHostException {
        Uuid uuid = Uuid.randomUuid();
        ClientMetricsInstanceMetadata instanceMetadata = new ClientMetricsInstanceMetadata(uuid,
            ClientMetricsTestUtils.requestContextWithNullClientInfo());

        assertFalse(instanceMetadata.isMatch(Collections.singletonMap(ClientMetricsConfigs.CLIENT_SOFTWARE_NAME,
            Pattern.compile(".*"))));
        assertFalse(instanceMetadata.isMatch(Collections.singletonMap(ClientMetricsConfigs.CLIENT_SOFTWARE_VERSION,
            Pattern.compile(".*"))));
    }
}
