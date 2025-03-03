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

import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.test.TestUtils;
import org.apache.kafka.server.common.MetadataVersion;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.kafka.common.test.TestKitNodes.DEFAULT_BROKER_LISTENER_NAME;
import static org.apache.kafka.common.test.TestKitNodes.DEFAULT_BROKER_SECURITY_PROTOCOL;
import static org.apache.kafka.common.test.TestKitNodes.DEFAULT_CONTROLLER_LISTENER_NAME;
import static org.apache.kafka.common.test.TestKitNodes.DEFAULT_CONTROLLER_SECURITY_PROTOCOL;

public class ClusterConfigTest {

    private static Map<String, Object> fields(ClusterConfig config) {
        return Arrays.stream(config.getClass().getDeclaredFields()).collect(Collectors.toMap(Field::getName, f -> {
            f.setAccessible(true);
            return Assertions.assertDoesNotThrow(() -> f.get(config));
        }));
    }

    @Test
    public void testCopy() throws IOException {
        File trustStoreFile = TestUtils.tempFile();

        ClusterConfig clusterConfig = ClusterConfig.builder()
                .setTypes(Collections.singleton(Type.KRAFT))
                .setBrokers(3)
                .setControllers(2)
                .setDisksPerBroker(1)
                .setAutoStart(true)
                .setTags(Arrays.asList("name", "Generated Test"))
                .setBrokerSecurityProtocol(SecurityProtocol.PLAINTEXT)
                .setBrokerListenerName(ListenerName.normalised("EXTERNAL"))
                .setControllerSecurityProtocol(SecurityProtocol.SASL_PLAINTEXT)
                .setControllerListenerName(ListenerName.normalised("CONTROLLER"))
                .setTrustStoreFile(trustStoreFile)
                .setMetadataVersion(MetadataVersion.IBP_0_8_0)
                .setServerProperties(Collections.singletonMap("broker", "broker_value"))
                .setConsumerProperties(Collections.singletonMap("consumer", "consumer_value"))
                .setProducerProperties(Collections.singletonMap("producer", "producer_value"))
                .setAdminClientProperties(Collections.singletonMap("admin_client", "admin_client_value"))
                .setSaslClientProperties(Collections.singletonMap("sasl_client", "sasl_client_value"))
                .setSaslServerProperties(Collections.singletonMap("sasl_server", "sasl_server_value"))
                .setPerServerProperties(Collections.singletonMap(0, Collections.singletonMap("broker_0", "broker_0_value")))
                .build();

        Map<String, Object> clusterConfigFields = fields(clusterConfig);
        Map<String, Object> copyFields = fields(clusterConfig);
        Assertions.assertEquals(clusterConfigFields, copyFields);
    }

    @Test
    public void testBrokerLessThanZero() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> ClusterConfig.builder()
                .setBrokers(-1)
                .setControllers(1)
                .setDisksPerBroker(1)
                .build());
    }

    @Test
    public void testControllersLessThanZero() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> ClusterConfig.builder()
                .setBrokers(1)
                .setControllers(-1)
                .setDisksPerBroker(1)
                .build());
    }

    @Test
    public void testDisksPerBrokerIsZero() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> ClusterConfig.builder()
                .setBrokers(1)
                .setControllers(1)
                .setDisksPerBroker(0)
                .build());
    }

    @Test
    public void testDisplayTags() {
        List<String> tags = Arrays.asList("tag 1", "tag 2", "tag 3");
        ClusterConfig clusterConfig = ClusterConfig.defaultBuilder().setTags(tags).build();

        Set<String> expectedDisplayTags = clusterConfig.displayTags();

        Assertions.assertTrue(expectedDisplayTags.contains("tag 1"));
        Assertions.assertTrue(expectedDisplayTags.contains("tag 2"));
        Assertions.assertTrue(expectedDisplayTags.contains("tag 3"));
        Assertions.assertTrue(expectedDisplayTags.contains("MetadataVersion=" + MetadataVersion.latestTesting()));
        Assertions.assertTrue(expectedDisplayTags.contains("BrokerSecurityProtocol=" + DEFAULT_BROKER_SECURITY_PROTOCOL));
        Assertions.assertTrue(expectedDisplayTags.contains("BrokerListenerName=" + ListenerName.normalised(DEFAULT_BROKER_LISTENER_NAME)));
        Assertions.assertTrue(expectedDisplayTags.contains("ControllerSecurityProtocol=" + DEFAULT_CONTROLLER_SECURITY_PROTOCOL));
        Assertions.assertTrue(expectedDisplayTags.contains("ControllerListenerName=" + ListenerName.normalised(DEFAULT_CONTROLLER_LISTENER_NAME)));
    }
}
