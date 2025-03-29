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
package org.apache.kafka.clients.admin;


import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class StaticBrokerConfigTest {
    private static final String TOPIC = "topic";
    private static final String CUSTOM_VALUE = "1048576";

    /**
     * synonyms of `segment.bytes`
     */
    private static final String LOG_SEGMENT_BYTES = "log.segment.bytes";

    @ClusterTest(types = {Type.KRAFT},
        serverProperties = {
            @ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
            @ClusterConfigProperty(id = 3000, key = LOG_SEGMENT_BYTES, value = CUSTOM_VALUE)
        })
    public void testTopicConfigsGetImpactedIfStaticConfigsAddToController(ClusterInstance cluster)
        throws ExecutionException, InterruptedException {
        try (
            Admin admin = cluster.admin();
            Admin adminUsingBootstrapController = cluster.admin(Map.of(), true)
        ) {
            ConfigEntry configEntry = admin.createTopics(List.of(new NewTopic(TOPIC, 1, (short) 1)))
                .config(TOPIC).get().get(TopicConfig.SEGMENT_BYTES_CONFIG);
            assertEquals(ConfigEntry.ConfigSource.STATIC_BROKER_CONFIG, configEntry.source());
            assertEquals(CUSTOM_VALUE, configEntry.value(), "Config value should be custom value since controller has related static config");

            ConfigResource brokerResource = new ConfigResource(ConfigResource.Type.BROKER, "0");
            configEntry = admin.describeConfigs(List.of(brokerResource)).all().get().get(brokerResource).get(LOG_SEGMENT_BYTES);
            assertEquals(ConfigEntry.ConfigSource.DEFAULT_CONFIG, configEntry.source());
            assertNotEquals(CUSTOM_VALUE,
                configEntry.value(),
                "Config value should not be custom value since broker doesn't have related static config");

            ConfigResource controllerResource = new ConfigResource(ConfigResource.Type.BROKER, "3000");
            configEntry = adminUsingBootstrapController.describeConfigs(List.of(controllerResource))
                .all().get().get(controllerResource).get(LOG_SEGMENT_BYTES);
            assertEquals(ConfigEntry.ConfigSource.STATIC_BROKER_CONFIG, configEntry.source());
            assertEquals(CUSTOM_VALUE,
                configEntry.value(),
                "Config value should be custom value since controller has related static config");
        }
    }

    @ClusterTest(types = {Type.KRAFT},
        serverProperties = {
            @ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
            @ClusterConfigProperty(id = 0, key = LOG_SEGMENT_BYTES, value = CUSTOM_VALUE)
        })
    public void testTopicConfigsGetImpactedIfStaticConfigsAddToBroker(ClusterInstance cluster)
        throws ExecutionException, InterruptedException {
        try (
            Admin admin = cluster.admin();
            Admin adminUsingBootstrapController = cluster.admin(Map.of(), true)
        ) {
            ConfigEntry configEntry = admin.createTopics(List.of(new NewTopic(TOPIC, 1, (short) 1)))
                .config(TOPIC).get().get(TopicConfig.SEGMENT_BYTES_CONFIG);
            assertEquals(ConfigEntry.ConfigSource.DEFAULT_CONFIG, configEntry.source());
            assertNotEquals(CUSTOM_VALUE,
                configEntry.value(),
                "Config value should not be custom value since controller doesn't have static config");

            ConfigResource brokerResource = new ConfigResource(ConfigResource.Type.BROKER, "0");
            configEntry = admin.describeConfigs(List.of(brokerResource)).all().get().get(brokerResource).get(LOG_SEGMENT_BYTES);
            assertEquals(ConfigEntry.ConfigSource.STATIC_BROKER_CONFIG, configEntry.source());
            assertEquals(CUSTOM_VALUE,
                configEntry.value(),
                "Config value should be custom value since broker has related static config");

            ConfigResource controllerResource = new ConfigResource(ConfigResource.Type.BROKER, "3000");
            configEntry = adminUsingBootstrapController.describeConfigs(List.of(controllerResource))
                .all().get().get(controllerResource).get(LOG_SEGMENT_BYTES);
            assertEquals(ConfigEntry.ConfigSource.DEFAULT_CONFIG, configEntry.source());
            assertNotEquals(CUSTOM_VALUE,
                configEntry.value(),
                "Config value should not be custom value since controller doesn't have related static config");
        }
    }
}
