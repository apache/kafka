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


import kafka.server.KafkaConfig;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.coordinator.group.GroupConfig;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.raft.KRaftConfigs;
import org.apache.kafka.server.config.ServerConfigs;
import org.apache.kafka.server.metrics.ClientMetricsConfigs;
import org.apache.kafka.storage.internals.log.LogConfig;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

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

    @ClusterTest(types = {Type.KRAFT})
    public void testInternalConfigsDoNotReturnForDescribeConfigs(ClusterInstance cluster) throws Exception {
        try (
                Admin admin = cluster.admin();
                Admin controllerAdmin = cluster.admin(Map.of(), true)
        ) {
            ConfigResource brokerResource = new ConfigResource(ConfigResource.Type.BROKER, "0");
            ConfigResource topicResource = new ConfigResource(ConfigResource.Type.TOPIC, TOPIC);
            ConfigResource groupResource = new ConfigResource(ConfigResource.Type.GROUP, "testGroup");
            ConfigResource clientMetricsResource = new ConfigResource(ConfigResource.Type.CLIENT_METRICS, "testClient");

            admin.createTopics(List.of(new NewTopic(TOPIC, 1, (short) 1))).config(TOPIC).get();
            // make sure the topic metadata exist
            cluster.waitTopicCreation(TOPIC, 1);
            Map<ConfigResource, Config> configResourceMap = admin.describeConfigs(
                    List.of(brokerResource, topicResource, groupResource, clientMetricsResource)).all().get();

            // test for case ConfigResource.Type == BROKER
            // Notice: since the testing framework actively sets three internal configurations when starting the
            // broker (see org.apache.kafka.common.test.KafkaClusterTestKit.Builder.createNodeConfig()),
            // so the API `describeConfigs` will also return these three configurations. However, other internal
            // configurations will not be returned
            Set<String> ignoreConfigNames = Set.of(
                    ServerConfigs.UNSTABLE_FEATURE_VERSIONS_ENABLE_CONFIG,
                    ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG,
                    KRaftConfigs.SERVER_MAX_STARTUP_TIME_MS_CONFIG);
            Config brokerConfig = configResourceMap.get(brokerResource);
            assertNotContainsInternalConfig(brokerConfig, KafkaConfig.configDef().configKeys(), ignoreConfigNames);

            // test for case ConfigResource.Type == TOPIC
            Config topicConfig = configResourceMap.get(topicResource);
            assertNotContainsAnyInternalConfig(topicConfig, LogConfig.configKeys());

            // test for case ConfigResource.Type == GROUP
            Config groupConfig = configResourceMap.get(groupResource);
            assertNotContainsAnyInternalConfig(groupConfig, GroupConfig.configDef().configKeys());

            // test for case ConfigResource.Type == CLIENT_METRICS
            Config clientMetricsConfig = configResourceMap.get(clientMetricsResource);
            assertNotContainsAnyInternalConfig(clientMetricsConfig, ClientMetricsConfigs.configDef().configKeys());

            // test for controller node, and ConfigResource.Type == BROKER
            ConfigResource controllerResource = new ConfigResource(ConfigResource.Type.BROKER, "3000");
            Map<ConfigResource, Config> controllerConfigMap = controllerAdmin.describeConfigs(List.of(controllerResource)).all().get();
            Config controllerConfig = controllerConfigMap.get(controllerResource);
            assertNotContainsInternalConfig(controllerConfig, KafkaConfig.configDef().configKeys(), ignoreConfigNames);
        }
    }

    @ClusterTest(types = {Type.KRAFT})
    public void testInternalConfigsDoNotReturnForCreateTopics(ClusterInstance cluster) throws Exception {
        try (Admin admin = cluster.admin()) {
            // test for createTopics API
            Config config = admin.createTopics(List.of(new NewTopic(TOPIC, 1, (short) 1))).config(TOPIC).get();
            assertNotContainsAnyInternalConfig(config, LogConfig.configKeys());
        }
    }

    private void assertNotContainsAnyInternalConfig(Config config, Map<String, ConfigDef.ConfigKey> configKeyMap) {
        assertNotContainsInternalConfig(config, configKeyMap, Set.of());
    }

    private void assertNotContainsInternalConfig(Config config, Map<String, ConfigDef.ConfigKey> configKeyMap,
                                                 Set<String> ignoreConfigNames) {
        assertFalse(config.entries().isEmpty());
        for (ConfigEntry topicConfigEntry : config.entries()) {
            String configName = topicConfigEntry.name();
            ConfigDef.ConfigKey configKey = configKeyMap.get(configName);

            assertNotNull(configKey, "The ConfigKey of the config named '" + configName + "' should not be null");
            if (!ignoreConfigNames.contains(configName)) {
                assertFalse(configKey.internalConfig, "The config named '" + configName + "' is an internal config and should not be returned");
            }
        }
    }
}
