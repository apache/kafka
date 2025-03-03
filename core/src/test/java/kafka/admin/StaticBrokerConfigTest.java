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
package kafka.admin;


import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfig;
import org.apache.kafka.common.test.api.ClusterTemplate;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.common.test.junit.ClusterTestExtensions;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;

import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@ExtendWith(value = ClusterTestExtensions.class)
public class StaticBrokerConfigTest {
    private static final String TOPIC = "topic";
    private static final String CUSTOM_VALUE = "12345";

    /**
     * synonyms of `segment.bytes`
     */
    private static final String LOG_SEGMENT_BYTES = "log.segment.bytes";

    @ClusterTemplate("controllerTestConfig")
    public void testDescribeConfigsShouldNotReturnTopicRelatedQuorumControllerConfigs(ClusterInstance cluster)
        throws ExecutionException, InterruptedException {
        try (Admin admin = cluster.admin()) {
            CreateTopicsResult createResult = admin.createTopics(List.of(new NewTopic(TOPIC, 1, (short) 1)));
            ConfigEntry config = createResult.config(TOPIC).get().get(TopicConfig.SEGMENT_BYTES_CONFIG);
            assertNotNull(config, "Create Topic result should include static topic config");
            assertEquals(CUSTOM_VALUE, config.value(), "Config value should be custom value since controller have related static config");

            ConfigResource resource = new ConfigResource(ConfigResource.Type.BROKER, "0");
            Config keyToConfigEntry = admin.describeConfigs(List.of(resource)).all().get().get(resource);
            assertNotEquals(CUSTOM_VALUE, keyToConfigEntry.get(LOG_SEGMENT_BYTES).value(),
                "Config value should not be custom value since broker don't have related static config");
        }
    }

    @ClusterTemplate("controllerTestConfig")
    public void testDescribeConfigsShouldReturnTopicRelatedQuorumControllerConfigsWhenUsingBootstrapController(ClusterInstance cluster)
        throws ExecutionException, InterruptedException {
        try (
            Admin admin = cluster.admin();
            Admin adminUsingBootstrapController = cluster.admin(Map.of(), true);
        ) {
            CreateTopicsResult createResult = admin.createTopics(List.of(new NewTopic(TOPIC, 1, (short) 1)));
            ConfigEntry config = createResult.config(TOPIC).get().get(TopicConfig.SEGMENT_BYTES_CONFIG);
            assertNotNull(config, "Create Topic result should include static topic config");
            assertEquals(CUSTOM_VALUE, config.value(), "Config value should be custom value since controller have related static config");

            ConfigResource resource = new ConfigResource(ConfigResource.Type.BROKER, "0");
            Config keyToConfigEntry = adminUsingBootstrapController.describeConfigs(List.of(resource)).all().get().get(resource);
            assertEquals(CUSTOM_VALUE, keyToConfigEntry.get(LOG_SEGMENT_BYTES).value(),
                "Config value should be custom value since controller have related static config");
        }
    }

    @ClusterTemplate("brokerTestConfig")
    public void testDescribeConfigsShouldReturnTopicRelatedBrokerConfigsEvenIfNotInTopicCreation(ClusterInstance cluster)
        throws ExecutionException, InterruptedException {
        try (
            Admin admin = cluster.admin();
        ) {
            CreateTopicsResult createResult = admin.createTopics(List.of(new NewTopic(TOPIC, 1, (short) 1)));
            ConfigEntry config = createResult.config(TOPIC).get().get(TopicConfig.SEGMENT_BYTES_CONFIG);
            assertNotNull(config, "Create Topic result should include default topic config");
            assertNotEquals(CUSTOM_VALUE, config.value(),
                "Config value should not be custom value since controller don't have static config");

            ConfigResource resource = new ConfigResource(ConfigResource.Type.BROKER, "0");
            Config configResourceConfigMap = admin.describeConfigs(List.of(resource)).all().get().get(resource);
            ConfigEntry configValue = configResourceConfigMap.get(LOG_SEGMENT_BYTES);
            assertNotNull(configValue, "Broker should include related static config");
            assertEquals(CUSTOM_VALUE, configValue.value(), "Config value should be custom value since broker have related static config");
        }
    }

    @ClusterTemplate("brokerTestConfig")
    public void testDescribeConfigsShouldNotReturnTopicRelatedBrokerConfigsWhenUsingBootstrapController(ClusterInstance cluster)
        throws ExecutionException, InterruptedException {
        try (
            Admin admin = cluster.admin();
            Admin adminUsingBootstrapController = cluster.admin(Map.of(), true);
        ) {
            CreateTopicsResult createResult = admin.createTopics(List.of(new NewTopic(TOPIC, 1, (short) 1)));
            ConfigEntry config = createResult.config(TOPIC).get().get(TopicConfig.SEGMENT_BYTES_CONFIG);
            assertNotNull(config, "Create Topic result should include default topic config");
            assertNotEquals(CUSTOM_VALUE, config.value(),
                "Config value should not be custom value since controller don't have static config");

            ConfigResource resource = new ConfigResource(ConfigResource.Type.BROKER, "0");
            Config configResourceConfigMap = adminUsingBootstrapController.describeConfigs(List.of(resource)).all().get().get(resource);
            ConfigEntry configValue = configResourceConfigMap.get(LOG_SEGMENT_BYTES);
            assertNotNull(configValue);
            assertNotEquals(CUSTOM_VALUE, configValue.value(), "Config value should not be custom value since controller don't have related static config");
        }
    }


    private static List<ClusterConfig> controllerTestConfig() {
        return List.of(ClusterConfig.defaultBuilder()
            .setTypes(Set.of(Type.KRAFT))
            .setPerServerProperties(Map.of(3000, Map.of(LOG_SEGMENT_BYTES, CUSTOM_VALUE)))
            .setServerProperties(Map.of(
                GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, "1", GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, "1")
            )
            .build());
    }

    private static List<ClusterConfig> brokerTestConfig() {
        return List.of(ClusterConfig.defaultBuilder()
            .setTypes(Set.of(Type.KRAFT))
            .setPerServerProperties(Map.of(0, Map.of(LOG_SEGMENT_BYTES, CUSTOM_VALUE)))
            .setServerProperties(Map.of(
                GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, "1", GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, "1")
            )
            .build());
    }
}
