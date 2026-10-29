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

import org.apache.kafka.clients.admin.AlterConfigOp.OpType;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.network.SocketServerConfigs;
import org.apache.kafka.server.config.ServerConfigs;
import org.apache.kafka.server.config.ServerLogConfigs;
import org.apache.kafka.server.policy.AlterConfigPolicy;
import org.apache.kafka.storage.internals.log.LogConfig;

import org.junit.jupiter.api.BeforeEach;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.test.TestUtils.assertFutureThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests AdminClient calls when the broker is configured with policies - AlterConfigPolicy.
 */

@ClusterTestDefaults(
    brokers = 3,
    serverProperties = {
        @ClusterConfigProperty(key = ServerLogConfigs.ALTER_CONFIG_POLICY_CLASS_NAME_CONFIG, value = "org.apache.kafka.clients.admin.AdminClientWithPoliciesIntegrationTest$Policy"),
    }
)
public class AdminClientWithPoliciesIntegrationTest {
    private final ClusterInstance clusterInstance;
    private static List<AlterConfigPolicy.RequestMetadata> validations = new ArrayList<>();

    AdminClientWithPoliciesIntegrationTest(ClusterInstance clusterInstance) {
        this.clusterInstance = clusterInstance;
    }

    @BeforeEach
    public void setup() throws InterruptedException {
        clusterInstance.waitForReadyBrokers();
    }

    @ClusterTest
    public void testInvalidAlterConfigsDueToPolicy() throws Exception {
        try (final Admin adminClient = clusterInstance.admin()) {
            // Create topics
            String topic1 = "invalid-alter-configs-due-to-policy-topic-1";
            String topic2 = "invalid-alter-configs-due-to-policy-topic-2";
            String topic3 = "invalid-alter-configs-due-to-policy-topic-3";
            clusterInstance.createTopic(topic1, 1, (short) 1);
            clusterInstance.createTopic(topic2, 1, (short) 1);
            clusterInstance.createTopic(topic3, 1, (short) 1);

            ConfigResource topicResource1 = new ConfigResource(ConfigResource.Type.TOPIC, topic1);
            ConfigResource topicResource2 = new ConfigResource(ConfigResource.Type.TOPIC, topic2);
            ConfigResource topicResource3 = new ConfigResource(ConfigResource.Type.TOPIC, topic3);

            // Set a mutable broker config
            ConfigResource brokerResource = new ConfigResource(ConfigResource.Type.BROKER, "0"); // "0" represents the broker ID
            Map<ConfigResource, Collection<AlterConfigOp>> configOps = Map.of(
                    brokerResource, List.of(new AlterConfigOp(new ConfigEntry(ServerConfigs.MESSAGE_MAX_BYTES_CONFIG, "50000"), OpType.SET))
            );
            adminClient.incrementalAlterConfigs(configOps).all().get();
            assertEquals(Set.of(ServerConfigs.MESSAGE_MAX_BYTES_CONFIG), validationsForResource(brokerResource).get(0).configs().keySet());
            validations.clear();

            Map<ConfigResource, Collection<AlterConfigOp>> alterConfigs = new HashMap<>();
            alterConfigs.put(topicResource1, List.of(
                    new AlterConfigOp(new ConfigEntry(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0.9"), OpType.SET),
                    new AlterConfigOp(new ConfigEntry(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2"), OpType.SET)
            ));
            alterConfigs.put(topicResource2, List.of(new AlterConfigOp(new ConfigEntry(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0.8"), OpType.SET)));
            alterConfigs.put(topicResource3, List.of(new AlterConfigOp(new ConfigEntry(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "-1"), OpType.SET)));
            alterConfigs.put(brokerResource, List.of(new AlterConfigOp(new ConfigEntry(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "12313"), OpType.SET)));

            // Alter configs: second is valid, the others are invalid
            AlterConfigsResult alterResult = adminClient.incrementalAlterConfigs(alterConfigs);
            assertEquals(Set.of(topicResource1, topicResource2, topicResource3, brokerResource), alterResult.values().keySet());
            assertFutureThrows(PolicyViolationException.class, alterResult.values().get(topicResource1));
            alterResult.values().get(topicResource2).get();
            assertFutureThrows(InvalidConfigurationException.class, alterResult.values().get(topicResource3));
            assertFutureThrows(InvalidRequestException.class, alterResult.values().get(brokerResource));
            assertTrue(validationsForResource(brokerResource).isEmpty(),
                    "Should not see the broker resource in the AlterConfig policy when the broker configs are not being updated.");
            validations.clear();

            // Verify that the second resource was updated and the others were not
            clusterInstance.ensureConsistentMetadata();
            DescribeConfigsResult describeResult = adminClient.describeConfigs(List.of(topicResource1, topicResource2, topicResource3, brokerResource));
            var configs = describeResult.all().get();
            assertEquals(4, configs.size());

            assertEquals(String.valueOf(LogConfig.DEFAULT_MIN_CLEANABLE_DIRTY_RATIO), configs.get(topicResource1).get(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG).value());
            assertEquals(String.valueOf(ServerLogConfigs.MIN_IN_SYNC_REPLICAS_DEFAULT), configs.get(topicResource1).get(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG).value());

            assertEquals("0.8", configs.get(topicResource2).get(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG).value());

            assertNull(configs.get(brokerResource).get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG).value());

            // Alter configs with validateOnly = true: only second is valid
            alterConfigs.put(topicResource2, List.of(new AlterConfigOp(new ConfigEntry(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0.7"), OpType.SET)));
            alterResult = adminClient.incrementalAlterConfigs(alterConfigs, new AlterConfigsOptions().validateOnly(true));

            assertFutureThrows(PolicyViolationException.class, alterResult.values().get(topicResource1));
            alterResult.values().get(topicResource2).get();
            assertFutureThrows(InvalidConfigurationException.class, alterResult.values().get(topicResource3));
            assertFutureThrows(InvalidRequestException.class, alterResult.values().get(brokerResource));
            assertTrue(validationsForResource(brokerResource).isEmpty(),
                    "Should not see the broker resource in the AlterConfig policy when the broker configs are not being updated.");
            validations.clear();

            // Verify that no resources are updated since validate_only = true
            clusterInstance.ensureConsistentMetadata();
            describeResult = adminClient.describeConfigs(List.of(topicResource1, topicResource2, topicResource3, brokerResource));
            configs = describeResult.all().get();
            assertEquals(4, configs.size());

            assertEquals(String.valueOf(LogConfig.DEFAULT_MIN_CLEANABLE_DIRTY_RATIO), configs.get(topicResource1).get(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG).value());
            assertEquals(String.valueOf(ServerLogConfigs.MIN_IN_SYNC_REPLICAS_DEFAULT), configs.get(topicResource1).get(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG).value());

            assertEquals("0.8", configs.get(topicResource2).get(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG).value());

            assertNull(configs.get(brokerResource).get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG).value());

            // Do an incremental alter config on the broker, ensure we don't see the broker config we set earlier in the policy
            alterResult = adminClient.incrementalAlterConfigs(Map.of(
                    brokerResource, List.of(new AlterConfigOp(new ConfigEntry(SocketServerConfigs.MAX_CONNECTIONS_CONFIG, "9999"), OpType.SET))
            ));
            alterResult.all().get();
            assertEquals(Set.of(SocketServerConfigs.MAX_CONNECTIONS_CONFIG), validationsForResource(brokerResource).get(0).configs().keySet());
        }
    }

    private static List<AlterConfigPolicy.RequestMetadata> validationsForResource(ConfigResource resource) {
        return validations.stream().filter(req -> req.resource().equals(resource)).toList();
    }

    /**
     * Used in @ClusterTestDefaults serverProperties, so it may appear unused in the IDE.
     */
    public static class Policy implements AlterConfigPolicy {
        private Map<String, ?> configs;
        private boolean closed = false;


        @Override
        public void configure(Map<String, ?> configs) {
            validations.clear();
            this.configs = configs;
        }

        @Override
        public void validate(AlterConfigPolicy.RequestMetadata requestMetadata) {
            validations.add(requestMetadata);
            assertFalse(closed, "Policy should not be closed");
            assertFalse(configs.isEmpty(), "configure should have been called with non empty configs");
            assertFalse(requestMetadata.configs().isEmpty(), "request configs should not be empty");
            assertFalse(requestMetadata.resource().name().isEmpty(), "resource name should not be empty");
            if (requestMetadata.configs().containsKey(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG)) {
                throw new PolicyViolationException("Min in sync replicas cannot be updated");
            }
        }

        @Override
        public void close() throws Exception {
            this.closed = true;
        }
    }
}
