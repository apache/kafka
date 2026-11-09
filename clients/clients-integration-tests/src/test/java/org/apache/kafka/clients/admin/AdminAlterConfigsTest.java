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
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.coordinator.group.GroupConfig;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.network.SocketServerConfigs;
import org.apache.kafka.server.config.QuotaConfig;
import org.apache.kafka.server.config.ServerConfigs;
import org.apache.kafka.server.config.ServerLogConfigs;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.test.TestUtils;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.kafka.test.TestUtils.assertFutureThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@ClusterTestDefaults(brokers = 3, types = {Type.KRAFT})
public class AdminAlterConfigsTest {

    private final ClusterInstance clusterInstance;

    AdminAlterConfigsTest(ClusterInstance clusterInstance) {
        this.clusterInstance = clusterInstance;
    }

    @ClusterTest
    public void testValidIncrementalAlterConfigs() throws Exception {
        try (Admin admin = clusterInstance.admin()) {
            // Create topics
            String topic1 = "incremental-alter-configs-topic-1";
            ConfigResource topic1Resource = new ConfigResource(ConfigResource.Type.TOPIC, topic1);
            Map<String, String> topic1CreateConfigs = Map.of(
                TopicConfig.RETENTION_MS_CONFIG, "60000000",
                TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT
            );
            clusterInstance.createTopic(topic1, 1, (short) 1, topic1CreateConfigs);

            String topic2 = "incremental-alter-configs-topic-2";
            ConfigResource topic2Resource = new ConfigResource(ConfigResource.Type.TOPIC, topic2);
            clusterInstance.createTopic(topic2, 1, (short) 1);

            // Alter topic configs
            List<AlterConfigOp> topic1AlterConfigs = List.of(
                new AlterConfigOp(new ConfigEntry(TopicConfig.FLUSH_MS_CONFIG, "1000"), AlterConfigOp.OpType.SET),
                new AlterConfigOp(new ConfigEntry(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE), AlterConfigOp.OpType.APPEND),
                new AlterConfigOp(new ConfigEntry(TopicConfig.RETENTION_MS_CONFIG, ""), AlterConfigOp.OpType.DELETE)
            );

            // Test SET and APPEND on previously unset properties
            List<AlterConfigOp> topic2AlterConfigs = List.of(
                new AlterConfigOp(new ConfigEntry(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0.9"), AlterConfigOp.OpType.SET),
                new AlterConfigOp(new ConfigEntry(TopicConfig.COMPRESSION_TYPE_CONFIG, "lz4"), AlterConfigOp.OpType.SET),
                new AlterConfigOp(new ConfigEntry(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT), AlterConfigOp.OpType.APPEND)
            );

            AlterConfigsResult alterResult = admin.incrementalAlterConfigs(Map.of(
                topic1Resource, topic1AlterConfigs,
                topic2Resource, topic2AlterConfigs
            ));

            assertEquals(Set.of(topic1Resource, topic2Resource), alterResult.values().keySet());
            alterResult.all().get();

            clusterInstance.ensureConsistentMetadata();

            // Verify that topics were updated correctly
            DescribeConfigsResult describeResult = admin.describeConfigs(List.of(topic1Resource, topic2Resource));
            Map<ConfigResource, Config> configs = describeResult.all().get();

            assertEquals(2, configs.size());

            assertEquals("1000", configs.get(topic1Resource).get(TopicConfig.FLUSH_MS_CONFIG).value());
            assertEquals("compact,delete", configs.get(topic1Resource).get(TopicConfig.CLEANUP_POLICY_CONFIG).value());
            assertEquals(String.valueOf(LogConfig.DEFAULT_RETENTION_MS), configs.get(topic1Resource).get(TopicConfig.RETENTION_MS_CONFIG).value());

            assertEquals("0.9", configs.get(topic2Resource).get(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG).value());
            assertEquals("lz4", configs.get(topic2Resource).get(TopicConfig.COMPRESSION_TYPE_CONFIG).value());
            assertEquals("delete,compact", configs.get(topic2Resource).get(TopicConfig.CLEANUP_POLICY_CONFIG).value());

            // verify subtract operation, including from an empty property
            topic1AlterConfigs = List.of(
                new AlterConfigOp(new ConfigEntry(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT), AlterConfigOp.OpType.SUBTRACT),
                new AlterConfigOp(new ConfigEntry(QuotaConfig.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG, "0"), AlterConfigOp.OpType.SUBTRACT)
            );

            // subtract all from this list property
            topic2AlterConfigs = List.of(
                new AlterConfigOp(new ConfigEntry(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT + "," + TopicConfig.CLEANUP_POLICY_DELETE), AlterConfigOp.OpType.SUBTRACT)
            );

            alterResult = admin.incrementalAlterConfigs(Map.of(
                topic1Resource, topic1AlterConfigs,
                topic2Resource, topic2AlterConfigs
            ));
            assertEquals(Set.of(topic1Resource, topic2Resource), alterResult.values().keySet());
            alterResult.all().get();

            clusterInstance.ensureConsistentMetadata();

            // Verify that topics were updated correctly
            describeResult = admin.describeConfigs(List.of(topic1Resource, topic2Resource));
            configs = describeResult.all().get();

            assertEquals(2, configs.size());

            assertEquals("delete", configs.get(topic1Resource).get(TopicConfig.CLEANUP_POLICY_CONFIG).value());
            assertEquals("1000", configs.get(topic1Resource).get(TopicConfig.FLUSH_MS_CONFIG).value()); // verify previous change is still intact
            assertEquals("", configs.get(topic1Resource).get(QuotaConfig.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG).value());
            assertEquals("", configs.get(topic2Resource).get(TopicConfig.CLEANUP_POLICY_CONFIG).value());

            // Alter topics with validateOnly=true
            topic1AlterConfigs = List.of(
                new AlterConfigOp(new ConfigEntry(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT), AlterConfigOp.OpType.APPEND)
            );

            alterResult = admin.incrementalAlterConfigs(Map.of(
                topic1Resource, topic1AlterConfigs
            ), new AlterConfigsOptions().validateOnly(true));
            alterResult.all().get();

            // Verify that topics were not updated due to validateOnly = true
            describeResult = admin.describeConfigs(List.of(topic1Resource));
            configs = describeResult.all().get();

            assertEquals("delete", configs.get(topic1Resource).get(TopicConfig.CLEANUP_POLICY_CONFIG).value());

            // Alter topics with validateOnly=true with invalid configs
            topic1AlterConfigs = List.of(
                new AlterConfigOp(new ConfigEntry(TopicConfig.COMPRESSION_TYPE_CONFIG, "zip"), AlterConfigOp.OpType.SET)
            );

            alterResult = admin.incrementalAlterConfigs(Map.of(
                topic1Resource, topic1AlterConfigs
            ), new AlterConfigsOptions().validateOnly(true));

            assertFutureThrows(InvalidConfigurationException.class, alterResult.values().get(topic1Resource),
                "Invalid value zip for configuration compression.type: String must be one of: uncompressed, zstd, lz4, snappy, gzip, producer");
        }
    }

    @ClusterTest
    public void testAppendAlreadyExistsConfigsAndSubtractNotExistsConfigs() throws Exception {
        try (Admin admin = clusterInstance.admin()) {
            // Create topics
            String topic = "incremental-alter-configs-topic";
            ConfigResource topicResource = new ConfigResource(ConfigResource.Type.TOPIC, topic);

            List<Integer> brokerIds = clusterInstance.brokerIds().stream().sorted().toList();
            String appendValues = "0:" + brokerIds.get(0);
            String subtractValues = brokerIds.subList(1, brokerIds.size()).stream()
                .map(brokerId -> "0:" + brokerId)
                .collect(Collectors.joining(","));
            assertNotEquals("", subtractValues);

            Map<String, String> topicCreateConfigs = Map.of(QuotaConfig.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG, appendValues);
            clusterInstance.createTopic(topic, 1, (short) 1, topicCreateConfigs);

            // Append value that is already present
            List<AlterConfigOp> topicAppendConfigs = List.of(
                new AlterConfigOp(new ConfigEntry(QuotaConfig.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG, appendValues), AlterConfigOp.OpType.APPEND)
            );

            AlterConfigsResult appendResult = admin.incrementalAlterConfigs(Map.of(topicResource, topicAppendConfigs));
            appendResult.all().get();

            // Subtract values that are not present
            List<AlterConfigOp> topicSubtractConfigs = List.of(
                new AlterConfigOp(new ConfigEntry(QuotaConfig.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG, subtractValues), AlterConfigOp.OpType.SUBTRACT)
            );
            AlterConfigsResult subtractResult = admin.incrementalAlterConfigs(Map.of(topicResource, topicSubtractConfigs));
            subtractResult.all().get();

            clusterInstance.ensureConsistentMetadata();

            // Verify that topics were updated correctly
            DescribeConfigsResult describeResult = admin.describeConfigs(List.of(topicResource));
            Map<ConfigResource, Config> configs = describeResult.all().get();

            assertEquals(appendValues, configs.get(topicResource).get(QuotaConfig.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG).value());
        }
    }

    @ClusterTest
    public void testIncrementalAlterConfigsDeleteAndSetBrokerConfigs() throws Exception {
        try (Admin admin = clusterInstance.admin()) {
            ConfigResource broker0Resource = new ConfigResource(ConfigResource.Type.BROKER, "0");
            admin.incrementalAlterConfigs(Map.of(broker0Resource,
                List.of(new AlterConfigOp(new ConfigEntry(QuotaConfig.LEADER_REPLICATION_THROTTLED_RATE_CONFIG, "123"),
                        AlterConfigOp.OpType.SET),
                    new AlterConfigOp(new ConfigEntry(QuotaConfig.FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG, "456"),
                        AlterConfigOp.OpType.SET)
                ))).all().get();
            TestUtils.waitForCondition(() -> {
                Map<String, String> broker0Configs = new HashMap<>();
                admin.describeConfigs(List.of(broker0Resource)).all().get().get(broker0Resource).entries()
                    .forEach(entry -> broker0Configs.put(entry.name(), entry.value()));
                return "123".equals(broker0Configs.getOrDefault(QuotaConfig.LEADER_REPLICATION_THROTTLED_RATE_CONFIG, "")) &&
                    "456".equals(broker0Configs.getOrDefault(QuotaConfig.FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG, ""));
            }, "Expected to see the broker properties we just set");
            admin.incrementalAlterConfigs(Map.of(broker0Resource,
                List.of(new AlterConfigOp(new ConfigEntry(QuotaConfig.LEADER_REPLICATION_THROTTLED_RATE_CONFIG, ""),
                        AlterConfigOp.OpType.DELETE),
                    new AlterConfigOp(new ConfigEntry(QuotaConfig.FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG, "654"),
                        AlterConfigOp.OpType.SET),
                    new AlterConfigOp(new ConfigEntry(QuotaConfig.REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG, "987"),
                        AlterConfigOp.OpType.SET)
                ))).all().get();
            TestUtils.waitForCondition(() -> {
                Map<String, String> broker0Configs = new HashMap<>();
                admin.describeConfigs(List.of(broker0Resource)).all().get().get(broker0Resource).entries()
                    .forEach(entry -> broker0Configs.put(entry.name(), entry.value()));
                return "".equals(broker0Configs.getOrDefault(QuotaConfig.LEADER_REPLICATION_THROTTLED_RATE_CONFIG, "")) &&
                    "654".equals(broker0Configs.getOrDefault(QuotaConfig.FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG, "")) &&
                    "987".equals(broker0Configs.getOrDefault(QuotaConfig.REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG, ""));
            }, "Expected to see the broker properties we just modified");
        }
    }

    @ClusterTest
    public void testIncrementalAlterConfigsDeleteBrokerConfigs() throws Exception {
        try (Admin admin = clusterInstance.admin()) {
            ConfigResource broker0Resource = new ConfigResource(ConfigResource.Type.BROKER, "0");
            admin.incrementalAlterConfigs(Map.of(broker0Resource,
                List.of(new AlterConfigOp(new ConfigEntry(QuotaConfig.LEADER_REPLICATION_THROTTLED_RATE_CONFIG, "123"),
                        AlterConfigOp.OpType.SET),
                    new AlterConfigOp(new ConfigEntry(QuotaConfig.FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG, "456"),
                        AlterConfigOp.OpType.SET),
                    new AlterConfigOp(new ConfigEntry(QuotaConfig.REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG, "789"),
                        AlterConfigOp.OpType.SET)
                ))).all().get();
            TestUtils.waitForCondition(() -> {
                Map<String, String> broker0Configs = new HashMap<>();
                admin.describeConfigs(List.of(broker0Resource)).all().get().get(broker0Resource).entries()
                    .forEach(entry -> broker0Configs.put(entry.name(), entry.value()));
                return "123".equals(broker0Configs.getOrDefault(QuotaConfig.LEADER_REPLICATION_THROTTLED_RATE_CONFIG, "")) &&
                    "456".equals(broker0Configs.getOrDefault(QuotaConfig.FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG, "")) &&
                    "789".equals(broker0Configs.getOrDefault(QuotaConfig.REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG, ""));
            }, "Expected to see the broker properties we just set");
            admin.incrementalAlterConfigs(Map.of(broker0Resource,
                List.of(new AlterConfigOp(new ConfigEntry(QuotaConfig.LEADER_REPLICATION_THROTTLED_RATE_CONFIG, ""),
                        AlterConfigOp.OpType.DELETE),
                    new AlterConfigOp(new ConfigEntry(QuotaConfig.FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG, ""),
                        AlterConfigOp.OpType.DELETE),
                    new AlterConfigOp(new ConfigEntry(QuotaConfig.REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG, ""),
                        AlterConfigOp.OpType.DELETE)
                ))).all().get();
            TestUtils.waitForCondition(() -> {
                Map<String, String> broker0Configs = new HashMap<>();
                admin.describeConfigs(List.of(broker0Resource)).all().get().get(broker0Resource).entries()
                    .forEach(entry -> broker0Configs.put(entry.name(), entry.value()));
                return "".equals(broker0Configs.getOrDefault(QuotaConfig.LEADER_REPLICATION_THROTTLED_RATE_CONFIG, "")) &&
                    "".equals(broker0Configs.getOrDefault(QuotaConfig.FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG, "")) &&
                    "".equals(broker0Configs.getOrDefault(QuotaConfig.REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG, ""));
            }, "Expected to see the broker properties we just removed to be deleted");
        }
    }

    @ClusterTest
    public void testInvalidIncrementalAlterConfigs() throws Exception {
        try (Admin admin = clusterInstance.admin()) {
            // Create topics
            String topic1 = "incremental-alter-configs-topic-1";
            ConfigResource topic1Resource = new ConfigResource(ConfigResource.Type.TOPIC, topic1);
            clusterInstance.createTopic(topic1, 1, (short) 1);

            String topic2 = "incremental-alter-configs-topic-2";
            ConfigResource topic2Resource = new ConfigResource(ConfigResource.Type.TOPIC, topic2);
            clusterInstance.createTopic(topic2, 1, (short) 1);

            // Add duplicate Keys for topic1
            List<AlterConfigOp> topic1AlterConfigs = List.of(
                new AlterConfigOp(new ConfigEntry(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0.75"), AlterConfigOp.OpType.SET),
                new AlterConfigOp(new ConfigEntry(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0.65"), AlterConfigOp.OpType.SET),
                new AlterConfigOp(new ConfigEntry(TopicConfig.COMPRESSION_TYPE_CONFIG, "gzip"), AlterConfigOp.OpType.SET) // valid entry
            );

            // Add valid config for topic2
            List<AlterConfigOp> topic2AlterConfigs = List.of(
                new AlterConfigOp(new ConfigEntry(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0.9"), AlterConfigOp.OpType.SET)
            );

            AlterConfigsResult alterResult = admin.incrementalAlterConfigs(Map.of(
                topic1Resource, topic1AlterConfigs,
                topic2Resource, topic2AlterConfigs
            ));
            assertEquals(Set.of(topic1Resource, topic2Resource), alterResult.values().keySet());

            // InvalidRequestException error for topic1
            assertFutureThrows(InvalidRequestException.class, alterResult.values().get(topic1Resource),
                "Error due to duplicate config keys");

            // Operation should succeed for topic2
            alterResult.values().get(topic2Resource).get();
            clusterInstance.ensureConsistentMetadata();

            // Verify that topic1 is not config not updated, and topic2 config is updated
            DescribeConfigsResult describeResult = admin.describeConfigs(List.of(topic1Resource, topic2Resource));
            Map<ConfigResource, Config> configs = describeResult.all().get();
            assertEquals(2, configs.size());

            assertEquals(String.valueOf(LogConfig.DEFAULT_MIN_CLEANABLE_DIRTY_RATIO), configs.get(topic1Resource).get(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG).value());
            assertEquals(ServerLogConfigs.COMPRESSION_TYPE_DEFAULT, configs.get(topic1Resource).get(TopicConfig.COMPRESSION_TYPE_CONFIG).value());
            assertEquals("0.9", configs.get(topic2Resource).get(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG).value());

            // Check invalid use of append/subtract operation types
            topic1AlterConfigs = List.of(
                new AlterConfigOp(new ConfigEntry(TopicConfig.COMPRESSION_TYPE_CONFIG, "gzip"), AlterConfigOp.OpType.APPEND)
            );

            topic2AlterConfigs = List.of(
                new AlterConfigOp(new ConfigEntry(TopicConfig.COMPRESSION_TYPE_CONFIG, "snappy"), AlterConfigOp.OpType.SUBTRACT)
            );

            alterResult = admin.incrementalAlterConfigs(Map.of(
                topic1Resource, topic1AlterConfigs,
                topic2Resource, topic2AlterConfigs
            ));
            assertEquals(Set.of(topic1Resource, topic2Resource), alterResult.values().keySet());

            assertFutureThrows(InvalidConfigurationException.class, alterResult.values().get(topic1Resource),
                "Can't APPEND to key compression.type because its type is not LIST.");

            assertFutureThrows(InvalidConfigurationException.class, alterResult.values().get(topic2Resource),
                "Can't SUBTRACT to key compression.type because its type is not LIST.");

            // Try to add invalid config
            topic1AlterConfigs = List.of(
                new AlterConfigOp(new ConfigEntry(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "1.1"), AlterConfigOp.OpType.SET)
            );

            alterResult = admin.incrementalAlterConfigs(Map.of(
                topic1Resource, topic1AlterConfigs
            ));
            assertEquals(Set.of(topic1Resource), alterResult.values().keySet());

            assertFutureThrows(InvalidConfigurationException.class, alterResult.values().get(topic1Resource),
                "Invalid value 1.1 for configuration min.cleanable.dirty.ratio: Value must be no more than 1");
        }
    }

    @ClusterTest
    public void testIncrementalAlterAndDescribeGroupConfigs() throws Exception {
        try (Admin admin = clusterInstance.admin()) {
            String group = "describe-alter-configs-group";
            ConfigResource groupResource = new ConfigResource(ConfigResource.Type.GROUP, group);

            // Alter group configs
            List<AlterConfigOp> groupAlterConfigs = List.of(
                new AlterConfigOp(new ConfigEntry(GroupConfig.CONSUMER_SESSION_TIMEOUT_MS_CONFIG, "50000"), AlterConfigOp.OpType.SET),
                new AlterConfigOp(new ConfigEntry(GroupConfig.CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG, ""), AlterConfigOp.OpType.DELETE)
            );

            AlterConfigsResult alterResult = admin.incrementalAlterConfigs(Map.of(
                groupResource, groupAlterConfigs
            ));

            assertEquals(Set.of(groupResource), alterResult.values().keySet());
            alterResult.all().get(15, TimeUnit.SECONDS);

            clusterInstance.ensureConsistentMetadata();

            // Describe group config, verify that group config was updated correctly
            DescribeConfigsResult describeResult = admin.describeConfigs(List.of(groupResource));
            Map<ConfigResource, Config> configs = describeResult.all().get(15, TimeUnit.SECONDS);

            assertEquals(1, configs.size());

            assertEquals("50000", configs.get(groupResource).get(GroupConfig.CONSUMER_SESSION_TIMEOUT_MS_CONFIG).value());
            assertEquals(ConfigEntry.ConfigSource.DYNAMIC_GROUP_CONFIG, configs.get(groupResource).get(GroupConfig.CONSUMER_SESSION_TIMEOUT_MS_CONFIG).source());
            assertEquals(String.valueOf(GroupCoordinatorConfig.CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_DEFAULT), configs.get(groupResource).get(GroupConfig.CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG).value());
            assertEquals(ConfigEntry.ConfigSource.DEFAULT_CONFIG, configs.get(groupResource).get(GroupConfig.CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG).source());

            // Alter group with validateOnly=true
            groupAlterConfigs = List.of(
                new AlterConfigOp(new ConfigEntry(GroupConfig.CONSUMER_SESSION_TIMEOUT_MS_CONFIG, "60000"), AlterConfigOp.OpType.SET)
            );

            alterResult = admin.incrementalAlterConfigs(Map.of(
                groupResource, groupAlterConfigs
            ), new AlterConfigsOptions().validateOnly(true));
            alterResult.all().get(15, TimeUnit.SECONDS);

            // Verify that group config was not updated due to validateOnly = true
            describeResult = admin.describeConfigs(List.of(groupResource));
            configs = describeResult.all().get(15, TimeUnit.SECONDS);

            assertEquals("50000", configs.get(groupResource).get(GroupConfig.CONSUMER_SESSION_TIMEOUT_MS_CONFIG).value());

            // Alter group with validateOnly=true with invalid configs
            groupAlterConfigs = List.of(
                new AlterConfigOp(new ConfigEntry(GroupConfig.CONSUMER_SESSION_TIMEOUT_MS_CONFIG, "5"), AlterConfigOp.OpType.SET)
            );

            alterResult = admin.incrementalAlterConfigs(Map.of(
                groupResource, groupAlterConfigs
            ), new AlterConfigsOptions().validateOnly(true));

            assertFutureThrows(InvalidConfigurationException.class,
                alterResult.values().get(groupResource),
                "consumer.session.timeout.ms must be in the range 45000 to 60000 inclusive.");
        }
    }

    @ClusterTest
    public void testInvalidAlterConfigs() throws Exception {
        try (Admin admin = clusterInstance.admin()) {
            checkInvalidAlterConfigs(admin);
        }
    }

    private void checkInvalidAlterConfigs(Admin admin) throws Exception {
        // Create topics
        String topic1 = "invalid-alter-configs-topic-1";
        ConfigResource topicResource1 = new ConfigResource(ConfigResource.Type.TOPIC, topic1);
        clusterInstance.createTopic(topic1, 1, (short) 1);

        String topic2 = "invalid-alter-configs-topic-2";
        ConfigResource topicResource2 = new ConfigResource(ConfigResource.Type.TOPIC, topic2);
        clusterInstance.createTopic(topic2, 1, (short) 1);

        ConfigResource brokerResource = new ConfigResource(ConfigResource.Type.BROKER,
            String.valueOf(clusterInstance.brokerIds().iterator().next()));

        // Alter configs: first and third are invalid, second is valid
        Map<ConfigResource, Collection<AlterConfigOp>> alterConfigs = new HashMap<>();
        alterConfigs.put(topicResource1, List.of(
            new AlterConfigOp(new ConfigEntry(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "1.1"), AlterConfigOp.OpType.SET),
            new AlterConfigOp(new ConfigEntry(TopicConfig.COMPRESSION_TYPE_CONFIG, "lz4"), AlterConfigOp.OpType.SET)
        ));
        alterConfigs.put(topicResource2, List.of(new AlterConfigOp(new ConfigEntry(TopicConfig.COMPRESSION_TYPE_CONFIG, "snappy"), AlterConfigOp.OpType.SET)));
        alterConfigs.put(brokerResource, List.of(new AlterConfigOp(new ConfigEntry(SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG, "EXTERNAL://localhost:0,INTERNAL://localhost:0"), AlterConfigOp.OpType.SET)));
        AlterConfigsResult alterResult = admin.incrementalAlterConfigs(alterConfigs);

        assertEquals(Set.of(topicResource1, topicResource2, brokerResource), alterResult.values().keySet());
        assertFutureThrows(InvalidConfigurationException.class, alterResult.values().get(topicResource1));
        alterResult.values().get(topicResource2).get();
        assertFutureThrows(InvalidRequestException.class, alterResult.values().get(brokerResource));

        // Verify that first and third resources were not updated and second was updated
        clusterInstance.ensureConsistentMetadata();
        DescribeConfigsResult describeResult = admin.describeConfigs(List.of(topicResource1, topicResource2, brokerResource));
        Map<ConfigResource, Config> configs = describeResult.all().get();
        assertEquals(3, configs.size());

        assertEquals(String.valueOf(LogConfig.DEFAULT_MIN_CLEANABLE_DIRTY_RATIO),
            configs.get(topicResource1).get(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG).value());
        assertEquals(ServerLogConfigs.COMPRESSION_TYPE_DEFAULT,
            configs.get(topicResource1).get(TopicConfig.COMPRESSION_TYPE_CONFIG).value());

        assertEquals("snappy", configs.get(topicResource2).get(TopicConfig.COMPRESSION_TYPE_CONFIG).value());

        assertEquals(ServerLogConfigs.COMPRESSION_TYPE_DEFAULT, configs.get(brokerResource).get(ServerConfigs.COMPRESSION_TYPE_CONFIG).value());

        // Alter configs with validateOnly = true: first and third are invalid, second is valid
        alterConfigs.put(topicResource1, List.of(
            new AlterConfigOp(new ConfigEntry(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "1.1"), AlterConfigOp.OpType.SET),
            new AlterConfigOp(new ConfigEntry(TopicConfig.COMPRESSION_TYPE_CONFIG, "lz4"), AlterConfigOp.OpType.SET)
        ));
        alterConfigs.put(topicResource2, List.of(new AlterConfigOp(new ConfigEntry(TopicConfig.COMPRESSION_TYPE_CONFIG, "gzip"), AlterConfigOp.OpType.SET)));
        alterConfigs.put(brokerResource, List.of(new AlterConfigOp(new ConfigEntry(SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG, "EXTERNAL://localhost:0,INTERNAL://localhost:0"), AlterConfigOp.OpType.SET)));
        alterResult = admin.incrementalAlterConfigs(alterConfigs, new AlterConfigsOptions().validateOnly(true));

        assertEquals(Set.of(topicResource1, topicResource2, brokerResource), alterResult.values().keySet());
        assertFutureThrows(InvalidConfigurationException.class, alterResult.values().get(topicResource1));
        alterResult.values().get(topicResource2).get();
        assertFutureThrows(InvalidRequestException.class, alterResult.values().get(brokerResource));

        // Verify that no resources are updated since validate_only = true
        clusterInstance.ensureConsistentMetadata();
        describeResult = admin.describeConfigs(List.of(topicResource1, topicResource2, brokerResource));
        configs = describeResult.all().get();
        assertEquals(3, configs.size());

        assertEquals(String.valueOf(LogConfig.DEFAULT_MIN_CLEANABLE_DIRTY_RATIO),
            configs.get(topicResource1).get(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG).value());
        assertEquals(ServerLogConfigs.COMPRESSION_TYPE_DEFAULT,
            configs.get(topicResource1).get(TopicConfig.COMPRESSION_TYPE_CONFIG).value());

        assertEquals("snappy", configs.get(topicResource2).get(TopicConfig.COMPRESSION_TYPE_CONFIG).value());

        assertEquals(ServerLogConfigs.COMPRESSION_TYPE_DEFAULT, configs.get(brokerResource).get(ServerConfigs.COMPRESSION_TYPE_CONFIG).value());
    }

    // Verify that createTopics and alterConfigs fail with null values
    @ClusterTest
    public void testNullConfigs() throws Exception {
        String topic = "topic";
        try (Admin admin = clusterInstance.admin()) {
            Map<String, String> invalidConfigs = new HashMap<>();
            invalidConfigs.put(TopicConfig.RETENTION_BYTES_CONFIG, null);
            invalidConfigs.put(TopicConfig.COMPRESSION_TYPE_CONFIG, "producer");
            NewTopic newTopic = new NewTopic(topic, 2, (short) 3);
            assertFutureThrows(InvalidConfigurationException.class,
                admin.createTopics(List.of(newTopic.configs(invalidConfigs))).all(),
                "Null value not supported for topic configs: retention.bytes"
            );

            Map<String, String> validConfigs = Map.of(TopicConfig.COMPRESSION_TYPE_CONFIG, "producer");
            admin.createTopics(List.of(newTopic.configs(validConfigs))).all().get();
            clusterInstance.waitTopicCreation(topic, 2);
            validateLogConfig(topic, "producer");

            ConfigResource topicResource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
            List<AlterConfigOp> alterOps = List.of(
                new AlterConfigOp(new ConfigEntry(TopicConfig.RETENTION_BYTES_CONFIG, null), AlterConfigOp.OpType.SET),
                new AlterConfigOp(new ConfigEntry(TopicConfig.COMPRESSION_TYPE_CONFIG, "lz4"), AlterConfigOp.OpType.SET)
            );
            assertFutureThrows(InvalidRequestException.class,
                admin.incrementalAlterConfigs(Map.of(topicResource, alterOps)).all(),
                "Null value not supported for : retention.bytes"
            );
            validateLogConfig(topic, "producer");
        }
    }

    private void validateLogConfig(String topic, String compressionType) throws Exception {
        clusterInstance.ensureConsistentMetadata();
        Properties topicConfigs = clusterInstance.brokers().values().iterator().next().metadataCache().topicConfig(topic);
        LogConfig logConfig = LogConfig.fromProps(Map.of(), topicConfigs);

        assertEquals(compressionType, logConfig.originals().get(TopicConfig.COMPRESSION_TYPE_CONFIG));
        assertNull(logConfig.originals().get(TopicConfig.RETENTION_BYTES_CONFIG));
        assertEquals(ServerLogConfigs.LOG_RETENTION_BYTES_DEFAULT, logConfig.retentionSize);
    }

    @ClusterTest
    public void testAppendConfigToEmptyDefaultValue() throws Exception {
        testAppendConfig(Map.of(), "0:0", "0:0");
    }

    @ClusterTest
    public void testAppendConfigToExistentValue() throws Exception {
        Map<String, String> configs = Map.of(QuotaConfig.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG, "1:1");
        testAppendConfig(configs, "0:0", "1:1,0:0");
    }

    private void testAppendConfig(Map<String, String> configs, String append, String expected) throws Exception {
        try (Admin admin = clusterInstance.admin()) {
            String topic = "topic";
            clusterInstance.createTopic(topic, 1, (short) 1, configs);
            ConfigResource topicResource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
            List<AlterConfigOp> topicAlterConfigs = List.of(
                new AlterConfigOp(new ConfigEntry(QuotaConfig.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG, append), AlterConfigOp.OpType.APPEND)
            );

            AlterConfigsResult alterResult = admin.incrementalAlterConfigs(Map.of(
                topicResource, topicAlterConfigs
            ));
            alterResult.all().get(15, TimeUnit.SECONDS);

            clusterInstance.ensureConsistentMetadata();
            ConfigEntry config = admin.describeConfigs(List.of(topicResource)).all().get().get(topicResource).get(QuotaConfig.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG);
            assertEquals(expected, config.value());
        }
    }
}
