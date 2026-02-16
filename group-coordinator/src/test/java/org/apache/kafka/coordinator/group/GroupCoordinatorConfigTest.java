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
package org.apache.kafka.coordinator.group;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.record.internal.CompressionType;
import org.apache.kafka.coordinator.group.api.assignor.ConsumerGroupPartitionAssignor;
import org.apache.kafka.coordinator.group.api.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.api.assignor.GroupSpec;
import org.apache.kafka.coordinator.group.api.assignor.PartitionAssignorException;
import org.apache.kafka.coordinator.group.api.assignor.ShareGroupPartitionAssignor;
import org.apache.kafka.coordinator.group.api.assignor.SubscribedTopicDescriber;
import org.apache.kafka.coordinator.group.assignor.RangeAssignor;
import org.apache.kafka.coordinator.group.assignor.SimpleAssignor;
import org.apache.kafka.coordinator.group.assignor.UniformAssignor;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class GroupCoordinatorConfigTest {

    public static class CustomAssignor implements ConsumerGroupPartitionAssignor, Configurable, ShareGroupPartitionAssignor {
        public Map<String, ?> configs;

        @Override
        public void configure(Map<String, ?> configs) {
            this.configs = configs;
        }

        @Override
        public String name() {
            return "CustomAssignor";
        }

        @Override
        public GroupAssignment assign(
            GroupSpec groupSpec,
            SubscribedTopicDescriber subscribedTopicDescriber
        ) throws PartitionAssignorException {
            return null;
        }
    }

    @Test
    public void testConsumerGroupAssignorFullClassNames() {
        // The full class name of the assignors is part of our public api. Hence,
        // we should ensure that they are not changed by mistake.
        assertEquals(
            "org.apache.kafka.coordinator.group.assignor.UniformAssignor",
            UniformAssignor.class.getName()
        );
        assertEquals(
            "org.apache.kafka.coordinator.group.assignor.RangeAssignor",
            RangeAssignor.class.getName()
        );
    }

    @Test
    public void testConsumerGroupAssignors() {
        Map<String, Object> configs = new HashMap<>();
        GroupCoordinatorConfig config;
        List<ConsumerGroupPartitionAssignor> assignors;

        // Test short names.
        configs.put(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, "range, uniform");
        config = createConfig(configs);
        assignors = config.consumerGroupAssignors();
        assertEquals(2, assignors.size());
        assertInstanceOf(RangeAssignor.class, assignors.get(0));
        assertInstanceOf(UniformAssignor.class, assignors.get(1));

        // Test custom assignor.
        configs.put(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, CustomAssignor.class.getName());
        config = createConfig(configs);
        assignors = config.consumerGroupAssignors();
        assertEquals(1, assignors.size());
        assertInstanceOf(CustomAssignor.class, assignors.get(0));
        assertNotNull(((CustomAssignor) assignors.get(0)).configs);

        // Test with classes.
        configs.put(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(RangeAssignor.class, CustomAssignor.class));
        config = createConfig(configs);
        assignors = config.consumerGroupAssignors();
        assertEquals(2, assignors.size());
        assertInstanceOf(RangeAssignor.class, assignors.get(0));
        assertInstanceOf(CustomAssignor.class, assignors.get(1));

        // Test combination of short name and class.
        configs.put(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, "uniform, " + CustomAssignor.class.getName());
        config = createConfig(configs);
        assignors = config.consumerGroupAssignors();
        assertEquals(2, assignors.size());
        assertInstanceOf(UniformAssignor.class, assignors.get(0));
        assertInstanceOf(CustomAssignor.class, assignors.get(1));

        // Test combination of short name and class.
        configs.put(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of("uniform", CustomAssignor.class.getName()));
        config = createConfig(configs);
        assignors = config.consumerGroupAssignors();
        assertEquals(2, assignors.size());
        assertInstanceOf(UniformAssignor.class, assignors.get(0));
        assertInstanceOf(CustomAssignor.class, assignors.get(1));
    }

    @Test
    public void testShareGroupAssignorFullClassNames() {
        // The full class name of the assignors is part of our public api. Hence,
        // we should ensure that they are not changed by mistake.
        assertEquals(
            "org.apache.kafka.coordinator.group.assignor.SimpleAssignor",
            SimpleAssignor.class.getName()
        );
    }

    @Test
    public void testShareGroupAssignors() {
        Map<String, Object> configs = new HashMap<>();
        GroupCoordinatorConfig config;
        List<ShareGroupPartitionAssignor> assignors;

        // Test default config.
        config = createConfig(configs);
        assignors = config.shareGroupAssignors();
        assertEquals(1, assignors.size());

        // Test short names.
        configs.put(GroupCoordinatorConfig.SHARE_GROUP_ASSIGNORS_CONFIG, "simple");
        config = createConfig(configs);
        assignors = config.shareGroupAssignors();
        assertEquals(1, assignors.size());
        assertInstanceOf(SimpleAssignor.class, assignors.get(0));

        // Test custom assignor.
        configs.put(GroupCoordinatorConfig.SHARE_GROUP_ASSIGNORS_CONFIG, CustomAssignor.class.getName());
        config = createConfig(configs);
        assignors = config.shareGroupAssignors();
        assertEquals(1, assignors.size());
        assertInstanceOf(CustomAssignor.class, assignors.get(0));
        assertNotNull(((CustomAssignor) assignors.get(0)).configs);

        // Test must contain only one assignor.
        configs.put(GroupCoordinatorConfig.SHARE_GROUP_ASSIGNORS_CONFIG, "simple, " + CustomAssignor.class.getName());
        assertEquals("group.share.assignors must contain exactly one assignor, but found 2",
            assertThrows(IllegalArgumentException.class, () -> createConfig(configs)).getMessage());
    }

    @Test
    public void testConfigs() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(GroupCoordinatorConfig.GROUP_COORDINATOR_NUM_THREADS_CONFIG, 10);
        configs.put(GroupCoordinatorConfig.GROUP_COORDINATOR_APPEND_LINGER_MS_CONFIG, 10);
        configs.put(GroupCoordinatorConfig.CONSUMER_GROUP_SESSION_TIMEOUT_MS_CONFIG, 555);
        configs.put(GroupCoordinatorConfig.CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_CONFIG, 200);
        configs.put(GroupCoordinatorConfig.CONSUMER_GROUP_MAX_SIZE_CONFIG, 55);
        configs.put(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(RangeAssignor.class));
        configs.put(GroupCoordinatorConfig.OFFSETS_TOPIC_SEGMENT_BYTES_CONFIG, 2222);
        configs.put(GroupCoordinatorConfig.OFFSET_METADATA_MAX_SIZE_CONFIG, 3333);
        configs.put(GroupCoordinatorConfig.GROUP_MAX_SIZE_CONFIG, 60);
        configs.put(GroupCoordinatorConfig.GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, 3000);
        configs.put(GroupCoordinatorConfig.GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG, 120);
        configs.put(GroupCoordinatorConfig.GROUP_MAX_SESSION_TIMEOUT_MS_CONFIG, 10 * 60 * 1000);
        configs.put(GroupCoordinatorConfig.OFFSETS_RETENTION_CHECK_INTERVAL_MS_CONFIG, 600000);
        configs.put(GroupCoordinatorConfig.OFFSETS_RETENTION_MINUTES_CONFIG, 24 * 60 * 60 * 1000);
        configs.put(GroupCoordinatorConfig.OFFSET_COMMIT_TIMEOUT_MS_CONFIG, 5000);
        configs.put(GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG, ConsumerGroupMigrationPolicy.DISABLED.name());
        configs.put(GroupCoordinatorConfig.OFFSETS_TOPIC_COMPRESSION_CODEC_CONFIG, (int) CompressionType.GZIP.id);
        configs.put(GroupCoordinatorConfig.OFFSETS_LOAD_BUFFER_SIZE_CONFIG, 555);
        configs.put(GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, 111);
        configs.put(GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, (short) 11);
        configs.put(GroupCoordinatorConfig.CONSUMER_GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG, 333);
        configs.put(GroupCoordinatorConfig.CONSUMER_GROUP_MAX_SESSION_TIMEOUT_MS_CONFIG, 666);
        configs.put(GroupCoordinatorConfig.CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_CONFIG, 111);
        configs.put(GroupCoordinatorConfig.CONSUMER_GROUP_MAX_HEARTBEAT_INTERVAL_MS_CONFIG, 222);
        configs.put(GroupCoordinatorConfig.CONSUMER_GROUP_REGEX_REFRESH_INTERVAL_MS_CONFIG, 15 * 60 * 1000);
        configs.put(GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, 5000);
        configs.put(GroupCoordinatorConfig.CACHED_BUFFER_MAX_BYTES_CONFIG, 2 * 1024 * 1024);

        GroupCoordinatorConfig config = createConfig(configs);

        assertEquals(10, config.numThreads());
        assertEquals(555, config.consumerGroupSessionTimeoutMs());
        assertEquals(200, config.consumerGroupHeartbeatIntervalMs());
        assertEquals(55, config.consumerGroupMaxSize());
        assertEquals(1, config.consumerGroupAssignors().size());
        assertEquals(RangeAssignor.NAME, config.consumerGroupAssignors().get(0).name());
        assertEquals(2222, config.offsetsTopicSegmentBytes());
        assertEquals(3333, config.offsetMetadataMaxSize());
        assertEquals(60, config.classicGroupMaxSize());
        assertEquals(3000, config.classicGroupInitialRebalanceDelayMs());
        assertEquals(5 * 60 * 1000, config.classicGroupNewMemberJoinTimeoutMs());
        assertEquals(120, config.classicGroupMinSessionTimeoutMs());
        assertEquals(10 * 60 * 1000, config.classicGroupMaxSessionTimeoutMs());
        assertEquals(10 * 60 * 1000, config.offsetsRetentionCheckIntervalMs());
        assertEquals(Duration.ofMinutes(24 * 60 * 60 * 1000L).toMillis(), config.offsetsRetentionMs());
        assertEquals(5000, config.offsetCommitTimeoutMs());
        assertEquals(CompressionType.GZIP, config.offsetTopicCompressionType());
        assertEquals(OptionalInt.of(10), config.appendLingerMs());
        assertEquals(555, config.offsetsLoadBufferSize());
        assertEquals(111, config.offsetsTopicPartitions());
        assertEquals(11, config.offsetsTopicReplicationFactor());
        assertEquals(333, config.consumerGroupMinSessionTimeoutMs());
        assertEquals(666, config.consumerGroupMaxSessionTimeoutMs());
        assertEquals(111, config.consumerGroupMinHeartbeatIntervalMs());
        assertEquals(222, config.consumerGroupMaxHeartbeatIntervalMs());
        assertEquals(15 * 60 * 1000, config.consumerGroupRegexRefreshIntervalMs());
        assertEquals(5000, config.streamsGroupInitialRebalanceDelayMs());
        assertEquals(2 * 1024 * 1024, config.cachedBufferMaxBytes());
    }

    @Test
    public void testInvalidConfigs() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(GroupCoordinatorConfig.CONSUMER_GROUP_MAX_HEARTBEAT_INTERVAL_MS_CONFIG, 10);
        configs.put(GroupCoordinatorConfig.CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_CONFIG, 20);
        configs.put(GroupCoordinatorConfig.CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_CONFIG, 20);
        assertEquals("group.consumer.max.heartbeat.interval.ms must be greater than or equal to group.consumer.min.heartbeat.interval.ms",
                assertThrows(IllegalArgumentException.class, () -> createConfig(configs)).getMessage());

        configs.clear();
        configs.put(GroupCoordinatorConfig.CONSUMER_GROUP_MAX_HEARTBEAT_INTERVAL_MS_CONFIG, 30);
        configs.put(GroupCoordinatorConfig.CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_CONFIG, 20);
        configs.put(GroupCoordinatorConfig.CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_CONFIG, 10);
        assertEquals("group.consumer.heartbeat.interval.ms must be greater than or equal to group.consumer.min.heartbeat.interval.ms",
                assertThrows(IllegalArgumentException.class, () -> createConfig(configs)).getMessage());

        configs.clear();
        configs.put(GroupCoordinatorConfig.CONSUMER_GROUP_MAX_HEARTBEAT_INTERVAL_MS_CONFIG, 30);
        configs.put(GroupCoordinatorConfig.CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_CONFIG, 20);
        configs.put(GroupCoordinatorConfig.CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_CONFIG, 40);
        assertEquals("group.consumer.heartbeat.interval.ms must be less than or equal to group.consumer.max.heartbeat.interval.ms",
                assertThrows(IllegalArgumentException.class, () -> createConfig(configs)).getMessage());

        configs.clear();
        configs.put(GroupCoordinatorConfig.CONSUMER_GROUP_MAX_SESSION_TIMEOUT_MS_CONFIG, 10);
        configs.put(GroupCoordinatorConfig.CONSUMER_GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG, 20);
        configs.put(GroupCoordinatorConfig.CONSUMER_GROUP_SESSION_TIMEOUT_MS_CONFIG, 20);
        assertEquals("group.consumer.max.session.timeout.ms must be greater than or equal to group.consumer.min.session.timeout.ms",
                assertThrows(IllegalArgumentException.class, () -> createConfig(configs)).getMessage());

        configs.clear();
        configs.put(GroupCoordinatorConfig.CONSUMER_GROUP_MAX_SESSION_TIMEOUT_MS_CONFIG, 30);
        configs.put(GroupCoordinatorConfig.CONSUMER_GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG, 20);
        configs.put(GroupCoordinatorConfig.CONSUMER_GROUP_SESSION_TIMEOUT_MS_CONFIG, 10);
        assertEquals("group.consumer.session.timeout.ms must be greater than or equal to group.consumer.min.session.timeout.ms",
                assertThrows(IllegalArgumentException.class, () -> createConfig(configs)).getMessage());

        configs.clear();
        configs.put(GroupCoordinatorConfig.CONSUMER_GROUP_MAX_SESSION_TIMEOUT_MS_CONFIG, 30);
        configs.put(GroupCoordinatorConfig.CONSUMER_GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG, 20);
        configs.put(GroupCoordinatorConfig.CONSUMER_GROUP_SESSION_TIMEOUT_MS_CONFIG, 40);
        assertEquals("group.consumer.session.timeout.ms must be less than or equal to group.consumer.max.session.timeout.ms",
                assertThrows(IllegalArgumentException.class, () -> createConfig(configs)).getMessage());

        configs.clear();
        configs.put(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, Object.class);
        assertEquals("Invalid value class java.lang.Object for configuration group.consumer.assignors: Expected a comma separated list.",
                assertThrows(ConfigException.class, () -> createConfig(configs)).getMessage());

        configs.clear();
        configs.put(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(Object.class));
        assertEquals("class java.lang.Object is not an instance of org.apache.kafka.coordinator.group.api.assignor.ConsumerGroupPartitionAssignor",
                assertThrows(KafkaException.class, () -> createConfig(configs)).getMessage());

        configs.clear();
        configs.put(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, Object.class.getName());
        assertEquals("java.lang.Object is not an instance of org.apache.kafka.coordinator.group.api.assignor.ConsumerGroupPartitionAssignor",
            assertThrows(KafkaException.class, () -> createConfig(configs)).getMessage());

        configs.clear();
        configs.put(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, "foo");
        assertEquals("Class foo cannot be found",
            assertThrows(KafkaException.class, () -> createConfig(configs)).getMessage());

        configs.clear();
        configs.put(GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG, "foobar");
        assertEquals("Invalid value foobar for configuration group.consumer.migration.policy: String must be one of (case insensitive): DISABLED, DOWNGRADE, UPGRADE, BIDIRECTIONAL",
                assertThrows(ConfigException.class, () -> createConfig(configs)).getMessage());

        configs.clear();
        configs.put(GroupCoordinatorConfig.OFFSETS_TOPIC_COMPRESSION_CODEC_CONFIG, -100);
        assertEquals("Unknown compression type id: -100",
                assertThrows(IllegalArgumentException.class, () -> createConfig(configs)).getMessage());

        configs.clear();
        configs.put(GroupCoordinatorConfig.CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_CONFIG, 45000);
        configs.put(GroupCoordinatorConfig.CONSUMER_GROUP_MAX_HEARTBEAT_INTERVAL_MS_CONFIG, 60000);
        configs.put(GroupCoordinatorConfig.CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_CONFIG, 50000);
        configs.put(GroupCoordinatorConfig.CONSUMER_GROUP_SESSION_TIMEOUT_MS_CONFIG, 50000);
        assertEquals("group.consumer.heartbeat.interval.ms must be less than group.consumer.session.timeout.ms",
                assertThrows(IllegalArgumentException.class, () -> createConfig(configs)).getMessage());

        configs.clear();
        configs.put(GroupCoordinatorConfig.OFFSET_COMMIT_TIMEOUT_MS_CONFIG, 5000);
        configs.put(GroupCoordinatorConfig.SHARE_GROUP_INITIALIZE_RETRY_INTERVAL_MS_CONFIG, 1000);
        assertEquals(5000, createConfig(configs).shareGroupInitializeRetryIntervalMs());

        configs.clear();
        configs.put(GroupCoordinatorConfig.STREAMS_GROUP_MIN_HEARTBEAT_INTERVAL_MS_CONFIG, 45000);
        configs.put(GroupCoordinatorConfig.STREAMS_GROUP_MAX_HEARTBEAT_INTERVAL_MS_CONFIG, 60000);
        configs.put(GroupCoordinatorConfig.STREAMS_GROUP_HEARTBEAT_INTERVAL_MS_CONFIG, 50000);
        configs.put(GroupCoordinatorConfig.STREAMS_GROUP_SESSION_TIMEOUT_MS_CONFIG, 50000);
        assertEquals("group.streams.heartbeat.interval.ms must be less than group.streams.session.timeout.ms",
            assertThrows(IllegalArgumentException.class, () -> createConfig(configs)).getMessage());

        configs.clear();
        configs.put(GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, -1);
        assertEquals("Invalid value -1 for configuration group.streams.initial.rebalance.delay.ms: Value must be at least 0",
            assertThrows(ConfigException.class, () -> createConfig(configs)).getMessage());
    }

    @Test
    public void testAppendLingerMs() {
        GroupCoordinatorConfig config = createConfig(Map.of(GroupCoordinatorConfig.GROUP_COORDINATOR_APPEND_LINGER_MS_CONFIG, -1));
        assertEquals(OptionalInt.empty(), config.appendLingerMs());

        config = createConfig(Map.of(GroupCoordinatorConfig.GROUP_COORDINATOR_APPEND_LINGER_MS_CONFIG, 0));
        assertEquals(OptionalInt.of(0), config.appendLingerMs());

        config = createConfig(Map.of(GroupCoordinatorConfig.GROUP_COORDINATOR_APPEND_LINGER_MS_CONFIG, 5));
        assertEquals(OptionalInt.of(5), config.appendLingerMs());
    }

    public static GroupCoordinatorConfig createGroupCoordinatorConfig(
        int offsetMetadataMaxSize,
        long offsetsRetentionCheckIntervalMs,
        int offsetsRetentionMinutes
    ) {
        Map<String, Object> configs = new HashMap<>();
        configs.put(GroupCoordinatorConfig.GROUP_COORDINATOR_NUM_THREADS_CONFIG, 1);
        configs.put(GroupCoordinatorConfig.GROUP_COORDINATOR_APPEND_LINGER_MS_CONFIG, 10);
        configs.put(GroupCoordinatorConfig.CONSUMER_GROUP_SESSION_TIMEOUT_MS_CONFIG, 45);
        configs.put(GroupCoordinatorConfig.CONSUMER_GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG, 45);
        configs.put(GroupCoordinatorConfig.CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_CONFIG, 5);
        configs.put(GroupCoordinatorConfig.CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_CONFIG, 5);
        configs.put(GroupCoordinatorConfig.CONSUMER_GROUP_MAX_SIZE_CONFIG, Integer.MAX_VALUE);
        configs.put(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(RangeAssignor.class));
        configs.put(GroupCoordinatorConfig.OFFSETS_TOPIC_SEGMENT_BYTES_CONFIG, 1000);
        configs.put(GroupCoordinatorConfig.OFFSET_METADATA_MAX_SIZE_CONFIG, offsetMetadataMaxSize);
        configs.put(GroupCoordinatorConfig.GROUP_MAX_SIZE_CONFIG, Integer.MAX_VALUE);
        configs.put(GroupCoordinatorConfig.GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, 3000);
        configs.put(GroupCoordinatorConfig.GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG, 120);
        configs.put(GroupCoordinatorConfig.GROUP_MAX_SESSION_TIMEOUT_MS_CONFIG, 10 * 5 * 1000);
        configs.put(GroupCoordinatorConfig.OFFSETS_RETENTION_CHECK_INTERVAL_MS_CONFIG, offsetsRetentionCheckIntervalMs);
        configs.put(GroupCoordinatorConfig.OFFSETS_RETENTION_MINUTES_CONFIG, offsetsRetentionMinutes);
        configs.put(GroupCoordinatorConfig.OFFSET_COMMIT_TIMEOUT_MS_CONFIG, 5000);
        configs.put(GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG, ConsumerGroupMigrationPolicy.DISABLED.name());
        configs.put(GroupCoordinatorConfig.OFFSETS_TOPIC_COMPRESSION_CODEC_CONFIG, (int) CompressionType.NONE.id);
        configs.put(GroupCoordinatorConfig.SHARE_GROUP_SESSION_TIMEOUT_MS_CONFIG, 45);
        configs.put(GroupCoordinatorConfig.SHARE_GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG, 45);
        configs.put(GroupCoordinatorConfig.SHARE_GROUP_HEARTBEAT_INTERVAL_MS_CONFIG, 5);
        configs.put(GroupCoordinatorConfig.SHARE_GROUP_MIN_HEARTBEAT_INTERVAL_MS_CONFIG, 5);
        configs.put(GroupCoordinatorConfig.SHARE_GROUP_MAX_SIZE_CONFIG, 1000);
        configs.put(GroupCoordinatorConfig.CACHED_BUFFER_MAX_BYTES_CONFIG, 1024 * 1024);

        return createConfig(configs);
    }

    @Test
    public void testStreamsGroupInitialRebalanceDelayDefaultValue() {
        Map<String, Object> configs = new HashMap<>();
        GroupCoordinatorConfig config = createConfig(configs);
        assertEquals(3000, config.streamsGroupInitialRebalanceDelayMs());
        assertEquals(GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_DEFAULT,
            config.streamsGroupInitialRebalanceDelayMs());
    }

    @Test
    public void testStreamsGroupInitialRebalanceDelayCustomValue() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, 7000);
        GroupCoordinatorConfig config = createConfig(configs);
        assertEquals(7000, config.streamsGroupInitialRebalanceDelayMs());
    }

    public static GroupCoordinatorConfig createConfig(Map<String, Object> configs) {
        return new GroupCoordinatorConfig(new AbstractConfig(
            GroupCoordinatorConfig.CONFIG_DEF,
            configs,
            false
        ));
    }
}
