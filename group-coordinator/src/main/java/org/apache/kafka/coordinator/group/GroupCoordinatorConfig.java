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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.group.api.assignor.ConsumerGroupPartitionAssignor;
import org.apache.kafka.coordinator.group.assignor.RangeAssignor;
import org.apache.kafka.coordinator.group.assignor.UniformAssignor;
import org.apache.kafka.coordinator.group.modern.share.ShareGroupConfig;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.kafka.common.config.ConfigDef.Importance.HIGH;
import static org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM;
import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.Range.between;
import static org.apache.kafka.common.config.ConfigDef.Type.BOOLEAN;
import static org.apache.kafka.common.config.ConfigDef.Type.INT;
import static org.apache.kafka.common.config.ConfigDef.Type.LIST;
import static org.apache.kafka.common.config.ConfigDef.Type.LONG;
import static org.apache.kafka.common.config.ConfigDef.Type.SHORT;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;
import static org.apache.kafka.common.utils.Utils.require;

/**
 * The group coordinator configurations.
 * This configuration utilizes several local variables instead of calling AbstractConfig#get.... as all configs here
 * are static and non-dynamic, with some being accessed extremely frequently (e.g., offsets.commit.timeout.ms).
 * Using local variable is advantageous as it avoids the overhead of repeatedly looking up these configurations in AbstractConfig.
 */
public class GroupCoordinatorConfig {
    /** ********* Group coordinator configuration ***********/
    public static final String GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG = "group.min.session.timeout.ms";
    public static final String GROUP_MIN_SESSION_TIMEOUT_MS_DOC = "The minimum allowed session timeout for registered consumers. Shorter timeouts result in quicker failure detection at the cost of more frequent consumer heartbeating, which can overwhelm broker resources.";
    public static final int GROUP_MIN_SESSION_TIMEOUT_MS_DEFAULT = 6000;

    public static final String GROUP_MAX_SESSION_TIMEOUT_MS_CONFIG = "group.max.session.timeout.ms";
    public static final String GROUP_MAX_SESSION_TIMEOUT_MS_DOC = "The maximum allowed session timeout for registered consumers. Longer timeouts give consumers more time to process messages in between heartbeats at the cost of a longer time to detect failures.";
    public static final int GROUP_MAX_SESSION_TIMEOUT_MS_DEFAULT = 1800000;

    public static final String GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG = "group.initial.rebalance.delay.ms";
    public static final String GROUP_INITIAL_REBALANCE_DELAY_MS_DOC = "The amount of time the group coordinator will wait for more consumers to join a new group before performing the first rebalance. A longer delay means potentially fewer rebalances, but increases the time until processing begins.";
    public static final int GROUP_INITIAL_REBALANCE_DELAY_MS_DEFAULT = 3000;

    public static final String GROUP_MAX_SIZE_CONFIG = "group.max.size";
    public static final String GROUP_MAX_SIZE_DOC = "The maximum number of consumers that a single consumer group can accommodate.";
    public static final int GROUP_MAX_SIZE_DEFAULT = Integer.MAX_VALUE;

    /** New group coordinator configs */
    public static final String NEW_GROUP_COORDINATOR_ENABLE_CONFIG = "group.coordinator.new.enable";
    public static final String NEW_GROUP_COORDINATOR_ENABLE_DOC = "Enable the new group coordinator.";
    public static final boolean NEW_GROUP_COORDINATOR_ENABLE_DEFAULT = true;

    public static final String GROUP_COORDINATOR_REBALANCE_PROTOCOLS_CONFIG = "group.coordinator.rebalance.protocols";
    public static final String GROUP_COORDINATOR_REBALANCE_PROTOCOLS_DOC = "The list of enabled rebalance protocols." +
            "The " + Group.GroupType.SHARE + " rebalance protocol is in early access and therefore must not be used in production.";
    public static final List<String> GROUP_COORDINATOR_REBALANCE_PROTOCOLS_DEFAULT = List.of(
        Group.GroupType.CLASSIC.toString(),
        Group.GroupType.CONSUMER.toString()
    );
    public static final String GROUP_COORDINATOR_APPEND_LINGER_MS_CONFIG = "group.coordinator.append.linger.ms";
    public static final String GROUP_COORDINATOR_APPEND_LINGER_MS_DOC = "The duration in milliseconds that the coordinator will " +
        "wait for writes to accumulate before flushing them to disk. Transactional writes are not accumulated.";
    public static final int GROUP_COORDINATOR_APPEND_LINGER_MS_DEFAULT = 5;

    public static final String GROUP_COORDINATOR_NUM_THREADS_CONFIG = "group.coordinator.threads";
    public static final String GROUP_COORDINATOR_NUM_THREADS_DOC = "The number of threads used by the group coordinator.";
    public static final int GROUP_COORDINATOR_NUM_THREADS_DEFAULT = 1;

    /** Consumer group configs */
    public static final String CONSUMER_GROUP_SESSION_TIMEOUT_MS_CONFIG = "group.consumer.session.timeout.ms";
    public static final String CONSUMER_GROUP_SESSION_TIMEOUT_MS_DOC = "The timeout to detect client failures when using the consumer group protocol.";
    public static final int CONSUMER_GROUP_SESSION_TIMEOUT_MS_DEFAULT = 45000;

    public static final String CONSUMER_GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG = "group.consumer.min.session.timeout.ms";
    public static final String CONSUMER_GROUP_MIN_SESSION_TIMEOUT_MS_DOC = "The minimum allowed session timeout for registered consumers.";
    public static final int CONSUMER_GROUP_MIN_SESSION_TIMEOUT_MS_DEFAULT = 45000;

    public static final String CONSUMER_GROUP_MAX_SESSION_TIMEOUT_MS_CONFIG = "group.consumer.max.session.timeout.ms";
    public static final String CONSUMER_GROUP_MAX_SESSION_TIMEOUT_MS_DOC = "The maximum allowed session timeout for registered consumers.";
    public static final int CONSUMER_GROUP_MAX_SESSION_TIMEOUT_MS_DEFAULT = 60000;

    public static final String CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_CONFIG = "group.consumer.heartbeat.interval.ms";
    public static final String CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_DOC = "The heartbeat interval given to the members of a consumer group.";
    public static final int CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_DEFAULT = 5000;

    public static final String CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_CONFIG = "group.consumer.min.heartbeat.interval.ms";
    public static final String CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_DOC = "The minimum heartbeat interval for registered consumers.";
    public static final int CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_DEFAULT = 5000;

    public static final String CONSUMER_GROUP_MAX_HEARTBEAT_INTERVAL_MS_CONFIG = "group.consumer.max.heartbeat.interval.ms";
    public static final String CONSUMER_GROUP_MAX_HEARTBEAT_INTERVAL_MS_DOC = "The maximum heartbeat interval for registered consumers.";
    public static final int CONSUMER_GROUP_MAX_HEARTBEAT_INTERVAL_MS_DEFAULT = 15000;

    public static final String CONSUMER_GROUP_MAX_SIZE_CONFIG = "group.consumer.max.size";
    public static final String CONSUMER_GROUP_MAX_SIZE_DOC = "The maximum number of consumers that a single consumer group can accommodate. This value will only impact the new consumer coordinator. To configure the classic consumer coordinator check " + GROUP_MAX_SIZE_CONFIG + " instead.";
    public static final int CONSUMER_GROUP_MAX_SIZE_DEFAULT = Integer.MAX_VALUE;

    public static final String CONSUMER_GROUP_ASSIGNORS_CONFIG = "group.consumer.assignors";
    public static final String CONSUMER_GROUP_ASSIGNORS_DOC = "The server side assignors as a list of full class names. The first one in the list is considered as the default assignor to be used in the case where the consumer does not specify an assignor.";
    public static final List<String> CONSUMER_GROUP_ASSIGNORS_DEFAULT = List.of(
        UniformAssignor.class.getName(),
        RangeAssignor.class.getName()
    );

    public static final String CONSUMER_GROUP_MIGRATION_POLICY_CONFIG = "group.consumer.migration.policy";
    public static final String CONSUMER_GROUP_MIGRATION_POLICY_DEFAULT = ConsumerGroupMigrationPolicy.BIDIRECTIONAL.toString();
    public static final String CONSUMER_GROUP_MIGRATION_POLICY_DOC = "The config that enables converting the non-empty classic group using the consumer embedded protocol to the non-empty consumer group using the consumer group protocol and vice versa; " +
            "conversions of empty groups in both directions are always enabled regardless of this policy. " +
            ConsumerGroupMigrationPolicy.BIDIRECTIONAL + ": both upgrade from classic group to consumer group and downgrade from consumer group to classic group are enabled, " +
            ConsumerGroupMigrationPolicy.UPGRADE + ": only upgrade from classic group to consumer group is enabled, " +
            ConsumerGroupMigrationPolicy.DOWNGRADE + ": only downgrade from consumer group to classic group is enabled, " +
            ConsumerGroupMigrationPolicy.DISABLED + ": neither upgrade nor downgrade is enabled.";

    /** Share group configs */
    public static final String SHARE_GROUP_MAX_SIZE_CONFIG = "group.share.max.size";
    public static final int SHARE_GROUP_MAX_SIZE_DEFAULT = 200;
    public static final String SHARE_GROUP_MAX_SIZE_DOC = "The maximum number of members that a single share group can accommodate.";

    public static final String SHARE_GROUP_SESSION_TIMEOUT_MS_CONFIG = "group.share.session.timeout.ms";
    public static final int SHARE_GROUP_SESSION_TIMEOUT_MS_DEFAULT = 45000;
    public static final String SHARE_GROUP_SESSION_TIMEOUT_MS_DOC = "The timeout to detect client failures when using the share group protocol.";

    public static final String SHARE_GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG = "group.share.min.session.timeout.ms";
    public static final int SHARE_GROUP_MIN_SESSION_TIMEOUT_MS_DEFAULT = 45000;
    public static final String SHARE_GROUP_MIN_SESSION_TIMEOUT_MS_DOC = "The minimum allowed session timeout for share group members.";

    public static final String SHARE_GROUP_MAX_SESSION_TIMEOUT_MS_CONFIG = "group.share.max.session.timeout.ms";
    public static final int SHARE_GROUP_MAX_SESSION_TIMEOUT_MS_DEFAULT = 60000;
    public static final String SHARE_GROUP_MAX_SESSION_TIMEOUT_MS_DOC = "The maximum allowed session timeout for share group members.";

    public static final String SHARE_GROUP_HEARTBEAT_INTERVAL_MS_CONFIG = "group.share.heartbeat.interval.ms";
    public static final int SHARE_GROUP_HEARTBEAT_INTERVAL_MS_DEFAULT = 5000;
    public static final String SHARE_GROUP_HEARTBEAT_INTERVAL_MS_DOC = "The heartbeat interval given to the members of a share group.";

    public static final String SHARE_GROUP_MIN_HEARTBEAT_INTERVAL_MS_CONFIG = "group.share.min.heartbeat.interval.ms";
    public static final int SHARE_GROUP_MIN_HEARTBEAT_INTERVAL_MS_DEFAULT = 5000;
    public static final String SHARE_GROUP_MIN_HEARTBEAT_INTERVAL_MS_DOC = "The minimum heartbeat interval for share group members.";

    public static final String SHARE_GROUP_MAX_HEARTBEAT_INTERVAL_MS_CONFIG = "group.share.max.heartbeat.interval.ms";
    public static final int SHARE_GROUP_MAX_HEARTBEAT_INTERVAL_MS_DEFAULT = 15000;
    public static final String SHARE_GROUP_MAX_HEARTBEAT_INTERVAL_MS_DOC = "The maximum heartbeat interval for share group members.";

    public static final String OFFSET_METADATA_MAX_SIZE_CONFIG = "offset.metadata.max.bytes";
    public static final int OFFSET_METADATA_MAX_SIZE_DEFAULT = 4096;
    public static final String OFFSET_METADATA_MAX_SIZE_DOC = "The maximum size for a metadata entry associated with an offset commit.";

    public static final String OFFSETS_LOAD_BUFFER_SIZE_CONFIG = "offsets.load.buffer.size";
    public static final int OFFSETS_LOAD_BUFFER_SIZE_DEFAULT = 5 * 1024 * 1024;
    public static final String OFFSETS_LOAD_BUFFER_SIZE_DOC = "Batch size for reading from the offsets segments when loading offsets into the cache (soft-limit, overridden if records are too large).";

    public static final String OFFSETS_RETENTION_MINUTES_CONFIG = "offsets.retention.minutes";
    public static final int OFFSETS_RETENTION_MINUTES_DEFAULT = 7 * 24 * 60;
    public static final String OFFSETS_RETENTION_MINUTES_DOC = "For subscribed consumers, committed offset of a specific partition will be expired and discarded when 1) this retention period has elapsed after the consumer group loses all its consumers (i.e. becomes empty); " +
            "2) this retention period has elapsed since the last time an offset is committed for the partition and the group is no longer subscribed to the corresponding topic. " +
            "For standalone consumers (using manual assignment), offsets will be expired after this retention period has elapsed since the time of last commit. " +
            "Note that when a group is deleted via the delete-group request, its committed offsets will also be deleted without extra retention period; " +
            "also when a topic is deleted via the delete-topic request, upon propagated metadata update any group's committed offsets for that topic will also be deleted without extra retention period.";

    public static final String OFFSETS_RETENTION_CHECK_INTERVAL_MS_CONFIG = "offsets.retention.check.interval.ms";
    public static final long OFFSETS_RETENTION_CHECK_INTERVAL_MS_DEFAULT = 600000L;
    public static final String OFFSETS_RETENTION_CHECK_INTERVAL_MS_DOC = "Frequency at which to check for stale offsets";

    public static final String OFFSETS_TOPIC_PARTITIONS_CONFIG = "offsets.topic.num.partitions";
    public static final int OFFSETS_TOPIC_PARTITIONS_DEFAULT = 50;
    public static final String OFFSETS_TOPIC_PARTITIONS_DOC = "The number of partitions for the offset commit topic (should not change after deployment).";

    public static final String OFFSETS_TOPIC_SEGMENT_BYTES_CONFIG = "offsets.topic.segment.bytes";
    public static final int OFFSETS_TOPIC_SEGMENT_BYTES_DEFAULT = 100 * 1024 * 1024;
    public static final String OFFSETS_TOPIC_SEGMENT_BYTES_DOC = "The offsets topic segment bytes should be kept relatively small in order to facilitate faster log compaction and cache loads.";

    public static final String OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG = "offsets.topic.replication.factor";
    public static final short OFFSETS_TOPIC_REPLICATION_FACTOR_DEFAULT = 3;
    public static final String OFFSETS_TOPIC_REPLICATION_FACTOR_DOC = "The replication factor for the offsets topic (set higher to ensure availability). " +
            "Internal topic creation will fail until the cluster size meets this replication factor requirement.";

    public static final String OFFSETS_TOPIC_COMPRESSION_CODEC_CONFIG = "offsets.topic.compression.codec";
    public static final CompressionType OFFSETS_TOPIC_COMPRESSION_CODEC_DEFAULT = CompressionType.NONE;
    public static final String OFFSETS_TOPIC_COMPRESSION_CODEC_DOC = "Compression codec for the offsets topic - compression may be used to achieve \"atomic\" commits.";

    public static final String OFFSET_COMMIT_TIMEOUT_MS_CONFIG = "offsets.commit.timeout.ms";
    public static final int OFFSET_COMMIT_TIMEOUT_MS_DEFAULT = 5000;
    public static final String OFFSET_COMMIT_TIMEOUT_MS_DOC = "Offset commit will be delayed until all replicas for the offsets topic receive the commit " +
            "or this timeout is reached. This is similar to the producer request timeout.";

    public static final ConfigDef GROUP_COORDINATOR_CONFIG_DEF = new ConfigDef()
            .define(GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG, INT, GROUP_MIN_SESSION_TIMEOUT_MS_DEFAULT, MEDIUM, GROUP_MIN_SESSION_TIMEOUT_MS_DOC)
            .define(GROUP_MAX_SESSION_TIMEOUT_MS_CONFIG, INT, GROUP_MAX_SESSION_TIMEOUT_MS_DEFAULT, MEDIUM, GROUP_MAX_SESSION_TIMEOUT_MS_DOC)
            .define(GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, INT, GROUP_INITIAL_REBALANCE_DELAY_MS_DEFAULT, MEDIUM, GROUP_INITIAL_REBALANCE_DELAY_MS_DOC)
            .define(GROUP_MAX_SIZE_CONFIG, INT, GROUP_MAX_SIZE_DEFAULT, atLeast(1), MEDIUM, GROUP_MAX_SIZE_DOC);

    public static final ConfigDef NEW_GROUP_CONFIG_DEF = new ConfigDef()
            .define(GROUP_COORDINATOR_REBALANCE_PROTOCOLS_CONFIG, LIST, GROUP_COORDINATOR_REBALANCE_PROTOCOLS_DEFAULT,
                    ConfigDef.ValidList.in(Group.GroupType.documentValidValues()), MEDIUM, GROUP_COORDINATOR_REBALANCE_PROTOCOLS_DOC)
            .define(GROUP_COORDINATOR_NUM_THREADS_CONFIG, INT, GROUP_COORDINATOR_NUM_THREADS_DEFAULT, atLeast(1), MEDIUM, GROUP_COORDINATOR_NUM_THREADS_DOC)
            .define(GROUP_COORDINATOR_APPEND_LINGER_MS_CONFIG, INT, GROUP_COORDINATOR_APPEND_LINGER_MS_DEFAULT, atLeast(0), MEDIUM, GROUP_COORDINATOR_APPEND_LINGER_MS_DOC)
            // Internal configuration used by integration and system tests.
            .defineInternal(NEW_GROUP_COORDINATOR_ENABLE_CONFIG, BOOLEAN, NEW_GROUP_COORDINATOR_ENABLE_DEFAULT, null, MEDIUM, NEW_GROUP_COORDINATOR_ENABLE_DOC);

    public static final ConfigDef OFFSET_MANAGEMENT_CONFIG_DEF = new ConfigDef()
            .define(OFFSET_METADATA_MAX_SIZE_CONFIG, INT, OFFSET_METADATA_MAX_SIZE_DEFAULT, HIGH, OFFSET_METADATA_MAX_SIZE_DOC)
            .define(OFFSETS_LOAD_BUFFER_SIZE_CONFIG, INT, OFFSETS_LOAD_BUFFER_SIZE_DEFAULT, atLeast(1), HIGH, OFFSETS_LOAD_BUFFER_SIZE_DOC)
            .define(OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, SHORT, OFFSETS_TOPIC_REPLICATION_FACTOR_DEFAULT, atLeast(1), HIGH, OFFSETS_TOPIC_REPLICATION_FACTOR_DOC)
            .define(OFFSETS_TOPIC_PARTITIONS_CONFIG, INT, OFFSETS_TOPIC_PARTITIONS_DEFAULT, atLeast(1), HIGH, OFFSETS_TOPIC_PARTITIONS_DOC)
            .define(OFFSETS_TOPIC_SEGMENT_BYTES_CONFIG, INT, OFFSETS_TOPIC_SEGMENT_BYTES_DEFAULT, atLeast(1), HIGH, OFFSETS_TOPIC_SEGMENT_BYTES_DOC)
            .define(OFFSETS_TOPIC_COMPRESSION_CODEC_CONFIG, INT, (int) OFFSETS_TOPIC_COMPRESSION_CODEC_DEFAULT.id, HIGH, OFFSETS_TOPIC_COMPRESSION_CODEC_DOC)
            .define(OFFSETS_RETENTION_MINUTES_CONFIG, INT, OFFSETS_RETENTION_MINUTES_DEFAULT, atLeast(1), HIGH, OFFSETS_RETENTION_MINUTES_DOC)
            .define(OFFSETS_RETENTION_CHECK_INTERVAL_MS_CONFIG, LONG, OFFSETS_RETENTION_CHECK_INTERVAL_MS_DEFAULT, atLeast(1), HIGH, OFFSETS_RETENTION_CHECK_INTERVAL_MS_DOC)
            .define(OFFSET_COMMIT_TIMEOUT_MS_CONFIG, INT, OFFSET_COMMIT_TIMEOUT_MS_DEFAULT, atLeast(1), HIGH, OFFSET_COMMIT_TIMEOUT_MS_DOC);

    public static final ConfigDef CONSUMER_GROUP_CONFIG_DEF = new ConfigDef()
            .define(CONSUMER_GROUP_SESSION_TIMEOUT_MS_CONFIG, INT, CONSUMER_GROUP_SESSION_TIMEOUT_MS_DEFAULT, atLeast(1), MEDIUM, CONSUMER_GROUP_SESSION_TIMEOUT_MS_DOC)
            .define(CONSUMER_GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG, INT, CONSUMER_GROUP_MIN_SESSION_TIMEOUT_MS_DEFAULT, atLeast(1), MEDIUM, CONSUMER_GROUP_MIN_SESSION_TIMEOUT_MS_DOC)
            .define(CONSUMER_GROUP_MAX_SESSION_TIMEOUT_MS_CONFIG, INT, CONSUMER_GROUP_MAX_SESSION_TIMEOUT_MS_DEFAULT, atLeast(1), MEDIUM, CONSUMER_GROUP_MAX_SESSION_TIMEOUT_MS_DOC)
            .define(CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_CONFIG, INT, CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_DEFAULT, atLeast(1), MEDIUM, CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_DOC)
            .define(CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_CONFIG, INT, CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_DEFAULT, atLeast(1), MEDIUM, CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_DOC)
            .define(CONSUMER_GROUP_MAX_HEARTBEAT_INTERVAL_MS_CONFIG, INT, CONSUMER_GROUP_MAX_HEARTBEAT_INTERVAL_MS_DEFAULT, atLeast(1), MEDIUM, CONSUMER_GROUP_MAX_HEARTBEAT_INTERVAL_MS_DOC)
            .define(CONSUMER_GROUP_MAX_SIZE_CONFIG, INT, CONSUMER_GROUP_MAX_SIZE_DEFAULT, atLeast(1), MEDIUM, CONSUMER_GROUP_MAX_SIZE_DOC)
            .define(CONSUMER_GROUP_ASSIGNORS_CONFIG, LIST, CONSUMER_GROUP_ASSIGNORS_DEFAULT, null, MEDIUM, CONSUMER_GROUP_ASSIGNORS_DOC)
            .define(CONSUMER_GROUP_MIGRATION_POLICY_CONFIG, STRING, CONSUMER_GROUP_MIGRATION_POLICY_DEFAULT, ConfigDef.CaseInsensitiveValidString.in(Utils.enumOptions(ConsumerGroupMigrationPolicy.class)), MEDIUM, CONSUMER_GROUP_MIGRATION_POLICY_DOC);

    public static final ConfigDef SHARE_GROUP_CONFIG_DEF = new ConfigDef()
            .define(SHARE_GROUP_SESSION_TIMEOUT_MS_CONFIG, INT, SHARE_GROUP_SESSION_TIMEOUT_MS_DEFAULT, atLeast(1), MEDIUM, SHARE_GROUP_SESSION_TIMEOUT_MS_DOC)
            .define(SHARE_GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG, INT, SHARE_GROUP_MIN_SESSION_TIMEOUT_MS_DEFAULT, atLeast(1), MEDIUM, SHARE_GROUP_MIN_SESSION_TIMEOUT_MS_DOC)
            .define(SHARE_GROUP_MAX_SESSION_TIMEOUT_MS_CONFIG, INT, SHARE_GROUP_MAX_SESSION_TIMEOUT_MS_DEFAULT, atLeast(1), MEDIUM, SHARE_GROUP_MAX_SESSION_TIMEOUT_MS_DOC)
            .define(SHARE_GROUP_HEARTBEAT_INTERVAL_MS_CONFIG, INT, SHARE_GROUP_HEARTBEAT_INTERVAL_MS_DEFAULT, atLeast(1), MEDIUM, SHARE_GROUP_HEARTBEAT_INTERVAL_MS_DOC)
            .define(SHARE_GROUP_MIN_HEARTBEAT_INTERVAL_MS_CONFIG, INT, SHARE_GROUP_MIN_HEARTBEAT_INTERVAL_MS_DEFAULT, atLeast(1), MEDIUM, SHARE_GROUP_MIN_HEARTBEAT_INTERVAL_MS_DOC)
            .define(SHARE_GROUP_MAX_HEARTBEAT_INTERVAL_MS_CONFIG, INT, SHARE_GROUP_MAX_HEARTBEAT_INTERVAL_MS_DEFAULT, atLeast(1), MEDIUM, SHARE_GROUP_MAX_HEARTBEAT_INTERVAL_MS_DOC)
            .define(SHARE_GROUP_MAX_SIZE_CONFIG, INT, SHARE_GROUP_MAX_SIZE_DEFAULT, between(1, 1000), MEDIUM, SHARE_GROUP_MAX_SIZE_DOC);

    /**
     * The timeout used to wait for a new member in milliseconds.
     */
    public static final int CLASSIC_GROUP_NEW_MEMBER_JOIN_TIMEOUT_MS = 5 * 60 * 1000;

    private final int numThreads;
    private final int appendLingerMs;
    private final int consumerGroupSessionTimeoutMs;
    private final int consumerGroupHeartbeatIntervalMs;
    private final int consumerGroupMaxSize;
    private final List<ConsumerGroupPartitionAssignor> consumerGroupAssignors;
    private final int offsetsTopicSegmentBytes;
    private final int offsetMetadataMaxSize;
    private final int classicGroupMaxSize;
    private final int classicGroupInitialRebalanceDelayMs;
    private final int classicGroupMinSessionTimeoutMs;
    private final int classicGroupMaxSessionTimeoutMs;
    private final long offsetsRetentionCheckIntervalMs;
    private final long offsetsRetentionMs;
    private final int offsetCommitTimeoutMs;
    private final ConsumerGroupMigrationPolicy consumerGroupMigrationPolicy;
    private final CompressionType offsetTopicCompressionType;
    private final int offsetsLoadBufferSize;
    private final int offsetsTopicPartitions;
    private final short offsetsTopicReplicationFactor;
    private final int consumerGroupMinSessionTimeoutMs;
    private final int consumerGroupMaxSessionTimeoutMs;
    private final int consumerGroupMinHeartbeatIntervalMs;
    private final int consumerGroupMaxHeartbeatIntervalMs;
    // Share group configurations
    private final int shareGroupMaxSize;
    private final int shareGroupSessionTimeoutMs;
    private final int shareGroupMinSessionTimeoutMs;
    private final int shareGroupMaxSessionTimeoutMs;
    private final int shareGroupHeartbeatIntervalMs;
    private final int shareGroupMinHeartbeatIntervalMs;
    private final int shareGroupMaxHeartbeatIntervalMs;

    @SuppressWarnings("this-escape")
    public GroupCoordinatorConfig(AbstractConfig config) {
        this.numThreads = config.getInt(GroupCoordinatorConfig.GROUP_COORDINATOR_NUM_THREADS_CONFIG);
        this.appendLingerMs = config.getInt(GroupCoordinatorConfig.GROUP_COORDINATOR_APPEND_LINGER_MS_CONFIG);
        this.consumerGroupSessionTimeoutMs = config.getInt(GroupCoordinatorConfig.CONSUMER_GROUP_SESSION_TIMEOUT_MS_CONFIG);
        this.consumerGroupHeartbeatIntervalMs = config.getInt(GroupCoordinatorConfig.CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_CONFIG);
        this.consumerGroupMaxSize = config.getInt(GroupCoordinatorConfig.CONSUMER_GROUP_MAX_SIZE_CONFIG);
        this.consumerGroupAssignors = consumerGroupAssignors(config);
        this.offsetsTopicSegmentBytes = config.getInt(GroupCoordinatorConfig.OFFSETS_TOPIC_SEGMENT_BYTES_CONFIG);
        this.offsetMetadataMaxSize = config.getInt(GroupCoordinatorConfig.OFFSET_METADATA_MAX_SIZE_CONFIG);
        this.classicGroupMaxSize = config.getInt(GroupCoordinatorConfig.GROUP_MAX_SIZE_CONFIG);
        this.classicGroupInitialRebalanceDelayMs = config.getInt(GroupCoordinatorConfig.GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG);
        this.classicGroupMinSessionTimeoutMs = config.getInt(GroupCoordinatorConfig.GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG);
        this.classicGroupMaxSessionTimeoutMs = config.getInt(GroupCoordinatorConfig.GROUP_MAX_SESSION_TIMEOUT_MS_CONFIG);
        this.offsetsRetentionCheckIntervalMs = config.getLong(GroupCoordinatorConfig.OFFSETS_RETENTION_CHECK_INTERVAL_MS_CONFIG);
        this.offsetsRetentionMs = config.getInt(GroupCoordinatorConfig.OFFSETS_RETENTION_MINUTES_CONFIG) * 60L * 1000L;
        this.offsetCommitTimeoutMs = config.getInt(GroupCoordinatorConfig.OFFSET_COMMIT_TIMEOUT_MS_CONFIG);
        this.consumerGroupMigrationPolicy = ConsumerGroupMigrationPolicy.parse(
                config.getString(GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG));
        this.offsetTopicCompressionType = Optional.ofNullable(config.getInt(GroupCoordinatorConfig.OFFSETS_TOPIC_COMPRESSION_CODEC_CONFIG))
                .map(CompressionType::forId)
                .orElse(null);
        this.offsetsLoadBufferSize = config.getInt(GroupCoordinatorConfig.OFFSETS_LOAD_BUFFER_SIZE_CONFIG);
        this.offsetsTopicPartitions = config.getInt(GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG);
        this.offsetsTopicReplicationFactor = config.getShort(GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG);
        this.consumerGroupMinSessionTimeoutMs = config.getInt(GroupCoordinatorConfig.CONSUMER_GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG);
        this.consumerGroupMaxSessionTimeoutMs = config.getInt(GroupCoordinatorConfig.CONSUMER_GROUP_MAX_SESSION_TIMEOUT_MS_CONFIG);
        this.consumerGroupMinHeartbeatIntervalMs = config.getInt(GroupCoordinatorConfig.CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_CONFIG);
        this.consumerGroupMaxHeartbeatIntervalMs = config.getInt(GroupCoordinatorConfig.CONSUMER_GROUP_MAX_HEARTBEAT_INTERVAL_MS_CONFIG);
        // Share group configurations
        this.shareGroupSessionTimeoutMs = config.getInt(GroupCoordinatorConfig.SHARE_GROUP_SESSION_TIMEOUT_MS_CONFIG);
        this.shareGroupMinSessionTimeoutMs = config.getInt(GroupCoordinatorConfig.SHARE_GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG);
        this.shareGroupMaxSessionTimeoutMs = config.getInt(GroupCoordinatorConfig.SHARE_GROUP_MAX_SESSION_TIMEOUT_MS_CONFIG);
        this.shareGroupHeartbeatIntervalMs = config.getInt(GroupCoordinatorConfig.SHARE_GROUP_HEARTBEAT_INTERVAL_MS_CONFIG);
        this.shareGroupMinHeartbeatIntervalMs = config.getInt(GroupCoordinatorConfig.SHARE_GROUP_MIN_HEARTBEAT_INTERVAL_MS_CONFIG);
        this.shareGroupMaxHeartbeatIntervalMs = config.getInt(GroupCoordinatorConfig.SHARE_GROUP_MAX_HEARTBEAT_INTERVAL_MS_CONFIG);
        this.shareGroupMaxSize = config.getInt(GroupCoordinatorConfig.SHARE_GROUP_MAX_SIZE_CONFIG);

        // New group coordinator configs validation.
        require(consumerGroupMaxHeartbeatIntervalMs >= consumerGroupMinHeartbeatIntervalMs,
                String.format("%s must be greater than or equal to %s", CONSUMER_GROUP_MAX_HEARTBEAT_INTERVAL_MS_CONFIG, CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_CONFIG));
        require(consumerGroupHeartbeatIntervalMs >= consumerGroupMinHeartbeatIntervalMs,
                String.format("%s must be greater than or equal to %s", CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_CONFIG, CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_CONFIG));
        require(consumerGroupHeartbeatIntervalMs <= consumerGroupMaxHeartbeatIntervalMs,
                String.format("%s must be less than or equal to %s", CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_CONFIG, CONSUMER_GROUP_MAX_HEARTBEAT_INTERVAL_MS_CONFIG));

        require(consumerGroupMaxSessionTimeoutMs >= consumerGroupMinSessionTimeoutMs,
                String.format("%s must be greater than or equal to %s", CONSUMER_GROUP_MAX_SESSION_TIMEOUT_MS_CONFIG, CONSUMER_GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG));
        require(consumerGroupSessionTimeoutMs >= consumerGroupMinSessionTimeoutMs,
                String.format("%s must be greater than or equal to %s", CONSUMER_GROUP_SESSION_TIMEOUT_MS_CONFIG, CONSUMER_GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG));
        require(consumerGroupSessionTimeoutMs <= consumerGroupMaxSessionTimeoutMs,
                String.format("%s must be less than or equal to %s", CONSUMER_GROUP_SESSION_TIMEOUT_MS_CONFIG, CONSUMER_GROUP_MAX_SESSION_TIMEOUT_MS_CONFIG));
        require(consumerGroupHeartbeatIntervalMs < consumerGroupSessionTimeoutMs,
                String.format("%s must be less than %s", CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_CONFIG, CONSUMER_GROUP_SESSION_TIMEOUT_MS_CONFIG));
        // Share group configs validation.
        require(shareGroupMaxHeartbeatIntervalMs >= shareGroupMinHeartbeatIntervalMs,
            String.format("%s must be greater than or equal to %s",
                SHARE_GROUP_MAX_HEARTBEAT_INTERVAL_MS_CONFIG, SHARE_GROUP_MIN_HEARTBEAT_INTERVAL_MS_CONFIG));
        require(shareGroupHeartbeatIntervalMs >= shareGroupMinHeartbeatIntervalMs,
            String.format("%s must be greater than or equal to %s",
                SHARE_GROUP_HEARTBEAT_INTERVAL_MS_CONFIG, SHARE_GROUP_MIN_HEARTBEAT_INTERVAL_MS_CONFIG));
        require(shareGroupHeartbeatIntervalMs <= shareGroupMaxHeartbeatIntervalMs,
            String.format("%s must be less than or equal to %s",
                SHARE_GROUP_HEARTBEAT_INTERVAL_MS_CONFIG, SHARE_GROUP_MAX_HEARTBEAT_INTERVAL_MS_CONFIG));

        require(shareGroupMaxSessionTimeoutMs >= shareGroupMinSessionTimeoutMs,
            String.format("%s must be greater than or equal to %s",
                SHARE_GROUP_MAX_SESSION_TIMEOUT_MS_CONFIG, SHARE_GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG));
        require(shareGroupSessionTimeoutMs >= shareGroupMinSessionTimeoutMs,
            String.format("%s must be greater than or equal to %s",
                SHARE_GROUP_SESSION_TIMEOUT_MS_CONFIG, SHARE_GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG));
        require(shareGroupSessionTimeoutMs <= shareGroupMaxSessionTimeoutMs,
            String.format("%s must be less than or equal to %s",
                SHARE_GROUP_SESSION_TIMEOUT_MS_CONFIG, SHARE_GROUP_MAX_SESSION_TIMEOUT_MS_CONFIG));

        require(shareGroupHeartbeatIntervalMs < shareGroupSessionTimeoutMs,
            String.format("%s must be less than %s",
                SHARE_GROUP_HEARTBEAT_INTERVAL_MS_CONFIG, SHARE_GROUP_SESSION_TIMEOUT_MS_CONFIG));
    }

    public static GroupCoordinatorConfig fromProps(
        Map<?, ?> props
    ) {
        return new GroupCoordinatorConfig(
            new AbstractConfig(
                Utils.mergeConfigs(List.of(
                    GroupCoordinatorConfig.GROUP_COORDINATOR_CONFIG_DEF,
                    GroupCoordinatorConfig.NEW_GROUP_CONFIG_DEF,
                    GroupCoordinatorConfig.OFFSET_MANAGEMENT_CONFIG_DEF,
                    GroupCoordinatorConfig.CONSUMER_GROUP_CONFIG_DEF,
                    GroupCoordinatorConfig.SHARE_GROUP_CONFIG_DEF
                )),
                props
            )
        );
    }

    protected List<ConsumerGroupPartitionAssignor> consumerGroupAssignors(
        AbstractConfig config
    ) {
        return Collections.unmodifiableList(
            config.getConfiguredInstances(
                GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG,
                ConsumerGroupPartitionAssignor.class
            )
        );
    }

    /**
     * Copy the subset of properties that are relevant to consumer group and share group.
     */
    public Map<String, Integer> extractGroupConfigMap(ShareGroupConfig shareGroupConfig) {
        Map<String, Integer> defaultConfigs = new HashMap<>();
        defaultConfigs.putAll(extractConsumerGroupConfigMap());
        defaultConfigs.putAll(shareGroupConfig.extractShareGroupConfigMap());
        return Collections.unmodifiableMap(defaultConfigs);
    }

    /**
     * Copy the subset of properties that are relevant to consumer group.
     */
    public Map<String, Integer> extractConsumerGroupConfigMap() {
        return Map.of(
            GroupConfig.CONSUMER_SESSION_TIMEOUT_MS_CONFIG, consumerGroupSessionTimeoutMs(),
            GroupConfig.CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG, consumerGroupHeartbeatIntervalMs());
    }

    /**
     * The number of threads or event loops running.
     */
    public int numThreads() {
        return numThreads;
    }

    /**
     * The duration in milliseconds that the coordinator will wait for writes to
     * accumulate before flushing them to disk.
     */
    public int appendLingerMs() {
        return appendLingerMs;
    }

    /**
     * The consumer group session timeout in milliseconds.
     */
    public int consumerGroupSessionTimeoutMs() {
        return consumerGroupSessionTimeoutMs;
    }

    /**
     * The consumer group heartbeat interval in milliseconds.
     */
    public int consumerGroupHeartbeatIntervalMs() {
        return consumerGroupHeartbeatIntervalMs;
    }

    /**
     * The consumer group maximum size.
     */
    public int consumerGroupMaxSize() {
        return consumerGroupMaxSize;
    }

    /**
     * The consumer group assignors.
     */
    public List<ConsumerGroupPartitionAssignor> consumerGroupAssignors() {
        return consumerGroupAssignors;
    }

    /**
     * The offsets topic segment bytes should be kept relatively small to facilitate faster
     * log compaction and faster offset loads.
     */
    public int offsetsTopicSegmentBytes() {
        return offsetsTopicSegmentBytes;
    }

    /**
     * The maximum size for a metadata entry associated with an offset commit.
     */
    public int offsetMetadataMaxSize() {
        return offsetMetadataMaxSize;
    }

    /**
     * The classic group maximum size.
     */
    public int classicGroupMaxSize() {
        return classicGroupMaxSize;
    }

    /**
     * The delay in milliseconds introduced for the first rebalance of a classic group.
     */
    public int classicGroupInitialRebalanceDelayMs() {
        return classicGroupInitialRebalanceDelayMs;
    }

    /**
     * The timeout used to wait for a new member in milliseconds.
     */
    public int classicGroupNewMemberJoinTimeoutMs() {
        return CLASSIC_GROUP_NEW_MEMBER_JOIN_TIMEOUT_MS;
    }

    /**
     * The classic group minimum session timeout.
     */
    public int classicGroupMinSessionTimeoutMs() {
        return classicGroupMinSessionTimeoutMs;
    }

    /**
     * The classic group maximum session timeout.
     */
    public int classicGroupMaxSessionTimeoutMs() {
        return classicGroupMaxSessionTimeoutMs;
    }

    /**
     * Frequency at which to check for expired offsets.
     */
    public long offsetsRetentionCheckIntervalMs() {
        return offsetsRetentionCheckIntervalMs;
    }

    /**
     * For subscribed consumers, committed offset of a specific partition will be expired and discarded when:
     *     1) This retention period has elapsed after the consumer group loses all its consumers (i.e. becomes empty);
     *     2) This retention period has elapsed since the last time an offset is committed for the partition AND
     *        the group is no longer subscribed to the corresponding topic.
     *
     * For standalone consumers (using manual assignment), offsets will be expired after this retention period has
     * elapsed since the time of last commit.
     *
     * Note that when a group is deleted via the DeleteGroups request, its committed offsets will also be deleted immediately;
     *
     * Also, when a topic is deleted via the delete-topic request, upon propagated metadata update any group's
     *     committed offsets for that topic will also be deleted without extra retention period.
     */
    public long offsetsRetentionMs() {
        return offsetsRetentionMs;
    }

    /**
     * Offset commit will be delayed until all replicas for the offsets topic receive the commit
     * or this timeout is reached
     */
    public int offsetCommitTimeoutMs() {
        return offsetCommitTimeoutMs;
    }

    /**
     * The config indicating whether group protocol upgrade/downgrade are allowed.
     */
    public ConsumerGroupMigrationPolicy consumerGroupMigrationPolicy() {
        return consumerGroupMigrationPolicy;
    }

    /**
     * The compression type used to compress records in batches.
     */
    public CompressionType offsetTopicCompressionType() {
        return offsetTopicCompressionType;
    }

    /**
     * Batch size for reading from the offsets segments when loading offsets into
     * the cache (soft-limit, overridden if records are too large).
     */
    public int offsetsLoadBufferSize() {
        return offsetsLoadBufferSize;
    }

    /**
     * The number of partitions for the offset commit topic (should not change after deployment).
     */
    public int offsetsTopicPartitions() {
        return offsetsTopicPartitions;
    }

    /**
     * The replication factor for the offsets topic (set higher to ensure availability).
     * Internal topic creation will fail until the cluster size meets this replication factor requirement.
     */
    public short offsetsTopicReplicationFactor() {
        return offsetsTopicReplicationFactor;
    }

    /**
     * The minimum allowed session timeout for registered consumers.
     */
    public int consumerGroupMinSessionTimeoutMs() {
        return consumerGroupMinSessionTimeoutMs;
    }

    /**
     * The maximum allowed session timeout for registered consumers.
     */
    public int consumerGroupMaxSessionTimeoutMs() {
        return consumerGroupMaxSessionTimeoutMs;
    }

    /**
     * The minimum heartbeat interval for registered consumers.
     */
    public int consumerGroupMinHeartbeatIntervalMs() {
        return consumerGroupMinHeartbeatIntervalMs;
    }

    /**
     * The maximum heartbeat interval for registered consumers.
     */
    public int consumerGroupMaxHeartbeatIntervalMs() {
        return consumerGroupMaxHeartbeatIntervalMs;
    }

    /**
     * The share group session timeout in milliseconds.
     */
    public int shareGroupSessionTimeoutMs() {
        return shareGroupSessionTimeoutMs;
    }

    /**
     * The consumer group heartbeat interval in milliseconds.
     */
    public int shareGroupHeartbeatIntervalMs() {
        return shareGroupHeartbeatIntervalMs;
    }

    /**
     * The share group maximum size.
     */
    public int shareGroupMaxSize() {
        return shareGroupMaxSize;
    }

    /**
     * The minimum allowed session timeout for registered share consumers.
     */
    public int shareGroupMinSessionTimeoutMs() {
        return shareGroupMinSessionTimeoutMs;
    }

    /**
     * The maximum allowed session timeout for registered share consumers.
     */
    public int shareGroupMaxSessionTimeoutMs() {
        return shareGroupMaxSessionTimeoutMs;
    }

    /**
     * The minimum heartbeat interval for registered share consumers.
     */
    public int shareGroupMinHeartbeatIntervalMs() {
        return shareGroupMinHeartbeatIntervalMs;
    }

    /**
     * The maximum heartbeat interval for registered share consumers.
     */
    public int shareGroupMaxHeartbeatIntervalMs() {
        return shareGroupMaxHeartbeatIntervalMs;
    }
}
