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

import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.coordinator.group.assignor.PartitionAssignor;
import org.apache.kafka.coordinator.group.assignor.RangeAssignor;
import org.apache.kafka.coordinator.group.assignor.UniformAssignor;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The group coordinator configurations.
 */
public class GroupCoordinatorConfig {
    /** ********* Group coordinator configuration ***********/
    public final static String GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG = "group.min.session.timeout.ms";
    public final static String GROUP_MIN_SESSION_TIMEOUT_MS_DOC = "The minimum allowed session timeout for registered consumers. Shorter timeouts result in quicker failure detection at the cost of more frequent consumer heartbeating, which can overwhelm broker resources.";
    public static final int GROUP_MIN_SESSION_TIMEOUT_MS_DEFAULT = 6000;

    public final static String GROUP_MAX_SESSION_TIMEOUT_MS_CONFIG = "group.max.session.timeout.ms";
    public final static String GROUP_MAX_SESSION_TIMEOUT_MS_DOC = "The maximum allowed session timeout for registered consumers. Longer timeouts give consumers more time to process messages in between heartbeats at the cost of a longer time to detect failures.";
    public static final int GROUP_MAX_SESSION_TIMEOUT_MS_DEFAULT = 1800000;

    public final static String GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG = "group.initial.rebalance.delay.ms";
    public final static String GROUP_INITIAL_REBALANCE_DELAY_MS_DOC = "The amount of time the group coordinator will wait for more consumers to join a new group before performing the first rebalance. A longer delay means potentially fewer rebalances, but increases the time until processing begins.";
    public static final int GROUP_INITIAL_REBALANCE_DELAY_MS_DEFAULT = 3000;

    public final static String GROUP_MAX_SIZE_CONFIG = "group.max.size";
    public final static String GROUP_MAX_SIZE_DOC = "The maximum number of consumers that a single consumer group can accommodate.";
    public static final int GROUP_MAX_SIZE_DEFAULT = Integer.MAX_VALUE;

    /** New group coordinator configs */
    public final static String NEW_GROUP_COORDINATOR_ENABLE_CONFIG = "group.coordinator.new.enable";
    public final static String NEW_GROUP_COORDINATOR_ENABLE_DOC = "Enable the new group coordinator.";
    public static final boolean NEW_GROUP_COORDINATOR_ENABLE_DEFAULT = false;

    public final static String GROUP_COORDINATOR_REBALANCE_PROTOCOLS_CONFIG = "group.coordinator.rebalance.protocols";
    public final static String GROUP_COORDINATOR_REBALANCE_PROTOCOLS_DOC = "The list of enabled rebalance protocols. Supported protocols: " +
            Arrays.stream(Group.GroupType.values()).map(Group.GroupType::toString).collect(Collectors.joining(",")) + ". " +
            "The " + Group.GroupType.CONSUMER + " rebalance protocol is in early access and therefore must not be used in production.";
    public static final List<String> GROUP_COORDINATOR_REBALANCE_PROTOCOLS_DEFAULT = Collections.singletonList(Group.GroupType.CLASSIC.toString());

    public final static String GROUP_COORDINATOR_NUM_THREADS_CONFIG = "group.coordinator.threads";
    public final static String GROUP_COORDINATOR_NUM_THREADS_DOC = "The number of threads used by the group coordinator.";
    public static final int GROUP_COORDINATOR_NUM_THREADS_DEFAULT = 1;

    /** Consumer group configs */
    public final static String CONSUMER_GROUP_SESSION_TIMEOUT_MS_CONFIG = "group.consumer.session.timeout.ms";
    public final static String CONSUMER_GROUP_SESSION_TIMEOUT_MS_DOC = "The timeout to detect client failures when using the consumer group protocol.";
    public static final int CONSUMER_GROUP_SESSION_TIMEOUT_MS_DEFAULT = 45000;

    public final static String CONSUMER_GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG = "group.consumer.min.session.timeout.ms";
    public final static String CONSUMER_GROUP_MIN_SESSION_TIMEOUT_MS_DOC = "The minimum allowed session timeout for registered consumers.";
    public static final int CONSUMER_GROUP_MIN_SESSION_TIMEOUT_MS_DEFAULT = 45000;

    public final static String CONSUMER_GROUP_MAX_SESSION_TIMEOUT_MS_CONFIG = "group.consumer.max.session.timeout.ms";
    public final static String CONSUMER_GROUP_MAX_SESSION_TIMEOUT_MS_DOC = "The maximum allowed session timeout for registered consumers.";
    public static final int CONSUMER_GROUP_MAX_SESSION_TIMEOUT_MS_DEFAULT = 60000;

    public final static String CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_CONFIG = "group.consumer.heartbeat.interval.ms";
    public final static String CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_DOC = "The heartbeat interval given to the members of a consumer group.";
    public static final int CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_DEFAULT = 5000;

    public final static String CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_CONFIG = "group.consumer.min.heartbeat.interval.ms";
    public final static String CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_DOC = "The minimum heartbeat interval for registered consumers.";
    public static final int CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_DEFAULT = 5000;

    public final static String CONSUMER_GROUP_MAX_HEARTBEAT_INTERVAL_MS_CONFIG = "group.consumer.max.heartbeat.interval.ms";
    public final static String CONSUMER_GROUP_MAX_HEARTBEAT_INTERVAL_MS_DOC = "The maximum heartbeat interval for registered consumers.";
    public static final int CONSUMER_GROUP_MAX_HEARTBEAT_INTERVAL_MS_DEFAULT = 15000;

    public final static String CONSUMER_GROUP_MAX_SIZE_CONFIG = "group.consumer.max.size";
    public final static String CONSUMER_GROUP_MAX_SIZE_DOC = "The maximum number of consumers that a single consumer group can accommodate. This value will only impact the new consumer coordinator. To configure the classic consumer coordinator check " + GROUP_MAX_SIZE_CONFIG + " instead.";
    public static final int CONSUMER_GROUP_MAX_SIZE_DEFAULT = Integer.MAX_VALUE;

    public final static String CONSUMER_GROUP_ASSIGNORS_CONFIG = "group.consumer.assignors";
    public final static String CONSUMER_GROUP_ASSIGNORS_DOC = "The server side assignors as a list of full class names. The first one in the list is considered as the default assignor to be used in the case where the consumer does not specify an assignor.";
    public static final List<String> CONSUMER_GROUP_ASSIGNORS_DEFAULT = Collections.unmodifiableList(Arrays.asList(
            UniformAssignor.class.getName(),
            RangeAssignor.class.getName()
    ));

    public final static String CONSUMER_GROUP_MIGRATION_POLICY_CONFIG = "group.consumer.migration.policy";
    public static final String CONSUMER_GROUP_MIGRATION_POLICY_DEFAULT = ConsumerGroupMigrationPolicy.DISABLED.toString();
    public final static String CONSUMER_GROUP_MIGRATION_POLICY_DOC = "The config that enables converting the non-empty classic group using the consumer embedded protocol to the non-empty consumer group using the consumer group protocol and vice versa; " +
            "conversions of empty groups in both directions are always enabled regardless of this policy. " +
            ConsumerGroupMigrationPolicy.BIDIRECTIONAL + ": both upgrade from classic group to consumer group and downgrade from consumer group to classic group are enabled, " +
            ConsumerGroupMigrationPolicy.UPGRADE + ": only upgrade from classic group to consumer group is enabled, " +
            ConsumerGroupMigrationPolicy.DOWNGRADE + ": only downgrade from consumer group to classic group is enabled, " +
            ConsumerGroupMigrationPolicy.DISABLED + ": neither upgrade nor downgrade is enabled.";

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

    @Deprecated
    public static final String OFFSET_COMMIT_REQUIRED_ACKS_CONFIG = "offsets.commit.required.acks";
    public static final short OFFSET_COMMIT_REQUIRED_ACKS_DEFAULT = -1;
    public static final String OFFSET_COMMIT_REQUIRED_ACKS_DOC = "DEPRECATED: The required acks before the commit can be accepted. In general, the default (-1) should not be overridden.";

    /**
     * The timeout used to wait for a new member in milliseconds.
     */
    public static final int CLASSIC_GROUP_NEW_MEMBER_JOIN_TIMEOUT_MS = 5 * 60 * 1000;

    /**
     * The number of threads or event loops running.
     */
    public final int numThreads;

    /**
     * The consumer group session timeout in milliseconds.
     */
    public final int consumerGroupSessionTimeoutMs;

    /**
     * The consumer group heartbeat interval in milliseconds.
     */
    public final int consumerGroupHeartbeatIntervalMs;

    /**
     * The consumer group maximum size.
     */
    public final int consumerGroupMaxSize;

    /**
     * The consumer group assignors.
     */
    public final List<PartitionAssignor> consumerGroupAssignors;

    /**
     * The offsets topic segment bytes should be kept relatively small to facilitate faster
     * log compaction and faster offset loads.
     */
    public final int offsetsTopicSegmentBytes;

    /**
     * The maximum size for a metadata entry associated with an offset commit.
     */
    public final int offsetMetadataMaxSize;

    /**
     * The classic group maximum size.
     */
    public final int classicGroupMaxSize;

    /**
     * The delay in milliseconds introduced for the first rebalance of a classic group.
     */
    public final int classicGroupInitialRebalanceDelayMs;

    /**
     * The timeout used to wait for a new member in milliseconds.
     */
    public final int classicGroupNewMemberJoinTimeoutMs;

    /**
     * The classic group minimum session timeout.
     */
    public final int classicGroupMinSessionTimeoutMs;

    /**
     * The classic group maximum session timeout.
     */
    public final int classicGroupMaxSessionTimeoutMs;

    /**
     * Frequency at which to check for expired offsets.
     */
    public final long offsetsRetentionCheckIntervalMs;

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
    public final long offsetsRetentionMs;

    /**
     * Offset commit will be delayed until all replicas for the offsets topic receive the commit
     * or this timeout is reached
     */
    public final int offsetCommitTimeoutMs;

    /**
     * The config indicating whether group protocol upgrade/downgrade are allowed.
     */
    public final ConsumerGroupMigrationPolicy consumerGroupMigrationPolicy;

    public GroupCoordinatorConfig(
        int numThreads,
        int consumerGroupSessionTimeoutMs,
        int consumerGroupHeartbeatIntervalMs,
        int consumerGroupMaxSize,
        List<PartitionAssignor> consumerGroupAssignors,
        int offsetsTopicSegmentBytes,
        int offsetMetadataMaxSize,
        int classicGroupMaxSize,
        int classicGroupInitialRebalanceDelayMs,
        int classicGroupNewMemberJoinTimeoutMs,
        int classicGroupMinSessionTimeoutMs,
        int classicGroupMaxSessionTimeoutMs,
        long offsetsRetentionCheckIntervalMs,
        long offsetsRetentionMs,
        int offsetCommitTimeoutMs,
        ConsumerGroupMigrationPolicy consumerGroupMigrationPolicy
    ) {
        this.numThreads = numThreads;
        this.consumerGroupSessionTimeoutMs = consumerGroupSessionTimeoutMs;
        this.consumerGroupHeartbeatIntervalMs = consumerGroupHeartbeatIntervalMs;
        this.consumerGroupMaxSize = consumerGroupMaxSize;
        this.consumerGroupAssignors = consumerGroupAssignors;
        this.offsetsTopicSegmentBytes = offsetsTopicSegmentBytes;
        this.offsetMetadataMaxSize = offsetMetadataMaxSize;
        this.classicGroupMaxSize = classicGroupMaxSize;
        this.classicGroupInitialRebalanceDelayMs = classicGroupInitialRebalanceDelayMs;
        this.classicGroupNewMemberJoinTimeoutMs = classicGroupNewMemberJoinTimeoutMs;
        this.classicGroupMinSessionTimeoutMs = classicGroupMinSessionTimeoutMs;
        this.classicGroupMaxSessionTimeoutMs = classicGroupMaxSessionTimeoutMs;
        this.offsetsRetentionCheckIntervalMs = offsetsRetentionCheckIntervalMs;
        this.offsetsRetentionMs = offsetsRetentionMs;
        this.offsetCommitTimeoutMs = offsetCommitTimeoutMs;
        this.consumerGroupMigrationPolicy = consumerGroupMigrationPolicy;
    }
}
