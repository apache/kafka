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

import org.apache.kafka.coordinator.group.assignor.PartitionAssignor;

import java.util.List;

/**
 * The group coordinator configurations.
 */
public class GroupCoordinatorConfig {

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
        int offsetCommitTimeoutMs
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
    }
}
