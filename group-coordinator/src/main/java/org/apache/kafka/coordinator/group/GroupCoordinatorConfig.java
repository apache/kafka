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
    public static final int GENERIC_GROUP_NEW_MEMBER_JOIN_TIMEOUT_MS = 5 * 60 * 1000;

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
     * The generic group maximum size.
     */
    public final int genericGroupMaxSize;

    /**
     * The delay in milliseconds introduced for the first rebalance of a generic group.
     */
    public final int genericGroupInitialRebalanceDelayMs;

    /**
     * The timeout used to wait for a new member in milliseconds.
     */
    public final int genericGroupNewMemberJoinTimeoutMs;

    /**
     * The generic group minimum session timeout.
     */
    public final int genericGroupMinSessionTimeoutMs;

    /**
     * The generic group maximum session timeout.
     */
    public final int genericGroupMaxSessionTimeoutMs;

    public GroupCoordinatorConfig(
        int numThreads,
        int consumerGroupSessionTimeoutMs,
        int consumerGroupHeartbeatIntervalMs,
        int consumerGroupMaxSize,
        List<PartitionAssignor> consumerGroupAssignors,
        int offsetsTopicSegmentBytes,
        int offsetMetadataMaxSize,
        int genericGroupMaxSize,
        int genericGroupInitialRebalanceDelayMs,
        int genericGroupNewMemberJoinTimeoutMs,
        int genericGroupMinSessionTimeoutMs,
        int genericGroupMaxSessionTimeoutMs
    ) {
        this.numThreads = numThreads;
        this.consumerGroupSessionTimeoutMs = consumerGroupSessionTimeoutMs;
        this.consumerGroupHeartbeatIntervalMs = consumerGroupHeartbeatIntervalMs;
        this.consumerGroupMaxSize = consumerGroupMaxSize;
        this.consumerGroupAssignors = consumerGroupAssignors;
        this.offsetsTopicSegmentBytes = offsetsTopicSegmentBytes;
        this.offsetMetadataMaxSize = offsetMetadataMaxSize;
        this.genericGroupMaxSize = genericGroupMaxSize;
        this.genericGroupInitialRebalanceDelayMs = genericGroupInitialRebalanceDelayMs;
        this.genericGroupNewMemberJoinTimeoutMs = genericGroupNewMemberJoinTimeoutMs;
        this.genericGroupMinSessionTimeoutMs = genericGroupMinSessionTimeoutMs;
        this.genericGroupMaxSessionTimeoutMs = genericGroupMaxSessionTimeoutMs;
    }
}
