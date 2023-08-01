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

import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.message.HeartbeatRequestData;
import org.apache.kafka.common.message.HeartbeatResponseData;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.message.SyncGroupRequestData;
import org.apache.kafka.common.message.SyncGroupResponseData;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupCurrentMemberAssignmentKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupCurrentMemberAssignmentValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMemberMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMemberMetadataValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMetadataValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupPartitionMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupPartitionMetadataValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMemberKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMemberValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMetadataValue;
import org.apache.kafka.coordinator.group.generated.GroupMetadataKey;
import org.apache.kafka.coordinator.group.generated.GroupMetadataValue;
import org.apache.kafka.coordinator.group.generated.OffsetCommitKey;
import org.apache.kafka.coordinator.group.generated.OffsetCommitValue;
import org.apache.kafka.coordinator.group.runtime.CoordinatorShard;
import org.apache.kafka.coordinator.group.runtime.CoordinatorShardBuilder;
import org.apache.kafka.coordinator.group.runtime.CoordinatorResult;
import org.apache.kafka.coordinator.group.runtime.CoordinatorTimer;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.timeline.SnapshotRegistry;

import java.util.concurrent.CompletableFuture;

/**
 * The group coordinator shard is a replicated state machine that manages the metadata of all
 * generic and consumer groups. It holds the hard and the soft state of the groups. This class
 * has two kinds of methods:
 * 1) The request handlers which handle the requests and generate a response and records to
 *    mutate the hard state. Those records will be written by the runtime and applied to the
 *    hard state via the replay methods.
 * 2) The replay methods which apply records to the hard state. Those are used in the request
 *    handling as well as during the initial loading of the records from the partitions.
 */
public class GroupCoordinatorShard implements CoordinatorShard<Record> {

    public static class Builder implements CoordinatorShardBuilder<GroupCoordinatorShard, Record> {
        private final GroupCoordinatorConfig config;
        private LogContext logContext;
        private SnapshotRegistry snapshotRegistry;
        private Time time;
        private CoordinatorTimer<Void, Record> timer;

        public Builder(
            GroupCoordinatorConfig config
        ) {
            this.config = config;
        }

        @Override
        public CoordinatorShardBuilder<GroupCoordinatorShard, Record> withLogContext(
            LogContext logContext
        ) {
            this.logContext = logContext;
            return this;
        }

        @Override
        public CoordinatorShardBuilder<GroupCoordinatorShard, Record> withTime(
            Time time
        ) {
            this.time = time;
            return this;
        }

        @Override
        public CoordinatorShardBuilder<GroupCoordinatorShard, Record> withTimer(
            CoordinatorTimer<Void, Record> timer
        ) {
            this.timer = timer;
            return this;
        }

        @Override
        public CoordinatorShardBuilder<GroupCoordinatorShard, Record> withSnapshotRegistry(
            SnapshotRegistry snapshotRegistry
        ) {
            this.snapshotRegistry = snapshotRegistry;
            return this;
        }

        @Override
        public GroupCoordinatorShard build() {
            if (logContext == null) logContext = new LogContext();
            if (config == null)
                throw new IllegalArgumentException("Config must be set.");
            if (snapshotRegistry == null)
                throw new IllegalArgumentException("SnapshotRegistry must be set.");
            if (time == null)
                throw new IllegalArgumentException("Time must be set.");
            if (timer == null)
                throw new IllegalArgumentException("Timer must be set.");

            GroupMetadataManager groupMetadataManager = new GroupMetadataManager.Builder()
                .withLogContext(logContext)
                .withSnapshotRegistry(snapshotRegistry)
                .withTime(time)
                .withTimer(timer)
                .withConsumerGroupAssignors(config.consumerGroupAssignors)
                .withConsumerGroupMaxSize(config.consumerGroupMaxSize)
                .withConsumerGroupHeartbeatInterval(config.consumerGroupHeartbeatIntervalMs)
                .withGenericGroupInitialRebalanceDelayMs(config.genericGroupInitialRebalanceDelayMs)
                .withGenericGroupNewMemberJoinTimeoutMs(config.genericGroupNewMemberJoinTimeoutMs)
                .withGenericGroupMinSessionTimeoutMs(config.genericGroupMinSessionTimeoutMs)
                .withGenericGroupMaxSessionTimeoutMs(config.genericGroupMaxSessionTimeoutMs)
                .build();

            OffsetMetadataManager offsetMetadataManager = new OffsetMetadataManager.Builder()
                .withLogContext(logContext)
                .withSnapshotRegistry(snapshotRegistry)
                .withTime(time)
                .withGroupMetadataManager(groupMetadataManager)
                .withOffsetMetadataMaxSize(config.offsetMetadataMaxSize)
                .build();

            return new GroupCoordinatorShard(
                groupMetadataManager,
                offsetMetadataManager
            );
        }
    }

    /**
     * The group metadata manager.
     */
    private final GroupMetadataManager groupMetadataManager;

    /**
     * The offset metadata manager.
     */
    private final OffsetMetadataManager offsetMetadataManager;

    /**
     * Constructor.
     *
     * @param groupMetadataManager  The group metadata manager.
     * @param offsetMetadataManager The offset metadata manager.
     */
    GroupCoordinatorShard(
        GroupMetadataManager groupMetadataManager,
        OffsetMetadataManager offsetMetadataManager
    ) {
        this.groupMetadataManager = groupMetadataManager;
        this.offsetMetadataManager = offsetMetadataManager;
    }

    /**
     * Handles a ConsumerGroupHeartbeat request.
     *
     * @param context The request context.
     * @param request The actual ConsumerGroupHeartbeat request.
     *
     * @return A Result containing the ConsumerGroupHeartbeat response and
     *         a list of records to update the state machine.
     */
    public CoordinatorResult<ConsumerGroupHeartbeatResponseData, Record> consumerGroupHeartbeat(
        RequestContext context,
        ConsumerGroupHeartbeatRequestData request
    ) {
        return groupMetadataManager.consumerGroupHeartbeat(context, request);
    }

    /**
     * Handles a JoinGroup request.
     *
     * @param context The request context.
     * @param request The actual JoinGroup request.
     *
     * @return A Result containing the JoinGroup response and
     *         a list of records to update the state machine.
     */
    public CoordinatorResult<Void, Record> genericGroupJoin(
        RequestContext context,
        JoinGroupRequestData request,
        CompletableFuture<JoinGroupResponseData> responseFuture
    ) {
        return groupMetadataManager.genericGroupJoin(
            context,
            request,
            responseFuture
        );
    }

    /**
     * Handles a SyncGroup request.
     *
     * @param context The request context.
     * @param request The actual SyncGroup request.
     *
     * @return A Result containing the SyncGroup response and
     *         a list of records to update the state machine.
     */
    public CoordinatorResult<Void, Record> genericGroupSync(
        RequestContext context,
        SyncGroupRequestData request,
        CompletableFuture<SyncGroupResponseData> responseFuture
    ) {
        return groupMetadataManager.genericGroupSync(
            context,
            request,
            responseFuture
        );
    }

    /**
     * Handles a generic group HeartbeatRequest.
     *
     * @param context The request context.
     * @param request The actual Heartbeat request.
     *
     * @return The HeartbeatResponse.
     */
    public HeartbeatResponseData genericGroupHeartbeat(
        RequestContext context,
        HeartbeatRequestData request
    ) {
        return groupMetadataManager.genericGroupHeartbeat(
            context,
            request
        );
    }

    /**
     * Handles a OffsetCommit request.
     *
     * @param context The request context.
     * @param request The actual OffsetCommit request.
     *
     * @return A Result containing the OffsetCommitResponse response and
     *         a list of records to update the state machine.
     */
    public CoordinatorResult<OffsetCommitResponseData, Record> commitOffset(
        RequestContext context,
        OffsetCommitRequestData request
    ) throws ApiException {
        return offsetMetadataManager.commitOffset(context, request);
    }

    /**
     * The coordinator has been loaded. This is used to apply any
     * post loading operations (e.g. registering timers).
     *
     * @param newImage  The metadata image.
     */
    @Override
    public void onLoaded(MetadataImage newImage) {
        MetadataDelta emptyDelta = new MetadataDelta(newImage);
        groupMetadataManager.onNewMetadataImage(newImage, emptyDelta);
        offsetMetadataManager.onNewMetadataImage(newImage, emptyDelta);

        groupMetadataManager.onLoaded();
    }

    /**
     * A new metadata image is available.
     *
     * @param newImage  The new metadata image.
     * @param delta     The delta image.
     */
    @Override
    public void onNewMetadataImage(MetadataImage newImage, MetadataDelta delta) {
        groupMetadataManager.onNewMetadataImage(newImage, delta);
        offsetMetadataManager.onNewMetadataImage(newImage, delta);
    }

    /**
     * @return The ApiMessage or null.
     */
    private ApiMessage messageOrNull(ApiMessageAndVersion apiMessageAndVersion) {
        if (apiMessageAndVersion == null) {
            return null;
        } else {
            return apiMessageAndVersion.message();
        }
    }

    /**
     * Replays the Record to update the hard state of the group coordinator.

     * @param record The record to apply to the state machine.
     * @throws RuntimeException
     */
    @Override
    public void replay(Record record) throws RuntimeException {
        ApiMessageAndVersion key = record.key();
        ApiMessageAndVersion value = record.value();

        switch (key.version()) {
            case 0:
            case 1:
                offsetMetadataManager.replay(
                    (OffsetCommitKey) key.message(),
                    (OffsetCommitValue) messageOrNull(value)
                );
                break;

            case 2:
                groupMetadataManager.replay(
                    (GroupMetadataKey) key.message(),
                    (GroupMetadataValue) messageOrNull(value)
                );
                break;

            case 3:
                groupMetadataManager.replay(
                    (ConsumerGroupMetadataKey) key.message(),
                    (ConsumerGroupMetadataValue) messageOrNull(value)
                );
                break;

            case 4:
                groupMetadataManager.replay(
                    (ConsumerGroupPartitionMetadataKey) key.message(),
                    (ConsumerGroupPartitionMetadataValue) messageOrNull(value)
                );
                break;

            case 5:
                groupMetadataManager.replay(
                    (ConsumerGroupMemberMetadataKey) key.message(),
                    (ConsumerGroupMemberMetadataValue) messageOrNull(value)
                );
                break;

            case 6:
                groupMetadataManager.replay(
                    (ConsumerGroupTargetAssignmentMetadataKey) key.message(),
                    (ConsumerGroupTargetAssignmentMetadataValue) messageOrNull(value)
                );
                break;

            case 7:
                groupMetadataManager.replay(
                    (ConsumerGroupTargetAssignmentMemberKey) key.message(),
                    (ConsumerGroupTargetAssignmentMemberValue) messageOrNull(value)
                );
                break;

            case 8:
                groupMetadataManager.replay(
                    (ConsumerGroupCurrentMemberAssignmentKey) key.message(),
                    (ConsumerGroupCurrentMemberAssignmentValue) messageOrNull(value)
                );
                break;

            default:
                throw new IllegalStateException("Received an unknown record type " + key.version()
                    + " in " + record);
        }
    }
}
