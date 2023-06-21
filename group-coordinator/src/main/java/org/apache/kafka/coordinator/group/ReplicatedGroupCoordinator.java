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
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.utils.LogContext;
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
import org.apache.kafka.coordinator.group.runtime.Coordinator;
import org.apache.kafka.coordinator.group.runtime.CoordinatorBuilder;
import org.apache.kafka.coordinator.group.runtime.CoordinatorResult;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.timeline.SnapshotRegistry;

/**
 * The group coordinator replicated state machine that manages the metadata of all generic and
 * consumer groups. It holds the hard and the soft state of the groups. This class has two kinds
 * of methods:
 * 1) The request handlers which handle the requests and generate a response and records to
 *    mutate the hard state. Those records will be written by the runtime and applied to the
 *    hard state via the replay methods.
 * 2) The replay methods which apply records to the hard state. Those are used in the request
 *    handling as well as during the initial loading of the records from the partitions.
 */
public class ReplicatedGroupCoordinator implements Coordinator<Record> {

    public static class Builder implements CoordinatorBuilder<ReplicatedGroupCoordinator, Record> {
        private final GroupCoordinatorConfig config;
        private LogContext logContext;
        private SnapshotRegistry snapshotRegistry;

        public Builder(
            GroupCoordinatorConfig config
        ) {
            this.config = config;
        }

        @Override
        public CoordinatorBuilder<ReplicatedGroupCoordinator, Record> withLogContext(
            LogContext logContext
        ) {
            this.logContext = logContext;
            return this;
        }

        @Override
        public CoordinatorBuilder<ReplicatedGroupCoordinator, Record> withSnapshotRegistry(
            SnapshotRegistry snapshotRegistry
        ) {
            this.snapshotRegistry = snapshotRegistry;
            return this;
        }

        @Override
        public ReplicatedGroupCoordinator build() {
            if (logContext == null) logContext = new LogContext();
            if (config == null)
                throw new IllegalArgumentException("Config must be set.");
            if (snapshotRegistry == null)
                throw new IllegalArgumentException("SnapshotRegistry must be set.");

            return new ReplicatedGroupCoordinator(
                new GroupMetadataManager.Builder()
                    .withLogContext(logContext)
                    .withSnapshotRegistry(snapshotRegistry)
                    .withAssignors(config.consumerGroupAssignors)
                    .withConsumerGroupMaxSize(config.consumerGroupMaxSize)
                    .withConsumerGroupHeartbeatInterval(config.consumerGroupHeartbeatIntervalMs)
                    .build()
            );
        }
    }

    /**
     * The group metadata manager.
     */
    private final GroupMetadataManager groupMetadataManager;

    /**
     * Constructor.
     *
     * @param groupMetadataManager The group metadata manager.
     */
    ReplicatedGroupCoordinator(
        GroupMetadataManager groupMetadataManager
    ) {
        this.groupMetadataManager = groupMetadataManager;
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
