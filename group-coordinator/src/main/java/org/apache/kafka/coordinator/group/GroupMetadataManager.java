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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.CoordinatorNotAvailableException;
import org.apache.kafka.common.errors.FencedMemberEpochException;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.GroupMaxSizeReachedException;
import org.apache.kafka.common.errors.IllegalGenerationException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.errors.UnsupportedAssignorException;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.message.DescribeGroupsResponseData;
import org.apache.kafka.common.message.HeartbeatRequestData;
import org.apache.kafka.common.message.HeartbeatResponseData;
import org.apache.kafka.common.message.JoinGroupRequestData.JoinGroupRequestProtocol;
import org.apache.kafka.common.message.JoinGroupRequestData.JoinGroupRequestProtocolCollection;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.message.LeaveGroupRequestData;
import org.apache.kafka.common.message.LeaveGroupRequestData.MemberIdentity;
import org.apache.kafka.common.message.LeaveGroupResponseData;
import org.apache.kafka.common.message.LeaveGroupResponseData.MemberResponse;
import org.apache.kafka.common.message.ListGroupsResponseData;
import org.apache.kafka.common.message.SyncGroupRequestData;
import org.apache.kafka.common.message.SyncGroupResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.coordinator.group.assignor.PartitionAssignor;
import org.apache.kafka.coordinator.group.assignor.PartitionAssignorException;
import org.apache.kafka.coordinator.group.consumer.Assignment;
import org.apache.kafka.coordinator.group.consumer.ConsumerGroup;
import org.apache.kafka.coordinator.group.consumer.ConsumerGroupMember;
import org.apache.kafka.coordinator.group.consumer.CurrentAssignmentBuilder;
import org.apache.kafka.coordinator.group.consumer.TargetAssignmentBuilder;
import org.apache.kafka.coordinator.group.consumer.TopicMetadata;
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
import org.apache.kafka.coordinator.group.generic.GenericGroup;
import org.apache.kafka.coordinator.group.generic.GenericGroupMember;
import org.apache.kafka.coordinator.group.generic.GenericGroupState;
import org.apache.kafka.coordinator.group.runtime.CoordinatorResult;
import org.apache.kafka.coordinator.group.runtime.CoordinatorTimer;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.TopicImage;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;
import org.apache.kafka.timeline.TimelineHashSet;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.kafka.common.protocol.Errors.COORDINATOR_NOT_AVAILABLE;
import static org.apache.kafka.common.protocol.Errors.ILLEGAL_GENERATION;
import static org.apache.kafka.common.protocol.Errors.NOT_COORDINATOR;
import static org.apache.kafka.common.protocol.Errors.UNKNOWN_SERVER_ERROR;
import static org.apache.kafka.common.requests.ConsumerGroupHeartbeatRequest.LEAVE_GROUP_MEMBER_EPOCH;
import static org.apache.kafka.common.requests.JoinGroupRequest.UNKNOWN_MEMBER_ID;
import static org.apache.kafka.coordinator.group.Group.GroupType.CONSUMER;
import static org.apache.kafka.coordinator.group.Group.GroupType.GENERIC;
import static org.apache.kafka.coordinator.group.RecordHelpers.newCurrentAssignmentRecord;
import static org.apache.kafka.coordinator.group.RecordHelpers.newCurrentAssignmentTombstoneRecord;
import static org.apache.kafka.coordinator.group.RecordHelpers.newGroupEpochRecord;
import static org.apache.kafka.coordinator.group.RecordHelpers.newGroupSubscriptionMetadataRecord;
import static org.apache.kafka.coordinator.group.RecordHelpers.newMemberSubscriptionRecord;
import static org.apache.kafka.coordinator.group.RecordHelpers.newMemberSubscriptionTombstoneRecord;
import static org.apache.kafka.coordinator.group.RecordHelpers.newTargetAssignmentTombstoneRecord;
import static org.apache.kafka.coordinator.group.Utils.ofSentinel;
import static org.apache.kafka.coordinator.group.generic.GenericGroupMember.EMPTY_ASSIGNMENT;
import static org.apache.kafka.coordinator.group.generic.GenericGroupState.COMPLETING_REBALANCE;
import static org.apache.kafka.coordinator.group.generic.GenericGroupState.DEAD;
import static org.apache.kafka.coordinator.group.generic.GenericGroupState.EMPTY;
import static org.apache.kafka.coordinator.group.generic.GenericGroupState.PREPARING_REBALANCE;
import static org.apache.kafka.coordinator.group.generic.GenericGroupState.STABLE;

/**
 * The GroupMetadataManager manages the metadata of all generic and consumer groups. It holds
 * the hard and the soft state of the groups. This class has two kinds of methods:
 * 1) The request handlers which handle the requests and generate a response and records to
 *    mutate the hard state. Those records will be written by the runtime and applied to the
 *    hard state via the replay methods.
 * 2) The replay methods which apply records to the hard state. Those are used in the request
 *    handling as well as during the initial loading of the records from the partitions.
 */
public class GroupMetadataManager {

    public static class Builder {
        private LogContext logContext = null;
        private SnapshotRegistry snapshotRegistry = null;
        private Time time = null;
        private CoordinatorTimer<Void, Record> timer = null;
        private List<PartitionAssignor> consumerGroupAssignors = null;
        private int consumerGroupMaxSize = Integer.MAX_VALUE;
        private int consumerGroupHeartbeatIntervalMs = 5000;
        private int consumerGroupMetadataRefreshIntervalMs = Integer.MAX_VALUE;
        private MetadataImage metadataImage = null;
        private int consumerGroupSessionTimeoutMs = 45000;
        private int genericGroupMaxSize = Integer.MAX_VALUE;
        private int genericGroupInitialRebalanceDelayMs = 3000;
        private int genericGroupNewMemberJoinTimeoutMs = 5 * 60 * 1000;
        private int genericGroupMinSessionTimeoutMs;
        private int genericGroupMaxSessionTimeoutMs;

        Builder withLogContext(LogContext logContext) {
            this.logContext = logContext;
            return this;
        }

        Builder withSnapshotRegistry(SnapshotRegistry snapshotRegistry) {
            this.snapshotRegistry = snapshotRegistry;
            return this;
        }

        Builder withTime(Time time) {
            this.time = time;
            return this;
        }

        Builder withTimer(CoordinatorTimer<Void, Record> timer) {
            this.timer = timer;
            return this;
        }

        Builder withConsumerGroupAssignors(List<PartitionAssignor> consumerGroupAssignors) {
            this.consumerGroupAssignors = consumerGroupAssignors;
            return this;
        }

        Builder withConsumerGroupMaxSize(int consumerGroupMaxSize) {
            this.consumerGroupMaxSize = consumerGroupMaxSize;
            return this;
        }

        Builder withConsumerGroupSessionTimeout(int consumerGroupSessionTimeoutMs) {
            this.consumerGroupSessionTimeoutMs = consumerGroupSessionTimeoutMs;
            return this;
        }

        Builder withConsumerGroupHeartbeatInterval(int consumerGroupHeartbeatIntervalMs) {
            this.consumerGroupHeartbeatIntervalMs = consumerGroupHeartbeatIntervalMs;
            return this;
        }

        Builder withConsumerGroupMetadataRefreshIntervalMs(int consumerGroupMetadataRefreshIntervalMs) {
            this.consumerGroupMetadataRefreshIntervalMs = consumerGroupMetadataRefreshIntervalMs;
            return this;
        }

        Builder withMetadataImage(MetadataImage metadataImage) {
            this.metadataImage = metadataImage;
            return this;
        }

        Builder withGenericGroupMaxSize(int genericGroupMaxSize) {
            this.genericGroupMaxSize = genericGroupMaxSize;
            return this;
        }

        Builder withGenericGroupInitialRebalanceDelayMs(int genericGroupInitialRebalanceDelayMs) {
            this.genericGroupInitialRebalanceDelayMs = genericGroupInitialRebalanceDelayMs;
            return this;
        }

        Builder withGenericGroupNewMemberJoinTimeoutMs(int genericGroupNewMemberJoinTimeoutMs) {
            this.genericGroupNewMemberJoinTimeoutMs = genericGroupNewMemberJoinTimeoutMs;
            return this;
        }

        Builder withGenericGroupMinSessionTimeoutMs(int genericGroupMinSessionTimeoutMs) {
            this.genericGroupMinSessionTimeoutMs = genericGroupMinSessionTimeoutMs;
            return this;
        }

        Builder withGenericGroupMaxSessionTimeoutMs(int genericGroupMaxSessionTimeoutMs) {
            this.genericGroupMaxSessionTimeoutMs = genericGroupMaxSessionTimeoutMs;
            return this;
        }

        GroupMetadataManager build() {
            if (logContext == null) logContext = new LogContext();
            if (snapshotRegistry == null) snapshotRegistry = new SnapshotRegistry(logContext);
            if (metadataImage == null) metadataImage = MetadataImage.EMPTY;
            if (time == null) time = Time.SYSTEM;

            if (timer == null)
                throw new IllegalArgumentException("Timer must be set.");
            if (consumerGroupAssignors == null || consumerGroupAssignors.isEmpty())
                throw new IllegalArgumentException("Assignors must be set before building.");

            return new GroupMetadataManager(
                snapshotRegistry,
                logContext,
                time,
                timer,
                consumerGroupAssignors,
                metadataImage,
                consumerGroupMaxSize,
                consumerGroupSessionTimeoutMs,
                consumerGroupHeartbeatIntervalMs,
                consumerGroupMetadataRefreshIntervalMs,
                genericGroupMaxSize,
                genericGroupInitialRebalanceDelayMs,
                genericGroupNewMemberJoinTimeoutMs,
                genericGroupMinSessionTimeoutMs,
                genericGroupMaxSessionTimeoutMs
            );
        }
    }

    /**
     * The log context.
     */
    private final LogContext logContext;

    /**
     * The logger.
     */
    private final Logger log;

    /**
     * The snapshot registry.
     */
    private final SnapshotRegistry snapshotRegistry;

    /**
     * The system time.
     */
    private final Time time;

    /**
     * The system timer.
     */
    private final CoordinatorTimer<Void, Record> timer;

    /**
     * The supported partition assignors keyed by their name.
     */
    private final Map<String, PartitionAssignor> assignors;

    /**
     * The default assignor used.
     */
    private final PartitionAssignor defaultAssignor;

    /**
     * The generic and consumer groups keyed by their name.
     */
    private final TimelineHashMap<String, Group> groups;

    /**
     * The group ids keyed by topic names.
     */
    private final TimelineHashMap<String, TimelineHashSet<String>> groupsByTopics;

    /**
     * The maximum number of members allowed in a single consumer group.
     */
    private final int consumerGroupMaxSize;

    /**
     * The heartbeat interval for consumer groups.
     */
    private final int consumerGroupHeartbeatIntervalMs;

    /**
     * The session timeout for consumer groups.
     */
    private final int consumerGroupSessionTimeoutMs;

    /**
     * The metadata refresh interval.
     */
    private final int consumerGroupMetadataRefreshIntervalMs;

    /**
     * The metadata image.
     */
    private MetadataImage metadataImage;

    /**
     * An empty result returned to the state machine. This means that
     * there are no records to append to the log.
     *
     * Package private for testing.
     */
    static final CoordinatorResult<Void, Record> EMPTY_RESULT =
        new CoordinatorResult<>(Collections.emptyList(), CompletableFuture.completedFuture(null));

    /**
     * The maximum number of members allowed in a single generic group.
     */
    private final int genericGroupMaxSize;

    /**
     * Initial rebalance delay for members joining a generic group.
     */
    private final int genericGroupInitialRebalanceDelayMs;

    /**
     * The timeout used to wait for a new member in milliseconds.
     */
    private final int genericGroupNewMemberJoinTimeoutMs;

    /**
     * The group minimum session timeout.
     */
    private final int genericGroupMinSessionTimeoutMs;

    /**
     * The group maximum session timeout.
     */
    private final int genericGroupMaxSessionTimeoutMs;

    private GroupMetadataManager(
        SnapshotRegistry snapshotRegistry,
        LogContext logContext,
        Time time,
        CoordinatorTimer<Void, Record> timer,
        List<PartitionAssignor> assignors,
        MetadataImage metadataImage,
        int consumerGroupMaxSize,
        int consumerGroupSessionTimeoutMs,
        int consumerGroupHeartbeatIntervalMs,
        int consumerGroupMetadataRefreshIntervalMs,
        int genericGroupMaxSize,
        int genericGroupInitialRebalanceDelayMs,
        int genericGroupNewMemberJoinTimeoutMs,
        int genericGroupMinSessionTimeoutMs,
        int genericGroupMaxSessionTimeoutMs
    ) {
        this.logContext = logContext;
        this.log = logContext.logger(GroupMetadataManager.class);
        this.snapshotRegistry = snapshotRegistry;
        this.time = time;
        this.timer = timer;
        this.metadataImage = metadataImage;
        this.assignors = assignors.stream().collect(Collectors.toMap(PartitionAssignor::name, Function.identity()));
        this.defaultAssignor = assignors.get(0);
        this.groups = new TimelineHashMap<>(snapshotRegistry, 0);
        this.groupsByTopics = new TimelineHashMap<>(snapshotRegistry, 0);
        this.consumerGroupMaxSize = consumerGroupMaxSize;
        this.consumerGroupSessionTimeoutMs = consumerGroupSessionTimeoutMs;
        this.consumerGroupHeartbeatIntervalMs = consumerGroupHeartbeatIntervalMs;
        this.consumerGroupMetadataRefreshIntervalMs = consumerGroupMetadataRefreshIntervalMs;
        this.genericGroupMaxSize = genericGroupMaxSize;
        this.genericGroupInitialRebalanceDelayMs = genericGroupInitialRebalanceDelayMs;
        this.genericGroupNewMemberJoinTimeoutMs = genericGroupNewMemberJoinTimeoutMs;
        this.genericGroupMinSessionTimeoutMs = genericGroupMinSessionTimeoutMs;
        this.genericGroupMaxSessionTimeoutMs = genericGroupMaxSessionTimeoutMs;
    }

    /**
     * @return The current metadata image used by the group metadata manager.
     */
    public MetadataImage image() {
        return metadataImage;
    }

    /**
     * @return The group corresponding to the group id or throw GroupIdNotFoundException.
     */
    public Group group(String groupId) throws GroupIdNotFoundException {
        Group group = groups.get(groupId, Long.MAX_VALUE);
        if (group == null) {
            throw new GroupIdNotFoundException(String.format("Group %s not found.", groupId));
        }
        return group;
    }

    /**
     * @return The group corresponding to the group id at the given committed offset
     *         or throw GroupIdNotFoundException.
     */
    public Group group(String groupId, long committedOffset) throws GroupIdNotFoundException {
        Group group = groups.get(groupId, committedOffset);
        if (group == null) {
            throw new GroupIdNotFoundException(String.format("Group %s not found.", groupId));
        }
        return group;
    }

    /**
     * Get the Group List.
     *
     * @param statesFilter The states of the groups we want to list.
     *                     If empty all groups are returned with their state.
     * @param committedOffset A specified committed offset corresponding to this shard
     *
     * @return A list containing the ListGroupsResponseData.ListedGroup
     */

    public List<ListGroupsResponseData.ListedGroup> listGroups(List<String> statesFilter, long committedOffset) {
        Stream<Group> groupStream = groups.values(committedOffset).stream();
        if (!statesFilter.isEmpty()) {
            groupStream = groupStream.filter(group -> statesFilter.contains(group.stateAsString(committedOffset)));
        }
        return groupStream.map(group -> group.asListedGroup(committedOffset)).collect(Collectors.toList());
    }

    /**
     * Handles a DescribeGroup request.
     *
     * @param groupIds          The IDs of the groups to describe.
     * @param committedOffset   A specified committed offset corresponding to this shard.
     *
     * @return A list containing the DescribeGroupsResponseData.DescribedGroup.
     */
    public List<DescribeGroupsResponseData.DescribedGroup> describeGroups(
        List<String> groupIds,
        long committedOffset
    ) {
        final List<DescribeGroupsResponseData.DescribedGroup> describedGroups = new ArrayList<>();
        groupIds.forEach(groupId -> {
            try {
                GenericGroup group = genericGroup(groupId, committedOffset);

                if (group.isInState(STABLE)) {
                    if (!group.protocolName().isPresent()) {
                        throw new IllegalStateException("Invalid null group protocol for stable group");
                    }

                    describedGroups.add(new DescribeGroupsResponseData.DescribedGroup()
                        .setGroupId(groupId)
                        .setGroupState(group.stateAsString())
                        .setProtocolType(group.protocolType().orElse(""))
                        .setProtocolData(group.protocolName().get())
                        .setMembers(group.allMembers().stream()
                            .map(member -> member.describe(group.protocolName().get()))
                            .collect(Collectors.toList())
                        )
                    );
                } else {
                    describedGroups.add(new DescribeGroupsResponseData.DescribedGroup()
                        .setGroupId(groupId)
                        .setGroupState(group.stateAsString())
                        .setProtocolType(group.protocolType().orElse(""))
                        .setMembers(group.allMembers().stream()
                            .map(member -> member.describeNoMetadata())
                            .collect(Collectors.toList())
                        )
                    );
                }
            } catch (GroupIdNotFoundException exception) {
                describedGroups.add(new DescribeGroupsResponseData.DescribedGroup()
                    .setGroupId(groupId)
                    .setGroupState(DEAD.toString())
                );
            }
        });
        return describedGroups;
    }

    /**
     * Gets or maybe creates a consumer group.
     *
     * @param groupId           The group id.
     * @param createIfNotExists A boolean indicating whether the group should be
     *                          created if it does not exist.
     *
     * @return A ConsumerGroup.
     * @throws GroupIdNotFoundException if the group does not exist and createIfNotExists is false or
     *                                  if the group is not a consumer group.
     *
     * Package private for testing.
     */
    ConsumerGroup getOrMaybeCreateConsumerGroup(
        String groupId,
        boolean createIfNotExists
    ) throws GroupIdNotFoundException {
        Group group = groups.get(groupId);

        if (group == null && !createIfNotExists) {
            throw new GroupIdNotFoundException(String.format("Consumer group %s not found.", groupId));
        }

        if (group == null) {
            ConsumerGroup consumerGroup = new ConsumerGroup(snapshotRegistry, groupId);
            groups.put(groupId, consumerGroup);
            return consumerGroup;
        } else {
            if (group.type() == CONSUMER) {
                return (ConsumerGroup) group;
            } else {
                // We don't support upgrading/downgrading between protocols at the moment so
                // we throw an exception if a group exists with the wrong type.
                throw new GroupIdNotFoundException(String.format("Group %s is not a consumer group.", groupId));
            }
        }
    }

    /**
     * Gets or maybe creates a generic group.
     *
     * @param groupId           The group id.
     * @param createIfNotExists A boolean indicating whether the group should be
     *                          created if it does not exist.
     *
     * @return A GenericGroup.
     * @throws UnknownMemberIdException if the group does not exist and createIfNotExists is false.
     * @throws GroupIdNotFoundException if the group is not a generic group.
     *
     * Package private for testing.
     */
    GenericGroup getOrMaybeCreateGenericGroup(
        String groupId,
        boolean createIfNotExists
    ) throws UnknownMemberIdException, GroupIdNotFoundException {
        Group group = groups.get(groupId);

        if (group == null && !createIfNotExists) {
            throw new UnknownMemberIdException(String.format("Generic group %s not found.", groupId));
        }

        if (group == null) {
            GenericGroup genericGroup = new GenericGroup(logContext, groupId, GenericGroupState.EMPTY, time);
            groups.put(groupId, genericGroup);
            return genericGroup;
        } else {
            if (group.type() == GENERIC) {
                return (GenericGroup) group;
            } else {
                // We don't support upgrading/downgrading between protocols at the moment so
                // we throw an exception if a group exists with the wrong type.
                throw new GroupIdNotFoundException(String.format("Group %s is not a generic group.",
                    groupId));
            }
        }
    }

    /**
     * Gets a generic group by committed offset.
     *
     * @param groupId           The group id.
     * @param committedOffset   A specified committed offset corresponding to this shard.
     *
     * @return A GenericGroup.
     * @throws GroupIdNotFoundException if the group does not exist or is not a generic group.
     */
    public GenericGroup genericGroup(
        String groupId,
        long committedOffset
    ) throws GroupIdNotFoundException {
        Group group = group(groupId, committedOffset);

        if (group.type() == GENERIC) {
            return (GenericGroup) group;
        } else {
            // We don't support upgrading/downgrading between protocols at the moment so
            // we throw an exception if a group exists with the wrong type.
            throw new GroupIdNotFoundException(String.format("Group %s is not a generic group.",
                groupId));
        }
    }

    /**
     * Removes the group.
     *
     * @param groupId The group id.
     */
    private void removeGroup(
        String groupId
    ) {
        groups.remove(groupId);
    }

    /**
     * Throws an InvalidRequestException if the value is non-null and empty.
     *
     * @param value The value.
     * @param error The error message.
     * @throws InvalidRequestException
     */
    private void throwIfEmptyString(
        String value,
        String error
    ) throws InvalidRequestException {
        if (value != null && value.isEmpty()) {
            throw new InvalidRequestException(error);
        }
    }

    /**
     * Throws an InvalidRequestException if the value is non-null.
     *
     * @param value The value.
     * @param error The error message.
     * @throws InvalidRequestException
     */
    private void throwIfNotNull(
        Object value,
        String error
    ) throws InvalidRequestException {
        if (value != null) {
            throw new InvalidRequestException(error);
        }
    }

    /**
     * Validates the request.
     *
     * @param request The request to validate.
     *
     * @throws InvalidRequestException if the request is not valid.
     * @throws UnsupportedAssignorException if the assignor is not supported.
     */
    private void throwIfConsumerGroupHeartbeatRequestIsInvalid(
        ConsumerGroupHeartbeatRequestData request
    ) throws InvalidRequestException, UnsupportedAssignorException {
        throwIfEmptyString(request.groupId(), "GroupId can't be empty.");
        throwIfEmptyString(request.instanceId(), "InstanceId can't be empty.");
        throwIfEmptyString(request.rackId(), "RackId can't be empty.");
        throwIfNotNull(request.subscribedTopicRegex(), "SubscribedTopicRegex is not supported yet.");

        if (request.memberEpoch() > 0 || request.memberEpoch() == LEAVE_GROUP_MEMBER_EPOCH) {
            throwIfEmptyString(request.memberId(), "MemberId can't be empty.");
        } else if (request.memberEpoch() == 0) {
            if (request.rebalanceTimeoutMs() == -1) {
                throw new InvalidRequestException("RebalanceTimeoutMs must be provided in first request.");
            }
            if (request.topicPartitions() == null || !request.topicPartitions().isEmpty()) {
                throw new InvalidRequestException("TopicPartitions must be empty when (re-)joining.");
            }
            if (request.subscribedTopicNames() == null || request.subscribedTopicNames().isEmpty()) {
                throw new InvalidRequestException("SubscribedTopicNames must be set in first request.");
            }
        } else {
            throw new InvalidRequestException("MemberEpoch is invalid.");
        }

        if (request.serverAssignor() != null && !assignors.containsKey(request.serverAssignor())) {
            throw new UnsupportedAssignorException("ServerAssignor " + request.serverAssignor()
                + " is not supported. Supported assignors: " + String.join(", ", assignors.keySet())
                + ".");
        }
    }

    /**
     * Verifies that the partitions currently owned by the member (the ones set in the
     * request) matches the ones that the member should own. It matches if the consumer
     * only owns partitions which are in the assigned partitions. It does not match if
     * it owns any other partitions.
     *
     * @param ownedTopicPartitions  The partitions provided by the consumer in the request.
     * @param target                The partitions that the member should have.
     *
     * @return A boolean indicating whether the owned partitions are a subset or not.
     */
    private boolean isSubset(
        List<ConsumerGroupHeartbeatRequestData.TopicPartitions> ownedTopicPartitions,
        Map<Uuid, Set<Integer>> target
    ) {
        if (ownedTopicPartitions == null) return false;

        for (ConsumerGroupHeartbeatRequestData.TopicPartitions topicPartitions : ownedTopicPartitions) {
            Set<Integer> partitions = target.get(topicPartitions.topicId());
            if (partitions == null) return false;
            for (Integer partitionId : topicPartitions.partitions()) {
                if (!partitions.contains(partitionId)) return false;
            }
        }

        return true;
    }

    /**
     * Checks whether the consumer group can accept a new member or not based on the
     * max group size defined.
     *
     * @param group     The consumer group.
     * @param memberId  The member id.
     *
     * @throws GroupMaxSizeReachedException if the maximum capacity has been reached.
     */
    private void throwIfConsumerGroupIsFull(
        ConsumerGroup group,
        String memberId
    ) throws GroupMaxSizeReachedException {
        // If the consumer group has reached its maximum capacity, the member is rejected if it is not
        // already a member of the consumer group.
        if (group.numMembers() >= consumerGroupMaxSize && (memberId.isEmpty() || !group.hasMember(memberId))) {
            throw new GroupMaxSizeReachedException("The consumer group has reached its maximum capacity of "
                + consumerGroupMaxSize + " members.");
        }
    }

    /**
     * Validates the member epoch provided in the heartbeat request.
     *
     * @param member                The consumer group member.
     * @param receivedMemberEpoch   The member epoch.
     * @param ownedTopicPartitions  The owned partitions.
     *
     * @throws FencedMemberEpochException if the provided epoch is ahead or behind the epoch known
     *                                    by this coordinator.
     */
    private void throwIfMemberEpochIsInvalid(
        ConsumerGroupMember member,
        int receivedMemberEpoch,
        List<ConsumerGroupHeartbeatRequestData.TopicPartitions> ownedTopicPartitions
    ) {
        if (receivedMemberEpoch > member.memberEpoch()) {
            throw new FencedMemberEpochException("The consumer group member has a greater member "
                + "epoch (" + receivedMemberEpoch + ") than the one known by the group coordinator ("
                + member.memberEpoch() + "). The member must abandon all its partitions and rejoin.");
        } else if (receivedMemberEpoch < member.memberEpoch()) {
            // If the member comes with the previous epoch and has a subset of the current assignment partitions,
            // we accept it because the response with the bumped epoch may have been lost.
            if (receivedMemberEpoch != member.previousMemberEpoch() || !isSubset(ownedTopicPartitions, member.assignedPartitions())) {
                throw new FencedMemberEpochException("The consumer group member has a smaller member "
                    + "epoch (" + receivedMemberEpoch + ") than the one known by the group coordinator ("
                    + member.memberEpoch() + "). The member must abandon all its partitions and rejoin.");
            }
        }
    }

    private ConsumerGroupHeartbeatResponseData.Assignment createResponseAssignment(
        ConsumerGroupMember member
    ) {
        return new ConsumerGroupHeartbeatResponseData.Assignment()
            .setTopicPartitions(fromAssignmentMap(member.assignedPartitions()));
    }

    private List<ConsumerGroupHeartbeatResponseData.TopicPartitions> fromAssignmentMap(
        Map<Uuid, Set<Integer>> assignment
    ) {
        return assignment.entrySet().stream()
            .map(keyValue -> new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                .setTopicId(keyValue.getKey())
                .setPartitions(new ArrayList<>(keyValue.getValue())))
            .collect(Collectors.toList());
    }

    /**
     * Handles a regular heartbeat from a consumer group member. It mainly consists of
     * three parts:
     * 1) The member is created or updated. The group epoch is bumped if the member
     *    has been created or updated.
     * 2) The target assignment for the consumer group is updated if the group epoch
     *    is larger than the current target assignment epoch.
     * 3) The member's assignment is reconciled with the target assignment.
     *
     * @param groupId               The group id from the request.
     * @param memberId              The member id from the request.
     * @param memberEpoch           The member epoch from the request.
     * @param instanceId            The instance id from the request or null.
     * @param rackId                The rack id from the request or null.
     * @param rebalanceTimeoutMs    The rebalance timeout from the request or -1.
     * @param clientId              The client id.
     * @param clientHost            The client host.
     * @param subscribedTopicNames  The list of subscribed topic names from the request
     *                              of null.
     * @param subscribedTopicRegex  The regular expression based subscription from the
     *                              request or null.
     * @param assignorName          The assignor name from the request or null.
     * @param ownedTopicPartitions  The list of owned partitions from the request or null.
     *
     * @return A Result containing the ConsumerGroupHeartbeat response and
     *         a list of records to update the state machine.
     */
    private CoordinatorResult<ConsumerGroupHeartbeatResponseData, Record> consumerGroupHeartbeat(
        String groupId,
        String memberId,
        int memberEpoch,
        String instanceId,
        String rackId,
        int rebalanceTimeoutMs,
        String clientId,
        String clientHost,
        List<String> subscribedTopicNames,
        String subscribedTopicRegex,
        String assignorName,
        List<ConsumerGroupHeartbeatRequestData.TopicPartitions> ownedTopicPartitions
    ) throws ApiException {
        final long currentTimeMs = time.milliseconds();
        final List<Record> records = new ArrayList<>();

        // Get or create the consumer group.
        boolean createIfNotExists = memberEpoch == 0;
        final ConsumerGroup group = getOrMaybeCreateConsumerGroup(groupId, createIfNotExists);
        throwIfConsumerGroupIsFull(group, memberId);

        // Get or create the member.
        if (memberId.isEmpty()) memberId = Uuid.randomUuid().toString();
        final ConsumerGroupMember member = group.getOrMaybeCreateMember(memberId, createIfNotExists);
        throwIfMemberEpochIsInvalid(member, memberEpoch, ownedTopicPartitions);

        if (memberEpoch == 0) {
            log.info("[GroupId {}] Member {} joins the consumer group.", groupId, memberId);
        }

        // 1. Create or update the member. If the member is new or has changed, a ConsumerGroupMemberMetadataValue
        // record is written to the __consumer_offsets partition to persist the change. If the subscriptions have
        // changed, the subscription metadata is updated and persisted by writing a ConsumerGroupPartitionMetadataValue
        // record to the __consumer_offsets partition. Finally, the group epoch is bumped if the subscriptions have
        // changed, and persisted by writing a ConsumerGroupMetadataValue record to the partition.
        int groupEpoch = group.groupEpoch();
        Map<String, TopicMetadata> subscriptionMetadata = group.subscriptionMetadata();
        ConsumerGroupMember updatedMember = new ConsumerGroupMember.Builder(member)
            .maybeUpdateInstanceId(Optional.ofNullable(instanceId))
            .maybeUpdateRackId(Optional.ofNullable(rackId))
            .maybeUpdateRebalanceTimeoutMs(ofSentinel(rebalanceTimeoutMs))
            .maybeUpdateServerAssignorName(Optional.ofNullable(assignorName))
            .maybeUpdateSubscribedTopicNames(Optional.ofNullable(subscribedTopicNames))
            .maybeUpdateSubscribedTopicRegex(Optional.ofNullable(subscribedTopicRegex))
            .setClientId(clientId)
            .setClientHost(clientHost)
            .build();

        boolean bumpGroupEpoch = false;
        if (!updatedMember.equals(member)) {
            records.add(newMemberSubscriptionRecord(groupId, updatedMember));

            if (!updatedMember.subscribedTopicNames().equals(member.subscribedTopicNames())) {
                log.info("[GroupId {}] Member {} updated its subscribed topics to: {}.",
                    groupId, memberId, updatedMember.subscribedTopicNames());
                bumpGroupEpoch = true;
            }

            if (!updatedMember.subscribedTopicRegex().equals(member.subscribedTopicRegex())) {
                log.info("[GroupId {}] Member {} updated its subscribed regex to: {}.",
                    groupId, memberId, updatedMember.subscribedTopicRegex());
                bumpGroupEpoch = true;
            }
        }

        if (bumpGroupEpoch || group.hasMetadataExpired(currentTimeMs)) {
            // The subscription metadata is updated in two cases:
            // 1) The member has updated its subscriptions;
            // 2) The refresh deadline has been reached.
            subscriptionMetadata = group.computeSubscriptionMetadata(
                member,
                updatedMember,
                metadataImage.topics(),
                metadataImage.cluster()
            );

            if (!subscriptionMetadata.equals(group.subscriptionMetadata())) {
                log.info("[GroupId {}] Computed new subscription metadata: {}.",
                    groupId, subscriptionMetadata);
                bumpGroupEpoch = true;
                records.add(newGroupSubscriptionMetadataRecord(groupId, subscriptionMetadata));
            }

            if (bumpGroupEpoch) {
                groupEpoch += 1;
                records.add(newGroupEpochRecord(groupId, groupEpoch));
                log.info("[GroupId {}] Bumped group epoch to {}.", groupId, groupEpoch);
            }

            group.setMetadataRefreshDeadline(currentTimeMs + consumerGroupMetadataRefreshIntervalMs, groupEpoch);
        }

        // 2. Update the target assignment if the group epoch is larger than the target assignment epoch. The
        // delta between the existing and the new target assignment is persisted to the partition.
        int targetAssignmentEpoch = group.assignmentEpoch();
        Assignment targetAssignment = group.targetAssignment(memberId);
        if (groupEpoch > targetAssignmentEpoch) {
            String preferredServerAssignor = group.computePreferredServerAssignor(
                member,
                updatedMember
            ).orElse(defaultAssignor.name());

            try {
                TargetAssignmentBuilder.TargetAssignmentResult assignmentResult =
                    new TargetAssignmentBuilder(groupId, groupEpoch, assignors.get(preferredServerAssignor))
                        .withMembers(group.members())
                        .withSubscriptionMetadata(subscriptionMetadata)
                        .withTargetAssignment(group.targetAssignment())
                        .addOrUpdateMember(memberId, updatedMember)
                        .build();

                log.info("[GroupId {}] Computed a new target assignment for epoch {}: {}.",
                    groupId, groupEpoch, assignmentResult.targetAssignment());

                records.addAll(assignmentResult.records());
                targetAssignment = assignmentResult.targetAssignment().get(memberId);
                targetAssignmentEpoch = groupEpoch;
            } catch (PartitionAssignorException ex) {
                String msg = String.format("Failed to compute a new target assignment for epoch %d: %s",
                    groupEpoch, ex.getMessage());
                log.error("[GroupId {}] {}.", groupId, msg);
                throw new UnknownServerException(msg, ex);
            }
        }

        // 3. Reconcile the member's assignment with the target assignment. This is only required if
        // the member is not stable or if a new target assignment has been installed.
        boolean assignmentUpdated = false;
        if (updatedMember.state() != ConsumerGroupMember.MemberState.STABLE || updatedMember.targetMemberEpoch() != targetAssignmentEpoch) {
            ConsumerGroupMember prevMember = updatedMember;
            updatedMember = new CurrentAssignmentBuilder(updatedMember)
                .withTargetAssignment(targetAssignmentEpoch, targetAssignment)
                .withCurrentPartitionEpoch(group::currentPartitionEpoch)
                .withOwnedTopicPartitions(ownedTopicPartitions)
                .build();

            // Checking the reference is enough here because a new instance
            // is created only when the state has changed.
            if (updatedMember != prevMember) {
                assignmentUpdated = true;
                records.add(newCurrentAssignmentRecord(groupId, updatedMember));

                log.info("[GroupId {}] Member {} transitioned from {} to {}.",
                    groupId, memberId, member.currentAssignmentSummary(), updatedMember.currentAssignmentSummary());

                if (updatedMember.state() == ConsumerGroupMember.MemberState.REVOKING) {
                    scheduleConsumerGroupRevocationTimeout(
                        groupId,
                        memberId,
                        updatedMember.rebalanceTimeoutMs(),
                        updatedMember.memberEpoch()
                    );
                } else {
                    cancelConsumerGroupRevocationTimeout(groupId, memberId);
                }
            }
        }

        scheduleConsumerGroupSessionTimeout(groupId, memberId);

        // Prepare the response.
        ConsumerGroupHeartbeatResponseData response = new ConsumerGroupHeartbeatResponseData()
            .setMemberId(updatedMember.memberId())
            .setMemberEpoch(updatedMember.memberEpoch())
            .setHeartbeatIntervalMs(consumerGroupHeartbeatIntervalMs);

        // The assignment is only provided in the following cases:
        // 1. The member reported its owned partitions;
        // 2. The member just joined or rejoined to group (epoch equals to zero);
        // 3. The member's assignment has been updated.
        if (ownedTopicPartitions != null || memberEpoch == 0 || assignmentUpdated) {
            response.setAssignment(createResponseAssignment(updatedMember));
        }

        return new CoordinatorResult<>(records, response);
    }

    /**
     * Handles leave request from a consumer group member.
     * @param groupId       The group id from the request.
     * @param memberId      The member id from the request.
     *
     * @return A Result containing the ConsumerGroupHeartbeat response and
     *         a list of records to update the state machine.
     */
    private CoordinatorResult<ConsumerGroupHeartbeatResponseData, Record> consumerGroupLeave(
        String groupId,
        String memberId
    ) throws ApiException {
        ConsumerGroup group = getOrMaybeCreateConsumerGroup(groupId, false);
        ConsumerGroupMember member = group.getOrMaybeCreateMember(memberId, false);

        log.info("[GroupId " + groupId + "] Member " + memberId + " left the consumer group.");

        List<Record> records = consumerGroupFenceMember(group, member);
        return new CoordinatorResult<>(records, new ConsumerGroupHeartbeatResponseData()
            .setMemberId(memberId)
            .setMemberEpoch(LEAVE_GROUP_MEMBER_EPOCH));
    }

    /**
     * Fences a member from a consumer group.
     *
     * @param group       The group.
     * @param member      The member.
     *
     * @return A list of records to be applied to the state.
     */
    private List<Record> consumerGroupFenceMember(
        ConsumerGroup group,
        ConsumerGroupMember member
    ) {
        List<Record> records = new ArrayList<>();

        // Write tombstones for the member. The order matters here.
        records.add(newCurrentAssignmentTombstoneRecord(group.groupId(), member.memberId()));
        records.add(newTargetAssignmentTombstoneRecord(group.groupId(), member.memberId()));
        records.add(newMemberSubscriptionTombstoneRecord(group.groupId(), member.memberId()));

        // We update the subscription metadata without the leaving member.
        Map<String, TopicMetadata> subscriptionMetadata = group.computeSubscriptionMetadata(
            member,
            null,
            metadataImage.topics(),
            metadataImage.cluster()
        );

        if (!subscriptionMetadata.equals(group.subscriptionMetadata())) {
            log.info("[GroupId {}] Computed new subscription metadata: {}.",
                group.groupId(), subscriptionMetadata);
            records.add(newGroupSubscriptionMetadataRecord(group.groupId(), subscriptionMetadata));
        }

        // We bump the group epoch.
        int groupEpoch = group.groupEpoch() + 1;
        records.add(newGroupEpochRecord(group.groupId(), groupEpoch));

        // Cancel all the timers of the member.
        cancelConsumerGroupSessionTimeout(group.groupId(), member.memberId());
        cancelConsumerGroupRevocationTimeout(group.groupId(), member.memberId());

        return records;
    }

    /**
     * Schedules (or reschedules) the session timeout for the member.
     *
     * @param groupId       The group id.
     * @param memberId      The member id.
     */
    private void scheduleConsumerGroupSessionTimeout(
        String groupId,
        String memberId
    ) {
        String key = consumerGroupSessionTimeoutKey(groupId, memberId);
        timer.schedule(key, consumerGroupSessionTimeoutMs, TimeUnit.MILLISECONDS, true, () -> {
            try {
                ConsumerGroup group = getOrMaybeCreateConsumerGroup(groupId, false);
                ConsumerGroupMember member = group.getOrMaybeCreateMember(memberId, false);
                log.info("[GroupId {}] Member {} fenced from the group because its session expired.",
                    groupId, memberId);
                return new CoordinatorResult<>(consumerGroupFenceMember(group, member));
            } catch (GroupIdNotFoundException ex) {
                log.debug("[GroupId {}] Could not fence {} because the group does not exist.",
                    groupId, memberId);
            } catch (UnknownMemberIdException ex) {
                log.debug("[GroupId {}] Could not fence {} because the member does not exist.",
                    groupId, memberId);
            }

            return new CoordinatorResult<>(Collections.emptyList());
        });
    }

    /**
     * Cancels the session timeout of the member.
     *
     * @param groupId       The group id.
     * @param memberId      The member id.
     */
    private void cancelConsumerGroupSessionTimeout(
        String groupId,
        String memberId
    ) {
        timer.cancel(consumerGroupSessionTimeoutKey(groupId, memberId));
    }

    /**
     * Schedules a revocation timeout for the member.
     *
     * @param groupId               The group id.
     * @param memberId              The member id.
     * @param revocationTimeoutMs   The revocation timeout.
     * @param expectedMemberEpoch   The expected member epoch.
     */
    private void scheduleConsumerGroupRevocationTimeout(
        String groupId,
        String memberId,
        long revocationTimeoutMs,
        int expectedMemberEpoch
    ) {
        String key = consumerGroupRevocationTimeoutKey(groupId, memberId);
        timer.schedule(key, revocationTimeoutMs, TimeUnit.MILLISECONDS, true, () -> {
            try {
                ConsumerGroup group = getOrMaybeCreateConsumerGroup(groupId, false);
                ConsumerGroupMember member = group.getOrMaybeCreateMember(memberId, false);

                if (member.state() != ConsumerGroupMember.MemberState.REVOKING ||
                    member.memberEpoch() != expectedMemberEpoch) {
                    log.debug("[GroupId {}] Ignoring revocation timeout for {} because the member " +
                        "state does not match the expected state.", groupId, memberId);
                    return new CoordinatorResult<>(Collections.emptyList());
                }

                log.info("[GroupId {}] Member {} fenced from the group because " +
                    "it failed to revoke partitions within {}ms.", groupId, memberId, revocationTimeoutMs);
                return new CoordinatorResult<>(consumerGroupFenceMember(group, member));
            } catch (GroupIdNotFoundException ex) {
                log.debug("[GroupId {}] Could not fence {}} because the group does not exist.",
                    groupId, memberId);
            } catch (UnknownMemberIdException ex) {
                log.debug("[GroupId {}] Could not fence {} because the member does not exist.",
                    groupId, memberId);
            }

            return new CoordinatorResult<>(Collections.emptyList());
        });
    }

    /**
     * Cancels the revocation timeout of the member.
     *
     * @param groupId       The group id.
     * @param memberId      The member id.
     */
    private void cancelConsumerGroupRevocationTimeout(
        String groupId,
        String memberId
    ) {
        timer.cancel(consumerGroupRevocationTimeoutKey(groupId, memberId));
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
    ) throws ApiException {
        throwIfConsumerGroupHeartbeatRequestIsInvalid(request);

        if (request.memberEpoch() == LEAVE_GROUP_MEMBER_EPOCH) {
            return consumerGroupLeave(
                request.groupId(),
                request.memberId()
            );
        } else {
            // Otherwise, it is a regular heartbeat.
            return consumerGroupHeartbeat(
                request.groupId(),
                request.memberId(),
                request.memberEpoch(),
                request.instanceId(),
                request.rackId(),
                request.rebalanceTimeoutMs(),
                context.clientId(),
                context.clientAddress.toString(),
                request.subscribedTopicNames(),
                request.subscribedTopicRegex(),
                request.serverAssignor(),
                request.topicPartitions()
            );
        }
    }

    /**
     * Replays ConsumerGroupMemberMetadataKey/Value to update the hard state of
     * the consumer group. It updates the subscription part of the member or
     * delete the member.
     *
     * @param key   A ConsumerGroupMemberMetadataKey key.
     * @param value A ConsumerGroupMemberMetadataValue record.
     */
    public void replay(
        ConsumerGroupMemberMetadataKey key,
        ConsumerGroupMemberMetadataValue value
    ) {
        String groupId = key.groupId();
        String memberId = key.memberId();

        ConsumerGroup consumerGroup = getOrMaybeCreateConsumerGroup(groupId, value != null);
        Set<String> oldSubscribedTopicNames = new HashSet<>(consumerGroup.subscribedTopicNames());

        if (value != null) {
            ConsumerGroupMember oldMember = consumerGroup.getOrMaybeCreateMember(memberId, true);
            consumerGroup.updateMember(new ConsumerGroupMember.Builder(oldMember)
                .updateWith(value)
                .build());
        } else {
            ConsumerGroupMember oldMember = consumerGroup.getOrMaybeCreateMember(memberId, false);
            if (oldMember.memberEpoch() != LEAVE_GROUP_MEMBER_EPOCH) {
                throw new IllegalStateException("Received a tombstone record to delete member " + memberId
                    + " but did not receive ConsumerGroupCurrentMemberAssignmentValue tombstone.");
            }
            if (consumerGroup.targetAssignment().containsKey(memberId)) {
                throw new IllegalStateException("Received a tombstone record to delete member " + memberId
                    + " but did not receive ConsumerGroupTargetAssignmentMetadataValue tombstone.");
            }
            consumerGroup.removeMember(memberId);
        }

        updateGroupsByTopics(groupId, oldSubscribedTopicNames, consumerGroup.subscribedTopicNames());
    }

    /**
     * @return The set of groups subscribed to the topic.
     */
    public Set<String> groupsSubscribedToTopic(String topicName) {
        Set<String> groups = groupsByTopics.get(topicName);
        return groups != null ? groups : Collections.emptySet();
    }

    /**
     * Subscribes a group to a topic.
     *
     * @param groupId   The group id.
     * @param topicName The topic name.
     */
    private void subscribeGroupToTopic(
        String groupId,
        String topicName
    ) {
        groupsByTopics
            .computeIfAbsent(topicName, __ -> new TimelineHashSet<>(snapshotRegistry, 1))
            .add(groupId);
    }

    /**
     * Unsubscribes a group from a topic.
     *
     * @param groupId   The group id.
     * @param topicName The topic name.
     */
    private void unsubscribeGroupFromTopic(
        String groupId,
        String topicName
    ) {
        groupsByTopics.computeIfPresent(topicName, (__, groupIds) -> {
            groupIds.remove(groupId);
            return groupIds.isEmpty() ? null : groupIds;
        });
    }

    /**
     * Updates the group by topics mapping.
     *
     * @param groupId               The group id.
     * @param oldSubscribedTopics   The old group subscriptions.
     * @param newSubscribedTopics   The new group subscriptions.
     */
    private void updateGroupsByTopics(
        String groupId,
        Set<String> oldSubscribedTopics,
        Set<String> newSubscribedTopics
    ) {
        if (oldSubscribedTopics.isEmpty()) {
            newSubscribedTopics.forEach(topicName ->
                subscribeGroupToTopic(groupId, topicName)
            );
        } else if (newSubscribedTopics.isEmpty()) {
            oldSubscribedTopics.forEach(topicName ->
                unsubscribeGroupFromTopic(groupId, topicName)
            );
        } else {
            oldSubscribedTopics.forEach(topicName -> {
                if (!newSubscribedTopics.contains(topicName)) {
                    unsubscribeGroupFromTopic(groupId, topicName);
                }
            });
            newSubscribedTopics.forEach(topicName -> {
                if (!oldSubscribedTopics.contains(topicName)) {
                    subscribeGroupToTopic(groupId, topicName);
                }
            });
        }
    }

    /**
     * Replays ConsumerGroupMetadataKey/Value to update the hard state of
     * the consumer group. It updates the group epoch of the consumer
     * group or deletes the consumer group.
     *
     * @param key   A ConsumerGroupMetadataKey key.
     * @param value A ConsumerGroupMetadataValue record.
     */
    public void replay(
        ConsumerGroupMetadataKey key,
        ConsumerGroupMetadataValue value
    ) {
        String groupId = key.groupId();

        if (value != null) {
            ConsumerGroup consumerGroup = getOrMaybeCreateConsumerGroup(groupId, true);
            consumerGroup.setGroupEpoch(value.epoch());
        } else {
            ConsumerGroup consumerGroup = getOrMaybeCreateConsumerGroup(groupId, false);
            if (!consumerGroup.members().isEmpty()) {
                throw new IllegalStateException("Received a tombstone record to delete group " + groupId
                    + " but the group still has " + consumerGroup.members().size() + " members.");
            }
            if (!consumerGroup.targetAssignment().isEmpty()) {
                throw new IllegalStateException("Received a tombstone record to delete group " + groupId
                    + " but the target assignment still has " + consumerGroup.targetAssignment().size()
                    + " members.");
            }
            if (consumerGroup.assignmentEpoch() != -1) {
                throw new IllegalStateException("Received a tombstone record to delete group " + groupId
                    + " but did not receive ConsumerGroupTargetAssignmentMetadataValue tombstone.");
            }
            removeGroup(groupId);
        }

    }

    /**
     * Replays ConsumerGroupPartitionMetadataKey/Value to update the hard state of
     * the consumer group. It updates the subscription metadata of the consumer
     * group.
     *
     * @param key   A ConsumerGroupPartitionMetadataKey key.
     * @param value A ConsumerGroupPartitionMetadataValue record.
     */
    public void replay(
        ConsumerGroupPartitionMetadataKey key,
        ConsumerGroupPartitionMetadataValue value
    ) {
        String groupId = key.groupId();
        ConsumerGroup consumerGroup = getOrMaybeCreateConsumerGroup(groupId, false);

        if (value != null) {
            Map<String, TopicMetadata> subscriptionMetadata = new HashMap<>();
            value.topics().forEach(topicMetadata -> {
                subscriptionMetadata.put(topicMetadata.topicName(), TopicMetadata.fromRecord(topicMetadata));
            });
            consumerGroup.setSubscriptionMetadata(subscriptionMetadata);
        } else {
            consumerGroup.setSubscriptionMetadata(Collections.emptyMap());
        }
    }

    /**
     * Replays ConsumerGroupTargetAssignmentMemberKey/Value to update the hard state of
     * the consumer group. It updates the target assignment of the member or deletes it.
     *
     * @param key   A ConsumerGroupTargetAssignmentMemberKey key.
     * @param value A ConsumerGroupTargetAssignmentMemberValue record.
     */
    public void replay(
        ConsumerGroupTargetAssignmentMemberKey key,
        ConsumerGroupTargetAssignmentMemberValue value
    ) {
        String groupId = key.groupId();
        String memberId = key.memberId();
        ConsumerGroup consumerGroup = getOrMaybeCreateConsumerGroup(groupId, false);

        if (value != null) {
            consumerGroup.updateTargetAssignment(memberId, Assignment.fromRecord(value));
        } else {
            consumerGroup.removeTargetAssignment(memberId);
        }
    }

    /**
     * Replays ConsumerGroupTargetAssignmentMetadataKey/Value to update the hard state of
     * the consumer group. It updates the target assignment epoch or set it to -1 to signal
     * that it has been deleted.
     *
     * @param key   A ConsumerGroupTargetAssignmentMetadataKey key.
     * @param value A ConsumerGroupTargetAssignmentMetadataValue record.
     */
    public void replay(
        ConsumerGroupTargetAssignmentMetadataKey key,
        ConsumerGroupTargetAssignmentMetadataValue value
    ) {
        String groupId = key.groupId();
        ConsumerGroup consumerGroup = getOrMaybeCreateConsumerGroup(groupId, false);

        if (value != null) {
            consumerGroup.setTargetAssignmentEpoch(value.assignmentEpoch());
        } else {
            if (!consumerGroup.targetAssignment().isEmpty()) {
                throw new IllegalStateException("Received a tombstone record to delete target assignment of " + groupId
                    + " but the assignment still has " + consumerGroup.targetAssignment().size() + " members.");
            }
            consumerGroup.setTargetAssignmentEpoch(-1);
        }
    }

    /**
     * Replays ConsumerGroupCurrentMemberAssignmentKey/Value to update the hard state of
     * the consumer group. It updates the assignment of a member or deletes it.
     *
     * @param key   A ConsumerGroupCurrentMemberAssignmentKey key.
     * @param value A ConsumerGroupCurrentMemberAssignmentValue record.
     */
    public void replay(
        ConsumerGroupCurrentMemberAssignmentKey key,
        ConsumerGroupCurrentMemberAssignmentValue value
    ) {
        String groupId = key.groupId();
        String memberId = key.memberId();
        ConsumerGroup consumerGroup = getOrMaybeCreateConsumerGroup(groupId, false);
        ConsumerGroupMember oldMember = consumerGroup.getOrMaybeCreateMember(memberId, false);

        if (value != null) {
            ConsumerGroupMember newMember = new ConsumerGroupMember.Builder(oldMember)
                .updateWith(value)
                .build();
            consumerGroup.updateMember(newMember);
        } else {
            ConsumerGroupMember newMember = new ConsumerGroupMember.Builder(oldMember)
                .setMemberEpoch(LEAVE_GROUP_MEMBER_EPOCH)
                .setPreviousMemberEpoch(LEAVE_GROUP_MEMBER_EPOCH)
                .setTargetMemberEpoch(LEAVE_GROUP_MEMBER_EPOCH)
                .setAssignedPartitions(Collections.emptyMap())
                .setPartitionsPendingRevocation(Collections.emptyMap())
                .setPartitionsPendingAssignment(Collections.emptyMap())
                .build();
            consumerGroup.updateMember(newMember);
        }
    }

    /**
     * A new metadata image is available.
     *
     * @param newImage  The new metadata image.
     * @param delta     The delta image.
     */
    public void onNewMetadataImage(MetadataImage newImage, MetadataDelta delta) {
        metadataImage = newImage;

        // Notify all the groups subscribed to the created, updated or
        // deleted topics.
        Optional.ofNullable(delta.topicsDelta()).ifPresent(topicsDelta -> {
            Set<String> allGroupIds = new HashSet<>();
            topicsDelta.changedTopics().forEach((topicId, topicDelta) -> {
                String topicName = topicDelta.name();
                allGroupIds.addAll(groupsSubscribedToTopic(topicName));
            });
            topicsDelta.deletedTopicIds().forEach(topicId -> {
                TopicImage topicImage = delta.image().topics().getTopic(topicId);
                allGroupIds.addAll(groupsSubscribedToTopic(topicImage.name()));
            });
            allGroupIds.forEach(groupId -> {
                Group group = groups.get(groupId);
                if (group != null && group.type() == Group.GroupType.CONSUMER) {
                    ((ConsumerGroup) group).requestMetadataRefresh();
                }
            });
        });
    }

    /**
     * The coordinator has been loaded. Session timeouts are registered
     * for all members.
     */
    public void onLoaded() {
        groups.forEach((groupId, group) -> {
            switch (group.type()) {
                case CONSUMER:
                    ConsumerGroup consumerGroup = (ConsumerGroup) group;
                    log.info("Loaded consumer group {} with {} members.", groupId, consumerGroup.members().size());
                    consumerGroup.members().forEach((memberId, member) -> {
                        log.debug("Loaded member {} in consumer group {}.", memberId, groupId);
                        scheduleConsumerGroupSessionTimeout(groupId, memberId);
                        if (member.state() == ConsumerGroupMember.MemberState.REVOKING) {
                            scheduleConsumerGroupRevocationTimeout(
                                groupId,
                                memberId,
                                member.rebalanceTimeoutMs(),
                                member.memberEpoch()
                            );
                        }
                    });
                    break;

                case GENERIC:
                    GenericGroup genericGroup = (GenericGroup) group;
                    log.info("Loaded generic group {} with {} members.", groupId, genericGroup.allMembers().size());
                    genericGroup.allMembers().forEach(member -> {
                        log.debug("Loaded member {} in generic group {}.", member.memberId(), groupId);
                        rescheduleGenericGroupMemberHeartbeat(genericGroup, member);
                    });

                    if (genericGroup.size() > genericGroupMaxSize) {
                        // In case the max size config has changed.
                        prepareRebalance(genericGroup, "Freshly-loaded group " + groupId +
                            " (size " + genericGroup.size() + ") is over capacity " + genericGroupMaxSize +
                            ". Rebalancing in order to give a chance for consumers to commit offsets");
                    }
                    break;
            }
        });
    }

    public static String consumerGroupSessionTimeoutKey(String groupId, String memberId) {
        return "session-timeout-" + groupId + "-" + memberId;
    }

    public static String consumerGroupRevocationTimeoutKey(String groupId, String memberId) {
        return "revocation-timeout-" + groupId + "-" + memberId;
    }

    /**
     * Replays GroupMetadataKey/Value to update the soft state of
     * the generic group.
     *
     * @param key   A GroupMetadataKey key.
     * @param value A GroupMetadataValue record.
     */
    public void replay(
        GroupMetadataKey key,
        GroupMetadataValue value
    ) {
        String groupId = key.group();

        if (value == null)  {
            // Tombstone. Group should be removed.
            removeGroup(groupId);
        } else {
            List<GenericGroupMember> loadedMembers = new ArrayList<>();
            for (GroupMetadataValue.MemberMetadata member : value.members()) {
                int rebalanceTimeout = member.rebalanceTimeout() == -1 ?
                    member.sessionTimeout() : member.rebalanceTimeout();

                JoinGroupRequestProtocolCollection supportedProtocols = new JoinGroupRequestProtocolCollection();
                supportedProtocols.add(new JoinGroupRequestProtocol()
                    .setName(value.protocol())
                    .setMetadata(member.subscription()));

                GenericGroupMember loadedMember = new GenericGroupMember(
                    member.memberId(),
                    Optional.ofNullable(member.groupInstanceId()),
                    member.clientId(),
                    member.clientHost(),
                    rebalanceTimeout,
                    member.sessionTimeout(),
                    value.protocolType(),
                    supportedProtocols,
                    member.assignment()
                );

                loadedMembers.add(loadedMember);
            }

            String protocolType = value.protocolType();

            GenericGroup genericGroup = new GenericGroup(
                this.logContext,
                groupId,
                loadedMembers.isEmpty() ? EMPTY : STABLE,
                time,
                value.generation(),
                protocolType == null || protocolType.isEmpty() ? Optional.empty() : Optional.of(protocolType),
                Optional.ofNullable(value.protocol()),
                Optional.ofNullable(value.leader()),
                value.currentStateTimestamp() == -1 ? Optional.empty() : Optional.of(value.currentStateTimestamp())
            );

            loadedMembers.forEach(member -> genericGroup.add(member, null));
            groups.put(groupId, genericGroup);

            genericGroup.setSubscribedTopics(
                genericGroup.computeSubscribedTopics()
            );
        }
    }

    /**
     * Handle a JoinGroupRequest.
     *
     * @param context        The request context.
     * @param request        The actual JoinGroup request.
     * @param responseFuture The join group response future.
     *
     * @return The result that contains records to append if the join group phase completes.
     */
    public CoordinatorResult<Void, Record> genericGroupJoin(
        RequestContext context,
        JoinGroupRequestData request,
        CompletableFuture<JoinGroupResponseData> responseFuture
    ) {
        CoordinatorResult<Void, Record> result = EMPTY_RESULT;

        String groupId = request.groupId();
        String memberId = request.memberId();
        int sessionTimeoutMs = request.sessionTimeoutMs();

        if (sessionTimeoutMs < genericGroupMinSessionTimeoutMs ||
            sessionTimeoutMs > genericGroupMaxSessionTimeoutMs
        ) {
            responseFuture.complete(new JoinGroupResponseData()
                .setMemberId(memberId)
                .setErrorCode(Errors.INVALID_SESSION_TIMEOUT.code())
            );
        } else {
            boolean isUnknownMember = memberId.equals(UNKNOWN_MEMBER_ID);
            // Group is created if it does not exist and the member id is UNKNOWN. if member
            // is specified but group does not exist, request is rejected with GROUP_ID_NOT_FOUND
            GenericGroup group;
            boolean isNewGroup = !groups.containsKey(groupId);
            try {
                group = getOrMaybeCreateGenericGroup(groupId, isUnknownMember);
            } catch (Throwable t) {
                responseFuture.complete(new JoinGroupResponseData()
                    .setMemberId(memberId)
                    .setErrorCode(Errors.forException(t).code())
                );
                return EMPTY_RESULT;
            }

            if (!acceptJoiningMember(group, memberId)) {
                group.remove(memberId);
                responseFuture.complete(new JoinGroupResponseData()
                    .setMemberId(UNKNOWN_MEMBER_ID)
                    .setErrorCode(Errors.GROUP_MAX_SIZE_REACHED.code())
                );
            } else if (isUnknownMember) {
                result = genericGroupJoinNewMember(
                    context,
                    request,
                    group,
                    responseFuture
                );
            } else {
                result = genericGroupJoinExistingMember(
                    context,
                    request,
                    group,
                    responseFuture
                );
            }

            if (isNewGroup && result == EMPTY_RESULT) {
                // If there are no records to append and if a group was newly created, we need to append
                // records to the log to commit the group to the timeline data structure.
                CompletableFuture<Void> appendFuture = new CompletableFuture<>();
                appendFuture.whenComplete((__, t) -> {
                    if (t != null) {
                        // We failed to write the empty group metadata. This will revert the snapshot, removing
                        // the newly created group.
                        log.warn("Failed to write empty metadata for group {}: {}", group.groupId(), t.getMessage());

                        responseFuture.complete(new JoinGroupResponseData()
                            .setErrorCode(appendGroupMetadataErrorToResponseError(Errors.forException(t)).code()));
                    }
                });

                List<Record> records = Collections.singletonList(
                    RecordHelpers.newEmptyGroupMetadataRecord(group, metadataImage.features().metadataVersion())
                );

                return new CoordinatorResult<>(records, appendFuture);
            }
        }
        return result;
    }

    /**
     * Attempt to complete join group phase. We do not complete
     * the join group phase if this is the initial rebalance.
     *
     * @param group The group.
     *
     * @return The coordinator result that will be appended to the log.
     */
    private CoordinatorResult<Void, Record> maybeCompleteJoinPhase(GenericGroup group) {
        if (group.isInState(PREPARING_REBALANCE) &&
            group.hasAllMembersJoined() &&
            group.previousState() != EMPTY
        ) {
            return completeGenericGroupJoin(group);
        }

        return EMPTY_RESULT;
    }

    /**
     * Handle a new member generic group join.
     *
     * @param context         The request context.
     * @param request         The join group request.
     * @param group           The group to add the member.
     * @param responseFuture  The response future to complete.
     *
     * @return The coordinator result that will be appended to the log.
     */
    private CoordinatorResult<Void, Record> genericGroupJoinNewMember(
        RequestContext context,
        JoinGroupRequestData request,
        GenericGroup group,
        CompletableFuture<JoinGroupResponseData> responseFuture
    ) {
        if (group.isInState(DEAD)) {
            // If the group is marked as dead, it means some other thread has just removed the group
            // from the coordinator metadata; it is likely that the group has migrated to some other
            // coordinator OR the group is in a transient unstable phase. Let the member retry
            // finding the correct coordinator and rejoin.
            responseFuture.complete(new JoinGroupResponseData()
                .setMemberId(UNKNOWN_MEMBER_ID)
                .setErrorCode(COORDINATOR_NOT_AVAILABLE.code())
            );
        } else if (!group.supportsProtocols(request.protocolType(), request.protocols())) {
            responseFuture.complete(new JoinGroupResponseData()
                .setMemberId(UNKNOWN_MEMBER_ID)
                .setErrorCode(Errors.INCONSISTENT_GROUP_PROTOCOL.code())
            );
        } else {
            Optional<String> groupInstanceId = Optional.ofNullable(request.groupInstanceId());
            String newMemberId = group.generateMemberId(context.clientId(), groupInstanceId);

            if (groupInstanceId.isPresent()) {
                return genericGroupJoinNewStaticMember(
                    context,
                    request,
                    group,
                    newMemberId,
                    responseFuture
                );
            } else {
                return genericGroupJoinNewDynamicMember(
                    context,
                    request,
                    group,
                    newMemberId,
                    responseFuture
                );
            }
        }

        return EMPTY_RESULT;
    }

    /**
     * Handle new static member join. If there was an existing member id for the group instance id,
     * replace that member. Otherwise, add the member and rebalance.
     *
     * @param context         The request context.
     * @param request         The join group request.
     * @param group           The group to add the member.
     * @param newMemberId     The newly generated member id.
     * @param responseFuture  The response future to complete.
     *
     * @return The coordinator result that will be appended to the log.
     */
    private CoordinatorResult<Void, Record> genericGroupJoinNewStaticMember(
        RequestContext context,
        JoinGroupRequestData request,
        GenericGroup group,
        String newMemberId,
        CompletableFuture<JoinGroupResponseData> responseFuture
    ) {
        String groupInstanceId = request.groupInstanceId();
        String existingMemberId = group.staticMemberId(groupInstanceId);
        if (existingMemberId != null) {
            log.info("Static member with groupInstanceId={} and unknown member id joins " +
                    "group {} in {} state. Replacing previously mapped member {} with this groupInstanceId.",
                groupInstanceId, group.groupId(), group.currentState(), existingMemberId);

            return updateStaticMemberThenRebalanceOrCompleteJoin(
                context,
                request,
                group,
                existingMemberId,
                newMemberId,
                responseFuture
            );
        } else {
            log.info("Static member with groupInstanceId={} and unknown member id joins " +
                    "group {} in {} state. Created a new member id {} for this member and added to the group.",
                groupInstanceId, group.groupId(), group.currentState(), newMemberId);

            return addMemberThenRebalanceOrCompleteJoin(context, request, group, newMemberId, responseFuture);
        }
    }

    /**
     * Handle a new dynamic member join. If the member id field is required, the group metadata manager
     * will add the new member id to the pending members and respond with MEMBER_ID_REQUIRED along with
     * the new member id for the client to join with.
     *
     * Otherwise, add the new member to the group and rebalance.
     *
     * @param context         The request context.
     * @param request         The join group request.
     * @param group           The group to add the member.
     * @param newMemberId     The newly generated member id.
     * @param responseFuture  The response future to complete.
     *
     * @return The coordinator result that will be appended to the log.
     */
    private CoordinatorResult<Void, Record> genericGroupJoinNewDynamicMember(
        RequestContext context,
        JoinGroupRequestData request,
        GenericGroup group,
        String newMemberId,
        CompletableFuture<JoinGroupResponseData> responseFuture
    ) {
        if (JoinGroupRequest.requiresKnownMemberId(context.apiVersion())) {
            // If member id required, register the member in the pending member list and send
            // back a response to call for another join group request with allocated member id.
            log.info("Dynamic member with unknown member id joins group {} in {} state. " +
                    "Created a new member id {} and requesting the member to rejoin with this id.",
                group.groupId(), group.currentState(), newMemberId);

            group.addPendingMember(newMemberId);
            String genericGroupHeartbeatKey = genericGroupHeartbeatKey(group.groupId(), newMemberId);

            timer.schedule(
                genericGroupHeartbeatKey,
                request.sessionTimeoutMs(),
                TimeUnit.MILLISECONDS,
                false,
                () -> expireGenericGroupMemberHeartbeat(group, newMemberId)
            );

            responseFuture.complete(new JoinGroupResponseData()
                .setMemberId(newMemberId)
                .setErrorCode(Errors.MEMBER_ID_REQUIRED.code())
            );
        } else {
            log.info("Dynamic member with unknown member id joins group {} in state {}. " +
                    "Created a new member id {} and added the member to the group.",
                group.groupId(), group.currentState(), newMemberId);

            return addMemberThenRebalanceOrCompleteJoin(context, request, group, newMemberId, responseFuture);
        }

        return EMPTY_RESULT;
    }

    /**
     * Handle a join group request for an existing member.
     *
     * @param context         The request context.
     * @param request         The join group request.
     * @param group           The group to add the member.
     * @param responseFuture  The response future to complete.
     *
     * @return The coordinator result that will be appended to the log.
     */
    private CoordinatorResult<Void, Record> genericGroupJoinExistingMember(
        RequestContext context,
        JoinGroupRequestData request,
        GenericGroup group,
        CompletableFuture<JoinGroupResponseData> responseFuture
    ) {
        String memberId = request.memberId();
        String groupInstanceId = request.groupInstanceId();

        if (group.isInState(DEAD)) {
            // If the group is marked as dead, it means the group was recently removed the group
            // from the coordinator metadata; it is likely that the group has migrated to some other
            // coordinator OR the group is in a transient unstable phase. Let the member retry
            // finding the correct coordinator and rejoin.
            responseFuture.complete(new JoinGroupResponseData()
                .setMemberId(memberId)
                .setErrorCode(COORDINATOR_NOT_AVAILABLE.code())
            );
        } else if (!group.supportsProtocols(request.protocolType(), request.protocols())) {
            responseFuture.complete(new JoinGroupResponseData()
                .setMemberId(memberId)
                .setErrorCode(Errors.INCONSISTENT_GROUP_PROTOCOL.code())
            );
        } else if (group.isPendingMember(memberId)) {
            // A rejoining pending member will be accepted. Note that pending member cannot be a static member.
            if (groupInstanceId != null) {
                throw new IllegalStateException("Received unexpected JoinGroup with groupInstanceId=" +
                    groupInstanceId + " for pending member with memberId=" + memberId);
            }

            log.debug("Pending dynamic member with id {} joins group {} in {} state. Adding to the group now.",
                memberId, group.groupId(), group.currentState());

            return addMemberThenRebalanceOrCompleteJoin(
                context,
                request,
                group,
                memberId,
                responseFuture
            );
        } else {
            try {
                group.validateMember(
                    memberId,
                    groupInstanceId,
                    "join-group"
                );
            } catch (KafkaException ex) {
                responseFuture.complete(new JoinGroupResponseData()
                    .setMemberId(memberId)
                    .setErrorCode(Errors.forException(ex).code())
                    .setProtocolType(null)
                    .setProtocolName(null)
                );
                return EMPTY_RESULT;
            }

            GenericGroupMember member = group.member(memberId);
            if (group.isInState(PREPARING_REBALANCE)) {
                return updateMemberThenRebalanceOrCompleteJoin(
                    request,
                    group,
                    member,
                    "Member " + member.memberId() + " is joining group during " + group.stateAsString() +
                        "; client reason: " + JoinGroupRequest.joinReason(request),
                    responseFuture
                );
            } else if (group.isInState(COMPLETING_REBALANCE)) {
                if (member.matches(request.protocols())) {
                    // Member is joining with the same metadata (which could be because it failed to
                    // receive the initial JoinGroup response), so just return current group information
                    // for the current generation.
                    responseFuture.complete(new JoinGroupResponseData()
                        .setMembers(group.isLeader(memberId) ?
                            group.currentGenericGroupMembers() : Collections.emptyList())
                        .setMemberId(memberId)
                        .setGenerationId(group.generationId())
                        .setProtocolName(group.protocolName().orElse(null))
                        .setProtocolType(group.protocolType().orElse(null))
                        .setLeader(group.leaderOrNull())
                        .setSkipAssignment(false)
                    );
                } else {
                    // Member has changed metadata, so force a rebalance
                    return updateMemberThenRebalanceOrCompleteJoin(
                        request,
                        group,
                        member,
                        "Updating metadata for member " + memberId + " during " + group.stateAsString() +
                            "; client reason: " + JoinGroupRequest.joinReason(request),
                        responseFuture
                    );
                }
            } else if (group.isInState(STABLE)) {
                if (group.isLeader(memberId)) {
                    // Force a rebalance if the leader sends JoinGroup;
                    // This allows the leader to trigger rebalances for changes affecting assignment
                    // which do not affect the member metadata (such as topic metadata changes for the consumer)
                    return updateMemberThenRebalanceOrCompleteJoin(
                        request,
                        group,
                        member,
                        "Leader " + memberId + " re-joining group during " + group.stateAsString() +
                            "; client reason: " + JoinGroupRequest.joinReason(request),
                        responseFuture
                    );
                } else if (!member.matches(request.protocols())) {
                    return updateMemberThenRebalanceOrCompleteJoin(
                        request,
                        group,
                        member,
                        "Updating metadata for member " + memberId + " during " + group.stateAsString() +
                            "; client reason: " + JoinGroupRequest.joinReason(request),
                        responseFuture
                    );
                } else {
                    // For followers with no actual change to their metadata, just return group information
                    // for the current generation which will allow them to issue SyncGroup.
                    responseFuture.complete(new JoinGroupResponseData()
                        .setMembers(Collections.emptyList())
                        .setMemberId(memberId)
                        .setGenerationId(group.generationId())
                        .setProtocolName(group.protocolName().orElse(null))
                        .setProtocolType(group.protocolType().orElse(null))
                        .setLeader(group.leaderOrNull())
                        .setSkipAssignment(false)
                    );
                }
            } else {
                // Group reached unexpected (Empty) state. Let the joining member reset their generation and rejoin.
                log.warn("Attempt to add rejoining member {} of group {} in unexpected group state {}",
                    memberId, group.groupId(), group.stateAsString());

                responseFuture.complete(new JoinGroupResponseData()
                    .setMemberId(memberId)
                    .setErrorCode(Errors.UNKNOWN_MEMBER_ID.code())
                );
            }
        }

        return EMPTY_RESULT;
    }

    /**
     * Complete the join group phase. Remove all dynamic members that have not rejoined
     * during this stage and proceed with the next generation for this group. The generation id
     * is incremented and the group transitions to CompletingRebalance state if there is at least
     * one member.
     *
     * If the group is in Empty state, append a new group metadata record to the log. Otherwise,
     * complete all members' join group response futures and wait for sync requests from members.
     *
     * @param group The group that is completing the join group phase.
     *
     * @return The coordinator result that will be appended to the log.
     */
    private CoordinatorResult<Void, Record> completeGenericGroupJoin(
        GenericGroup group
    ) {
        timer.cancel(genericGroupJoinKey(group.groupId()));
        String groupId = group.groupId();

        Map<String, GenericGroupMember> notYetRejoinedDynamicMembers =
            group.notYetRejoinedMembers().entrySet().stream()
                .filter(entry -> !entry.getValue().isStaticMember())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        if (!notYetRejoinedDynamicMembers.isEmpty()) {
            notYetRejoinedDynamicMembers.values().forEach(failedMember -> {
                group.remove(failedMember.memberId());
                timer.cancel(genericGroupHeartbeatKey(group.groupId(), failedMember.memberId()));
            });

            log.info("Group {} removed dynamic members who haven't joined: {}",
                groupId, notYetRejoinedDynamicMembers.keySet());
        }

        if (group.isInState(DEAD)) {
            log.info("Group {} is dead, skipping rebalance stage.", groupId);
        } else if (!group.maybeElectNewJoinedLeader() && !group.allMembers().isEmpty()) {
            // If all members are not rejoining, we will postpone the completion
            // of rebalance preparing stage, and send out another delayed operation
            // until session timeout removes all the non-responsive members.
            log.error("Group {} could not complete rebalance because no members rejoined.", groupId);

            timer.schedule(
                genericGroupJoinKey(groupId),
                group.rebalanceTimeoutMs(),
                TimeUnit.MILLISECONDS,
                false,
                () -> completeGenericGroupJoin(group)
            );

            return EMPTY_RESULT;
        } else {
            group.initNextGeneration();
            if (group.isInState(EMPTY)) {
                log.info("Group {} with generation {} is now empty.", groupId, group.generationId());

                CompletableFuture<Void> appendFuture = new CompletableFuture<>();
                appendFuture.whenComplete((__, t) -> {
                    if (t != null) {
                        // We failed to write the empty group metadata. If the broker fails before another rebalance,
                        // the previous generation written to the log will become active again (and most likely timeout).
                        // This should be safe since there are no active members in an empty generation, so we just warn.
                        log.warn("Failed to write empty metadata for group {}: {}", group.groupId(), t.getMessage());
                    }
                });

                List<Record> records = Collections.singletonList(RecordHelpers.newGroupMetadataRecord(
                    group, Collections.emptyMap(), metadataImage.features().metadataVersion()));

                return new CoordinatorResult<>(records, appendFuture);

            } else {
                log.info("Stabilized group {} generation {} with {} members.",
                    groupId, group.generationId(), group.size());

                // Complete the awaiting join group response future for all the members after rebalancing
                group.allMembers().forEach(member -> {
                    List<JoinGroupResponseData.JoinGroupResponseMember> members = Collections.emptyList();
                    if (group.isLeader(member.memberId())) {
                        members = group.currentGenericGroupMembers();
                    }

                    JoinGroupResponseData response = new JoinGroupResponseData()
                        .setMembers(members)
                        .setMemberId(member.memberId())
                        .setGenerationId(group.generationId())
                        .setProtocolName(group.protocolName().orElse(null))
                        .setProtocolType(group.protocolType().orElse(null))
                        .setLeader(group.leaderOrNull())
                        .setSkipAssignment(false)
                        .setErrorCode(Errors.NONE.code());

                    group.completeJoinFuture(member, response);
                    rescheduleGenericGroupMemberHeartbeat(group, member);
                    member.setIsNew(false);

                    group.addPendingSyncMember(member.memberId());
                });

                schedulePendingSync(group);
            }
        }

        return EMPTY_RESULT;
    }

    /**
     * Wait for sync requests for the group.
     *
     * @param group The group.
     */
    private void schedulePendingSync(GenericGroup group) {
        timer.schedule(genericGroupSyncKey(group.groupId()),
            group.rebalanceTimeoutMs(),
            TimeUnit.MILLISECONDS,
            false,
            () -> expirePendingSync(group, group.generationId()));
    }

    /**
     * Invoked when the heartbeat operation is expired from the timer. Possibly remove the member and
     * try complete the join phase.
     *
     * @param group     The group.
     * @param memberId  The member id.
     *
     * @return The coordinator result that will be appended to the log.
     */
    private CoordinatorResult<Void, Record> expireGenericGroupMemberHeartbeat(
        GenericGroup group,
        String memberId
    ) {
        if (group.isInState(DEAD)) {
            log.info("Received notification of heartbeat expiration for member {} after group {} " +
                    "had already been unloaded or deleted.",
                memberId, group.groupId());
        } else if (group.isPendingMember(memberId)) {
            log.info("Pending member {} in group {} has been removed after session timeout expiration.",
                memberId, group.groupId());

            return removePendingMemberAndUpdateGenericGroup(group, memberId);
        } else if (!group.hasMemberId(memberId)) {
            log.debug("Member {} has already been removed from the group.", memberId);
        } else {
            GenericGroupMember member = group.member(memberId);
            if (!member.hasSatisfiedHeartbeat()) {
                log.info("Member {} in group {} has failed, removing it from the group.",
                    member.memberId(), group.groupId());

                return removeMemberAndUpdateGenericGroup(
                    group,
                    member,
                    "removing member " + member.memberId() + " on heartbeat expiration."
                );
            }
        }
        return EMPTY_RESULT;
    }

    /**
     * Invoked when the heartbeat key is expired from the timer. Possibly remove the member
     * from the group and try to complete the join phase.
     *
     * @param group     The group.
     * @param member    The member.
     * @param reason    The reason for removing the member.
     *
     * @return The coordinator result that will be appended to the log.
     */
    private CoordinatorResult<Void, Record> removeMemberAndUpdateGenericGroup(
        GenericGroup group,
        GenericGroupMember member,
        String reason
    ) {
        // New members may timeout with a pending JoinGroup while the group is still rebalancing, so we have
        // to invoke the response future before removing the member. We return UNKNOWN_MEMBER_ID so
        // that the consumer will retry the JoinGroup request if it is still active.
        group.completeJoinFuture(member, new JoinGroupResponseData()
            .setMemberId(UNKNOWN_MEMBER_ID)
            .setErrorCode(Errors.UNKNOWN_MEMBER_ID.code())
        );
        group.remove(member.memberId());

        if (group.isInState(STABLE) || group.isInState(COMPLETING_REBALANCE)) {
            return maybePrepareRebalanceOrCompleteJoin(group, reason);
        } else if (group.isInState(PREPARING_REBALANCE) && group.hasAllMembersJoined()) {
            return completeGenericGroupJoin(group);
        }

        return EMPTY_RESULT;
    }

    /**
     * Remove a pending member from the group and possibly complete the join phase.
     *
     * @param group     The group.
     * @param memberId  The member id.
     *
     * @return The coordinator result that will be appended to the log.
     */
    private CoordinatorResult<Void, Record> removePendingMemberAndUpdateGenericGroup(
        GenericGroup group,
        String memberId
    ) {
        group.remove(memberId);

        if (group.isInState(PREPARING_REBALANCE) && group.hasAllMembersJoined()) {
            return completeGenericGroupJoin(group);
        }

        return EMPTY_RESULT;
    }

    /**
     * Update an existing member. Then begin a rebalance or complete the join phase.
     *
     * @param request         The join group request.
     * @param group           The group to add the member.
     * @param member          The member.
     * @param joinReason      The client reason for the join request.
     * @param responseFuture  The response future to complete.
     *
     * @return The coordinator result that will be appended to the log.
     */
    private CoordinatorResult<Void, Record> updateMemberThenRebalanceOrCompleteJoin(
        JoinGroupRequestData request,
        GenericGroup group,
        GenericGroupMember member,
        String joinReason,
        CompletableFuture<JoinGroupResponseData> responseFuture
    ) {
        group.updateMember(
            member,
            request.protocols(),
            request.rebalanceTimeoutMs(),
            request.sessionTimeoutMs(),
            responseFuture
        );

        return maybePrepareRebalanceOrCompleteJoin(group, joinReason);
    }

    /**
     * Add a member then rebalance or complete join.
     *
     * @param context         The request context.
     * @param request         The join group request.
     * @param group           The group to add the member.
     * @param memberId        The member id.
     * @param responseFuture  The response future to complete.
     *
     * @return The coordinator result that will be appended to the log.
     */
    private CoordinatorResult<Void, Record> addMemberThenRebalanceOrCompleteJoin(
        RequestContext context,
        JoinGroupRequestData request,
        GenericGroup group,
        String memberId,
        CompletableFuture<JoinGroupResponseData> responseFuture
    ) {
        Optional<String> groupInstanceId = Optional.ofNullable(request.groupInstanceId());
        GenericGroupMember member = new GenericGroupMember(
            memberId,
            groupInstanceId,
            context.clientId(),
            context.clientAddress().toString(),
            request.rebalanceTimeoutMs(),
            request.sessionTimeoutMs(),
            request.protocolType(),
            request.protocols()
        );

        member.setIsNew(true);

        // Update the newMemberAdded flag to indicate that the initial rebalance can be further delayed
        if (group.isInState(PREPARING_REBALANCE) && group.previousState() == EMPTY) {
            group.setNewMemberAdded(true);
        }
        
        group.add(member, responseFuture);

        // The session timeout does not affect new members since they do not have their memberId and
        // cannot send heartbeats. Furthermore, we cannot detect disconnects because sockets are muted
        // while the JoinGroup request is parked. If the client does disconnect (e.g. because of a request
        // timeout during a long rebalance), they may simply retry which will lead to a lot of defunct
        // members in the rebalance. To prevent this going on indefinitely, we time out JoinGroup requests
        // for new members. If the new member is still there, we expect it to retry.
        rescheduleGenericGroupMemberHeartbeat(group, member, genericGroupNewMemberJoinTimeoutMs);

        return maybePrepareRebalanceOrCompleteJoin(group, "Adding new member " + memberId + " with group instance id " +
            request.groupInstanceId() + "; client reason: " + JoinGroupRequest.joinReason(request));
    }

    /**
     * Prepare a rebalance if the group is in a valid state. Otherwise, try
     * to complete the join phase.
     *
     * @param group           The group to rebalance.
     * @param reason          The reason for the rebalance.
     *
     * @return The coordinator result that will be appended to the log.
     */
    private CoordinatorResult<Void, Record> maybePrepareRebalanceOrCompleteJoin(
        GenericGroup group,
        String reason
    ) {
        if (group.canRebalance()) {
            return prepareRebalance(group, reason);
        } else {
            return maybeCompleteJoinPhase(group);
        }
    }

    /**
     * Prepare a rebalance.
     *
     * @param group           The group to rebalance.
     * @param reason          The reason for the rebalance.
     *
     * @return The coordinator result that will be appended to the log.
     *
     * Package private for testing.
     */
    CoordinatorResult<Void, Record> prepareRebalance(
        GenericGroup group,
        String reason
    ) {
        // If any members are awaiting sync, cancel their request and have them rejoin.
        if (group.isInState(COMPLETING_REBALANCE)) {
            resetAndPropagateAssignmentWithError(group, Errors.REBALANCE_IN_PROGRESS);
        }

        // If a sync expiration is pending, cancel it.
        removeSyncExpiration(group);

        boolean isInitialRebalance = group.isInState(EMPTY);
        if (isInitialRebalance) {
            // The group is new. Provide more time for the members to join.
            int delayMs = genericGroupInitialRebalanceDelayMs;
            int remainingMs = Math.max(group.rebalanceTimeoutMs() - genericGroupInitialRebalanceDelayMs, 0);

            timer.schedule(
                genericGroupJoinKey(group.groupId()),
                delayMs,
                TimeUnit.MILLISECONDS,
                false,
                () -> tryCompleteInitialRebalanceElseSchedule(group, delayMs, remainingMs)
            );
        }

        group.transitionTo(PREPARING_REBALANCE);

        log.info("Preparing to rebalance group {} in state {} with old generation {} (reason: {}).",
            group.groupId(), group.currentState(), group.generationId(), reason);

        return isInitialRebalance ? EMPTY_RESULT : maybeCompleteJoinElseSchedule(group);
    }

    /**
     * Try to complete the join phase. Otherwise, schedule a new join operation.
     *
     * @param group The group.
     *
     * @return The coordinator result that will be appended to the log.
     */
    private CoordinatorResult<Void, Record> maybeCompleteJoinElseSchedule(
        GenericGroup group
    ) {
        String genericGroupJoinKey = genericGroupJoinKey(group.groupId());
        if (group.hasAllMembersJoined()) {
            // All members have joined. Proceed to sync phase.
            return completeGenericGroupJoin(group);
        } else {
            timer.schedule(
                genericGroupJoinKey,
                group.rebalanceTimeoutMs(),
                TimeUnit.MILLISECONDS,
                false,
                () -> completeGenericGroupJoin(group)
            );
            return EMPTY_RESULT;
        }
    }

    /**
     * Try to complete the join phase of the initial rebalance.
     * Otherwise, extend the rebalance.
     *
     * @param group The group under initial rebalance.
     *
     * @return The coordinator result that will be appended to the log.
     */
    private CoordinatorResult<Void, Record> tryCompleteInitialRebalanceElseSchedule(
        GenericGroup group,
        int delayMs,
        int remainingMs
    ) {
        if (group.newMemberAdded() && remainingMs != 0) {
            // A new member was added. Extend the delay.
            group.setNewMemberAdded(false);
            int newDelayMs = Math.min(genericGroupInitialRebalanceDelayMs, remainingMs);
            int newRemainingMs = Math.max(remainingMs - delayMs, 0);

            timer.schedule(
                genericGroupJoinKey(group.groupId()),
                newDelayMs,
                TimeUnit.MILLISECONDS,
                false,
                () -> tryCompleteInitialRebalanceElseSchedule(group, newDelayMs, newRemainingMs)
            );
        } else {
            // No more time remaining. Complete the join phase.
            return completeGenericGroupJoin(group);
        }

        return EMPTY_RESULT;
    }

    /**
     * Reset assignment for all members and propagate the error to all members in the group.
     * 
     * @param group  The group.
     * @param error  The error to propagate.
     */
    private void resetAndPropagateAssignmentWithError(GenericGroup group, Errors error) {
        if (!group.isInState(COMPLETING_REBALANCE)) {
            throw new IllegalStateException("Group " + group.groupId() + " must be in " + COMPLETING_REBALANCE.name() +
                " state but is in " + group.currentState() + ".");
        }

        group.allMembers().forEach(member -> member.setAssignment(EMPTY_ASSIGNMENT));
        propagateAssignment(group, error);
    }

    /**
     * Sets assignment for group and propagate assignment and error to all members.
     *
     * @param group      The group.
     * @param assignment The assignment for all members.
     */
    private void setAndPropagateAssignment(GenericGroup group, Map<String, byte[]> assignment) {
        if (!group.isInState(COMPLETING_REBALANCE)) {
            throw new IllegalStateException("The group must be in CompletingRebalance state " +
                "to set and propagate assignment.");
        }

        group.allMembers().forEach(member ->
            member.setAssignment(assignment.getOrDefault(member.memberId(), EMPTY_ASSIGNMENT)));

        propagateAssignment(group, Errors.NONE);
    }

    /**
     * Propagate assignment and error to all members.
     *
     * @param group  The group.
     * @param error  The error to propagate.
     */
    private void propagateAssignment(GenericGroup group, Errors error) {
        Optional<String> protocolName = Optional.empty();
        Optional<String> protocolType = Optional.empty();
        if (error == Errors.NONE) {
            protocolName = group.protocolName();
            protocolType = group.protocolType();
        }

        for (GenericGroupMember member : group.allMembers()) {
            if (!member.hasAssignment() && error == Errors.NONE) {
                log.warn("Sending empty assignment to member {} of {} for " + "generation {} with no errors",
                    member.memberId(), group.groupId(), group.generationId());
            }

            if (group.completeSyncFuture(member,
                new SyncGroupResponseData()
                    .setProtocolName(protocolName.orElse(null))
                    .setProtocolType(protocolType.orElse(null))
                    .setAssignment(member.assignment())
                    .setErrorCode(error.code()))) {

                // Reset the session timeout for members after propagating the member's assignment.
                // This is because if any member's session expired while we were still awaiting either
                // the leader sync group or the append future, its expiration will be ignored and no
                // future heartbeat expectations will not be scheduled.
                rescheduleGenericGroupMemberHeartbeat(group, member);
            }
        }
    }

    /**
     * Complete and schedule next heartbeat.
     *
     * @param group    The group.
     * @param member   The member.
     */
    public void rescheduleGenericGroupMemberHeartbeat(
        GenericGroup group,
        GenericGroupMember member
    ) {
        rescheduleGenericGroupMemberHeartbeat(group, member, member.sessionTimeoutMs());
    }

    /**
     * Reschedule the heartbeat.
     *
     * @param group      The group.
     * @param member     The member.
     * @param timeoutMs  The timeout for the new heartbeat.
     */
    private void rescheduleGenericGroupMemberHeartbeat(
        GenericGroup group,
        GenericGroupMember member,
        long timeoutMs
    ) {
        String genericGroupHeartbeatKey = genericGroupHeartbeatKey(group.groupId(), member.memberId());

        // Reschedule the next heartbeat expiration deadline
        timer.schedule(genericGroupHeartbeatKey,
            timeoutMs,
            TimeUnit.MILLISECONDS,
            false,
            () -> expireGenericGroupMemberHeartbeat(group, member.memberId()));
    }

    /**
     * Remove the sync key from the timer and clear all pending sync members from the group.
     * Invoked when a new rebalance is triggered.
     *
     * @param group  The group.
     */
    private void removeSyncExpiration(GenericGroup group) {
        group.clearPendingSyncMembers();
        timer.cancel(genericGroupSyncKey(group.groupId()));
    }

    /**
     * Expire pending sync.
     *
     * @param group           The group.
     * @param generationId    The generation when the pending sync was originally scheduled.
     *
     * @return The coordinator result that will be appended to the log.
     * */
    private CoordinatorResult<Void, Record> expirePendingSync(
        GenericGroup group,
        int generationId
    ) {
        if (generationId != group.generationId()) {
            log.error("Received unexpected notification of sync expiration for {} with an old " +
                "generation {} while the group has {}.", group.groupId(), generationId, group.generationId());
        } else {
            if (group.isInState(DEAD) || group.isInState(EMPTY) || group.isInState(PREPARING_REBALANCE)) {
                log.error("Received unexpected notification of sync expiration after group {} already " +
                    "transitioned to {} state.", group.groupId(), group.stateAsString());
            } else if (group.isInState(COMPLETING_REBALANCE) || group.isInState(STABLE)) {
                if (!group.hasReceivedSyncFromAllMembers()) {
                    Set<String> pendingSyncMembers = new HashSet<>(group.allPendingSyncMembers());
                    pendingSyncMembers.forEach(memberId -> {
                        group.remove(memberId);
                        timer.cancel(genericGroupHeartbeatKey(group.groupId(), memberId));
                    });

                    log.debug("Group {} removed members who haven't sent their sync requests: {}",
                        group.groupId(), pendingSyncMembers);

                    return prepareRebalance(group, "Removing " + pendingSyncMembers + " on pending sync request expiration");
                }
            }
        }

        return EMPTY_RESULT;
    }

    /**
     * Checks whether the group can accept a joining member.
     *
     * @param group      The group.
     * @param memberId   The member.
     *
     * @return whether the group can accept a joining member.
     */
    private boolean acceptJoiningMember(GenericGroup group, String memberId) {
        switch (group.currentState()) {
            case EMPTY:
            case DEAD:
                // Always accept the request when the group is empty or dead
                return true;
            case PREPARING_REBALANCE:
                // An existing member is accepted if it is already awaiting. New members are accepted
                // up to the max group size. Note that the number of awaiting members is used here
                // for two reasons:
                // 1) the group size is not reliable as it could already be above the max group size
                //    if the max group size was reduced.
                // 2) using the number of awaiting members allows to kick out the last rejoining
                //    members of the group.
                return (group.hasMemberId(memberId) && group.member(memberId).isAwaitingJoin()) ||
                    group.numAwaitingJoinResponse() < genericGroupMaxSize;
            case COMPLETING_REBALANCE:
            case STABLE:
                // An existing member is accepted. New members are accepted up to the max group size.
                // Note that the group size is used here. When the group transitions to CompletingRebalance,
                // members who haven't rejoined are removed.
                return group.hasMemberId(memberId) || group.size() < genericGroupMaxSize;
            default:
                throw new IllegalStateException("Unknown group state: " + group.stateAsString());
        }
    }

    /**
     * Update a static member then rebalance or complete join.
     *
     * @param context         The request context.
     * @param request         The join group request.
     * @param group           The group of the static member.
     * @param oldMemberId     The existing static member id.
     * @param newMemberId     The new joining static member id.
     * @param responseFuture  The response future to complete.
     *
     * @return The coordinator result that will be appended to the log.
     */
    private CoordinatorResult<Void, Record> updateStaticMemberThenRebalanceOrCompleteJoin(
        RequestContext context,
        JoinGroupRequestData request,
        GenericGroup group,
        String oldMemberId,
        String newMemberId,
        CompletableFuture<JoinGroupResponseData> responseFuture
    ) {
        String currentLeader = group.leaderOrNull();
        GenericGroupMember newMember = group.replaceStaticMember(request.groupInstanceId(), oldMemberId, newMemberId);

        // Heartbeat of old member id will expire without effect since the group no longer contains that member id.
        // New heartbeat shall be scheduled with new member id.
        rescheduleGenericGroupMemberHeartbeat(group, newMember);

        int oldRebalanceTimeoutMs = newMember.rebalanceTimeoutMs();
        int oldSessionTimeoutMs = newMember.sessionTimeoutMs();
        JoinGroupRequestProtocolCollection oldProtocols = newMember.supportedProtocols();

        group.updateMember(
            newMember,
            request.protocols(),
            request.rebalanceTimeoutMs(),
            request.sessionTimeoutMs(),
            responseFuture
        );

        if (group.isInState(STABLE)) {
            // Check if group's selected protocol of next generation will change, if not, simply store group to persist
            // the updated static member, if yes, rebalance should be triggered to keep the group's assignment
            // and selected protocol consistent
            String groupInstanceId = request.groupInstanceId();
            String selectedProtocolForNextGeneration = group.selectProtocol();
            if (group.protocolName().orElse("").equals(selectedProtocolForNextGeneration)) {
                log.info("Static member which joins during Stable stage and doesn't affect " +
                    "the selected protocol will not trigger a rebalance.");

                Map<String, byte[]> groupAssignment = group.groupAssignment();
                CompletableFuture<Void> appendFuture = new CompletableFuture<>();
                appendFuture.whenComplete((__, t) -> {
                    if (t != null) {
                        log.warn("Failed to persist metadata for group {} static member {} with " +
                            "group instance id {} due to {}. Reverting to old member id {}.",
                            group.groupId(), newMemberId, groupInstanceId, t.getMessage(), oldMemberId);

                        // Failed to persist the member id of the given static member, revert the update of the static member in the group.
                        group.updateMember(newMember, oldProtocols, oldRebalanceTimeoutMs, oldSessionTimeoutMs, null);
                        GenericGroupMember oldMember = group.replaceStaticMember(groupInstanceId, newMemberId, oldMemberId);
                        rescheduleGenericGroupMemberHeartbeat(group, oldMember);

                        responseFuture.complete(
                            new JoinGroupResponseData()
                                .setMembers(Collections.emptyList())
                                .setMemberId(UNKNOWN_MEMBER_ID)
                                .setGenerationId(group.generationId())
                                .setProtocolName(group.protocolName().orElse(null))
                                .setProtocolType(group.protocolType().orElse(null))
                                .setLeader(currentLeader)
                                .setSkipAssignment(false)
                                .setErrorCode(appendGroupMetadataErrorToResponseError(Errors.forException(t)).code()));

                    } else if (JoinGroupRequest.supportsSkippingAssignment(context.apiVersion())) {
                        boolean isLeader = group.isLeader(newMemberId);

                        group.completeJoinFuture(newMember, new JoinGroupResponseData()
                            .setMembers(isLeader ? group.currentGenericGroupMembers() : Collections.emptyList())
                            .setMemberId(newMemberId)
                            .setGenerationId(group.generationId())
                            .setProtocolName(group.protocolName().orElse(null))
                            .setProtocolType(group.protocolType().orElse(null))
                            .setLeader(group.leaderOrNull())
                            .setSkipAssignment(isLeader)
                        );
                    } else {
                        group.completeJoinFuture(newMember, new JoinGroupResponseData()
                            .setMembers(Collections.emptyList())
                            .setMemberId(newMemberId)
                            .setGenerationId(group.generationId())
                            .setProtocolName(group.protocolName().orElse(null))
                            .setProtocolType(group.protocolType().orElse(null))
                            .setLeader(currentLeader)
                            .setSkipAssignment(false)
                        );
                    }
                });

                List<Record> records = Collections.singletonList(
                    RecordHelpers.newGroupMetadataRecord(group, groupAssignment, metadataImage.features().metadataVersion())
                );

                return new CoordinatorResult<>(records, appendFuture);
            } else {
                return maybePrepareRebalanceOrCompleteJoin(
                    group,
                    "Group's selectedProtocol will change because static member " +
                        newMember.memberId() + " with instance id " + groupInstanceId +
                        " joined with change of protocol; client reason: " + JoinGroupRequest.joinReason(request)
                );
            }
        } else if (group.isInState(COMPLETING_REBALANCE)) {
            // if the group is in after-sync stage, upon getting a new join-group of a known static member
            // we should still trigger a new rebalance, since the old member may already be sent to the leader
            // for assignment, and hence when the assignment gets back there would be a mismatch of the old member id
            // with the new replaced member id. As a result the new member id would not get any assignment.
            return prepareRebalance(
                group,
                "Updating metadata for static member " + newMember.memberId() + " with instance id " +
                    request.groupInstanceId() + "; client reason: " + JoinGroupRequest.joinReason(request)
            );
        } else if (group.isInState(EMPTY) || group.isInState(DEAD)) {
            throw new IllegalStateException("Group " + group.groupId() + " was not supposed to be in the state " +
                group.stateAsString() + " when the unknown static member " + request.groupInstanceId() + " rejoins.");

        }
        return maybeCompleteJoinPhase(group);
    }

    /**
     * Handle a SyncGroupRequest.
     *
     * @param context        The request context.
     * @param request        The actual SyncGroup request.
     * @param responseFuture The sync group response future.
     *
     * @return The result that contains records to append if the group metadata manager received assignments.
     */
    public CoordinatorResult<Void, Record> genericGroupSync(
        RequestContext context,
        SyncGroupRequestData request,
        CompletableFuture<SyncGroupResponseData> responseFuture
    ) throws UnknownMemberIdException, GroupIdNotFoundException {
        String groupId = request.groupId();
        String memberId = request.memberId();
        GenericGroup group;
        try {
            group = getOrMaybeCreateGenericGroup(groupId, false);
        } catch (Throwable t) {
            responseFuture.complete(new SyncGroupResponseData()
                .setErrorCode(Errors.forException(t).code())
            );
            return EMPTY_RESULT;
        }

        Optional<Errors> errorOpt = validateSyncGroup(group, request);
        if (errorOpt.isPresent()) {
            responseFuture.complete(new SyncGroupResponseData()
                .setErrorCode(errorOpt.get().code()));
        } else if (group.isInState(EMPTY)) {
            responseFuture.complete(new SyncGroupResponseData()
                .setErrorCode(Errors.UNKNOWN_MEMBER_ID.code()));
        } else if (group.isInState(PREPARING_REBALANCE)) {
            responseFuture.complete(new SyncGroupResponseData()
                .setErrorCode(Errors.REBALANCE_IN_PROGRESS.code()));
        } else if (group.isInState(COMPLETING_REBALANCE)) {
            group.member(memberId).setAwaitingSyncFuture(responseFuture);
            removePendingSyncMember(group, request.memberId());

            // If this is the leader, then we can attempt to persist state and transition to stable
            if (group.isLeader(memberId)) {
                log.info("Assignment received from leader {} for group {} for generation {}. " +
                    "The group has {} members, {} of which are static.",
                    memberId, groupId, group.generationId(), group.size(), group.allStaticMemberIds().size());

                // Fill all members with corresponding member assignment. If the member assignment
                // does not exist, fill with an empty assignment.
                Map<String, byte[]> assignment = new HashMap<>();
                request.assignments().forEach(memberAssignment ->
                    assignment.put(memberAssignment.memberId(), memberAssignment.assignment())
                );

                Map<String, byte[]> membersWithMissingAssignment = new HashMap<>();
                group.allMembers().forEach(member -> {
                    if (!assignment.containsKey(member.memberId())) {
                        membersWithMissingAssignment.put(member.memberId(), EMPTY_ASSIGNMENT);
                    }
                });
                assignment.putAll(membersWithMissingAssignment);

                if (!membersWithMissingAssignment.isEmpty()) {
                    log.warn("Setting empty assignments for members {} of {} for generation {}.",
                        membersWithMissingAssignment, groupId, group.generationId());
                }

                CompletableFuture<Void> appendFuture = new CompletableFuture<>();
                appendFuture.whenComplete((__, t) -> {
                    // Another member may have joined the group while we were awaiting this callback,
                    // so we must ensure we are still in the CompletingRebalance state and the same generation
                    // when it gets invoked. if we have transitioned to another state, then do nothing
                    if (group.isInState(COMPLETING_REBALANCE) && request.generationId() == group.generationId()) {
                        if (t != null) {
                            Errors error = Errors.forException(t);
                            resetAndPropagateAssignmentWithError(group, error);
                            maybePrepareRebalanceOrCompleteJoin(group, "Error " + error + " when storing group assignment" +
                                "during SyncGroup (member: " + memberId + ").");
                        } else {
                            // Update group's assignment and propagate to all members.
                            setAndPropagateAssignment(group, assignment);
                            group.transitionTo(STABLE);
                        }
                    }
                });

                List<Record> records = Collections.singletonList(
                    RecordHelpers.newGroupMetadataRecord(group, assignment, metadataImage.features().metadataVersion())
                );
                return new CoordinatorResult<>(records, appendFuture);
            }
        } else if (group.isInState(STABLE)) {
            removePendingSyncMember(group, memberId);

            // If the group is stable, we just return the current assignment
            GenericGroupMember member = group.member(memberId);
            responseFuture.complete(new SyncGroupResponseData()
                .setProtocolType(group.protocolType().orElse(null))
                .setProtocolName(group.protocolName().orElse(null))
                .setAssignment(member.assignment())
                .setErrorCode(Errors.NONE.code()));
        } else if (group.isInState(DEAD)) {
            throw new IllegalStateException("Reached unexpected condition for Dead group " + groupId);
        }

        return EMPTY_RESULT;
    }

    // Visible for testing
    static Errors appendGroupMetadataErrorToResponseError(Errors appendError) {
        switch (appendError) {
            case UNKNOWN_TOPIC_OR_PARTITION:
            case NOT_ENOUGH_REPLICAS:
                return COORDINATOR_NOT_AVAILABLE;

            case NOT_LEADER_OR_FOLLOWER:
            case KAFKA_STORAGE_ERROR:
                return NOT_COORDINATOR;

            case MESSAGE_TOO_LARGE:
            case RECORD_LIST_TOO_LARGE:
            case INVALID_FETCH_SIZE:
                return UNKNOWN_SERVER_ERROR;

            default:
                return appendError;
        }
    }

    private Optional<Errors> validateSyncGroup(
        GenericGroup group,
        SyncGroupRequestData request
    ) {
        if (group.isInState(DEAD)) {
            // If the group is marked as dead, it means some other thread has just removed the group
            // from the coordinator metadata; this is likely that the group has migrated to some other
            // coordinator OR the group is in a transient unstable phase. Let the member retry
            // finding the correct coordinator and rejoin.
            return Optional.of(COORDINATOR_NOT_AVAILABLE);
        } else {
            try {
                group.validateMember(
                    request.memberId(),
                    request.groupInstanceId(),
                    "sync-group"
                );
            } catch (KafkaException ex) {
                return Optional.of(Errors.forException(ex));
            }

            if (request.generationId() != group.generationId()) {
                return Optional.of(Errors.ILLEGAL_GENERATION);
            } else if (isProtocolInconsistent(request.protocolType(), group.protocolType().orElse(null)) ||
                       isProtocolInconsistent(request.protocolName(), group.protocolName().orElse(null))) {
                return Optional.of(Errors.INCONSISTENT_GROUP_PROTOCOL);
            } else {
                return Optional.empty();
            }
        }
    }

    private void removePendingSyncMember(
        GenericGroup group,
        String memberId
    ) {
        group.removePendingSyncMember(memberId);
        String syncKey = genericGroupSyncKey(group.groupId());
        switch (group.currentState()) {
            case DEAD:
            case EMPTY:
            case PREPARING_REBALANCE:
                timer.cancel(syncKey);
                break;
            case COMPLETING_REBALANCE:
            case STABLE:
                if (group.hasReceivedSyncFromAllMembers()) {
                    timer.cancel(syncKey);
                }
                break;
            default:
                throw new IllegalStateException("Unknown group state: " + group.stateAsString());
        }
    }

    /**
     * Handle a generic group HeartbeatRequest.
     *
     * @param context        The request context.
     * @param request        The actual Heartbeat request.
     *
     * @return The Heartbeat response.
     */
    public HeartbeatResponseData genericGroupHeartbeat(
        RequestContext context,
        HeartbeatRequestData request
    ) {
        GenericGroup group = getOrMaybeCreateGenericGroup(request.groupId(), false);

        validateGenericGroupHeartbeat(group, request.memberId(), request.groupInstanceId(), request.generationId());

        switch (group.currentState()) {
            case EMPTY:
                return new HeartbeatResponseData().setErrorCode(Errors.UNKNOWN_MEMBER_ID.code());

            case PREPARING_REBALANCE:
                rescheduleGenericGroupMemberHeartbeat(group, group.member(request.memberId()));
                return new HeartbeatResponseData().setErrorCode(Errors.REBALANCE_IN_PROGRESS.code());

            case COMPLETING_REBALANCE:
            case STABLE:
                // Consumers may start sending heartbeats after join-group response, while the group
                // is in CompletingRebalance state. In this case, we should treat them as
                // normal heartbeat requests and reset the timer
                rescheduleGenericGroupMemberHeartbeat(group, group.member(request.memberId()));
                return new HeartbeatResponseData();

            default:
                throw new IllegalStateException("Reached unexpected state " +
                    group.currentState() + " for group " + group.groupId());
        }
    }

    /**
     * Validates a generic group heartbeat request.
     *
     * @param group              The group.
     * @param memberId           The member id.
     * @param groupInstanceId    The group instance id.
     * @param generationId       The generation id.
     *
     * @throws CoordinatorNotAvailableException If group is Dead.
     * @throws IllegalGenerationException       If the generation id in the request and the generation id of the
     *                                          group does not match.
     */
    private void validateGenericGroupHeartbeat(
        GenericGroup group,
        String memberId,
        String groupInstanceId,
        int generationId
    ) throws CoordinatorNotAvailableException, IllegalGenerationException {
        if (group.isInState(DEAD)) {
            throw COORDINATOR_NOT_AVAILABLE.exception();
        } else {
            group.validateMember(
                memberId,
                groupInstanceId,
                "heartbeat"
            );

            if (generationId != group.generationId()) {
                throw ILLEGAL_GENERATION.exception();
            }
        }
    }

    /**
     * Handle a generic LeaveGroupRequest.
     *
     * @param context        The request context.
     * @param request        The actual LeaveGroup request.
     *
     * @return The LeaveGroup response and the GroupMetadata record to append if the group
     *         no longer has any members.
     */
    public CoordinatorResult<LeaveGroupResponseData, Record> genericGroupLeave(
        RequestContext context,
        LeaveGroupRequestData request
    ) throws UnknownMemberIdException, GroupIdNotFoundException {
        GenericGroup group = getOrMaybeCreateGenericGroup(request.groupId(), false);
        if (group.isInState(DEAD)) {
            return new CoordinatorResult<>(
                Collections.emptyList(),
                new LeaveGroupResponseData()
                    .setErrorCode(COORDINATOR_NOT_AVAILABLE.code())
            );
        }

        List<MemberResponse> memberResponses = new ArrayList<>();

        for (MemberIdentity member: request.members()) {
            String reason = member.reason() != null ? member.reason() : "not provided";
            // The LeaveGroup API allows administrative removal of members by GroupInstanceId
            // in which case we expect the MemberId to be undefined.
            if (UNKNOWN_MEMBER_ID.equals(member.memberId())) {
                if (member.groupInstanceId() != null && group.hasStaticMember(member.groupInstanceId())) {
                    removeCurrentMemberFromGenericGroup(
                        group,
                        group.staticMemberId(member.groupInstanceId()),
                        reason
                    );
                    memberResponses.add(
                        new MemberResponse()
                            .setMemberId(member.memberId())
                            .setGroupInstanceId(member.groupInstanceId())
                    );
                } else {
                    memberResponses.add(
                        new MemberResponse()
                            .setMemberId(member.memberId())
                            .setGroupInstanceId(member.groupInstanceId())
                            .setErrorCode(Errors.UNKNOWN_MEMBER_ID.code())
                    );
                }
            } else if (group.isPendingMember(member.memberId())) {
                group.remove(member.memberId());
                timer.cancel(genericGroupHeartbeatKey(group.groupId(), member.memberId()));
                log.info("[Group {}] Pending member {} has left group through explicit `LeaveGroup` request; client reason: {}",
                    group.groupId(), member.memberId(), reason);

                memberResponses.add(
                    new MemberResponse()
                        .setMemberId(member.memberId())
                        .setGroupInstanceId(member.groupInstanceId())
                );
            } else {
                try {
                    group.validateMember(member.memberId(), member.groupInstanceId(), "leave-group");
                    removeCurrentMemberFromGenericGroup(
                        group,
                        member.memberId(),
                        reason
                    );
                    memberResponses.add(
                        new MemberResponse()
                            .setMemberId(member.memberId())
                            .setGroupInstanceId(member.groupInstanceId())
                    );
                } catch (KafkaException e) {
                    memberResponses.add(
                        new MemberResponse()
                            .setMemberId(member.memberId())
                            .setGroupInstanceId(member.groupInstanceId())
                            .setErrorCode(Errors.forException(e).code())
                    );
                }
            }
        }

        List<String> validLeaveGroupMembers = memberResponses.stream()
            .filter(response -> response.errorCode() == Errors.NONE.code())
            .map(MemberResponse::memberId)
            .collect(Collectors.toList());

        String reason = "explicit `LeaveGroup` request for (" + String.join(", ", validLeaveGroupMembers) + ") members.";
        CoordinatorResult<Void, Record> coordinatorResult = EMPTY_RESULT;

        if (!validLeaveGroupMembers.isEmpty()) {
            switch (group.currentState()) {
                case STABLE:
                case COMPLETING_REBALANCE:
                    coordinatorResult = maybePrepareRebalanceOrCompleteJoin(group, reason);
                    break;
                case PREPARING_REBALANCE:
                    coordinatorResult = maybeCompleteJoinPhase(group);
                    break;
                default:
            }
        }

        return new CoordinatorResult<>(
            coordinatorResult.records(),
            new LeaveGroupResponseData()
                .setMembers(memberResponses),
            coordinatorResult.appendFuture()
        );
    }

    /**
     * Remove a member from the group. Cancel member's heartbeat, and prepare rebalance
     * or complete the join phase if necessary.
     * 
     * @param group     The generic group.
     * @param memberId  The member id.
     * @param reason    The reason for the LeaveGroup request.
     *
     */
    private void removeCurrentMemberFromGenericGroup(
        GenericGroup group,
        String memberId,
        String reason
    ) {
        GenericGroupMember member = group.member(memberId);
        timer.cancel(genericGroupHeartbeatKey(group.groupId(), memberId));
        log.info("[Group {}] Member {} has left group through explicit `LeaveGroup` request; client reason: {}",
            group.groupId(), memberId, reason);

        // New members may timeout with a pending JoinGroup while the group is still rebalancing, so we have
        // to invoke the callback before removing the member. We return UNKNOWN_MEMBER_ID so that the consumer
        // will retry the JoinGroup request if is still active.
        group.completeJoinFuture(
            member,
            new JoinGroupResponseData()
                .setMemberId(UNKNOWN_MEMBER_ID)
                .setErrorCode(Errors.UNKNOWN_MEMBER_ID.code())
        );
        group.remove(member.memberId());
    }

    /**
     * Handles a DeleteGroups request.
     * Populates the record list passed in with record to update the state machine.
     * Validations are done in {@link GroupCoordinatorShard#deleteGroups(RequestContext, List)} by
     * calling {@link GroupMetadataManager#validateDeleteGroup(String)}.
     *
     * @param groupId The id of the group to be deleted. It has been checked in {@link GroupMetadataManager#validateDeleteGroup}.
     * @param records The record list to populate.
     */
    public void deleteGroup(
        String groupId,
        List<Record> records
    ) {
        // At this point, we have already validated the group id, so we know that the group exists and that no exception will be thrown.
        group(groupId).createGroupTombstoneRecords(records);
    }

    /**
     * Validates the DeleteGroups request.
     *
     * @param groupId The id of the group to be deleted.
     */
    void validateDeleteGroup(String groupId) throws ApiException {
        Group group = group(groupId);
        group.validateDeleteGroup();
    }

    /**
     * Delete the group if it exists and is in Empty state.
     *
     * @param groupId The group id.
     * @param records The list of records to append the group metadata tombstone records.
     */
    public void maybeDeleteGroup(String groupId, List<Record> records) {
        Group group = groups.get(groupId);
        if (group != null && group.isEmpty()) {
            deleteGroup(groupId, records);
        }
    }

    /**
     * Checks whether the given protocol type or name in the request is inconsistent with the group's.
     *
     * @param protocolTypeOrName       The request's protocol type or name.
     * @param groupProtocolTypeOrName  The group's protoocl type or name.
     *
     * @return  True if protocol is inconsistent, false otherwise.
     */
    private boolean isProtocolInconsistent(
        String protocolTypeOrName,
        String groupProtocolTypeOrName
    ) {
        return protocolTypeOrName != null
            && groupProtocolTypeOrName != null
            && !groupProtocolTypeOrName.equals(protocolTypeOrName);
    }

    /**
     * @return The set of all groups' ids.
     */
    public Set<String> groupIds() {
        return Collections.unmodifiableSet(this.groups.keySet());
    }

    /**
     * Generate a generic group heartbeat key for the timer.
     *
     * Package private for testing.
     *
     * @param groupId   The group id.
     * @param memberId  The member id.
     *
     * @return the heartbeat key.
     */
    static String genericGroupHeartbeatKey(String groupId, String memberId) {
        return "heartbeat-" + groupId + "-" + memberId;
    }

    /**
     * Generate a generic group join key for the timer.
     *
     * Package private for testing.
     *
     * @param groupId   The group id.
     *
     * @return the join key.
     */
    static String genericGroupJoinKey(String groupId) {
        return "join-" + groupId;
    }

    /**
     * Generate a generic group sync key for the timer.
     *
     * Package private for testing.
     *
     * @param groupId   The group id.
     *
     * @return the sync key.
     */
    static String genericGroupSyncKey(String groupId) {
        return "sync-" + groupId;
    }
}
