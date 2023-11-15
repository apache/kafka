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
package org.apache.kafka.coordinator.group.generic;

import org.apache.kafka.clients.consumer.internals.ConsumerProtocol;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.CoordinatorNotAvailableException;
import org.apache.kafka.common.errors.FencedInstanceIdException;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.IllegalGenerationException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.message.JoinGroupRequestData.JoinGroupRequestProtocolCollection;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.message.ListGroupsResponseData;
import org.apache.kafka.common.message.SyncGroupResponseData;
import org.apache.kafka.common.message.JoinGroupResponseData.JoinGroupResponseMember;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.SchemaException;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.coordinator.group.Group;
import org.apache.kafka.coordinator.group.OffsetExpirationCondition;
import org.apache.kafka.coordinator.group.OffsetExpirationConditionImpl;
import org.apache.kafka.coordinator.group.Record;
import org.apache.kafka.coordinator.group.RecordHelpers;
import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.apache.kafka.coordinator.group.generic.GenericGroupState.COMPLETING_REBALANCE;
import static org.apache.kafka.coordinator.group.generic.GenericGroupState.DEAD;
import static org.apache.kafka.coordinator.group.generic.GenericGroupState.EMPTY;
import static org.apache.kafka.coordinator.group.generic.GenericGroupState.PREPARING_REBALANCE;
import static org.apache.kafka.coordinator.group.generic.GenericGroupState.STABLE;

/**
 * This class holds metadata for a generic group where the
 * member assignment is driven solely from the client side.
 *
 * The APIs members use to make changes to the group membership
 * consist of JoinGroup, SyncGroup, and LeaveGroup.
 */
public class GenericGroup implements Group {

    /**
     * Empty generation.
     */
    public static final int NO_GENERATION = -1;

    /**
     * Protocol with empty name.
     */
    public static final String NO_PROTOCOL_NAME = "";

    /**
     * No leader.
     */
    public static final String NO_LEADER = "";

    /**
     * Delimiter used to join a randomly generated UUID
     * with a client id or a group instance id.
     */
    private static final String MEMBER_ID_DELIMITER = "-";

    /**
     * The slf4j logger.
     */
    private final Logger log;

    /**
     * The group id.
     */
    private final String groupId;

    /**
     * The time.
     */
    private final Time time;

    /**
     * The current group state.
     */
    private GenericGroupState state;

    /**
     * The previous group state.
     */
    private GenericGroupState previousState;

    /**
     * The timestamp of when the group transitioned
     * to its current state.
     */
    private Optional<Long> currentStateTimestamp;

    /**
     * The protocol type used for rebalance.
     */
    private Optional<String> protocolType;

    /**
     * The protocol name used for rebalance.
     */
    private Optional<String> protocolName;

    /**
     * The generation id.
     */
    private int generationId;

    /**
     * The id of the group's leader.
     */
    private Optional<String> leaderId;

    /**
     * The members of the group.
     */
    private final Map<String, GenericGroupMember> members = new HashMap<>();

    /**
     * The static members of the group.
     */
    private final Map<String, String> staticMembers = new HashMap<>();

    /**
     * Members who have yet to (re)join the group
     * during the join group phase.
     */
    private final Set<String> pendingJoinMembers = new HashSet<>();

    /**
     * The number of members awaiting a join response.
     */
    private int numMembersAwaitingJoinResponse = 0;

    /**
     * Map of protocol names to the number of members that support them.
     */
    private final Map<String, Integer> supportedProtocols = new HashMap<>();

    /**
     * Members who have yet to sync with the group
     * during the sync group phase.
     */
    private final Set<String> pendingSyncMembers = new HashSet<>();

    /**
     * The list of topics to which the group members are subscribed.
     */
    private Optional<Set<String>> subscribedTopics = Optional.empty();

    /**
     * A flag to indicate whether a new member was added. Used
     * to further delay initial joins (new group).
     */
    private boolean newMemberAdded = false;

    public GenericGroup(
        LogContext logContext,
        String groupId,
        GenericGroupState initialState,
        Time time
    ) {
        this(
            logContext,
            groupId,
            initialState,
            time,
            0,
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.of(time.milliseconds())
        );
    }

    public GenericGroup(
        LogContext logContext,
        String groupId,
        GenericGroupState initialState,
        Time time,
        int generationId,
        Optional<String> protocolType,
        Optional<String> protocolName,
        Optional<String> leaderId,
        Optional<Long> currentStateTimestamp
    ) {
        Objects.requireNonNull(logContext);
        this.log = logContext.logger(GenericGroup.class);
        this.groupId = Objects.requireNonNull(groupId);
        this.state = Objects.requireNonNull(initialState);
        this.previousState = DEAD;
        this.time = Objects.requireNonNull(time);
        this.generationId = generationId;
        this.protocolType = protocolType;
        this.protocolName = protocolName;
        this.leaderId = leaderId;
        this.currentStateTimestamp = currentStateTimestamp;
    }

    /**
     * The type of this group.
     *
     * @return The group type (Generic).
     */
    @Override
    public GroupType type() {
        return GroupType.GENERIC;
    }

    /**
     * The state of this group.
     *
     * @return The current state as a String.
     */
    @Override
    public String stateAsString() {
        return this.state.toString();
    }

    /**
     * The state of this group based on the committed offset.
     *
     * @return The current state as a String.
     */
    @Override
    public String stateAsString(long committedOffset) {
        return this.state.toString();
    }

    /**
     * @return the group id.
     */
    public String groupId() {
        return this.groupId;
    }

    /**
     * @return the generation id.
     */
    public int generationId() {
        return this.generationId;
    }

    /**
     * @return the protocol name.
     */
    public Optional<String> protocolName() {
        return this.protocolName;
    }

    /**
     * @return the protocol type.
     */
    public Optional<String> protocolType() {
        return this.protocolType;
    }

    /**
     * @return the current group state.
     */
    public GenericGroupState currentState() {
        return this.state;
    }

    public GenericGroupState previousState() {
        return this.previousState;
    }

    /**
     * @return true if a new member was added.
     */
    public boolean newMemberAdded() {
        return this.newMemberAdded;
    }

    /**
     * Compares the group's current state with the given state.
     *
     * @param groupState the state to match against.
     * @return true if the state matches, false otherwise.
     */
    public boolean isInState(GenericGroupState groupState) {
        return this.state == groupState;
    }

    /**
     * To identify whether the given member id is part of this group.
     *
     * @param memberId the given member id.
     * @return true if the member is part of this group, false otherwise.
     */
    public boolean hasMemberId(String memberId) {
        return members.containsKey(memberId);
    }

    /**
     * Get the member metadata associated with the provided member id.
     *
     * @param memberId the member id.
     * @return the member metadata if it exists, null otherwise.
     */
    public GenericGroupMember member(String memberId) {
        return members.get(memberId);
    }

    /**
     * @return the total number of members in this group.
     */
    public int size() {
        return members.size();
    }

    /**
     * Used to identify whether the given member is the leader of this group.
     *
     * @param memberId the member id.
     * @return true if the member is the leader, false otherwise.
     */
    public boolean isLeader(String memberId) {
        return leaderId.map(id -> id.equals(memberId)).orElse(false);
    }

    /**
     * @return the leader id or null if a leader does not exist.
     */
    public String leaderOrNull() {
        return leaderId.orElse(null);
    }

    /**
     * @return the current state timestamp.
     */
    public long currentStateTimestampOrDefault() {
        return currentStateTimestamp.orElse(-1L);
    }

    /**
     * Sets newMemberAdded.
     *
     * @param value the value to set.
     */
    public void setNewMemberAdded(boolean value) {
        this.newMemberAdded = value;
    }

    /**
     * Sets subscribedTopics.
     *
     * @param subscribedTopics the value to set.
     */
    public void setSubscribedTopics(Optional<Set<String>> subscribedTopics) {
        this.subscribedTopics = subscribedTopics;
    }

    /**
     * @return whether the group is using the consumer protocol.
     */
    public boolean usesConsumerGroupProtocol() {
        return protocolType.map(type ->
            type.equals(ConsumerProtocol.PROTOCOL_TYPE)
        ).orElse(false);
    }

    /**
     * Add a member to this group.
     *
     * @param member  the member to add.
     */
    public void add(GenericGroupMember member) {
        add(member, null);
    }

    /**
     * Add a member to this group.
     *
     * @param member  the member to add.
     * @param future  the future to complete once the join group phase completes.
     */
    public void add(GenericGroupMember member, CompletableFuture<JoinGroupResponseData> future) {
        member.groupInstanceId().ifPresent(instanceId -> {
            if (staticMembers.containsKey(instanceId)) {
                throw new IllegalStateException("Static member with groupInstanceId=" +
                    instanceId + " cannot be added to group " + groupId + " since" +
                    " it is already a member.");
            }
            staticMembers.put(instanceId, member.memberId());
        });

        if (members.isEmpty()) {
            this.protocolType = Optional.of(member.protocolType());
        }

        if (!Objects.equals(this.protocolType.orElse(null), member.protocolType())) {
            throw new IllegalStateException("The group and member's protocol type must be the same.");
        }

        if (!supportsProtocols(member)) {
            throw new IllegalStateException("None of the member's protocols can be supported.");
        }

        if (!leaderId.isPresent()) {
            leaderId = Optional.of(member.memberId());
        }

        members.put(member.memberId(), member);
        incrementSupportedProtocols(member);
        member.setAwaitingJoinFuture(future);

        if (member.isAwaitingJoin()) {
            numMembersAwaitingJoinResponse++;
        }

        pendingJoinMembers.remove(member.memberId());
    }

    /**
     * Remove a member from the group.
     *
     * @param memberId the member id to remove.
     */
    public void remove(String memberId) {
        GenericGroupMember removedMember = members.remove(memberId);
        if (removedMember != null) {
            decrementSupportedProtocols(removedMember);
            if (removedMember.isAwaitingJoin()) {
                numMembersAwaitingJoinResponse--;
            }

            removedMember.groupInstanceId().ifPresent(staticMembers::remove);
        }

        if (isLeader(memberId)) {
            Iterator<String> iter = members.keySet().iterator();
            String newLeader = iter.hasNext() ? iter.next() : null;
            leaderId = Optional.ofNullable(newLeader);
        }

        pendingJoinMembers.remove(memberId);
        pendingSyncMembers.remove(memberId);
    }

    /**
     * Check whether current leader has rejoined. If not, choose a
     * new leader from one of the joined members.
     *
     * Return false if
     *   1. the group is currently empty (has no designated leader)
     *   2. no member rejoined
     *
     * @return true if a new leader was elected or the existing
     *         leader rejoined, false otherwise.
     */
    public boolean maybeElectNewJoinedLeader() {
        if (leaderId.isPresent()) {
            GenericGroupMember currentLeader = member(leaderId.get());
            if (!currentLeader.isAwaitingJoin()) {
                for (GenericGroupMember member : members.values()) {
                    if (member.isAwaitingJoin()) {
                        leaderId = Optional.of(member.memberId());
                        log.info("Group leader [memberId: {}, groupInstanceId: {}] " +
                                "failed to join before the rebalance timeout. Member {} " +
                                "was elected as the new leader.",
                            currentLeader.memberId(),
                            currentLeader.groupInstanceId().orElse("None"),
                            member
                        );
                        return true;
                    }
                }
                log.info("Group leader [memberId: {}, groupInstanceId: {}] " +
                        "failed to join before the rebalance timeout and the " +
                        "group couldn't proceed to the next generation because " +
                        "no member joined.",
                    currentLeader.memberId(),
                    currentLeader.groupInstanceId().orElse("None")
                );
                return false;
            }
            return true;
        }
        return false;
    }

    /**
     * [For static members only]: Replace the old member id with the new one,
     * keep everything else unchanged and return the updated member.
     *
     * @param groupInstanceId  the group instance id.
     * @param oldMemberId      the old member id.
     * @param newMemberId      the new member id that will replace the old member id.
     * @return the member with the new id.
     */
    public GenericGroupMember replaceStaticMember(
        String groupInstanceId,
        String oldMemberId,
        String newMemberId
    ) {
        GenericGroupMember removedMember = members.remove(oldMemberId);
        if (removedMember == null) {
            throw new IllegalArgumentException("Cannot replace non-existing member id " + oldMemberId);
        }

        // Fence potential duplicate member immediately if someone awaits join/sync future.
        JoinGroupResponseData joinGroupResponse = new JoinGroupResponseData()
            .setMembers(Collections.emptyList())
            .setMemberId(oldMemberId)
            .setGenerationId(NO_GENERATION)
            .setProtocolName(null)
            .setProtocolType(null)
            .setLeader(NO_LEADER)
            .setSkipAssignment(false)
            .setErrorCode(Errors.FENCED_INSTANCE_ID.code());
        completeJoinFuture(removedMember, joinGroupResponse);

        SyncGroupResponseData syncGroupResponse = new SyncGroupResponseData()
            .setAssignment(new byte[0])
            .setProtocolName(null)
            .setProtocolType(null)
            .setErrorCode(Errors.FENCED_INSTANCE_ID.code());
        completeSyncFuture(removedMember, syncGroupResponse);

        GenericGroupMember newMember = new GenericGroupMember(
            newMemberId,
            removedMember.groupInstanceId(),
            removedMember.clientId(),
            removedMember.clientHost(),
            removedMember.rebalanceTimeoutMs(),
            removedMember.sessionTimeoutMs(),
            removedMember.protocolType(),
            removedMember.supportedProtocols(),
            removedMember.assignment()
        );

        members.put(newMemberId, newMember);

        if (isLeader(oldMemberId)) {
            leaderId = Optional.of(newMemberId);
        }

        staticMembers.put(groupInstanceId, newMemberId);
        return newMember;
    }

    /**
     * Check whether a member has joined the group.
     *
     * @param memberId the member id.
     * @return true if the member has yet to join, false otherwise.
     */
    public boolean isPendingMember(String memberId) {
        return pendingJoinMembers.contains(memberId);
    }

    /**
     * Add a pending member.
     *
     * @param memberId the member id.
     * @return true if the group did not already have the pending member,
     *         false otherwise.
     */
    public boolean addPendingMember(String memberId) {
        if (hasMemberId(memberId)) {
            throw new IllegalStateException("Attempt to add pending member " + memberId +
                " which is already a stable member of the group.");
        }
        return pendingJoinMembers.add(memberId);
    }

    /**
     * @return number of members that are pending join.
     */
    public int numPendingJoinMembers() {
        return pendingJoinMembers.size();
    }

    /**
     * Add a pending sync member.
     *
     * @param memberId the member id.
     * @return true if the group did not already have the pending sync member,
     *         false otherwise.
     */
    public boolean addPendingSyncMember(String memberId) {
        if (!hasMemberId(memberId)) {
            throw new IllegalStateException("Attempt to add pending sync member " + memberId +
                " which is already a stable member of the group.");
        }

        return pendingSyncMembers.add(memberId);
    }

    /**
     * Remove a member that has not yet synced.
     *
     * @param memberId the member id.
     * @return true if the group did store this member, false otherwise.
     */
    public boolean removePendingSyncMember(String memberId) {
        if (!hasMemberId(memberId)) {
            throw new IllegalStateException("Attempt to add pending member " + memberId +
                " which is already a stable member of the group.");
        }
        return pendingSyncMembers.remove(memberId);
    }

    /**
     * @return true if all members have sent sync group requests,
     *         false otherwise.
     */
    public boolean hasReceivedSyncFromAllMembers() {
        return pendingSyncMembers.isEmpty();
    }

    /**
     * @return members that have yet to sync.
     */
    public Set<String> allPendingSyncMembers() {
        return pendingSyncMembers;
    }

    /**
     * Clear members pending sync.
     */
    public void clearPendingSyncMembers() {
        pendingSyncMembers.clear();
    }

    /**
     * Checks whether the given group instance id exists as
     * a static member.
     *
     * @param groupInstanceId the group instance id.
     * @return true if a static member with the group instance id exists,
     *         false otherwise.
     */
    public boolean hasStaticMember(String groupInstanceId) {
        return staticMembers.containsKey(groupInstanceId);
    }

    /**
     * Get member id of a static member that matches the given group
     * instance id.
     *
     * @param groupInstanceId the group instance id.
     * @return the static member if it exists.
     */
    public String staticMemberId(String groupInstanceId) {
        return staticMembers.get(groupInstanceId);
    }

    /**
     * @return members who have yet to rejoin during the
     *         join group phase.
     */
    public Map<String, GenericGroupMember> notYetRejoinedMembers() {
        Map<String, GenericGroupMember> notYetRejoinedMembers = new HashMap<>();
        members.values().forEach(member -> {
            if (!member.isAwaitingJoin()) {
                notYetRejoinedMembers.put(member.memberId(), member);
            }
        });
        return notYetRejoinedMembers;
    }

    /**
     * @return whether all members have joined.
     */
    public boolean hasAllMembersJoined() {
        return members.size() == numMembersAwaitingJoinResponse && pendingJoinMembers.isEmpty();
    }

    /**
     * @return the ids of all members in the group.
     */
    public Set<String> allMemberIds() {
        return members.keySet();
    }

    /**
     * @return the ids of all static members in the group.
     */
    public Set<String> allStaticMemberIds() {
        return new HashSet<>(staticMembers.values());
    }

    // For testing only.
    public Set<String> allDynamicMemberIds() {
        Set<String> dynamicMemberSet = new HashSet<>(allMemberIds());
        staticMembers.values().forEach(dynamicMemberSet::remove);
        return dynamicMemberSet;
    }

    /**
     * @return number of members waiting for a join group response.
     */
    public int numAwaitingJoinResponse() {
        return numMembersAwaitingJoinResponse;
    }

    /**
     * @return all members.
     */
    public Collection<GenericGroupMember> allMembers() {
        return members.values();
    }

    /**
     * @return the group's rebalance timeout in milliseconds.
     *         It is the max of all members' rebalance timeout.
     */
    public int rebalanceTimeoutMs() {
        int maxRebalanceTimeoutMs = 0;
        for (GenericGroupMember member : members.values()) {
            maxRebalanceTimeoutMs = Math.max(maxRebalanceTimeoutMs, member.rebalanceTimeoutMs());
        }
        return maxRebalanceTimeoutMs;
    }

    /**
     * Generate a member id from the given client and group instance ids.
     *
     * @param clientId         the client id.
     * @param groupInstanceId  the group instance id.
     * @return the generated id.
     */
    public String generateMemberId(String clientId, Optional<String> groupInstanceId) {
        return groupInstanceId.map(s -> s + MEMBER_ID_DELIMITER + UUID.randomUUID())
            .orElseGet(() -> clientId + MEMBER_ID_DELIMITER + UUID.randomUUID());
    }

    /**
     * Validates that (1) the group instance id exists and is mapped to the member id
     * if the group instance id is provided; and (2) the member id exists in the group.
     *
     * @param memberId          The member id.
     * @param groupInstanceId   The group instance id.
     * @param operation         The operation.
     *
     * @throws UnknownMemberIdException
     * @throws FencedInstanceIdException
     */
    public void validateMember(
        String memberId,
        String groupInstanceId,
        String operation
    ) throws UnknownMemberIdException, FencedInstanceIdException {
        if (groupInstanceId != null) {
            String existingMemberId = staticMemberId(groupInstanceId);
            if (existingMemberId == null) {
                throw Errors.UNKNOWN_MEMBER_ID.exception();
            } else if (!existingMemberId.equals(memberId)) {
                log.info("Request memberId={} for static member with groupInstanceId={} " +
                         "is fenced by existing memberId={} during operation {}",
                    memberId, groupInstanceId, existingMemberId, operation);
                throw Errors.FENCED_INSTANCE_ID.exception();
            }
        }

        if (!hasMemberId(memberId)) {
            throw Errors.UNKNOWN_MEMBER_ID.exception();
        }
    }

    /**
     * Validates the OffsetCommit request.
     *
     * @param memberId          The member id.
     * @param groupInstanceId   The group instance id.
     * @param generationId      The generation id.
     */
    @Override
    public void validateOffsetCommit(
        String memberId,
        String groupInstanceId,
        int generationId
    ) throws CoordinatorNotAvailableException, UnknownMemberIdException, IllegalGenerationException, FencedInstanceIdException {
        if (isInState(DEAD)) {
            throw Errors.COORDINATOR_NOT_AVAILABLE.exception();
        }

        if (generationId < 0 && isInState(EMPTY)) {
            // When the generation id is -1, the request comes from either the admin client
            // or a consumer which does not use the group management facility. In this case,
            // the request can commit offsets if the group is empty.
            return;
        }

        if (generationId >= 0 || !memberId.isEmpty() || groupInstanceId != null) {
            validateMember(memberId, groupInstanceId, "offset-commit");

            if (generationId != this.generationId) {
                throw Errors.ILLEGAL_GENERATION.exception();
            }
        } else if (!isInState(EMPTY)) {
            // If the request does not contain the member id and the generation id (version 0),
            // offset commits are only accepted when the group is empty.
            throw Errors.UNKNOWN_MEMBER_ID.exception();
        }

        if (isInState(COMPLETING_REBALANCE)) {
            // We should not receive a commit request if the group has not completed rebalance;
            // but since the consumer's member.id and generation is valid, it means it has received
            // the latest group generation information from the JoinResponse.
            // So let's return a REBALANCE_IN_PROGRESS to let consumer handle it gracefully.
            throw Errors.REBALANCE_IN_PROGRESS.exception();
        }
    }

    /**
     * Validates the OffsetFetch request.
     *
     * @param memberId              The member id. This is not provided for generic groups.
     * @param memberEpoch           The member epoch for consumer groups. This is not provided for generic groups.
     * @param lastCommittedOffset   The last committed offsets in the timeline.
     */
    @Override
    public void validateOffsetFetch(
        String memberId,
        int memberEpoch,
        long lastCommittedOffset
    ) throws GroupIdNotFoundException {
        if (isInState(DEAD)) {
            throw new GroupIdNotFoundException(String.format("Group %s is in dead state.", groupId));
        }
    }

    /**
     * Validates the OffsetDelete request.
     */
    @Override
    public void validateOffsetDelete() throws ApiException {
        switch (currentState()) {
            case DEAD:
                throw new GroupIdNotFoundException(String.format("Group %s is in dead state.", groupId));
            case STABLE:
            case PREPARING_REBALANCE:
            case COMPLETING_REBALANCE:
                if (!usesConsumerGroupProtocol()) {
                    throw Errors.NON_EMPTY_GROUP.exception();
                }
                break;
            default:
        }
    }

    /**
     * Validates the DeleteGroups request.
     */
    @Override
    public void validateDeleteGroup() throws ApiException {
        switch (currentState()) {
            case DEAD:
                throw new GroupIdNotFoundException(String.format("Group %s is in dead state.", groupId));
            case STABLE:
            case PREPARING_REBALANCE:
            case COMPLETING_REBALANCE:
                throw Errors.NON_EMPTY_GROUP.exception();
            default:
        }
    }

    /**
     * Populates the list of records with tombstone(s) for deleting the group.
     *
     * @param records The list of records.
     */
    @Override
    public void createGroupTombstoneRecords(List<Record> records) {
        records.add(RecordHelpers.newGroupMetadataTombstoneRecord(groupId()));
    }

    @Override
    public boolean isEmpty() {
        return isInState(EMPTY);
    }

    /**
     * Return the offset expiration condition to be used for this group. This is based on several factors
     * such as the group state, the protocol type, and the GroupMetadata record version.
     *
     * See {@link org.apache.kafka.coordinator.group.OffsetExpirationCondition}
     *
     * @return The offset expiration condition for the group or Empty if no such condition exists.
     */
    @Override
    public Optional<OffsetExpirationCondition> offsetExpirationCondition() {
        if (protocolType.isPresent()) {
            if (isInState(EMPTY)) {
                // No members exist in the group =>
                // - If current state timestamp exists and retention period has passed since group became Empty,
                //   expire all offsets with no pending offset commit;
                // - If there is no current state timestamp (old group metadata schema) and retention period has passed
                //   since the last commit timestamp, expire the offset
                return Optional.of(new OffsetExpirationConditionImpl(
                    offsetAndMetadata -> currentStateTimestamp.orElse(offsetAndMetadata.commitTimestampMs))
                );
            } else if (usesConsumerGroupProtocol() && subscribedTopics.isPresent() && isInState(STABLE)) {
                // Consumers exist in the group and group is Stable =>
                // - If the group is aware of the subscribed topics and retention period has passed since the
                //   last commit timestamp, expire the offset.
                return Optional.of(new OffsetExpirationConditionImpl(offsetAndMetadata -> offsetAndMetadata.commitTimestampMs));
            }
        } else {
            // protocolType is None => standalone (simple) consumer, that uses Kafka for offset storage. Only
            // expire offsets where retention period has passed since their last commit.
            return Optional.of(new OffsetExpirationConditionImpl(offsetAndMetadata -> offsetAndMetadata.commitTimestampMs));
        }
        // If none of the conditions above are met, do not expire any offsets.
        return Optional.empty();
    }

    /**
     * Verify the member id is up to date for static members. Return true if both conditions met:
     *   1. given member is a known static member to group
     *   2. group stored member id doesn't match with given member id
     *
     * @param groupInstanceId  the group instance id.
     * @param memberId         the member id.
     * @return whether the static member is fenced based on the condition above.
     */
    public boolean isStaticMemberFenced(
        String groupInstanceId,
        String memberId
    ) {
        String existingMemberId = staticMemberId(groupInstanceId);
        return existingMemberId != null && !existingMemberId.equals(memberId);
    }

    /**
     * @return whether the group can rebalance.
     */
    public boolean canRebalance() {
        return PREPARING_REBALANCE.validPreviousStates().contains(state);
    }

    /**
     * Transition to a group state.
     * @param groupState the group state.
     */
    public void transitionTo(GenericGroupState groupState) {
        assertValidTransition(groupState);
        previousState = state;
        state = groupState;
        currentStateTimestamp = Optional.of(time.milliseconds());
    }

    /**
     * Select a protocol that will be used for this group. Each member
     * will vote for a protocol and the one with the most votes will
     * be selected. Only a protocol that is supported by all members
     * can be selected.
     *
     * @return the name of the selected protocol.
     */
    public String selectProtocol() {
        if (members.isEmpty()) {
            throw new IllegalStateException("Cannot select protocol for empty group");
        }

        // select the protocol for this group which is supported by all members
        Set<String> candidates = candidateProtocols();

        // let each member vote for one of the protocols
        Map<String, Integer> votesByProtocol = new HashMap<>();
        allMembers().stream().map(member -> member.vote(candidates))
            .forEach(protocolName -> {
                int count = votesByProtocol.getOrDefault(protocolName, 0);
                votesByProtocol.put(protocolName, count + 1);
            });

        // choose the one with the most votes
        return votesByProtocol.entrySet().stream()
            .max(Comparator.comparingInt(Map.Entry::getValue))
            .map(Map.Entry::getKey).orElse(null);
    }

    /**
     * Increment the protocol count for all of the member's
     * supported protocols.
     *
     * @param member the member.
     */
    private void incrementSupportedProtocols(GenericGroupMember member) {
        member.supportedProtocols().forEach(protocol -> {
            int count = supportedProtocols.getOrDefault(protocol.name(), 0);
            supportedProtocols.put(protocol.name(), count + 1);
        });
    }

    /**
     * Decrement the protocol count for all of the member's
     * supported protocols.
     *
     * @param member the member.
     */
    private void decrementSupportedProtocols(GenericGroupMember member) {
        member.supportedProtocols().forEach(protocol -> {
            int count = supportedProtocols.getOrDefault(protocol.name(), 0);
            supportedProtocols.put(protocol.name(), count - 1);
        });
    }

    /**
     * A candidate protocol must be supported by all members.
     *
     * @return a set of candidate protocols that can be chosen as the protocol
     *         for the group.
     */
    private Set<String> candidateProtocols() {
        // get the set of protocols that are commonly supported by all members
        return supportedProtocols.entrySet().stream()
            .filter(protocol -> protocol.getValue() == members.size())
            .map(Map.Entry::getKey).collect(Collectors.toSet());
    }

    /**
     * Checks whether at least one of the given protocols can be supported. A
     * protocol can be supported if it is supported by all members.
     *
     * @param member               the member to check.
     *
     * @return a boolean based on the condition mentioned above.
     */
    public boolean supportsProtocols(GenericGroupMember member) {
        return supportsProtocols(
            member.protocolType(),
            GenericGroupMember.plainProtocolSet(member.supportedProtocols())
        );
    }

    /**
     * Checks whether at least one of the given protocols can be supported. A
     * protocol can be supported if it is supported by all members.
     *
     * @param memberProtocolType  the member protocol type.
     * @param memberProtocols     the set of protocol names.
     *
     * @return a boolean based on the condition mentioned above.
     */
    public boolean supportsProtocols(
        String memberProtocolType,
        JoinGroupRequestProtocolCollection memberProtocols
    ) {
        return supportsProtocols(
            memberProtocolType,
            GenericGroupMember.plainProtocolSet(memberProtocols)
        );
    }

    /**
     * Checks whether at least one of the given protocols can be supported. A
     * protocol can be supported if it is supported by all members.
     *
     * @param memberProtocolType  the member protocol type.
     * @param memberProtocols     the set of protocol names.
     *
     * @return a boolean based on the condition mentioned above.
     */
    public boolean supportsProtocols(String memberProtocolType, Set<String> memberProtocols) {
        if (isInState(EMPTY)) {
            return !memberProtocolType.isEmpty() && !memberProtocols.isEmpty();
        } else {
            return protocolType.map(type -> type.equals(memberProtocolType)).orElse(false) &&
                memberProtocols.stream()
                    .anyMatch(name -> supportedProtocols.getOrDefault(name, 0) == members.size());
        }
    }

    /**
     * @return the topics that the group is subscribed to.
     */
    public Optional<Set<String>> subscribedTopics() {
        return subscribedTopics;
    }

    /**
     * Returns true if the generic group is actively subscribed to the topic. When the generic group does not know,
     * because the information is not available yet or because it has failed to parse the Consumer Protocol, we
     * consider the group not subscribed to the topic if the group is not using any protocol or not using the
     * consumer group protocol.
     *
     * @param topic  The topic name.
     *
     * @return whether the group is subscribed to the topic.
     */
    public boolean isSubscribedToTopic(String topic) {
        return subscribedTopics.map(topics -> topics.contains(topic))
            .orElse(usesConsumerGroupProtocol());
    }

    /**
     * Collects the set of topics that the members are subscribed to when the Protocol Type is equal
     * to 'consumer'. None is returned if
     * - the protocol type is not equal to 'consumer';
     * - the protocol is not defined yet; or
     * - the protocol metadata does not comply with the schema.
     *
     * @return the subscribed topics or None based on the condition above.
     */
    public Optional<Set<String>> computeSubscribedTopics() {
        if (!protocolType.isPresent()) {
            return Optional.empty();
        }
        String type = protocolType.get();
        if (!type.equals(ConsumerProtocol.PROTOCOL_TYPE)) {
            return Optional.empty();
        }
        if (members.isEmpty()) {
            return Optional.of(Collections.emptySet());
        }

        if (protocolName.isPresent()) {
            try {
                Set<String> allSubscribedTopics = new HashSet<>();
                members.values().forEach(member -> {
                    // The consumer protocol is parsed with V0 which is the based prefix of all versions.
                    // This way the consumer group manager does not depend on any specific existing or
                    // future versions of the consumer protocol. VO must prefix all new versions.
                    ByteBuffer buffer = ByteBuffer.wrap(member.metadata(protocolName.get()));
                    ConsumerProtocol.deserializeVersion(buffer);
                    allSubscribedTopics.addAll(new HashSet<>(
                        ConsumerProtocol.deserializeSubscription(buffer, (short) 0).topics()
                    ));
                });
                return Optional.of(allSubscribedTopics);
            } catch (SchemaException e) {
                log.warn("Failed to parse Consumer Protocol " + ConsumerProtocol.PROTOCOL_TYPE + ":" +
                    protocolName.get() + " of group " + groupId + ". " +
                    "Consumer group coordinator is not aware of the subscribed topics.", e);
            }
        }

        return Optional.empty();
    }

    /**
     * Update a member.
     *
     * @param member              the member.
     * @param protocols           the list of protocols.
     * @param rebalanceTimeoutMs  the rebalance timeout in milliseconds.
     * @param sessionTimeoutMs    the session timeout in milliseconds.
     * @param future              the future that is invoked once the join phase is complete.
     */
    public void updateMember(
        GenericGroupMember member,
        JoinGroupRequestProtocolCollection protocols,
        int rebalanceTimeoutMs,
        int sessionTimeoutMs,
        CompletableFuture<JoinGroupResponseData> future
    ) {
        decrementSupportedProtocols(member);
        member.setSupportedProtocols(protocols);
        incrementSupportedProtocols(member);
        member.setRebalanceTimeoutMs(rebalanceTimeoutMs);
        member.setSessionTimeoutMs(sessionTimeoutMs);

        if (future != null && !member.isAwaitingJoin()) {
            numMembersAwaitingJoinResponse++;
        } else if (future == null && member.isAwaitingJoin()) {
            numMembersAwaitingJoinResponse--;
        }
        member.setAwaitingJoinFuture(future);
    }

    /**
     * Complete the join future.
     * 
     * @param member    the member.
     * @param response  the join response to complete the future with.
     * @return true if a join future actually completes.
     */
    public boolean completeJoinFuture(
        GenericGroupMember member,
        JoinGroupResponseData response
    ) {
        if (member.isAwaitingJoin()) {
            member.awaitingJoinFuture().complete(response);
            member.setAwaitingJoinFuture(null);
            numMembersAwaitingJoinResponse--;
            return true;
        }
        return false;
    }

    /**
     * Complete a member's sync future.
     * 
     * @param member    the member.
     * @param response  the sync response to complete the future with.
     * @return true if a sync future actually completes.
     */
    public boolean completeSyncFuture(
        GenericGroupMember member,
        SyncGroupResponseData response
    ) {
        if (member.isAwaitingSync()) {
            member.awaitingSyncFuture().complete(response);
            member.setAwaitingSyncFuture(null);
            return true;
        }
        return false;
    }

    /**
     * Initiate the next generation for the group.
     */
    public void initNextGeneration() {
        generationId++;
        if (!members.isEmpty()) {
            protocolName = Optional.of(selectProtocol());
            subscribedTopics = computeSubscribedTopics();
            transitionTo(COMPLETING_REBALANCE);
        } else {
            protocolName = Optional.empty();
            subscribedTopics = computeSubscribedTopics();
            transitionTo(EMPTY);
        }
        clearPendingSyncMembers();
    }

    /**
     * Get all members formatted as a join response.
     *
     * @return the members.
     */
    public List<JoinGroupResponseMember> currentGenericGroupMembers() {
        if (isInState(DEAD) || isInState(PREPARING_REBALANCE)) {
            throw new IllegalStateException("Cannot obtain generic member metadata for group " +
                groupId + " in state " + state);
        }

        return members.values().stream().map(member ->
            new JoinGroupResponseMember()
                .setMemberId(member.memberId())
                .setGroupInstanceId(member.groupInstanceId().orElse(null))
                .setMetadata(member.metadata(protocolName.orElse(null))))
            .collect(Collectors.toList());
    }

    /**
     * @return the group formatted as a list group response based on the committed offset.
     */
    public ListGroupsResponseData.ListedGroup asListedGroup(long committedOffset) {
        return new ListGroupsResponseData.ListedGroup()
            .setGroupId(groupId)
            .setProtocolType(protocolType.orElse(""))
            .setGroupState(state.toString());
    }

    /**
     * @return All member assignments keyed by member id.
     */
    public Map<String, byte[]> groupAssignment() {
        return allMembers().stream().collect(Collectors.toMap(
            GenericGroupMember::memberId, GenericGroupMember::assignment
        ));
    }

    /**
     * Checks whether the transition to the target state is valid.
     *
     * @param targetState the target state to transition to.
     */
    private void assertValidTransition(GenericGroupState targetState) {
        if (!targetState.validPreviousStates().contains(state)) {
            throw new IllegalStateException("Group " + groupId + " should be in one of " +
                targetState.validPreviousStates() + " states before moving to " + targetState +
                " state. Instead it is in " + state + " state.");
        }
    }

    @Override
    public String toString() {
        return "GenericGroupMetadata(" +
            "groupId=" + groupId + ", " +
            "generation=" + generationId + ", " +
            "protocolType=" + protocolType + ", " +
            "currentState=" + currentState() + ", " +
            "members=" + members + ")";
    }
}
