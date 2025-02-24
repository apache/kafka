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
package org.apache.kafka.coordinator.group.streams;

import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.StaleMemberEpochException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.ListGroupsResponseData;
import org.apache.kafka.common.message.StreamsGroupDescribeResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.group.Group;
import org.apache.kafka.coordinator.group.OffsetExpirationCondition;
import org.apache.kafka.coordinator.group.OffsetExpirationConditionImpl;
import org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetricsShard;
import org.apache.kafka.coordinator.group.streams.topics.ConfiguredSubtopology;
import org.apache.kafka.coordinator.group.streams.topics.ConfiguredTopology;
import org.apache.kafka.coordinator.group.streams.topics.InternalTopicManager;
import org.apache.kafka.image.TopicImage;
import org.apache.kafka.image.TopicsImage;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;
import org.apache.kafka.timeline.TimelineInteger;
import org.apache.kafka.timeline.TimelineObject;

import org.slf4j.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

import static org.apache.kafka.coordinator.group.streams.StreamsGroup.StreamsGroupState.ASSIGNING;
import static org.apache.kafka.coordinator.group.streams.StreamsGroup.StreamsGroupState.EMPTY;
import static org.apache.kafka.coordinator.group.streams.StreamsGroup.StreamsGroupState.NOT_READY;
import static org.apache.kafka.coordinator.group.streams.StreamsGroup.StreamsGroupState.RECONCILING;
import static org.apache.kafka.coordinator.group.streams.StreamsGroup.StreamsGroupState.STABLE;

/**
 * A Streams Group. All the metadata in this class are backed by records in the __consumer_offsets partitions.
 */
public class StreamsGroup implements Group {

    /**
     * The protocol type for streams groups. There is only one protocol type, "streams".
     */
    private static final String PROTOCOL_TYPE = "streams";

    public enum StreamsGroupState {
        EMPTY("Empty"),
        NOT_READY("NotReady"),
        ASSIGNING("Assigning"),
        RECONCILING("Reconciling"),
        STABLE("Stable"),
        DEAD("Dead");

        private final String name;

        private final String lowerCaseName;

        StreamsGroupState(String name) {
            this.name = name;
            if (Objects.equals(name, "NotReady")) {
                this.lowerCaseName = "not_ready";
            } else {
                this.lowerCaseName = name.toLowerCase(Locale.ROOT);
            }
        }

        @Override
        public String toString() {
            return name;
        }

        public String toLowerCaseString() {
            return lowerCaseName;
        }
    }

    public static class DeadlineAndEpoch {

        static final DeadlineAndEpoch EMPTY = new DeadlineAndEpoch(0L, 0);

        public final long deadlineMs;
        public final int epoch;

        DeadlineAndEpoch(long deadlineMs, int epoch) {
            this.deadlineMs = deadlineMs;
            this.epoch = epoch;
        }
    }

    private final LogContext logContext;
    private final Logger log;

    /**
     * The snapshot registry.
     */
    private final SnapshotRegistry snapshotRegistry;

    /**
     * The group ID.
     */
    private final String groupId;

    /**
     * The group state.
     */
    private final TimelineObject<StreamsGroupState> state;

    /**
     * The group epoch. The epoch is incremented whenever the topology, topic metadata or the set of members changes and it will trigger
     * the computation of a new assignment for the group.
     */
    private final TimelineInteger groupEpoch;

    /**
     * The group members.
     */
    private final TimelineHashMap<String, StreamsGroupMember> members;

    /**
     * The static group members.
     */
    private final TimelineHashMap<String, String> staticMembers;

    /**
     * The metadata associated with each subscribed topic name.
     */
    private final TimelineHashMap<String, TopicMetadata> partitionMetadata;

    /**
     * The target assignment epoch. An assignment epoch smaller than the group epoch means that a new assignment is required. The assignment
     * epoch is updated when a new assignment is installed.
     */
    private final TimelineInteger targetAssignmentEpoch;

    /**
     * The target assignment per member ID.
     */
    private final TimelineHashMap<String, TasksTuple> targetAssignment;

    /**
     * These maps map each active/standby/warmup task to the process ID(s) of their current owner.
     * The mapping is of the form <code>subtopology -> partition -> memberId</code>.
     * When a member revokes a partition, it removes its process ID from this map.
     * When a member gets a partition, it adds its process ID to this map.
     */
    private final TimelineHashMap<String, TimelineHashMap<Integer, String>> currentActiveTaskToProcessId;
    private final TimelineHashMap<String, TimelineHashMap<Integer, Set<String>>> currentStandbyTaskToProcessIds;
    private final TimelineHashMap<String, TimelineHashMap<Integer, Set<String>>> currentWarmupTaskToProcessIds;

    /**
     * The coordinator metrics.
     */
    private final GroupCoordinatorMetricsShard metrics;

    /**
     * The Streams topology.
     */
    private final TimelineObject<Optional<StreamsTopology>> topology;

    /**
     * The configured topology including resolved regular expressions.
     */
    private final TimelineObject<Optional<ConfiguredTopology>> configuredTopology;

    /**
     * The metadata refresh deadline. It consists of a timestamp in milliseconds together with the group epoch at the time of setting it.
     * The metadata refresh time is considered as a soft state (read that it is not stored in a timeline data structure). It is like this
     * because it is not persisted to the log. The group epoch is here to ensure that the metadata refresh deadline is invalidated if the
     * group epoch does not correspond to the current group epoch. This can happen if the metadata refresh deadline is updated after having
     * refreshed the metadata but the write operation failed. In this case, the time is not automatically rolled back.
     */
    private DeadlineAndEpoch metadataRefreshDeadline = DeadlineAndEpoch.EMPTY;

    public StreamsGroup(
        LogContext logContext,
        SnapshotRegistry snapshotRegistry,
        String groupId,
        GroupCoordinatorMetricsShard metrics
    ) {
        this.log = logContext.logger(StreamsGroup.class);
        this.logContext = logContext;
        this.snapshotRegistry = Objects.requireNonNull(snapshotRegistry);
        this.groupId = Objects.requireNonNull(groupId);
        this.state = new TimelineObject<>(snapshotRegistry, EMPTY);
        this.groupEpoch = new TimelineInteger(snapshotRegistry);
        this.members = new TimelineHashMap<>(snapshotRegistry, 0);
        this.staticMembers = new TimelineHashMap<>(snapshotRegistry, 0);
        this.partitionMetadata = new TimelineHashMap<>(snapshotRegistry, 0);
        this.targetAssignmentEpoch = new TimelineInteger(snapshotRegistry);
        this.targetAssignment = new TimelineHashMap<>(snapshotRegistry, 0);
        this.currentActiveTaskToProcessId = new TimelineHashMap<>(snapshotRegistry, 0);
        this.currentStandbyTaskToProcessIds = new TimelineHashMap<>(snapshotRegistry, 0);
        this.currentWarmupTaskToProcessIds = new TimelineHashMap<>(snapshotRegistry, 0);
        this.metrics = Objects.requireNonNull(metrics);
        this.topology = new TimelineObject<>(snapshotRegistry, Optional.empty());
        this.configuredTopology = new TimelineObject<>(snapshotRegistry, Optional.empty());
    }

    /**
     * @return The group type (Streams).
     */
    @Override
    public GroupType type() {
        return GroupType.STREAMS;
    }

    /**
     * @return The current state as a String.
     */
    @Override
    public String stateAsString() {
        return state.get().toString();
    }

    /**
     * @return The current state as a String with given committedOffset.
     */
    public String stateAsString(long committedOffset) {
        return state.get(committedOffset).toString();
    }

    /**
     * @return the group formatted as a list group response based on the committed offset.
     */
    public ListGroupsResponseData.ListedGroup asListedGroup(long committedOffset) {
        return new ListGroupsResponseData.ListedGroup()
            .setGroupId(groupId)
            .setProtocolType(PROTOCOL_TYPE)
            .setGroupState(state.get(committedOffset).toString())
            .setGroupType(type().toString());
    }

    public Optional<ConfiguredTopology> configuredTopology() {
        return configuredTopology.get();
    }

    public Optional<StreamsTopology> topology() {
        return topology.get();
    }

    public void setTopology(StreamsTopology topology) {
        this.topology.set(Optional.ofNullable(topology));
        maybeUpdateConfiguredTopology();
        maybeUpdateGroupState();
    }

    /**
     * @return The group ID.
     */
    @Override
    public String groupId() {
        return groupId;
    }

    /**
     * @return The current state.
     */
    public StreamsGroupState state() {
        return state.get();
    }

    /**
     * @return The group epoch.
     */
    public int groupEpoch() {
        return groupEpoch.get();
    }

    /**
     * Sets the group epoch.
     *
     * @param groupEpoch The new group epoch.
     */
    public void setGroupEpoch(int groupEpoch) {
        this.groupEpoch.set(groupEpoch);
        maybeUpdateGroupState();
    }

    /**
     * @return The target assignment epoch.
     */
    public int assignmentEpoch() {
        return targetAssignmentEpoch.get();
    }

    /**
     * Sets the assignment epoch.
     *
     * @param targetAssignmentEpoch The new assignment epoch.
     */
    public void setTargetAssignmentEpoch(int targetAssignmentEpoch) {
        this.targetAssignmentEpoch.set(targetAssignmentEpoch);
        maybeUpdateGroupState();
    }

    /**
     * Get member ID of a static member that matches the given group instance ID.
     *
     * @param groupInstanceId The group instance ID.
     * @return The member ID corresponding to the given instance ID or null if it does not exist
     */
    public String staticMemberId(String groupInstanceId) {
        return staticMembers.get(groupInstanceId);
    }

    /**
     * Gets or creates a new member but without adding it to the group. Adding a member is done via the
     * {@link StreamsGroup#updateMember(StreamsGroupMember)} method.
     *
     * @param memberId          The member ID.
     * @param createIfNotExists Booleans indicating whether the member must be created if it does not exist.
     * @return A StreamsGroupMember.
     */
    public StreamsGroupMember getOrMaybeCreateMember(
        String memberId,
        boolean createIfNotExists
    ) throws UnknownMemberIdException {
        StreamsGroupMember member = members.get(memberId);
        if (member != null) {
            return member;
        }

        if (!createIfNotExists) {
            throw new UnknownMemberIdException(
                String.format("Member %s is not a member of group %s.", memberId, groupId)
            );
        }

        return new StreamsGroupMember.Builder(memberId).build();
    }

    /**
     * Gets a static member.
     *
     * @param instanceId The group instance ID.
     * @return The member corresponding to the given instance ID or null if it does not exist
     */
    public StreamsGroupMember staticMember(String instanceId) {
        String existingMemberId = staticMemberId(instanceId);
        return existingMemberId == null ? null : getOrMaybeCreateMember(existingMemberId, false);
    }

    /**
     * Adds or updates the member.
     *
     * @param newMember The new member state.
     */
    public void updateMember(StreamsGroupMember newMember) {
        if (newMember == null) {
            throw new IllegalArgumentException("newMember cannot be null.");
        }
        StreamsGroupMember oldMember = members.put(newMember.memberId(), newMember);
        maybeUpdateTaskProcessId(oldMember, newMember);
        updateStaticMember(newMember);
        maybeUpdateGroupState();
    }

    /**
     * Updates the member ID stored against the instance ID if the member is a static member.
     *
     * @param newMember The new member state.
     */
    private void updateStaticMember(StreamsGroupMember newMember) {
        if (newMember.instanceId() != null && newMember.instanceId().isPresent()) {
            staticMembers.put(newMember.instanceId().get(), newMember.memberId());
        }
    }

    /**
     * Remove the member from the group.
     *
     * @param memberId The member ID to remove.
     */
    public void removeMember(String memberId) {
        StreamsGroupMember oldMember = members.remove(memberId);
        maybeRemoveTaskProcessId(oldMember);
        removeStaticMember(oldMember);
        maybeUpdateGroupState();
    }

    /**
     * Remove the static member mapping if the removed member is static.
     *
     * @param oldMember The member to remove.
     */
    private void removeStaticMember(StreamsGroupMember oldMember) {
        if (oldMember.instanceId() != null && oldMember.instanceId().isPresent()) {
            staticMembers.remove(oldMember.instanceId().get());
        }
    }

    /**
     * Returns true if the member exists.
     *
     * @param memberId The member ID.
     * @return A boolean indicating whether the member exists or not.
     */
    public boolean hasMember(String memberId) {
        return members.containsKey(memberId);
    }

    /**
     * @return The number of members.
     */
    public int numMembers() {
        return members.size();
    }

    /**
     * @return An immutable map containing all the members keyed by their ID.
     */
    public Map<String, StreamsGroupMember> members() {
        return Collections.unmodifiableMap(members);
    }

    /**
     * @return An immutable map containing all the static members keyed by instance ID.
     */
    public Map<String, String> staticMembers() {
        return Collections.unmodifiableMap(staticMembers);
    }

    /**
     * Returns the target assignment of the member.
     *
     * @return The StreamsGroupMemberAssignment or an EMPTY one if it does not exist.
     */
    public TasksTuple targetAssignment(String memberId) {
        return targetAssignment.getOrDefault(memberId, TasksTuple.EMPTY);
    }

    /**
     * Updates the target assignment of a member.
     *
     * @param memberId            The member ID.
     * @param newTargetAssignment The new target assignment.
     */
    public void updateTargetAssignment(String memberId, TasksTuple newTargetAssignment) {
        targetAssignment.put(memberId, newTargetAssignment);
    }

    /**
     * Removes the target assignment of a member.
     *
     * @param memberId The member id.
     */
    public void removeTargetAssignment(String memberId) {
        targetAssignment.remove(memberId);
    }

    /**
     * @return An immutable map containing all the target assignment keyed by member ID.
     */
    public Map<String, TasksTuple> targetAssignment() {
        return Collections.unmodifiableMap(targetAssignment);
    }

    /**
     * Returns the current process ID of a task or null if the task does not have one.
     *
     * @param subtopologyId  The topic ID.
     * @param taskId         The task ID.
     * @return The process ID or null.
     */
    public String currentActiveTaskProcessId(
        String subtopologyId, int taskId
    ) {
        Map<Integer, String> tasks = currentActiveTaskToProcessId.get(subtopologyId);
        if (tasks == null) {
            return null;
        } else {
            return tasks.getOrDefault(taskId, null);
        }
    }

    /**
     * Returns the current process IDs of a task or empty set if the task does not have one.
     *
     * @param subtopologyId The topic ID.
     * @param taskId        The task ID.
     * @return The process IDs or empty set.
     */
    public Set<String> currentStandbyTaskProcessIds(
        String subtopologyId, int taskId
    ) {
        Map<Integer, Set<String>> tasks = currentStandbyTaskToProcessIds.get(subtopologyId);
        if (tasks == null) {
            return Collections.emptySet();
        } else {
            return tasks.getOrDefault(taskId, Collections.emptySet());
        }
    }

    /**
     * Returns the current process ID of a task or empty set if the task does not have one.
     *
     * @param subtopologyId The topic ID.
     * @param taskId        The process ID.
     * @return The member IDs or empty set.
     */
    public Set<String> currentWarmupTaskProcessIds(
        String subtopologyId, int taskId
    ) {
        Map<Integer, Set<String>> tasks = currentWarmupTaskToProcessIds.get(subtopologyId);
        if (tasks == null) {
            return Collections.emptySet();
        } else {
            return tasks.getOrDefault(taskId, Collections.emptySet());
        }
    }

    /**
     * @return An immutable map of partition metadata for each topic that are inputs for this streams group.
     */
    public Map<String, TopicMetadata> partitionMetadata() {
        return Collections.unmodifiableMap(partitionMetadata);
    }

    /**
     * Updates the partition metadata. This replaces the previous one.
     *
     * @param partitionMetadata The new partition metadata.
     */
    public void setPartitionMetadata(
        Map<String, TopicMetadata> partitionMetadata
    ) {
        this.partitionMetadata.clear();
        this.partitionMetadata.putAll(partitionMetadata);
        maybeUpdateConfiguredTopology();
        maybeUpdateGroupState();
    }

    /**
     * Computes the partition metadata based on the current topology and the current topics image.
     *
     * @param topicsImage The current metadata for all available topics.
     * @param topology    The current metadata for the Streams topology
     * @return An immutable map of partition metadata for each topic that the Streams topology is using (besides non-repartition sink topics)
     */
    public Map<String, TopicMetadata> computePartitionMetadata(
        TopicsImage topicsImage,
        StreamsTopology topology
    ) {
        Set<String> requiredTopicNames = topology.requiredTopics();

        // Create the topic metadata for each subscribed topic.
        Map<String, TopicMetadata> newPartitionMetadata = new HashMap<>(requiredTopicNames.size());

        requiredTopicNames.forEach(topicName -> {
            TopicImage topicImage = topicsImage.getTopic(topicName);
            if (topicImage != null) {
                newPartitionMetadata.put(topicName, new TopicMetadata(
                    topicImage.id(),
                    topicImage.name(),
                    topicImage.partitions().size())
                );
            }
        });

        return Collections.unmodifiableMap(newPartitionMetadata);
    }

    /**
     * Updates the metadata refresh deadline.
     *
     * @param deadlineMs The deadline in milliseconds.
     * @param groupEpoch The associated group epoch.
     */
    public void setMetadataRefreshDeadline(
        long deadlineMs,
        int groupEpoch
    ) {
        this.metadataRefreshDeadline = new DeadlineAndEpoch(deadlineMs, groupEpoch);
    }

    /**
     * Requests a metadata refresh.
     */
    public void requestMetadataRefresh() {
        this.metadataRefreshDeadline = DeadlineAndEpoch.EMPTY;
    }

    /**
     * Checks if a metadata refresh is required. A refresh is required in two cases: 1) The deadline is smaller or equal to the current
     * time; 2) The group epoch associated with the deadline is larger than the current group epoch. This means that the operations which
     * updated the deadline failed.
     *
     * @param currentTimeMs The current time in milliseconds.
     * @return A boolean indicating whether a refresh is required or not.
     */
    public boolean hasMetadataExpired(long currentTimeMs) {
        return currentTimeMs >= metadataRefreshDeadline.deadlineMs || groupEpoch() < metadataRefreshDeadline.epoch;
    }

    /**
     * @return The metadata refresh deadline.
     */
    public DeadlineAndEpoch metadataRefreshDeadline() {
        return metadataRefreshDeadline;
    }

    /**
     * Validates the OffsetCommit request.
     *
     * @param memberId          The member ID.
     * @param groupInstanceId   The group instance ID.
     * @param memberEpoch       The member epoch.
     * @param isTransactional   Whether the offset commit is transactional or not.
     * @param apiVersion        The api version.
     * @throws UnknownMemberIdException  If the member is not found.
     * @throws StaleMemberEpochException If the provided member epoch doesn't match the actual member epoch.
     */
    @Override
    public void validateOffsetCommit(
        String memberId,
        String groupInstanceId,
        int memberEpoch,
        boolean isTransactional,
        short apiVersion
    ) throws UnknownMemberIdException, StaleMemberEpochException {
        // When the member epoch is -1, the request comes from either the admin client
        // or a consumer which does not use the group management facility. In this case,
        // the request can commit offsets if the group is empty.
        if (memberEpoch < 0 && members().isEmpty()) return;

        // The TxnOffsetCommit API does not require the member ID, the generation ID and the group instance ID fields.
        // Hence, they are only validated if any of them is provided
        if (isTransactional && memberEpoch == JoinGroupRequest.UNKNOWN_GENERATION_ID &&
            memberId.equals(JoinGroupRequest.UNKNOWN_MEMBER_ID) && groupInstanceId == null)
            return;

        final StreamsGroupMember member = getOrMaybeCreateMember(memberId, false);

        // If the commit is not transactional and the member uses the new streams protocol (KIP-1071),
        // the member should be using the OffsetCommit API version >= 9.
        if (!isTransactional && apiVersion < 9) {
            throw new UnsupportedVersionException("OffsetCommit version 9 or above must be used " +
                "by members using the streams group protocol");
        }

        validateMemberEpoch(memberEpoch, member.memberEpoch());
    }

    /**
     * Validates the OffsetFetch request.
     *
     * @param memberId            The member ID for streams groups.
     * @param memberEpoch         The member epoch for streams groups.
     * @param lastCommittedOffset The last committed offsets in the timeline.
     */
    @Override
    public void validateOffsetFetch(
        String memberId,
        int memberEpoch,
        long lastCommittedOffset
    ) throws UnknownMemberIdException, StaleMemberEpochException {
        // When the member ID is null and the member epoch is -1, the request either comes
        // from the admin client or from a client which does not provide them. In this case,
        // the fetch request is accepted.
        if (memberId == null && memberEpoch < 0) {
            return;
        }

        final StreamsGroupMember member = members.get(memberId, lastCommittedOffset);
        if (member == null) {
            throw new UnknownMemberIdException(String.format("Member %s is not a member of group %s.",
                memberId, groupId));
        }
        validateMemberEpoch(memberEpoch, member.memberEpoch());
    }

    /**
     * Validates the OffsetDelete request.
     */
    @Override
    public void validateOffsetDelete() {
    }

    /**
     * Validates the DeleteGroups request.
     */
    @Override
    public void validateDeleteGroup() throws ApiException {
        if (state() != StreamsGroupState.EMPTY) {
            throw Errors.NON_EMPTY_GROUP.exception();
        }
    }

    @Override
    public boolean isSubscribedToTopic(String topic) {
        Optional<ConfiguredTopology> maybeConfiguredTopology = configuredTopology.get();
        if (maybeConfiguredTopology.isEmpty() || !maybeConfiguredTopology.get().isReady()) {
            return false;
        }
        for (ConfiguredSubtopology sub : maybeConfiguredTopology.get().subtopologies().orElse(new TreeMap<>()).values()) {
            if (sub.sourceTopics().contains(topic) || sub.repartitionSourceTopics().containsKey(topic)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Populates the list of records with tombstone(s) for deleting the group.
     *
     * @param records The list of records.
     */
    @Override
    public void createGroupTombstoneRecords(List<CoordinatorRecord> records) {
        members().forEach((memberId, member) ->
            records.add(StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentTombstoneRecord(groupId(), memberId))
        );

        members().forEach((memberId, member) ->
            records.add(StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentTombstoneRecord(groupId(), memberId))
        );
        records.add(StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentEpochTombstoneRecord(groupId()));

        members().forEach((memberId, member) ->
            records.add(StreamsCoordinatorRecordHelpers.newStreamsGroupMemberTombstoneRecord(groupId(), memberId))
        );

        records.add(StreamsCoordinatorRecordHelpers.newStreamsGroupPartitionMetadataTombstoneRecord(groupId()));
        records.add(StreamsCoordinatorRecordHelpers.newStreamsGroupEpochTombstoneRecord(groupId()));
        records.add(StreamsCoordinatorRecordHelpers.newStreamsGroupTopologyRecordTombstone(groupId()));
    }

    @Override
    public boolean isEmpty() {
        return state() == StreamsGroupState.EMPTY;
    }

    /**
     * See {@link org.apache.kafka.coordinator.group.OffsetExpirationCondition}
     *
     * @return The offset expiration condition for the group or Empty if no such condition exists.
     */
    @Override
    public Optional<OffsetExpirationCondition> offsetExpirationCondition() {
        return Optional.of(new OffsetExpirationConditionImpl(offsetAndMetadata -> offsetAndMetadata.commitTimestampMs));
    }

    @Override
    public boolean isInStates(Set<String> statesFilter, long committedOffset) {
        return statesFilter.contains(state.get(committedOffset).toLowerCaseString());
    }

    /**
     * Throws a StaleMemberEpochException if the received member epoch does not match the expected member epoch.
     */
    private void validateMemberEpoch(
        int receivedMemberEpoch,
        int expectedMemberEpoch
    ) throws StaleMemberEpochException {
        if (receivedMemberEpoch != expectedMemberEpoch) {
            throw new StaleMemberEpochException(String.format("The received member epoch %d does not match "
                + "the expected member epoch %d.", receivedMemberEpoch, expectedMemberEpoch));
        }
    }

    /**
     * Updates the current state of the group.
     */
    private void maybeUpdateGroupState() {
        StreamsGroupState newState = STABLE;
        if (members.isEmpty()) {
            newState = EMPTY;
        } else if (topology().isEmpty() || configuredTopology().isEmpty() || !configuredTopology().get().isReady()) {
            newState = NOT_READY;
        } else if (groupEpoch.get() > targetAssignmentEpoch.get()) {
            newState = ASSIGNING;
        } else {
            for (StreamsGroupMember member : members.values()) {
                if (!member.isReconciledTo(targetAssignmentEpoch.get())) {
                    newState = RECONCILING;
                    break;
                }
            }
        }

        state.set(newState);
    }

    private void maybeUpdateConfiguredTopology() {
        if (topology.get().isPresent()) {
            final StreamsTopology streamsTopology = topology.get().get();

            log.info("[GroupId {}] Configuring the topology {}", groupId, streamsTopology);
            this.configuredTopology.set(Optional.of(InternalTopicManager.configureTopics(logContext, streamsTopology, partitionMetadata)));

        } else {
            configuredTopology.set(Optional.empty());
        }
    }

    /**
     * Updates the tasks process IDs based on the old and the new member.
     *
     * @param oldMember The old member.
     * @param newMember The new member.
     */
    private void maybeUpdateTaskProcessId(
        StreamsGroupMember oldMember,
        StreamsGroupMember newMember
    ) {
        maybeRemoveTaskProcessId(oldMember);
        addTaskProcessId(
            newMember.assignedTasks(),
            newMember.processId()
        );
        addTaskProcessId(
            newMember.tasksPendingRevocation(),
            newMember.processId()
        );
    }

    /**
     * Removes the task process IDs for the provided member.
     *
     * @param oldMember The old member.
     */
    private void maybeRemoveTaskProcessId(
        StreamsGroupMember oldMember
    ) {
        if (oldMember != null) {
            removeTaskProcessIds(oldMember.assignedTasks(), oldMember.processId());
            removeTaskProcessIds(oldMember.tasksPendingRevocation(), oldMember.processId());
        }
    }

    void removeTaskProcessIds(
        TasksTuple tasks,
        String processId
    ) {
        if (tasks != null) {
            removeTaskProcessIds(tasks.activeTasks(), currentActiveTaskToProcessId, processId);
            removeTaskProcessIdsFromSet(tasks.standbyTasks(), currentStandbyTaskToProcessIds, processId);
            removeTaskProcessIdsFromSet(tasks.warmupTasks(), currentWarmupTaskToProcessIds, processId);
        }
    }

    /**
     * Removes the task process IDs based on the provided assignment.
     *
     * @param assignment    The assignment.
     * @param expectedProcessId The expected process ID.
     * @throws IllegalStateException if the process ID does not match the expected one. package-private for testing.
     */
    private void removeTaskProcessIds(
        Map<String, Set<Integer>> assignment,
        TimelineHashMap<String, TimelineHashMap<Integer, String>> currentTasksProcessId,
        String expectedProcessId
    ) {
        assignment.forEach((subtopologyId, assignedPartitions) -> {
            currentTasksProcessId.compute(subtopologyId, (__, partitionsOrNull) -> {
                if (partitionsOrNull != null) {
                    assignedPartitions.forEach(partitionId -> {
                        String prevValue = partitionsOrNull.remove(partitionId);
                        if (!Objects.equals(prevValue, expectedProcessId)) {
                            throw new IllegalStateException(
                                String.format("Cannot remove the process ID %s from task %s_%s because the partition is " +
                                    "still owned at a different process ID %s", expectedProcessId, subtopologyId, partitionId, prevValue));
                        }
                    });
                    if (partitionsOrNull.isEmpty()) {
                        return null;
                    } else {
                        return partitionsOrNull;
                    }
                } else {
                    throw new IllegalStateException(
                        String.format("Cannot remove the process ID %s from %s because it does not have any processId",
                            expectedProcessId, subtopologyId));
                }
            });
        });
    }

    /**
     * Removes the task process IDs based on the provided assignment.
     *
     * @param assignment    The assignment.
     * @param processIdToRemove The expected process ID.
     * @throws IllegalStateException if the process ID does not match the expected one. package-private for testing.
     */
    private void removeTaskProcessIdsFromSet(
        Map<String, Set<Integer>> assignment,
        TimelineHashMap<String, TimelineHashMap<Integer, Set<String>>> currentTasksProcessId,
        String processIdToRemove
    ) {
        assignment.forEach((subtopologyId, assignedPartitions) -> {
            currentTasksProcessId.compute(subtopologyId, (__, partitionsOrNull) -> {
                if (partitionsOrNull != null) {
                    assignedPartitions.forEach(partitionId -> {
                        if (!partitionsOrNull.get(partitionId).remove(processIdToRemove)) {
                            throw new IllegalStateException(
                                String.format("Cannot remove the process ID %s from task %s_%s because the task is " +
                                    "not owned by this process ID", processIdToRemove, subtopologyId, partitionId));
                        }
                    });
                    if (partitionsOrNull.isEmpty()) {
                        return null;
                    } else {
                        return partitionsOrNull;
                    }
                } else {
                    throw new IllegalStateException(
                        String.format("Cannot remove the process ID %s from %s because it does not have any process ID",
                            processIdToRemove, subtopologyId));
                }
            });
        });
    }

    /**
     * Adds the partition epoch based on the provided assignment.
     *
     * @param tasks     The assigned tasks.
     * @param processId The process ID.
     * @throws IllegalStateException if the partition already has an epoch assigned. package-private for testing.
     */
    void addTaskProcessId(
        TasksTuple tasks,
        String processId
    ) {
        if (tasks != null && processId != null) {
            addTaskProcessId(tasks.activeTasks(), processId, currentActiveTaskToProcessId);
            addTaskProcessIdToSet(tasks.standbyTasks(), processId, currentStandbyTaskToProcessIds);
            addTaskProcessIdToSet(tasks.warmupTasks(), processId, currentWarmupTaskToProcessIds);
        }
    }

    private void addTaskProcessId(
        Map<String, Set<Integer>> tasks,
        String processId,
        TimelineHashMap<String, TimelineHashMap<Integer, String>> currentTaskProcessId
    ) {
        tasks.forEach((subtopologyId, assignedTaskPartitions) -> {
            currentTaskProcessId.compute(subtopologyId, (__, partitionsOrNull) -> {
                if (partitionsOrNull == null) {
                    partitionsOrNull = new TimelineHashMap<>(snapshotRegistry, assignedTaskPartitions.size());
                }
                for (Integer partitionId : assignedTaskPartitions) {
                    String prevValue = partitionsOrNull.put(partitionId, processId);
                    if (prevValue != null) {
                        throw new IllegalStateException(
                            String.format("Cannot set the process ID of %s-%s to %s because the partition is " +
                                "still owned by process ID %s", subtopologyId, partitionId, processId, prevValue));
                    }
                }
                return partitionsOrNull;
            });
        });
    }

    private void addTaskProcessIdToSet(
        Map<String, Set<Integer>> tasks,
        String processId,
        TimelineHashMap<String, TimelineHashMap<Integer, Set<String>>> currentTaskProcessId
    ) {
        tasks.forEach((subtopologyId, assignedTaskPartitions) -> {
            currentTaskProcessId.compute(subtopologyId, (__, partitionsOrNull) -> {
                if (partitionsOrNull == null) {
                    partitionsOrNull = new TimelineHashMap<>(snapshotRegistry, assignedTaskPartitions.size());
                }
                for (Integer partitionId : assignedTaskPartitions) {
                    partitionsOrNull.computeIfAbsent(partitionId, ___ -> new HashSet<>()).add(processId);
                }
                return partitionsOrNull;
            });
        });
    }

    public StreamsGroupDescribeResponseData.DescribedGroup asDescribedGroup(
        long committedOffset
    ) {
        StreamsGroupDescribeResponseData.DescribedGroup describedGroup = new StreamsGroupDescribeResponseData.DescribedGroup()
            .setGroupId(groupId)
            .setGroupEpoch(groupEpoch.get(committedOffset))
            .setGroupState(state.get(committedOffset).toString())
            .setAssignmentEpoch(targetAssignmentEpoch.get(committedOffset))
            .setTopology(configuredTopology.get(committedOffset).map(ConfiguredTopology::asStreamsGroupDescribeTopology).orElse(null));
        members.entrySet(committedOffset).forEach(
            entry -> describedGroup.members().add(
                entry.getValue().asStreamsGroupDescribeMember(
                    targetAssignment.get(entry.getValue().memberId(), committedOffset)
                )
            )
        );
        return describedGroup;
    }

}
