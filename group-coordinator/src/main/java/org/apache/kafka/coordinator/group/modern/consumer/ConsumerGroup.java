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
package org.apache.kafka.coordinator.group.modern.consumer;

import org.apache.kafka.clients.consumer.internals.ConsumerProtocol;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.IllegalGenerationException;
import org.apache.kafka.common.errors.StaleMemberEpochException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.ConsumerGroupDescribeResponseData;
import org.apache.kafka.common.message.ConsumerProtocolAssignment;
import org.apache.kafka.common.message.ConsumerProtocolSubscription;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.SchemaException;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpers;
import org.apache.kafka.coordinator.group.OffsetExpirationCondition;
import org.apache.kafka.coordinator.group.OffsetExpirationConditionImpl;
import org.apache.kafka.coordinator.group.Utils;
import org.apache.kafka.coordinator.group.api.assignor.SubscriptionType;
import org.apache.kafka.coordinator.group.classic.ClassicGroup;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMemberMetadataValue;
import org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetricsShard;
import org.apache.kafka.coordinator.group.modern.Assignment;
import org.apache.kafka.coordinator.group.modern.MemberState;
import org.apache.kafka.coordinator.group.modern.ModernGroup;
import org.apache.kafka.coordinator.group.modern.ModernGroupMember;
import org.apache.kafka.coordinator.group.modern.SubscriptionCount;
import org.apache.kafka.image.TopicsImage;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;
import org.apache.kafka.timeline.TimelineInteger;
import org.apache.kafka.timeline.TimelineObject;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static org.apache.kafka.coordinator.group.Utils.toOptional;
import static org.apache.kafka.coordinator.group.Utils.toTopicPartitionMap;
import static org.apache.kafka.coordinator.group.api.assignor.SubscriptionType.HETEROGENEOUS;
import static org.apache.kafka.coordinator.group.api.assignor.SubscriptionType.HOMOGENEOUS;
import static org.apache.kafka.coordinator.group.classic.ClassicGroupMember.EMPTY_ASSIGNMENT;
import static org.apache.kafka.coordinator.group.modern.consumer.ConsumerGroup.ConsumerGroupState.ASSIGNING;
import static org.apache.kafka.coordinator.group.modern.consumer.ConsumerGroup.ConsumerGroupState.EMPTY;
import static org.apache.kafka.coordinator.group.modern.consumer.ConsumerGroup.ConsumerGroupState.RECONCILING;
import static org.apache.kafka.coordinator.group.modern.consumer.ConsumerGroup.ConsumerGroupState.STABLE;
import static org.apache.kafka.coordinator.group.modern.consumer.ConsumerGroupMember.subscribedTopicRegexOrNull;

/**
 * A Consumer Group. All the metadata in this class are backed by
 * records in the __consumer_offsets partitions.
 */
public class ConsumerGroup extends ModernGroup<ConsumerGroupMember> {

    public enum ConsumerGroupState {
        EMPTY("Empty"),
        ASSIGNING("Assigning"),
        RECONCILING("Reconciling"),
        STABLE("Stable"),
        DEAD("Dead");

        private final String name;

        private final String lowerCaseName;

        ConsumerGroupState(String name) {
            this.name = name;
            this.lowerCaseName = name.toLowerCase(Locale.ROOT);
        }

        @Override
        public String toString() {
            return name;
        }

        public String toLowerCaseString() {
            return lowerCaseName;
        }
    }

    /**
     * The group state.
     */
    private final TimelineObject<ConsumerGroupState> state;

    /**
     * The static group members.
     */
    private final TimelineHashMap<String, String> staticMembers;

    /**
     * The number of members supporting each server assignor name.
     */
    private final TimelineHashMap<String, Integer> serverAssignors;

    /**
     * The coordinator metrics.
     */
    private final GroupCoordinatorMetricsShard metrics;

    /**
     * The number of members that use the classic protocol.
     */
    private final TimelineInteger numClassicProtocolMembers;

    /**
     * Map of protocol names to the number of members that use classic protocol and support them.
     */
    private final TimelineHashMap<String, Integer> classicProtocolMembersSupportedProtocols;

    /**
     * The current partition epoch maps each topic-partitions to their current epoch where
     * the epoch is the epoch of their owners. When a member revokes a partition, it removes
     * its epochs from this map. When a member gets a partition, it adds its epochs to this map.
     */
    private final TimelineHashMap<Uuid, TimelineHashMap<Integer, Integer>> currentPartitionEpoch;

    /**
     * The number of members subscribed to each regular expressions.
     */
    private final TimelineHashMap<String, Integer> subscribedRegularExpressions;

    /**
     * The resolved regular expressions.
     */
    private final TimelineHashMap<String, ResolvedRegularExpression> resolvedRegularExpressions;

    public ConsumerGroup(
        SnapshotRegistry snapshotRegistry,
        String groupId,
        GroupCoordinatorMetricsShard metrics
    ) {
        super(snapshotRegistry, groupId);
        this.state = new TimelineObject<>(snapshotRegistry, EMPTY);
        this.staticMembers = new TimelineHashMap<>(snapshotRegistry, 0);
        this.serverAssignors = new TimelineHashMap<>(snapshotRegistry, 0);
        this.metrics = Objects.requireNonNull(metrics);
        this.numClassicProtocolMembers = new TimelineInteger(snapshotRegistry);
        this.classicProtocolMembersSupportedProtocols = new TimelineHashMap<>(snapshotRegistry, 0);
        this.currentPartitionEpoch = new TimelineHashMap<>(snapshotRegistry, 0);
        this.subscribedRegularExpressions = new TimelineHashMap<>(snapshotRegistry, 0);
        this.resolvedRegularExpressions = new TimelineHashMap<>(snapshotRegistry, 0);
    }

    /**
     * @return The group type (Consumer).
     */
    @Override
    public GroupType type() {
        return GroupType.CONSUMER;
    }

    /**
     * @return The group protocol type (consumer).
     */
    @Override
    public String protocolType() {
        return ConsumerProtocol.PROTOCOL_TYPE;
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
     * @return The current state.
     */
    public ConsumerGroupState state() {
        return state.get();
    }

    /**
     * @return The current state based on committed offset.
     */
    public ConsumerGroupState state(long committedOffset) {
        return state.get(committedOffset);
    }

    /**
     * Sets the number of members using the classic protocol.
     *
     * @param numClassicProtocolMembers The new NumClassicProtocolMembers.
     */
    public void setNumClassicProtocolMembers(int numClassicProtocolMembers) {
        this.numClassicProtocolMembers.set(numClassicProtocolMembers);
    }

    /**
     * Get member id of a static member that matches the given group
     * instance id.
     *
     * @param groupInstanceId The group instance id.
     *
     * @return The member id corresponding to the given instance id or null if it does not exist
     */
    public String staticMemberId(String groupInstanceId) {
        if (groupInstanceId == null) return null;
        return staticMembers.get(groupInstanceId);
    }

    /**
     * Gets or creates a new member but without adding it to the group. Adding a member
     * is done via the {@link ConsumerGroup#updateMember(ConsumerGroupMember)} method.
     *
     * @param memberId          The member id.
     * @param createIfNotExists Booleans indicating whether the member must be
     *                          created if it does not exist.
     *
     * @return A ConsumerGroupMember.
     * @throws UnknownMemberIdException when the member does not exist and createIfNotExists is false.
     */
    public ConsumerGroupMember getOrMaybeCreateMember(
        String memberId,
        boolean createIfNotExists
    ) throws UnknownMemberIdException {
        ConsumerGroupMember member = members.get(memberId);
        if (member != null) return member;

        if (!createIfNotExists) {
            throw new UnknownMemberIdException(
                String.format("Member %s is not a member of group %s.", memberId, groupId)
            );
        }

        return new ConsumerGroupMember.Builder(memberId).build();
    }

    /**
     * Gets a static member.
     *
     * @param instanceId The group instance id.
     *
     * @return The member corresponding to the given instance id or null if it does not exist
     */
    public ConsumerGroupMember staticMember(String instanceId) {
        String existingMemberId = staticMemberId(instanceId);
        return existingMemberId == null ? null : getOrMaybeCreateMember(existingMemberId, false);
    }

    /**
     * Returns true if the static member exists.
     *
     * @param instanceId The instance id.
     *
     * @return A boolean indicating whether the member exists or not.
     */
    public boolean hasStaticMember(String instanceId) {
        if (instanceId == null) return false;
        return staticMembers.containsKey(instanceId);
    }

    /**
     * Returns the target assignment associated to the provided member id if
     * the instance id is null; otherwise returns the target assignment associated
     * to the instance id.
     *
     * @param memberId      The member id.
     * @param instanceId    The instance id.
     *
     * @return The Assignment or EMPTY if it does not exist.
     */
    public Assignment targetAssignment(String memberId, String instanceId) {
        if (instanceId == null) {
            return targetAssignment(memberId);
        } else {
            String previousMemberId = staticMemberId(instanceId);
            if (previousMemberId != null) {
                return targetAssignment(previousMemberId);
            }
        }
        return Assignment.EMPTY;
    }

    @Override
    public void updateMember(ConsumerGroupMember newMember) {
        if (newMember == null) {
            throw new IllegalArgumentException("newMember cannot be null.");
        }
        ConsumerGroupMember oldMember = members.put(newMember.memberId(), newMember);
        maybeUpdateSubscribedTopicNames(oldMember, newMember);
        maybeUpdateServerAssignors(oldMember, newMember);
        maybeUpdatePartitionEpoch(oldMember, newMember);
        maybeUpdateSubscribedRegularExpression(oldMember, newMember);
        updateStaticMember(newMember);
        maybeUpdateGroupState();
        maybeUpdateGroupSubscriptionType();
        maybeUpdateNumClassicProtocolMembers(oldMember, newMember);
        maybeUpdateClassicProtocolMembersSupportedProtocols(oldMember, newMember);
    }

    /**
     * Updates the member id stored against the instance id if the member is a static member.
     *
     * @param newMember The new member state.
     */
    private void updateStaticMember(ConsumerGroupMember newMember) {
        if (newMember.instanceId() != null) {
            staticMembers.put(newMember.instanceId(), newMember.memberId());
        }
    }

    @Override
    public void removeMember(String memberId) {
        ConsumerGroupMember oldMember = members.remove(memberId);
        maybeUpdateSubscribedTopicNames(oldMember, null);
        maybeUpdateServerAssignors(oldMember, null);
        maybeRemovePartitionEpoch(oldMember);
        maybeUpdateSubscribedRegularExpression(oldMember, null);
        removeStaticMember(oldMember);
        maybeUpdateGroupState();
        maybeUpdateGroupSubscriptionType();
        maybeUpdateNumClassicProtocolMembers(oldMember, null);
        maybeUpdateClassicProtocolMembersSupportedProtocols(oldMember, null);
    }

    /**
     * Remove the static member mapping if the removed member is static.
     *
     * @param oldMember The member to remove.
     */
    private void removeStaticMember(ConsumerGroupMember oldMember) {
        if (oldMember != null && oldMember.instanceId() != null) {
            staticMembers.remove(oldMember.instanceId());
        }
    }

    /**
     * Updates the subscription count.
     *
     * @param oldMember             The old member.
     * @param newMember             The new member.
     *
     * @return Copy of the map of topics to the count of number of subscribers.
     */
    public Map<String, SubscriptionCount> computeSubscribedTopicNames(
        ConsumerGroupMember oldMember,
        ConsumerGroupMember newMember
    ) {
        Map<String, SubscriptionCount> subscribedTopicsNames = super.computeSubscribedTopicNames(oldMember, newMember);
        String oldSubscribedTopicRegex = subscribedTopicRegexOrNull(oldMember);

        if (oldSubscribedTopicRegex != null) {
            String newSubscribedTopicRegex = subscribedTopicRegexOrNull(newMember);

            // If the old member was the last one subscribed to the regex and the new member
            // is not subscribed to it, we must remove it from the subscribed topic names.
            if (!oldSubscribedTopicRegex.equals(newSubscribedTopicRegex) && numSubscribedMembers(oldSubscribedTopicRegex) == 1) {
                resolvedRegularExpression(oldSubscribedTopicRegex).ifPresent(resolvedRegularExpression ->
                    resolvedRegularExpression.topics.forEach(topic -> subscribedTopicsNames.compute(topic, SubscriptionCount::decRegexCount))
                );
            }
        }

        return subscribedTopicsNames;
    }

    /**
     * Computes an updated version of the subscribed regular expressions based on
     * the new/old members.
     *
     * @param oldMember The old member.
     * @param newMember The new member.
     * @return An unmodifiable and updated copy of the map.
     */
    public Map<String, Integer> computeSubscribedRegularExpressions(
        ConsumerGroupMember oldMember,
        ConsumerGroupMember newMember
    ) {
        String oldRegex = subscribedTopicRegexOrNull(oldMember);
        String newRegex = subscribedTopicRegexOrNull(newMember);

        if (!Objects.equals(oldRegex, newRegex)) {
            Map<String, Integer> newSubscribedRegularExpressions = new HashMap<>(subscribedRegularExpressions);
            if (oldRegex != null) {
                newSubscribedRegularExpressions.compute(oldRegex, Utils::decValue);
            }
            if (newRegex != null) {
                newSubscribedRegularExpressions.compute(newRegex, Utils::incValue);
            }
            return Collections.unmodifiableMap(newSubscribedRegularExpressions);
        } else {
            return Collections.unmodifiableMap(subscribedRegularExpressions);
        }
    }

    /**
     * Computes an updated copy of the subscribed topic names without the provided
     * removed members and removed regular expressions.
     *
     * @param removedMembers    The set of removed members.
     * @param removedRegexes    The set of removed regular expressions.
     *
     * @return Copy of the map of topics to the count of number of subscribers.
     */
    public Map<String, SubscriptionCount> computeSubscribedTopicNamesWithoutDeletedMembers(
        Set<ConsumerGroupMember> removedMembers,
        Set<String> removedRegexes
    ) {
        Map<String, SubscriptionCount> subscribedTopicsNames = super.computeSubscribedTopicNames(removedMembers);

        removedRegexes.forEach(regex ->
            resolvedRegularExpression(regex).ifPresent(resolvedRegularExpression ->
                resolvedRegularExpression.topics.forEach(topic ->
                    subscribedTopicsNames.compute(topic, SubscriptionCount::decRegexCount)
                )
            )
        );

        return subscribedTopicsNames;
    }

    /**
     * Update the resolved regular expression.
     *
     * @param regex                         The regular expression.
     * @param newResolvedRegularExpression  The regular expression's metadata.
     */
    public void updateResolvedRegularExpression(
        String regex,
        ResolvedRegularExpression newResolvedRegularExpression
    ) {
        removeResolvedRegularExpression(regex);
        if (newResolvedRegularExpression != null) {
            resolvedRegularExpressions.put(regex, newResolvedRegularExpression);
            newResolvedRegularExpression.topics.forEach(topicName -> subscribedTopicNames.compute(topicName, SubscriptionCount::incRegexCount));
        }
    }

    /**
     * Remove the resolved regular expression.
     *
     * @param regex The regular expression.
     */
    public void removeResolvedRegularExpression(String regex) {
        ResolvedRegularExpression oldResolvedRegularExpression = resolvedRegularExpressions.remove(regex);
        if (oldResolvedRegularExpression != null) {
            oldResolvedRegularExpression.topics.forEach(topicName -> subscribedTopicNames.compute(topicName, SubscriptionCount::decRegexCount));
        }
    }

    /**
     * @return The last time resolved regular expressions were refreshed or Long.MIN_VALUE if
     * there are no resolved regular expression. Note that we use the timestamp of the first
     * entry as a proxy for all of them. They are always resolved together.
     */
    public long lastResolvedRegularExpressionRefreshTimeMs() {
        Iterator<ResolvedRegularExpression> iterator = resolvedRegularExpressions.values().iterator();
        if (iterator.hasNext()) {
            return iterator.next().timestamp;
        } else {
            return Long.MIN_VALUE;
        }
    }

    /**
     * @return The version of the regular expressions or Zero if there are no resolved regular expression.
     */
    public long lastResolvedRegularExpressionVersion() {
        Iterator<ResolvedRegularExpression> iterator = resolvedRegularExpressions.values().iterator();
        if (iterator.hasNext()) {
            return iterator.next().version;
        } else {
            return 0L;
        }
    }

    /**
     * Return an optional containing the resolved regular expression corresponding to the provided regex
     * or an empty optional.
     *
     * @param regex The regular expression.
     * @return The optional containing the resolved regular expression or an empty optional.
     */
    public Optional<ResolvedRegularExpression> resolvedRegularExpression(String regex) {
        return Optional.ofNullable(resolvedRegularExpressions.get(regex));
    }

    /**
     * @return The number of resolved regular expressions.
     */
    public int numResolvedRegularExpressions() {
        return resolvedRegularExpressions.size();
    }

    /**
     * @return The number of members subscribed to the provided regex.
     */
    public int numSubscribedMembers(String regex) {
        return subscribedRegularExpressions.getOrDefault(regex, 0);
    }

    /**
     * @return An immutable map containing all the subscribed regular expressions
     *         with the subscribers counts.
     */
    public Map<String, Integer> subscribedRegularExpressions() {
        return Collections.unmodifiableMap(subscribedRegularExpressions);
    }

    /**
     * @return The number of members that use the classic protocol.
     */
    public int numClassicProtocolMembers() {
        return numClassicProtocolMembers.get();
    }

    /**
     * @return The map of the protocol name and the number of members using the classic protocol that support it.
     */
    public Map<String, Integer> classicMembersSupportedProtocols() {
        return Collections.unmodifiableMap(classicProtocolMembersSupportedProtocols);
    }

    /**
     * @return An immutable Map containing all the static members keyed by instance id.
     */
    public Map<String, String> staticMembers() {
        return Collections.unmodifiableMap(staticMembers);
    }

    /**
     * @return An immutable Map containing all the resolved regular expressions.
     */
    public Map<String, ResolvedRegularExpression> resolvedRegularExpressions() {
        return Collections.unmodifiableMap(resolvedRegularExpressions);
    }

    /**
     * Returns the current epoch of a partition or -1 if the partition
     * does not have one.
     *
     * @param topicId       The topic id.
     * @param partitionId   The partition id.
     *
     * @return The epoch or -1.
     */
    public int currentPartitionEpoch(
        Uuid topicId,
        int partitionId
    ) {
        Map<Integer, Integer> partitions = currentPartitionEpoch.get(topicId);
        if (partitions == null) {
            return -1;
        } else {
            return partitions.getOrDefault(partitionId, -1);
        }
    }

    /**
     * Compute the preferred (server side) assignor for the group while
     * taking into account the updated member. The computation relies
     * on {{@link ConsumerGroup#serverAssignors}} persisted structure
     * but it does not update it.
     *
     * @param oldMember The old member.
     * @param newMember The new member.
     *
     * @return An Optional containing the preferred assignor.
     */
    public Optional<String> computePreferredServerAssignor(
        ConsumerGroupMember oldMember,
        ConsumerGroupMember newMember
    ) {
        // Copy the current count and update it.
        Map<String, Integer> counts = new HashMap<>(this.serverAssignors);
        maybeUpdateServerAssignors(counts, oldMember, newMember);

        return counts.entrySet().stream()
            .max(Map.Entry.comparingByValue())
            .map(Map.Entry::getKey);
    }

    /**
     * @return The preferred assignor for the group.
     */
    public Optional<String> preferredServerAssignor() {
        return preferredServerAssignor(Long.MAX_VALUE);
    }

    /**
     * @return The preferred assignor for the group with given offset.
     */
    public Optional<String> preferredServerAssignor(long committedOffset) {
        return serverAssignors.entrySet(committedOffset).stream()
            .max(Map.Entry.comparingByValue())
            .map(Map.Entry::getKey);
    }

    /**
     * Validates the OffsetCommit request.
     *
     * @param memberId          The member id.
     * @param groupInstanceId   The group instance id.
     * @param memberEpoch       The member epoch.
     * @param isTransactional   Whether the offset commit is transactional or not. It has no
     *                          impact when a consumer group is used.
     * @param apiVersion        The api version.
     * @throws UnknownMemberIdException     If the member is not found.
     * @throws StaleMemberEpochException    If the member uses the consumer protocol and the provided
     *                                      member epoch doesn't match the actual member epoch.
     * @throws IllegalGenerationException   If the member uses the classic protocol and the provided
     *                                      generation id is not equal to the member epoch.
     */
    @Override
    public void validateOffsetCommit(
        String memberId,
        String groupInstanceId,
        int memberEpoch,
        boolean isTransactional,
        short apiVersion
    ) throws UnknownMemberIdException, StaleMemberEpochException, IllegalGenerationException {
        // When the member epoch is -1, the request comes from either the admin client
        // or a consumer which does not use the group management facility. In this case,
        // the request can commit offsets if the group is empty.
        if (memberEpoch < 0 && members().isEmpty()) return;

        // The TxnOffsetCommit API does not require the member id, the generation id and the group instance id fields.
        // Hence, they are only validated if any of them is provided
        if (isTransactional && memberEpoch == JoinGroupRequest.UNKNOWN_GENERATION_ID &&
            memberId.equals(JoinGroupRequest.UNKNOWN_MEMBER_ID) && groupInstanceId == null)
            return;

        final ConsumerGroupMember member = getOrMaybeCreateMember(memberId, false);

        // If the commit is not transactional and the member uses the new consumer protocol (KIP-848),
        // the member should be using the OffsetCommit API version >= 9.
        if (!isTransactional && !member.useClassicProtocol() && apiVersion < 9) {
            throw new UnsupportedVersionException("OffsetCommit version 9 or above must be used " +
                "by members using the modern group protocol");
        }

        validateMemberEpoch(memberEpoch, member.memberEpoch(), member.useClassicProtocol());
    }

    /**
     * Validates the OffsetFetch request.
     *
     * @param memberId              The member id for consumer groups.
     * @param memberEpoch           The member epoch for consumer groups.
     * @param lastCommittedOffset   The last committed offsets in the timeline.
     * @throws UnknownMemberIdException     If the member is not found.
     * @throws StaleMemberEpochException    If the member uses the consumer protocol and the provided
     *                                      member epoch doesn't match the actual member epoch.
     * @throws IllegalGenerationException   If the member uses the classic protocol and the provided
     *                                      generation id is not equal to the member epoch.
     */
    @Override
    public void validateOffsetFetch(
        String memberId,
        int memberEpoch,
        long lastCommittedOffset
    ) throws UnknownMemberIdException, StaleMemberEpochException, IllegalGenerationException {
        // When the member id is null and the member epoch is -1, the request either comes
        // from the admin client or from a client which does not provide them. In this case,
        // the fetch request is accepted.
        if (memberId == null && memberEpoch < 0) return;

        final ConsumerGroupMember member = members.get(memberId, lastCommittedOffset);
        if (member == null) {
            throw new UnknownMemberIdException(String.format("Member %s is not a member of group %s.",
                memberId, groupId));
        }
        validateMemberEpoch(memberEpoch, member.memberEpoch(), member.useClassicProtocol());
    }

    /**
     * Validates the OffsetDelete request.
     */
    @Override
    public void validateOffsetDelete() {
        // Do nothing.
    }

    /**
     * Validates the DeleteGroups request.
     */
    @Override
    public void validateDeleteGroup() throws ApiException {
        if (state() != ConsumerGroupState.EMPTY) {
            throw Errors.NON_EMPTY_GROUP.exception();
        }
    }

    /**
     * Populates the list of records with tombstone(s) for deleting the group.
     *
     * @param records The list of records.
     */
    @Override
    public void createGroupTombstoneRecords(List<CoordinatorRecord> records) {
        members.keySet().forEach(memberId ->
            records.add(GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord(groupId, memberId))
        );

        members.keySet().forEach(memberId ->
            records.add(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentTombstoneRecord(groupId, memberId))
        );
        records.add(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentEpochTombstoneRecord(groupId));

        members.keySet().forEach(memberId ->
            records.add(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord(groupId, memberId))
        );

        resolvedRegularExpressions.keySet().forEach(regex ->
            records.add(GroupCoordinatorRecordHelpers.newConsumerGroupRegularExpressionTombstone(groupId, regex))
        );

        records.add(GroupCoordinatorRecordHelpers.newConsumerGroupSubscriptionMetadataTombstoneRecord(groupId));
        records.add(GroupCoordinatorRecordHelpers.newConsumerGroupEpochTombstoneRecord(groupId));
    }

    /**
     * Populates the list of records with tombstone(s) for deleting the group.
     * If the removed member is the leaving member, create its tombstone with
     * the joining member id.
     *
     * @param records           The list of records.
     * @param leavingMemberId   The leaving member id.
     * @param joiningMemberId   The joining member id.
     */
    public void createGroupTombstoneRecordsWithReplacedMember(
        List<CoordinatorRecord> records,
        String leavingMemberId,
        String joiningMemberId
    ) {
        members.keySet().forEach(memberId -> {
            String removedMemberId = memberId.equals(leavingMemberId) ? joiningMemberId : memberId;
            records.add(GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord(groupId, removedMemberId));
        });

        members.keySet().forEach(memberId -> {
            String removedMemberId = memberId.equals(leavingMemberId) ? joiningMemberId : memberId;
            records.add(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentTombstoneRecord(groupId, removedMemberId));
        });
        records.add(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentEpochTombstoneRecord(groupId));

        members.keySet().forEach(memberId -> {
            String removedMemberId = memberId.equals(leavingMemberId) ? joiningMemberId : memberId;
            records.add(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord(groupId, removedMemberId));
        });

        resolvedRegularExpressions.keySet().forEach(regex ->
            records.add(GroupCoordinatorRecordHelpers.newConsumerGroupRegularExpressionTombstone(groupId, regex))
        );

        records.add(GroupCoordinatorRecordHelpers.newConsumerGroupSubscriptionMetadataTombstoneRecord(groupId));
        records.add(GroupCoordinatorRecordHelpers.newConsumerGroupEpochTombstoneRecord(groupId));
    }

    @Override
    public boolean isEmpty() {
        return state() == ConsumerGroupState.EMPTY;
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
     * Throws an exception if the received member epoch does not match the expected member epoch.
     *
     * @param receivedMemberEpoch   The received member epoch or generation id.
     * @param expectedMemberEpoch   The expected member epoch.
     * @param useClassicProtocol    The boolean indicating whether the checked member uses the classic protocol.
     * @throws StaleMemberEpochException    if the member with unmatched member epoch uses the consumer protocol.
     * @throws IllegalGenerationException   if the member with unmatched generation id uses the classic protocol.
     */
    private void validateMemberEpoch(
        int receivedMemberEpoch,
        int expectedMemberEpoch,
        boolean useClassicProtocol
    ) throws StaleMemberEpochException, IllegalGenerationException {
        if (receivedMemberEpoch != expectedMemberEpoch) {
            if (useClassicProtocol) {
                throw new IllegalGenerationException(String.format("The received generation id %d does not match " +
                    "the expected member epoch %d.", receivedMemberEpoch, expectedMemberEpoch));
            } else {
                throw new StaleMemberEpochException(String.format("The received member epoch %d does not match "
                    + "the expected member epoch %d.", receivedMemberEpoch, expectedMemberEpoch));
            }
        }
    }

    /**
     * Computes the subscription type based on the provided information.
     *
     * @param subscribedRegularExpressions  The subscribed regular expression count.
     * @param subscribedTopicNames          The subscribed topic name count.
     * @param numberOfMembers               The number of members in the group.
     *
     * @return The subscription type.
     */
    public static SubscriptionType subscriptionType(
        Map<String, Integer> subscribedRegularExpressions,
        Map<String, SubscriptionCount> subscribedTopicNames,
        int numberOfMembers
    ) {
        if (subscribedRegularExpressions.isEmpty()) {
            // If the members do not use regular expressions, the subscription is
            // considered as homogeneous if all the members are subscribed to the
            // same topics. Otherwise, it is considered as heterogeneous.
            for (SubscriptionCount subscriberCount : subscribedTopicNames.values()) {
                if (subscriberCount.byNameCount != numberOfMembers) {
                    return HETEROGENEOUS;
                }
            }
            return HOMOGENEOUS;
        } else {
            int count = subscribedRegularExpressions.values().iterator().next();
            if (count == numberOfMembers) {
                // If all the members are subscribed to a single regular expressions
                // and none of them are subscribed to topic names, the subscription
                // is considered as homogeneous. If some members are subscribed to
                // topic names too, the subscription is considered as heterogeneous.
                for (SubscriptionCount subscriberCount : subscribedTopicNames.values()) {
                    if (subscriberCount.byRegexCount != 1 || subscriberCount.byNameCount > 0) {
                        return HETEROGENEOUS;
                    }
                }
                return HOMOGENEOUS;
            } else {
                // The subscription is considered as heterogeneous because
                // there is a mix of regular expressions.
                return SubscriptionType.HETEROGENEOUS;
            }
        }
    }

    @Override
    protected void maybeUpdateGroupSubscriptionType() {
        subscriptionType.set(subscriptionType(
            subscribedRegularExpressions,
            subscribedTopicNames,
            members.size()
        ));
    }

    @Override
    protected void maybeUpdateGroupState() {
        ConsumerGroupState previousState = state.get();
        ConsumerGroupState newState = STABLE;
        if (members.isEmpty()) {
            newState = EMPTY;
        } else if (groupEpoch.get() > targetAssignmentEpoch.get()) {
            newState = ASSIGNING;
        } else {
            for (ModernGroupMember member : members.values()) {
                if (!member.isReconciledTo(targetAssignmentEpoch.get())) {
                    newState = RECONCILING;
                    break;
                }
            }
        }

        state.set(newState);
        metrics.onConsumerGroupStateTransition(previousState, newState);
    }

    /**
     * Updates the server assignors count.
     *
     * @param oldMember The old member.
     * @param newMember The new member.
     */
    private void maybeUpdateServerAssignors(
        ConsumerGroupMember oldMember,
        ConsumerGroupMember newMember
    ) {
        maybeUpdateServerAssignors(serverAssignors, oldMember, newMember);
    }

    /**
     * Updates the server assignors count.
     *
     * @param serverAssignorCount   The count to update.
     * @param oldMember             The old member.
     * @param newMember             The new member.
     */
    private static void maybeUpdateServerAssignors(
        Map<String, Integer> serverAssignorCount,
        ConsumerGroupMember oldMember,
        ConsumerGroupMember newMember
    ) {
        if (oldMember != null) {
            oldMember.serverAssignorName().ifPresent(name ->
                serverAssignorCount.compute(name, Utils::decValue)
            );
        }
        if (newMember != null) {
            newMember.serverAssignorName().ifPresent(name ->
                serverAssignorCount.compute(name, Utils::incValue)
            );
        }
    }

    /**
     * Updates the number of the members that use the regular expression.
     *
     * @param oldMember The old member.
     * @param newMember The new member.
     */
    private void maybeUpdateSubscribedRegularExpression(
        ConsumerGroupMember oldMember,
        ConsumerGroupMember newMember
    ) {
        // Decrement the count of the old regex.
        if (oldMember != null && oldMember.subscribedTopicRegex() != null && !oldMember.subscribedTopicRegex().isEmpty()) {
            subscribedRegularExpressions.compute(oldMember.subscribedTopicRegex(), Utils::decValue);
        }
        // Increment the count of the new regex.
        if (newMember != null && newMember.subscribedTopicRegex() != null && !newMember.subscribedTopicRegex().isEmpty()) {
            subscribedRegularExpressions.compute(newMember.subscribedTopicRegex(), Utils::incValue);
        }
    }

    /**
     * Updates the number of the members that use the classic protocol.
     *
     * @param oldMember The old member.
     * @param newMember The new member.
     */
    private void maybeUpdateNumClassicProtocolMembers(
        ConsumerGroupMember oldMember,
        ConsumerGroupMember newMember
    ) {
        int delta = 0;
        if (oldMember != null && oldMember.useClassicProtocol()) {
            delta--;
        }
        if (newMember != null && newMember.useClassicProtocol()) {
            delta++;
        }
        setNumClassicProtocolMembers(numClassicProtocolMembers() + delta);
    }

    /**
     * Updates the supported protocol count of the members that use the classic protocol.
     *
     * @param oldMember The old member.
     * @param newMember The new member.
     */
    private void maybeUpdateClassicProtocolMembersSupportedProtocols(
        ConsumerGroupMember oldMember,
        ConsumerGroupMember newMember
    ) {
        if (oldMember != null) {
            oldMember.supportedClassicProtocols().ifPresent(protocols ->
                protocols.forEach(protocol ->
                    classicProtocolMembersSupportedProtocols.compute(protocol.name(), Utils::decValue)
                )
            );
        }
        if (newMember != null) {
            newMember.supportedClassicProtocols().ifPresent(protocols ->
                protocols.forEach(protocol ->
                    classicProtocolMembersSupportedProtocols.compute(protocol.name(), Utils::incValue)
                )
            );
        }
    }

    /**
     * Updates the partition epochs based on the old and the new member.
     *
     * @param oldMember The old member.
     * @param newMember The new member.
     */
    private void maybeUpdatePartitionEpoch(
        ConsumerGroupMember oldMember,
        ConsumerGroupMember newMember
    ) {
        maybeRemovePartitionEpoch(oldMember);
        addPartitionEpochs(newMember.assignedPartitions(), newMember.memberEpoch());
        addPartitionEpochs(newMember.partitionsPendingRevocation(), newMember.memberEpoch());
    }

    /**
     * Removes the partition epochs for the provided member.
     *
     * @param oldMember The old member.
     */
    private void maybeRemovePartitionEpoch(
        ConsumerGroupMember oldMember
    ) {
        if (oldMember != null) {
            removePartitionEpochs(oldMember.assignedPartitions(), oldMember.memberEpoch());
            removePartitionEpochs(oldMember.partitionsPendingRevocation(), oldMember.memberEpoch());
        }
    }

    /**
     * Removes the partition epochs based on the provided assignment.
     *
     * @param assignment    The assignment.
     * @param expectedEpoch The expected epoch.
     * @throws IllegalStateException if the epoch does not match the expected one.
     * package-private for testing.
     */
    void removePartitionEpochs(
        Map<Uuid, Set<Integer>> assignment,
        int expectedEpoch
    ) {
        assignment.forEach((topicId, assignedPartitions) -> {
            currentPartitionEpoch.compute(topicId, (__, partitionsOrNull) -> {
                if (partitionsOrNull != null) {
                    assignedPartitions.forEach(partitionId -> {
                        Integer prevValue = partitionsOrNull.remove(partitionId);
                        if (prevValue != expectedEpoch) {
                            throw new IllegalStateException(
                                String.format("Cannot remove the epoch %d from %s-%s because the partition is " +
                                    "still owned at a different epoch %d", expectedEpoch, topicId, partitionId, prevValue));
                        }
                    });
                    if (partitionsOrNull.isEmpty()) {
                        return null;
                    } else {
                        return partitionsOrNull;
                    }
                } else {
                    throw new IllegalStateException(
                        String.format("Cannot remove the epoch %d from %s because it does not have any epoch",
                            expectedEpoch, topicId));
                }
            });
        });
    }

    /**
     * Adds the partitions epoch based on the provided assignment.
     *
     * @param assignment    The assignment.
     * @param epoch         The new epoch.
     * @throws IllegalStateException if the partition already has an epoch assigned.
     * package-private for testing.
     */
    void addPartitionEpochs(
        Map<Uuid, Set<Integer>> assignment,
        int epoch
    ) {
        assignment.forEach((topicId, assignedPartitions) -> {
            currentPartitionEpoch.compute(topicId, (__, partitionsOrNull) -> {
                if (partitionsOrNull == null) {
                    partitionsOrNull = new TimelineHashMap<>(snapshotRegistry, assignedPartitions.size());
                }
                for (Integer partitionId : assignedPartitions) {
                    Integer prevValue = partitionsOrNull.put(partitionId, epoch);
                    if (prevValue != null) {
                        throw new IllegalStateException(
                            String.format("Cannot set the epoch of %s-%s to %d because the partition is " +
                                "still owned at epoch %d", topicId, partitionId, epoch, prevValue));
                    }
                }
                return partitionsOrNull;
            });
        });
    }

    public ConsumerGroupDescribeResponseData.DescribedGroup asDescribedGroup(
        long committedOffset,
        String defaultAssignor,
        TopicsImage topicsImage
    ) {
        ConsumerGroupDescribeResponseData.DescribedGroup describedGroup = new ConsumerGroupDescribeResponseData.DescribedGroup()
            .setGroupId(groupId)
            .setAssignorName(preferredServerAssignor(committedOffset).orElse(defaultAssignor))
            .setGroupEpoch(groupEpoch.get(committedOffset))
            .setGroupState(state.get(committedOffset).toString())
            .setAssignmentEpoch(targetAssignmentEpoch.get(committedOffset));
        members.entrySet(committedOffset).forEach(
            entry -> describedGroup.members().add(
                entry.getValue().asConsumerGroupDescribeMember(
                    targetAssignment.get(entry.getValue().memberId(), committedOffset),
                    topicsImage
                )
            )
        );
        return describedGroup;
    }

    /**
     * Create a new consumer group according to the given classic group.
     *
     * @param snapshotRegistry  The SnapshotRegistry.
     * @param metrics           The GroupCoordinatorMetricsShard.
     * @param classicGroup      The converted classic group.
     * @param topicsImage       The TopicsImage for topic id and topic name conversion.
     * @return  The created ConsumerGroup.
     *
     * @throws SchemaException if any member's subscription or assignment cannot be deserialized.
     * @throws UnsupportedVersionException if userData from a custom assignor would be lost.
     */
    public static ConsumerGroup fromClassicGroup(
        SnapshotRegistry snapshotRegistry,
        GroupCoordinatorMetricsShard metrics,
        ClassicGroup classicGroup,
        TopicsImage topicsImage
    ) {
        String groupId = classicGroup.groupId();
        ConsumerGroup consumerGroup = new ConsumerGroup(snapshotRegistry, groupId, metrics);
        consumerGroup.setGroupEpoch(classicGroup.generationId());
        consumerGroup.setTargetAssignmentEpoch(classicGroup.generationId());

        classicGroup.allMembers().forEach(classicGroupMember -> {
            // The assigned partition can be empty if the member just joined and has never synced.
            // We should accept the empty assignment.
            Map<Uuid, Set<Integer>> assignedPartitions;
            if (Arrays.equals(classicGroupMember.assignment(), EMPTY_ASSIGNMENT)) {
                assignedPartitions = Collections.emptyMap();
            } else {
                ConsumerProtocolAssignment assignment = ConsumerProtocol.deserializeConsumerProtocolAssignment(
                    ByteBuffer.wrap(classicGroupMember.assignment())
                );
                if (assignment.userData() != null && assignment.userData().hasRemaining()) {
                    throw new UnsupportedVersionException("userData from a custom assignor would be lost");
                }
                assignedPartitions = toTopicPartitionMap(assignment, topicsImage);
            }

            // Every member is guaranteed to have metadata set when it joins,
            // so we don't check for empty subscription here.
            ConsumerProtocolSubscription subscription = ConsumerProtocol.deserializeConsumerProtocolSubscription(
                ByteBuffer.wrap(classicGroupMember.metadata(classicGroup.protocolName().get()))
            );

            // The target assignment and the assigned partitions of each member are set based on the last
            // assignment of the classic group. All the members are put in the Stable state. If the classic
            // group was in Preparing Rebalance or Completing Rebalance states, the classic members are
            // asked to rejoin the group to re-trigger a rebalance or collect their assignments.
            ConsumerGroupMember newMember = new ConsumerGroupMember.Builder(classicGroupMember.memberId())
                .setMemberEpoch(classicGroup.generationId())
                .setState(MemberState.STABLE)
                .setPreviousMemberEpoch(classicGroup.generationId())
                .setInstanceId(classicGroupMember.groupInstanceId().orElse(null))
                .setRackId(toOptional(subscription.rackId()).orElse(null))
                .setRebalanceTimeoutMs(classicGroupMember.rebalanceTimeoutMs())
                .setClientId(classicGroupMember.clientId())
                .setClientHost(classicGroupMember.clientHost())
                .setSubscribedTopicNames(subscription.topics())
                .setAssignedPartitions(assignedPartitions)
                .setClassicMemberMetadata(
                    new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                        .setSessionTimeoutMs(classicGroupMember.sessionTimeoutMs())
                        .setSupportedProtocols(ConsumerGroupMember.classicProtocolListFromJoinRequestProtocolCollection(
                            classicGroupMember.supportedProtocols()
                        ))
                )
                .build();
            consumerGroup.updateTargetAssignment(newMember.memberId(), new Assignment(assignedPartitions));
            consumerGroup.updateMember(newMember);
        });

        return consumerGroup;
    }

    /**
     * Populate the record list with the records needed to create the given consumer group.
     *
     * @param records The list to which the new records are added.
     */
    public void createConsumerGroupRecords(
        List<CoordinatorRecord> records
    ) {
        members().forEach((__, consumerGroupMember) ->
            records.add(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId(), consumerGroupMember))
        );

        records.add(GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord(groupId(), groupEpoch()));

        members().forEach((consumerGroupMemberId, consumerGroupMember) ->
            records.add(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(
                groupId(),
                consumerGroupMemberId,
                targetAssignment(consumerGroupMemberId).partitions()
            ))
        );

        records.add(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentEpochRecord(groupId(), groupEpoch()));

        members().forEach((__, consumerGroupMember) ->
            records.add(GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord(groupId(), consumerGroupMember))
        );
    }

    /**
     * Checks whether at least one of the given protocols can be supported. A
     * protocol can be supported if it is supported by all members that use the
     * classic protocol.
     *
     * @param memberProtocolType  The member protocol type.
     * @param memberProtocols     The set of protocol names.
     *
     * @return A boolean based on the condition mentioned above.
     */
    public boolean supportsClassicProtocols(String memberProtocolType, Set<String> memberProtocols) {
        if (ConsumerProtocol.PROTOCOL_TYPE.equals(memberProtocolType)) {
            if (isEmpty()) {
                return !memberProtocols.isEmpty();
            } else {
                return memberProtocols.stream().anyMatch(
                    name -> classicProtocolMembersSupportedProtocols.getOrDefault(name, 0) == numClassicProtocolMembers()
                );
            }
        }
        return false;
    }

    /**
     * Checks whether all the members use the classic protocol except the given member.
     *
     * @param memberId The member to remove.
     * @return A boolean indicating whether all the members use the classic protocol.
     */
    public boolean allMembersUseClassicProtocolExcept(String memberId) {
        return numClassicProtocolMembers() == members().size() - 1 &&
            !getOrMaybeCreateMember(memberId, false).useClassicProtocol();
    }

    /**
     * Checks whether the member has any unreleased partition.
     *
     * @param member The member to check.
     * @return A boolean indicating whether the member has partitions in the target
     *         assignment that hasn't been revoked by other members.
     */
    public boolean waitingOnUnreleasedPartition(ConsumerGroupMember member) {
        if (member.state() == MemberState.UNRELEASED_PARTITIONS) {
            for (Map.Entry<Uuid, Set<Integer>> entry : targetAssignment().get(member.memberId()).partitions().entrySet()) {
                Uuid topicId = entry.getKey();
                Set<Integer> assignedPartitions = member.assignedPartitions().getOrDefault(topicId, Collections.emptySet());

                for (int partition : entry.getValue()) {
                    if (!assignedPartitions.contains(partition) && currentPartitionEpoch(topicId, partition) != -1) {
                        return true;
                    }
                }
            }
        }
        return false;
    }
}
