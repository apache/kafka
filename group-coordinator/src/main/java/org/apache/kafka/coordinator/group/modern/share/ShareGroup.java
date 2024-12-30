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
package org.apache.kafka.coordinator.group.modern.share;

import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.message.ShareGroupDescribeResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpers;
import org.apache.kafka.coordinator.group.OffsetExpirationCondition;
import org.apache.kafka.coordinator.group.modern.ModernGroup;
import org.apache.kafka.image.TopicsImage;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineObject;

import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;

/**
 * A Share Group.
 */
public class ShareGroup extends ModernGroup<ShareGroupMember> {

    public static final String PROTOCOL_TYPE = "share";

    public enum ShareGroupState {
        EMPTY("Empty"),
        STABLE("Stable"),
        DEAD("Dead"),
        UNKNOWN("Unknown");

        private final String name;

        private final String lowerCaseName;

        ShareGroupState(String name) {
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
    private final TimelineObject<ShareGroupState> state;

    public ShareGroup(
        SnapshotRegistry snapshotRegistry,
        String groupId
    ) {
        super(snapshotRegistry, groupId);
        this.state = new TimelineObject<>(snapshotRegistry, ShareGroupState.EMPTY);
    }

    /**
     * @return The group type (Share).
     */
    @Override
    public GroupType type() {
        return GroupType.SHARE;
    }

    /**
     * @return The group protocol type (share).
     */
    @Override
    public String protocolType() {
        return PROTOCOL_TYPE;
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
    public ShareGroupState state() {
        return state.get();
    }

    /**
     * @return The current state based on committed offset.
     */
    public ShareGroupState state(long committedOffset) {
        return state.get(committedOffset);
    }

    /**
     * Gets or creates a member.
     *
     * @param memberId          The member id.
     * @param createIfNotExists Booleans indicating whether the member must be
     *                          created if it does not exist.
     *
     * @return A ShareGroupMember.
     * @throws UnknownMemberIdException when the member does not exist and createIfNotExists is false.
     */
    public ShareGroupMember getOrMaybeCreateMember(
        String memberId,
        boolean createIfNotExists
    ) throws UnknownMemberIdException {
        ShareGroupMember member = members.get(memberId);
        if (member != null) return member;

        if (!createIfNotExists) {
            throw new UnknownMemberIdException(
                String.format("Member %s is not a member of group %s.", memberId, groupId));
        }

        member = new ShareGroupMember.Builder(memberId).build();
        updateMember(member);
        return member;
    }

    /**
     * Updates the member.
     *
     * @param newMember The new share group member.
     */
    @Override
    public void updateMember(ShareGroupMember newMember) {
        if (newMember == null) {
            throw new IllegalArgumentException("newMember cannot be null.");
        }

        ShareGroupMember oldMember = members.put(newMember.memberId(), newMember);
        maybeUpdateSubscribedTopicNames(oldMember, newMember);
        maybeUpdateGroupState();
        maybeUpdateGroupSubscriptionType();
    }

    /**
     * Remove the member from the group.
     *
     * @param memberId The member id to remove.
     */
    public void removeMember(String memberId) {
        ShareGroupMember oldMember = members.remove(memberId);
        maybeUpdateSubscribedTopicNames(oldMember, null);
        maybeUpdateGroupState();
        maybeUpdateGroupSubscriptionType();
    }

    @Override
    public void validateOffsetCommit(
        String memberId,
        String groupInstanceId,
        int memberEpoch,
        boolean isTransactional,
        short apiVersion
    ) {
        throw new GroupIdNotFoundException(String.format("Group %s is not a consumer group.", groupId));
    }

    @Override
    public void validateOffsetFetch(
        String memberId,
        int memberEpoch,
        long lastCommittedOffset
    ) {
        throw new GroupIdNotFoundException(String.format("Group %s is not a consumer group.", groupId));
    }

    @Override
    public void validateOffsetDelete() {
        throw new GroupIdNotFoundException(String.format("Group %s is not a consumer group.", groupId));
    }

    /**
     * Validates the DeleteGroups request.
     */
    @Override
    public void validateDeleteGroup() throws ApiException {
        if (state() != ShareGroupState.EMPTY) {
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
        members().forEach((memberId, member) ->
            records.add(GroupCoordinatorRecordHelpers.newShareGroupCurrentAssignmentTombstoneRecord(groupId(), memberId))
        );

        members().forEach((memberId, member) ->
            records.add(GroupCoordinatorRecordHelpers.newShareGroupTargetAssignmentTombstoneRecord(groupId(), memberId))
        );
        records.add(GroupCoordinatorRecordHelpers.newShareGroupTargetAssignmentEpochTombstoneRecord(groupId()));

        members().forEach((memberId, member) ->
            records.add(GroupCoordinatorRecordHelpers.newShareGroupMemberSubscriptionTombstoneRecord(groupId(), memberId))
        );

        records.add(GroupCoordinatorRecordHelpers.newShareGroupSubscriptionMetadataTombstoneRecord(groupId()));
        records.add(GroupCoordinatorRecordHelpers.newShareGroupEpochTombstoneRecord(groupId()));
    }

    @Override
    public boolean isEmpty() {
        return state() == ShareGroupState.EMPTY;
    }

    @Override
    public Optional<OffsetExpirationCondition> offsetExpirationCondition() {
        throw new UnsupportedOperationException("offsetExpirationCondition is not supported for Share Groups.");
    }

    @Override
    public boolean isInStates(final Set<String> statesFilter, final long committedOffset) {
        return statesFilter.contains(state.get(committedOffset).toLowerCaseString());
    }

    /**
     * Updates the current state of the group.
     */
    @Override
    protected void maybeUpdateGroupState() {
        ShareGroupState newState = ShareGroupState.STABLE;
        if (members.isEmpty()) {
            newState = ShareGroupState.EMPTY;
        }

        state.set(newState);
    }

    public ShareGroupDescribeResponseData.DescribedGroup asDescribedGroup(
        long committedOffset,
        String defaultAssignor,
        TopicsImage topicsImage
    ) {
        ShareGroupDescribeResponseData.DescribedGroup describedGroup = new ShareGroupDescribeResponseData.DescribedGroup()
            .setGroupId(groupId)
            .setAssignorName(defaultAssignor)
            .setGroupEpoch(groupEpoch.get(committedOffset))
            .setGroupState(state.get(committedOffset).toString())
            .setAssignmentEpoch(targetAssignmentEpoch.get(committedOffset));
        members.entrySet(committedOffset).forEach(
            entry -> describedGroup.members().add(
                entry.getValue().asShareGroupDescribeMember(
                    topicsImage
                )
            )
        );
        return describedGroup;
    }
}
