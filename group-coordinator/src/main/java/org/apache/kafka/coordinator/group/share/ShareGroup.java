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
package org.apache.kafka.coordinator.group.share;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.message.ShareGroupDescribeResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.coordinator.group.AbstractGroup;
import org.apache.kafka.coordinator.group.GroupMember;
import org.apache.kafka.coordinator.group.Record;
import org.apache.kafka.coordinator.group.RecordHelpers;
import org.apache.kafka.image.TopicsImage;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;
import org.apache.kafka.timeline.TimelineObject;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.coordinator.group.share.ShareGroup.ShareGroupState.EMPTY;
import static org.apache.kafka.coordinator.group.share.ShareGroup.ShareGroupState.STABLE;

/**
 * A Share Group.
 */
public class ShareGroup extends AbstractGroup {

    public enum ShareGroupState {
        EMPTY("empty"),
        STABLE("stable"),
        DEAD("dead"),
        UNKNOWN("unknown");

        private final String name;

        ShareGroupState(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return name;
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
     * Gets or creates a member.
     *
     * @param memberId          The member id.
     * @param createIfNotExists Booleans indicating whether the member must be
     *                          created if it does not exist.
     *
     * @return A ShareGroupMember.
     */
    // TODO may be just make common group member method.
    public ShareGroupMember getOrMaybeCreateMember(
        String memberId,
        boolean createIfNotExists
    ) {
        ShareGroupMember member = (ShareGroupMember) members.get(memberId);
        if (member == null) {
          if (!createIfNotExists) {
            throw new UnknownMemberIdException(String.format("Member %s is not a member of group %s.",
                    memberId, groupId));
          }
          member = new ShareGroupMember.Builder(memberId).build();
          members.put(memberId, member);
        }

        return member;
    }

    /**
     * Updates the member.
     *
     * @param newMember The new member state.
     */
    public void updateMember(GroupMember newMember) {
        if (newMember == null) {
            throw new IllegalArgumentException("newMember cannot be null.");
        }
        if (!(newMember instanceof ShareGroupMember)) {
            throw new IllegalArgumentException("newMember must be an instance of ShareGroupMember.");
        }

        ShareGroupMember newShareGroupMember = (ShareGroupMember) newMember;
        ShareGroupMember oldMember = (ShareGroupMember) members.put(newMember.memberId(), newShareGroupMember);
        maybeUpdateSubscribedTopicNames(oldMember, newShareGroupMember);
        maybeUpdatePartitionEpoch(oldMember, newShareGroupMember);
        maybeUpdateGroupState();
    }

    /**
     * Remove the member from the group.
     *
     * @param memberId The member id to remove.
     */
    public void removeMember(String memberId) {
        ShareGroupMember oldMember = (ShareGroupMember) members.remove(memberId);
        maybeUpdateSubscribedTopicNames(oldMember, null);
        maybeRemovePartitionEpoch(oldMember);
        maybeUpdateGroupState();
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
    // TODO: records are less generated for share consumer hence we might want to manual things before
    //  replay.
    @Override
    public void createGroupTombstoneRecords(List<Record> records) {
        records.add(RecordHelpers.newGroupEpochTombstoneRecord(groupId()));
    }

    @Override
    public boolean isEmpty() {
        return state() == ShareGroupState.EMPTY;
    }

    @Override
    public boolean isInStates(final Set<String> statesFilter, final long committedOffset) {
        return false;
    }

    /**
     * Updates the current state of the group.
     */
    @Override
    protected void maybeUpdateGroupState() {
        ShareGroupState newState = STABLE;
        if (members.isEmpty()) {
            newState = EMPTY;
        }

        state.set(newState);
    }

    /**
     * Updates the partition epochs based on the old and the new member.
     *
     * @param oldMember The old member.
     * @param newMember The new member.
     */
    private void maybeUpdatePartitionEpoch(
            ShareGroupMember oldMember,
            ShareGroupMember newMember
    ) {
        maybeRemovePartitionEpoch(oldMember);
        addPartitionEpochs(newMember.assignedPartitions(), newMember.memberEpoch());
    }

    /**
     * Removes the partition epochs for the provided member.
     *
     * @param oldMember The old member.
     */
    private void maybeRemovePartitionEpoch(
        ShareGroupMember oldMember
    ) {
        if (oldMember != null) {
            removePartitionEpochs(oldMember.assignedPartitions());
        }
    }

    /**
     * Removes the partition epochs based on the provided assignment.
     *
     * @param assignment    The assignment.
     */
    private void removePartitionEpochs(
            Map<Uuid, Set<Integer>> assignment
    ) {
        assignment.forEach((topicId, assignedPartitions) -> {
            currentPartitionEpoch.compute(topicId, (__, partitionsOrNull) -> {
                if (partitionsOrNull != null) {
                    assignedPartitions.forEach(partitionsOrNull::remove);
                    if (partitionsOrNull.isEmpty()) {
                        return null;
                    } else {
                        return partitionsOrNull;
                    }
                } else {
                    return null;
                }
            });
        });
    }

    /**
     * Adds the partitions epoch based on the provided assignment.
     *
     * @param assignment    The assignment.
     * @param epoch         The new epoch.
     */
    private void addPartitionEpochs(
            Map<Uuid, Set<Integer>> assignment,
            int epoch
    ) {
        assignment.forEach((topicId, assignedPartitions) -> {
            currentPartitionEpoch.compute(topicId, (__, partitionsOrNull) -> {
                if (partitionsOrNull == null) {
                    partitionsOrNull = new TimelineHashMap<>(snapshotRegistry, assignedPartitions.size());
                }
                for (Integer partitionId : assignedPartitions) {
                    partitionsOrNull.put(partitionId, epoch);
                }
                return partitionsOrNull;
            });
        });
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
                ((ShareGroupMember) entry.getValue()).asShareGroupDescribeMember(
                    topicsImage
                )
            )
        );
        return describedGroup;
    }
}
