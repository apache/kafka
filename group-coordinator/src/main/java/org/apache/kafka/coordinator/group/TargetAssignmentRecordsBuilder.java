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

import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.group.modern.Assignment;
import org.apache.kafka.coordinator.group.streams.StreamsCoordinatorRecordHelpers;
import org.apache.kafka.coordinator.group.streams.TasksTuple;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Builds the records to persist a new target assignment for a group.
 *
 * Records are only created for members which have a new target assignment. If
 * their assignment did not change, no new record is needed.
 *
 * When a member is deleted, it is assumed that its target assignment record
 * is deleted as part of the member deletion process. In other words, this class
 * does not yield a tombstone for removed members.
 *
 * @param <A> The member's target assignment type.
 */
public abstract class TargetAssignmentRecordsBuilder<A> {

    /**
     * The logger.
     */
    private final Logger log;

    /**
     * The group id.
     */
    private final String groupId;

    /**
     * The target assignment metadata.
     */
    private TargetAssignmentMetadata targetAssignmentMetadata;

    /**
     * The static members in the group at the time the new target assignment was computed.
     */
    private Map<String, String> previousStaticMembers = Map.of();

    /**
     * The current member ids in the group.
     */
    private Set<String> currentMemberIds;

    /**
     * The current static members in the group.
     */
    private Map<String, String> currentStaticMembers = Map.of();

    /**
     * The current target assignment.
     */
    private Map<String, A> currentTargetAssignment;

    /**
     * The new target assignment.
     */
    private Map<String, A> newTargetAssignment;

    /**
     * Constructs the object.
     *
     * @param log       The logger.
     * @param groupId   The group id.
     */
    public TargetAssignmentRecordsBuilder(
        Logger log,
        String groupId
    ) {
        this.log = log;
        this.groupId = Objects.requireNonNull(groupId);
    }

    /**
     * Sets the target assignment metadata.
     *
     * @param targetAssignmentMetadata The target assignment metadata.
     * @return This object.
     */
    public TargetAssignmentRecordsBuilder<A> withTargetAssignmentMetadata(TargetAssignmentMetadata targetAssignmentMetadata) {
        this.targetAssignmentMetadata = Objects.requireNonNull(targetAssignmentMetadata);
        return this;
    }

    /**
     * Sets the static members in the group at the time the new target assignment was computed.
     *
     * @param previousStaticMembers The static members in the group at the time the new target assignment was computed.
     * @return This object.
     */
    public TargetAssignmentRecordsBuilder<A> withPreviousStaticMembers(Map<String, String> previousStaticMembers) {
        this.previousStaticMembers = Objects.requireNonNull(previousStaticMembers);
        return this;
    }

    /**
     * Sets the current member ids in the group.
     *
     * @param currentMemberIds The current member ids in the group.
     * @return This object.
     */
    public TargetAssignmentRecordsBuilder<A> withCurrentMemberIds(Set<String> currentMemberIds) {
        this.currentMemberIds = Objects.requireNonNull(currentMemberIds);
        return this;
    }

    /**
     * Sets the current static members in the group.
     *
     * @param currentStaticMembers The current static members in the group.
     * @return This object.
     */
    public TargetAssignmentRecordsBuilder<A> withCurrentStaticMembers(Map<String, String> currentStaticMembers) {
        this.currentStaticMembers = Objects.requireNonNull(currentStaticMembers);
        return this;
    }

    /**
     * Sets the current target assignment.
     *
     * @param currentTargetAssignment The current target assignment.
     * @return This object.
     */
    public TargetAssignmentRecordsBuilder<A> withCurrentTargetAssignment(Map<String, A> currentTargetAssignment) {
        this.currentTargetAssignment = Objects.requireNonNull(currentTargetAssignment);
        return this;
    }

    /**
     * Sets the new target assignment.
     *
     * @param newTargetAssignment The new target assignment.
     * @return This object.
     */
    public TargetAssignmentRecordsBuilder<A> withNewTargetAssignment(Map<String, A> newTargetAssignment) {
        this.newTargetAssignment = Objects.requireNonNull(newTargetAssignment);
        return this;
    }

    /**
     * Builds the records for the new target assignment.
     *
     * @return The records for the new target assignment.
     */
    public List<CoordinatorRecord> build() {
        List<CoordinatorRecord> records = new ArrayList<>();
        build(records);
        return records;
    }

    /**
     * Builds the records for the new target assignment.
     *
     * @param records The list to accumulate records.
     */
    public void build(List<CoordinatorRecord> records) {
        // The members in the group may have changed while the target assignment was computed.
        // We want to act as if concurrent member operations such as leaves and static member
        // replacements happened *after* the target assignment was computed.
        //
        //  * When members leave the group, we tombstone their target assignment.
        //  * When static members are replaced, we move the assignment from the old member id to the
        //    new member id.
        //  * When members rejoin with the same member id but a different instance id, they keep
        //    their existing assignment. This operation will never happen when using the official
        //    Java client and may be forbidden in the future.
        //
        // Thus, when building the new target assignment records,
        //  * we should not emit records for members that have left the group
        //  * we should relabel records with the latest member id for static members
        //  * we should match up members using member ids first, then fall back to instance ids.

        // Build map of replacement member ids for static members that have churned.
        Map<String, String> staticMemberIdRemapping = new HashMap<>();
        for (Map.Entry<String, String> entry : previousStaticMembers.entrySet()) {
            String instanceId = entry.getKey();
            String oldMemberId = entry.getValue();
            String newMemberId = currentStaticMembers.get(instanceId);

            if (currentMemberIds.contains(oldMemberId)) {
                // The old member id is still in the group. We must not create a remapping entry,
                // otherwise we could give the same assignment to two different members.
                continue;
            }

            if (newTargetAssignment.containsKey(newMemberId)) {
                // The new member id has been in the group the whole time. Avoid creating a
                // remapping entry. Note that this is best effort, since newTargetAssignment may not
                // contain entries for every member that used to be in the group.
                continue;
            }

            if (newMemberId != null) {
                staticMemberIdRemapping.put(newMemberId, oldMemberId);
            }
        }

        if (log.isDebugEnabled()) {
            logStaticMembers();
            logMembersLeftAndJoined(staticMemberIdRemapping);
        }

        for (String memberId : currentMemberIds) {
            String previousMemberId = staticMemberIdRemapping.getOrDefault(memberId, memberId);

            A oldMemberAssignment = currentTargetAssignment.get(memberId);
            A newMemberAssignment = newTargetAssignment.get(previousMemberId);
            if (newMemberAssignment == null) {
                newMemberAssignment = emptyMemberAssignment();
            }

            if (oldMemberAssignment == null ||
                !newMemberAssignment.equals(oldMemberAssignment)) {
                // If the member had no assignment or had a different assignment, we
                // create a record for the new assignment.
                records.add(newTargetAssignmentRecord(
                    groupId,
                    memberId,
                    newMemberAssignment
                ));
            }
        }

        // Bump the target assignment epoch.
        records.add(newTargetAssignmentMetadataRecord(
            groupId,
            targetAssignmentMetadata.assignmentEpoch(),
            targetAssignmentMetadata.assignmentTimestamp()
        ));
    }

    private void logStaticMembers() {
        for (Map.Entry<String, String> entry : previousStaticMembers.entrySet()) {
            String instanceId = entry.getKey();
            String oldMemberId = entry.getValue();
            String newMemberId = currentStaticMembers.get(instanceId);

            if (currentMemberIds.contains(oldMemberId) ||
                newTargetAssignment.containsKey(newMemberId)) {
                // The member has been in the group the whole time.
                continue;
            }

            if (newMemberId == null) {
                log.debug("[GroupId {}] Previous static member {} with instance id {} has no replacement, discarding their target assignment.",
                    groupId, oldMemberId, instanceId);
            } else {
                log.debug("[GroupId {}] Previous static member {} with instance id {} has been replaced by {}, transferring target assignment.",
                    groupId, oldMemberId, instanceId, newMemberId);
            }
        }

        for (Map.Entry<String, String> entry : currentStaticMembers.entrySet()) {
            String instanceId = entry.getKey();
            String newMemberId = entry.getValue();

            if (newTargetAssignment.containsKey(newMemberId)) {
                // The member has been in the group the whole time.
                continue;
            }

            if (!previousStaticMembers.containsKey(instanceId)) {
                log.debug("[GroupId {}] Current static member {} with instance id {} has no previous static member and will receive an empty target assignment.",
                    groupId, newMemberId, instanceId);
            }
        }
    }

    private void logMembersLeftAndJoined(Map<String, String> staticMemberIdRemapping) {
        for (String memberId : newTargetAssignment.keySet()) {
            if (currentMemberIds.contains(memberId)) {
                // The member has been in the group the whole time.
                continue;
            }

            log.debug("[GroupId {}] Member {} has left the group, discarding their target assignment unless they were static and a corresponding static member exists.",
                groupId, memberId);
        }

        for (String memberId : currentMemberIds) {
            if (newTargetAssignment.containsKey(memberId)) {
                // The member has been in the group the whole time.
                continue;
            }

            if (!staticMemberIdRemapping.containsKey(memberId)) {
                log.debug("[GroupId {}] Member {} is new and will receive an empty target assignment or has no target assignment.",
                    groupId, memberId);
            }
        }
    }

    protected abstract A emptyMemberAssignment();

    protected abstract CoordinatorRecord newTargetAssignmentRecord(
        String groupId,
        String memberId,
        A memberAssignment
    );

    protected abstract CoordinatorRecord newTargetAssignmentMetadataRecord(
        String groupId,
        int assignmentEpoch,
        long assignmentTimestampMs
    );

    public static class ConsumerTargetAssignmentRecordsBuilder extends TargetAssignmentRecordsBuilder<Assignment> {

        public ConsumerTargetAssignmentRecordsBuilder(Logger log, String groupId) {
            super(log, groupId);
        }

        @Override
        protected Assignment emptyMemberAssignment() {
            return Assignment.EMPTY;
        }

        @Override
        protected CoordinatorRecord newTargetAssignmentRecord(
            String groupId,
            String memberId,
            Assignment memberAssignment
        ) {
            return GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord(
                groupId,
                memberId,
                memberAssignment.partitions()
            );
        }

        @Override
        protected CoordinatorRecord newTargetAssignmentMetadataRecord(
            String groupId,
            int assignmentEpoch,
            long assignmentTimestampMs
        ) {
            return GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentMetadataRecord(
                groupId,
                assignmentEpoch,
                assignmentTimestampMs
            );
        }
    }

    public static class ShareTargetAssignmentRecordsBuilder extends TargetAssignmentRecordsBuilder<Assignment> {

        public ShareTargetAssignmentRecordsBuilder(Logger log, String groupId) {
            super(log, groupId);
        }

        @Override
        protected Assignment emptyMemberAssignment() {
            return Assignment.EMPTY;
        }

        @Override
        protected CoordinatorRecord newTargetAssignmentRecord(
            String groupId,
            String memberId,
            Assignment memberAssignment
        ) {
            return GroupCoordinatorRecordHelpers.newShareGroupTargetAssignmentRecord(
                groupId,
                memberId,
                memberAssignment.partitions()
            );
        }

        @Override
        protected CoordinatorRecord newTargetAssignmentMetadataRecord(
            String groupId,
            int assignmentEpoch,
            long assignmentTimestampMs
        ) {
            return GroupCoordinatorRecordHelpers.newShareGroupTargetAssignmentMetadataRecord(
                groupId,
                assignmentEpoch,
                assignmentTimestampMs
            );
        }
    }

    public static class StreamsTargetAssignmentRecordsBuilder extends TargetAssignmentRecordsBuilder<TasksTuple> {

        public StreamsTargetAssignmentRecordsBuilder(Logger log, String groupId) {
            super(log, groupId);
        }

        @Override
        protected TasksTuple emptyMemberAssignment() {
            return TasksTuple.EMPTY;
        }

        @Override
        protected CoordinatorRecord newTargetAssignmentRecord(
            String groupId,
            String memberId,
            TasksTuple memberAssignment
        ) {
            return StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentRecord(
                groupId,
                memberId,
                memberAssignment
            );
        }

        @Override
        protected CoordinatorRecord newTargetAssignmentMetadataRecord(
            String groupId,
            int assignmentEpoch,
            long assignmentTimestampMs
        ) {
            return StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentMetadataRecord(
                groupId,
                assignmentEpoch,
                assignmentTimestampMs
            );
        }
    }
}
