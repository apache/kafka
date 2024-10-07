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

import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StreamsGroupBuilder {

    private final String groupId;
    private final int groupEpoch;
    private int assignmentEpoch;
    private final Map<String, StreamsGroupMember> members = new HashMap<>();
    private final Map<String, Assignment> assignments = new HashMap<>();
    private Map<String, TopicMetadata> partitionMetadata = new HashMap<>();

    public StreamsGroupBuilder(String groupId, int groupEpoch) {
        this.groupId = groupId;
        this.groupEpoch = groupEpoch;
        this.assignmentEpoch = 0;
    }

    public StreamsGroupBuilder withMember(StreamsGroupMember member) {
        this.members.put(member.memberId(), member);
        return this;
    }

    public StreamsGroupBuilder withPartitionMetadata(
        Map<String, TopicMetadata> partitionMetadata) {
        this.partitionMetadata = partitionMetadata;
        return this;
    }

    public StreamsGroupBuilder withAssignment(String memberId, Assignment assignment) {
        this.assignments.put(memberId, assignment);
        return this;
    }

    public StreamsGroupBuilder withAssignmentEpoch(int assignmentEpoch) {
        this.assignmentEpoch = assignmentEpoch;
        return this;
    }

    public List<CoordinatorRecord> build() {
        List<CoordinatorRecord> records = new ArrayList<>();

        // Add records for members.
        members.forEach((memberId, member) ->
            records.add(
                CoordinatorStreamsRecordHelpers.newStreamsGroupMemberRecord(groupId, member))
        );

        if (!partitionMetadata.isEmpty()) {
            records.add(
                CoordinatorStreamsRecordHelpers.newStreamsGroupPartitionMetadataRecord(groupId,
                    partitionMetadata));
        }

        // Add group epoch record.
        records.add(
            CoordinatorStreamsRecordHelpers.newStreamsGroupEpochRecord(groupId, groupEpoch));

        // Add target assignment records.
        assignments.forEach((memberId, assignment) ->
            records.add(
                CoordinatorStreamsRecordHelpers.newStreamsGroupTargetAssignmentRecord(groupId, memberId,
                    assignment.activeTasks(), assignment.standbyTasks(), assignment.warmupTasks()))
        );

        // Add target assignment epoch.
        records.add(CoordinatorStreamsRecordHelpers.newStreamsGroupTargetAssignmentEpochRecord(groupId,
            assignmentEpoch));

        // Add current assignment records for members.
        members.forEach((memberId, member) ->
            records.add(
                CoordinatorStreamsRecordHelpers.newStreamsGroupCurrentAssignmentRecord(groupId, member))
        );

        return records;
    }
}
