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
import org.apache.kafka.coordinator.group.generated.StreamsGroupTopologyValue;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTopologyValue.Subtopology;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StreamsGroupBuilder {

    private final String groupId;
    private final int groupEpoch;
    private int targetAssignmentEpoch;
    private StreamsTopology topology;
    private final Map<String, StreamsGroupMember> members = new HashMap<>();
    private final Map<String, TasksTuple> targetAssignments = new HashMap<>();
    private long metadataHash = 0L;
    private int validatedTopologyEpoch = -1;

    public StreamsGroupBuilder(String groupId, int groupEpoch) {
        this.groupId = groupId;
        this.groupEpoch = groupEpoch;
        this.targetAssignmentEpoch = 0;
        this.topology = null;
    }

    public StreamsGroupBuilder withMember(StreamsGroupMember member) {
        this.members.put(member.memberId(), member);
        return this;
    }

    public StreamsGroupBuilder withMetadataHash(long metadataHash) {
        this.metadataHash = metadataHash;
        return this;
    }

    public StreamsGroupBuilder withValidatedTopologyEpoch(int validatedTopologyEpoch) {
        this.validatedTopologyEpoch = validatedTopologyEpoch;
        return this;
    }

    public StreamsGroupBuilder withTopology(StreamsTopology streamsTopology) {
        this.topology = streamsTopology;
        return this;
    }

    public StreamsGroupBuilder withTargetAssignment(String memberId, TasksTuple targetAssignment) {
        this.targetAssignments.put(memberId, targetAssignment);
        return this;
    }

    public StreamsGroupBuilder withTargetAssignmentEpoch(int targetAssignmentEpoch) {
        this.targetAssignmentEpoch = targetAssignmentEpoch;
        return this;
    }

    public List<CoordinatorRecord> build() {
        List<CoordinatorRecord> records = new ArrayList<>();

        // Add records for members.
        members.forEach((memberId, member) ->
            records.add(
                StreamsCoordinatorRecordHelpers.newStreamsGroupMemberRecord(groupId, member))
        );

        // Add group epoch record.
        records.add(
            StreamsCoordinatorRecordHelpers.newStreamsGroupMetadataRecord(groupId, groupEpoch, metadataHash, validatedTopologyEpoch));

        // Add target assignment records.
        targetAssignments.forEach((memberId, assignment) ->
            records.add(
                StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentRecord(groupId, memberId, assignment))
        );

        // Add topology record.
        if (topology != null) {
            records.add(StreamsCoordinatorRecordHelpers.newStreamsGroupTopologyRecord(
                groupId,
                    new StreamsGroupTopologyValue()
                        .setEpoch(topology.topologyEpoch())
                        .setSubtopologies(topology.subtopologies().values().stream().sorted(Comparator.comparing(Subtopology::subtopologyId)).toList()))
            );
        }

        // Add target assignment epoch.
        records.add(StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentEpochRecord(groupId,
            targetAssignmentEpoch));

        // Add current assignment records for members.
        members.forEach((memberId, member) ->
            records.add(
                StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentRecord(groupId, member))
        );

        return records;
    }

    public String groupId() {
        return groupId;
    }
}
