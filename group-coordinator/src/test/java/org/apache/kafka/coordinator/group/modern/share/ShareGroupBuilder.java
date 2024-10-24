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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpers;
import org.apache.kafka.coordinator.group.modern.Assignment;
import org.apache.kafka.image.TopicsImage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ShareGroupBuilder {
    private final String groupId;
    private final int groupEpoch;
    private int assignmentEpoch;
    private final Map<String, ShareGroupMember> members = new HashMap<>();
    private final Map<String, Assignment> assignments = new HashMap<>();
    private long subscriptionMetadataHash;

    public ShareGroupBuilder(String groupId, int groupEpoch) {
        this.groupId = groupId;
        this.groupEpoch = groupEpoch;
        this.assignmentEpoch = 0;
    }

    public ShareGroupBuilder withMember(ShareGroupMember member) {
        this.members.put(member.memberId(), member);
        return this;
    }

    public ShareGroupBuilder withSubscriptionMetadataHash(long subscriptionMetadataHash) {
        this.subscriptionMetadataHash = subscriptionMetadataHash;
        return this;
    }

    public ShareGroupBuilder withAssignment(String memberId, Map<Uuid, Set<Integer>> assignment) {
        this.assignments.put(memberId, new Assignment(assignment));
        return this;
    }

    public ShareGroupBuilder withAssignmentEpoch(int assignmentEpoch) {
        this.assignmentEpoch = assignmentEpoch;
        return this;
    }

    public List<CoordinatorRecord> build(TopicsImage topicsImage) {
        List<CoordinatorRecord> records = new ArrayList<>();

        // Add subscription records for members.
        members.forEach((memberId, member) ->
            records.add(GroupCoordinatorRecordHelpers.newShareGroupMemberSubscriptionRecord(groupId, member))
        );

        // Add group epoch record.
        records.add(GroupCoordinatorRecordHelpers.newShareGroupEpochRecord(groupId, groupEpoch, subscriptionMetadataHash));

        // Add target assignment records.
        assignments.forEach((memberId, assignment) ->
            records.add(GroupCoordinatorRecordHelpers.newShareGroupTargetAssignmentRecord(groupId, memberId, assignment.partitions()))
        );

        // Add target assignment epoch.
        records.add(GroupCoordinatorRecordHelpers.newShareGroupTargetAssignmentEpochRecord(groupId, assignmentEpoch));

        // Add current assignment records for members.
        members.forEach((memberId, member) ->
                records.add(GroupCoordinatorRecordHelpers.newShareGroupCurrentAssignmentRecord(groupId, member))
        );

        return records;
    }
}
