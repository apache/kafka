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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.coordinator.group.consumer.ClientAssignor;
import org.apache.kafka.coordinator.group.consumer.ConsumerGroupMember;
import org.apache.kafka.coordinator.group.consumer.TopicMetadata;
import org.apache.kafka.coordinator.group.consumer.VersionedMetadata;
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
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.coordinator.group.consumer.AssignmentTestUtil.mkSortedAssignment;
import static org.apache.kafka.coordinator.group.consumer.AssignmentTestUtil.mkSortedTopicAssignment;
import static org.apache.kafka.coordinator.group.consumer.AssignmentTestUtil.mkTopicAssignment;
import static org.apache.kafka.coordinator.group.RecordHelpers.newCurrentAssignmentRecord;
import static org.apache.kafka.coordinator.group.RecordHelpers.newCurrentAssignmentTombstoneRecord;
import static org.apache.kafka.coordinator.group.RecordHelpers.newGroupEpochRecord;
import static org.apache.kafka.coordinator.group.RecordHelpers.newGroupEpochTombstoneRecord;
import static org.apache.kafka.coordinator.group.RecordHelpers.newGroupSubscriptionMetadataRecord;
import static org.apache.kafka.coordinator.group.RecordHelpers.newGroupSubscriptionMetadataTombstoneRecord;
import static org.apache.kafka.coordinator.group.RecordHelpers.newMemberSubscriptionRecord;
import static org.apache.kafka.coordinator.group.RecordHelpers.newMemberSubscriptionTombstoneRecord;
import static org.apache.kafka.coordinator.group.RecordHelpers.newTargetAssignmentEpochRecord;
import static org.apache.kafka.coordinator.group.RecordHelpers.newTargetAssignmentEpochTombstoneRecord;
import static org.apache.kafka.coordinator.group.RecordHelpers.newTargetAssignmentRecord;
import static org.apache.kafka.coordinator.group.RecordHelpers.newTargetAssignmentTombstoneRecord;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class RecordHelpersTest {

    @Test
    public void testNewMemberSubscriptionRecord() {
        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member-id")
            .setInstanceId("instance-id")
            .setRackId("rack-id")
            .setRebalanceTimeoutMs(5000)
            .setClientId("client-id")
            .setClientHost("client-host")
            .setSubscribedTopicNames(Arrays.asList("foo", "zar", "bar"))
            .setSubscribedTopicRegex("regex")
            .setServerAssignorName("range")
            .setClientAssignors(Collections.singletonList(new ClientAssignor(
                "assignor",
                (byte) 0,
                (byte) 1,
                (byte) 10,
                new VersionedMetadata(
                    (byte) 5,
                    ByteBuffer.wrap("hello".getBytes(StandardCharsets.UTF_8))))))
            .build();

        Record expectedRecord = new Record(
            new ApiMessageAndVersion(
                new ConsumerGroupMemberMetadataKey()
                    .setGroupId("group-id")
                    .setMemberId("member-id"),
                (short) 5),
            new ApiMessageAndVersion(
                new ConsumerGroupMemberMetadataValue()
                    .setInstanceId("instance-id")
                    .setRackId("rack-id")
                    .setRebalanceTimeoutMs(5000)
                    .setClientId("client-id")
                    .setClientHost("client-host")
                    .setSubscribedTopicNames(Arrays.asList("bar", "foo", "zar"))
                    .setSubscribedTopicRegex("regex")
                    .setServerAssignor("range")
                    .setAssignors(Collections.singletonList(new ConsumerGroupMemberMetadataValue.Assignor()
                        .setName("assignor")
                        .setMinimumVersion((short) 1)
                        .setMaximumVersion((short) 10)
                        .setVersion((short) 5)
                        .setMetadata("hello".getBytes(StandardCharsets.UTF_8)))),
                (short) 0));

        assertEquals(expectedRecord, newMemberSubscriptionRecord(
            "group-id",
            member
        ));
    }

    @Test
    public void testNewMemberSubscriptionTombstoneRecord() {
        Record expectedRecord = new Record(
            new ApiMessageAndVersion(
                new ConsumerGroupMemberMetadataKey()
                    .setGroupId("group-id")
                    .setMemberId("member-id"),
                (short) 5
            ),
            null);

        assertEquals(expectedRecord, newMemberSubscriptionTombstoneRecord(
            "group-id",
            "member-id"
        ));
    }

    @Test
    public void testNewGroupSubscriptionMetadataRecord() {
        Uuid fooTopicId = Uuid.randomUuid();
        Uuid barTopicId = Uuid.randomUuid();
        Map<String, TopicMetadata> subscriptionMetadata = new LinkedHashMap<>();
        subscriptionMetadata.put("foo", new TopicMetadata(
            fooTopicId,
            "foo",
            10
        ));
        subscriptionMetadata.put("bar", new TopicMetadata(
            barTopicId,
            "bar",
            20
        ));

        Record expectedRecord = new Record(
            new ApiMessageAndVersion(
                new ConsumerGroupPartitionMetadataKey()
                    .setGroupId("group-id"),
                (short) 4
            ),
            new ApiMessageAndVersion(
                new ConsumerGroupPartitionMetadataValue()
                    .setTopics(Arrays.asList(
                        new ConsumerGroupPartitionMetadataValue.TopicMetadata()
                            .setTopicId(fooTopicId)
                            .setTopicName("foo")
                            .setNumPartitions(10),
                        new ConsumerGroupPartitionMetadataValue.TopicMetadata()
                            .setTopicId(barTopicId)
                            .setTopicName("bar")
                            .setNumPartitions(20))),
                (short) 0));

        assertEquals(expectedRecord, newGroupSubscriptionMetadataRecord(
            "group-id",
            subscriptionMetadata
        ));
    }

    @Test
    public void testNewGroupSubscriptionMetadataTombstoneRecord() {
        Record expectedRecord = new Record(
            new ApiMessageAndVersion(
                new ConsumerGroupPartitionMetadataKey()
                    .setGroupId("group-id"),
                (short) 4
            ),
            null);

        assertEquals(expectedRecord, newGroupSubscriptionMetadataTombstoneRecord(
            "group-id"
        ));
    }

    @Test
    public void testNewGroupEpochRecord() {
        Record expectedRecord = new Record(
            new ApiMessageAndVersion(
                new ConsumerGroupMetadataKey()
                    .setGroupId("group-id"),
                (short) 3),
            new ApiMessageAndVersion(
                new ConsumerGroupMetadataValue()
                    .setEpoch(10),
                (short) 0));

        assertEquals(expectedRecord, newGroupEpochRecord(
            "group-id",
            10
        ));
    }

    @Test
    public void testNewGroupEpochTombstoneRecord() {
        Record expectedRecord = new Record(
            new ApiMessageAndVersion(
                new ConsumerGroupMetadataKey()
                    .setGroupId("group-id"),
                (short) 3),
            null);

        assertEquals(expectedRecord, newGroupEpochTombstoneRecord(
            "group-id"
        ));
    }

    @Test
    public void testNewTargetAssignmentRecord() {
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        Map<Uuid, Set<Integer>> partitions = mkSortedAssignment(
            mkTopicAssignment(topicId1, 11, 12, 13),
            mkTopicAssignment(topicId2, 21, 22, 23)
        );

        Record expectedRecord = new Record(
            new ApiMessageAndVersion(
                new ConsumerGroupTargetAssignmentMemberKey()
                    .setGroupId("group-id")
                    .setMemberId("member-id"),
                (short) 7),
            new ApiMessageAndVersion(
                new ConsumerGroupTargetAssignmentMemberValue()
                    .setTopicPartitions(Arrays.asList(
                        new ConsumerGroupTargetAssignmentMemberValue.TopicPartition()
                            .setTopicId(topicId1)
                            .setPartitions(Arrays.asList(11, 12, 13)),
                        new ConsumerGroupTargetAssignmentMemberValue.TopicPartition()
                            .setTopicId(topicId2)
                            .setPartitions(Arrays.asList(21, 22, 23)))),
                (short) 0));

        assertEquals(expectedRecord, newTargetAssignmentRecord(
            "group-id",
            "member-id",
            partitions
        ));
    }

    @Test
    public void testNewTargetAssignmentTombstoneRecord() {
        Record expectedRecord = new Record(
            new ApiMessageAndVersion(
                new ConsumerGroupTargetAssignmentMemberKey()
                    .setGroupId("group-id")
                    .setMemberId("member-id"),
                (short) 7),
            null);

        assertEquals(expectedRecord, newTargetAssignmentTombstoneRecord(
            "group-id",
            "member-id"
        ));
    }

    @Test
    public void testNewTargetAssignmentEpochRecord() {
        Record expectedRecord = new Record(
            new ApiMessageAndVersion(
                new ConsumerGroupTargetAssignmentMetadataKey()
                    .setGroupId("group-id"),
                (short) 6),
            new ApiMessageAndVersion(
                new ConsumerGroupTargetAssignmentMetadataValue()
                    .setAssignmentEpoch(10),
                (short) 0));

        assertEquals(expectedRecord, newTargetAssignmentEpochRecord(
            "group-id",
            10
        ));
    }

    @Test
    public void testNewTargetAssignmentEpochTombstoneRecord() {
        Record expectedRecord = new Record(
            new ApiMessageAndVersion(
                new ConsumerGroupTargetAssignmentMetadataKey()
                    .setGroupId("group-id"),
                (short) 6),
            null);

        assertEquals(expectedRecord, newTargetAssignmentEpochTombstoneRecord(
            "group-id"
        ));
    }

    @Test
    public void testNewCurrentAssignmentRecord() {
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        Map<Uuid, Set<Integer>> assigned = mkSortedAssignment(
            mkSortedTopicAssignment(topicId1, 11, 12, 13),
            mkSortedTopicAssignment(topicId2, 21, 22, 23)
        );

        Map<Uuid, Set<Integer>> revoking = mkSortedAssignment(
            mkSortedTopicAssignment(topicId1, 14, 15, 16),
            mkSortedTopicAssignment(topicId2, 24, 25, 26)
        );

        Map<Uuid, Set<Integer>> assigning = mkSortedAssignment(
            mkSortedTopicAssignment(topicId1, 17, 18, 19),
            mkSortedTopicAssignment(topicId2, 27, 28, 29)
        );

        Record expectedRecord = new Record(
            new ApiMessageAndVersion(
                new ConsumerGroupCurrentMemberAssignmentKey()
                    .setGroupId("group-id")
                    .setMemberId("member-id"),
                (short) 8),
            new ApiMessageAndVersion(
                new ConsumerGroupCurrentMemberAssignmentValue()
                    .setMemberEpoch(22)
                    .setPreviousMemberEpoch(21)
                    .setTargetMemberEpoch(23)
                    .setAssignedPartitions(Arrays.asList(
                        new ConsumerGroupCurrentMemberAssignmentValue.TopicPartitions()
                            .setTopicId(topicId1)
                            .setPartitions(Arrays.asList(11, 12, 13)),
                        new ConsumerGroupCurrentMemberAssignmentValue.TopicPartitions()
                            .setTopicId(topicId2)
                            .setPartitions(Arrays.asList(21, 22, 23))))
                    .setPartitionsPendingRevocation(Arrays.asList(
                        new ConsumerGroupCurrentMemberAssignmentValue.TopicPartitions()
                            .setTopicId(topicId1)
                            .setPartitions(Arrays.asList(14, 15, 16)),
                        new ConsumerGroupCurrentMemberAssignmentValue.TopicPartitions()
                            .setTopicId(topicId2)
                            .setPartitions(Arrays.asList(24, 25, 26))))
                    .setPartitionsPendingAssignment(Arrays.asList(
                        new ConsumerGroupCurrentMemberAssignmentValue.TopicPartitions()
                            .setTopicId(topicId1)
                            .setPartitions(Arrays.asList(17, 18, 19)),
                        new ConsumerGroupCurrentMemberAssignmentValue.TopicPartitions()
                            .setTopicId(topicId2)
                            .setPartitions(Arrays.asList(27, 28, 29)))),
                (short) 0));

        assertEquals(expectedRecord, newCurrentAssignmentRecord(
            "group-id",
            new ConsumerGroupMember.Builder("member-id")
                .setMemberEpoch(22)
                .setPreviousMemberEpoch(21)
                .setNextMemberEpoch(23)
                .setAssignedPartitions(assigned)
                .setPartitionsPendingRevocation(revoking)
                .setPartitionsPendingAssignment(assigning)
                .build()
        ));
    }

    @Test
    public void testNewCurrentAssignmentTombstoneRecord() {
        Record expectedRecord = new Record(
            new ApiMessageAndVersion(
                new ConsumerGroupCurrentMemberAssignmentKey()
                    .setGroupId("group-id")
                    .setMemberId("member-id"),
                (short) 8),
            null);

        assertEquals(expectedRecord, newCurrentAssignmentTombstoneRecord(
            "group-id",
            "member-id"
        ));
    }
}
