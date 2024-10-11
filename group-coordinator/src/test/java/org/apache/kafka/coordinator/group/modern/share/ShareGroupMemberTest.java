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
import org.apache.kafka.common.message.ShareGroupDescribeResponseData;
import org.apache.kafka.coordinator.group.MetadataImageBuilder;
import org.apache.kafka.coordinator.group.generated.ShareGroupMemberMetadataValue;
import org.apache.kafka.image.MetadataImage;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkAssignment;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkTopicAssignment;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class ShareGroupMemberTest {

    @Test
    public void testNewMember() {
        Uuid topicId1 = Uuid.randomUuid();

        ShareGroupMember member = new ShareGroupMember.Builder("member-id")
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setRackId("rack-id")
            .setClientId("client-id")
            .setClientHost("hostname")
            .setSubscribedTopicNames(Arrays.asList("foo", "bar"))
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(topicId1, 1, 2, 3)))
            .build();

        assertEquals("member-id", member.memberId());
        assertEquals(10, member.memberEpoch());
        assertEquals(9, member.previousMemberEpoch());
        assertNull(member.instanceId());
        assertEquals("rack-id", member.rackId());
        assertEquals("client-id", member.clientId());
        assertEquals("hostname", member.clientHost());
        assertEquals(Set.of("bar", "foo"), member.subscribedTopicNames());
        assertEquals(mkAssignment(mkTopicAssignment(topicId1, 1, 2, 3)), member.assignedPartitions());
    }

    @Test
    public void testEquals() {
        Uuid topicId1 = Uuid.randomUuid();

        ShareGroupMember member1 = new ShareGroupMember.Builder("member-id")
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setRackId("rack-id")
            .setClientId("client-id")
            .setClientHost("hostname")
            .setSubscribedTopicNames(Arrays.asList("foo", "bar"))
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(topicId1, 1, 2, 3)))
            .build();

        ShareGroupMember member2 = new ShareGroupMember.Builder("member-id")
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setRackId("rack-id")
            .setClientId("client-id")
            .setClientHost("hostname")
            .setSubscribedTopicNames(Arrays.asList("foo", "bar"))
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(topicId1, 1, 2, 3)))
            .build();

        assertEquals(member1, member2);
    }

    @Test
    public void testUpdateMember() {
        Uuid topicId1 = Uuid.randomUuid();

        ShareGroupMember member = new ShareGroupMember.Builder("member-id")
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setRackId("rack-id")
            .setClientId("client-id")
            .setClientHost("hostname")
            .setSubscribedTopicNames(Arrays.asList("foo", "bar"))
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(topicId1, 1, 2, 3)))
            .build();

        // This is a no-op.
        ShareGroupMember updatedMember = new ShareGroupMember.Builder(member)
            .maybeUpdateRackId(Optional.empty())
            .maybeUpdateSubscribedTopicNames(Optional.empty())
            .build();

        assertEquals(member, updatedMember);

        updatedMember = new ShareGroupMember.Builder(member)
            .maybeUpdateRackId(Optional.of("new-rack-id"))
            .maybeUpdateSubscribedTopicNames(Optional.of(Collections.singletonList("zar")))
            .build();

        assertNull(member.instanceId());
        assertEquals("new-rack-id", updatedMember.rackId());
        // Names are sorted.
        assertEquals(Set.of("zar"), updatedMember.subscribedTopicNames());
    }

    @Test
    public void testUpdateWithShareGroupMemberMetadataValue() {
        ShareGroupMemberMetadataValue record = new ShareGroupMemberMetadataValue()
            .setClientId("client-id")
            .setClientHost("host-id")
            .setRackId("rack-id")
            .setSubscribedTopicNames(Arrays.asList("foo", "bar"));

        ShareGroupMember member = new ShareGroupMember.Builder("member-id")
            .updateWith(record)
            .build();

        assertNull(member.instanceId());
        assertEquals("rack-id", member.rackId());
        assertEquals("client-id", member.clientId());
        assertEquals("host-id", member.clientHost());
        assertEquals(Set.of("bar", "foo"), member.subscribedTopicNames());
    }

    @Test
    public void testAsShareGroupDescribeMember() {
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();
        MetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(topicId1, "topic1", 3)
            .addTopic(topicId2, "topic2", 3)
            .build();
        List<String> subscribedTopicNames = Arrays.asList("topic1", "topic2");
        List<Integer> assignedPartitions = Arrays.asList(0, 1, 2);
        int epoch = 10;
        ShareGroupMemberMetadataValue record = new ShareGroupMemberMetadataValue()
            .setClientId("client-id")
            .setClientHost("host-id")
            .setRackId("rack-id")
            .setSubscribedTopicNames(subscribedTopicNames);

        String memberId = Uuid.randomUuid().toString();
        ShareGroupMember member = new ShareGroupMember.Builder(memberId)
            .updateWith(record)
            .setMemberEpoch(epoch)
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(topicId1, 0, 1, 2)))
            .build();

        ShareGroupDescribeResponseData.Member actual = member.asShareGroupDescribeMember(metadataImage.topics());
        ShareGroupDescribeResponseData.Member expected = new ShareGroupDescribeResponseData.Member()
            .setMemberId(memberId)
            .setMemberEpoch(epoch)
            .setClientId("client-id")
            .setRackId("rack-id")
            .setClientHost("host-id")
            .setSubscribedTopicNames(subscribedTopicNames)
            .setAssignment(
                new ShareGroupDescribeResponseData.Assignment()
                    .setTopicPartitions(Collections.singletonList(new ShareGroupDescribeResponseData.TopicPartitions()
                        .setTopicId(topicId1)
                        .setTopicName("topic1")
                        .setPartitions(assignedPartitions)
                    ))
            );

        assertEquals(expected, actual);
    }
}
