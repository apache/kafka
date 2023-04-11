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
package org.apache.kafka.coordinator.group.consumer;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.coordinator.group.GroupMetadataManagerTest;
import org.apache.kafka.image.TopicsImage;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.apache.kafka.coordinator.group.consumer.AssignmentTestUtil.mkAssignment;
import static org.apache.kafka.coordinator.group.consumer.AssignmentTestUtil.mkTopicAssignment;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConsumerGroupTest {

    private ConsumerGroup createConsumerGroup(String groupId) {
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(new LogContext());
        return new ConsumerGroup(snapshotRegistry, groupId);
    }

    @Test
    public void testGetOrCreateMember() {
        ConsumerGroup consumerGroup = createConsumerGroup("foo");
        ConsumerGroupMember member;

        // Create a group.
        member = consumerGroup.getOrMaybeCreateMember("member-id", true);
        assertEquals("member-id", member.memberId());

        // Get that group back.
        member = consumerGroup.getOrMaybeCreateMember("member-id", false);
        assertEquals("member-id", member.memberId());

        assertThrows(UnknownMemberIdException.class, () ->
            consumerGroup.getOrMaybeCreateMember("does-not-exist", false));
    }

    @Test
    public void testUpdateMember() {
        ConsumerGroup consumerGroup = createConsumerGroup("foo");
        ConsumerGroupMember member;

        member = consumerGroup.getOrMaybeCreateMember("member", true);

        member = new ConsumerGroupMember.Builder(member)
            .setSubscribedTopicNames(Arrays.asList("foo", "bar"))
            .build();

        consumerGroup.updateMember(member);

        assertEquals(member, consumerGroup.getOrMaybeCreateMember("member", false));
    }

    @Test
    public void testRemoveMember() {
        ConsumerGroup consumerGroup = createConsumerGroup("foo");

        consumerGroup.getOrMaybeCreateMember("member", true);
        assertTrue(consumerGroup.hasMember("member"));

        consumerGroup.removeMember("member");
        assertFalse(consumerGroup.hasMember("member"));

    }

    @Test
    public void testUpdatingMemberUpdatesPartitionEpoch() {
        Uuid fooTopicId = Uuid.randomUuid();
        Uuid barTopicId = Uuid.randomUuid();
        Uuid zarTopicId = Uuid.randomUuid();

        ConsumerGroup consumerGroup = createConsumerGroup("foo");
        ConsumerGroupMember member;

        member = new ConsumerGroupMember.Builder("member")
            .setMemberEpoch(10)
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(fooTopicId, 1, 2, 3)))
            .setPartitionsPendingRevocation(mkAssignment(
                mkTopicAssignment(barTopicId, 4, 5, 6)))
            .setPartitionsPendingAssignment(mkAssignment(
                mkTopicAssignment(zarTopicId, 7, 8, 9)))
            .build();

        consumerGroup.updateMember(member);

        assertEquals(10, consumerGroup.currentPartitionEpoch(fooTopicId, 1));
        assertEquals(10, consumerGroup.currentPartitionEpoch(fooTopicId, 2));
        assertEquals(10, consumerGroup.currentPartitionEpoch(fooTopicId, 3));
        assertEquals(10, consumerGroup.currentPartitionEpoch(barTopicId, 4));
        assertEquals(10, consumerGroup.currentPartitionEpoch(barTopicId, 5));
        assertEquals(10, consumerGroup.currentPartitionEpoch(barTopicId, 6));
        assertEquals(-1, consumerGroup.currentPartitionEpoch(zarTopicId, 7));
        assertEquals(-1, consumerGroup.currentPartitionEpoch(zarTopicId, 8));
        assertEquals(-1, consumerGroup.currentPartitionEpoch(zarTopicId, 9));

        member = new ConsumerGroupMember.Builder(member)
            .setMemberEpoch(10)
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(barTopicId, 1, 2, 3)))
            .setPartitionsPendingRevocation(mkAssignment(
                mkTopicAssignment(zarTopicId, 4, 5, 6)))
            .setPartitionsPendingAssignment(mkAssignment(
                mkTopicAssignment(fooTopicId, 7, 8, 9)))
            .build();

        consumerGroup.updateMember(member);

        assertEquals(10, consumerGroup.currentPartitionEpoch(barTopicId, 1));
        assertEquals(10, consumerGroup.currentPartitionEpoch(barTopicId, 2));
        assertEquals(10, consumerGroup.currentPartitionEpoch(barTopicId, 3));
        assertEquals(10, consumerGroup.currentPartitionEpoch(zarTopicId, 4));
        assertEquals(10, consumerGroup.currentPartitionEpoch(zarTopicId, 5));
        assertEquals(10, consumerGroup.currentPartitionEpoch(zarTopicId, 6));
        assertEquals(-1, consumerGroup.currentPartitionEpoch(fooTopicId, 7));
        assertEquals(-1, consumerGroup.currentPartitionEpoch(fooTopicId, 8));
        assertEquals(-1, consumerGroup.currentPartitionEpoch(fooTopicId, 9));
    }

    @Test
    public void testDeletingMemberRemovesPartitionEpoch() {
        Uuid fooTopicId = Uuid.randomUuid();
        Uuid barTopicId = Uuid.randomUuid();
        Uuid zarTopicId = Uuid.randomUuid();

        ConsumerGroup consumerGroup = createConsumerGroup("foo");
        ConsumerGroupMember member;

        member = new ConsumerGroupMember.Builder("member")
            .setMemberEpoch(10)
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(fooTopicId, 1, 2, 3)))
            .setPartitionsPendingRevocation(mkAssignment(
                mkTopicAssignment(barTopicId, 4, 5, 6)))
            .setPartitionsPendingAssignment(mkAssignment(
                mkTopicAssignment(zarTopicId, 7, 8, 9)))
            .build();

        consumerGroup.updateMember(member);

        assertEquals(10, consumerGroup.currentPartitionEpoch(fooTopicId, 1));
        assertEquals(10, consumerGroup.currentPartitionEpoch(fooTopicId, 2));
        assertEquals(10, consumerGroup.currentPartitionEpoch(fooTopicId, 3));
        assertEquals(10, consumerGroup.currentPartitionEpoch(barTopicId, 4));
        assertEquals(10, consumerGroup.currentPartitionEpoch(barTopicId, 5));
        assertEquals(10, consumerGroup.currentPartitionEpoch(barTopicId, 6));
        assertEquals(-1, consumerGroup.currentPartitionEpoch(zarTopicId, 7));
        assertEquals(-1, consumerGroup.currentPartitionEpoch(zarTopicId, 8));
        assertEquals(-1, consumerGroup.currentPartitionEpoch(zarTopicId, 9));

        consumerGroup.removeMember(member.memberId());

        assertEquals(-1, consumerGroup.currentPartitionEpoch(barTopicId, 1));
        assertEquals(-1, consumerGroup.currentPartitionEpoch(barTopicId, 2));
        assertEquals(-1, consumerGroup.currentPartitionEpoch(barTopicId, 3));
        assertEquals(-1, consumerGroup.currentPartitionEpoch(zarTopicId, 4));
        assertEquals(-1, consumerGroup.currentPartitionEpoch(zarTopicId, 5));
        assertEquals(-1, consumerGroup.currentPartitionEpoch(zarTopicId, 6));
        assertEquals(-1, consumerGroup.currentPartitionEpoch(fooTopicId, 7));
        assertEquals(-1, consumerGroup.currentPartitionEpoch(fooTopicId, 8));
        assertEquals(-1, consumerGroup.currentPartitionEpoch(fooTopicId, 9));
    }

    @Test
    public void testGroupState() {
        ConsumerGroup consumerGroup = createConsumerGroup("foo");
        assertEquals(ConsumerGroup.ConsumerGroupState.EMPTY, consumerGroup.state());

        ConsumerGroupMember member1 = new ConsumerGroupMember.Builder("member1")
            .setMemberEpoch(1)
            .setPreviousMemberEpoch(0)
            .setNextMemberEpoch(1)
            .build();

        consumerGroup.updateMember(member1);
        consumerGroup.setGroupEpoch(1);

        assertEquals(ConsumerGroup.ConsumerGroupState.ASSIGNING, consumerGroup.state());

        ConsumerGroupMember member2 = new ConsumerGroupMember.Builder("member2")
            .setMemberEpoch(1)
            .setPreviousMemberEpoch(0)
            .setNextMemberEpoch(1)
            .build();

        consumerGroup.updateMember(member2);
        consumerGroup.setGroupEpoch(2);

        assertEquals(ConsumerGroup.ConsumerGroupState.ASSIGNING, consumerGroup.state());

        consumerGroup.setAssignmentEpoch(2);

        assertEquals(ConsumerGroup.ConsumerGroupState.RECONCILING, consumerGroup.state());

        member1 = new ConsumerGroupMember.Builder(member1)
            .setMemberEpoch(2)
            .setPreviousMemberEpoch(1)
            .setNextMemberEpoch(2)
            .build();
        consumerGroup.updateMember(member1);

        assertEquals(ConsumerGroup.ConsumerGroupState.RECONCILING, consumerGroup.state());

        member2 = new ConsumerGroupMember.Builder(member2)
            .setMemberEpoch(2)
            .setPreviousMemberEpoch(1)
            .setNextMemberEpoch(2)
            .build();
        consumerGroup.updateMember(member2);

        assertEquals(ConsumerGroup.ConsumerGroupState.STABLE, consumerGroup.state());

        consumerGroup.removeMember("member1");
        consumerGroup.removeMember("member2");

        assertEquals(ConsumerGroup.ConsumerGroupState.EMPTY, consumerGroup.state());
    }

    @Test
    public void testPreferredServerAssignor() {
        ConsumerGroup consumerGroup = createConsumerGroup("foo");

        consumerGroup.updateMember(new ConsumerGroupMember.Builder("member1")
            .setServerAssignorName("range")
            .build());
        consumerGroup.updateMember(new ConsumerGroupMember.Builder("member2")
            .setServerAssignorName("range")
            .build());
        consumerGroup.updateMember(new ConsumerGroupMember.Builder("member3")
            .setServerAssignorName("uniform")
            .build());

        assertEquals(Optional.of("range"), consumerGroup.preferredServerAssignor(
            null,
            Optional.empty())
        );

        assertEquals(Optional.of("uniform"), consumerGroup.preferredServerAssignor(
            "member2",
            Optional.of("uniform"))
        );

        consumerGroup.updateMember(new ConsumerGroupMember.Builder("member1")
            .setServerAssignorName(null)
            .build());
        consumerGroup.updateMember(new ConsumerGroupMember.Builder("member2")
            .setServerAssignorName(null)
            .build());
        consumerGroup.updateMember(new ConsumerGroupMember.Builder("member3")
            .setServerAssignorName(null)
            .build());

        assertEquals(Optional.empty(), consumerGroup.preferredServerAssignor(
            null,
            Optional.empty())
        );
    }

    @Test
    public void testUpdateSubscriptionMetadata() {
        Uuid fooTopicId = Uuid.randomUuid();
        Uuid barTopicId = Uuid.randomUuid();
        Uuid zarTopicId = Uuid.randomUuid();

        TopicsImage image = new GroupMetadataManagerTest.TopicsImageBuilder()
            .addTopic(fooTopicId, "foo", 1)
            .addTopic(barTopicId, "bar", 2)
            .addTopic(zarTopicId, "zar", 3)
            .build();

        ConsumerGroup consumerGroup = createConsumerGroup("foo");

        consumerGroup.updateMember(new ConsumerGroupMember.Builder("member1")
            .setSubscribedTopicNames(Arrays.asList("foo", "bar"))
            .build());
        consumerGroup.updateMember(new ConsumerGroupMember.Builder("member2")
            .setSubscribedTopicNames(Arrays.asList("foo", "bar"))
            .build());
        consumerGroup.updateMember(new ConsumerGroupMember.Builder("member3")
            .setSubscribedTopicNames(Arrays.asList("bar", "zar"))
            .build());

        Map<String, TopicMetadata> expectedSubscriptionMetadata = new HashMap<>();
        expectedSubscriptionMetadata.put("foo", new TopicMetadata(fooTopicId, "foo", 1));
        expectedSubscriptionMetadata.put("bar", new TopicMetadata(barTopicId, "bar", 2));
        expectedSubscriptionMetadata.put("zar", new TopicMetadata(zarTopicId, "zar", 3));
        assertEquals(expectedSubscriptionMetadata, consumerGroup.computeSubscriptionMetadata(
            null,
            Collections.emptyList(),
            image
        ));

        expectedSubscriptionMetadata.remove("zar");
        assertEquals(expectedSubscriptionMetadata, consumerGroup.computeSubscriptionMetadata(
            "member3",
            Collections.emptyList(),
            image
        ));
    }
}
