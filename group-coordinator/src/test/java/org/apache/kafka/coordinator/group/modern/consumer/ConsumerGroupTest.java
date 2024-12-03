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

import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.GroupNotEmptyException;
import org.apache.kafka.common.errors.IllegalGenerationException;
import org.apache.kafka.common.errors.StaleMemberEpochException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.message.ConsumerGroupDescribeResponseData;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.common.utils.annotation.ApiKeyVersionsSource;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.group.Group;
import org.apache.kafka.coordinator.group.GroupCoordinatorRecordHelpers;
import org.apache.kafka.coordinator.group.MetadataImageBuilder;
import org.apache.kafka.coordinator.group.OffsetAndMetadata;
import org.apache.kafka.coordinator.group.OffsetExpirationCondition;
import org.apache.kafka.coordinator.group.OffsetExpirationConditionImpl;
import org.apache.kafka.coordinator.group.classic.ClassicGroup;
import org.apache.kafka.coordinator.group.classic.ClassicGroupMember;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMemberMetadataValue;
import org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetricsShard;
import org.apache.kafka.coordinator.group.modern.Assignment;
import org.apache.kafka.coordinator.group.modern.MemberState;
import org.apache.kafka.coordinator.group.modern.TopicMetadata;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.timeline.SnapshotRegistry;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.coordinator.group.Assertions.assertRecordEquals;
import static org.apache.kafka.coordinator.group.Assertions.assertRecordsEquals;
import static org.apache.kafka.coordinator.group.Assertions.assertUnorderedListEquals;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkAssignment;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkTopicAssignment;
import static org.apache.kafka.coordinator.group.api.assignor.SubscriptionType.HETEROGENEOUS;
import static org.apache.kafka.coordinator.group.api.assignor.SubscriptionType.HOMOGENEOUS;
import static org.apache.kafka.coordinator.group.classic.ClassicGroupState.STABLE;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class ConsumerGroupTest {

    private ConsumerGroup createConsumerGroup(String groupId) {
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(new LogContext());
        return new ConsumerGroup(
            snapshotRegistry,
            groupId,
            mock(GroupCoordinatorMetricsShard.class)
        );
    }

    @Test
    public void testGetOrCreateMember() {
        ConsumerGroup consumerGroup = createConsumerGroup("foo");
        ConsumerGroupMember member;

        // Create a member.
        member = consumerGroup.getOrMaybeCreateMember("member-id", true);
        assertEquals("member-id", member.memberId());

        // Add member to the group.
        consumerGroup.updateMember(member);

        // Get that member back.
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
    public void testNoStaticMember() {
        ConsumerGroup consumerGroup = createConsumerGroup("foo");

        // Create a new member which is not static
        consumerGroup.getOrMaybeCreateMember("member", true);
        assertNull(consumerGroup.staticMember("instance-id"));
    }

    @Test
    public void testGetStaticMemberByInstanceId() {
        ConsumerGroup consumerGroup = createConsumerGroup("foo");
        ConsumerGroupMember member;

        member = consumerGroup.getOrMaybeCreateMember("member", true);

        member = new ConsumerGroupMember.Builder(member)
            .setSubscribedTopicNames(Arrays.asList("foo", "bar"))
            .setInstanceId("instance")
            .build();

        consumerGroup.updateMember(member);

        assertEquals(member, consumerGroup.staticMember("instance"));
        assertEquals(member, consumerGroup.getOrMaybeCreateMember("member", false));
        assertEquals(member.memberId(), consumerGroup.staticMemberId("instance"));
    }

    @Test
    public void testRemoveMember() {
        ConsumerGroup consumerGroup = createConsumerGroup("foo");

        ConsumerGroupMember member = consumerGroup.getOrMaybeCreateMember("member", true);
        consumerGroup.updateMember(member);
        assertTrue(consumerGroup.hasMember("member"));

        consumerGroup.removeMember("member");
        assertFalse(consumerGroup.hasMember("member"));

    }

    @Test
    public void testRemoveStaticMember() {
        ConsumerGroup consumerGroup = createConsumerGroup("foo");

        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member")
            .setSubscribedTopicNames(Arrays.asList("foo", "bar"))
            .setInstanceId("instance")
            .build();

        consumerGroup.updateMember(member);
        assertTrue(consumerGroup.hasMember("member"));

        consumerGroup.removeMember("member");
        assertFalse(consumerGroup.hasMember("member"));
        assertNull(consumerGroup.staticMember("instance"));
        assertNull(consumerGroup.staticMemberId("instance"));
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
            .setMemberEpoch(11)
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(barTopicId, 1, 2, 3)))
            .setPartitionsPendingRevocation(mkAssignment(
                mkTopicAssignment(zarTopicId, 4, 5, 6)))
            .build();

        consumerGroup.updateMember(member);

        assertEquals(11, consumerGroup.currentPartitionEpoch(barTopicId, 1));
        assertEquals(11, consumerGroup.currentPartitionEpoch(barTopicId, 2));
        assertEquals(11, consumerGroup.currentPartitionEpoch(barTopicId, 3));
        assertEquals(11, consumerGroup.currentPartitionEpoch(zarTopicId, 4));
        assertEquals(11, consumerGroup.currentPartitionEpoch(zarTopicId, 5));
        assertEquals(11, consumerGroup.currentPartitionEpoch(zarTopicId, 6));
        assertEquals(-1, consumerGroup.currentPartitionEpoch(fooTopicId, 7));
        assertEquals(-1, consumerGroup.currentPartitionEpoch(fooTopicId, 8));
        assertEquals(-1, consumerGroup.currentPartitionEpoch(fooTopicId, 9));
    }

    @Test
    public void testUpdatingMemberUpdatesPartitionEpochWhenPartitionIsReassignedBeforeBeingRevoked() {
        Uuid fooTopicId = Uuid.randomUuid();

        ConsumerGroup consumerGroup = createConsumerGroup("foo");
        ConsumerGroupMember member;

        member = new ConsumerGroupMember.Builder("member")
            .setMemberEpoch(10)
            .setAssignedPartitions(Collections.emptyMap())
            .setPartitionsPendingRevocation(mkAssignment(
                mkTopicAssignment(fooTopicId, 1)))
            .build();

        consumerGroup.updateMember(member);

        assertEquals(10, consumerGroup.currentPartitionEpoch(fooTopicId, 1));

        member = new ConsumerGroupMember.Builder(member)
            .setMemberEpoch(11)
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(fooTopicId, 1)))
            .setPartitionsPendingRevocation(Collections.emptyMap())
            .build();

        consumerGroup.updateMember(member);

        assertEquals(11, consumerGroup.currentPartitionEpoch(fooTopicId, 1));
    }

    @Test
    public void testUpdatingMemberUpdatesPartitionEpochWhenPartitionIsNotReleased() {
        Uuid fooTopicId = Uuid.randomUuid();
        ConsumerGroup consumerGroup = createConsumerGroup("foo");

        ConsumerGroupMember m1 = new ConsumerGroupMember.Builder("m1")
            .setMemberEpoch(10)
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(fooTopicId, 1)))
            .build();

        consumerGroup.updateMember(m1);

        ConsumerGroupMember m2 = new ConsumerGroupMember.Builder("m2")
            .setMemberEpoch(10)
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(fooTopicId, 1)))
            .build();

        // m2 should not be able to acquire foo-1 because the partition is
        // still owned by another member.
        assertThrows(IllegalStateException.class, () -> consumerGroup.updateMember(m2));
    }

    @Test
    public void testRemovePartitionEpochs() {
        Uuid fooTopicId = Uuid.randomUuid();
        ConsumerGroup consumerGroup = createConsumerGroup("foo");

        // Removing should fail because there is no epoch set.
        assertThrows(IllegalStateException.class, () -> consumerGroup.removePartitionEpochs(
            mkAssignment(
                mkTopicAssignment(fooTopicId, 1)
            ),
            10
        ));

        ConsumerGroupMember m1 = new ConsumerGroupMember.Builder("m1")
            .setMemberEpoch(10)
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(fooTopicId, 1)))
            .build();

        consumerGroup.updateMember(m1);

        // Removing should fail because the expected epoch is incorrect.
        assertThrows(IllegalStateException.class, () -> consumerGroup.removePartitionEpochs(
            mkAssignment(
                mkTopicAssignment(fooTopicId, 1)
            ),
            11
        ));
    }

    @Test
    public void testAddPartitionEpochs() {
        Uuid fooTopicId = Uuid.randomUuid();
        ConsumerGroup consumerGroup = createConsumerGroup("foo");

        consumerGroup.addPartitionEpochs(
            mkAssignment(
                mkTopicAssignment(fooTopicId, 1)
            ),
            10
        );

        // Changing the epoch should fail because the owner of the partition
        // should remove it first.
        assertThrows(IllegalStateException.class, () -> consumerGroup.addPartitionEpochs(
            mkAssignment(
                mkTopicAssignment(fooTopicId, 1)
            ),
            11
        ));
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
    public void testWaitingOnUnreleasedPartition() {
        Uuid fooTopicId = Uuid.randomUuid();
        Uuid barTopicId = Uuid.randomUuid();
        Uuid zarTopicId = Uuid.randomUuid();
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();

        ConsumerGroup consumerGroup = createConsumerGroup("foo");
        consumerGroup.updateTargetAssignment(memberId1, new Assignment(mkAssignment(
            mkTopicAssignment(fooTopicId, 1, 2, 3),
            mkTopicAssignment(zarTopicId, 7, 8, 9)
        )));

        ConsumerGroupMember member1 = new ConsumerGroupMember.Builder(memberId1)
            .setMemberEpoch(10)
            .setState(MemberState.UNRELEASED_PARTITIONS)
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(fooTopicId, 1, 2, 3)))
            .setPartitionsPendingRevocation(mkAssignment(
                mkTopicAssignment(barTopicId, 4, 5, 6)))
            .build();
        consumerGroup.updateMember(member1);

        assertFalse(consumerGroup.waitingOnUnreleasedPartition(member1));

        ConsumerGroupMember member2 = new ConsumerGroupMember.Builder(memberId2)
            .setMemberEpoch(10)
            .setPartitionsPendingRevocation(mkAssignment(
                mkTopicAssignment(zarTopicId, 7)))
            .build();
        consumerGroup.updateMember(member2);

        assertTrue(consumerGroup.waitingOnUnreleasedPartition(member1));
    }

    @Test
    public void testGroupState() {
        Uuid fooTopicId = Uuid.randomUuid();
        ConsumerGroup consumerGroup = createConsumerGroup("foo");
        assertEquals(ConsumerGroup.ConsumerGroupState.EMPTY, consumerGroup.state());

        ConsumerGroupMember member1 = new ConsumerGroupMember.Builder("member1")
            .setState(MemberState.STABLE)
            .setMemberEpoch(1)
            .setPreviousMemberEpoch(0)
            .build();

        consumerGroup.updateMember(member1);
        consumerGroup.setGroupEpoch(1);

        assertEquals(MemberState.STABLE, member1.state());
        assertEquals(ConsumerGroup.ConsumerGroupState.ASSIGNING, consumerGroup.state());

        ConsumerGroupMember member2 = new ConsumerGroupMember.Builder("member2")
            .setState(MemberState.STABLE)
            .setMemberEpoch(1)
            .setPreviousMemberEpoch(0)
            .build();

        consumerGroup.updateMember(member2);
        consumerGroup.setGroupEpoch(2);

        assertEquals(MemberState.STABLE, member2.state());
        assertEquals(ConsumerGroup.ConsumerGroupState.ASSIGNING, consumerGroup.state());

        consumerGroup.setTargetAssignmentEpoch(2);

        assertEquals(ConsumerGroup.ConsumerGroupState.RECONCILING, consumerGroup.state());

        member1 = new ConsumerGroupMember.Builder(member1)
            .setState(MemberState.STABLE)
            .setMemberEpoch(2)
            .setPreviousMemberEpoch(1)
            .build();

        consumerGroup.updateMember(member1);

        assertEquals(MemberState.STABLE, member1.state());
        assertEquals(ConsumerGroup.ConsumerGroupState.RECONCILING, consumerGroup.state());

        // Member 2 is not stable so the group stays in reconciling state.
        member2 = new ConsumerGroupMember.Builder(member2)
            .setState(MemberState.UNREVOKED_PARTITIONS)
            .setMemberEpoch(2)
            .setPreviousMemberEpoch(1)
            .build();

        consumerGroup.updateMember(member2);

        assertEquals(MemberState.UNREVOKED_PARTITIONS, member2.state());
        assertEquals(ConsumerGroup.ConsumerGroupState.RECONCILING, consumerGroup.state());

        member2 = new ConsumerGroupMember.Builder(member2)
            .setState(MemberState.STABLE)
            .setMemberEpoch(2)
            .setPreviousMemberEpoch(1)
            .build();

        consumerGroup.updateMember(member2);

        assertEquals(MemberState.STABLE, member2.state());
        assertEquals(ConsumerGroup.ConsumerGroupState.STABLE, consumerGroup.state());

        consumerGroup.removeMember("member1");
        consumerGroup.removeMember("member2");

        assertEquals(ConsumerGroup.ConsumerGroupState.EMPTY, consumerGroup.state());
    }

    @Test
    public void testGroupTypeFromString() {

        assertEquals(Group.GroupType.parse("classic"), Group.GroupType.CLASSIC);

        // Test case insensitivity.
        assertEquals(Group.GroupType.parse("Consumer"), Group.GroupType.CONSUMER);

        // Test with invalid group type.
        assertEquals(Group.GroupType.parse("Invalid"), Group.GroupType.UNKNOWN);
    }

    @Test
    public void testPreferredServerAssignor() {
        ConsumerGroup consumerGroup = createConsumerGroup("foo");

        ConsumerGroupMember member1 = new ConsumerGroupMember.Builder("member1")
            .setServerAssignorName("range")
            .build();
        ConsumerGroupMember member2 = new ConsumerGroupMember.Builder("member2")
            .setServerAssignorName("range")
            .build();
        ConsumerGroupMember member3 = new ConsumerGroupMember.Builder("member3")
            .setServerAssignorName("uniform")
            .build();

        // The group is empty so the preferred assignor should be empty.
        assertEquals(
            Optional.empty(),
            consumerGroup.preferredServerAssignor()
        );

        // Member 1 has got an updated assignor but this is not reflected in the group yet so
        // we pass the updated member. The assignor should be range.
        assertEquals(
            Optional.of("range"),
            consumerGroup.computePreferredServerAssignor(null, member1)
        );

        // Update the group with member 1.
        consumerGroup.updateMember(member1);

        // Member 1 is in the group so the assignor should be range.
        assertEquals(
            Optional.of("range"),
            consumerGroup.preferredServerAssignor()
        );

        // Member 1 has been removed but this is not reflected in the group yet so
        // we pass the removed member. The assignor should be range.
        assertEquals(
            Optional.empty(),
            consumerGroup.computePreferredServerAssignor(member1, null)
        );

        // Member 2 has got an updated assignor but this is not reflected in the group yet so
        // we pass the updated member. The assignor should be range.
        assertEquals(
            Optional.of("range"),
            consumerGroup.computePreferredServerAssignor(null, member2)
        );

        // Update the group with member 2.
        consumerGroup.updateMember(member2);

        // Member 1 and 2 are in the group so the assignor should be range.
        assertEquals(
            Optional.of("range"),
            consumerGroup.preferredServerAssignor()
        );

        // Update the group with member 3.
        consumerGroup.updateMember(member3);

        // Member 1, 2 and 3 are in the group so the assignor should be range.
        assertEquals(
            Optional.of("range"),
            consumerGroup.preferredServerAssignor()
        );

        // Members without assignors
        ConsumerGroupMember updatedMember1 = new ConsumerGroupMember.Builder("member1")
            .setServerAssignorName(null)
            .build();
        ConsumerGroupMember updatedMember2 = new ConsumerGroupMember.Builder("member2")
            .setServerAssignorName(null)
            .build();
        ConsumerGroupMember updatedMember3 = new ConsumerGroupMember.Builder("member3")
            .setServerAssignorName(null)
            .build();

        // Member 1 has removed it assignor but this is not reflected in the group yet so
        // we pass the updated member. The assignor should be range or uniform.
        Optional<String> assignor = consumerGroup.computePreferredServerAssignor(member1, updatedMember1);
        assertTrue(assignor.equals(Optional.of("range")) || assignor.equals(Optional.of("uniform")));

        // Update the group.
        consumerGroup.updateMember(updatedMember1);

        // Member 2 has removed it assignor but this is not reflected in the group yet so
        // we pass the updated member. The assignor should be range or uniform.
        assertEquals(
            Optional.of("uniform"),
            consumerGroup.computePreferredServerAssignor(member2, updatedMember2)
        );

        // Update the group.
        consumerGroup.updateMember(updatedMember2);

        // Only member 3 is left in the group so the assignor should be uniform.
        assertEquals(
            Optional.of("uniform"),
            consumerGroup.preferredServerAssignor()
        );

        // Member 3 has removed it assignor but this is not reflected in the group yet so
        // we pass the updated member. The assignor should be empty.
        assertEquals(
            Optional.empty(),
            consumerGroup.computePreferredServerAssignor(member3, updatedMember3)
        );

        // Update the group.
        consumerGroup.updateMember(updatedMember3);

        // The group is empty so the assignor should be empty as well.
        assertEquals(
            Optional.empty(),
            consumerGroup.preferredServerAssignor()
        );
    }

    @Test
    public void testUpdateSubscriptionMetadata() {
        Uuid fooTopicId = Uuid.randomUuid();
        Uuid barTopicId = Uuid.randomUuid();
        Uuid zarTopicId = Uuid.randomUuid();

        MetadataImage image = new MetadataImageBuilder()
            .addTopic(fooTopicId, "foo", 1)
            .addTopic(barTopicId, "bar", 2)
            .addTopic(zarTopicId, "zar", 3)
            .addRacks()
            .build();

        ConsumerGroupMember member1 = new ConsumerGroupMember.Builder("member1")
            .setSubscribedTopicNames(Collections.singletonList("foo"))
            .build();
        ConsumerGroupMember member2 = new ConsumerGroupMember.Builder("member2")
            .setSubscribedTopicNames(Collections.singletonList("bar"))
            .build();
        ConsumerGroupMember member3 = new ConsumerGroupMember.Builder("member3")
            .setSubscribedTopicNames(Collections.singletonList("zar"))
            .build();

        ConsumerGroup consumerGroup = createConsumerGroup("group-foo");

        // It should be empty by default.
        assertEquals(
            Collections.emptyMap(),
            consumerGroup.computeSubscriptionMetadata(
                consumerGroup.computeSubscribedTopicNames(null, null),
                image.topics(),
                image.cluster()
            )
        );

        // Compute while taking into account member 1.
        assertEquals(
            mkMap(
                mkEntry("foo", new TopicMetadata(fooTopicId, "foo", 1))
            ),
            consumerGroup.computeSubscriptionMetadata(
                consumerGroup.computeSubscribedTopicNames(null, member1),
                image.topics(),
                image.cluster()
            )
        );

        // Updating the group with member1.
        consumerGroup.updateMember(member1);

        // It should return foo now.
        assertEquals(
            mkMap(
                mkEntry("foo", new TopicMetadata(fooTopicId, "foo", 1))
            ),
            consumerGroup.computeSubscriptionMetadata(
                consumerGroup.computeSubscribedTopicNames(null, null),
                image.topics(),
                image.cluster()
            )
        );

        // Compute while taking into account removal of member 1.
        assertEquals(
            Collections.emptyMap(),
            consumerGroup.computeSubscriptionMetadata(
                consumerGroup.computeSubscribedTopicNames(member1, null),
                image.topics(),
                image.cluster()
            )
        );

        // Compute while taking into account member 2.
        assertEquals(
            mkMap(
                mkEntry("foo", new TopicMetadata(fooTopicId, "foo", 1)),
                mkEntry("bar", new TopicMetadata(barTopicId, "bar", 2))
            ),
            consumerGroup.computeSubscriptionMetadata(
                consumerGroup.computeSubscribedTopicNames(null, member2),
                image.topics(),
                image.cluster()
            )
        );

        // Updating the group with member2.
        consumerGroup.updateMember(member2);

        // It should return foo and bar.
        assertEquals(
            mkMap(
                mkEntry("foo", new TopicMetadata(fooTopicId, "foo", 1)),
                mkEntry("bar", new TopicMetadata(barTopicId, "bar", 2))
            ),
            consumerGroup.computeSubscriptionMetadata(
                consumerGroup.computeSubscribedTopicNames(null, null),
                image.topics(),
                image.cluster()
            )
        );

        // Compute while taking into account removal of member 2.
        assertEquals(
            mkMap(
                mkEntry("foo", new TopicMetadata(fooTopicId, "foo", 1))
            ),
            consumerGroup.computeSubscriptionMetadata(
                consumerGroup.computeSubscribedTopicNames(member2, null),
                image.topics(),
                image.cluster()
            )
        );

        // Removing member1 results in returning bar.
        assertEquals(
            mkMap(
                mkEntry("bar", new TopicMetadata(barTopicId, "bar", 2))
            ),
            consumerGroup.computeSubscriptionMetadata(
                consumerGroup.computeSubscribedTopicNames(member1, null),
                image.topics(),
                image.cluster()
            )
        );

        // Compute while taking into account member 3.
        assertEquals(
            mkMap(
                mkEntry("foo", new TopicMetadata(fooTopicId, "foo", 1)),
                mkEntry("bar", new TopicMetadata(barTopicId, "bar", 2)),
                mkEntry("zar", new TopicMetadata(zarTopicId, "zar", 3))
            ),
            consumerGroup.computeSubscriptionMetadata(
                consumerGroup.computeSubscribedTopicNames(null, member3),
                image.topics(),
                image.cluster()
            )
        );

        // Updating group with member3.
        consumerGroup.updateMember(member3);

        // It should return foo, bar and zar.
        assertEquals(
            mkMap(
                mkEntry("foo", new TopicMetadata(fooTopicId, "foo", 1)),
                mkEntry("bar", new TopicMetadata(barTopicId, "bar", 2)),
                mkEntry("zar", new TopicMetadata(zarTopicId, "zar", 3))
            ),
            consumerGroup.computeSubscriptionMetadata(
                consumerGroup.computeSubscribedTopicNames(null, null),
                image.topics(),
                image.cluster()
            )
        );

        // Compute while taking into account removal of member 1, member 2 and member 3
        assertEquals(
            Collections.emptyMap(),
            consumerGroup.computeSubscriptionMetadata(
                consumerGroup.computeSubscribedTopicNames(new HashSet<>(Arrays.asList(member1, member2, member3))),
                image.topics(),
                image.cluster()
            )
        );

        // Compute while taking into account removal of member 2 and member 3.
        assertEquals(
            mkMap(
                mkEntry("foo", new TopicMetadata(fooTopicId, "foo", 1))
            ),
            consumerGroup.computeSubscriptionMetadata(
                consumerGroup.computeSubscribedTopicNames(new HashSet<>(Arrays.asList(member2, member3))),
                image.topics(),
                image.cluster()
            )
        );

        // Compute while taking into account removal of member 1.
        assertEquals(
            mkMap(
                mkEntry("bar", new TopicMetadata(barTopicId, "bar", 2)),
                mkEntry("zar", new TopicMetadata(zarTopicId, "zar", 3))
            ),
            consumerGroup.computeSubscriptionMetadata(
                consumerGroup.computeSubscribedTopicNames(Collections.singleton(member1)),
                image.topics(),
                image.cluster()
            )
        );

        // It should return foo, bar and zar.
        assertEquals(
            mkMap(
                mkEntry("foo", new TopicMetadata(fooTopicId, "foo", 1)),
                mkEntry("bar", new TopicMetadata(barTopicId, "bar", 2)),
                mkEntry("zar", new TopicMetadata(zarTopicId, "zar", 3))
            ),
            consumerGroup.computeSubscriptionMetadata(
                consumerGroup.computeSubscribedTopicNames(Collections.emptySet()),
                image.topics(),
                image.cluster()
            )
        );
    }

    @Test
    public void testUpdateSubscribedTopicNamesAndSubscriptionType() {
        ConsumerGroupMember member1 = new ConsumerGroupMember.Builder("member1")
            .setSubscribedTopicNames(Collections.singletonList("foo"))
            .build();
        ConsumerGroupMember member2 = new ConsumerGroupMember.Builder("member2")
            .setSubscribedTopicNames(Arrays.asList("bar", "foo"))
            .build();
        ConsumerGroupMember member3 = new ConsumerGroupMember.Builder("member3")
            .setSubscribedTopicNames(Arrays.asList("bar", "foo"))
            .build();

        ConsumerGroup consumerGroup = createConsumerGroup("group-foo");

        // It should be empty by default.
        assertEquals(
            Collections.emptyMap(),
            consumerGroup.subscribedTopicNames()
        );

        // It should be Homogeneous by default.
        assertEquals(
            HOMOGENEOUS,
            consumerGroup.subscriptionType()
        );

        consumerGroup.updateMember(member1);

        // It should be Homogeneous since there is just 1 member
        assertEquals(
            HOMOGENEOUS,
            consumerGroup.subscriptionType()
        );

        consumerGroup.updateMember(member2);

        assertEquals(
            HETEROGENEOUS,
            consumerGroup.subscriptionType()
        );

        consumerGroup.updateMember(member3);

        assertEquals(
            HETEROGENEOUS,
            consumerGroup.subscriptionType()
        );

        consumerGroup.removeMember(member1.memberId());

        assertEquals(
            HOMOGENEOUS,
            consumerGroup.subscriptionType()
        );

        ConsumerGroupMember member4 = new ConsumerGroupMember.Builder("member2")
            .setSubscribedTopicNames(Arrays.asList("bar", "foo", "zar"))
            .build();

        consumerGroup.updateMember(member4);

        assertEquals(
            HETEROGENEOUS,
            consumerGroup.subscriptionType()
        );
    }

    @Test
    public void testUpdateInvertedAssignment() {
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(new LogContext());
        GroupCoordinatorMetricsShard metricsShard = mock(GroupCoordinatorMetricsShard.class);
        ConsumerGroup consumerGroup = new ConsumerGroup(snapshotRegistry, "test-group", metricsShard);
        Uuid topicId = Uuid.randomUuid();
        String memberId1 = "member1";
        String memberId2 = "member2";

        // Initial assignment for member1
        Assignment initialAssignment = new Assignment(Collections.singletonMap(
            topicId,
            new HashSet<>(Collections.singletonList(0))
        ));
        consumerGroup.updateTargetAssignment(memberId1, initialAssignment);

        // Verify that partition 0 is assigned to member1.
        assertEquals(
            mkMap(
                mkEntry(topicId, mkMap(mkEntry(0, memberId1)))
            ),
            consumerGroup.invertedTargetAssignment()
        );

        // New assignment for member1
        Assignment newAssignment = new Assignment(Collections.singletonMap(
            topicId,
            new HashSet<>(Collections.singletonList(1))
        ));
        consumerGroup.updateTargetAssignment(memberId1, newAssignment);

        // Verify that partition 0 is no longer assigned and partition 1 is assigned to member1
        assertEquals(
            mkMap(
                mkEntry(topicId, mkMap(mkEntry(1, memberId1)))
            ),
            consumerGroup.invertedTargetAssignment()
        );

        // New assignment for member2 to add partition 1
        Assignment newAssignment2 = new Assignment(Collections.singletonMap(
            topicId,
            new HashSet<>(Collections.singletonList(1))
        ));
        consumerGroup.updateTargetAssignment(memberId2, newAssignment2);

        // Verify that partition 1 is assigned to member2
        assertEquals(
            mkMap(
                mkEntry(topicId, mkMap(mkEntry(1, memberId2)))
            ),
            consumerGroup.invertedTargetAssignment()
        );

        // New assignment for member1 to revoke partition 1 and assign partition 0
        Assignment newAssignment1 = new Assignment(Collections.singletonMap(
            topicId,
            new HashSet<>(Collections.singletonList(0))
        ));
        consumerGroup.updateTargetAssignment(memberId1, newAssignment1);

        // Verify that partition 1 is still assigned to member2 and partition 0 is assigned to member1
        assertEquals(
            mkMap(
                mkEntry(topicId, mkMap(
                    mkEntry(0, memberId1),
                    mkEntry(1, memberId2)
                ))
            ),
            consumerGroup.invertedTargetAssignment()
        );

        // Test remove target assignment for member1
        consumerGroup.removeTargetAssignment(memberId1);

        // Verify that partition 0 is no longer assigned and partition 1 is still assigned to member2
        assertEquals(
            mkMap(
                mkEntry(topicId, mkMap(mkEntry(1, memberId2)))
            ),
            consumerGroup.invertedTargetAssignment()
        );
    }

    @Test
    public void testMetadataRefreshDeadline() {
        MockTime time = new MockTime();
        ConsumerGroup group = createConsumerGroup("group-foo");

        // Group epoch starts at 0.
        assertEquals(0, group.groupEpoch());

        // The refresh time deadline should be empty when the group is created or loaded.
        assertTrue(group.hasMetadataExpired(time.milliseconds()));
        assertEquals(0L, group.metadataRefreshDeadline().deadlineMs);
        assertEquals(0, group.metadataRefreshDeadline().epoch);

        // Set the refresh deadline. The metadata remains valid because the deadline
        // has not past and the group epoch is correct.
        group.setMetadataRefreshDeadline(time.milliseconds() + 1000, group.groupEpoch());
        assertFalse(group.hasMetadataExpired(time.milliseconds()));
        assertEquals(time.milliseconds() + 1000, group.metadataRefreshDeadline().deadlineMs);
        assertEquals(group.groupEpoch(), group.metadataRefreshDeadline().epoch);

        // Advance past the deadline. The metadata should have expired.
        time.sleep(1001L);
        assertTrue(group.hasMetadataExpired(time.milliseconds()));

        // Set the refresh time deadline with a higher group epoch. The metadata is considered
        // as expired because the group epoch attached to the deadline is higher than the
        // current group epoch.
        group.setMetadataRefreshDeadline(time.milliseconds() + 1000, group.groupEpoch() + 1);
        assertTrue(group.hasMetadataExpired(time.milliseconds()));
        assertEquals(time.milliseconds() + 1000, group.metadataRefreshDeadline().deadlineMs);
        assertEquals(group.groupEpoch() + 1, group.metadataRefreshDeadline().epoch);

        // Advance the group epoch.
        group.setGroupEpoch(group.groupEpoch() + 1);

        // Set the refresh deadline. The metadata remains valid because the deadline
        // has not past and the group epoch is correct.
        group.setMetadataRefreshDeadline(time.milliseconds() + 1000, group.groupEpoch());
        assertFalse(group.hasMetadataExpired(time.milliseconds()));
        assertEquals(time.milliseconds() + 1000, group.metadataRefreshDeadline().deadlineMs);
        assertEquals(group.groupEpoch(), group.metadataRefreshDeadline().epoch);

        // Request metadata refresh. The metadata expires immediately.
        group.requestMetadataRefresh();
        assertTrue(group.hasMetadataExpired(time.milliseconds()));
        assertEquals(0L, group.metadataRefreshDeadline().deadlineMs);
        assertEquals(0, group.metadataRefreshDeadline().epoch);
    }

    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.TXN_OFFSET_COMMIT)
    public void testValidateTransactionalOffsetCommit(short version) {
        boolean isTransactional = true;
        ConsumerGroup group = createConsumerGroup("group-foo");

        // Simulate a call from the admin client without member id and member epoch.
        // This should pass only if the group is empty.
        group.validateOffsetCommit("", "", -1, isTransactional, version);

        // The member does not exist.
        assertThrows(UnknownMemberIdException.class, () ->
            group.validateOffsetCommit("member-id", null, 0, isTransactional, version));

        // Create a member.
        group.updateMember(new ConsumerGroupMember.Builder("member-id").build());

        // A call from the admin client should fail as the group is not empty.
        assertThrows(UnknownMemberIdException.class, () ->
            group.validateOffsetCommit("", "", -1, isTransactional, version));

        // The member epoch is stale.
        assertThrows(StaleMemberEpochException.class, () ->
            group.validateOffsetCommit("member-id", "", 10, isTransactional, version));

        // This should succeed.
        group.validateOffsetCommit("member-id", "", 0, isTransactional, version);

        // This should succeed.
        group.validateOffsetCommit("", null, -1, isTransactional, version);
    }

    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.OFFSET_COMMIT)
    public void testValidateOffsetCommit(short version) {
        boolean isTransactional = false;
        ConsumerGroup group = createConsumerGroup("group-foo");

        // Simulate a call from the admin client without member id and member epoch.
        // This should pass only if the group is empty.
        group.validateOffsetCommit("", "", -1, isTransactional, version);

        // The member does not exist.
        assertThrows(UnknownMemberIdException.class, () ->
            group.validateOffsetCommit("member-id", null, 0, isTransactional, version));

        // Create members.
        group.updateMember(
            new ConsumerGroupMember
                .Builder("new-protocol-member-id").build()
        );
        group.updateMember(
            new ConsumerGroupMember.Builder("old-protocol-member-id")
                .setClassicMemberMetadata(new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata())
                .build()
        );

        // A call from the admin client should fail as the group is not empty.
        assertThrows(UnknownMemberIdException.class, () ->
            group.validateOffsetCommit("", "", -1, isTransactional, version));
        assertThrows(UnknownMemberIdException.class, () ->
            group.validateOffsetCommit("", null, -1, isTransactional, version));

        // The member epoch is stale.
        if (version >= 9) {
            assertThrows(StaleMemberEpochException.class, () ->
                group.validateOffsetCommit("new-protocol-member-id", "", 10, isTransactional, version));
        } else {
            assertThrows(UnsupportedVersionException.class, () ->
                group.validateOffsetCommit("new-protocol-member-id", "", 10, isTransactional, version));
        }
        assertThrows(IllegalGenerationException.class, () ->
            group.validateOffsetCommit("old-protocol-member-id", "", 10, isTransactional, version));

        // This should succeed.
        if (version >= 9) {
            group.validateOffsetCommit("new-protocol-member-id", "", 0, isTransactional, version);
        } else {
            assertThrows(UnsupportedVersionException.class, () ->
                group.validateOffsetCommit("new-protocol-member-id", "", 0, isTransactional, version));
        }
    }

    @Test
    public void testAsListedGroup() {
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(new LogContext());
        GroupCoordinatorMetricsShard metricsShard = new GroupCoordinatorMetricsShard(
            snapshotRegistry,
            Collections.emptyMap(),
            new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)
        );
        ConsumerGroup group = new ConsumerGroup(snapshotRegistry, "group-foo", metricsShard);
        snapshotRegistry.idempotentCreateSnapshot(0);
        assertEquals(ConsumerGroup.ConsumerGroupState.EMPTY.toString(), group.stateAsString(0));
        group.updateMember(new ConsumerGroupMember.Builder("member1")
            .setSubscribedTopicNames(Collections.singletonList("foo"))
            .build());
        snapshotRegistry.idempotentCreateSnapshot(1);
        assertEquals(ConsumerGroup.ConsumerGroupState.EMPTY.toString(), group.stateAsString(0));
        assertEquals(ConsumerGroup.ConsumerGroupState.STABLE.toString(), group.stateAsString(1));
    }

    @Test
    public void testValidateOffsetFetch() {
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(new LogContext());
        ConsumerGroup group = new ConsumerGroup(
            snapshotRegistry,
            "group-foo",
            mock(GroupCoordinatorMetricsShard.class)
        );

        // Simulate a call from the admin client without member id and member epoch.
        group.validateOffsetFetch(null, -1, Long.MAX_VALUE);

        // The member does not exist.
        assertThrows(UnknownMemberIdException.class, () ->
            group.validateOffsetFetch("member-id", 0, Long.MAX_VALUE));

        // Create a member.
        snapshotRegistry.idempotentCreateSnapshot(0);
        group.updateMember(new ConsumerGroupMember.Builder("member-id").build());

        // The member does not exist at last committed offset 0.
        assertThrows(UnknownMemberIdException.class, () ->
            group.validateOffsetFetch("member-id", 0, 0));

        // The member exists but the epoch is stale when the last committed offset is not considered.
        assertThrows(StaleMemberEpochException.class, () ->
            group.validateOffsetFetch("member-id", 10, Long.MAX_VALUE));

        // This should succeed.
        group.validateOffsetFetch("member-id", 0, Long.MAX_VALUE);
    }

    @Test
    public void testValidateDeleteGroup() {
        ConsumerGroup consumerGroup = createConsumerGroup("foo");

        assertEquals(ConsumerGroup.ConsumerGroupState.EMPTY, consumerGroup.state());
        assertDoesNotThrow(consumerGroup::validateDeleteGroup);

        ConsumerGroupMember member1 = new ConsumerGroupMember.Builder("member1")
            .setMemberEpoch(1)
            .setPreviousMemberEpoch(0)
            .build();
        consumerGroup.updateMember(member1);

        assertEquals(ConsumerGroup.ConsumerGroupState.RECONCILING, consumerGroup.state());
        assertThrows(GroupNotEmptyException.class, consumerGroup::validateDeleteGroup);

        consumerGroup.setGroupEpoch(1);

        assertEquals(ConsumerGroup.ConsumerGroupState.ASSIGNING, consumerGroup.state());
        assertThrows(GroupNotEmptyException.class, consumerGroup::validateDeleteGroup);

        consumerGroup.setTargetAssignmentEpoch(1);

        assertEquals(ConsumerGroup.ConsumerGroupState.STABLE, consumerGroup.state());
        assertThrows(GroupNotEmptyException.class, consumerGroup::validateDeleteGroup);
    }

    @Test
    public void testOffsetExpirationCondition() {
        long currentTimestamp = 30000L;
        long commitTimestamp = 20000L;
        long offsetsRetentionMs = 10000L;
        OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(15000L, OptionalInt.empty(), "", commitTimestamp, OptionalLong.empty());
        ConsumerGroup group = new ConsumerGroup(new SnapshotRegistry(new LogContext()), "group-id", mock(GroupCoordinatorMetricsShard.class));

        Optional<OffsetExpirationCondition> offsetExpirationCondition = group.offsetExpirationCondition();
        assertTrue(offsetExpirationCondition.isPresent());

        OffsetExpirationConditionImpl condition = (OffsetExpirationConditionImpl) offsetExpirationCondition.get();
        assertEquals(commitTimestamp, condition.baseTimestamp().apply(offsetAndMetadata));
        assertTrue(condition.isOffsetExpired(offsetAndMetadata, currentTimestamp, offsetsRetentionMs));
    }

    @Test
    public void testIsSubscribedToTopic() {
        Uuid fooTopicId = Uuid.randomUuid();
        Uuid barTopicId = Uuid.randomUuid();

        MetadataImage image = new MetadataImageBuilder()
            .addTopic(fooTopicId, "foo", 1)
            .addTopic(barTopicId, "bar", 2)
            .addRacks()
            .build();

        ConsumerGroupMember member1 = new ConsumerGroupMember.Builder("member1")
            .setSubscribedTopicNames(Collections.singletonList("foo"))
            .build();
        ConsumerGroupMember member2 = new ConsumerGroupMember.Builder("member2")
            .setSubscribedTopicNames(Collections.singletonList("bar"))
            .build();

        ConsumerGroup consumerGroup = createConsumerGroup("group-foo");

        consumerGroup.updateMember(member1);
        consumerGroup.updateMember(member2);

        assertEquals(
            mkMap(
                mkEntry("foo", new TopicMetadata(fooTopicId, "foo", 1)),
                mkEntry("bar", new TopicMetadata(barTopicId, "bar", 2))
            ),
            consumerGroup.computeSubscriptionMetadata(
                consumerGroup.computeSubscribedTopicNames(null, null),
                image.topics(),
                image.cluster()
            )
        );

        assertTrue(consumerGroup.isSubscribedToTopic("foo"));
        assertTrue(consumerGroup.isSubscribedToTopic("bar"));

        consumerGroup.removeMember("member1");
        assertFalse(consumerGroup.isSubscribedToTopic("foo"));

        consumerGroup.removeMember("member2");
        assertFalse(consumerGroup.isSubscribedToTopic("bar"));
    }

    @Test
    public void testAsDescribedGroup() {
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(new LogContext());
        ConsumerGroup group = new ConsumerGroup(snapshotRegistry, "group-id-1", mock(GroupCoordinatorMetricsShard.class));
        snapshotRegistry.idempotentCreateSnapshot(0);
        assertEquals(ConsumerGroup.ConsumerGroupState.EMPTY.toString(), group.stateAsString(0));

        group.updateMember(new ConsumerGroupMember.Builder("member1")
                .setSubscribedTopicNames(Collections.singletonList("foo"))
                .setServerAssignorName("assignorName")
                .build());
        group.updateMember(new ConsumerGroupMember.Builder("member2")
                .build());
        snapshotRegistry.idempotentCreateSnapshot(1);

        ConsumerGroupDescribeResponseData.DescribedGroup expected = new ConsumerGroupDescribeResponseData.DescribedGroup()
            .setGroupId("group-id-1")
            .setGroupState(ConsumerGroup.ConsumerGroupState.STABLE.toString())
            .setGroupEpoch(0)
            .setAssignmentEpoch(0)
            .setAssignorName("assignorName")
            .setMembers(Arrays.asList(
                new ConsumerGroupDescribeResponseData.Member()
                    .setMemberId("member1")
                    .setSubscribedTopicNames(Collections.singletonList("foo"))
                    .setSubscribedTopicRegex("")
                    .setMemberType((byte) 1),
                new ConsumerGroupDescribeResponseData.Member().setMemberId("member2")
                    .setSubscribedTopicRegex("")
                    .setMemberType((byte) 1)
            ));
        ConsumerGroupDescribeResponseData.DescribedGroup actual = group.asDescribedGroup(1, "",
            new MetadataImageBuilder().build().topics());

        assertEquals(expected, actual);
    }

    @Test
    public void testStateTransitionMetrics() {
        // Confirm metrics is not updated when a new ConsumerGroup is created but only when the group transitions
        // its state.
        GroupCoordinatorMetricsShard metrics = mock(GroupCoordinatorMetricsShard.class);
        ConsumerGroup consumerGroup = new ConsumerGroup(
            new SnapshotRegistry(new LogContext()),
            "group-id",
            metrics
        );

        assertEquals(ConsumerGroup.ConsumerGroupState.EMPTY, consumerGroup.state());
        verify(metrics, times(0)).onConsumerGroupStateTransition(null, ConsumerGroup.ConsumerGroupState.EMPTY);

        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member")
            .setMemberEpoch(1)
            .setPreviousMemberEpoch(0)
            .build();

        consumerGroup.updateMember(member);

        assertEquals(ConsumerGroup.ConsumerGroupState.RECONCILING, consumerGroup.state());
        verify(metrics, times(1)).onConsumerGroupStateTransition(ConsumerGroup.ConsumerGroupState.EMPTY, ConsumerGroup.ConsumerGroupState.RECONCILING);

        consumerGroup.setGroupEpoch(1);

        assertEquals(ConsumerGroup.ConsumerGroupState.ASSIGNING, consumerGroup.state());
        verify(metrics, times(1)).onConsumerGroupStateTransition(ConsumerGroup.ConsumerGroupState.RECONCILING, ConsumerGroup.ConsumerGroupState.ASSIGNING);

        consumerGroup.setTargetAssignmentEpoch(1);

        assertEquals(ConsumerGroup.ConsumerGroupState.STABLE, consumerGroup.state());
        verify(metrics, times(1)).onConsumerGroupStateTransition(ConsumerGroup.ConsumerGroupState.ASSIGNING, ConsumerGroup.ConsumerGroupState.STABLE);

        consumerGroup.removeMember("member");

        assertEquals(ConsumerGroup.ConsumerGroupState.EMPTY, consumerGroup.state());
        verify(metrics, times(1)).onConsumerGroupStateTransition(ConsumerGroup.ConsumerGroupState.STABLE, ConsumerGroup.ConsumerGroupState.EMPTY);
    }

    @Test
    public void testIsInStatesCaseInsensitive() {
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(new LogContext());
        GroupCoordinatorMetricsShard metricsShard = new GroupCoordinatorMetricsShard(
            snapshotRegistry,
            Collections.emptyMap(),
            new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)
        );
        ConsumerGroup group = new ConsumerGroup(snapshotRegistry, "group-foo", metricsShard);
        snapshotRegistry.idempotentCreateSnapshot(0);
        assertTrue(group.isInStates(Collections.singleton("empty"), 0));
        assertFalse(group.isInStates(Collections.singleton("Empty"), 0));

        group.updateMember(new ConsumerGroupMember.Builder("member1")
            .setSubscribedTopicNames(Collections.singletonList("foo"))
            .build());
        snapshotRegistry.idempotentCreateSnapshot(1);
        assertTrue(group.isInStates(Collections.singleton("empty"), 0));
        assertTrue(group.isInStates(Collections.singleton("stable"), 1));
        assertFalse(group.isInStates(Collections.singleton("empty"), 1));
    }

    @Test
    public void testClassicMembersSupportedProtocols() {
        ConsumerGroup consumerGroup = createConsumerGroup("foo");
        List<ConsumerGroupMemberMetadataValue.ClassicProtocol> rangeProtocol = new ArrayList<>();
        rangeProtocol.add(new ConsumerGroupMemberMetadataValue.ClassicProtocol()
            .setName("range")
            .setMetadata(new byte[0]));

        List<ConsumerGroupMemberMetadataValue.ClassicProtocol> roundRobinAndRangeProtocols = new ArrayList<>();
        roundRobinAndRangeProtocols.add(new ConsumerGroupMemberMetadataValue.ClassicProtocol()
            .setName("roundrobin")
            .setMetadata(new byte[0]));
        roundRobinAndRangeProtocols.add(new ConsumerGroupMemberMetadataValue.ClassicProtocol()
            .setName("range")
            .setMetadata(new byte[0]));

        ConsumerGroupMember member1 = new ConsumerGroupMember.Builder("member-1")
            .setClassicMemberMetadata(new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                .setSupportedProtocols(rangeProtocol))
            .build();
        consumerGroup.updateMember(member1);

        ConsumerGroupMember member2 = new ConsumerGroupMember.Builder("member-2")
            .setClassicMemberMetadata(new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                .setSupportedProtocols(roundRobinAndRangeProtocols))
            .build();
        consumerGroup.updateMember(member2);

        assertEquals(2, consumerGroup.classicMembersSupportedProtocols().get("range"));
        assertEquals(1, consumerGroup.classicMembersSupportedProtocols().get("roundrobin"));
        assertTrue(consumerGroup.supportsClassicProtocols(ConsumerProtocol.PROTOCOL_TYPE, new HashSet<>(Arrays.asList("range", "sticky"))));
        assertFalse(consumerGroup.supportsClassicProtocols(ConsumerProtocol.PROTOCOL_TYPE, new HashSet<>(Arrays.asList("sticky", "roundrobin"))));

        member2 = new ConsumerGroupMember.Builder(member2)
            .setClassicMemberMetadata(new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                .setSupportedProtocols(rangeProtocol))
            .build();
        consumerGroup.updateMember(member2);

        assertEquals(2, consumerGroup.classicMembersSupportedProtocols().get("range"));
        assertFalse(consumerGroup.classicMembersSupportedProtocols().containsKey("roundrobin"));

        member1 = new ConsumerGroupMember.Builder(member1)
            .setClassicMemberMetadata(new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                .setSupportedProtocols(roundRobinAndRangeProtocols))
            .build();
        consumerGroup.updateMember(member1);
        member2 = new ConsumerGroupMember.Builder(member2)
            .setClassicMemberMetadata(new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                .setSupportedProtocols(roundRobinAndRangeProtocols))
            .build();
        consumerGroup.updateMember(member2);

        assertEquals(2, consumerGroup.classicMembersSupportedProtocols().get("range"));
        assertEquals(2, consumerGroup.classicMembersSupportedProtocols().get("roundrobin"));
        assertTrue(consumerGroup.supportsClassicProtocols(ConsumerProtocol.PROTOCOL_TYPE, new HashSet<>(Arrays.asList("sticky", "roundrobin"))));
    }

    @Test
    public void testNumClassicProtocolMembers() {
        ConsumerGroup consumerGroup = createConsumerGroup("foo");
        List<ConsumerGroupMemberMetadataValue.ClassicProtocol> protocols = new ArrayList<>();
        protocols.add(new ConsumerGroupMemberMetadataValue.ClassicProtocol()
            .setName("range")
            .setMetadata(new byte[0]));

        // The group has member 1 (using the classic protocol).
        ConsumerGroupMember member1 = new ConsumerGroupMember.Builder("member-1")
            .setClassicMemberMetadata(new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                .setSupportedProtocols(protocols))
            .build();
        consumerGroup.updateMember(member1);
        assertEquals(1, consumerGroup.numClassicProtocolMembers());

        // The group has member 1 (using the classic protocol) and member 2 (using the consumer protocol).
        ConsumerGroupMember member2 = new ConsumerGroupMember.Builder("member-2")
            .build();
        consumerGroup.updateMember(member2);
        assertEquals(1, consumerGroup.numClassicProtocolMembers());
        assertFalse(consumerGroup.allMembersUseClassicProtocolExcept("member-1"));
        assertTrue(consumerGroup.allMembersUseClassicProtocolExcept("member-2"));

        // The group has member 2 (using the consumer protocol) and member 3 (using the consumer protocol).
        consumerGroup.removeMember(member1.memberId());
        ConsumerGroupMember member3 = new ConsumerGroupMember.Builder("member-3")
            .build();
        consumerGroup.updateMember(member3);
        assertEquals(0, consumerGroup.numClassicProtocolMembers());
        assertFalse(consumerGroup.allMembersUseClassicProtocolExcept("member-2"));

        // The group has member 2 (using the classic protocol).
        consumerGroup.removeMember(member2.memberId());
        member2 = new ConsumerGroupMember.Builder("member-2")
            .setClassicMemberMetadata(new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                .setSupportedProtocols(protocols))
            .build();
        consumerGroup.updateMember(member2);
        assertEquals(1, consumerGroup.numClassicProtocolMembers());
    }

    @Test
    public void testCreateGroupTombstoneRecordsWithReplacedMember() {
        String groupId = "group";
        String memberId1 = "member-1";
        String memberId2 = "member-2";
        String newMemberId2 = "new-member-2";

        ConsumerGroup consumerGroup = createConsumerGroup(groupId);
        List<ConsumerGroupMemberMetadataValue.ClassicProtocol> protocols = new ArrayList<>();
        protocols.add(new ConsumerGroupMemberMetadataValue.ClassicProtocol()
            .setName("range")
            .setMetadata(new byte[0]));

        ConsumerGroupMember member1 = new ConsumerGroupMember.Builder(memberId1)
            .setClassicMemberMetadata(new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                .setSupportedProtocols(protocols))
            .build();
        consumerGroup.updateMember(member1);

        ConsumerGroupMember member2 = new ConsumerGroupMember.Builder(memberId2)
            .setInstanceId("instance-id-2")
            .build();
        consumerGroup.updateMember(member2);

        List<CoordinatorRecord> records = new ArrayList<>();
        consumerGroup.createGroupTombstoneRecordsWithReplacedMember(records, memberId2, newMemberId2);

        List<CoordinatorRecord> expectedRecords = Arrays.asList(
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord(groupId, memberId1),
            GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord(groupId, newMemberId2),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentTombstoneRecord(groupId, memberId1),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentTombstoneRecord(groupId, newMemberId2),
            GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentEpochTombstoneRecord(groupId),
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord(groupId, memberId1),
            GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord(groupId, newMemberId2),
            GroupCoordinatorRecordHelpers.newConsumerGroupSubscriptionMetadataTombstoneRecord(groupId),
            GroupCoordinatorRecordHelpers.newConsumerGroupEpochTombstoneRecord(groupId)
        );
        assertEquals(expectedRecords.size(), records.size());
        assertUnorderedListEquals(expectedRecords.subList(0, 2), records.subList(0, 2));
        assertUnorderedListEquals(expectedRecords.subList(2, 4), records.subList(2, 4));
        assertRecordEquals(expectedRecords.get(4), records.get(4));
        assertUnorderedListEquals(expectedRecords.subList(5, 7), records.subList(5, 7));
        assertRecordsEquals(expectedRecords.subList(7, 9), records.subList(7, 9));
    }

    @Test
    public void testFromClassicGroup() {
        MockTime time = new MockTime();
        LogContext logContext = new LogContext();
        String groupId = "group-id";
        String memberId = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        MetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 1)
            .addTopic(barTopicId, barTopicName, 1)
            .addRacks()
            .build();

        ClassicGroup classicGroup = new ClassicGroup(
            logContext,
            groupId,
            STABLE,
            time,
            10,
            Optional.of(ConsumerProtocol.PROTOCOL_TYPE),
            Optional.of("range"),
            Optional.empty(),
            Optional.of(time.milliseconds())
        );

        ClassicGroupMember member = new ClassicGroupMember(
            memberId,
            Optional.empty(),
            "client-id",
            "client-host",
            5000,
            500,
            ConsumerProtocol.PROTOCOL_TYPE,
            new JoinGroupRequestData.JoinGroupRequestProtocolCollection(Collections.singletonList(
                new JoinGroupRequestData.JoinGroupRequestProtocol()
                    .setName("range")
                    .setMetadata(Utils.toArray(ConsumerProtocol.serializeSubscription(new ConsumerPartitionAssignor.Subscription(
                            Arrays.asList(fooTopicName, barTopicName),
                            null,
                            Arrays.asList(
                                new TopicPartition(fooTopicName, 0),
                                new TopicPartition(barTopicName, 0))))))
            ).iterator()),
            Utils.toArray(ConsumerProtocol.serializeAssignment(new ConsumerPartitionAssignor.Assignment(Arrays.asList(
                new TopicPartition(fooTopicName, 0),
                new TopicPartition(barTopicName, 0)
            ))))
        );
        classicGroup.add(member);

        ConsumerGroup consumerGroup = ConsumerGroup.fromClassicGroup(
            new SnapshotRegistry(logContext),
            mock(GroupCoordinatorMetricsShard.class),
            classicGroup,
            metadataImage.topics()
        );

        ConsumerGroup expectedConsumerGroup = new ConsumerGroup(
            new SnapshotRegistry(logContext),
            groupId,
            mock(GroupCoordinatorMetricsShard.class)
        );
        expectedConsumerGroup.setGroupEpoch(10);
        expectedConsumerGroup.setTargetAssignmentEpoch(10);
        expectedConsumerGroup.updateTargetAssignment(memberId, new Assignment(mkAssignment(
            mkTopicAssignment(fooTopicId, 0)
        )));
        expectedConsumerGroup.updateMember(new ConsumerGroupMember.Builder(memberId)
            .setMemberEpoch(classicGroup.generationId())
            .setState(MemberState.STABLE)
            .setPreviousMemberEpoch(classicGroup.generationId())
            .setInstanceId(null)
            .setRackId(null)
            .setRebalanceTimeoutMs(member.rebalanceTimeoutMs())
            .setClientId(member.clientId())
            .setClientHost(member.clientHost())
            .setSubscribedTopicNames(Arrays.asList(fooTopicName, barTopicName))
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(fooTopicId, 0),
                mkTopicAssignment(barTopicId, 0)))
            .setClassicMemberMetadata(
                new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                    .setSessionTimeoutMs(member.sessionTimeoutMs())
                    .setSupportedProtocols(Collections.singletonList(
                        new ConsumerGroupMemberMetadataValue.ClassicProtocol()
                            .setName("range")
                            .setMetadata(Utils.toArray(ConsumerProtocol.serializeSubscription(new ConsumerPartitionAssignor.Subscription(
                                Arrays.asList(fooTopicName, barTopicName),
                                null,
                                Arrays.asList(
                                    new TopicPartition(fooTopicName, 0),
                                    new TopicPartition(barTopicName, 0)))))))))
            .build());

        assertEquals(expectedConsumerGroup.groupId(), consumerGroup.groupId());
        assertEquals(expectedConsumerGroup.groupEpoch(), consumerGroup.groupEpoch());
        assertEquals(expectedConsumerGroup.state(), consumerGroup.state());
        assertEquals(expectedConsumerGroup.preferredServerAssignor(), consumerGroup.preferredServerAssignor());
        assertEquals(expectedConsumerGroup.members(), consumerGroup.members());
    }

    @Test
    public void testSubscribedRegularExpressionCount() {
        ConsumerGroup consumerGroup = createConsumerGroup("foo");

        ConsumerGroupMember member1 = new ConsumerGroupMember.Builder("member1")
            .setSubscribedTopicRegex("regex1")
            .build();
        ConsumerGroupMember member2 = new ConsumerGroupMember.Builder("member2")
            .setSubscribedTopicRegex("regex2")
            .build();
        ConsumerGroupMember member3 = new ConsumerGroupMember.Builder("member3")
            .setSubscribedTopicRegex("regex1")
            .build();
        ConsumerGroupMember member4 = new ConsumerGroupMember.Builder("member4")
            .build();

        // Assert the initial state.
        assertEquals(0, consumerGroup.numSubscribedMembers(""));
        assertEquals(0, consumerGroup.numSubscribedMembers("regex1"));
        assertEquals(0, consumerGroup.numSubscribedMembers("regex2"));
        assertEquals(0, consumerGroup.numSubscribedMembers("regex3"));

        // Add member 1.
        consumerGroup.updateMember(member1);
        assertEquals(0, consumerGroup.numSubscribedMembers(""));
        assertEquals(1, consumerGroup.numSubscribedMembers("regex1"));
        assertEquals(0, consumerGroup.numSubscribedMembers("regex2"));
        assertEquals(0, consumerGroup.numSubscribedMembers("regex3"));

        // Add member 2.
        consumerGroup.updateMember(member2);
        assertEquals(0, consumerGroup.numSubscribedMembers(""));
        assertEquals(1, consumerGroup.numSubscribedMembers("regex1"));
        assertEquals(1, consumerGroup.numSubscribedMembers("regex2"));
        assertEquals(0, consumerGroup.numSubscribedMembers("regex3"));

        // Add member 3.
        consumerGroup.updateMember(member3);
        assertEquals(0, consumerGroup.numSubscribedMembers(""));
        assertEquals(2, consumerGroup.numSubscribedMembers("regex1"));
        assertEquals(1, consumerGroup.numSubscribedMembers("regex2"));
        assertEquals(0, consumerGroup.numSubscribedMembers("regex3"));

        // Add member 4.
        consumerGroup.updateMember(member4);
        assertEquals(0, consumerGroup.numSubscribedMembers(""));
        assertEquals(2, consumerGroup.numSubscribedMembers("regex1"));
        assertEquals(1, consumerGroup.numSubscribedMembers("regex2"));
        assertEquals(0, consumerGroup.numSubscribedMembers("regex3"));

        // Update member 3.
        member3 = new ConsumerGroupMember.Builder(member3)
            .setSubscribedTopicRegex("regex2")
            .build();
        consumerGroup.updateMember(member3);
        assertEquals(0, consumerGroup.numSubscribedMembers(""));
        assertEquals(1, consumerGroup.numSubscribedMembers("regex1"));
        assertEquals(2, consumerGroup.numSubscribedMembers("regex2"));
        assertEquals(0, consumerGroup.numSubscribedMembers("regex3"));

        // Remove member 1.
        consumerGroup.removeMember(member1.memberId());
        assertEquals(0, consumerGroup.numSubscribedMembers(""));
        assertEquals(0, consumerGroup.numSubscribedMembers("regex1"));
        assertEquals(2, consumerGroup.numSubscribedMembers("regex2"));
        assertEquals(0, consumerGroup.numSubscribedMembers("regex3"));

        // Remove member 2.
        consumerGroup.removeMember(member2.memberId());
        assertEquals(0, consumerGroup.numSubscribedMembers(""));
        assertEquals(0, consumerGroup.numSubscribedMembers("regex1"));
        assertEquals(1, consumerGroup.numSubscribedMembers("regex2"));
        assertEquals(0, consumerGroup.numSubscribedMembers("regex3"));

        // Remove member 3.
        consumerGroup.removeMember(member3.memberId());
        assertEquals(0, consumerGroup.numSubscribedMembers(""));
        assertEquals(0, consumerGroup.numSubscribedMembers("regex1"));
        assertEquals(0, consumerGroup.numSubscribedMembers("regex2"));
        assertEquals(0, consumerGroup.numSubscribedMembers("regex3"));
    }

    @Test
    public void testUpdateAndRemoveRegularExpression() {
        ConsumerGroup consumerGroup = createConsumerGroup("foo");

        ConsumerGroupMember member1 = new ConsumerGroupMember.Builder("member1")
            .setSubscribedTopicNames(Arrays.asList("foo", "bar", "zar"))
            .build();
        consumerGroup.updateMember(member1);

        ConsumerGroupMember member2 = new ConsumerGroupMember.Builder("member2")
            .setSubscribedTopicNames(Arrays.asList("foo", "bar"))
            .build();
        consumerGroup.updateMember(member2);

        // Verify initial state.
        assertEquals(
            Map.of(
                "foo", 2,
                "bar", 2,
                "zar", 1
            ),
            consumerGroup.subscribedTopicNames()
        );

        // Add a regex.
        consumerGroup.updateResolvedRegularExpression(
            "foo|bar",
            new ResolvedRegularExpression(
                Set.of("foo", "bar"),
                10L,
                12345L
            )
        );

        assertEquals(
            Map.of(
                "foo", 3,
                "bar", 3,
                "zar", 1
            ),
            consumerGroup.subscribedTopicNames()
        );

        // Add a regex.
        consumerGroup.updateResolvedRegularExpression(
            "foobar",
            new ResolvedRegularExpression(
                Set.of("foobar"),
                10L,
                12345L
            )
        );

        assertEquals(
            Map.of(
                "foo", 3,
                "bar", 3,
                "zar", 1,
                "foobar", 1
            ),
            consumerGroup.subscribedTopicNames()
        );

        // Update a regex.
        consumerGroup.updateResolvedRegularExpression(
            "foo|bar",
            new ResolvedRegularExpression(
                Set.of("foo"),
                10L,
                12345L
            )
        );

        assertEquals(
            Map.of(
                "foo", 3,
                "bar", 2,
                "zar", 1,
                "foobar", 1
            ),
            consumerGroup.subscribedTopicNames()
        );

        // Remove a regex.
        consumerGroup.removeResolvedRegularExpression("foo|bar");

        assertEquals(
            Map.of(
                "foo", 2,
                "bar", 2,
                "zar", 1,
                "foobar", 1
            ),
            consumerGroup.subscribedTopicNames()
        );

        // Remove another regex.
        consumerGroup.removeResolvedRegularExpression("foobar");

        assertEquals(
            Map.of(
                "foo", 2,
                "bar", 2,
                "zar", 1
            ),
            consumerGroup.subscribedTopicNames()
        );
    }
}
