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
import org.apache.kafka.common.errors.GroupNotEmptyException;
import org.apache.kafka.common.errors.StaleMemberEpochException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.coordinator.group.GroupMetadataManagerTest;
import org.apache.kafka.coordinator.group.OffsetAndMetadata;
import org.apache.kafka.coordinator.group.OffsetExpirationCondition;
import org.apache.kafka.coordinator.group.OffsetExpirationConditionImpl;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkAssignment;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkTopicAssignment;
import static org.apache.kafka.coordinator.group.RecordHelpersTest.mkMapOfPartitionRacks;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
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
            .setMemberEpoch(11)
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(barTopicId, 1, 2, 3)))
            .setPartitionsPendingRevocation(mkAssignment(
                mkTopicAssignment(zarTopicId, 4, 5, 6)))
            .setPartitionsPendingAssignment(mkAssignment(
                mkTopicAssignment(fooTopicId, 7, 8, 9)))
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
        Uuid fooTopicId = Uuid.randomUuid();
        ConsumerGroup consumerGroup = createConsumerGroup("foo");
        assertEquals(ConsumerGroup.ConsumerGroupState.EMPTY, consumerGroup.state());

        ConsumerGroupMember member1 = new ConsumerGroupMember.Builder("member1")
            .setMemberEpoch(1)
            .setPreviousMemberEpoch(0)
            .setTargetMemberEpoch(1)
            .build();

        consumerGroup.updateMember(member1);
        consumerGroup.setGroupEpoch(1);

        assertEquals(ConsumerGroupMember.MemberState.STABLE, member1.state());
        assertEquals(ConsumerGroup.ConsumerGroupState.ASSIGNING, consumerGroup.state());

        ConsumerGroupMember member2 = new ConsumerGroupMember.Builder("member2")
            .setMemberEpoch(1)
            .setPreviousMemberEpoch(0)
            .setTargetMemberEpoch(1)
            .build();

        consumerGroup.updateMember(member2);
        consumerGroup.setGroupEpoch(2);

        assertEquals(ConsumerGroupMember.MemberState.STABLE, member2.state());
        assertEquals(ConsumerGroup.ConsumerGroupState.ASSIGNING, consumerGroup.state());

        consumerGroup.setTargetAssignmentEpoch(2);

        assertEquals(ConsumerGroup.ConsumerGroupState.RECONCILING, consumerGroup.state());

        member1 = new ConsumerGroupMember.Builder(member1)
            .setMemberEpoch(2)
            .setPreviousMemberEpoch(1)
            .setTargetMemberEpoch(2)
            .build();

        consumerGroup.updateMember(member1);

        assertEquals(ConsumerGroupMember.MemberState.STABLE, member1.state());
        assertEquals(ConsumerGroup.ConsumerGroupState.RECONCILING, consumerGroup.state());

        // Member 2 is not stable so the group stays in reconciling state.
        member2 = new ConsumerGroupMember.Builder(member2)
            .setMemberEpoch(2)
            .setPreviousMemberEpoch(1)
            .setTargetMemberEpoch(2)
            .setPartitionsPendingAssignment(mkAssignment(
                mkTopicAssignment(fooTopicId, 0)))
            .build();

        consumerGroup.updateMember(member2);

        assertEquals(ConsumerGroupMember.MemberState.ASSIGNING, member2.state());
        assertEquals(ConsumerGroup.ConsumerGroupState.RECONCILING, consumerGroup.state());

        member2 = new ConsumerGroupMember.Builder(member2)
            .setMemberEpoch(2)
            .setPreviousMemberEpoch(1)
            .setTargetMemberEpoch(2)
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(fooTopicId, 0)))
            .setPartitionsPendingAssignment(Collections.emptyMap())
            .build();

        consumerGroup.updateMember(member2);

        assertEquals(ConsumerGroupMember.MemberState.STABLE, member2.state());
        assertEquals(ConsumerGroup.ConsumerGroupState.STABLE, consumerGroup.state());

        consumerGroup.removeMember("member1");
        consumerGroup.removeMember("member2");

        assertEquals(ConsumerGroup.ConsumerGroupState.EMPTY, consumerGroup.state());
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

        MetadataImage image = new GroupMetadataManagerTest.MetadataImageBuilder()
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
                null,
                null,
                image.topics(),
                image.cluster()
            )
        );

        // Compute while taking into account member 1.
        assertEquals(
            mkMap(
                mkEntry("foo", new TopicMetadata(fooTopicId, "foo", 1, mkMapOfPartitionRacks(1)))
            ),
            consumerGroup.computeSubscriptionMetadata(
                null,
                member1,
                image.topics(),
                image.cluster()
            )
        );

        // Updating the group with member1.
        consumerGroup.updateMember(member1);

        // It should return foo now.
        assertEquals(
            mkMap(
                mkEntry("foo", new TopicMetadata(fooTopicId, "foo", 1, mkMapOfPartitionRacks(1)))
            ),
            consumerGroup.computeSubscriptionMetadata(
                null,
                null,
                image.topics(),
                image.cluster()
            )
        );

        // Compute while taking into account removal of member 1.
        assertEquals(
            Collections.emptyMap(),
            consumerGroup.computeSubscriptionMetadata(
                member1,
                null,
                image.topics(),
                image.cluster()
            )
        );

        // Compute while taking into account member 2.
        assertEquals(
            mkMap(
                mkEntry("foo", new TopicMetadata(fooTopicId, "foo", 1, mkMapOfPartitionRacks(1))),
                mkEntry("bar", new TopicMetadata(barTopicId, "bar", 2, mkMapOfPartitionRacks(2)))
            ),
            consumerGroup.computeSubscriptionMetadata(
                null,
                member2,
                image.topics(),
                image.cluster()
            )
        );

        // Updating the group with member2.
        consumerGroup.updateMember(member2);

        // It should return foo and bar.
        assertEquals(
            mkMap(
                mkEntry("foo", new TopicMetadata(fooTopicId, "foo", 1, mkMapOfPartitionRacks(1))),
                mkEntry("bar", new TopicMetadata(barTopicId, "bar", 2, mkMapOfPartitionRacks(2)))
            ),
            consumerGroup.computeSubscriptionMetadata(
                null,
                null,
                image.topics(),
                image.cluster()
            )
        );

        // Compute while taking into account removal of member 2.
        assertEquals(
            mkMap(
                mkEntry("foo", new TopicMetadata(fooTopicId, "foo", 1, mkMapOfPartitionRacks(1)))
            ),
            consumerGroup.computeSubscriptionMetadata(
                member2,
                null,
                image.topics(),
                image.cluster()
            )
        );

        // Removing member1 results in returning bar.
        assertEquals(
            mkMap(
                mkEntry("bar", new TopicMetadata(barTopicId, "bar", 2, mkMapOfPartitionRacks(2)))
            ),
            consumerGroup.computeSubscriptionMetadata(
                member1,
                null,
                image.topics(),
                image.cluster()
            )
        );

        // Compute while taking into account member 3.
        assertEquals(
            mkMap(
                mkEntry("foo", new TopicMetadata(fooTopicId, "foo", 1, mkMapOfPartitionRacks(1))),
                mkEntry("bar", new TopicMetadata(barTopicId, "bar", 2, mkMapOfPartitionRacks(2))),
                mkEntry("zar", new TopicMetadata(zarTopicId, "zar", 3, mkMapOfPartitionRacks(3)))
            ),
            consumerGroup.computeSubscriptionMetadata(
                null,
                member3,
                image.topics(),
                image.cluster()
            )
        );

        // Updating group with member3.
        consumerGroup.updateMember(member3);

        // It should return foo, bar and zar.
        assertEquals(
            mkMap(
                mkEntry("foo", new TopicMetadata(fooTopicId, "foo", 1, mkMapOfPartitionRacks(1))),
                mkEntry("bar", new TopicMetadata(barTopicId, "bar", 2, mkMapOfPartitionRacks(2))),
                mkEntry("zar", new TopicMetadata(zarTopicId, "zar", 3, mkMapOfPartitionRacks(3)))
            ),
            consumerGroup.computeSubscriptionMetadata(
                null,
                null,
                image.topics(),
                image.cluster()
            )
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

    @Test
    public void testValidateOffsetCommit() {
        ConsumerGroup group = createConsumerGroup("group-foo");

        // Simulate a call from the admin client without member id and member epoch.
        // This should pass only if the group is empty.
        group.validateOffsetCommit("", "", -1);

        // The member does not exist.
        assertThrows(UnknownMemberIdException.class, () ->
            group.validateOffsetCommit("member-id", null, 0));

        // Create a member.
        group.getOrMaybeCreateMember("member-id", true);

        // A call from the admin client should fail as the group is not empty.
        assertThrows(UnknownMemberIdException.class, () ->
            group.validateOffsetCommit("", "", -1));

        // The member epoch is stale.
        assertThrows(StaleMemberEpochException.class, () ->
            group.validateOffsetCommit("member-id", "", 10));

        // This should succeed.
        group.validateOffsetCommit("member-id", "", 0);
    }

    @Test
    public void testAsListedGroup() {
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(new LogContext());
        ConsumerGroup group = new ConsumerGroup(snapshotRegistry, "group-foo");
        snapshotRegistry.getOrCreateSnapshot(0);
        assertEquals(ConsumerGroup.ConsumerGroupState.EMPTY.toString(), group.stateAsString(0));
        group.updateMember(new ConsumerGroupMember.Builder("member1")
            .setSubscribedTopicNames(Collections.singletonList("foo"))
            .build());
        snapshotRegistry.getOrCreateSnapshot(1);
        assertEquals(ConsumerGroup.ConsumerGroupState.EMPTY.toString(), group.stateAsString(0));
        assertEquals(ConsumerGroup.ConsumerGroupState.STABLE.toString(), group.stateAsString(1));
    }

    @Test
    public void testValidateOffsetFetch() {
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(new LogContext());
        ConsumerGroup group = new ConsumerGroup(snapshotRegistry, "group-foo");

        // Simulate a call from the admin client without member id and member epoch.
        group.validateOffsetFetch(null, -1, Long.MAX_VALUE);

        // The member does not exist.
        assertThrows(UnknownMemberIdException.class, () ->
            group.validateOffsetFetch("member-id", 0, Long.MAX_VALUE));

        // Create a member.
        snapshotRegistry.getOrCreateSnapshot(0);
        group.getOrMaybeCreateMember("member-id", true);

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
            .setTargetMemberEpoch(1)
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
        ConsumerGroup group = new ConsumerGroup(new SnapshotRegistry(new LogContext()), "group-id");

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

        MetadataImage image = new GroupMetadataManagerTest.MetadataImageBuilder()
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
                mkEntry("foo", new TopicMetadata(fooTopicId, "foo", 1, mkMapOfPartitionRacks(1))),
                mkEntry("bar", new TopicMetadata(barTopicId, "bar", 2, mkMapOfPartitionRacks(2)))
            ),
            consumerGroup.computeSubscriptionMetadata(
                null,
                null,
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
}
