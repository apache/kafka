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

import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.group.classic.ClassicGroup;
import org.apache.kafka.coordinator.group.classic.ClassicGroupMember;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupCurrentMemberAssignmentKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupCurrentMemberAssignmentValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMemberMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMemberMetadataValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMetadataValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupPartitionMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupPartitionMetadataValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupRegularExpressionKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupRegularExpressionValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMemberKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMemberValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMetadataValue;
import org.apache.kafka.coordinator.group.generated.CoordinatorRecordType;
import org.apache.kafka.coordinator.group.generated.GroupMetadataKey;
import org.apache.kafka.coordinator.group.generated.GroupMetadataValue;
import org.apache.kafka.coordinator.group.generated.ShareGroupCurrentMemberAssignmentKey;
import org.apache.kafka.coordinator.group.generated.ShareGroupCurrentMemberAssignmentValue;
import org.apache.kafka.coordinator.group.generated.ShareGroupMemberMetadataKey;
import org.apache.kafka.coordinator.group.generated.ShareGroupMemberMetadataValue;
import org.apache.kafka.coordinator.group.generated.ShareGroupMetadataKey;
import org.apache.kafka.coordinator.group.generated.ShareGroupMetadataValue;
import org.apache.kafka.coordinator.group.generated.ShareGroupPartitionMetadataKey;
import org.apache.kafka.coordinator.group.generated.ShareGroupPartitionMetadataValue;
import org.apache.kafka.coordinator.group.generated.ShareGroupTargetAssignmentMemberKey;
import org.apache.kafka.coordinator.group.generated.ShareGroupTargetAssignmentMemberValue;
import org.apache.kafka.coordinator.group.generated.ShareGroupTargetAssignmentMetadataKey;
import org.apache.kafka.coordinator.group.generated.ShareGroupTargetAssignmentMetadataValue;
import org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetricsShard;
import org.apache.kafka.coordinator.group.modern.MemberState;
import org.apache.kafka.coordinator.group.modern.TopicMetadata;
import org.apache.kafka.coordinator.group.modern.consumer.ConsumerGroup;
import org.apache.kafka.coordinator.group.modern.consumer.ConsumerGroupMember;
import org.apache.kafka.coordinator.group.modern.consumer.ResolvedRegularExpression;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.timeline.SnapshotRegistry;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;

import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkAssignment;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkTopicAssignment;
import static org.apache.kafka.coordinator.group.Group.GroupType.CONSUMER;
import static org.apache.kafka.coordinator.group.classic.ClassicGroupState.STABLE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

public class GroupMetadataStoreTest {
    private GroupMetadataStore groupMetadataStore;
    private Time time;

    @BeforeEach
    public void setUp() {
        time = mock(Time.SYSTEM.getClass());
        groupMetadataStore = new GroupMetadataStore(new SnapshotRegistry(new LogContext()), mock(GroupCoordinatorMetricsShard.class), time);
    }

    @Test
    public void testGroupIdsByTopics() {
        String groupId1 = "group1";
        String groupId2 = "group2";

        assertEquals(Collections.emptySet(), groupMetadataStore.groupsSubscribedToTopic("foo"));
        assertEquals(Collections.emptySet(), groupMetadataStore.groupsSubscribedToTopic("bar"));
        assertEquals(Collections.emptySet(), groupMetadataStore.groupsSubscribedToTopic("zar"));

        // M1 in group 1 subscribes to foo and bar.
        replay(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId1,
            new ConsumerGroupMember.Builder("group1-m1")
                .setSubscribedTopicNames(List.of("foo", "bar"))
                .build()));

        assertEquals(Set.of(groupId1), groupMetadataStore.groupsSubscribedToTopic("foo"));
        assertEquals(Set.of(groupId1), groupMetadataStore.groupsSubscribedToTopic("bar"));
        assertEquals(Collections.emptySet(), groupMetadataStore.groupsSubscribedToTopic("zar"));

        // M1 in group 2 subscribes to foo, bar and zar.
        replay(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId2,
            new ConsumerGroupMember.Builder("group2-m1")
                .setSubscribedTopicNames(List.of("foo", "bar", "zar"))
                .build()));

        assertEquals(Set.of(groupId1, groupId2), groupMetadataStore.groupsSubscribedToTopic("foo"));
        assertEquals(Set.of(groupId1, groupId2), groupMetadataStore.groupsSubscribedToTopic("bar"));
        assertEquals(Set.of(groupId2), groupMetadataStore.groupsSubscribedToTopic("zar"));

        // M2 in group 1 subscribes to bar and zar.
        replay(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId1,
            new ConsumerGroupMember.Builder("group1-m2")
                .setSubscribedTopicNames(List.of("bar", "zar"))
                .build()));

        assertEquals(Set.of(groupId1, groupId2), groupMetadataStore.groupsSubscribedToTopic("foo"));
        assertEquals(Set.of(groupId1, groupId2), groupMetadataStore.groupsSubscribedToTopic("bar"));
        assertEquals(Set.of(groupId1, groupId2), groupMetadataStore.groupsSubscribedToTopic("zar"));

        // M2 in group 2 subscribes to foo and bar.
        replay(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId2,
            new ConsumerGroupMember.Builder("group2-m2")
                .setSubscribedTopicNames(List.of("foo", "bar"))
                .build()));

        assertEquals(Set.of(groupId1, groupId2), groupMetadataStore.groupsSubscribedToTopic("foo"));
        assertEquals(Set.of(groupId1, groupId2), groupMetadataStore.groupsSubscribedToTopic("bar"));
        assertEquals(Set.of(groupId1, groupId2), groupMetadataStore.groupsSubscribedToTopic("zar"));

        // M1 in group 1 is removed.
        replay(GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord(groupId1, "group1-m1"));
        replay(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord(groupId1, "group1-m1"));

        assertEquals(Set.of(groupId2), groupMetadataStore.groupsSubscribedToTopic("foo"));
        assertEquals(Set.of(groupId1, groupId2), groupMetadataStore.groupsSubscribedToTopic("bar"));
        assertEquals(Set.of(groupId1, groupId2), groupMetadataStore.groupsSubscribedToTopic("zar"));

        // M1 in group 2 subscribes to nothing.
        replay(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId2,
            new ConsumerGroupMember.Builder("group2-m1")
                .setSubscribedTopicNames(Collections.emptyList())
                .build()));

        assertEquals(Set.of(groupId2), groupMetadataStore.groupsSubscribedToTopic("foo"));
        assertEquals(Set.of(groupId1, groupId2), groupMetadataStore.groupsSubscribedToTopic("bar"));
        assertEquals(Set.of(groupId1), groupMetadataStore.groupsSubscribedToTopic("zar"));

        // M2 in group 2 subscribes to foo.
        replay(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId2,
            new ConsumerGroupMember.Builder("group2-m2")
                .setSubscribedTopicNames(Collections.singletonList("foo"))
                .build()));

        assertEquals(Set.of(groupId2), groupMetadataStore.groupsSubscribedToTopic("foo"));
        assertEquals(Set.of(groupId1), groupMetadataStore.groupsSubscribedToTopic("bar"));
        assertEquals(Set.of(groupId1), groupMetadataStore.groupsSubscribedToTopic("zar"));

        // M2 in group 2 subscribes to nothing.
        replay(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId2,
            new ConsumerGroupMember.Builder("group2-m2")
                .setSubscribedTopicNames(Collections.emptyList())
                .build()));

        assertEquals(Collections.emptySet(), groupMetadataStore.groupsSubscribedToTopic("foo"));
        assertEquals(Set.of(groupId1), groupMetadataStore.groupsSubscribedToTopic("bar"));
        assertEquals(Set.of(groupId1), groupMetadataStore.groupsSubscribedToTopic("zar"));

        // M2 in group 1 subscribes to nothing.
        replay(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord(groupId1,
            new ConsumerGroupMember.Builder("group1-m2")
                .setSubscribedTopicNames(Collections.emptyList())
                .build()));

        assertEquals(Collections.emptySet(), groupMetadataStore.groupsSubscribedToTopic("foo"));
        assertEquals(Collections.emptySet(), groupMetadataStore.groupsSubscribedToTopic("bar"));
        assertEquals(Collections.emptySet(), groupMetadataStore.groupsSubscribedToTopic("zar"));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testReplayGroupMetadataRecords(boolean useDefaultRebalanceTimeout) {

        byte[] subscription = ConsumerProtocol.serializeSubscription(new ConsumerPartitionAssignor.Subscription(
            Collections.singletonList("foo"))).array();
        List<GroupMetadataValue.MemberMetadata> members = new ArrayList<>();
        List<ClassicGroupMember> expectedMembers = new ArrayList<>();
        JoinGroupRequestData.JoinGroupRequestProtocolCollection expectedProtocols = new JoinGroupRequestData.JoinGroupRequestProtocolCollection(0);
        expectedProtocols.add(new JoinGroupRequestData.JoinGroupRequestProtocol()
            .setName("range")
            .setMetadata(subscription));

        IntStream.range(0, 2).forEach(i -> {
            members.add(new GroupMetadataValue.MemberMetadata()
                .setMemberId("member-" + i)
                .setGroupInstanceId("group-instance-id-" + i)
                .setSubscription(subscription)
                .setAssignment(new byte[]{2})
                .setClientId("client-" + i)
                .setClientHost("host-" + i)
                .setSessionTimeout(4000)
                .setRebalanceTimeout(useDefaultRebalanceTimeout ? -1 : 9000)
            );

            expectedMembers.add(new ClassicGroupMember(
                "member-" + i,
                Optional.of("group-instance-id-" + i),
                "client-" + i,
                "host-" + i,
                useDefaultRebalanceTimeout ? 4000 : 9000,
                4000,
                "consumer",
                expectedProtocols,
                new byte[]{2}
            ));
        });

        CoordinatorRecord groupMetadataRecord = GroupMetadataManagerTestContext.newGroupMetadataRecord("group-id",
            new GroupMetadataValue()
                .setMembers(members)
                .setGeneration(1)
                .setLeader("member-0")
                .setProtocolType("consumer")
                .setProtocol("range")
                .setCurrentStateTimestamp(time.milliseconds()));
        replay(groupMetadataRecord);
        ClassicGroup group = groupMetadataStore.getOrMaybeCreateClassicGroup("group-id", false);

        ClassicGroup expectedGroup = new ClassicGroup(
            new LogContext(),
            "group-id",
            STABLE,
            time,
            1,
            Optional.of("consumer"),
            Optional.of("range"),
            Optional.of("member-0"),
            Optional.of(time.milliseconds())
        );
        expectedMembers.forEach(expectedGroup::add);

        assertEquals(expectedGroup.groupId(), group.groupId());
        assertEquals(expectedGroup.generationId(), group.generationId());
        assertEquals(expectedGroup.protocolType(), group.protocolType());
        assertEquals(expectedGroup.protocolName(), group.protocolName());
        assertEquals(expectedGroup.leaderOrNull(), group.leaderOrNull());
        assertEquals(expectedGroup.currentState(), group.currentState());
        assertEquals(expectedGroup.currentStateTimestampOrDefault(), group.currentStateTimestampOrDefault());
        assertEquals(expectedGroup.currentClassicGroupMembers(), group.currentClassicGroupMembers());
    }

    @Test
    public void testReplayConsumerGroupMemberMetadata() {
        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member")
            .setClientId("clientid")
            .setClientHost("clienthost")
            .setServerAssignorName("range")
            .setRackId("rackid")
            .setSubscribedTopicNames(Collections.singletonList("foo"))
            .build();

        // The group and the member are created if they do not exist.
        replay(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionRecord("foo", member));
        assertEquals(member, consumerGroup("foo").getOrMaybeCreateMember("member", false));
    }

    @Test
    public void testReplayConsumerGroupMemberMetadataTombstone() {
        // The group still exists but the member is already gone. Replaying the
        // ConsumerGroupMemberMetadata tombstone should be a no-op.
        replay(GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord("foo", 10));
        replay(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord("foo", "m1"));
        assertThrows(UnknownMemberIdException.class, () -> consumerGroup("foo").getOrMaybeCreateMember("m1", false));

        // The group may not exist at all. Replaying the ConsumerGroupMemberMetadata tombstone
        // should a no-op.
        replay(GroupCoordinatorRecordHelpers.newConsumerGroupMemberSubscriptionTombstoneRecord("bar", "m1"));
        assertThrows(GroupIdNotFoundException.class, () -> consumerGroup("bar"));
    }

    @Test
    public void testReplayConsumerGroupMetadata() {
        // The group is created if it does not exist.
        replay(GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord("foo", 10));
        assertEquals(10, consumerGroup("foo").groupEpoch());
    }

    @Test
    public void testReplayConsumerGroupMetadataTombstone() {
        // The group may not exist at all. Replaying the ConsumerGroupMetadata tombstone
        // should be a no-op.
        replay(GroupCoordinatorRecordHelpers.newConsumerGroupEpochTombstoneRecord("foo"));
        assertThrows(GroupIdNotFoundException.class, () -> consumerGroup("foo"));
    }

    @Test
    public void testReplayConsumerGroupPartitionMetadata() {
        Map<String, TopicMetadata> metadata = Collections.singletonMap(
            "bar",
            new TopicMetadata(Uuid.randomUuid(), "bar", 10)
        );

        // The group is created if it does not exist.
        replay(GroupCoordinatorRecordHelpers.newConsumerGroupSubscriptionMetadataRecord("foo", metadata));
        assertEquals(metadata, consumerGroup("foo").subscriptionMetadata());
    }

    @Test
    public void testReplayConsumerGroupPartitionMetadataTombstone() {
        // The group may not exist at all. Replaying the ConsumerGroupPartitionMetadata tombstone
        // should be a no-op.
        replay(GroupCoordinatorRecordHelpers.newConsumerGroupSubscriptionMetadataTombstoneRecord("foo"));
        assertThrows(GroupIdNotFoundException.class, () -> consumerGroup("foo"));
    }

    @Test
    public void testReplayConsumerGroupTargetAssignmentMember() {
        Map<Uuid, Set<Integer>> assignment = mkAssignment(
            mkTopicAssignment(Uuid.randomUuid(), 0, 1, 2)
        );

        // The group is created if it does not exist.
        replay(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentRecord("foo", "m1", assignment));
        assertEquals(assignment, consumerGroup("foo").targetAssignment("m1").partitions());
    }

    @Test
    public void testReplayConsumerGroupTargetAssignmentMemberTombstone() {
        // The group may not exist at all. Replaying the ConsumerGroupTargetAssignmentMember tombstone
        // should be a no-op.
        replay(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentTombstoneRecord("foo", "m1"));
        assertThrows(GroupIdNotFoundException.class, () -> consumerGroup("foo"));
    }

    @Test
    public void testReplayConsumerGroupTargetAssignmentMetadata() {
        // The group is created if it does not exist.
        replay(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentEpochRecord("foo", 10));
        assertEquals(10, consumerGroup("foo").assignmentEpoch());
    }

    @Test
    public void testReplayConsumerGroupTargetAssignmentMetadataTombstone() {
        // The group may not exist at all. Replaying the ConsumerGroupTargetAssignmentMetadata tombstone
        // should be a no-op.
        replay(GroupCoordinatorRecordHelpers.newConsumerGroupTargetAssignmentEpochTombstoneRecord("foo"));
        assertThrows(GroupIdNotFoundException.class, () -> consumerGroup("foo"));
    }

    @Test
    public void testReplayConsumerGroupCurrentMemberAssignment() {
        ConsumerGroupMember member = new ConsumerGroupMember.Builder("member")
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setState(MemberState.UNRELEASED_PARTITIONS)
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(Uuid.randomUuid(), 0, 1, 2)))
            .build();

        // The group and the member are created if they do not exist.
        replay(GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentRecord("bar", member));
        assertEquals(member, consumerGroup("bar").getOrMaybeCreateMember("member", false));
    }

    @Test
    public void testReplayConsumerGroupCurrentMemberAssignmentTombstone() {
        // The group still exists but the member is already gone. Replaying the
        // ConsumerGroupCurrentMemberAssignment tombstone should be a no-op.
        replay(GroupCoordinatorRecordHelpers.newConsumerGroupEpochRecord("foo", 10));
        replay(GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord("foo", "m1"));
        assertThrows(UnknownMemberIdException.class, () -> consumerGroup("foo").getOrMaybeCreateMember("m1", false));

        // The group may not exist at all. Replaying the ConsumerGroupCurrentMemberAssignment tombstone
        // should be a no-op.
        replay(GroupCoordinatorRecordHelpers.newConsumerGroupCurrentAssignmentTombstoneRecord("bar", "m1"));
        assertThrows(GroupIdNotFoundException.class, () -> consumerGroup("bar"));
    }

    @Test
    public void testReplayConsumerGroupRegularExpression() {
        ResolvedRegularExpression resolvedRegularExpression = new ResolvedRegularExpression(
            Set.of("abc", "abcd"),
            10L,
            12345L
        );

        replay(GroupCoordinatorRecordHelpers.newConsumerGroupRegularExpressionRecord(
            "foo",
            "abc*",
            resolvedRegularExpression
        ));

        assertEquals(
            Optional.of(resolvedRegularExpression),
            consumerGroup("foo").resolvedRegularExpression("abc*")
        );
    }

    @Test
    public void testReplayConsumerGroupRegularExpressionTombstone() {
        // The group may not exist at all. Replaying the ConsumerGroupRegularExpression tombstone
        // should be a no-op.
        replay(GroupCoordinatorRecordHelpers.newConsumerGroupRegularExpressionTombstone("foo", "abc*"));
        assertThrows(GroupIdNotFoundException.class, () -> consumerGroup("foo"));

        // Otherwise, it should remove the regular expression.
        ResolvedRegularExpression resolvedRegularExpression = new ResolvedRegularExpression(
            Set.of("abc", "abcd"),
            10L,
            12345L
        );

        replay(GroupCoordinatorRecordHelpers.newConsumerGroupRegularExpressionRecord(
            "foo",
            "abc*",
            resolvedRegularExpression
        ));

        replay(GroupCoordinatorRecordHelpers.newConsumerGroupRegularExpressionTombstone(
            "foo",
            "abc*"
        ));

        assertEquals(
            Optional.empty(),
            consumerGroup("foo").resolvedRegularExpression("abc*")
        );
    }

    private void replay(
        CoordinatorRecord record
    ) {
        ApiMessage key = record.key();
        ApiMessageAndVersion value = record.value();

        if (key == null) {
            throw new IllegalStateException("Received a null key in " + record);
        }

        switch (CoordinatorRecordType.fromId(record.key().apiKey())) {
            case GROUP_METADATA:
                groupMetadataStore.replay(
                    (GroupMetadataKey) key,
                    (GroupMetadataValue) messageOrNull(value)
                );
                break;
            case CONSUMER_GROUP_MEMBER_METADATA:
                groupMetadataStore.replay(
                    (ConsumerGroupMemberMetadataKey) key,
                    (ConsumerGroupMemberMetadataValue) messageOrNull(value)
                );
                break;

            case CONSUMER_GROUP_METADATA:
                groupMetadataStore.replay(
                    (ConsumerGroupMetadataKey) key,
                    (ConsumerGroupMetadataValue) messageOrNull(value)
                );
                break;

            case CONSUMER_GROUP_PARTITION_METADATA:
                groupMetadataStore.replay(
                    (ConsumerGroupPartitionMetadataKey) key,
                    (ConsumerGroupPartitionMetadataValue) messageOrNull(value)
                );
                break;

            case CONSUMER_GROUP_TARGET_ASSIGNMENT_MEMBER:
                groupMetadataStore.replay(
                    (ConsumerGroupTargetAssignmentMemberKey) key,
                    (ConsumerGroupTargetAssignmentMemberValue) messageOrNull(value)
                );
                break;

            case CONSUMER_GROUP_TARGET_ASSIGNMENT_METADATA:
                groupMetadataStore.replay(
                    (ConsumerGroupTargetAssignmentMetadataKey) key,
                    (ConsumerGroupTargetAssignmentMetadataValue) messageOrNull(value)
                );
                break;

            case CONSUMER_GROUP_CURRENT_MEMBER_ASSIGNMENT:
                groupMetadataStore.replay(
                    (ConsumerGroupCurrentMemberAssignmentKey) key,
                    (ConsumerGroupCurrentMemberAssignmentValue) messageOrNull(value)
                );
                break;

            case SHARE_GROUP_MEMBER_METADATA:
                groupMetadataStore.replay(
                    (ShareGroupMemberMetadataKey) key,
                    (ShareGroupMemberMetadataValue) messageOrNull(value)
                );
                break;

            case SHARE_GROUP_METADATA:
                groupMetadataStore.replay(
                    (ShareGroupMetadataKey) key,
                    (ShareGroupMetadataValue) messageOrNull(value)
                );
                break;

            case SHARE_GROUP_PARTITION_METADATA:
                groupMetadataStore.replay(
                    (ShareGroupPartitionMetadataKey) key,
                    (ShareGroupPartitionMetadataValue) messageOrNull(value)
                );
                break;

            case SHARE_GROUP_TARGET_ASSIGNMENT_MEMBER:
                groupMetadataStore.replay(
                    (ShareGroupTargetAssignmentMemberKey) key,
                    (ShareGroupTargetAssignmentMemberValue) messageOrNull(value)
                );
                break;

            case SHARE_GROUP_TARGET_ASSIGNMENT_METADATA:
                groupMetadataStore.replay(
                    (ShareGroupTargetAssignmentMetadataKey) key,
                    (ShareGroupTargetAssignmentMetadataValue) messageOrNull(value)
                );
                break;

            case SHARE_GROUP_CURRENT_MEMBER_ASSIGNMENT:
                groupMetadataStore.replay(
                    (ShareGroupCurrentMemberAssignmentKey) key,
                    (ShareGroupCurrentMemberAssignmentValue) messageOrNull(value)
                );
                break;

            case CONSUMER_GROUP_REGULAR_EXPRESSION:
                groupMetadataStore.replay(
                    (ConsumerGroupRegularExpressionKey) key,
                    (ConsumerGroupRegularExpressionValue) messageOrNull(value)
                );
                break;

            default:
                throw new IllegalStateException("Received an unknown record type " + record.key().apiKey()
                    + " in " + record);
        }
    }

    private ApiMessage messageOrNull(ApiMessageAndVersion apiMessageAndVersion) {
        if (apiMessageAndVersion == null) {
            return null;
        } else {
            return apiMessageAndVersion.message();
        }
    }

    private ConsumerGroup consumerGroup(
        String groupId
    ) throws GroupIdNotFoundException {
        Group group = groupMetadataStore.group(groupId, Long.MAX_VALUE);

        if (group.type() == CONSUMER) {
            return (ConsumerGroup) group;
        } else {
            throw new GroupIdNotFoundException(String.format("Group %s is not a consumer group", groupId));
        }
    }
}