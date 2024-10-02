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
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.GroupNotEmptyException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.message.ShareGroupDescribeResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.annotation.ApiKeyVersionsSource;
import org.apache.kafka.coordinator.group.Group;
import org.apache.kafka.coordinator.group.MetadataImageBuilder;
import org.apache.kafka.coordinator.group.modern.Assignment;
import org.apache.kafka.coordinator.group.modern.MemberState;
import org.apache.kafka.coordinator.group.modern.TopicMetadata;
import org.apache.kafka.coordinator.group.modern.share.ShareGroup.ShareGroupState;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.timeline.SnapshotRegistry;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.coordinator.group.api.assignor.SubscriptionType.HETEROGENEOUS;
import static org.apache.kafka.coordinator.group.api.assignor.SubscriptionType.HOMOGENEOUS;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ShareGroupTest {

    @Test
    public void testType() {
        ShareGroup shareGroup = createShareGroup("foo");
        assertEquals(Group.GroupType.SHARE, shareGroup.type());
    }

    @Test
    public void testProtocolType() {
        ShareGroup shareGroup = createShareGroup("foo");
        assertEquals("share", shareGroup.protocolType());
    }

    @Test
    public void testGetOrCreateMember() {
        ShareGroup shareGroup = createShareGroup("foo");
        ShareGroupMember member;

        // Create a member.
        member = shareGroup.getOrMaybeCreateMember("member-id", true);
        assertEquals("member-id", member.memberId());

        // Add member to the group.
        shareGroup.updateMember(member);

        // Get that member back.
        member = shareGroup.getOrMaybeCreateMember("member-id", false);
        assertEquals("member-id", member.memberId());

        assertThrows(UnknownMemberIdException.class, () ->
            shareGroup.getOrMaybeCreateMember("does-not-exist", false));
    }

    @Test
    public void testUpdateMember() {
        ShareGroup shareGroup = createShareGroup("foo");
        ShareGroupMember member;

        member = shareGroup.getOrMaybeCreateMember("member", true);

        member = new ShareGroupMember.Builder(member)
            .setSubscribedTopicNames(Arrays.asList("foo", "bar"))
            .build();

        shareGroup.updateMember(member);

        assertEquals(member, shareGroup.getOrMaybeCreateMember("member", false));
    }

    @Test
    public void testRemoveMember() {
        ShareGroup shareGroup = createShareGroup("foo");

        ShareGroupMember member = shareGroup.getOrMaybeCreateMember("member", true);
        shareGroup.updateMember(member);
        assertTrue(shareGroup.hasMember("member"));

        shareGroup.removeMember("member");
        assertFalse(shareGroup.hasMember("member"));

    }

    @Test
    public void testGroupState() {
        ShareGroup shareGroup = createShareGroup("foo");
        assertEquals(ShareGroup.ShareGroupState.EMPTY, shareGroup.state());
        assertEquals("Empty", shareGroup.stateAsString());

        ShareGroupMember member1 = new ShareGroupMember.Builder("member1")
            .setState(MemberState.STABLE)
            .setMemberEpoch(1)
            .setPreviousMemberEpoch(0)
            .build();

        shareGroup.updateMember(member1);
        shareGroup.setGroupEpoch(1);

        assertEquals(MemberState.STABLE, member1.state());
        assertEquals(ShareGroupState.STABLE, shareGroup.state());
        assertEquals("Stable", shareGroup.stateAsString());
    }

    @Test
    public void testGroupTypeFromString() {
        assertEquals(Group.GroupType.parse("share"), Group.GroupType.SHARE);
        // Test case insensitivity.
        assertEquals(Group.GroupType.parse("Share"), Group.GroupType.SHARE);
        assertEquals(Group.GroupType.parse("SHare"), Group.GroupType.SHARE);
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

        ShareGroupMember member1 = new ShareGroupMember.Builder("member1")
            .setSubscribedTopicNames(Collections.singletonList("foo"))
            .build();
        ShareGroupMember member2 = new ShareGroupMember.Builder("member2")
            .setSubscribedTopicNames(Collections.singletonList("bar"))
            .build();
        ShareGroupMember member3 = new ShareGroupMember.Builder("member3")
            .setSubscribedTopicNames(Collections.singletonList("zar"))
            .build();

        ShareGroup shareGroup = createShareGroup("group-foo");

        // It should be empty by default.
        assertEquals(
            Collections.emptyMap(),
            shareGroup.computeSubscriptionMetadata(
                shareGroup.computeSubscribedTopicNames(null, null),
                image.topics(),
                image.cluster()
            )
        );

        // Compute while taking into account member 1.
        assertEquals(
            mkMap(
                mkEntry("foo", new TopicMetadata(fooTopicId, "foo", 1))
            ),
            shareGroup.computeSubscriptionMetadata(
                shareGroup.computeSubscribedTopicNames(null, member1),
                image.topics(),
                image.cluster()
            )
        );

        // Updating the group with member1.
        shareGroup.updateMember(member1);

        // It should return foo now.
        assertEquals(
            mkMap(
                mkEntry("foo", new TopicMetadata(fooTopicId, "foo", 1))
            ),
            shareGroup.computeSubscriptionMetadata(
                shareGroup.computeSubscribedTopicNames(null, null),
                image.topics(),
                image.cluster()
            )
        );

        // Compute while taking into account removal of member 1.
        assertEquals(
            Collections.emptyMap(),
            shareGroup.computeSubscriptionMetadata(
                shareGroup.computeSubscribedTopicNames(member1, null),
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
            shareGroup.computeSubscriptionMetadata(
                shareGroup.computeSubscribedTopicNames(null, member2),
                image.topics(),
                image.cluster()
            )
        );

        // Updating the group with member2.
        shareGroup.updateMember(member2);

        // It should return foo and bar.
        assertEquals(
            mkMap(
                mkEntry("foo", new TopicMetadata(fooTopicId, "foo", 1)),
                mkEntry("bar", new TopicMetadata(barTopicId, "bar", 2))
            ),
            shareGroup.computeSubscriptionMetadata(
                shareGroup.computeSubscribedTopicNames(null, null),
                image.topics(),
                image.cluster()
            )
        );

        // Compute while taking into account removal of member 2.
        assertEquals(
            mkMap(
                mkEntry("foo", new TopicMetadata(fooTopicId, "foo", 1))
            ),
            shareGroup.computeSubscriptionMetadata(
                shareGroup.computeSubscribedTopicNames(member2, null),
                image.topics(),
                image.cluster()
            )
        );

        // Removing member1 results in returning bar.
        assertEquals(
            mkMap(
                mkEntry("bar", new TopicMetadata(barTopicId, "bar", 2))
            ),
            shareGroup.computeSubscriptionMetadata(
                shareGroup.computeSubscribedTopicNames(member1, null),
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
            shareGroup.computeSubscriptionMetadata(
                shareGroup.computeSubscribedTopicNames(null, member3),
                image.topics(),
                image.cluster()
            )
        );

        // Updating group with member3.
        shareGroup.updateMember(member3);

        // It should return foo, bar and zar.
        assertEquals(
            mkMap(
                mkEntry("foo", new TopicMetadata(fooTopicId, "foo", 1)),
                mkEntry("bar", new TopicMetadata(barTopicId, "bar", 2)),
                mkEntry("zar", new TopicMetadata(zarTopicId, "zar", 3))
            ),
            shareGroup.computeSubscriptionMetadata(
                shareGroup.computeSubscribedTopicNames(null, null),
                image.topics(),
                image.cluster()
            )
        );

        // Compute while taking into account removal of member 1, member 2 and member 3
        assertEquals(
            Collections.emptyMap(),
            shareGroup.computeSubscriptionMetadata(
                shareGroup.computeSubscribedTopicNames(new HashSet<>(Arrays.asList(member1, member2, member3))),
                image.topics(),
                image.cluster()
            )
        );

        // Compute while taking into account removal of member 2 and member 3.
        assertEquals(
            mkMap(
                mkEntry("foo", new TopicMetadata(fooTopicId, "foo", 1))
            ),
            shareGroup.computeSubscriptionMetadata(
                shareGroup.computeSubscribedTopicNames(new HashSet<>(Arrays.asList(member2, member3))),
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
            shareGroup.computeSubscriptionMetadata(
                shareGroup.computeSubscribedTopicNames(Collections.singleton(member1)),
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
            shareGroup.computeSubscriptionMetadata(
                shareGroup.computeSubscribedTopicNames(Collections.emptySet()),
                image.topics(),
                image.cluster()
            )
        );
    }

    @Test
    public void testUpdateSubscribedTopicNamesAndSubscriptionType() {
        ShareGroupMember member1 = new ShareGroupMember.Builder("member1")
            .setSubscribedTopicNames(Collections.singletonList("foo"))
            .build();
        ShareGroupMember member2 = new ShareGroupMember.Builder("member2")
            .setSubscribedTopicNames(Arrays.asList("bar", "foo"))
            .build();
        ShareGroupMember member3 = new ShareGroupMember.Builder("member3")
            .setSubscribedTopicNames(Arrays.asList("bar", "foo"))
            .build();

        ShareGroup shareGroup = createShareGroup("group-foo");

        // It should be empty by default.
        assertEquals(
            Collections.emptyMap(),
            shareGroup.subscribedTopicNames()
        );

        // It should be Homogeneous by default.
        assertEquals(
            HOMOGENEOUS,
            shareGroup.subscriptionType()
        );

        shareGroup.updateMember(member1);

        // It should be Homogeneous since there is just 1 member
        assertEquals(
            HOMOGENEOUS,
            shareGroup.subscriptionType()
        );

        shareGroup.updateMember(member2);

        assertEquals(
            HETEROGENEOUS,
            shareGroup.subscriptionType()
        );

        shareGroup.updateMember(member3);

        assertEquals(
            HETEROGENEOUS,
            shareGroup.subscriptionType()
        );

        shareGroup.removeMember(member1.memberId());

        assertEquals(
            HOMOGENEOUS,
            shareGroup.subscriptionType()
        );

        ShareGroupMember member4 = new ShareGroupMember.Builder("member2")
            .setSubscribedTopicNames(Arrays.asList("bar", "foo", "zar"))
            .build();

        shareGroup.updateMember(member4);

        assertEquals(
            HETEROGENEOUS,
            shareGroup.subscriptionType()
        );
    }

    @Test
    public void testUpdateInvertedAssignment() {
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(new LogContext());
        ShareGroup shareGroup = new ShareGroup(snapshotRegistry, "test-group");
        Uuid topicId = Uuid.randomUuid();
        String memberId1 = "member1";
        String memberId2 = "member2";

        // Initial assignment for member1
        Assignment initialAssignment = new Assignment(Collections.singletonMap(
            topicId,
            new HashSet<>(Collections.singletonList(0))
        ));
        shareGroup.updateTargetAssignment(memberId1, initialAssignment);

        // Verify that partition 0 is assigned to member1.
        assertEquals(
            mkMap(
                mkEntry(topicId, mkMap(mkEntry(0, memberId1)))
            ),
            shareGroup.invertedTargetAssignment()
        );

        // New assignment for member1
        Assignment newAssignment = new Assignment(Collections.singletonMap(
            topicId,
            new HashSet<>(Collections.singletonList(1))
        ));
        shareGroup.updateTargetAssignment(memberId1, newAssignment);

        // Verify that partition 0 is no longer assigned and partition 1 is assigned to member1
        assertEquals(
            mkMap(
                mkEntry(topicId, mkMap(mkEntry(1, memberId1)))
            ),
            shareGroup.invertedTargetAssignment()
        );

        // New assignment for member2 to add partition 1
        Assignment newAssignment2 = new Assignment(Collections.singletonMap(
            topicId,
            new HashSet<>(Collections.singletonList(1))
        ));
        shareGroup.updateTargetAssignment(memberId2, newAssignment2);

        // Verify that partition 1 is assigned to member2
        assertEquals(
            mkMap(
                mkEntry(topicId, mkMap(mkEntry(1, memberId2)))
            ),
            shareGroup.invertedTargetAssignment()
        );

        // New assignment for member1 to revoke partition 1 and assign partition 0
        Assignment newAssignment1 = new Assignment(Collections.singletonMap(
            topicId,
            new HashSet<>(Collections.singletonList(0))
        ));
        shareGroup.updateTargetAssignment(memberId1, newAssignment1);

        // Verify that partition 1 is still assigned to member2 and partition 0 is assigned to member1
        assertEquals(
            mkMap(
                mkEntry(topicId, mkMap(
                    mkEntry(0, memberId1),
                    mkEntry(1, memberId2)
                ))
            ),
            shareGroup.invertedTargetAssignment()
        );

        // Test remove target assignment for member1
        shareGroup.removeTargetAssignment(memberId1);

        // Verify that partition 0 is no longer assigned and partition 1 is still assigned to member2
        assertEquals(
            mkMap(
                mkEntry(topicId, mkMap(mkEntry(1, memberId2)))
            ),
            shareGroup.invertedTargetAssignment()
        );
    }

    @Test
    public void testMetadataRefreshDeadline() {
        MockTime time = new MockTime();
        ShareGroup shareGroup = createShareGroup("group-foo");

        // Group epoch starts at 0.
        assertEquals(0, shareGroup.groupEpoch());

        // The refresh time deadline should be empty when the group is created or loaded.
        assertTrue(shareGroup.hasMetadataExpired(time.milliseconds()));
        assertEquals(0L, shareGroup.metadataRefreshDeadline().deadlineMs);
        assertEquals(0, shareGroup.metadataRefreshDeadline().epoch);

        // Set the refresh deadline. The metadata remains valid because the deadline
        // has not past and the group epoch is correct.
        shareGroup.setMetadataRefreshDeadline(time.milliseconds() + 1000, shareGroup.groupEpoch());
        assertFalse(shareGroup.hasMetadataExpired(time.milliseconds()));
        assertEquals(time.milliseconds() + 1000, shareGroup.metadataRefreshDeadline().deadlineMs);
        assertEquals(shareGroup.groupEpoch(), shareGroup.metadataRefreshDeadline().epoch);

        // Advance past the deadline. The metadata should have expired.
        time.sleep(1001L);
        assertTrue(shareGroup.hasMetadataExpired(time.milliseconds()));

        // Set the refresh time deadline with a higher group epoch. The metadata is considered
        // as expired because the group epoch attached to the deadline is higher than the
        // current group epoch.
        shareGroup.setMetadataRefreshDeadline(time.milliseconds() + 1000, shareGroup.groupEpoch() + 1);
        assertTrue(shareGroup.hasMetadataExpired(time.milliseconds()));
        assertEquals(time.milliseconds() + 1000, shareGroup.metadataRefreshDeadline().deadlineMs);
        assertEquals(shareGroup.groupEpoch() + 1, shareGroup.metadataRefreshDeadline().epoch);

        // Advance the group epoch.
        shareGroup.setGroupEpoch(shareGroup.groupEpoch() + 1);

        // Set the refresh deadline. The metadata remains valid because the deadline
        // has not past and the group epoch is correct.
        shareGroup.setMetadataRefreshDeadline(time.milliseconds() + 1000, shareGroup.groupEpoch());
        assertFalse(shareGroup.hasMetadataExpired(time.milliseconds()));
        assertEquals(time.milliseconds() + 1000, shareGroup.metadataRefreshDeadline().deadlineMs);
        assertEquals(shareGroup.groupEpoch(), shareGroup.metadataRefreshDeadline().epoch);

        // Request metadata refresh. The metadata expires immediately.
        shareGroup.requestMetadataRefresh();
        assertTrue(shareGroup.hasMetadataExpired(time.milliseconds()));
        assertEquals(0L, shareGroup.metadataRefreshDeadline().deadlineMs);
        assertEquals(0, shareGroup.metadataRefreshDeadline().epoch);
    }

    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.OFFSET_COMMIT)
    public void testValidateOffsetCommit(short version) {
        ShareGroup shareGroup = createShareGroup("group-foo");
        assertThrows(GroupIdNotFoundException.class, () ->
            shareGroup.validateOffsetCommit(null, null, -1, false, version));
    }

    @Test
    public void testAsListedGroup() {
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(new LogContext());
        ShareGroup shareGroup = new ShareGroup(snapshotRegistry, "group-foo");
        snapshotRegistry.idempotentCreateSnapshot(0);
        assertEquals(ShareGroupState.EMPTY, shareGroup.state(0));
        assertEquals("Empty", shareGroup.stateAsString(0));
        shareGroup.updateMember(new ShareGroupMember.Builder("member1")
            .setSubscribedTopicNames(Collections.singletonList("foo"))
            .build());
        snapshotRegistry.idempotentCreateSnapshot(1);
        assertEquals(ShareGroupState.EMPTY, shareGroup.state(0));
        assertEquals("Empty", shareGroup.stateAsString(0));
        assertEquals(ShareGroupState.STABLE, shareGroup.state(1));
        assertEquals("Stable", shareGroup.stateAsString(1));
    }

    @Test
    public void testOffsetExpirationCondition() {
        ShareGroup shareGroup = createShareGroup("group-foo");
        assertThrows(UnsupportedOperationException.class, shareGroup::offsetExpirationCondition);
    }

    @Test
    public void testValidateOffsetFetch() {
        ShareGroup shareGroup = createShareGroup("group-foo");
        assertThrows(GroupIdNotFoundException.class, () ->
            shareGroup.validateOffsetFetch(null, -1, -1));
    }

    @Test
    public void testValidateOffsetDelete() {
        ShareGroup shareGroup = createShareGroup("group-foo");
        assertThrows(GroupIdNotFoundException.class, shareGroup::validateOffsetDelete);
    }

    @Test
    public void testValidateDeleteGroup() {
        ShareGroup shareGroup = createShareGroup("foo");

        assertEquals(ShareGroupState.EMPTY, shareGroup.state());
        assertDoesNotThrow(shareGroup::validateDeleteGroup);

        ShareGroupMember member1 = new ShareGroupMember.Builder("member1")
            .setMemberEpoch(1)
            .setPreviousMemberEpoch(0)
            .build();
        shareGroup.updateMember(member1);

        assertEquals(ShareGroupState.STABLE, shareGroup.state());
        assertThrows(GroupNotEmptyException.class, shareGroup::validateDeleteGroup);

        shareGroup.setGroupEpoch(1);

        assertEquals(ShareGroupState.STABLE, shareGroup.state());
        assertThrows(GroupNotEmptyException.class, shareGroup::validateDeleteGroup);

        shareGroup.setTargetAssignmentEpoch(1);

        assertEquals(ShareGroupState.STABLE, shareGroup.state());
        assertThrows(GroupNotEmptyException.class, shareGroup::validateDeleteGroup);
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

        ShareGroupMember member1 = new ShareGroupMember.Builder("member1")
            .setSubscribedTopicNames(Collections.singletonList("foo"))
            .build();
        ShareGroupMember member2 = new ShareGroupMember.Builder("member2")
            .setSubscribedTopicNames(Collections.singletonList("bar"))
            .build();

        ShareGroup shareGroup = createShareGroup("group-foo");

        shareGroup.updateMember(member1);
        shareGroup.updateMember(member2);

        assertEquals(
            mkMap(
                mkEntry("foo", new TopicMetadata(fooTopicId, "foo", 1)),
                mkEntry("bar", new TopicMetadata(barTopicId, "bar", 2))
            ),
            shareGroup.computeSubscriptionMetadata(
                shareGroup.computeSubscribedTopicNames(null, null),
                image.topics(),
                image.cluster()
            )
        );

        assertTrue(shareGroup.isSubscribedToTopic("foo"));
        assertTrue(shareGroup.isSubscribedToTopic("bar"));

        shareGroup.removeMember("member1");
        assertFalse(shareGroup.isSubscribedToTopic("foo"));

        shareGroup.removeMember("member2");
        assertFalse(shareGroup.isSubscribedToTopic("bar"));
    }

    @Test
    public void testAsDescribedGroup() {
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(new LogContext());
        ShareGroup shareGroup = new ShareGroup(snapshotRegistry, "group-id-1");
        snapshotRegistry.idempotentCreateSnapshot(0);
        assertEquals(ShareGroupState.EMPTY.toString(), shareGroup.stateAsString(0));

        shareGroup.updateMember(new ShareGroupMember.Builder("member1")
                .setSubscribedTopicNames(Collections.singletonList("foo"))
                .build());
        shareGroup.updateMember(new ShareGroupMember.Builder("member2")
                .build());
        snapshotRegistry.idempotentCreateSnapshot(1);

        ShareGroupDescribeResponseData.DescribedGroup expected = new ShareGroupDescribeResponseData.DescribedGroup()
            .setGroupId("group-id-1")
            .setGroupState(ShareGroupState.STABLE.toString())
            .setGroupEpoch(0)
            .setAssignmentEpoch(0)
            .setAssignorName("assignorName")
            .setMembers(Arrays.asList(
                new ShareGroupDescribeResponseData.Member()
                    .setMemberId("member1")
                    .setSubscribedTopicNames(Collections.singletonList("foo")),
                new ShareGroupDescribeResponseData.Member().setMemberId("member2")
            ));
        ShareGroupDescribeResponseData.DescribedGroup actual = shareGroup.asDescribedGroup(1, "assignorName",
            new MetadataImageBuilder().build().topics());

        assertEquals(expected, actual);
    }

    @Test
    public void testIsInStatesCaseInsensitive() {
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(new LogContext());
        ShareGroup shareGroup = new ShareGroup(snapshotRegistry, "group-foo");
        snapshotRegistry.idempotentCreateSnapshot(0);
        assertTrue(shareGroup.isInStates(Collections.singleton("empty"), 0));
        assertFalse(shareGroup.isInStates(Collections.singleton("Empty"), 0));

        shareGroup.updateMember(new ShareGroupMember.Builder("member1")
            .setSubscribedTopicNames(Collections.singletonList("foo"))
            .build());
        snapshotRegistry.idempotentCreateSnapshot(1);
        assertTrue(shareGroup.isInStates(Collections.singleton("empty"), 0));
        assertTrue(shareGroup.isInStates(Collections.singleton("stable"), 1));
        assertFalse(shareGroup.isInStates(Collections.singleton("empty"), 1));
    }

    private ShareGroup createShareGroup(String groupId) {
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(new LogContext());
        return new ShareGroup(
            snapshotRegistry,
            groupId
        );
    }
}
