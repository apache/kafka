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
package org.apache.kafka.coordinator.group.classic;

import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.CoordinatorNotAvailableException;
import org.apache.kafka.common.errors.FencedInstanceIdException;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.GroupNotEmptyException;
import org.apache.kafka.common.errors.IllegalGenerationException;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.JoinGroupRequestData.JoinGroupRequestProtocol;
import org.apache.kafka.common.message.JoinGroupRequestData.JoinGroupRequestProtocolCollection;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.message.SyncGroupResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.common.utils.annotation.ApiKeyVersionsSource;
import org.apache.kafka.coordinator.group.MetadataImageBuilder;
import org.apache.kafka.coordinator.group.OffsetAndMetadata;
import org.apache.kafka.coordinator.group.OffsetExpirationCondition;
import org.apache.kafka.coordinator.group.OffsetExpirationConditionImpl;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMemberMetadataValue;
import org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetricsShard;
import org.apache.kafka.coordinator.group.modern.Assignment;
import org.apache.kafka.coordinator.group.modern.MemberState;
import org.apache.kafka.coordinator.group.modern.consumer.ConsumerGroup;
import org.apache.kafka.coordinator.group.modern.consumer.ConsumerGroupMember;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.timeline.SnapshotRegistry;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkAssignment;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkTopicAssignment;
import static org.apache.kafka.coordinator.group.classic.ClassicGroupState.COMPLETING_REBALANCE;
import static org.apache.kafka.coordinator.group.classic.ClassicGroupState.DEAD;
import static org.apache.kafka.coordinator.group.classic.ClassicGroupState.EMPTY;
import static org.apache.kafka.coordinator.group.classic.ClassicGroupState.PREPARING_REBALANCE;
import static org.apache.kafka.coordinator.group.classic.ClassicGroupState.STABLE;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

public class ClassicGroupTest {
    private final String protocolType = "consumer";
    private final String groupInstanceId = "groupInstanceId";
    private final String memberId = "memberId";
    private final String clientId = "clientId";
    private final String clientHost = "clientHost";
    private final int rebalanceTimeoutMs = 60000;
    private final int sessionTimeoutMs = 10000;
    private final LogContext logContext = new LogContext();

    private ClassicGroup group = null;

    @BeforeEach
    public void initialize() {
        group = new ClassicGroup(logContext, "groupId", EMPTY, Time.SYSTEM);
    }

    @Test
    public void testCanRebalanceWhenStable() {
        assertTrue(group.canRebalance());
    }

    @Test
    public void testCanRebalanceWhenCompletingRebalance() {
        group.transitionTo(PREPARING_REBALANCE);
        group.transitionTo(COMPLETING_REBALANCE);
        assertTrue(group.canRebalance());
    }

    @Test
    public void testCannotRebalanceWhenPreparingRebalance() {
        group.transitionTo(PREPARING_REBALANCE);
        assertFalse(group.canRebalance());
    }

    @Test
    public void testCannotRebalanceWhenDead() {
        group.transitionTo(PREPARING_REBALANCE);
        group.transitionTo(EMPTY);
        group.transitionTo(DEAD);
        assertFalse(group.canRebalance());
    }

    @Test
    public void testStableToPreparingRebalanceTransition() {
        group.transitionTo(PREPARING_REBALANCE);
        assertState(group, PREPARING_REBALANCE);
    }

    @Test
    public void testStableToDeadTransition() {
        group.transitionTo(DEAD);
        assertState(group, DEAD);
    }

    @Test
    public void testAwaitingRebalanceToPreparingRebalanceTransition() {
        group.transitionTo(PREPARING_REBALANCE);
        group.transitionTo(COMPLETING_REBALANCE);
        group.transitionTo(PREPARING_REBALANCE);
        assertState(group, PREPARING_REBALANCE);
    }

    @Test
    public void testPreparingRebalanceToDeadTransition() {
        group.transitionTo(PREPARING_REBALANCE);
        group.transitionTo(DEAD);
        assertState(group, DEAD);
    }

    @Test
    public void testPreparingRebalanceToEmptyTransition() {
        group.transitionTo(PREPARING_REBALANCE);
        group.transitionTo(EMPTY);
        assertState(group, EMPTY);
    }

    @Test
    public void testEmptyToDeadTransition() {
        group.transitionTo(PREPARING_REBALANCE);
        group.transitionTo(EMPTY);
        group.transitionTo(DEAD);
        assertState(group, DEAD);
    }

    @Test
    public void testAwaitingRebalanceToStableTransition() {
        group.transitionTo(PREPARING_REBALANCE);
        group.transitionTo(COMPLETING_REBALANCE);
        group.transitionTo(STABLE);
        assertState(group, STABLE);
    }

    @Test
    public void testEmptyToStableIllegalTransition() {
        assertThrows(IllegalStateException.class, () -> group.transitionTo(STABLE));
    }

    @Test
    public void testStableToStableIllegalTransition() {
        group.transitionTo(PREPARING_REBALANCE);
        group.transitionTo(COMPLETING_REBALANCE);
        group.transitionTo(STABLE);
        assertThrows(IllegalStateException.class, () -> group.transitionTo(STABLE));
    }

    @Test
    public void testEmptyToAwaitingRebalanceIllegalTransition() {
        assertThrows(IllegalStateException.class, () -> group.transitionTo(COMPLETING_REBALANCE));
    }

    @Test
    public void testPreparingRebalanceToPreparingRebalanceIllegalTransition() {
        group.transitionTo(PREPARING_REBALANCE);
        assertThrows(IllegalStateException.class, () -> group.transitionTo(PREPARING_REBALANCE));
    }

    @Test
    public void testPreparingRebalanceToStableIllegalTransition() {
        group.transitionTo(PREPARING_REBALANCE);
        assertThrows(IllegalStateException.class, () -> group.transitionTo(STABLE));
    }

    @Test
    public void testAwaitingRebalanceToAwaitingRebalanceIllegalTransition() {
        group.transitionTo(PREPARING_REBALANCE);
        group.transitionTo(COMPLETING_REBALANCE);
        assertThrows(IllegalStateException.class, () -> group.transitionTo(COMPLETING_REBALANCE));
    }

    @Test
    public void testDeadToDeadIllegalTransition() {
        group.transitionTo(PREPARING_REBALANCE);
        group.transitionTo(DEAD);
        group.transitionTo(DEAD);
        assertState(group, DEAD);
    }

    @Test
    public void testDeadToStableIllegalTransition() {
        group.transitionTo(PREPARING_REBALANCE);
        group.transitionTo(DEAD);
        assertThrows(IllegalStateException.class, () -> group.transitionTo(STABLE));
    }

    @Test
    public void testDeadToPreparingRebalanceIllegalTransition() {
        group.transitionTo(PREPARING_REBALANCE);
        group.transitionTo(DEAD);
        assertThrows(IllegalStateException.class, () -> group.transitionTo(PREPARING_REBALANCE));
    }

    @Test
    public void testDeadToAwaitingRebalanceIllegalTransition() {
        group.transitionTo(PREPARING_REBALANCE);
        group.transitionTo(DEAD);
        assertThrows(IllegalStateException.class, () -> group.transitionTo(COMPLETING_REBALANCE));
    }

    @Test
    public void testSelectProtocol() {
        JoinGroupRequestProtocolCollection member1Protocols = new JoinGroupRequestProtocolCollection();
        member1Protocols.add(new JoinGroupRequestProtocol()
            .setName("range")
            .setMetadata(new byte[0]));

        member1Protocols.add(new JoinGroupRequestProtocol()
            .setName("roundrobin")
            .setMetadata(new byte[0]));

        ClassicGroupMember member1 = new ClassicGroupMember(
            memberId,
            Optional.empty(),
            clientId,
            clientHost,
            rebalanceTimeoutMs,
            sessionTimeoutMs,
            protocolType,
            member1Protocols
        );
        group.add(member1);

        JoinGroupRequestProtocolCollection member2Protocols = new JoinGroupRequestProtocolCollection();
        member2Protocols.add(new JoinGroupRequestProtocol()
            .setName("roundrobin")
            .setMetadata(new byte[0]));

        member2Protocols.add(new JoinGroupRequestProtocol()
            .setName("range")
            .setMetadata(new byte[0]));

        ClassicGroupMember member2 = new ClassicGroupMember(
            "member2",
            Optional.empty(),
            clientId,
            clientHost,
            rebalanceTimeoutMs,
            sessionTimeoutMs,
            protocolType,
            member2Protocols
        );
        group.add(member2);

        // now could be either range or robin since there is no majority preference
        assertTrue(group.selectProtocol().equals("range") ||
            group.selectProtocol().equals("roundrobin"));

        ClassicGroupMember member3 = new ClassicGroupMember(
            "member3",
            Optional.empty(),
            clientId,
            clientHost,
            rebalanceTimeoutMs,
            sessionTimeoutMs,
            protocolType,
            member2Protocols
        );
        group.add(member3);

        // now we should prefer 'roundrobin'
        assertEquals("roundrobin", group.selectProtocol());
    }

    @Test
    public void testSelectProtocolRaisesIfNoMembers() {
        assertThrows(IllegalStateException.class, () -> group.selectProtocol());
    }

    @Test
    public void testSelectProtocolChoosesCompatibleProtocol() {
        JoinGroupRequestProtocolCollection member1Protocols = new JoinGroupRequestProtocolCollection();
        member1Protocols.add(new JoinGroupRequestProtocol()
            .setName("range")
            .setMetadata(new byte[0]));

        member1Protocols.add(new JoinGroupRequestProtocol()
            .setName("roundrobin")
            .setMetadata(new byte[0]));

        ClassicGroupMember member1 = new ClassicGroupMember(
            memberId,
            Optional.empty(),
            clientId,
            clientHost,
            rebalanceTimeoutMs,
            sessionTimeoutMs,
            protocolType,
            member1Protocols
        );
        group.add(member1);

        JoinGroupRequestProtocolCollection member2Protocols = new JoinGroupRequestProtocolCollection();
        member2Protocols.add(new JoinGroupRequestProtocol()
            .setName("roundrobin")
            .setMetadata(new byte[0]));

        member2Protocols.add(new JoinGroupRequestProtocol()
            .setName("foo")
            .setMetadata(new byte[0]));


        ClassicGroupMember member2 = new ClassicGroupMember(
            "member2",
            Optional.empty(),
            clientId,
            clientHost,
            rebalanceTimeoutMs,
            sessionTimeoutMs,
            protocolType,
            member2Protocols
        );
        group.add(member2);

        assertEquals("roundrobin", group.selectProtocol());
    }

    @Test
    public void testSupportsProtocols() {
        JoinGroupRequestProtocolCollection member1Protocols = new JoinGroupRequestProtocolCollection();
        member1Protocols.add(new JoinGroupRequestProtocol()
            .setName("range")
            .setMetadata(new byte[0]));

        member1Protocols.add(new JoinGroupRequestProtocol()
            .setName("roundrobin")
            .setMetadata(new byte[0]));

        ClassicGroupMember member1 = new ClassicGroupMember(
            memberId,
            Optional.empty(),
            clientId,
            clientHost,
            rebalanceTimeoutMs,
            sessionTimeoutMs,
            protocolType,
            member1Protocols
        );

        // by default, the group supports everything
        assertTrue(group.supportsProtocols(protocolType, Set.of("range", "roundrobin")));

        group.add(member1);
        group.transitionTo(PREPARING_REBALANCE);

        assertTrue(group.supportsProtocols(protocolType, Set.of("roundrobin", "foo")));
        assertTrue(group.supportsProtocols(protocolType, Set.of("range", "bar")));
        assertFalse(group.supportsProtocols(protocolType, Set.of("foo", "bar")));
    }

    @Test
    public void testSubscribedTopics() {
        // not able to compute it for a newly created group
        assertEquals(Optional.empty(), group.subscribedTopics());

        JoinGroupRequestProtocolCollection protocols = new JoinGroupRequestProtocolCollection();
        protocols.add(new JoinGroupRequestProtocol()
            .setName("range")
            .setMetadata(ConsumerProtocol.serializeSubscription(
                new ConsumerPartitionAssignor.Subscription(
                    Collections.singletonList("foo")
                )).array()));

        ClassicGroupMember member = new ClassicGroupMember(
            memberId,
            Optional.empty(),
            clientId,
            clientHost,
            rebalanceTimeoutMs,
            sessionTimeoutMs,
            protocolType,
            protocols
        );

        group.transitionTo(PREPARING_REBALANCE);
        group.add(member);

        group.initNextGeneration();

        Set<String> expectedTopics = new HashSet<>(Collections.singleton("foo"));
        assertEquals(expectedTopics, group.subscribedTopics().get());

        group.transitionTo(PREPARING_REBALANCE);
        group.remove(memberId);

        group.initNextGeneration();

        assertEquals(Optional.of(Collections.emptySet()), group.subscribedTopics());

        protocols = new JoinGroupRequestProtocolCollection();
        protocols.add(new JoinGroupRequestProtocol()
            .setName("range")
            .setMetadata(new byte[0]));

        ClassicGroupMember memberWithFaultyProtocol = new ClassicGroupMember(
            memberId,
            Optional.empty(),
            clientId,
            clientHost,
            rebalanceTimeoutMs,
            sessionTimeoutMs,
            protocolType,
            protocols
        );

        group.transitionTo(PREPARING_REBALANCE);
        group.add(memberWithFaultyProtocol);

        group.initNextGeneration();

        assertEquals(Optional.empty(), group.subscribedTopics());
    }

    @Test
    public void testSubscribedTopicsNonConsumerGroup() {
        // not able to compute it for a newly created group
        assertEquals(Optional.empty(), group.subscribedTopics());

        JoinGroupRequestProtocolCollection protocols = new JoinGroupRequestProtocolCollection();
        protocols.add(new JoinGroupRequestProtocol()
            .setName("range")
            .setMetadata(new byte[0]));

        ClassicGroupMember memberWithNonConsumerProtocol = new ClassicGroupMember(
            memberId,
            Optional.empty(),
            clientId,
            clientHost,
            rebalanceTimeoutMs,
            sessionTimeoutMs,
            "My Protocol",
            protocols
        );

        group.transitionTo(PREPARING_REBALANCE);
        group.add(memberWithNonConsumerProtocol);

        group.initNextGeneration();

        assertEquals(Optional.empty(), group.subscribedTopics());
    }

    @Test
    public void testInitNextGeneration() {
        JoinGroupRequestProtocolCollection protocols = new JoinGroupRequestProtocolCollection();
        protocols.add(new JoinGroupRequestProtocol()
            .setName("roundrobin")
            .setMetadata(new byte[0]));

        ClassicGroupMember member = new ClassicGroupMember(
            memberId,
            Optional.empty(),
            clientId,
            clientHost,
            rebalanceTimeoutMs,
            sessionTimeoutMs,
            protocolType,
            protocols
        );

        group.transitionTo(PREPARING_REBALANCE);
        group.add(member, new CompletableFuture<>());

        assertEquals(0, group.generationId());
        assertNull(group.protocolName().orElse(null));

        group.initNextGeneration();

        assertEquals(1, group.generationId());
        assertEquals("roundrobin", group.protocolName().orElse(null));
    }

    @Test
    public void testInitNextGenerationEmptyGroup() {
        assertEquals(EMPTY, group.currentState());
        assertEquals(0, group.generationId());
        assertNull(group.protocolName().orElse(null));

        group.transitionTo(PREPARING_REBALANCE);
        group.initNextGeneration();

        assertEquals(1, group.generationId());
        assertNull(group.protocolName().orElse(null));
    }

    @Test
    public void testUpdateMember() {
        JoinGroupRequestProtocolCollection protocols = new JoinGroupRequestProtocolCollection();
        protocols.add(new JoinGroupRequestProtocol()
            .setName("roundrobin")
            .setMetadata(new byte[0]));

        ClassicGroupMember member = new ClassicGroupMember(
            memberId,
            Optional.empty(),
            clientId,
            clientHost,
            rebalanceTimeoutMs,
            sessionTimeoutMs,
            protocolType,
            protocols
        );

        group.add(member);

        JoinGroupRequestProtocolCollection newProtocols = new JoinGroupRequestProtocolCollection();
        newProtocols.add(new JoinGroupRequestProtocol()
            .setName("range")
            .setMetadata(new byte[0]));

        newProtocols.add(new JoinGroupRequestProtocol()
            .setName("roundrobin")
            .setMetadata(new byte[0]));

        int newRebalanceTimeoutMs = 120000;
        int newSessionTimeoutMs = 20000;
        group.updateMember(member, newProtocols, newRebalanceTimeoutMs, newSessionTimeoutMs, null);

        assertEquals(group.rebalanceTimeoutMs(), newRebalanceTimeoutMs);
        assertEquals(member.sessionTimeoutMs(), newSessionTimeoutMs);
        assertEquals(newProtocols, member.supportedProtocols());
    }

    @Test
    public void testReplaceGroupInstanceWithNonExistingMember() {
        String newMemberId = "newMemberId";
        assertThrows(IllegalArgumentException.class, () ->
            group.replaceStaticMember(groupInstanceId, memberId, newMemberId));
    }

    @Test
    public void testReplaceGroupInstance() throws Exception {
        JoinGroupRequestProtocolCollection protocols = new JoinGroupRequestProtocolCollection();
        protocols.add(new JoinGroupRequestProtocol()
            .setName("roundrobin")
            .setMetadata(new byte[0]));

        ClassicGroupMember member = new ClassicGroupMember(
            memberId,
            Optional.of(groupInstanceId),
            clientId,
            clientHost,
            rebalanceTimeoutMs,
            sessionTimeoutMs,
            protocolType,
            protocols
        );

        CompletableFuture<JoinGroupResponseData> joinGroupFuture = new CompletableFuture<>();
        group.add(member, joinGroupFuture);

        CompletableFuture<SyncGroupResponseData> syncGroupFuture = new CompletableFuture<>();
        member.setAwaitingSyncFuture(syncGroupFuture);

        assertTrue(group.isLeader(memberId));
        assertEquals(memberId, group.staticMemberId(groupInstanceId));

        String newMemberId = "newMemberId";
        group.replaceStaticMember(groupInstanceId, memberId, newMemberId);

        assertTrue(group.isLeader(newMemberId));
        assertEquals(newMemberId, group.staticMemberId(groupInstanceId));
        assertEquals(Errors.FENCED_INSTANCE_ID.code(), joinGroupFuture.get().errorCode());
        assertEquals(Errors.FENCED_INSTANCE_ID.code(), syncGroupFuture.get().errorCode());
        assertFalse(member.isAwaitingJoin());
        assertFalse(member.isAwaitingSync());
    }

    @Test
    public void testCompleteJoinFuture() throws Exception {
        JoinGroupRequestProtocolCollection protocols = new JoinGroupRequestProtocolCollection();
        protocols.add(new JoinGroupRequestProtocol()
            .setName("roundrobin")
            .setMetadata(new byte[0]));

        ClassicGroupMember member = new ClassicGroupMember(
            memberId,
            Optional.empty(),
            clientId,
            clientHost,
            rebalanceTimeoutMs,
            sessionTimeoutMs,
            protocolType,
            protocols
        );

        CompletableFuture<JoinGroupResponseData> joinGroupFuture = new CompletableFuture<>();
        group.add(member, joinGroupFuture);

        assertTrue(group.hasAllMembersJoined());
        assertTrue(
            group.completeJoinFuture(member, new JoinGroupResponseData()
                .setMemberId(member.memberId())
                .setErrorCode(Errors.NONE.code()))
        );

        assertEquals(Errors.NONE.code(), joinGroupFuture.get().errorCode());
        assertEquals(memberId, joinGroupFuture.get().memberId());
        assertFalse(member.isAwaitingJoin());
        assertEquals(0, group.numAwaitingJoinResponse());
    }

    @Test
    public void testNotCompleteJoinFuture() {
        JoinGroupRequestProtocolCollection protocols = new JoinGroupRequestProtocolCollection();
        protocols.add(new JoinGroupRequestProtocol()
            .setName("roundrobin")
            .setMetadata(new byte[0]));

        ClassicGroupMember member = new ClassicGroupMember(
            memberId,
            Optional.empty(),
            clientId,
            clientHost,
            rebalanceTimeoutMs,
            sessionTimeoutMs,
            protocolType,
            protocols
        );

        group.add(member);

        assertFalse(member.isAwaitingJoin());
        assertFalse(
            group.completeJoinFuture(member, new JoinGroupResponseData()
                .setMemberId(member.memberId())
                .setErrorCode(Errors.NONE.code()))
        );

        assertFalse(member.isAwaitingJoin());
    }

    @Test
    public void testCompleteSyncFuture() throws Exception {
        JoinGroupRequestProtocolCollection protocols = new JoinGroupRequestProtocolCollection();
        protocols.add(new JoinGroupRequestProtocol()
            .setName("roundrobin")
            .setMetadata(new byte[0]));

        ClassicGroupMember member = new ClassicGroupMember(
            memberId,
            Optional.empty(),
            clientId,
            clientHost,
            rebalanceTimeoutMs,
            sessionTimeoutMs,
            protocolType,
            protocols
        );

        group.add(member);
        CompletableFuture<SyncGroupResponseData> syncGroupFuture = new CompletableFuture<>();
        member.setAwaitingSyncFuture(syncGroupFuture);

        assertTrue(group.completeSyncFuture(member, new SyncGroupResponseData()
            .setErrorCode(Errors.NONE.code())));

        assertEquals(0, group.numAwaitingJoinResponse());

        assertFalse(member.isAwaitingSync());
        assertEquals(Errors.NONE.code(), syncGroupFuture.get().errorCode());
    }

    @Test
    public void testNotCompleteSyncFuture() {
        JoinGroupRequestProtocolCollection protocols = new JoinGroupRequestProtocolCollection();
        protocols.add(new JoinGroupRequestProtocol()
            .setName("roundrobin")
            .setMetadata(new byte[0]));

        ClassicGroupMember member = new ClassicGroupMember(
            memberId,
            Optional.empty(),
            clientId,
            clientHost,
            rebalanceTimeoutMs,
            sessionTimeoutMs,
            protocolType,
            protocols
        );

        group.add(member);
        assertFalse(member.isAwaitingSync());

        assertFalse(group.completeSyncFuture(member, new SyncGroupResponseData()
            .setErrorCode(Errors.NONE.code())));

        assertFalse(member.isAwaitingSync());
    }

    @Test
    public void testCannotAddPendingMemberIfStable() {
        JoinGroupRequestProtocolCollection protocols = new JoinGroupRequestProtocolCollection();
        protocols.add(new JoinGroupRequestProtocol()
            .setName("roundrobin")
            .setMetadata(new byte[0]));

        ClassicGroupMember member = new ClassicGroupMember(
            memberId,
            Optional.empty(),
            clientId,
            clientHost,
            rebalanceTimeoutMs,
            sessionTimeoutMs,
            protocolType,
            protocols
        );

        group.add(member);
        assertThrows(IllegalStateException.class, () -> group.addPendingMember(memberId));
    }

    @Test
    public void testRemovalFromPendingAfterMemberIsStable() {
        JoinGroupRequestProtocolCollection protocols = new JoinGroupRequestProtocolCollection();
        protocols.add(new JoinGroupRequestProtocol()
            .setName("roundrobin")
            .setMetadata(new byte[0]));

        group.addPendingMember(memberId);
        assertFalse(group.hasMember(memberId));
        assertTrue(group.isPendingMember(memberId));

        ClassicGroupMember member = new ClassicGroupMember(
            memberId,
            Optional.empty(),
            clientId,
            clientHost,
            rebalanceTimeoutMs,
            sessionTimeoutMs,
            protocolType,
            protocols
        );

        group.add(member);
        assertTrue(group.hasMember(memberId));
        assertFalse(group.isPendingMember(memberId));
    }

    @Test
    public void testRemovalFromPendingWhenMemberIsRemoved() {
        group.addPendingMember(memberId);
        assertFalse(group.hasMember(memberId));
        assertTrue(group.isPendingMember(memberId));

        group.remove(memberId);
        assertFalse(group.hasMember(memberId));
        assertFalse(group.isPendingMember(memberId));
    }

    @Test
    public void testCannotAddStaticMemberIfAlreadyPresent() {
        JoinGroupRequestProtocolCollection protocols = new JoinGroupRequestProtocolCollection();
        protocols.add(new JoinGroupRequestProtocol()
            .setName("roundrobin")
            .setMetadata(new byte[0]));

        ClassicGroupMember member = new ClassicGroupMember(
            memberId,
            Optional.of(groupInstanceId),
            clientId,
            clientHost,
            rebalanceTimeoutMs,
            sessionTimeoutMs,
            protocolType,
            protocols
        );

        group.add(member);
        assertTrue(group.hasMember(memberId));
        assertTrue(group.hasStaticMember(groupInstanceId));

        // We are not permitted to add the member again if it is already present
        assertThrows(IllegalStateException.class, () -> group.add(member));
    }

    @Test
    public void testCannotAddPendingSyncOfUnknownMember() {
        assertThrows(IllegalStateException.class,
            () -> group.addPendingSyncMember(memberId));
    }

    @Test
    public void testCannotRemovePendingSyncOfUnknownMember() {
        assertThrows(IllegalStateException.class,
            () -> group.removePendingSyncMember(memberId));
    }

    @Test
    public void testCanAddAndRemovePendingSyncMember() {
        JoinGroupRequestProtocolCollection protocols = new JoinGroupRequestProtocolCollection();
        protocols.add(new JoinGroupRequestProtocol()
            .setName("roundrobin")
            .setMetadata(new byte[0]));

        ClassicGroupMember member = new ClassicGroupMember(
            memberId,
            Optional.empty(),
            clientId,
            clientHost,
            rebalanceTimeoutMs,
            sessionTimeoutMs,
            protocolType,
            protocols
        );

        group.add(member);
        assertTrue(group.addPendingSyncMember(memberId));
        assertEquals(Collections.singleton(memberId), group.allPendingSyncMembers());
        group.removePendingSyncMember(memberId);
        assertEquals(Collections.emptySet(), group.allPendingSyncMembers());
    }

    @Test
    public void testRemovalFromPendingSyncWhenMemberIsRemoved() {
        JoinGroupRequestProtocolCollection protocols = new JoinGroupRequestProtocolCollection();
        protocols.add(new JoinGroupRequestProtocol()
            .setName("roundrobin")
            .setMetadata(new byte[0]));

        ClassicGroupMember member = new ClassicGroupMember(
            memberId,
            Optional.of(groupInstanceId),
            clientId,
            clientHost,
            rebalanceTimeoutMs,
            sessionTimeoutMs,
            protocolType,
            protocols
        );

        group.add(member);
        assertTrue(group.addPendingSyncMember(memberId));
        assertEquals(Collections.singleton(memberId), group.allPendingSyncMembers());
        group.remove(memberId);
        assertEquals(Collections.emptySet(), group.allPendingSyncMembers());
    }

    @Test
    public void testNewGenerationClearsPendingSyncMembers() {
        JoinGroupRequestProtocolCollection protocols = new JoinGroupRequestProtocolCollection();
        protocols.add(new JoinGroupRequestProtocol()
            .setName("roundrobin")
            .setMetadata(new byte[0]));

        ClassicGroupMember member = new ClassicGroupMember(
            memberId,
            Optional.empty(),
            clientId,
            clientHost,
            rebalanceTimeoutMs,
            sessionTimeoutMs,
            protocolType,
            protocols
        );

        group.add(member);
        group.transitionTo(PREPARING_REBALANCE);
        assertTrue(group.addPendingSyncMember(memberId));
        assertEquals(Collections.singleton(memberId), group.allPendingSyncMembers());
        group.initNextGeneration();
        assertEquals(Collections.emptySet(), group.allPendingSyncMembers());
    }

    @Test
    public void testElectNewJoinedLeader() {
        JoinGroupRequestProtocolCollection protocols = new JoinGroupRequestProtocolCollection();
        protocols.add(new JoinGroupRequestProtocol()
            .setName("roundrobin")
            .setMetadata(new byte[0]));

        ClassicGroupMember leader = new ClassicGroupMember(
            memberId,
            Optional.empty(),
            clientId,
            clientHost,
            rebalanceTimeoutMs,
            sessionTimeoutMs,
            protocolType,
            protocols
        );

        group.add(leader);
        assertTrue(group.isLeader(memberId));
        assertFalse(leader.isAwaitingJoin());

        ClassicGroupMember newLeader = new ClassicGroupMember(
            "new-leader",
            Optional.empty(),
            clientId,
            clientHost,
            rebalanceTimeoutMs,
            sessionTimeoutMs,
            protocolType,
            protocols
        );
        group.add(newLeader, new CompletableFuture<>());

        ClassicGroupMember newMember = new ClassicGroupMember(
            "new-member",
            Optional.empty(),
            clientId,
            clientHost,
            rebalanceTimeoutMs,
            sessionTimeoutMs,
            protocolType,
            protocols
        );
        group.add(newMember);

        assertTrue(group.maybeElectNewJoinedLeader());
        assertTrue(group.isLeader("new-leader"));
    }

    @Test
    public void testMaybeElectNewJoinedLeaderChooseExisting() {
        JoinGroupRequestProtocolCollection protocols = new JoinGroupRequestProtocolCollection();
        protocols.add(new JoinGroupRequestProtocol()
            .setName("roundrobin")
            .setMetadata(new byte[0]));

        ClassicGroupMember leader = new ClassicGroupMember(
            memberId,
            Optional.empty(),
            clientId,
            clientHost,
            rebalanceTimeoutMs,
            sessionTimeoutMs,
            protocolType,
            protocols
        );

        group.add(leader, new CompletableFuture<>());
        assertTrue(group.isLeader(memberId));
        assertTrue(leader.isAwaitingJoin());

        ClassicGroupMember newMember = new ClassicGroupMember(
            "new-member",
            Optional.empty(),
            clientId,
            clientHost,
            rebalanceTimeoutMs,
            sessionTimeoutMs,
            protocolType,
            protocols
        );
        group.add(newMember);

        assertTrue(group.maybeElectNewJoinedLeader());
        assertTrue(group.isLeader(memberId));
    }

    @ParameterizedTest
    @ApiKeyVersionsSource(apiKey = ApiKeys.OFFSET_COMMIT)
    public void testValidateOffsetCommit(short version) {
        // A call from the admin client without any parameters should pass.
        group.validateOffsetCommit("", "", -1, false, version);

        // Add a member.
        group.add(new ClassicGroupMember(
            "member-id",
            Optional.of("instance-id"),
            "",
            "",
            100,
            100,
            "consumer",
            new JoinGroupRequestProtocolCollection(Collections.singletonList(
                new JoinGroupRequestProtocol()
                    .setName("roundrobin")
                    .setMetadata(new byte[0])).iterator())
        ));

        group.transitionTo(PREPARING_REBALANCE);
        group.initNextGeneration();

        // No parameters and the group is not empty.
        assertThrows(UnknownMemberIdException.class,
            () -> group.validateOffsetCommit("", "", -1, false, version));

        // A transactional offset commit without any parameters
        // and a non-empty group is accepted.
        group.validateOffsetCommit("", null, -1, true, version);

        // The member id does not exist.
        assertThrows(UnknownMemberIdException.class,
            () -> group.validateOffsetCommit("unknown", "unknown", -1, false, version));

        // The instance id does not exist.
        assertThrows(UnknownMemberIdException.class,
            () -> group.validateOffsetCommit("member-id", "unknown", -1, false, version));

        // The generation id is invalid.
        assertThrows(IllegalGenerationException.class,
            () -> group.validateOffsetCommit("member-id", "instance-id", 0, false, version));

        // Group is in prepare rebalance state.
        assertThrows(RebalanceInProgressException.class,
            () -> group.validateOffsetCommit("member-id", "instance-id", 1, false, version));

        // Group transitions to stable.
        group.transitionTo(STABLE);

        // This should work.
        group.validateOffsetCommit("member-id", "instance-id", 1, false, version);

        // Replace static member.
        group.replaceStaticMember("instance-id", "member-id", "new-member-id");

        // The old instance id should be fenced.
        assertThrows(FencedInstanceIdException.class,
            () -> group.validateOffsetCommit("member-id", "instance-id", 1, false, version));

        // Remove member and transitions to dead.
        group.remove("new-instance-id");
        group.transitionTo(DEAD);

        // This should fail with CoordinatorNotAvailableException.
        assertThrows(CoordinatorNotAvailableException.class,
            () -> group.validateOffsetCommit("member-id", "new-instance-id", 1, false, version));
    }

    @Test
    public void testValidateOffsetDelete() {
        assertFalse(group.usesConsumerGroupProtocol());
        group.transitionTo(PREPARING_REBALANCE);
        assertThrows(GroupNotEmptyException.class, group::validateOffsetDelete);
        group.transitionTo(COMPLETING_REBALANCE);
        assertThrows(GroupNotEmptyException.class, group::validateOffsetDelete);
        group.transitionTo(STABLE);
        assertThrows(GroupNotEmptyException.class, group::validateOffsetDelete);

        JoinGroupRequestProtocolCollection protocols = new JoinGroupRequestProtocolCollection();
        protocols.add(new JoinGroupRequestProtocol()
            .setName("roundrobin")
            .setMetadata(new byte[0]));
        ClassicGroupMember member = new ClassicGroupMember(
            memberId,
            Optional.of(groupInstanceId),
            clientId,
            clientHost,
            rebalanceTimeoutMs,
            sessionTimeoutMs,
            protocolType,
            protocols
        );
        group.add(member);

        assertTrue(group.usesConsumerGroupProtocol());
        group.transitionTo(PREPARING_REBALANCE);
        assertDoesNotThrow(group::validateOffsetDelete);
        group.transitionTo(COMPLETING_REBALANCE);
        assertDoesNotThrow(group::validateOffsetDelete);
        group.transitionTo(STABLE);
        assertDoesNotThrow(group::validateOffsetDelete);

        group.transitionTo(PREPARING_REBALANCE);
        group.transitionTo(EMPTY);
        assertDoesNotThrow(group::validateOffsetDelete);
        group.transitionTo(DEAD);
        assertThrows(GroupIdNotFoundException.class, group::validateOffsetDelete);
    }

    @Test
    public void testValidateDeleteGroup() {
        group.transitionTo(PREPARING_REBALANCE);
        assertThrows(GroupNotEmptyException.class, group::validateDeleteGroup);
        group.transitionTo(COMPLETING_REBALANCE);
        assertThrows(GroupNotEmptyException.class, group::validateDeleteGroup);
        group.transitionTo(STABLE);
        assertThrows(GroupNotEmptyException.class, group::validateDeleteGroup);
        group.transitionTo(PREPARING_REBALANCE);
        group.transitionTo(EMPTY);
        assertDoesNotThrow(group::validateDeleteGroup);
        group.transitionTo(DEAD);
        assertThrows(GroupIdNotFoundException.class, group::validateDeleteGroup);
    }

    @Test
    public void testOffsetExpirationCondition() {
        long currentTimestamp = 30000L;
        long commitTimestamp = 20000L;
        long offsetsRetentionMs = 10000L;
        OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(15000L, OptionalInt.empty(), "", commitTimestamp, OptionalLong.empty());
        MockTime time = new MockTime();
        long currentStateTimestamp = time.milliseconds();
        ClassicGroup group = new ClassicGroup(new LogContext(), "groupId", EMPTY, time);

        // 1. Test no protocol type. Simple consumer case, Base timestamp based off of last commit timestamp.
        Optional<OffsetExpirationCondition> offsetExpirationCondition = group.offsetExpirationCondition();
        assertTrue(offsetExpirationCondition.isPresent());

        OffsetExpirationConditionImpl condition = (OffsetExpirationConditionImpl) offsetExpirationCondition.get();
        assertEquals(commitTimestamp, condition.baseTimestamp().apply(offsetAndMetadata));
        assertTrue(condition.isOffsetExpired(offsetAndMetadata, currentTimestamp, offsetsRetentionMs));

        // 2. Test non-consumer protocol type + Empty state. Base timestamp based off of current state timestamp.
        JoinGroupRequestProtocolCollection protocols = new JoinGroupRequestProtocolCollection();
        protocols.add(new JoinGroupRequestProtocol()
            .setName("range")
            .setMetadata(ConsumerProtocol.serializeSubscription(
                new ConsumerPartitionAssignor.Subscription(Collections.singletonList("topic"))).array()));

        ClassicGroupMember memberWithNonConsumerProtocol = new ClassicGroupMember(
            "memberWithNonConsumerProtocol",
            Optional.empty(),
            clientId,
            clientHost,
            rebalanceTimeoutMs,
            sessionTimeoutMs,
            "My Protocol",
            protocols
        );

        group.add(memberWithNonConsumerProtocol);
        assertEquals("My Protocol", group.protocolType().get());

        offsetExpirationCondition = group.offsetExpirationCondition();
        assertTrue(offsetExpirationCondition.isPresent());

        condition = (OffsetExpirationConditionImpl) offsetExpirationCondition.get();
        assertEquals(currentStateTimestamp, condition.baseTimestamp().apply(offsetAndMetadata));
        assertTrue(condition.isOffsetExpired(offsetAndMetadata, currentStateTimestamp + offsetsRetentionMs, offsetsRetentionMs));

        // 3. Test non-consumer protocol type + non-Empty state. Do not expire any offsets.
        group.transitionTo(PREPARING_REBALANCE);
        offsetExpirationCondition = group.offsetExpirationCondition();
        assertFalse(offsetExpirationCondition.isPresent());

        // 4. Test consumer protocol type + subscribed topics + Stable state. Base timestamp based off of last commit timestamp.
        group.remove("memberWithNonConsumerProtocol");
        ClassicGroupMember memberWithConsumerProtocol = new ClassicGroupMember(
            "memberWithConsumerProtocol",
            Optional.empty(),
            clientId,
            clientHost,
            rebalanceTimeoutMs,
            sessionTimeoutMs,
            "consumer",
            protocols
        );
        group.add(memberWithConsumerProtocol);
        group.initNextGeneration();
        group.transitionTo(STABLE);
        assertTrue(group.subscribedTopics().get().contains("topic"));

        offsetExpirationCondition = group.offsetExpirationCondition();
        assertTrue(offsetExpirationCondition.isPresent());

        condition = (OffsetExpirationConditionImpl) offsetExpirationCondition.get();
        assertEquals(commitTimestamp, condition.baseTimestamp().apply(offsetAndMetadata));
        assertTrue(condition.isOffsetExpired(offsetAndMetadata, currentTimestamp, offsetsRetentionMs));

        // 5. Test consumer protocol type + subscribed topics + non-Stable state. Do not expire any offsets.
        group.transitionTo(PREPARING_REBALANCE);
        offsetExpirationCondition = group.offsetExpirationCondition();
        assertFalse(offsetExpirationCondition.isPresent());
    }

    @Test
    public void testIsSubscribedToTopic() {
        ClassicGroup group = new ClassicGroup(new LogContext(), "groupId", EMPTY, Time.SYSTEM);

        // 1. group has no protocol type => not subscribed
        assertFalse(group.isSubscribedToTopic("topic"));

        // 2. group does not use consumer group protocol type => not subscribed
        JoinGroupRequestProtocolCollection protocols = new JoinGroupRequestProtocolCollection();
        protocols.add(new JoinGroupRequestProtocol()
            .setName("range")
            .setMetadata(ConsumerProtocol.serializeSubscription(
                new ConsumerPartitionAssignor.Subscription(Collections.singletonList("topic"))).array()));

        ClassicGroupMember memberWithNonConsumerProtocol = new ClassicGroupMember(
            "memberWithNonConsumerProtocol",
            Optional.empty(),
            clientId,
            clientHost,
            rebalanceTimeoutMs,
            sessionTimeoutMs,
            "My Protocol",
            protocols
        );

        group.add(memberWithNonConsumerProtocol);
        group.transitionTo(PREPARING_REBALANCE);
        group.initNextGeneration();
        assertTrue(group.isInState(COMPLETING_REBALANCE));
        assertEquals(Optional.empty(), group.computeSubscribedTopics());
        assertFalse(group.isSubscribedToTopic("topic"));

        // 3. group uses consumer group protocol type but empty members => not subscribed
        group.remove("memberWithNonConsumerProtocol");
        ClassicGroupMember memberWithConsumerProtocol = new ClassicGroupMember(
            "memberWithConsumerProtocol",
            Optional.empty(),
            clientId,
            clientHost,
            rebalanceTimeoutMs,
            sessionTimeoutMs,
            "consumer",
            protocols
        );

        group.add(memberWithConsumerProtocol);
        group.remove("memberWithConsumerProtocol");
        group.transitionTo(PREPARING_REBALANCE);
        group.initNextGeneration();
        assertTrue(group.isInState(EMPTY));
        assertEquals(Optional.of(Collections.emptySet()), group.computeSubscribedTopics());
        assertTrue(group.usesConsumerGroupProtocol());
        assertFalse(group.isSubscribedToTopic("topic"));

        // 4. group uses consumer group protocol type with member subscription => subscribed
        group.add(memberWithConsumerProtocol);
        group.transitionTo(PREPARING_REBALANCE);
        group.initNextGeneration();
        assertTrue(group.isInState(COMPLETING_REBALANCE));
        assertEquals(Optional.of(Collections.singleton("topic")), group.computeSubscribedTopics());
        assertTrue(group.usesConsumerGroupProtocol());
        assertTrue(group.isSubscribedToTopic("topic"));
    }

    @Test
    public void testIsInStates() {
        ClassicGroup group = new ClassicGroup(new LogContext(), "groupId", EMPTY, Time.SYSTEM);
        assertTrue(group.isInStates(Collections.singleton("empty"), 0));

        group.transitionTo(PREPARING_REBALANCE);
        assertTrue(group.isInStates(Collections.singleton("preparingrebalance"), 0));
        assertFalse(group.isInStates(Collections.singleton("PreparingRebalance"), 0));


        group.transitionTo(COMPLETING_REBALANCE);
        assertTrue(group.isInStates(new HashSet<>(Collections.singletonList("completingrebalance")), 0));

        group.transitionTo(STABLE);
        assertTrue(group.isInStates(Collections.singleton("stable"), 0));
        assertFalse(group.isInStates(Collections.singleton("empty"), 0));

        group.transitionTo(DEAD);
        assertTrue(group.isInStates(new HashSet<>(Arrays.asList("dead", " ")), 0));
    }

    @Test
    public void testCompleteAllJoinFutures() throws ExecutionException, InterruptedException {
        JoinGroupRequestProtocolCollection protocols = new JoinGroupRequestProtocolCollection();
        protocols.add(new JoinGroupRequestProtocol()
            .setName("roundrobin")
            .setMetadata(new byte[0]));

        List<ClassicGroupMember> memberList = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            memberList.add(new ClassicGroupMember(
                memberId + i,
                Optional.empty(),
                clientId,
                clientHost,
                rebalanceTimeoutMs,
                sessionTimeoutMs,
                protocolType,
                protocols
            ));
        }

        List<CompletableFuture<JoinGroupResponseData>> joinGroupFutureList = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            CompletableFuture<JoinGroupResponseData> future = new CompletableFuture<>();
            group.add(memberList.get(i), future);
            joinGroupFutureList.add(future);
        }

        assertEquals(3, group.numAwaitingJoinResponse());

        group.completeAllJoinFutures(Errors.REBALANCE_IN_PROGRESS);

        for (int i = 0; i < 3; i++) {
            assertEquals(Errors.REBALANCE_IN_PROGRESS.code(), joinGroupFutureList.get(i).get().errorCode());
            assertEquals(memberId + i, joinGroupFutureList.get(i).get().memberId());
            assertFalse(memberList.get(i).isAwaitingJoin());
        }
        assertEquals(0, group.numAwaitingJoinResponse());
    }

    @Test
    public void testCompleteAllSyncFutures() throws ExecutionException, InterruptedException {
        JoinGroupRequestProtocolCollection protocols = new JoinGroupRequestProtocolCollection();
        protocols.add(new JoinGroupRequestProtocol()
            .setName("roundrobin")
            .setMetadata(new byte[0]));

        List<ClassicGroupMember> memberList = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            ClassicGroupMember member = new ClassicGroupMember(
                memberId + i,
                Optional.empty(),
                clientId,
                clientHost,
                rebalanceTimeoutMs,
                sessionTimeoutMs,
                protocolType,
                protocols
            );
            memberList.add(member);
            group.add(member);
        }

        List<CompletableFuture<SyncGroupResponseData>> syncGroupFutureList = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            CompletableFuture<SyncGroupResponseData> syncGroupFuture = new CompletableFuture<>();
            syncGroupFutureList.add(syncGroupFuture);
            memberList.get(i).setAwaitingSyncFuture(syncGroupFuture);
        }

        group.completeAllSyncFutures(Errors.REBALANCE_IN_PROGRESS);

        for (int i = 0; i < 3; i++) {
            assertFalse(memberList.get(i).isAwaitingSync());
            assertEquals(Errors.REBALANCE_IN_PROGRESS.code(), syncGroupFutureList.get(i).get().errorCode());
        }
    }

    @Test
    public void testFromConsumerGroupWithJoiningMember() {
        MockTime time = new MockTime();
        String groupId = "group-id";
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();
        String newMemberId2 = Uuid.randomUuid().toString();
        String instanceId2 = "instance-id-2";

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";

        MetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 2)
            .addRacks()
            .build();

        ConsumerGroup consumerGroup = new ConsumerGroup(
            new SnapshotRegistry(logContext),
            groupId,
            mock(GroupCoordinatorMetricsShard.class)
        );
        consumerGroup.setGroupEpoch(10);
        consumerGroup.setTargetAssignmentEpoch(10);

        consumerGroup.updateTargetAssignment(memberId1, new Assignment(mkAssignment(
            mkTopicAssignment(fooTopicId, 0)
        )));
        consumerGroup.updateTargetAssignment(memberId2, new Assignment(mkAssignment(
            mkTopicAssignment(fooTopicId, 1)
        )));

        List<ConsumerGroupMemberMetadataValue.ClassicProtocol> protocols1 = Collections.singletonList(createClassicProtocol(
            "range",
            Collections.singletonList(fooTopicName),
            Collections.singletonList(new TopicPartition(fooTopicName, 0))
        ));
        List<ConsumerGroupMemberMetadataValue.ClassicProtocol> protocols2 = Collections.singletonList(createClassicProtocol(
            "range",
            Collections.singletonList(fooTopicName),
            Collections.singletonList(new TopicPartition(fooTopicName, 1))
        ));

        ConsumerGroupMember member1 = new ConsumerGroupMember.Builder(memberId1)
            .setState(MemberState.STABLE)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setClientId("client-id")
            .setClientHost("client-host")
            .setSubscribedTopicNames(Collections.singletonList(fooTopicName))
            .setServerAssignorName("range")
            .setRebalanceTimeoutMs(45000)
            .setClassicMemberMetadata(
                new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                    .setSessionTimeoutMs(5000)
                    .setSupportedProtocols(protocols1))
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(fooTopicId, 0)))
            .build();
        consumerGroup.updateMember(member1);

        ConsumerGroupMember member2 = new ConsumerGroupMember.Builder(memberId2)
            .setInstanceId(instanceId2)
            .setState(MemberState.STABLE)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setClientId("client-id")
            .setClientHost("client-host")
            .setSubscribedTopicNames(Collections.singletonList(fooTopicName))
            .setServerAssignorName("range")
            .setRebalanceTimeoutMs(45000)
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(fooTopicId, 1)))
            .build();
        consumerGroup.updateMember(member2);

        ConsumerGroupMember newMember2 = new ConsumerGroupMember.Builder(member2, newMemberId2)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(0)
            .setClientId("client-id")
            .setClientHost("client-host")
            .setSubscribedTopicNames(Collections.singletonList(fooTopicName))
            .setServerAssignorName("range")
            .setRebalanceTimeoutMs(45000)
            .setClassicMemberMetadata(
                new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                    .setSessionTimeoutMs(5000)
                    .setSupportedProtocols(protocols2))
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(fooTopicId, 1)))
            .build();

        ClassicGroup classicGroup = ClassicGroup.fromConsumerGroup(
            consumerGroup,
            memberId2,
            newMember2,
            logContext,
            time,
            metadataImage
        );

        ClassicGroup expectedClassicGroup = new ClassicGroup(
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
        expectedClassicGroup.add(
            new ClassicGroupMember(
                memberId1,
                Optional.empty(),
                member1.clientId(),
                member1.clientHost(),
                member1.rebalanceTimeoutMs(),
                member1.classicProtocolSessionTimeout().get(),
                ConsumerProtocol.PROTOCOL_TYPE,
                new JoinGroupRequestData.JoinGroupRequestProtocolCollection(Collections.singletonList(
                    new JoinGroupRequestData.JoinGroupRequestProtocol()
                        .setName(protocols1.get(0).name())
                        .setMetadata(protocols1.get(0).metadata())
                ).iterator()),
                Utils.toArray(ConsumerProtocol.serializeAssignment(new ConsumerPartitionAssignor.Assignment(
                    Collections.singletonList(new TopicPartition(fooTopicName, 0))
                )))
            )
        );
        expectedClassicGroup.add(
            new ClassicGroupMember(
                newMemberId2,
                Optional.of(instanceId2),
                newMember2.clientId(),
                newMember2.clientHost(),
                newMember2.rebalanceTimeoutMs(),
                newMember2.classicProtocolSessionTimeout().get(),
                ConsumerProtocol.PROTOCOL_TYPE,
                new JoinGroupRequestData.JoinGroupRequestProtocolCollection(Collections.singletonList(
                    new JoinGroupRequestData.JoinGroupRequestProtocol()
                        .setName(protocols2.get(0).name())
                        .setMetadata(protocols2.get(0).metadata())
                ).iterator()),
                Utils.toArray(ConsumerProtocol.serializeAssignment(new ConsumerPartitionAssignor.Assignment(
                    Collections.singletonList(new TopicPartition(fooTopicName, 1))
                )))
            )
        );

        assertClassicGroupEquals(expectedClassicGroup, classicGroup);
    }

    @Test
    public void testFromConsumerGroupWithoutJoiningMember() {
        MockTime time = new MockTime();
        String groupId = "group-id";
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();
        String instanceId2 = "instance-id-2";

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";

        MetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(fooTopicId, fooTopicName, 2)
            .addRacks()
            .build();

        ConsumerGroup consumerGroup = new ConsumerGroup(
            new SnapshotRegistry(logContext),
            groupId,
            mock(GroupCoordinatorMetricsShard.class)
        );
        consumerGroup.setGroupEpoch(10);
        consumerGroup.setTargetAssignmentEpoch(10);
        consumerGroup.updateTargetAssignment(memberId1, new Assignment(mkAssignment(
            mkTopicAssignment(fooTopicId, 0)
        )));
        consumerGroup.updateTargetAssignment(memberId2, new Assignment(mkAssignment(
            mkTopicAssignment(fooTopicId, 1)
        )));

        List<ConsumerGroupMemberMetadataValue.ClassicProtocol> protocols1 = Collections.singletonList(createClassicProtocol(
            "range",
            Collections.singletonList(fooTopicName),
            Collections.singletonList(new TopicPartition(fooTopicName, 0))
        ));

        ConsumerGroupMember member1 = new ConsumerGroupMember.Builder(memberId1)
            .setState(MemberState.STABLE)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setClientId("client-id")
            .setClientHost("client-host")
            .setSubscribedTopicNames(Collections.singletonList(fooTopicName))
            .setServerAssignorName("range")
            .setRebalanceTimeoutMs(45000)
            .setClassicMemberMetadata(
                new ConsumerGroupMemberMetadataValue.ClassicMemberMetadata()
                    .setSessionTimeoutMs(5000)
                    .setSupportedProtocols(protocols1))
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(fooTopicId, 0)))
            .build();
        consumerGroup.updateMember(member1);

        ConsumerGroupMember member2 = new ConsumerGroupMember.Builder(memberId2)
            .setInstanceId(instanceId2)
            .setState(MemberState.STABLE)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setClientId("client-id")
            .setClientHost("client-host")
            .setSubscribedTopicNames(Collections.singletonList(fooTopicName))
            .setServerAssignorName("range")
            .setRebalanceTimeoutMs(45000)
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(fooTopicId, 1)))
            .build();
        consumerGroup.updateMember(member2);

        ClassicGroup classicGroup = ClassicGroup.fromConsumerGroup(
            consumerGroup,
            memberId2,
            null,
            logContext,
            time,
            metadataImage
        );

        ClassicGroup expectedClassicGroup = new ClassicGroup(
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
        expectedClassicGroup.add(
            new ClassicGroupMember(
                memberId1,
                Optional.empty(),
                member1.clientId(),
                member1.clientHost(),
                member1.rebalanceTimeoutMs(),
                member1.classicProtocolSessionTimeout().get(),
                ConsumerProtocol.PROTOCOL_TYPE,
                new JoinGroupRequestData.JoinGroupRequestProtocolCollection(Collections.singletonList(
                    new JoinGroupRequestData.JoinGroupRequestProtocol()
                        .setName(protocols1.get(0).name())
                        .setMetadata(protocols1.get(0).metadata())
                ).iterator()),
                Utils.toArray(ConsumerProtocol.serializeAssignment(new ConsumerPartitionAssignor.Assignment(
                    Collections.singletonList(new TopicPartition(fooTopicName, 0))
                )))
            )
        );

        assertClassicGroupEquals(expectedClassicGroup, classicGroup);
    }

    private void assertState(ClassicGroup group, ClassicGroupState targetState) {
        Set<ClassicGroupState> otherStates = new HashSet<>();
        otherStates.add(STABLE);
        otherStates.add(PREPARING_REBALANCE);
        otherStates.add(COMPLETING_REBALANCE);
        otherStates.add(DEAD);
        otherStates.remove(targetState);

        otherStates.forEach(otherState -> assertFalse(group.isInState(otherState)));
        assertTrue(group.isInState(targetState));
    }

    private void assertClassicGroupEquals(ClassicGroup expected, ClassicGroup actual) {
        assertEquals(expected.groupId(), actual.groupId());
        assertEquals(expected.protocolName(), actual.protocolName());
        assertEquals(expected.protocolType(), actual.protocolType());
        assertEquals(expected.leaderOrNull(), actual.leaderOrNull());
        assertEquals(expected.stateAsString(), actual.stateAsString());
        assertEquals(expected.generationId(), actual.generationId());
        assertEquals(expected.allMembers().size(), actual.allMembers().size());
        expected.allMembers().forEach(expectedMember ->
            assertClassicGroupMemberEquals(expectedMember, actual.member(expectedMember.memberId())));
    }

    private void assertClassicGroupMemberEquals(ClassicGroupMember expected, ClassicGroupMember actual) {
        assertEquals(expected.memberId(), actual.memberId());
        assertEquals(expected.groupInstanceId(), actual.groupInstanceId());
        assertEquals(expected.clientId(), actual.clientId());
        assertEquals(expected.clientHost(), actual.clientHost());
        assertEquals(expected.rebalanceTimeoutMs(), actual.rebalanceTimeoutMs());
        assertEquals(expected.sessionTimeoutMs(), actual.sessionTimeoutMs());
        assertEquals(expected.protocolType(), actual.protocolType());
        assertEquals(expected.supportedProtocols(), actual.supportedProtocols());
        assertArrayEquals(expected.assignment(), actual.assignment());
    }

    private ConsumerGroupMemberMetadataValue.ClassicProtocol createClassicProtocol(
        String protocolName,
        List<String> subscribedTopics,
        List<TopicPartition> assignedTopicPartitions
    ) {
        return new ConsumerGroupMemberMetadataValue.ClassicProtocol()
            .setName(protocolName)
            .setMetadata(Utils.toArray(ConsumerProtocol.serializeSubscription(new ConsumerPartitionAssignor.Subscription(
                subscribedTopics,
                null,
                assignedTopicPartitions
            ))));
    }
}
