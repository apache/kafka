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
package org.apache.kafka.coordinator.group.generic;

import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol;
import org.apache.kafka.common.errors.CoordinatorNotAvailableException;
import org.apache.kafka.common.errors.FencedInstanceIdException;
import org.apache.kafka.common.errors.IllegalGenerationException;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.message.JoinGroupRequestData.JoinGroupRequestProtocol;
import org.apache.kafka.common.message.JoinGroupRequestData.JoinGroupRequestProtocolCollection;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.message.SyncGroupResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.apache.kafka.coordinator.group.generic.GenericGroupState.COMPLETING_REBALANCE;
import static org.apache.kafka.coordinator.group.generic.GenericGroupState.DEAD;
import static org.apache.kafka.coordinator.group.generic.GenericGroupState.EMPTY;
import static org.apache.kafka.coordinator.group.generic.GenericGroupState.PREPARING_REBALANCE;
import static org.apache.kafka.coordinator.group.generic.GenericGroupState.STABLE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class GenericGroupTest {
    private final String protocolType = "consumer";
    private final String groupInstanceId = "groupInstanceId";
    private final String memberId = "memberId";
    private final String clientId = "clientId";
    private final String clientHost = "clientHost";
    private final int rebalanceTimeoutMs = 60000;
    private final int sessionTimeoutMs = 10000;

    private GenericGroup group = null;
    
    @BeforeEach
    public void initialize() {
        group = new GenericGroup(new LogContext(), "groupId", EMPTY, Time.SYSTEM);
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

        GenericGroupMember member1 = new GenericGroupMember(
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

        GenericGroupMember member2 = new GenericGroupMember(
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

        GenericGroupMember member3 = new GenericGroupMember(
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

        GenericGroupMember member1 = new GenericGroupMember(
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


        GenericGroupMember member2 = new GenericGroupMember(
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

        GenericGroupMember member1 = new GenericGroupMember(
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
        assertTrue(group.supportsProtocols(protocolType, mkSet("range", "roundrobin")));

        group.add(member1);
        group.transitionTo(PREPARING_REBALANCE);

        assertTrue(group.supportsProtocols(protocolType, mkSet("roundrobin", "foo")));
        assertTrue(group.supportsProtocols(protocolType, mkSet("range", "bar")));
        assertFalse(group.supportsProtocols(protocolType, mkSet("foo", "bar")));
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

        GenericGroupMember member = new GenericGroupMember(
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

        GenericGroupMember memberWithFaultyProtocol = new GenericGroupMember(
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

        GenericGroupMember memberWithNonConsumerProtocol = new GenericGroupMember(
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

        GenericGroupMember member = new GenericGroupMember(
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

        GenericGroupMember member = new GenericGroupMember(
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

        GenericGroupMember member = new GenericGroupMember(
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

        GenericGroupMember member = new GenericGroupMember(
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

        GenericGroupMember member = new GenericGroupMember(
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

        GenericGroupMember member = new GenericGroupMember(
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

        GenericGroupMember member = new GenericGroupMember(
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

        GenericGroupMember member = new GenericGroupMember(
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
        assertFalse(group.hasMemberId(memberId));
        assertTrue(group.isPendingMember(memberId));

        GenericGroupMember member = new GenericGroupMember(
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
        assertTrue(group.hasMemberId(memberId));
        assertFalse(group.isPendingMember(memberId));
    }

    @Test
    public void testRemovalFromPendingWhenMemberIsRemoved() {
        group.addPendingMember(memberId);
        assertFalse(group.hasMemberId(memberId));
        assertTrue(group.isPendingMember(memberId));

        group.remove(memberId);
        assertFalse(group.hasMemberId(memberId));
        assertFalse(group.isPendingMember(memberId));
    }

    @Test
    public void testCannotAddStaticMemberIfAlreadyPresent() {
        JoinGroupRequestProtocolCollection protocols = new JoinGroupRequestProtocolCollection();
        protocols.add(new JoinGroupRequestProtocol()
            .setName("roundrobin")
            .setMetadata(new byte[0]));

        GenericGroupMember member = new GenericGroupMember(
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
        assertTrue(group.hasMemberId(memberId));
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

        GenericGroupMember member = new GenericGroupMember(
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

        GenericGroupMember member = new GenericGroupMember(
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

        GenericGroupMember member = new GenericGroupMember(
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

        GenericGroupMember leader = new GenericGroupMember(
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

        GenericGroupMember newLeader = new GenericGroupMember(
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

        GenericGroupMember newMember = new GenericGroupMember(
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

        GenericGroupMember leader = new GenericGroupMember(
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

        GenericGroupMember newMember = new GenericGroupMember(
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

    @Test
    public void testValidateOffsetCommit() {
        // A call from the admin client without any parameters should pass.
        group.validateOffsetCommit("", "", -1);

        // Add a member.
        group.add(new GenericGroupMember(
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
            () -> group.validateOffsetCommit("", "", -1));

        // The member id does not exist.
        assertThrows(UnknownMemberIdException.class,
            () -> group.validateOffsetCommit("unknown", "unknown", -1));

        // The instance id does not exist.
        assertThrows(UnknownMemberIdException.class,
            () -> group.validateOffsetCommit("member-id", "unknown", -1));

        // The generation id is invalid.
        assertThrows(IllegalGenerationException.class,
            () -> group.validateOffsetCommit("member-id", "instance-id", 0));

        // Group is in prepare rebalance state.
        assertThrows(RebalanceInProgressException.class,
            () -> group.validateOffsetCommit("member-id", "instance-id", 1));

        // Group transitions to stable.
        group.transitionTo(STABLE);

        // This should work.
        group.validateOffsetCommit("member-id", "instance-id", 1);

        // Replace static member.
        group.replaceStaticMember("instance-id", "member-id", "new-member-id");

        // The old instance id should be fenced.
        assertThrows(FencedInstanceIdException.class,
            () -> group.validateOffsetCommit("member-id", "instance-id", 1));

        // Remove member and transitions to dead.
        group.remove("new-instance-id");
        group.transitionTo(DEAD);

        // This should fail with CoordinatorNotAvailableException.
        assertThrows(CoordinatorNotAvailableException.class,
            () -> group.validateOffsetCommit("member-id", "new-instance-id", 1));
    }

    private void assertState(GenericGroup group, GenericGroupState targetState) {
        Set<GenericGroupState> otherStates = new HashSet<>();
        otherStates.add(STABLE);
        otherStates.add(PREPARING_REBALANCE);
        otherStates.add(COMPLETING_REBALANCE);
        otherStates.add(DEAD);
        otherStates.remove(targetState);

        otherStates.forEach(otherState -> assertFalse(group.isInState(otherState)));
        assertTrue(group.isInState(targetState));
    }
}
