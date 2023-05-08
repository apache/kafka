/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.message.SyncGroupResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.kafka.coordinator.group.generic.GenericGroupState.CompletingRebalance;
import static org.apache.kafka.coordinator.group.generic.GenericGroupState.Dead;
import static org.apache.kafka.coordinator.group.generic.GenericGroupState.Empty;
import static org.apache.kafka.coordinator.group.generic.GenericGroupState.PreparingRebalance;
import static org.apache.kafka.coordinator.group.generic.GenericGroupState.Stable;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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
        group = new GenericGroup(new LogContext(), "groupId", Empty, Time.SYSTEM);
    }
    
    @Test
    public void testCanRebalanceWhenStable() {
        assertTrue(group.canRebalance());
    }
    
    @Test
    public void testCanRebalanceWhenCompletingRebalance() {
        group.transitionTo(PreparingRebalance);
        group.transitionTo(CompletingRebalance);
        assertTrue(group.canRebalance()); 
    }
    
    @Test
    public void testCannotRebalanceWhenPreparingRebalance() {
        group.transitionTo(PreparingRebalance);
        assertFalse(group.canRebalance());
    }

    @Test
    public void testCannotRebalanceWhenDead() {
        group.transitionTo(PreparingRebalance);
        group.transitionTo(Empty);
        group.transitionTo(Dead);
        assertFalse(group.canRebalance());
    }

    @Test
    public void testStableToPreparingRebalanceTransition() {
        group.transitionTo(PreparingRebalance);
        assertState(group, PreparingRebalance);
    }

    @Test
    public void testStableToDeadTransition() {
        group.transitionTo(Dead);
        assertState(group, Dead);
    }

    @Test
    public void testAwaitingRebalanceToPreparingRebalanceTransition() {
        group.transitionTo(PreparingRebalance);
        group.transitionTo(CompletingRebalance);
        group.transitionTo(PreparingRebalance);
        assertState(group, PreparingRebalance);
    }

    @Test
    public void testPreparingRebalanceToDeadTransition() {
        group.transitionTo(PreparingRebalance);
        group.transitionTo(Dead);
        assertState(group, Dead);
    }

    @Test
    public void testPreparingRebalanceToEmptyTransition() {
        group.transitionTo(PreparingRebalance);
        group.transitionTo(Empty);
        assertState(group, Empty);
    }

    @Test
    public void testEmptyToDeadTransition() {
        group.transitionTo(PreparingRebalance);
        group.transitionTo(Empty);
        group.transitionTo(Dead);
        assertState(group, Dead);
    }

    @Test
    public void testAwaitingRebalanceToStableTransition() {
        group.transitionTo(PreparingRebalance);
        group.transitionTo(CompletingRebalance);
        group.transitionTo(Stable);
        assertState(group, Stable);
    }

    @Test
    public void testEmptyToStableIllegalTransition() {
        assertThrows(IllegalStateException.class, () -> group.transitionTo(Stable));
    }

    @Test
    public void testStableToStableIllegalTransition() {
        group.transitionTo(PreparingRebalance);
        group.transitionTo(CompletingRebalance);
        group.transitionTo(Stable);
        assertThrows(IllegalStateException.class, () -> group.transitionTo(Stable));
    }

    @Test
    public void testEmptyToAwaitingRebalanceIllegalTransition() {
        assertThrows(IllegalStateException.class, () -> group.transitionTo(CompletingRebalance));
    }

    @Test
    public void testPreparingRebalanceToPreparingRebalanceIllegalTransition() {
        group.transitionTo(PreparingRebalance);
        assertThrows(IllegalStateException.class, () -> group.transitionTo(PreparingRebalance));
    }

    @Test
    public void testPreparingRebalanceToStableIllegalTransition() {
        group.transitionTo(PreparingRebalance);
        assertThrows(IllegalStateException.class, () -> group.transitionTo(Stable));
    }

    @Test
    public void testAwaitingRebalanceToAwaitingRebalanceIllegalTransition() {
        group.transitionTo(PreparingRebalance);
        group.transitionTo(CompletingRebalance);
        assertThrows(IllegalStateException.class, () -> group.transitionTo(CompletingRebalance));
    }

    @Test
    public void testDeadToDeadIllegalTransition() {
        group.transitionTo(PreparingRebalance);
        group.transitionTo(Dead);
        group.transitionTo(Dead);
        assertState(group, Dead);
    }

    @Test
    public void testDeadToStableIllegalTransition() {
        group.transitionTo(PreparingRebalance);
        group.transitionTo(Dead);
        assertThrows(IllegalStateException.class, () -> group.transitionTo(Stable));
    }

    @Test
    public void testDeadToPreparingRebalanceIllegalTransition() {
        group.transitionTo(PreparingRebalance);
        group.transitionTo(Dead);
        assertThrows(IllegalStateException.class, () -> group.transitionTo(PreparingRebalance));
    }

    @Test
    public void testDeadToAwaitingRebalanceIllegalTransition() {
        group.transitionTo(PreparingRebalance);
        group.transitionTo(Dead);
        assertThrows(IllegalStateException.class, () -> group.transitionTo(CompletingRebalance));
    }

    @Test
    public void testSelectProtocol() {
        List<Protocol> member1Protocols = Arrays.asList(
            new Protocol("range", new byte[0]),
            new Protocol("roundrobin", new byte[0])
        );

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

        List<Protocol> member2Protocols = Arrays.asList(
            new Protocol("roundrobin", new byte[0]),
        new Protocol("range", new byte[0])
        );

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
        List<Protocol> member1Protocols = Arrays.asList(
            new Protocol("range", new byte[0]),
            new Protocol("roundrobin", new byte[0])
        );

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

        List<Protocol> member2Protocols = Arrays.asList(
            new Protocol("roundrobin", new byte[0]),
            new Protocol("blah", new byte[0])
        );

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
        List<Protocol> member1Protocols = Arrays.asList(
            new Protocol("range", new byte[0]),
            new Protocol("roundrobin", new byte[0])
        );

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

        // by public voidault, the group supports everything
        Set<String> expectedProtocols = new HashSet<>();
        member1Protocols.forEach(protocol -> expectedProtocols.add(protocol.name()));
        assertTrue(group.supportsProtocols(protocolType, expectedProtocols));

        group.add(member1);
        group.transitionTo(PreparingRebalance);

        expectedProtocols.clear();
        expectedProtocols.add("roundrobin");
        expectedProtocols.add("foo");
        assertTrue(group.supportsProtocols(protocolType, expectedProtocols));

        expectedProtocols.clear();
        expectedProtocols.add("range");
        expectedProtocols.add("bar");
        assertTrue(group.supportsProtocols(protocolType, expectedProtocols));

        expectedProtocols.clear();
        expectedProtocols.add("foo");
        expectedProtocols.add("bar");
        assertFalse(group.supportsProtocols(protocolType, expectedProtocols));
    }

    @Test
    public void testSubscribedTopics() {
        // not able to compute it for a newly created group
        assertEquals(Optional.empty(), group.subscribedTopics());

        GenericGroupMember member = new GenericGroupMember(
            memberId,
            Optional.empty(),
            clientId,
            clientHost,
            rebalanceTimeoutMs,
            sessionTimeoutMs,
            protocolType,
            Collections.singletonList(
                new Protocol(
                    "range",
                    ConsumerProtocol.serializeSubscription(
                        new ConsumerPartitionAssignor.Subscription(
                            Collections.singletonList("foo")
                        )
                    ).array()
                )
            )
        );

        group.transitionTo(PreparingRebalance);
        group.add(member);

        group.initNextGeneration();

        Set<String> expectedTopics = new HashSet<>(Collections.singleton("foo"));
        assertEquals(expectedTopics, group.subscribedTopics().get());

        group.transitionTo(PreparingRebalance);
        group.remove(memberId);

        group.initNextGeneration();

        assertEquals(Optional.of(Collections.emptySet()), group.subscribedTopics());

        GenericGroupMember memberWithFaultyProtocol = new GenericGroupMember(
            memberId,
            Optional.empty(),
            clientId,
            clientHost,
            rebalanceTimeoutMs,
            sessionTimeoutMs,
            protocolType,
            Collections.singletonList(
                new Protocol(
                    "range",
                    new byte[0]
                )
            )
        );

        group.transitionTo(PreparingRebalance);
        group.add(memberWithFaultyProtocol);

        group.initNextGeneration();

        assertEquals(Optional.empty(), group.subscribedTopics());
    }

    @Test
    public void testSubscribedTopicsNonConsumerGroup() {
        // not able to compute it for a newly created group
        assertEquals(Optional.empty(), group.subscribedTopics());

        GenericGroupMember memberWithNonConsumerProtocol = new GenericGroupMember(
            memberId,
            Optional.empty(),
            clientId,
            clientHost,
            rebalanceTimeoutMs,
            sessionTimeoutMs,
            "My Protocol",
            Collections.singletonList(
                new Protocol(
                    "range",
                    new byte[0]
                )
            )
        );

        group.transitionTo(PreparingRebalance);
        group.add(memberWithNonConsumerProtocol);

        group.initNextGeneration();

        assertEquals(Optional.empty(), group.subscribedTopics());
    }

    @Test
    public void testInitNextGeneration() {
        GenericGroupMember member = new GenericGroupMember(
            memberId,
            Optional.empty(),
            clientId,
            clientHost,
            rebalanceTimeoutMs,
            sessionTimeoutMs,
            protocolType,
            Collections.singletonList(
                new Protocol(
                    "roundrobin",
                    new byte[0]
                )
            )
        );

        group.transitionTo(PreparingRebalance);
        group.add(member, new CompletableFuture<>());

        assertEquals(0, group.generationId());
        assertNull(group.protocolName().orElse(null));

        group.initNextGeneration();

        assertEquals(1, group.generationId());
        assertEquals("roundrobin", group.protocolName().orElse(null));
    }

    @Test
    public void testInitNextGenerationEmptyGroup() {
        assertEquals(Empty, group.currentState());
        assertEquals(0, group.generationId());
        assertNull(group.protocolName().orElse(null));

        group.transitionTo(PreparingRebalance);
        group.initNextGeneration();

        assertEquals(1, group.generationId());
        assertNull(group.protocolName().orElse(null));
    }

    @Test
    public void testUpdateMember() {
        GenericGroupMember member = new GenericGroupMember(
            memberId,
            Optional.empty(),
            clientId,
            clientHost,
            rebalanceTimeoutMs,
            sessionTimeoutMs,
            protocolType,
            Collections.singletonList(
                new Protocol(
                    "roundrobin",
                    new byte[0]
                )
            )
        );

        group.add(member);

        List<Protocol> newProtocols = Arrays.asList(
            new Protocol(
                "range",
                new byte[0]
            ),
            new Protocol(
                "roundrobin",
                new byte[0]
            )
        );
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
    public void testReplaceGroupInstance() {
        GenericGroupMember member = new GenericGroupMember(
            memberId,
            Optional.of(groupInstanceId),
            clientId,
            clientHost,
            rebalanceTimeoutMs,
            sessionTimeoutMs,
            protocolType,
            Collections.singletonList(
                new Protocol(
                    "roundrobin",
                    new byte[0]
                )
            )
        );

        AtomicBoolean joinAwaitingMemberFenced = new AtomicBoolean(false);
        CompletableFuture<JoinGroupResponseData> joinGroupFuture = new CompletableFuture<>();
        joinGroupFuture.whenComplete((joinGroupResult, __) ->
            joinAwaitingMemberFenced.set(joinGroupResult.errorCode() == Errors.FENCED_INSTANCE_ID.code()));
        group.add(member, joinGroupFuture);

        AtomicBoolean syncAwaitingMemberFenced = new AtomicBoolean(false);
        CompletableFuture<SyncGroupResponseData> syncGroupFuture = new CompletableFuture<>();
        syncGroupFuture.whenComplete((syncGroupResult, __) ->
            syncAwaitingMemberFenced.set(syncGroupResult.errorCode() == Errors.FENCED_INSTANCE_ID.code()));
        member.setAwaitingSyncCallback(syncGroupFuture);

        assertTrue(group.isLeader(memberId));
        assertEquals(memberId, group.currentStaticMemberId(groupInstanceId));

        String newMemberId = "newMemberId";
        group.replaceStaticMember(groupInstanceId, memberId, newMemberId);
        assertTrue(group.isLeader(newMemberId));
        assertEquals(newMemberId, group.currentStaticMemberId(groupInstanceId));
        assertTrue(joinAwaitingMemberFenced.get());
        assertTrue(syncAwaitingMemberFenced.get());
        assertFalse(member.isAwaitingJoin());
        assertFalse(member.isAwaitingSync());
    }

    @Test
    public void testCompleteJoinFuture() {
        GenericGroupMember member = new GenericGroupMember(
            memberId,
            Optional.empty(),
            clientId,
            clientHost,
            rebalanceTimeoutMs,
            sessionTimeoutMs,
            protocolType,
            Collections.singletonList(
                new Protocol(
                    "roundrobin",
                    new byte[0]
                )
            )
        );

        AtomicBoolean invoked = new AtomicBoolean(false);
        CompletableFuture<JoinGroupResponseData> joinGroupFuture = new CompletableFuture<>();
        joinGroupFuture.whenComplete((__, ___) ->
            invoked.set(true));
        group.add(member, joinGroupFuture);

        assertTrue(group.hasAllMembersJoined());
        group.completeJoinFuture(member, new JoinGroupResponseData()
            .setMemberId(member.memberId())
            .setErrorCode(Errors.NONE.code()));

        assertTrue(invoked.get());
        assertFalse(member.isAwaitingJoin());
    }

    @Test
    public void testNotCompleteJoinFuture() {
        GenericGroupMember member = new GenericGroupMember(
            memberId,
            Optional.empty(),
            clientId,
            clientHost,
            rebalanceTimeoutMs,
            sessionTimeoutMs,
            protocolType,
            Collections.singletonList(
                new Protocol(
                    "roundrobin",
                    new byte[0]
                )
            )
        );

        group.add(member);

        assertFalse(member.isAwaitingJoin());
        group.completeJoinFuture(member, new JoinGroupResponseData()
            .setMemberId(member.memberId())
            .setErrorCode(Errors.NONE.code()));

        assertFalse(member.isAwaitingJoin());
    }

    @Test
    public void testCompleteSyncFuture() {
        GenericGroupMember member = new GenericGroupMember(
            memberId,
            Optional.empty(),
            clientId,
            clientHost,
            rebalanceTimeoutMs,
            sessionTimeoutMs,
            protocolType,
            Collections.singletonList(
                new Protocol(
                    "roundrobin",
                    new byte[0]
                )
            )
        );

        group.add(member);
        CompletableFuture<SyncGroupResponseData> syncGroupFuture = new CompletableFuture<>();
        member.setAwaitingSyncCallback(syncGroupFuture);

        assertTrue(group.completeSyncFuture(member, new SyncGroupResponseData()
            .setErrorCode(Errors.NONE.code())));

        assertFalse(member.isAwaitingSync());
    }

    @Test
    public void testNotCompleteSyncFuture() {
        GenericGroupMember member = new GenericGroupMember(
            memberId,
            Optional.empty(),
            clientId,
            clientHost,
            rebalanceTimeoutMs,
            sessionTimeoutMs,
            protocolType,
            Collections.singletonList(
                new Protocol(
                    "roundrobin",
                    new byte[0]
                )
            )
        );

        group.add(member);

        assertFalse(group.completeSyncFuture(member, new SyncGroupResponseData()
            .setErrorCode(Errors.NONE.code())));

        assertFalse(member.isAwaitingSync());
    }

    @Test
    public void testCannotAddPendingMemberIfStable() {
        GenericGroupMember member = new GenericGroupMember(
            memberId,
            Optional.empty(),
            clientId,
            clientHost,
            rebalanceTimeoutMs,
            sessionTimeoutMs,
            protocolType,
            Collections.singletonList(
                new Protocol(
                    "roundrobin",
                    new byte[0]
                )
            )
        );

        group.add(member);
        assertThrows(IllegalStateException.class, () -> group.addPendingMember(memberId));
    }

    @Test
    public void testRemovalFromPendingAfterMemberIsStable() {
        group.addPendingMember(memberId);
        assertFalse(group.has(memberId));
        assertTrue(group.isPendingMember(memberId));

        GenericGroupMember member = new GenericGroupMember(
            memberId,
            Optional.empty(),
            clientId,
            clientHost,
            rebalanceTimeoutMs,
            sessionTimeoutMs,
            protocolType,
            Collections.singletonList(
                new Protocol(
                    "roundrobin",
                    new byte[0]
                )
            )
        );

        group.add(member);
        assertTrue(group.has(memberId));
        assertFalse(group.isPendingMember(memberId));
    }

    @Test
    public void testRemovalFromPendingWhenMemberIsRemoved() {
        group.addPendingMember(memberId);
        assertFalse(group.has(memberId));
        assertTrue(group.isPendingMember(memberId));

        group.remove(memberId);
        assertFalse(group.has(memberId));
        assertFalse(group.isPendingMember(memberId));
    }

    @Test
    public void testCannotAddStaticMemberIfAlreadyPresent() {
        GenericGroupMember member = new GenericGroupMember(
            memberId,
            Optional.of(groupInstanceId),
            clientId,
            clientHost,
            rebalanceTimeoutMs,
            sessionTimeoutMs,
            protocolType,
            Collections.singletonList(
                new Protocol(
                    "roundrobin",
                    new byte[0]
                )
            )
        );
        
        group.add(member);
        assertTrue(group.has(memberId));
        assertTrue(group.hasStaticMember(groupInstanceId));

        // We aren ot permitted to add the member again if it is already present
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
        GenericGroupMember member = new GenericGroupMember(
            memberId,
            Optional.empty(),
            clientId,
            clientHost,
            rebalanceTimeoutMs,
            sessionTimeoutMs,
            protocolType,
            Collections.singletonList(
                new Protocol(
                    "roundrobin",
                    new byte[0]
                )
            )
        );
        
        group.add(member);
        assertTrue(group.addPendingSyncMember(memberId));
        assertEquals(Collections.singleton(memberId), group.allPendingSyncMembers());
        group.removePendingSyncMember(memberId);
        assertEquals(Collections.emptySet(), group.allPendingSyncMembers());
    }

    @Test
    public void testRemovalFromPendingSyncWhenMemberIsRemoved() {
        GenericGroupMember member = new GenericGroupMember(
            memberId,
            Optional.of(groupInstanceId),
            clientId,
            clientHost,
            rebalanceTimeoutMs,
            sessionTimeoutMs,
            protocolType,
            Collections.singletonList(
                new Protocol(
                    "roundrobin",
                    new byte[0]
                )
            )
        );

        group.add(member);
        assertTrue(group.addPendingSyncMember(memberId));
        assertEquals(Collections.singleton(memberId), group.allPendingSyncMembers());
        group.remove(memberId);
        assertEquals(Collections.emptySet(), group.allPendingSyncMembers());
    }

    @Test
    public void testNewGenerationClearsPendingSyncMembers() {
        GenericGroupMember member = new GenericGroupMember(
            memberId,
            Optional.empty(),
            clientId,
            clientHost,
            rebalanceTimeoutMs,
            sessionTimeoutMs,
            protocolType,
            Collections.singletonList(
                new Protocol(
                    "roundrobin",
                    new byte[0]
                )
            )
        );

        group.add(member);
        group.transitionTo(PreparingRebalance);
        assertTrue(group.addPendingSyncMember(memberId));
        assertEquals(Collections.singleton(memberId), group.allPendingSyncMembers());
        group.initNextGeneration();
        assertEquals(Collections.emptySet(), group.allPendingSyncMembers());
    }

    private void assertState(GenericGroup group, GenericGroupState targetState) {
        Set<GenericGroupState> otherStates = new HashSet<>();
        otherStates.add(Stable);
        otherStates.add(PreparingRebalance);
        otherStates.add(CompletingRebalance);
        otherStates.add(Dead);
        otherStates.remove(targetState);

        otherStates.forEach(otherState -> assertFalse(group.is(otherState)));
        assertTrue(group.is(targetState));
    }
}
