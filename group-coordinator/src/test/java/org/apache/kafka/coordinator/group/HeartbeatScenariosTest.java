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

import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.internals.MemberState;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.ConsumerGroupHeartbeatRequest;
import org.apache.kafka.coordinator.group.modern.consumer.ConsumerGroup;

import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Scripted scenarios of the heartbeat simulation. Each scenario first checks that it reached the
 * situation it describes, then asserts the expected behavior. The scenarios that fail on the
 * current code are disabled, with the issue as the reason.
 */
public class HeartbeatScenariosTest {

    private HeartbeatSimulation simulation;
    private String testName;

    @BeforeAll
    public static void setUpLogging() {
        HeartbeatSimulation.setLogLevel(Level.OFF);
    }

    @AfterAll
    public static void resetLogging() {
        HeartbeatSimulation.resetLogLevel();
    }

    @BeforeEach
    public void setUp(TestInfo info) {
        testName = info.getDisplayName();
    }

    @AfterEach
    public void tearDown() {
        if (simulation != null && Boolean.getBoolean("heartbeat.sim.verbose")) {
            System.out.println("=== " + testName);
            simulation.trace().forEach(System.out::println);
            simulation.violations().forEach(System.out::println);
        }
    }

    private HeartbeatSimulation simulation(Map<String, Integer> topics) {
        simulation = HeartbeatSimulation.scripted(HeartbeatSimulation.Params.scripted(topics));
        return simulation;
    }

    private static Set<String> names(Set<TopicPartition> partitions) {
        return partitions.stream().map(TopicPartition::toString).collect(Collectors.toCollection(TreeSet::new));
    }

    private ConsumerGroup group() {
        return simulation.group().orElseThrow();
    }

    private List<String> violations(String kind) {
        return simulation.violations().stream().filter(v -> v.contains("] " + kind + ":")).toList();
    }

    /**
     * A dynamic member that closes with REMAIN_IN_GROUP does not send a heartbeat to leave the
     * group: it stays in the group until its session expires.
     */
    @Test
    @Disabled("KAFKA-20516: close(REMAIN_IN_GROUP) of a dynamic member sends a heartbeat to leave the group")
    public void testDynamicMemberClosedWithRemainInGroupStaysInGroup() {
        HeartbeatSimulation sim = simulation(Map.of("t0", 2));
        HeartbeatSimulation.Driver c1 = sim.start("C1", null, Set.of("t0"));
        sim.runUntil(3000);
        assertEquals(MemberState.STABLE, c1.consumer.state());
        String memberId = c1.consumer.memberId();

        c1.closeLater(CloseOptions.GroupMembershipOperation.REMAIN_IN_GROUP);
        sim.runUntil(4000);
        assertTrue(c1.consumer.isClosed());

        assertEquals(Set.of(memberId), group().members().keySet());
    }

    /**
     * The poll timer of a dynamic member expires while the network thread stalls, and the
     * application closes the consumer with REMAIN_IN_GROUP at that time. The network thread
     * closes the consumer without errors.
     */
    @Test
    @Disabled("KAFKA-21220: close(REMAIN_IN_GROUP) after the poll timer expired fails with an IllegalStateException")
    public void testDynamicMemberClosedWithRemainInGroupWhenPollTimerExpires() {
        HeartbeatSimulation sim = simulation(Map.of("t0", 2));
        HeartbeatSimulation.Driver c1 = sim.start("C1", null, Set.of("t0"));
        sim.runUntil(3000);
        assertEquals(MemberState.STABLE, c1.consumer.state());

        // The application processes records for longer than max.poll.interval.ms (10 seconds)
        // then closes the consumer; the network thread stalls around the expiration of the
        // poll timer.
        c1.pause(10_500);
        sim.at(12_900, "C1 network thread stalls", () -> c1.stallNetworkThread(700));
        c1.closeLater(CloseOptions.GroupMembershipOperation.REMAIN_IN_GROUP);
        sim.runUntil(20_000);
        assertEquals(List.of(), simulation.violations().stream().filter(v -> v.contains("exception:")).toList());
        assertTrue(c1.consumer.isClosed());
    }

    /**
     * A reconciliation adds a partition in the background while the application processes
     * records. The member leaves the group because its poll timer expires before the
     * application thread applies the assignment. When the application polls again, the
     * partition must not be passed to onPartitionsAssigned without being passed to
     * onPartitionsLost (or onPartitionsRevoked) afterwards.
     */
    @Test
    @Disabled("The assignment of a reconciliation is applied after the member left the group")
    public void testAssignmentOfAbortedReconciliationIsNotApplied() {
        HeartbeatSimulation sim = simulation(Map.of("t0", 2));
        HeartbeatSimulation.Driver c1 = sim.start("C1", null, Set.of("t0"));
        HeartbeatSimulation.Driver c2 = sim.start("C2", null, Set.of("t0"));
        sim.runUntil(3000);
        assertEquals(1, c1.consumer.assignedPartitions().size());
        assertEquals(1, c2.consumer.assignedPartitions().size());

        // C1 processes records for longer than max.poll.interval.ms while C2 leaves the group.
        c1.pause(12_000);
        sim.at(4000, "C2 closes", () -> c2.closeLater(CloseOptions.GroupMembershipOperation.DEFAULT));
        sim.runUntil(14_000);
        assertEquals(MemberState.STALE, c1.consumer.state());

        sim.runUntil(20_000);
        assertEquals(MemberState.STABLE, c1.consumer.state());
        assertEquals(names(c1.consumer.assignedPartitions()), names(c1.consumer.ownedByCallbacks()));
        assertEquals(List.of(), violations("stale-assignment-applied"));
    }

    /**
     * The heartbeat with which a member leaves the group fails to be written. The member is
     * still removed once its session expires, so its partitions go to the other members.
     */
    @Test
    @Disabled("KAFKA-21279: a member whose leave heartbeat fails to be written has no session timer")
    public void testMemberRemovedAfterFailedLeaveHeartbeat() {
        HeartbeatSimulation sim = simulation(Map.of("t0", 2));
        HeartbeatSimulation.Driver c1 = sim.start("C1", null, Set.of("t0"));
        HeartbeatSimulation.Driver c2 = sim.start("C2", null, Set.of("t0"));
        sim.runUntil(3000);
        assertEquals(2, group().members().size());

        sim.failAppend(request -> request.equals("C2 CONSUMER_GROUP_HEARTBEAT epoch -1"));
        c2.closeLater(CloseOptions.GroupMembershipOperation.DEFAULT);
        sim.runUntil(4000);
        assertTrue(c2.consumer.isClosed());
        assertTrue(sim.trace().stream().anyMatch(line -> line.contains("coordinator append fails for C2")));

        // Three session timeouts later.
        sim.runUntil(22_000);
        assertEquals(Set.of(c1.consumer.memberId()), group().members().keySet());
        assertEquals(Set.of("t0-0", "t0-1"), names(c1.consumer.assignedPartitions()));
    }

    /**
     * The member is fenced while the offset fetch that initializes the positions of its new
     * partitions is in flight. The consumer rejoins the group; poll does not throw.
     */
    @Test
    @Disabled("poll throws when the member is fenced while it fetches committed offsets")
    public void testFencedWhileFetchingCommittedOffsets() {
        HeartbeatSimulation sim = simulation(Map.of("t0", 2));
        HeartbeatSimulation.Driver c1 = sim.start("C1", null, Set.of("t0"));
        // All the requests of C1 are lost from the join response to after its session expired.
        sim.partition(c1, 50, 9_000);
        sim.runUntil(20_000);
        assertTrue(sim.trace().stream().anyMatch(line -> line.contains("C1 OFFSET_FETCH is lost")));

        assertEquals(MemberState.STABLE, c1.consumer.state());
        assertEquals(List.of(), violations("member-error-thrown"));
    }

    /**
     * A reconciliation revokes a partition and the application unsubscribes before the
     * reconciliation applies its assignment. The partition is passed to onPartitionsRevoked
     * once.
     */
    @Test
    @Disabled("unsubscribe releases the partitions revoked by a reconciliation in progress again")
    public void testUnsubscribeAfterRevocationOfReconciliation() {
        HeartbeatSimulation sim = simulation(Map.of("t0", 2));
        HeartbeatSimulation.Driver c1 = sim.start("C1", null, Set.of("t0"));
        sim.runUntil(3000);
        assertEquals(Set.of("t0-0", "t0-1"), names(c1.consumer.assignedPartitions()));

        // C2 joins: C1 revokes one partition in a callback that runs for one second, and the
        // application unsubscribes right after the poll in which the callback ran.
        c1.scriptedCallbackDurationMs = 1000;
        sim.start("C2", null, Set.of("t0"));
        sim.at(3500, "C1 unsubscribes", c1::unsubscribeLater);
        sim.runUntil(8000);
        assertEquals("onPartitionsAssigned [t0-0, t0-1]", c1.callbacks.get(0));
        assertTrue(c1.callbacks.get(1).matches("onPartitionsRevoked \\[t0-[01]\\]"), "C1 callbacks: " + c1.callbacks);

        assertEquals(List.of(), violations("callback-released-unowned"));
    }

    /**
     * The application unsubscribes, which revokes the partitions, then closes the consumer.
     * Close does not invoke onPartitionsLost for the partitions revoked by unsubscribe.
     */
    @Test
    @Disabled("close after unsubscribe calls onPartitionsLost for the partitions revoked by unsubscribe")
    public void testCloseAfterUnsubscribe() {
        HeartbeatSimulation sim = simulation(Map.of("t0", 2));
        HeartbeatSimulation.Driver c1 = sim.start("C1", null, Set.of("t0"));
        sim.runUntil(3000);
        assertEquals(Set.of("t0-0", "t0-1"), names(c1.consumer.assignedPartitions()));

        c1.unsubscribeLater();
        sim.runUntil(4000);
        assertEquals(List.of("onPartitionsAssigned [t0-0, t0-1]", "onPartitionsRevoked [t0-0, t0-1]"), c1.callbacks);

        c1.closeLater(CloseOptions.GroupMembershipOperation.DEFAULT);
        sim.runUntil(5000);
        assertTrue(c1.consumer.isClosed());

        assertEquals(List.of("onPartitionsAssigned [t0-0, t0-1]", "onPartitionsRevoked [t0-0, t0-1]"), c1.callbacks);
    }

    /**
     * The join heartbeat of a member is delayed past the request timeout. The member joins with
     * a second heartbeat and gets both partitions. When the first heartbeat reaches the
     * coordinator, the member is revoking one partition for a new member; the partition is not
     * given to the new member before the first one revoked it.
     */
    @Test
    @Disabled("A join heartbeat processed after the client gave up on it releases partitions the member owns")
    public void testDelayedJoinHeartbeat() {
        HeartbeatSimulation sim = simulation(Map.of("t0", 2));
        HeartbeatSimulation.Driver c1 = sim.start("C1", null, Set.of("t0"));
        sim.delayRequest(c1, ApiKeys.CONSUMER_GROUP_HEARTBEAT,
            request -> ((ConsumerGroupHeartbeatRequest) request).data().memberEpoch() == 0, 7000);
        sim.runUntil(6000);
        assertEquals(Set.of("t0-0", "t0-1"), names(c1.consumer.assignedPartitions()));

        // C2 joins while C1 processes records, so C1 has not revoked the partition of C2 when
        // the delayed heartbeat arrives.
        c1.pause(4000);
        sim.start("C2", null, Set.of("t0"));
        sim.runUntil(12_000);
        assertTrue(sim.trace().stream().anyMatch(line -> line.contains("C2 onPartitionsAssigned [t0-0]")),
            "C2 gets t0-0 while C1 owns it");

        assertEquals(List.of(), violations("released-by-stale-join"));
    }

    /**
     * The member rejoins after unsubscribing and its join heartbeat is delayed past the request
     * timeout. The second join heartbeat gets no partition because the other member still owns
     * the one it should get. When the first join heartbeat reaches the coordinator, that
     * partition is free and the coordinator assigns it at the same member epoch. The member
     * still gets the partition.
     */
    @Test
    @Disabled("A partition assigned in the response to a heartbeat the client gave up on is not consumed")
    public void testDelayedJoinHeartbeatAfterPartitionReleased() {
        HeartbeatSimulation sim = simulation(Map.of("t0", 2));
        HeartbeatSimulation.Driver c1 = sim.start("C1", null, Set.of("t0"));
        sim.at(1500, "start C2", () -> sim.start("C2", null, Set.of("t0")));
        sim.at(4000, "C1 unsubscribes", c1::unsubscribeLater);
        sim.at(6000, "C1 subscribes", () -> c1.subscribeLater(Set.of("t0")));
        AtomicInteger joins = new AtomicInteger();
        sim.delayRequest(c1, ApiKeys.CONSUMER_GROUP_HEARTBEAT,
            request -> ((ConsumerGroupHeartbeatRequest) request).data().memberEpoch() == 0 && joins.incrementAndGet() == 2, 6000);
        sim.runUntil(40_000);
        assertTrue(sim.trace().stream().anyMatch(line -> line.contains("C1 CONSUMER_GROUP_HEARTBEAT is delayed by 6000 ms")));

        // Both members are stable and the partitions that the coordinator assigns to C1 are the
        // ones that C1 consumes.
        var member = group().members().get(c1.consumer.memberId());
        assertEquals(org.apache.kafka.coordinator.group.modern.MemberState.STABLE, member.state());
        assertEquals(MemberState.STABLE, c1.consumer.state());
        Set<String> assigned = new TreeSet<>();
        member.assignedPartitions().forEach((topicId, partitions) -> partitions.keySet().forEach(p -> assigned.add("t0-" + p)));
        assertEquals(assigned, names(c1.consumer.assignedPartitions()));
    }

    /**
     * A partition moves from C1 to C2. Afterwards, the application of C1 commits an offset of the
     * partition that it processed before the revocation, at its current member epoch. The
     * coordinator rejects the commit because C1 does not own the partition anymore.
     */
    @Test
    @Disabled("The coordinator accepts commits at the member epoch for partitions the member does not own")
    public void testCommitOfRevokedPartition() {
        HeartbeatSimulation sim = simulation(Map.of("t0", 2));
        HeartbeatSimulation.Driver c1 = sim.start("C1", null, Set.of("t0"));
        sim.runUntil(3000);
        assertEquals(Set.of("t0-0", "t0-1"), names(c1.consumer.assignedPartitions()));

        HeartbeatSimulation.Driver c2 = sim.start("C2", null, Set.of("t0"));
        sim.runUntil(6000);
        assertEquals(1, c2.consumer.assignedPartitions().size());
        TopicPartition moved = c2.consumer.assignedPartitions().iterator().next();
        assertEquals(c1.consumer.memberEpoch(), group().members().get(c1.consumer.memberId()).memberEpoch());

        c1.commitSyncLater(Map.of(moved, 1L));
        sim.runUntil(7000);
        assertTrue(sim.trace().stream().anyMatch(line -> line.contains("C1 commit epoch")));

        assertEquals(List.of(), violations("commit-not-owned"));
    }
}
