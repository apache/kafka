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
package org.apache.kafka.raft;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.DescribeQuorumResponseData;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.raft.internals.BatchAccumulator;
import org.apache.kafka.raft.internals.ReplicaKey;
import org.apache.kafka.raft.internals.VoterSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.apache.kafka.raft.LeaderState.CHECK_QUORUM_TIMEOUT_FACTOR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LeaderStateTest {
    private final int localId = 0;
    private final Uuid localDirectoryId = Uuid.randomUuid();
    private final int epoch = 5;
    private final LogContext logContext = new LogContext();
    private final BatchAccumulator<?> accumulator = Mockito.mock(BatchAccumulator.class);
    private final MockTime time = new MockTime();
    private final int fetchTimeoutMs = 2000;
    private final int checkQuorumTimeoutMs = (int) (fetchTimeoutMs * CHECK_QUORUM_TIMEOUT_FACTOR);

    private LeaderState<?> newLeaderState(
        Set<Integer> voters,
        long epochStartOffset
    ) {
        return new LeaderState<>(
            time,
            localId,
            Uuid.randomUuid(),
            epoch,
            epochStartOffset,
            toMap(voters),
            voters,
            accumulator,
            fetchTimeoutMs,
            logContext
        );
    }

    @Test
    public void testRequireNonNullAccumulator() {
        assertThrows(NullPointerException.class, () -> new LeaderState<>(
            new MockTime(),
            localId,
            Uuid.randomUuid(),
            epoch,
            0,
            Collections.emptyMap(),
            Collections.emptySet(),
            null,
            fetchTimeoutMs,
            logContext
        ));
    }

    @Test
    public void testFollowerAcknowledgement() {
        int node1 = 1;
        int node2 = 2;
        LeaderState<?> state = newLeaderState(mkSet(localId, node1, node2), 0L);
        assertEquals(mkSet(node1, node2), state.nonAcknowledgingVoters());
        state.addAcknowledgementFrom(node1);
        assertEquals(singleton(node2), state.nonAcknowledgingVoters());
        state.addAcknowledgementFrom(node2);
        assertEquals(emptySet(), state.nonAcknowledgingVoters());
    }

    @Test
    public void testNonFollowerAcknowledgement() {
        int nonVoterId = 1;
        LeaderState<?> state = newLeaderState(singleton(localId), 0L);
        assertThrows(IllegalArgumentException.class, () -> state.addAcknowledgementFrom(nonVoterId));
    }

    @Test
    public void testUpdateHighWatermarkQuorumSizeOne() {
        Set<Integer> voterSet = singleton(localId);
        LeaderState<?> state = newLeaderState(voterSet, 15L);
        assertEquals(Optional.empty(), state.highWatermark());
        assertFalse(state.updateLocalState(new LogOffsetMetadata(15L), toMap(voterSet)));
        assertEquals(emptySet(), state.nonAcknowledgingVoters());
        assertEquals(Optional.empty(), state.highWatermark());
        assertTrue(state.updateLocalState(new LogOffsetMetadata(16L), toMap(voterSet)));
        assertEquals(Optional.of(new LogOffsetMetadata(16L)), state.highWatermark());
        assertTrue(state.updateLocalState(new LogOffsetMetadata(20), toMap(voterSet)));
        assertEquals(Optional.of(new LogOffsetMetadata(20L)), state.highWatermark());
    }

    @Test
    public void testNonMonotonicLocalEndOffsetUpdate() {
        Set<Integer> voterSet = singleton(localId);
        LeaderState<?> state = newLeaderState(voterSet, 15L);
        assertEquals(Optional.empty(), state.highWatermark());
        assertTrue(state.updateLocalState(new LogOffsetMetadata(16L), toMap(voterSet)));
        assertEquals(Optional.of(new LogOffsetMetadata(16L)), state.highWatermark());
        assertThrows(IllegalStateException.class,
            () -> state.updateLocalState(new LogOffsetMetadata(15L), toMap(voterSet)));
    }

    @Test
    public void testLastCaughtUpTimeVoters() {
        int node1 = 1;
        int node2 = 2;
        int currentTime = 1000;
        int fetchTime = 0;
        int caughtUpTime = -1;
        Set<Integer> voterSet = mkSet(localId, node1, node2);
        LeaderState<?> state = newLeaderState(voterSet, 10L);
        assertEquals(Optional.empty(), state.highWatermark());
        assertFalse(state.updateLocalState(new LogOffsetMetadata(10L), toMap(voterSet)));
        assertEquals(mkSet(node1, node2), state.nonAcknowledgingVoters());
        assertEquals(Optional.empty(), state.highWatermark());

        // Node 1 falls behind
        assertFalse(state.updateLocalState(new LogOffsetMetadata(11L), toMap(voterSet)));
        assertFalse(state.updateReplicaState(node1, Uuid.randomUuid(), ++fetchTime, new LogOffsetMetadata(10L)));
        assertEquals(currentTime, describeVoterState(state, localId, currentTime).lastCaughtUpTimestamp());
        assertEquals(caughtUpTime, describeVoterState(state, node1, currentTime).lastCaughtUpTimestamp());

        // Node 1 catches up to leader
        assertTrue(state.updateReplicaState(node1, Uuid.randomUuid(), ++fetchTime, new LogOffsetMetadata(11L)));
        caughtUpTime = fetchTime;
        assertEquals(currentTime, describeVoterState(state, localId, currentTime).lastCaughtUpTimestamp());
        assertEquals(caughtUpTime, describeVoterState(state, node1, currentTime).lastCaughtUpTimestamp());

        // Node 1 falls behind
        assertFalse(state.updateLocalState(new LogOffsetMetadata(100L), toMap(voterSet)));
        assertTrue(state.updateReplicaState(node1, Uuid.randomUuid(), ++fetchTime, new LogOffsetMetadata(50L)));
        assertEquals(currentTime, describeVoterState(state, localId, currentTime).lastCaughtUpTimestamp());
        assertEquals(caughtUpTime, describeVoterState(state, node1, currentTime).lastCaughtUpTimestamp());

        // Node 1 catches up to the last fetch offset
        int prevFetchTime = fetchTime;
        assertFalse(state.updateLocalState(new LogOffsetMetadata(200L), toMap(voterSet)));
        assertTrue(state.updateReplicaState(node1, Uuid.randomUuid(), ++fetchTime, new LogOffsetMetadata(100L)));
        caughtUpTime = prevFetchTime;
        assertEquals(currentTime, describeVoterState(state, localId, currentTime).lastCaughtUpTimestamp());
        assertEquals(caughtUpTime, describeVoterState(state, node1, currentTime).lastCaughtUpTimestamp());

        // Node2 has never caught up to leader
        assertEquals(-1L, describeVoterState(state, node2, currentTime).lastCaughtUpTimestamp());
        assertFalse(state.updateLocalState(new LogOffsetMetadata(300L), toMap(voterSet)));
        assertTrue(state.updateReplicaState(node2, Uuid.randomUuid(), ++fetchTime, new LogOffsetMetadata(200L)));
        assertEquals(-1L, describeVoterState(state, node2, currentTime).lastCaughtUpTimestamp());
        assertTrue(state.updateReplicaState(node2, Uuid.randomUuid(), ++fetchTime, new LogOffsetMetadata(250L)));
        assertEquals(-1L, describeVoterState(state, node2, currentTime).lastCaughtUpTimestamp());
    }

    @Test
    public void testLastCaughtUpTimeObserver() {
        int node1 = 1;
        int currentTime = 1000;
        int fetchTime = 0;
        int caughtUpTime = -1;
        Set<Integer> voterSet = singleton(localId);
        LeaderState<?> state = newLeaderState(voterSet, 5L);
        assertEquals(Optional.empty(), state.highWatermark());
        assertEquals(emptySet(), state.nonAcknowledgingVoters());

        // Node 1 falls behind
        assertTrue(state.updateLocalState(new LogOffsetMetadata(11L), toMap(voterSet)));
        assertFalse(state.updateReplicaState(node1, Uuid.randomUuid(), ++fetchTime, new LogOffsetMetadata(10L)));
        assertEquals(currentTime, describeVoterState(state, localId, currentTime).lastCaughtUpTimestamp());
        assertEquals(caughtUpTime, describeObserverState(state, node1, currentTime).lastCaughtUpTimestamp());

        // Node 1 catches up to leader
        assertFalse(state.updateReplicaState(node1, Uuid.randomUuid(), ++fetchTime, new LogOffsetMetadata(11L)));
        caughtUpTime = fetchTime;
        assertEquals(currentTime, describeVoterState(state, localId, currentTime).lastCaughtUpTimestamp());
        assertEquals(caughtUpTime, describeObserverState(state, node1, currentTime).lastCaughtUpTimestamp());

        // Node 1 falls behind
        assertTrue(state.updateLocalState(new LogOffsetMetadata(100L), toMap(voterSet)));
        assertFalse(state.updateReplicaState(node1, Uuid.randomUuid(), ++fetchTime, new LogOffsetMetadata(50L)));
        assertEquals(currentTime, describeVoterState(state, localId, currentTime).lastCaughtUpTimestamp());
        assertEquals(caughtUpTime, describeObserverState(state, node1, currentTime).lastCaughtUpTimestamp());

        // Node 1 catches up to the last fetch offset
        int prevFetchTime = fetchTime;
        assertTrue(state.updateLocalState(new LogOffsetMetadata(200L), toMap(voterSet)));
        assertFalse(state.updateReplicaState(node1, Uuid.randomUuid(), ++fetchTime, new LogOffsetMetadata(102L)));
        caughtUpTime = prevFetchTime;
        assertEquals(currentTime, describeVoterState(state, localId, currentTime).lastCaughtUpTimestamp());
        assertEquals(caughtUpTime, describeObserverState(state, node1, currentTime).lastCaughtUpTimestamp());

        // Node 1 catches up to leader
        assertFalse(state.updateReplicaState(node1, Uuid.randomUuid(), ++fetchTime, new LogOffsetMetadata(200L)));
        caughtUpTime = fetchTime;
        assertEquals(currentTime, describeVoterState(state, localId, currentTime).lastCaughtUpTimestamp());
        assertEquals(caughtUpTime, describeObserverState(state, node1, currentTime).lastCaughtUpTimestamp());
    }

    @Test
    public void testIdempotentEndOffsetUpdate() {
        Set<Integer> voterSet = singleton(localId);
        LeaderState<?> state = newLeaderState(voterSet, 15L);
        assertEquals(Optional.empty(), state.highWatermark());
        assertTrue(state.updateLocalState(new LogOffsetMetadata(16L), toMap(voterSet)));
        assertFalse(state.updateLocalState(new LogOffsetMetadata(16L), toMap(voterSet)));
        assertEquals(Optional.of(new LogOffsetMetadata(16L)), state.highWatermark());
    }

    @Test
    public void testUpdateHighWatermarkMetadata() {
        Set<Integer> voterSet = singleton(localId);
        LeaderState<?> state = newLeaderState(voterSet, 15L);
        assertEquals(Optional.empty(), state.highWatermark());

        LogOffsetMetadata initialHw = new LogOffsetMetadata(16L, Optional.of(new MockOffsetMetadata("bar")));
        assertTrue(state.updateLocalState(initialHw, toMap(voterSet)));
        assertEquals(Optional.of(initialHw), state.highWatermark());

        LogOffsetMetadata updateHw = new LogOffsetMetadata(16L, Optional.of(new MockOffsetMetadata("baz")));
        assertTrue(state.updateLocalState(updateHw, toMap(voterSet)));
        assertEquals(Optional.of(updateHw), state.highWatermark());
    }

    @Test
    public void testUpdateHighWatermarkQuorumSizeTwo() {
        int otherNodeId = 1;
        Set<Integer> voterSet = mkSet(localId, otherNodeId);
        LeaderState<?> state = newLeaderState(voterSet, 10L);
        assertFalse(state.updateLocalState(new LogOffsetMetadata(13L), toMap(voterSet)));
        assertEquals(singleton(otherNodeId), state.nonAcknowledgingVoters());
        assertEquals(Optional.empty(), state.highWatermark());
        assertFalse(state.updateReplicaState(otherNodeId, Uuid.randomUuid(), 0, new LogOffsetMetadata(10L)));
        assertEquals(emptySet(), state.nonAcknowledgingVoters());
        assertEquals(Optional.empty(), state.highWatermark());
        assertTrue(state.updateReplicaState(otherNodeId, Uuid.randomUuid(), 0, new LogOffsetMetadata(11L)));
        assertEquals(Optional.of(new LogOffsetMetadata(11L)), state.highWatermark());
        assertTrue(state.updateReplicaState(otherNodeId, Uuid.randomUuid(), 0, new LogOffsetMetadata(13L)));
        assertEquals(Optional.of(new LogOffsetMetadata(13L)), state.highWatermark());
    }

    @Test
    public void testUpdateHighWatermarkQuorumSizeThree() {
        int node1 = 1;
        int node2 = 2;
        Set<Integer> voterSet = mkSet(localId, node1, node2);
        LeaderState<?> state = newLeaderState(voterSet, 10L);
        assertFalse(state.updateLocalState(new LogOffsetMetadata(15L), toMap(voterSet)));
        assertEquals(mkSet(node1, node2), state.nonAcknowledgingVoters());
        assertEquals(Optional.empty(), state.highWatermark());
        assertFalse(state.updateReplicaState(node1, Uuid.randomUuid(), 0, new LogOffsetMetadata(10L)));
        assertEquals(singleton(node2), state.nonAcknowledgingVoters());
        assertEquals(Optional.empty(), state.highWatermark());
        assertFalse(state.updateReplicaState(node2, Uuid.randomUuid(), 0, new LogOffsetMetadata(10L)));
        assertEquals(emptySet(), state.nonAcknowledgingVoters());
        assertEquals(Optional.empty(), state.highWatermark());
        assertTrue(state.updateReplicaState(node2, Uuid.randomUuid(), 0, new LogOffsetMetadata(15L)));
        assertEquals(Optional.of(new LogOffsetMetadata(15L)), state.highWatermark());
        assertFalse(state.updateLocalState(new LogOffsetMetadata(20L), toMap(voterSet)));
        assertEquals(Optional.of(new LogOffsetMetadata(15L)), state.highWatermark());
        assertTrue(state.updateReplicaState(node1, Uuid.randomUuid(), 0, new LogOffsetMetadata(20L)));
        assertEquals(Optional.of(new LogOffsetMetadata(20L)), state.highWatermark());
        assertFalse(state.updateReplicaState(node2, Uuid.randomUuid(), 0, new LogOffsetMetadata(20L)));
        assertEquals(Optional.of(new LogOffsetMetadata(20L)), state.highWatermark());
    }

    @Test
    public void testHighWatermarkDoesIncreaseFromNewVoter() {
        int node1 = 1;
        int node2 = 2;
        Set<Integer> originalVoterSet = mkSet(localId, node1);
        LeaderState<?> state = newLeaderState(originalVoterSet, 5L);
        assertFalse(state.updateLocalState(new LogOffsetMetadata(15L), toMap(originalVoterSet)));
        assertTrue(state.updateReplicaState(node1, Uuid.randomUuid(), 0, new LogOffsetMetadata(10L)));
        assertEquals(Optional.of(new LogOffsetMetadata(10L)), state.highWatermark());

        // updating replica state of node2 before it joins voterSet should not increase HW to 15L
        assertFalse(state.updateReplicaState(node2, Uuid.randomUuid(), 0, new LogOffsetMetadata(15L)));
        assertEquals(Optional.of(new LogOffsetMetadata(10L)), state.highWatermark());

        // adding node2 to voterSet will cause HW to increase to 15L
        Set<Integer> voterSetWithNode2 = mkSet(localId, node1, node2);
        assertTrue(state.updateLocalState(new LogOffsetMetadata(15L), toMap(voterSetWithNode2)));
        assertEquals(Optional.of(new LogOffsetMetadata(15L)), state.highWatermark());

        // HW will not update to 16L until a majority reaches it
        assertFalse(state.updateLocalState(new LogOffsetMetadata(16L), toMap(voterSetWithNode2)));
        assertEquals(Optional.of(new LogOffsetMetadata(15L)), state.highWatermark());
        assertTrue(state.updateReplicaState(node2, Uuid.randomUuid(), 0, new LogOffsetMetadata(16L)));
        assertEquals(Optional.of(new LogOffsetMetadata(16L)), state.highWatermark());
    }

    @Test
    public void testHighWatermarkDoesNotDecreaseFromNewVoter() {
        int node1 = 1;
        int node2 = 2;
        int node3 = 3;
        // start with three voters with HW at 15L
        Set<Integer> originalVoterSet = mkSet(localId, node1, node2);
        LeaderState<?> state = newLeaderState(originalVoterSet, 5L);
        assertFalse(state.updateLocalState(new LogOffsetMetadata(15L), toMap(originalVoterSet)));
        assertTrue(state.updateReplicaState(node1, Uuid.randomUuid(), 0, new LogOffsetMetadata(15L)));
        assertEquals(Optional.of(new LogOffsetMetadata(15L)), state.highWatermark());
        assertFalse(state.updateReplicaState(node2, Uuid.randomUuid(), 0, new LogOffsetMetadata(10L)));

        // updating replica state of node3 before it joins voterSet
        assertFalse(state.updateReplicaState(node3, Uuid.randomUuid(), 0, new LogOffsetMetadata(10L)));

        // adding node3 to voterSet should not cause HW to decrease even if majority is < HW
        Set<Integer> voterSetWithNode3 = mkSet(localId, node1, node2, node3);
        assertFalse(state.updateLocalState(new LogOffsetMetadata(16L), toMap(voterSetWithNode3)));
        assertEquals(Optional.of(new LogOffsetMetadata(15L)), state.highWatermark());

        // HW will not decrease if calculated HW is anything lower than the last HW
        assertFalse(state.updateReplicaState(node2, Uuid.randomUuid(), 0, new LogOffsetMetadata(13L)));
        assertEquals(Optional.of(new LogOffsetMetadata(15L)), state.highWatermark());
        assertFalse(state.updateReplicaState(node3, Uuid.randomUuid(), 0, new LogOffsetMetadata(13L)));
        assertEquals(Optional.of(new LogOffsetMetadata(15L)), state.highWatermark());
        assertFalse(state.updateReplicaState(node1, Uuid.randomUuid(), 0, new LogOffsetMetadata(16L)));
        assertEquals(Optional.of(new LogOffsetMetadata(15L)), state.highWatermark());

        // HW will update to 16L once a majority of the voterSet is at least 16L
        assertTrue(state.updateReplicaState(node3, Uuid.randomUuid(), 0, new LogOffsetMetadata(16L)));
        assertEquals(Optional.of(new LogOffsetMetadata(16L)), state.highWatermark());
    }

    @Test
    public void testUpdateHighWatermarkRemovingFollowerFromVoterStates() {
        int node1 = 1;
        int node2 = 2;
        Set<Integer> originalVoterSet = mkSet(localId, node1, node2);
        LeaderState<?> state = newLeaderState(originalVoterSet, 10L);
        assertFalse(state.updateLocalState(new LogOffsetMetadata(15L), toMap(originalVoterSet)));
        assertTrue(state.updateReplicaState(node1, Uuid.randomUuid(), 0, new LogOffsetMetadata(15L)));
        assertFalse(state.updateReplicaState(node2, Uuid.randomUuid(), 0, new LogOffsetMetadata(10L)));
        assertEquals(Optional.of(new LogOffsetMetadata(15L)), state.highWatermark());

        // removing node1 should not decrement HW to 10L
        Set<Integer> voterSetWithoutNode1 = mkSet(localId, node2);
        assertFalse(state.updateLocalState(new LogOffsetMetadata(17L), toMap(voterSetWithoutNode1)));
        assertEquals(Optional.of(new LogOffsetMetadata(15L)), state.highWatermark());

        // HW cannot change until after node2 catches up to last HW
        assertFalse(state.updateReplicaState(node2, Uuid.randomUuid(), 0, new LogOffsetMetadata(14L)));
        assertEquals(Optional.of(new LogOffsetMetadata(15L)), state.highWatermark());
        assertFalse(state.updateLocalState(new LogOffsetMetadata(18L), toMap(voterSetWithoutNode1)));
        assertEquals(Optional.of(new LogOffsetMetadata(15L)), state.highWatermark());
        assertFalse(state.updateReplicaState(node1, Uuid.randomUuid(), 0, new LogOffsetMetadata(18L)));
        assertEquals(Optional.of(new LogOffsetMetadata(15L)), state.highWatermark());
        assertFalse(state.updateReplicaState(node2, Uuid.randomUuid(), 0, new LogOffsetMetadata(15L)));
        assertEquals(Optional.of(new LogOffsetMetadata(15L)), state.highWatermark());

        // HW should update to 16L
        assertTrue(state.updateReplicaState(node2, Uuid.randomUuid(), 0, new LogOffsetMetadata(16L)));
        assertEquals(Optional.of(new LogOffsetMetadata(16L)), state.highWatermark());
    }

    @Test
    public void testUpdateHighWatermarkQuorumRemovingLeaderFromVoterStates() {
        int node1 = 1;
        int node2 = 2;
        Set<Integer> originalVoterSet = mkSet(localId, node1, node2);
        LeaderState<?> state = newLeaderState(originalVoterSet, 10L);
        assertFalse(state.updateLocalState(new LogOffsetMetadata(15L), toMap(originalVoterSet)));
        assertTrue(state.updateReplicaState(node1, Uuid.randomUuid(), 0, new LogOffsetMetadata(15L)));
        assertFalse(state.updateReplicaState(node2, Uuid.randomUuid(), 0, new LogOffsetMetadata(10L)));
        assertEquals(Optional.of(new LogOffsetMetadata(15L)), state.highWatermark());

        // removing leader should not decrement HW to 10L
        Set<Integer> voterSetWithoutLeader = mkSet(node1, node2);
        assertFalse(state.updateLocalState(new LogOffsetMetadata(17L), toMap(voterSetWithoutLeader)));
        assertEquals(Optional.of(new LogOffsetMetadata(15L)), state.highWatermark());

        // HW cannot change until node2 catches up to last HW
        assertFalse(state.updateReplicaState(node1, Uuid.randomUuid(), 0, new LogOffsetMetadata(16L)));
        assertEquals(Optional.of(new LogOffsetMetadata(15L)), state.highWatermark());
        assertFalse(state.updateLocalState(new LogOffsetMetadata(18L), toMap(voterSetWithoutLeader)));
        assertEquals(Optional.of(new LogOffsetMetadata(15L)), state.highWatermark());
        assertFalse(state.updateReplicaState(node2, Uuid.randomUuid(), 0, new LogOffsetMetadata(14L)));
        assertEquals(Optional.of(new LogOffsetMetadata(15L)), state.highWatermark());
        assertFalse(state.updateReplicaState(node2, Uuid.randomUuid(), 0, new LogOffsetMetadata(15L)));
        assertEquals(Optional.of(new LogOffsetMetadata(15L)), state.highWatermark());

        // HW will not update to 16L until majority of remaining voterSet (node1, node2) are at least 16L
        assertTrue(state.updateReplicaState(node2, Uuid.randomUuid(), 0, new LogOffsetMetadata(16L)));
        assertEquals(Optional.of(new LogOffsetMetadata(16L)), state.highWatermark());
    }

    @Test
    public void testNonMonotonicHighWatermarkUpdate() {
        MockTime time = new MockTime();
        int node1 = 1;
        Set<Integer> voterSet = mkSet(localId, node1);
        LeaderState<?> state = newLeaderState(voterSet, 0L);
        state.updateLocalState(new LogOffsetMetadata(10L), toMap(voterSet));
        state.updateReplicaState(node1, Uuid.randomUuid(), time.milliseconds(), new LogOffsetMetadata(10L));
        assertEquals(Optional.of(new LogOffsetMetadata(10L)), state.highWatermark());

        // Follower crashes and disk is lost. It fetches an earlier offset to rebuild state.
        // The leader will report an error in the logs, but will not let the high watermark rewind
        assertFalse(state.updateReplicaState(node1, Uuid.randomUuid(), time.milliseconds(), new LogOffsetMetadata(5L)));
        assertEquals(5L, describeVoterState(state, node1, time.milliseconds()).logEndOffset());
        assertEquals(Optional.of(new LogOffsetMetadata(10L)), state.highWatermark());
    }

    @Test
    public void testGetNonLeaderFollowersByFetchOffsetDescending() {
        int node1 = 1;
        int node2 = 2;
        long leaderStartOffset = 10L;
        long leaderEndOffset = 15L;

        LeaderState<?> state = setUpLeaderAndFollowers(node1, node2, leaderStartOffset, leaderEndOffset);

        // Leader should not be included; the follower with larger offset should be prioritized.
        assertEquals(Arrays.asList(node2, node1), state.nonLeaderVotersByDescendingFetchOffset());
    }

    @Test
    public void testDescribeQuorumWithSingleVoter() {
        MockTime time = new MockTime();
        long leaderStartOffset = 10L;
        long leaderEndOffset = 15L;

        Set<Integer> voterSet = singleton(localId);
        LeaderState<?> state = newLeaderState(voterSet, leaderStartOffset);

        // Until we have updated local state, high watermark should be uninitialized
        assertEquals(Optional.empty(), state.highWatermark());
        DescribeQuorumResponseData.PartitionData partitionData = state.describeQuorum(time.milliseconds());
        assertEquals(-1, partitionData.highWatermark());
        assertEquals(localId, partitionData.leaderId());
        assertEquals(epoch, partitionData.leaderEpoch());
        assertEquals(Collections.emptyList(), partitionData.observers());
        assertEquals(1, partitionData.currentVoters().size());
        assertEquals(new DescribeQuorumResponseData.ReplicaState()
                .setReplicaId(localId)
                .setReplicaDirectoryId(localDirectoryId)
                .setLogEndOffset(-1)
                .setLastFetchTimestamp(time.milliseconds())
                .setLastCaughtUpTimestamp(time.milliseconds()),
            partitionData.currentVoters().get(0));


        // Now update the high watermark and verify the describe output
        assertTrue(state.updateLocalState(new LogOffsetMetadata(leaderEndOffset), toMap(voterSet)));
        assertEquals(Optional.of(new LogOffsetMetadata(leaderEndOffset)), state.highWatermark());

        time.sleep(500);

        partitionData = state.describeQuorum(time.milliseconds());
        assertEquals(leaderEndOffset, partitionData.highWatermark());
        assertEquals(localId, partitionData.leaderId());
        assertEquals(epoch, partitionData.leaderEpoch());
        assertEquals(Collections.emptyList(), partitionData.observers());
        assertEquals(1, partitionData.currentVoters().size());
        assertEquals(new DescribeQuorumResponseData.ReplicaState()
                .setReplicaId(localId)
                .setReplicaDirectoryId(localDirectoryId)
                .setLogEndOffset(leaderEndOffset)
                .setLastFetchTimestamp(time.milliseconds())
                .setLastCaughtUpTimestamp(time.milliseconds()),
            partitionData.currentVoters().get(0));
    }

    @Test
    public void testDescribeQuorumWithMultipleVoters() {
        MockTime time = new MockTime();
        int activeFollowerId = 1;
        Uuid activeFollowerDirectoryId = Uuid.randomUuid();
        int inactiveFollowerId = 2;
        Uuid inactiveFollowerDirectoryId = Uuid.randomUuid();
        long leaderStartOffset = 10L;
        long leaderEndOffset = 15L;

        Map<Integer, VoterSet.VoterNode> voters = new HashMap<>();
        voters.put(localId, voterNode(localId, localDirectoryId));
        voters.put(activeFollowerId, voterNode(activeFollowerId, activeFollowerDirectoryId));
        voters.put(inactiveFollowerId, voterNode(inactiveFollowerId, inactiveFollowerDirectoryId));

        LeaderState<?> state = new LeaderState<>(
            time,
            localId,
            Uuid.randomUuid(),
            epoch,
            leaderStartOffset,
            voters,
            voters.keySet(),
            accumulator,
            fetchTimeoutMs,
            logContext
        );
        assertFalse(state.updateLocalState(new LogOffsetMetadata(leaderEndOffset), voters));
        assertEquals(Optional.empty(), state.highWatermark());

        long activeFollowerFetchTimeMs = time.milliseconds();
        assertTrue(state.updateReplicaState(activeFollowerId, activeFollowerDirectoryId, activeFollowerFetchTimeMs, new LogOffsetMetadata(leaderEndOffset)));
        assertEquals(Optional.of(new LogOffsetMetadata(leaderEndOffset)), state.highWatermark());

        time.sleep(500);

        DescribeQuorumResponseData.PartitionData partitionData = state.describeQuorum(time.milliseconds());
        assertEquals(leaderEndOffset, partitionData.highWatermark());
        assertEquals(localId, partitionData.leaderId());
        assertEquals(epoch, partitionData.leaderEpoch());
        assertEquals(Collections.emptyList(), partitionData.observers());

        List<DescribeQuorumResponseData.ReplicaState> voterStates = partitionData.currentVoters();
        assertEquals(3, voterStates.size());

        DescribeQuorumResponseData.ReplicaState leaderState =
            findReplicaOrFail(localId, partitionData.currentVoters());
        assertEquals(new DescribeQuorumResponseData.ReplicaState()
                .setReplicaId(localId)
                .setReplicaDirectoryId(localDirectoryId)
                .setLogEndOffset(leaderEndOffset)
                .setLastFetchTimestamp(time.milliseconds())
                .setLastCaughtUpTimestamp(time.milliseconds()),
            leaderState);

        DescribeQuorumResponseData.ReplicaState activeFollowerState =
            findReplicaOrFail(activeFollowerId, partitionData.currentVoters());
        assertEquals(new DescribeQuorumResponseData.ReplicaState()
                .setReplicaId(activeFollowerId)
                .setReplicaDirectoryId(activeFollowerDirectoryId)
                .setLogEndOffset(leaderEndOffset)
                .setLastFetchTimestamp(activeFollowerFetchTimeMs)
                .setLastCaughtUpTimestamp(activeFollowerFetchTimeMs),
            activeFollowerState);

        DescribeQuorumResponseData.ReplicaState inactiveFollowerState =
            findReplicaOrFail(inactiveFollowerId, partitionData.currentVoters());
        assertEquals(new DescribeQuorumResponseData.ReplicaState()
                .setReplicaId(inactiveFollowerId)
                .setReplicaDirectoryId(inactiveFollowerDirectoryId)
                .setLogEndOffset(-1)
                .setLastFetchTimestamp(-1)
                .setLastCaughtUpTimestamp(-1),
            inactiveFollowerState);
    }

    private LeaderState<?> setUpLeaderAndFollowers(int follower1,
                                                   int follower2,
                                                   long leaderStartOffset,
                                                   long leaderEndOffset) {
        Set<Integer> voterSet = mkSet(localId, follower1, follower2);
        LeaderState<?> state = newLeaderState(voterSet, leaderStartOffset);
        state.updateLocalState(new LogOffsetMetadata(leaderEndOffset), toMap(voterSet));
        assertEquals(Optional.empty(), state.highWatermark());
        state.updateReplicaState(follower1, Uuid.randomUuid(), 0, new LogOffsetMetadata(leaderStartOffset));
        state.updateReplicaState(follower2, Uuid.randomUuid(), 0, new LogOffsetMetadata(leaderEndOffset));
        return state;
    }

    @Test
    public void testDescribeQuorumWithObservers() {
        MockTime time = new MockTime();
        int observerId = 10;
        Uuid observerDirectoryId = Uuid.randomUuid();
        long epochStartOffset = 10L;

        Set<Integer> voterSet = singleton(localId);
        LeaderState<?> state = newLeaderState(voterSet, epochStartOffset);
        assertTrue(state.updateLocalState(new LogOffsetMetadata(epochStartOffset + 1), toMap(voterSet)));
        assertEquals(Optional.of(new LogOffsetMetadata(epochStartOffset + 1)), state.highWatermark());

        time.sleep(500);
        long observerFetchTimeMs = time.milliseconds();
        assertFalse(state.updateReplicaState(observerId, observerDirectoryId, observerFetchTimeMs, new LogOffsetMetadata(epochStartOffset + 1)));

        time.sleep(500);
        DescribeQuorumResponseData.PartitionData partitionData = state.describeQuorum(time.milliseconds());
        assertEquals(epochStartOffset + 1, partitionData.highWatermark());
        assertEquals(localId, partitionData.leaderId());
        assertEquals(epoch, partitionData.leaderEpoch());

        assertEquals(1, partitionData.currentVoters().size());
        assertEquals(localId, partitionData.currentVoters().get(0).replicaId());

        List<DescribeQuorumResponseData.ReplicaState> observerStates = partitionData.observers();
        assertEquals(1, observerStates.size());

        DescribeQuorumResponseData.ReplicaState observerState = observerStates.get(0);
        assertEquals(new DescribeQuorumResponseData.ReplicaState()
                .setReplicaId(observerId)
                .setReplicaDirectoryId(observerDirectoryId)
                .setLogEndOffset(epochStartOffset + 1)
                .setLastFetchTimestamp(observerFetchTimeMs)
                .setLastCaughtUpTimestamp(observerFetchTimeMs),
            observerState);
    }

    @Test
    public void testDescribeQuorumWithVotersAndObservers() {
        MockTime time = new MockTime();
        int leader = localId;
        int node1 = 1;
        int node2 = 2;
        long epochStartOffset = 10L;

        Set<Integer> voterSet = mkSet(leader, node1, node2);
        LeaderState<?> state = newLeaderState(voterSet, epochStartOffset);
        assertFalse(state.updateLocalState(new LogOffsetMetadata(epochStartOffset + 1), toMap(voterSet)));
        assertTrue(state.updateReplicaState(node2, Uuid.randomUuid(), 0, new LogOffsetMetadata(epochStartOffset + 1)));
        assertEquals(Optional.of(new LogOffsetMetadata(epochStartOffset + 1)), state.highWatermark());

        // node1 becomes an observer
        long fetchTimeMs = time.milliseconds();
        assertFalse(state.updateReplicaState(node1, Uuid.randomUuid(), fetchTimeMs, new LogOffsetMetadata(epochStartOffset + 1)));
        Set<Integer> voterSetWithoutNode1 = mkSet(leader, node2);
        state.updateLocalState(new LogOffsetMetadata(epochStartOffset + 5), toMap(voterSetWithoutNode1));


        time.sleep(500);
        DescribeQuorumResponseData.PartitionData partitionData = state.describeQuorum(time.milliseconds());
        assertEquals(epochStartOffset + 1, partitionData.highWatermark());
        assertEquals(localId, partitionData.leaderId());
        assertEquals(epoch, partitionData.leaderEpoch());
        DescribeQuorumResponseData.ReplicaState observer = partitionData.observers().get(0);
        assertEquals(node1, observer.replicaId());
        assertEquals(epochStartOffset + 1, observer.logEndOffset());
        assertEquals(2, partitionData.currentVoters().size());

        // node1 catches up with leader, HW should not change
        time.sleep(500);
        fetchTimeMs = time.milliseconds();
        assertFalse(state.updateReplicaState(node1, Uuid.randomUuid(), fetchTimeMs, new LogOffsetMetadata(epochStartOffset + 5)));
        assertEquals(Optional.of(new LogOffsetMetadata(epochStartOffset + 1)), state.highWatermark());

        // node1 becomes a voter again, HW should change
        assertTrue(state.updateLocalState(new LogOffsetMetadata(epochStartOffset + 10), toMap(voterSet)));

        time.sleep(500);
        partitionData = state.describeQuorum(time.milliseconds());
        assertEquals(epochStartOffset + 5, partitionData.highWatermark());
        assertEquals(localId, partitionData.leaderId());
        assertEquals(epoch, partitionData.leaderEpoch());
        assertEquals(Collections.emptyList(), partitionData.observers());
        assertEquals(3, partitionData.currentVoters().size());
        DescribeQuorumResponseData.ReplicaState node1State = partitionData.currentVoters().stream()
            .filter(replicaState -> replicaState.replicaId() == node1)
            .findFirst().get();
        assertEquals(epochStartOffset + 5, node1State.logEndOffset());
        assertEquals(fetchTimeMs, node1State.lastFetchTimestamp());
    }

    @Test
    public void testClearInactiveObserversIgnoresLeader() {
        MockTime time = new MockTime();
        int followerId = 1;
        int observerId = 10;
        long epochStartOffset = 10L;

        Set<Integer> voterSet = mkSet(localId, followerId);
        LeaderState<?> state = newLeaderState(voterSet, epochStartOffset);
        assertFalse(state.updateLocalState(new LogOffsetMetadata(epochStartOffset + 1), toMap(voterSet)));
        assertTrue(state.updateReplicaState(followerId, Uuid.randomUuid(), time.milliseconds(), new LogOffsetMetadata(epochStartOffset + 1)));

        // observer is returned since its lastFetchTimestamp is within OBSERVER_SESSION_TIMEOUT_MS
        time.sleep(500);
        long observerFetchTimeMs = time.milliseconds();
        assertFalse(state.updateReplicaState(observerId, Uuid.randomUuid(), observerFetchTimeMs, new LogOffsetMetadata(epochStartOffset + 1)));

        time.sleep(500);
        DescribeQuorumResponseData.PartitionData partitionData = state.describeQuorum(time.milliseconds());
        assertEquals(epochStartOffset + 1, partitionData.highWatermark());
        assertEquals(localId, partitionData.leaderId());
        assertEquals(2, partitionData.currentVoters().size());
        assertEquals(1, partitionData.observers().size());
        assertEquals(observerId, partitionData.observers().get(0).replicaId());

        // observer is not returned once its lastFetchTimestamp surpasses OBSERVER_SESSION_TIMEOUT_MS
        time.sleep(LeaderState.OBSERVER_SESSION_TIMEOUT_MS);
        partitionData = state.describeQuorum(time.milliseconds());
        assertEquals(epochStartOffset + 1, partitionData.highWatermark());
        assertEquals(localId, partitionData.leaderId());
        assertEquals(2, partitionData.currentVoters().size());
        assertEquals(0, partitionData.observers().size());

        // leader becomes observer
        Set<Integer> voterSetWithoutLeader = mkSet(followerId);
        assertFalse(state.updateLocalState(new LogOffsetMetadata(epochStartOffset + 10), toMap(voterSetWithoutLeader)));

        // leader should be returned in describe quorum output
        time.sleep(LeaderState.OBSERVER_SESSION_TIMEOUT_MS);
        long describeQuorumCalledTime = time.milliseconds();
        partitionData = state.describeQuorum(describeQuorumCalledTime);
        assertEquals(epochStartOffset + 1, partitionData.highWatermark());
        assertEquals(localId, partitionData.leaderId());
        assertEquals(1, partitionData.currentVoters().size());
        assertEquals(1, partitionData.observers().size());
        DescribeQuorumResponseData.ReplicaState observer = partitionData.observers().get(0);
        assertEquals(localId, observer.replicaId());
        assertEquals(describeQuorumCalledTime, observer.lastFetchTimestamp());
    }

    @Test
    public void testCheckQuorum() {
        int node1 = 1;
        int node2 = 2;
        int node3 = 3;
        int node4 = 4;
        int observer5 = 5;
        LeaderState<?> state = newLeaderState(mkSet(localId, node1, node2, node3, node4), 0L);
        assertEquals(checkQuorumTimeoutMs, state.timeUntilCheckQuorumExpires(time.milliseconds()));
        int resignLeadershipTimeout = checkQuorumTimeoutMs;

        // checkQuorum timeout not exceeded, should not expire the timer
        time.sleep(resignLeadershipTimeout / 2);
        assertTrue(state.timeUntilCheckQuorumExpires(time.milliseconds()) > 0);

        // received fetch requests from 2 voter nodes, the timer should be reset
        state.updateCheckQuorumForFollowingVoter(node1, time.milliseconds());
        state.updateCheckQuorumForFollowingVoter(node2, time.milliseconds());
        assertEquals(checkQuorumTimeoutMs, state.timeUntilCheckQuorumExpires(time.milliseconds()));

        // Since the timer was reset, it won't expire this time.
        time.sleep(resignLeadershipTimeout / 2);
        long remainingMs = state.timeUntilCheckQuorumExpires(time.milliseconds());
        assertTrue(remainingMs > 0);

        // received fetch requests from 1 voter and 1 observer nodes, the timer should not be reset.
        state.updateCheckQuorumForFollowingVoter(node3, time.milliseconds());
        state.updateCheckQuorumForFollowingVoter(observer5, time.milliseconds());
        assertEquals(remainingMs, state.timeUntilCheckQuorumExpires(time.milliseconds()));

        // This time, the checkQuorum timer will be expired
        time.sleep(resignLeadershipTimeout / 2);
        assertEquals(0, state.timeUntilCheckQuorumExpires(time.milliseconds()));
    }

    @Test
    public void testCheckQuorumWithOneVoter() {
        int observer = 1;
        // Only 1 voter quorum
        LeaderState<?> state = newLeaderState(mkSet(localId), 0L);
        assertEquals(Long.MAX_VALUE, state.timeUntilCheckQuorumExpires(time.milliseconds()));

        // When checkQuorum timeout not exceeded and got no fetch request from voter, it should not expire the timer
        time.sleep(checkQuorumTimeoutMs);
        assertEquals(Long.MAX_VALUE, state.timeUntilCheckQuorumExpires(time.milliseconds()));

        // received fetch requests from 1 observer node, the timer still return Long.MAX_VALUE.
        state.updateCheckQuorumForFollowingVoter(observer, time.milliseconds());
        assertEquals(Long.MAX_VALUE, state.timeUntilCheckQuorumExpires(time.milliseconds()));
    }

    @Test
    public void testNoOpForNegativeRemoteNodeId() {
        MockTime time = new MockTime();
        int replicaId = -1;
        long epochStartOffset = 10L;

        LeaderState<?> state = newLeaderState(mkSet(localId), epochStartOffset);
        assertFalse(state.updateReplicaState(replicaId, Uuid.randomUuid(), 0, new LogOffsetMetadata(epochStartOffset)));

        DescribeQuorumResponseData.PartitionData partitionData = state.describeQuorum(time.milliseconds());
        List<DescribeQuorumResponseData.ReplicaState> observerStates = partitionData.observers();
        assertEquals(Collections.emptyList(), observerStates);
    }

    @Test
    public void testObserverStateExpiration() {
        MockTime time = new MockTime();
        int observerId = 10;
        long epochStartOffset = 10L;
        LeaderState<?> state = newLeaderState(mkSet(localId), epochStartOffset);

        state.updateReplicaState(observerId, Uuid.randomUuid(), time.milliseconds(), new LogOffsetMetadata(epochStartOffset));
        DescribeQuorumResponseData.PartitionData partitionData = state.describeQuorum(time.milliseconds());
        List<DescribeQuorumResponseData.ReplicaState> observerStates = partitionData.observers();
        assertEquals(1, observerStates.size());

        DescribeQuorumResponseData.ReplicaState observerState = observerStates.get(0);
        assertEquals(observerId, observerState.replicaId());

        time.sleep(LeaderState.OBSERVER_SESSION_TIMEOUT_MS);
        partitionData = state.describeQuorum(time.milliseconds());
        assertEquals(Collections.emptyList(), partitionData.observers());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testGrantVote(boolean isLogUpToDate) {
        LeaderState<?> state = newLeaderState(mkSet(1, 2, 3), 1);

        assertFalse(state.canGrantVote(ReplicaKey.of(1, Optional.empty()), isLogUpToDate));
        assertFalse(state.canGrantVote(ReplicaKey.of(2, Optional.empty()), isLogUpToDate));
        assertFalse(state.canGrantVote(ReplicaKey.of(3, Optional.empty()), isLogUpToDate));
    }

    private static class MockOffsetMetadata implements OffsetMetadata {
        private final String value;

        private MockOffsetMetadata(String value) {
            this.value = value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MockOffsetMetadata that = (MockOffsetMetadata) o;
            return Objects.equals(value, that.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(value);
        }
    }

    private DescribeQuorumResponseData.ReplicaState describeVoterState(
        LeaderState<?> state,
        int voterId,
        long currentTimeMs
    ) {
        DescribeQuorumResponseData.PartitionData partitionData = state.describeQuorum(currentTimeMs);
        return findReplicaOrFail(voterId, partitionData.currentVoters());
    }

    private DescribeQuorumResponseData.ReplicaState describeObserverState(
        LeaderState<?> state,
        int observerId,
        long currentTimeMs
    ) {
        DescribeQuorumResponseData.PartitionData partitionData = state.describeQuorum(currentTimeMs);
        return findReplicaOrFail(observerId, partitionData.observers());
    }

    private DescribeQuorumResponseData.ReplicaState findReplicaOrFail(
        int replicaId,
        List<DescribeQuorumResponseData.ReplicaState> replicas
    ) {
        return replicas.stream()
            .filter(observer -> observer.replicaId() == replicaId)
            .findFirst()
            .orElseThrow(() -> new AssertionError(
                "Failed to find expected replica state for replica " + replicaId
            ));
    }

    private Map<Integer, VoterSet.VoterNode> toMap(Set<Integer> data) {
        return data.stream().collect(Collectors.toMap(Function.identity(), id -> voterNode(id, id == localId ? localDirectoryId : Uuid.randomUuid())));
    }

    private VoterSet.VoterNode voterNode(int id, Uuid directoryId) {
        return new VoterSet.VoterNode(ReplicaKey.of(id, Optional.of(directoryId)), null, null);
    }
}
