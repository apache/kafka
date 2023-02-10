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

import org.apache.kafka.common.message.DescribeQuorumResponseData;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.raft.internals.BatchAccumulator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LeaderStateTest {
    private final int localId = 0;
    private final int epoch = 5;
    private final LogContext logContext = new LogContext();

    private final BatchAccumulator<?> accumulator = Mockito.mock(BatchAccumulator.class);

    private LeaderState<?> newLeaderState(
        Set<Integer> voters,
        long epochStartOffset
    ) {
        return new LeaderState<>(
            localId,
            epoch,
            epochStartOffset,
            voters,
            voters,
            accumulator,
            logContext
        );
    }

    @Test
    public void testRequireNonNullAccumulator() {
        assertThrows(NullPointerException.class, () -> new LeaderState<>(
            localId,
            epoch,
            0,
            Collections.emptySet(),
            Collections.emptySet(),
            null,
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
        LeaderState<?> state = newLeaderState(singleton(localId), 15L);
        assertEquals(Optional.empty(), state.highWatermark());
        assertFalse(state.updateLocalState(new LogOffsetMetadata(15L)));
        assertEquals(emptySet(), state.nonAcknowledgingVoters());
        assertEquals(Optional.empty(), state.highWatermark());
        assertTrue(state.updateLocalState(new LogOffsetMetadata(16L)));
        assertEquals(Optional.of(new LogOffsetMetadata(16L)), state.highWatermark());
        assertTrue(state.updateLocalState(new LogOffsetMetadata(20)));
        assertEquals(Optional.of(new LogOffsetMetadata(20L)), state.highWatermark());
    }

    @Test
    public void testNonMonotonicLocalEndOffsetUpdate() {
        LeaderState<?> state = newLeaderState(singleton(localId), 15L);
        assertEquals(Optional.empty(), state.highWatermark());
        assertTrue(state.updateLocalState(new LogOffsetMetadata(16L)));
        assertEquals(Optional.of(new LogOffsetMetadata(16L)), state.highWatermark());
        assertThrows(IllegalStateException.class,
            () -> state.updateLocalState(new LogOffsetMetadata(15L)));
    }

    @Test
    public void testLastCaughtUpTimeVoters() {
        int node1 = 1;
        int node2 = 2;
        int currentTime = 1000;
        int fetchTime = 0;
        int caughtUpTime = -1;
        LeaderState<?> state = newLeaderState(mkSet(localId, node1, node2), 10L);
        assertEquals(Optional.empty(), state.highWatermark());
        assertFalse(state.updateLocalState(new LogOffsetMetadata(10L)));
        assertEquals(mkSet(node1, node2), state.nonAcknowledgingVoters());
        assertEquals(Optional.empty(), state.highWatermark());

        // Node 1 falls behind
        assertFalse(state.updateLocalState(new LogOffsetMetadata(11L)));
        assertFalse(state.updateReplicaState(node1, ++fetchTime, new LogOffsetMetadata(10L)));
        assertEquals(currentTime, describeVoterState(state, localId, currentTime).lastCaughtUpTimestamp());
        assertEquals(caughtUpTime, describeVoterState(state, node1, currentTime).lastCaughtUpTimestamp());

        // Node 1 catches up to leader
        assertTrue(state.updateReplicaState(node1, ++fetchTime, new LogOffsetMetadata(11L)));
        caughtUpTime = fetchTime;
        assertEquals(currentTime, describeVoterState(state, localId, currentTime).lastCaughtUpTimestamp());
        assertEquals(caughtUpTime, describeVoterState(state, node1, currentTime).lastCaughtUpTimestamp());

        // Node 1 falls behind
        assertFalse(state.updateLocalState(new LogOffsetMetadata(100L)));
        assertTrue(state.updateReplicaState(node1, ++fetchTime, new LogOffsetMetadata(50L)));
        assertEquals(currentTime, describeVoterState(state, localId, currentTime).lastCaughtUpTimestamp());
        assertEquals(caughtUpTime, describeVoterState(state, node1, currentTime).lastCaughtUpTimestamp());

        // Node 1 catches up to the last fetch offset
        int prevFetchTime = fetchTime;
        assertFalse(state.updateLocalState(new LogOffsetMetadata(200L)));
        assertTrue(state.updateReplicaState(node1, ++fetchTime, new LogOffsetMetadata(100L)));
        caughtUpTime = prevFetchTime;
        assertEquals(currentTime, describeVoterState(state, localId, currentTime).lastCaughtUpTimestamp());
        assertEquals(caughtUpTime, describeVoterState(state, node1, currentTime).lastCaughtUpTimestamp());

        // Node2 has never caught up to leader
        assertEquals(-1L, describeVoterState(state, node2, currentTime).lastCaughtUpTimestamp());
        assertFalse(state.updateLocalState(new LogOffsetMetadata(300L)));
        assertTrue(state.updateReplicaState(node2, ++fetchTime, new LogOffsetMetadata(200L)));
        assertEquals(-1L, describeVoterState(state, node2, currentTime).lastCaughtUpTimestamp());
        assertTrue(state.updateReplicaState(node2, ++fetchTime, new LogOffsetMetadata(250L)));
        assertEquals(-1L, describeVoterState(state, node2, currentTime).lastCaughtUpTimestamp());
    }

    @Test
    public void testLastCaughtUpTimeObserver() {
        int node1 = 1;
        int currentTime = 1000;
        int fetchTime = 0;
        int caughtUpTime = -1;
        LeaderState<?> state = newLeaderState(singleton(localId), 5L);
        assertEquals(Optional.empty(), state.highWatermark());
        assertEquals(emptySet(), state.nonAcknowledgingVoters());

        // Node 1 falls behind
        assertTrue(state.updateLocalState(new LogOffsetMetadata(11L)));
        assertFalse(state.updateReplicaState(node1, ++fetchTime, new LogOffsetMetadata(10L)));
        assertEquals(currentTime, describeVoterState(state, localId, currentTime).lastCaughtUpTimestamp());
        assertEquals(caughtUpTime, describeObserverState(state, node1, currentTime).lastCaughtUpTimestamp());

        // Node 1 catches up to leader
        assertFalse(state.updateReplicaState(node1, ++fetchTime, new LogOffsetMetadata(11L)));
        caughtUpTime = fetchTime;
        assertEquals(currentTime, describeVoterState(state, localId, currentTime).lastCaughtUpTimestamp());
        assertEquals(caughtUpTime, describeObserverState(state, node1, currentTime).lastCaughtUpTimestamp());

        // Node 1 falls behind
        assertTrue(state.updateLocalState(new LogOffsetMetadata(100L)));
        assertFalse(state.updateReplicaState(node1, ++fetchTime, new LogOffsetMetadata(50L)));
        assertEquals(currentTime, describeVoterState(state, localId, currentTime).lastCaughtUpTimestamp());
        assertEquals(caughtUpTime, describeObserverState(state, node1, currentTime).lastCaughtUpTimestamp());

        // Node 1 catches up to the last fetch offset
        int prevFetchTime = fetchTime;
        assertTrue(state.updateLocalState(new LogOffsetMetadata(200L)));
        assertFalse(state.updateReplicaState(node1, ++fetchTime, new LogOffsetMetadata(102L)));
        caughtUpTime = prevFetchTime;
        assertEquals(currentTime, describeVoterState(state, localId, currentTime).lastCaughtUpTimestamp());
        assertEquals(caughtUpTime, describeObserverState(state, node1, currentTime).lastCaughtUpTimestamp());

        // Node 1 catches up to leader
        assertFalse(state.updateReplicaState(node1, ++fetchTime, new LogOffsetMetadata(200L)));
        caughtUpTime = fetchTime;
        assertEquals(currentTime, describeVoterState(state, localId, currentTime).lastCaughtUpTimestamp());
        assertEquals(caughtUpTime, describeObserverState(state, node1, currentTime).lastCaughtUpTimestamp());
    }

    @Test
    public void testIdempotentEndOffsetUpdate() {
        LeaderState<?> state = newLeaderState(singleton(localId), 15L);
        assertEquals(Optional.empty(), state.highWatermark());
        assertTrue(state.updateLocalState(new LogOffsetMetadata(16L)));
        assertFalse(state.updateLocalState(new LogOffsetMetadata(16L)));
        assertEquals(Optional.of(new LogOffsetMetadata(16L)), state.highWatermark());
    }

    @Test
    public void testUpdateHighWatermarkMetadata() {
        LeaderState<?> state = newLeaderState(singleton(localId), 15L);
        assertEquals(Optional.empty(), state.highWatermark());

        LogOffsetMetadata initialHw = new LogOffsetMetadata(16L, Optional.of(new MockOffsetMetadata("bar")));
        assertTrue(state.updateLocalState(initialHw));
        assertEquals(Optional.of(initialHw), state.highWatermark());

        LogOffsetMetadata updateHw = new LogOffsetMetadata(16L, Optional.of(new MockOffsetMetadata("baz")));
        assertTrue(state.updateLocalState(updateHw));
        assertEquals(Optional.of(updateHw), state.highWatermark());
    }

    @Test
    public void testUpdateHighWatermarkQuorumSizeTwo() {
        int otherNodeId = 1;
        LeaderState<?> state = newLeaderState(mkSet(localId, otherNodeId), 10L);
        assertFalse(state.updateLocalState(new LogOffsetMetadata(13L)));
        assertEquals(singleton(otherNodeId), state.nonAcknowledgingVoters());
        assertEquals(Optional.empty(), state.highWatermark());
        assertFalse(state.updateReplicaState(otherNodeId, 0, new LogOffsetMetadata(10L)));
        assertEquals(emptySet(), state.nonAcknowledgingVoters());
        assertEquals(Optional.empty(), state.highWatermark());
        assertTrue(state.updateReplicaState(otherNodeId, 0, new LogOffsetMetadata(11L)));
        assertEquals(Optional.of(new LogOffsetMetadata(11L)), state.highWatermark());
        assertTrue(state.updateReplicaState(otherNodeId, 0, new LogOffsetMetadata(13L)));
        assertEquals(Optional.of(new LogOffsetMetadata(13L)), state.highWatermark());
    }

    @Test
    public void testUpdateHighWatermarkQuorumSizeThree() {
        int node1 = 1;
        int node2 = 2;
        LeaderState<?> state = newLeaderState(mkSet(localId, node1, node2), 10L);
        assertFalse(state.updateLocalState(new LogOffsetMetadata(15L)));
        assertEquals(mkSet(node1, node2), state.nonAcknowledgingVoters());
        assertEquals(Optional.empty(), state.highWatermark());
        assertFalse(state.updateReplicaState(node1, 0, new LogOffsetMetadata(10L)));
        assertEquals(singleton(node2), state.nonAcknowledgingVoters());
        assertEquals(Optional.empty(), state.highWatermark());
        assertFalse(state.updateReplicaState(node2, 0, new LogOffsetMetadata(10L)));
        assertEquals(emptySet(), state.nonAcknowledgingVoters());
        assertEquals(Optional.empty(), state.highWatermark());
        assertTrue(state.updateReplicaState(node2, 0, new LogOffsetMetadata(15L)));
        assertEquals(Optional.of(new LogOffsetMetadata(15L)), state.highWatermark());
        assertFalse(state.updateLocalState(new LogOffsetMetadata(20L)));
        assertEquals(Optional.of(new LogOffsetMetadata(15L)), state.highWatermark());
        assertTrue(state.updateReplicaState(node1, 0, new LogOffsetMetadata(20L)));
        assertEquals(Optional.of(new LogOffsetMetadata(20L)), state.highWatermark());
        assertFalse(state.updateReplicaState(node2, 0, new LogOffsetMetadata(20L)));
        assertEquals(Optional.of(new LogOffsetMetadata(20L)), state.highWatermark());
    }

    @Test
    public void testNonMonotonicHighWatermarkUpdate() {
        MockTime time = new MockTime();
        int node1 = 1;
        LeaderState<?> state = newLeaderState(mkSet(localId, node1), 0L);
        state.updateLocalState(new LogOffsetMetadata(10L));
        state.updateReplicaState(node1, time.milliseconds(), new LogOffsetMetadata(10L));
        assertEquals(Optional.of(new LogOffsetMetadata(10L)), state.highWatermark());

        // Follower crashes and disk is lost. It fetches an earlier offset to rebuild state.
        // The leader will report an error in the logs, but will not let the high watermark rewind
        assertFalse(state.updateReplicaState(node1, time.milliseconds(), new LogOffsetMetadata(5L)));
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

        LeaderState<?> state = newLeaderState(mkSet(localId), leaderStartOffset);

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
                .setLogEndOffset(-1)
                .setLastFetchTimestamp(time.milliseconds())
                .setLastCaughtUpTimestamp(time.milliseconds()),
            partitionData.currentVoters().get(0));


        // Now update the high watermark and verify the describe output
        assertTrue(state.updateLocalState(new LogOffsetMetadata(leaderEndOffset)));
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
                .setLogEndOffset(leaderEndOffset)
                .setLastFetchTimestamp(time.milliseconds())
                .setLastCaughtUpTimestamp(time.milliseconds()),
            partitionData.currentVoters().get(0));
    }

    @Test
    public void testDescribeQuorumWithMultipleVoters() {
        MockTime time = new MockTime();
        int activeFollowerId = 1;
        int inactiveFollowerId = 2;
        long leaderStartOffset = 10L;
        long leaderEndOffset = 15L;

        LeaderState<?> state = newLeaderState(mkSet(localId, activeFollowerId, inactiveFollowerId), leaderStartOffset);
        assertFalse(state.updateLocalState(new LogOffsetMetadata(leaderEndOffset)));
        assertEquals(Optional.empty(), state.highWatermark());

        long activeFollowerFetchTimeMs = time.milliseconds();
        assertTrue(state.updateReplicaState(activeFollowerId, activeFollowerFetchTimeMs, new LogOffsetMetadata(leaderEndOffset)));
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
                .setLogEndOffset(leaderEndOffset)
                .setLastFetchTimestamp(time.milliseconds())
                .setLastCaughtUpTimestamp(time.milliseconds()),
            leaderState);

        DescribeQuorumResponseData.ReplicaState activeFollowerState =
            findReplicaOrFail(activeFollowerId, partitionData.currentVoters());
        assertEquals(new DescribeQuorumResponseData.ReplicaState()
                .setReplicaId(activeFollowerId)
                .setLogEndOffset(leaderEndOffset)
                .setLastFetchTimestamp(activeFollowerFetchTimeMs)
                .setLastCaughtUpTimestamp(activeFollowerFetchTimeMs),
            activeFollowerState);

        DescribeQuorumResponseData.ReplicaState inactiveFollowerState =
            findReplicaOrFail(inactiveFollowerId, partitionData.currentVoters());
        assertEquals(new DescribeQuorumResponseData.ReplicaState()
                .setReplicaId(inactiveFollowerId)
                .setLogEndOffset(-1)
                .setLastFetchTimestamp(-1)
                .setLastCaughtUpTimestamp(-1),
            inactiveFollowerState);
    }

    private LeaderState<?> setUpLeaderAndFollowers(int follower1,
                                                   int follower2,
                                                   long leaderStartOffset,
                                                   long leaderEndOffset) {
        LeaderState<?> state = newLeaderState(mkSet(localId, follower1, follower2), leaderStartOffset);
        state.updateLocalState(new LogOffsetMetadata(leaderEndOffset));
        assertEquals(Optional.empty(), state.highWatermark());
        state.updateReplicaState(follower1, 0, new LogOffsetMetadata(leaderStartOffset));
        state.updateReplicaState(follower2, 0, new LogOffsetMetadata(leaderEndOffset));
        return state;
    }

    @Test
    public void testDescribeQuorumWithObservers() {
        MockTime time = new MockTime();
        int observerId = 10;
        long epochStartOffset = 10L;

        LeaderState<?> state = newLeaderState(mkSet(localId), epochStartOffset);
        assertTrue(state.updateLocalState(new LogOffsetMetadata(epochStartOffset + 1)));
        assertEquals(Optional.of(new LogOffsetMetadata(epochStartOffset + 1)), state.highWatermark());

        time.sleep(500);
        long observerFetchTimeMs = time.milliseconds();
        assertFalse(state.updateReplicaState(observerId, observerFetchTimeMs, new LogOffsetMetadata(epochStartOffset + 1)));

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
                .setLogEndOffset(epochStartOffset + 1)
                .setLastFetchTimestamp(observerFetchTimeMs)
                .setLastCaughtUpTimestamp(observerFetchTimeMs),
            observerState);
    }

    @Test
    public void testNoOpForNegativeRemoteNodeId() {
        MockTime time = new MockTime();
        int replicaId = -1;
        long epochStartOffset = 10L;

        LeaderState<?> state = newLeaderState(mkSet(localId), epochStartOffset);
        assertFalse(state.updateReplicaState(replicaId, 0, new LogOffsetMetadata(epochStartOffset)));

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

        state.updateReplicaState(observerId, time.milliseconds(), new LogOffsetMetadata(epochStartOffset));
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
        LeaderState<?> state = newLeaderState(Utils.mkSet(1, 2, 3), 1);

        assertFalse(state.canGrantVote(1, isLogUpToDate));
        assertFalse(state.canGrantVote(2, isLogUpToDate));
        assertFalse(state.canGrantVote(3, isLogUpToDate));
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
        LeaderState state,
        int voterId,
        long currentTimeMs
    ) {
        DescribeQuorumResponseData.PartitionData partitionData = state.describeQuorum(currentTimeMs);
        return findReplicaOrFail(voterId, partitionData.currentVoters());
    }

    private DescribeQuorumResponseData.ReplicaState describeObserverState(
        LeaderState state,
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

}
