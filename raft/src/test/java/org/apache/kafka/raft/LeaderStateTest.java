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

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LeaderStateTest {
    private final int localId = 0;
    private final int epoch = 5;

    @Test
    public void testFollowerAcknowledgement() {
        int node1 = 1;
        int node2 = 2;
        LeaderState state = new LeaderState(localId, epoch, 0L, mkSet(localId, node1, node2), Collections.emptySet());
        assertEquals(mkSet(node1, node2), state.nonAcknowledgingVoters());
        state.addAcknowledgementFrom(node1);
        assertEquals(Collections.singleton(node2), state.nonAcknowledgingVoters());
        state.addAcknowledgementFrom(node2);
        assertEquals(Collections.emptySet(), state.nonAcknowledgingVoters());
    }

    @Test
    public void testNonFollowerAcknowledgement() {
        int nonVoterId = 1;
        LeaderState state = new LeaderState(localId, epoch, 0L, Collections.singleton(localId), Collections.emptySet());
        assertThrows(IllegalArgumentException.class, () -> state.addAcknowledgementFrom(nonVoterId));
    }

    @Test
    public void testUpdateHighWatermarkQuorumSizeOne() {
        LeaderState state = new LeaderState(localId, epoch, 15L, Collections.singleton(localId), Collections.emptySet());
        assertEquals(Optional.empty(), state.highWatermark());
        assertTrue(state.updateLocalState(0, new LogOffsetMetadata(15L)));
        assertEquals(Optional.of(new LogOffsetMetadata(15L)), state.highWatermark());
    }

    @Test
    public void testNonMonotonicEndOffsetUpdate() {
        LeaderState state = new LeaderState(localId, epoch, 15L, Collections.singleton(localId), Collections.emptySet());
        assertEquals(Optional.empty(), state.highWatermark());
        assertTrue(state.updateLocalState(0, new LogOffsetMetadata(15L)));
        assertEquals(Optional.of(new LogOffsetMetadata(15L)), state.highWatermark());
        assertThrows(IllegalArgumentException.class,
            () -> state.updateLocalState(0, new LogOffsetMetadata(14L)));
    }

    @Test
    public void testIdempotentEndOffsetUpdate() {
        LeaderState state = new LeaderState(localId, epoch, 15L, Collections.singleton(localId), Collections.emptySet());
        assertEquals(Optional.empty(), state.highWatermark());
        assertTrue(state.updateLocalState(0, new LogOffsetMetadata(15L)));
        assertFalse(state.updateLocalState(0, new LogOffsetMetadata(15L)));
        assertEquals(Optional.of(new LogOffsetMetadata(15L)), state.highWatermark());
    }

    @Test
    public void testUpdateHighWatermarkMetadata() {
        LeaderState state = new LeaderState(localId, epoch, 15L, Collections.singleton(localId), Collections.emptySet());
        assertEquals(Optional.empty(), state.highWatermark());

        LogOffsetMetadata initialHw = new LogOffsetMetadata(15L, Optional.of(new MockOffsetMetadata("foo")));
        assertTrue(state.updateLocalState(0, initialHw));
        assertEquals(Optional.of(initialHw), state.highWatermark());

        LogOffsetMetadata updateHw = new LogOffsetMetadata(15L, Optional.of(new MockOffsetMetadata("bar")));
        assertTrue(state.updateLocalState(0, updateHw));
        assertEquals(Optional.of(updateHw), state.highWatermark());
    }

    @Test
    public void testUpdateHighWatermarkQuorumSizeTwo() {
        int otherNodeId = 1;
        LeaderState state = new LeaderState(localId, epoch, 10L, mkSet(localId, otherNodeId), Collections.emptySet());
        assertFalse(state.updateLocalState(0, new LogOffsetMetadata(15L)));
        assertEquals(Optional.empty(), state.highWatermark());
        assertTrue(state.updateReplicaState(otherNodeId, 0, new LogOffsetMetadata(10L)));
        assertEquals(Collections.emptySet(), state.nonAcknowledgingVoters());
        assertEquals(Optional.of(new LogOffsetMetadata(10L)), state.highWatermark());
        assertTrue(state.updateReplicaState(otherNodeId, 0, new LogOffsetMetadata(15L)));
        assertEquals(Optional.of(new LogOffsetMetadata(15L)), state.highWatermark());
    }

    @Test
    public void testHighWatermarkUnknownUntilStartOfLeaderEpoch() {
        int otherNodeId = 1;
        LeaderState state = new LeaderState(localId, epoch, 15L, mkSet(localId, otherNodeId), Collections.emptySet());
        assertFalse(state.updateLocalState(0, new LogOffsetMetadata(20L)));
        assertEquals(Optional.empty(), state.highWatermark());
        assertFalse(state.updateReplicaState(otherNodeId, 0, new LogOffsetMetadata(10L)));
        assertEquals(Optional.empty(), state.highWatermark());
        assertTrue(state.updateReplicaState(otherNodeId, 0, new LogOffsetMetadata(15L)));
        assertEquals(Optional.of(new LogOffsetMetadata(15L)), state.highWatermark());
    }

    @Test
    public void testUpdateHighWatermarkQuorumSizeThree() {
        int node1 = 1;
        int node2 = 2;
        LeaderState state = new LeaderState(localId, epoch, 10L, mkSet(localId, node1, node2), Collections.emptySet());
        assertFalse(state.updateLocalState(0, new LogOffsetMetadata(15L)));
        assertEquals(Optional.empty(), state.highWatermark());
        assertTrue(state.updateReplicaState(node1, 0, new LogOffsetMetadata(10L)));
        assertEquals(Collections.singleton(node2), state.nonAcknowledgingVoters());
        assertEquals(Optional.of(new LogOffsetMetadata(10L)), state.highWatermark());
        assertTrue(state.updateReplicaState(node2, 0, new LogOffsetMetadata(15L)));
        assertEquals(Optional.of(new LogOffsetMetadata(15L)), state.highWatermark());
        assertFalse(state.updateLocalState(0, new LogOffsetMetadata(20L)));
        assertEquals(Optional.of(new LogOffsetMetadata(15L)), state.highWatermark());
        assertTrue(state.updateReplicaState(node2, 0, new LogOffsetMetadata(20L)));
        assertEquals(Optional.of(new LogOffsetMetadata(20L)), state.highWatermark());
        assertFalse(state.updateReplicaState(node1, 0, new LogOffsetMetadata(20L)));
        assertEquals(Optional.of(new LogOffsetMetadata(20L)), state.highWatermark());
    }

    @Test
    public void testGetNonLeaderFollowersByFetchOffsetDescending() {
        int node1 = 1;
        int node2 = 2;
        long leaderStartOffset = 10L;
        long leaderEndOffset = 15L;

        LeaderState state = setUpLeaderAndFollowers(node1, node2, leaderStartOffset, leaderEndOffset);

        // Leader should not be included; the follower with larger offset should be prioritized.
        assertEquals(Arrays.asList(node2, node1), state.nonLeaderVotersByDescendingFetchOffset());
    }

    @Test
    public void testGetVoterStates() {
        int node1 = 1;
        int node2 = 2;
        long leaderStartOffset = 10L;
        long leaderEndOffset = 15L;

        LeaderState state = setUpLeaderAndFollowers(node1, node2, leaderStartOffset, leaderEndOffset);

        assertEquals(mkMap(
            mkEntry(localId, leaderEndOffset),
            mkEntry(node1, leaderStartOffset),
            mkEntry(node2, leaderEndOffset)
        ), state.getVoterEndOffsets());
    }

    private LeaderState setUpLeaderAndFollowers(int follower1,
                                                int follower2,
                                                long leaderStartOffset,
                                                long leaderEndOffset) {
        LeaderState state = new LeaderState(localId, epoch, leaderStartOffset, mkSet(localId, follower1, follower2), Collections.emptySet());
        state.updateLocalState(0, new LogOffsetMetadata(leaderEndOffset));
        assertEquals(Optional.empty(), state.highWatermark());
        state.updateReplicaState(follower1, 0, new LogOffsetMetadata(leaderStartOffset));
        state.updateReplicaState(follower2, 0, new LogOffsetMetadata(leaderEndOffset));
        return state;
    }

    @Test
    public void testGetObserverStatesWithObserver() {
        int observerId = 10;
        long endOffset = 10L;

        LeaderState state = new LeaderState(localId, epoch, endOffset, mkSet(localId), Collections.emptySet());
        long timestamp = 20L;
        assertFalse(state.updateReplicaState(observerId, timestamp, new LogOffsetMetadata(endOffset)));

        assertEquals(Collections.singletonMap(observerId, endOffset), state.getObserverStates(timestamp));
    }

    @Test
    public void testNoOpForNegativeRemoteNodeId() {
        int observerId = -1;
        long endOffset = 10L;

        LeaderState state = new LeaderState(localId, epoch, endOffset, mkSet(localId), Collections.emptySet());
        assertFalse(state.updateReplicaState(observerId, 0, new LogOffsetMetadata(endOffset)));

        assertEquals(Collections.emptyMap(), state.getObserverStates(10));
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

}
