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

import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Utils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
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
    private final LogContext logContext = new LogContext();

    private LeaderState newLeaderState(
        Set<Integer> voters,
        long epochStartOffset
    ) {
        return new LeaderState(
            localId,
            epoch,
            epochStartOffset,
            voters,
            voters,
            logContext
        );
    }

    @Test
    public void testFollowerAcknowledgement() {
        int node1 = 1;
        int node2 = 2;
        LeaderState state = newLeaderState(mkSet(localId, node1, node2), 0L);
        assertEquals(mkSet(node1, node2), state.nonAcknowledgingVoters());
        state.addAcknowledgementFrom(node1);
        assertEquals(singleton(node2), state.nonAcknowledgingVoters());
        state.addAcknowledgementFrom(node2);
        assertEquals(emptySet(), state.nonAcknowledgingVoters());
    }

    @Test
    public void testNonFollowerAcknowledgement() {
        int nonVoterId = 1;
        LeaderState state = newLeaderState(singleton(localId), 0L);
        assertThrows(IllegalArgumentException.class, () -> state.addAcknowledgementFrom(nonVoterId));
    }

    @Test
    public void testUpdateHighWatermarkQuorumSizeOne() {
        LeaderState state = newLeaderState(singleton(localId), 15L);
        assertEquals(Optional.empty(), state.highWatermark());
        assertFalse(state.updateLocalState(0, new LogOffsetMetadata(15L)));
        assertEquals(emptySet(), state.nonAcknowledgingVoters());
        assertEquals(Optional.empty(), state.highWatermark());
        assertTrue(state.updateLocalState(0, new LogOffsetMetadata(16L)));
        assertEquals(Optional.of(new LogOffsetMetadata(16L)), state.highWatermark());
        assertTrue(state.updateLocalState(0, new LogOffsetMetadata(20)));
        assertEquals(Optional.of(new LogOffsetMetadata(20L)), state.highWatermark());
    }

    @Test
    public void testNonMonotonicLocalEndOffsetUpdate() {
        LeaderState state = newLeaderState(singleton(localId), 15L);
        assertEquals(Optional.empty(), state.highWatermark());
        assertTrue(state.updateLocalState(0, new LogOffsetMetadata(16L)));
        assertEquals(Optional.of(new LogOffsetMetadata(16L)), state.highWatermark());
        assertThrows(IllegalStateException.class,
            () -> state.updateLocalState(0, new LogOffsetMetadata(15L)));
    }

    @Test
    public void testIdempotentEndOffsetUpdate() {
        LeaderState state = newLeaderState(singleton(localId), 15L);
        assertEquals(Optional.empty(), state.highWatermark());
        assertTrue(state.updateLocalState(0, new LogOffsetMetadata(16L)));
        assertFalse(state.updateLocalState(0, new LogOffsetMetadata(16L)));
        assertEquals(Optional.of(new LogOffsetMetadata(16L)), state.highWatermark());
    }

    @Test
    public void testUpdateHighWatermarkMetadata() {
        LeaderState state = newLeaderState(singleton(localId), 15L);
        assertEquals(Optional.empty(), state.highWatermark());

        LogOffsetMetadata initialHw = new LogOffsetMetadata(16L, Optional.of(new MockOffsetMetadata("bar")));
        assertTrue(state.updateLocalState(0, initialHw));
        assertEquals(Optional.of(initialHw), state.highWatermark());

        LogOffsetMetadata updateHw = new LogOffsetMetadata(16L, Optional.of(new MockOffsetMetadata("baz")));
        assertTrue(state.updateLocalState(0, updateHw));
        assertEquals(Optional.of(updateHw), state.highWatermark());
    }

    @Test
    public void testUpdateHighWatermarkQuorumSizeTwo() {
        int otherNodeId = 1;
        LeaderState state = newLeaderState(mkSet(localId, otherNodeId), 10L);
        assertFalse(state.updateLocalState(0, new LogOffsetMetadata(13L)));
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
        LeaderState state = newLeaderState(mkSet(localId, node1, node2), 10L);
        assertFalse(state.updateLocalState(0, new LogOffsetMetadata(15L)));
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
        assertFalse(state.updateLocalState(0, new LogOffsetMetadata(20L)));
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
        LeaderState state = newLeaderState(mkSet(localId, node1), 0L);
        state.updateLocalState(time.milliseconds(), new LogOffsetMetadata(10L));
        state.updateReplicaState(node1, time.milliseconds(), new LogOffsetMetadata(10L));
        assertEquals(Optional.of(new LogOffsetMetadata(10L)), state.highWatermark());

        // Follower crashes and disk is lost. It fetches an earlier offset to rebuild state.
        // The leader will report an error in the logs, but will not let the high watermark rewind
        assertFalse(state.updateReplicaState(node1, time.milliseconds(), new LogOffsetMetadata(5L)));
        assertEquals(5L, state.getVoterEndOffsets().get(node1));
        assertEquals(Optional.of(new LogOffsetMetadata(10L)), state.highWatermark());
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
        LeaderState state = newLeaderState(mkSet(localId, follower1, follower2), leaderStartOffset);
        state.updateLocalState(0, new LogOffsetMetadata(leaderEndOffset));
        assertEquals(Optional.empty(), state.highWatermark());
        state.updateReplicaState(follower1, 0, new LogOffsetMetadata(leaderStartOffset));
        state.updateReplicaState(follower2, 0, new LogOffsetMetadata(leaderEndOffset));
        return state;
    }

    @Test
    public void testGetObserverStatesWithObserver() {
        int observerId = 10;
        long epochStartOffset = 10L;

        LeaderState state = newLeaderState(mkSet(localId), epochStartOffset);
        long timestamp = 20L;
        assertFalse(state.updateReplicaState(observerId, timestamp, new LogOffsetMetadata(epochStartOffset)));

        assertEquals(Collections.singletonMap(observerId, epochStartOffset), state.getObserverStates(timestamp));
    }

    @Test
    public void testNoOpForNegativeRemoteNodeId() {
        int observerId = -1;
        long epochStartOffset = 10L;

        LeaderState state = newLeaderState(mkSet(localId), epochStartOffset);
        assertFalse(state.updateReplicaState(observerId, 0, new LogOffsetMetadata(epochStartOffset)));

        assertEquals(Collections.emptyMap(), state.getObserverStates(10));
    }

    @Test
    public void testObserverStateExpiration() {
        MockTime time = new MockTime();
        int observerId = 10;
        long epochStartOffset = 10L;
        LeaderState state = newLeaderState(mkSet(localId), epochStartOffset);

        state.updateReplicaState(observerId, time.milliseconds(), new LogOffsetMetadata(epochStartOffset));
        assertEquals(singleton(observerId), state.getObserverStates(time.milliseconds()).keySet());

        time.sleep(LeaderState.OBSERVER_SESSION_TIMEOUT_MS);
        assertEquals(emptySet(), state.getObserverStates(time.milliseconds()).keySet());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testGrantVote(boolean isLogUpToDate) {
        LeaderState state = newLeaderState(Utils.mkSet(1, 2, 3), 1);

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

}
