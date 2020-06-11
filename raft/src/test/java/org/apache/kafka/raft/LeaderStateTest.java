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

import org.apache.kafka.common.utils.Utils;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.OptionalLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class LeaderStateTest {
    private final int localId = 0;
    private final int epoch = 5;

    @Test
    public void testFollowerEndorsement() {
        int node1 = 1;
        int node2 = 2;
        LeaderState state = new LeaderState(localId, epoch, 0L, Utils.mkSet(localId, node1, node2));
        assertEquals(Utils.mkSet(node1, node2), state.nonEndorsingFollowers());
        state.addEndorsementFrom(node1);
        assertEquals(Collections.singleton(node2), state.nonEndorsingFollowers());
        state.addEndorsementFrom(node2);
        assertEquals(Collections.emptySet(), state.nonEndorsingFollowers());
    }

    @Test
    public void testNonFollowerEndorsement() {
        int nonVoterId = 1;
        LeaderState state = new LeaderState(localId, epoch, 0L, Collections.singleton(localId));
        assertThrows(IllegalArgumentException.class, () -> state.addEndorsementFrom(nonVoterId));
    }

    @Test
    public void testUpdateHighWatermarkQuorumSizeOne() {
        LeaderState state = new LeaderState(localId, epoch, 15L, Collections.singleton(localId));
        assertEquals(OptionalLong.empty(), state.highWatermark());
        state.updateLocalEndOffset(15L);
        assertEquals(OptionalLong.of(15L), state.highWatermark());
    }

    @Test
    public void testNonMonotonicEndOffsetUpdate() {
        LeaderState state = new LeaderState(localId, epoch, 15L, Collections.singleton(localId));
        assertEquals(OptionalLong.empty(), state.highWatermark());
        state.updateLocalEndOffset(15L);
        assertEquals(OptionalLong.of(15L), state.highWatermark());
        assertThrows(IllegalArgumentException.class, () -> state.updateLocalEndOffset(14L));
    }

    @Test
    public void testIdempotentEndOffsetUpdate() {
        LeaderState state = new LeaderState(localId, epoch, 15L, Collections.singleton(localId));
        assertEquals(OptionalLong.empty(), state.highWatermark());
        state.updateLocalEndOffset(15L);
        state.updateLocalEndOffset(15L);
        assertEquals(OptionalLong.of(15L), state.highWatermark());
    }

    @Test
    public void testUpdateHighWatermarkQuorumSizeTwo() {
        int otherNodeId = 1;
        LeaderState state = new LeaderState(localId, epoch, 10L, Utils.mkSet(localId, otherNodeId));
        state.updateLocalEndOffset(15L);
        assertEquals(OptionalLong.empty(), state.highWatermark());
        state.updateEndOffset(otherNodeId, 10L);
        assertEquals(Collections.emptySet(), state.nonEndorsingFollowers());
        assertEquals(OptionalLong.of(10L), state.highWatermark());
        state.updateEndOffset(otherNodeId, 15L);
        assertEquals(OptionalLong.of(15L), state.highWatermark());
    }

    @Test
    public void testHighWatermarkUnknownUntilStartOfLeaderEpoch() {
        int otherNodeId = 1;
        LeaderState state = new LeaderState(localId, epoch, 15L, Utils.mkSet(localId, otherNodeId));
        state.updateLocalEndOffset(20L);
        assertEquals(OptionalLong.empty(), state.highWatermark());
        state.updateEndOffset(otherNodeId, 10L);
        assertEquals(OptionalLong.empty(), state.highWatermark());
        state.updateEndOffset(otherNodeId, 15L);
        assertEquals(OptionalLong.of(15L), state.highWatermark());
    }

    @Test
    public void testUpdateHighWatermarkQuorumSizeThree() {
        int node1 = 1;
        int node2 = 2;
        LeaderState state = new LeaderState(localId, epoch, 10L, Utils.mkSet(localId, node1, node2));
        state.updateLocalEndOffset(15L);
        assertEquals(OptionalLong.empty(), state.highWatermark());
        state.updateEndOffset(node1, 10L);
        assertEquals(Collections.singleton(node2), state.nonEndorsingFollowers());
        assertEquals(OptionalLong.of(10L), state.highWatermark());
        state.updateEndOffset(node2, 15L);
        assertEquals(OptionalLong.of(15L), state.highWatermark());
        state.updateLocalEndOffset(20L);
        assertEquals(OptionalLong.of(15L), state.highWatermark());
        state.updateEndOffset(node2, 20L);
        assertEquals(OptionalLong.of(20L), state.highWatermark());
        state.updateEndOffset(node1, 20L);
        assertEquals(OptionalLong.of(20L), state.highWatermark());
    }

    @Test
    public void testGetNonLeaderFollowersByFetchOffsetDescending() {
        int node1 = 1;
        int node2 = 2;
        LeaderState state = new LeaderState(localId, epoch, 10L, Utils.mkSet(localId, node1, node2));
        state.updateLocalEndOffset(15L);
        assertEquals(OptionalLong.empty(), state.highWatermark());
        state.updateEndOffset(node1, 10L);
        state.updateEndOffset(node2, 15L);

        // Leader should not be included; the follower with larger offset should be prioritized.
        assertEquals(Arrays.asList(node2, node1), state.nonLeaderVotersByDescendingFetchOffset());
    }
}
