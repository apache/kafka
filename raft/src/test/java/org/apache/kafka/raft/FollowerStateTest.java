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

import java.util.OptionalLong;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class FollowerStateTest {
    private final int epoch = 5;
    private final Set<Integer> voters = Utils.mkSet(1, 2);

    @Test
    public void testVoteForCandidate() {
        FollowerState state = new FollowerState(epoch, voters);
        assertTrue(state.assertNotAttached());
        assertFalse(state.hasVoted());

        int votedId = 1;
        assertTrue(state.grantVoteTo(votedId));
        assertTrue(state.hasVoted());
        assertTrue(state.hasVotedFor(votedId));
    }

    @Test
    public void testCannotChangeVote() {
        FollowerState state = new FollowerState(epoch, voters);
        int votedId = 1;
        int otherCandidateId = 2;
        assertTrue(state.grantVoteTo(votedId));
        assertThrows(IllegalArgumentException.class, () -> state.grantVoteTo(otherCandidateId));
    }

    @Test
    public void testIdempotentVote() {
        FollowerState state = new FollowerState(epoch, voters);
        int votedId = 1;
        assertTrue(state.grantVoteTo(votedId));
        assertFalse(state.grantVoteTo(votedId));
        assertTrue(state.hasVoted());
        assertTrue(state.hasVotedFor(votedId));
    }

    @Test
    public void testCannotVoteIfLeaderIsKnown() {
        FollowerState state = new FollowerState(epoch, voters);
        int leaderId = 1;
        int candidateId = 2;
        state.acknowledgeLeader(leaderId);
        assertFalse(state.hasVoted());
        assertThrows(IllegalArgumentException.class, () -> state.grantVoteTo(candidateId));
        assertFalse(state.hasVoted());
        assertEquals(leaderId, state.leaderId());
    }

    @Test
    public void testAckLeaderWithoutVoting() {
        FollowerState state = new FollowerState(epoch, voters);
        int leaderId = 1;
        state.acknowledgeLeader(leaderId);
        assertTrue(state.hasLeader());
        assertEquals(leaderId, state.leaderId());
    }

    @Test
    public void testAckLeaderAfterVoting() {
        FollowerState state = new FollowerState(epoch, voters);
        int candidateId = 1;
        int leaderId = 2;
        assertTrue(state.grantVoteTo(candidateId));
        assertTrue(state.acknowledgeLeader(leaderId));
        assertFalse(state.hasVotedFor(candidateId));
        assertFalse(state.hasVoted());
        assertTrue(state.hasLeader());
        assertEquals(leaderId, state.leaderId());
    }

    @Test
    public void testCannotChangeLeader() {
        FollowerState state = new FollowerState(epoch, voters);
        int leaderId = 1;
        int otherLeaderId = 2;
        assertTrue(state.acknowledgeLeader(leaderId));
        assertThrows(IllegalArgumentException.class, () -> state.acknowledgeLeader(otherLeaderId));
        assertTrue(state.hasLeader());
        assertEquals(leaderId, state.leaderId());
    }

    @Test
    public void testIdempotentLeaderAcknowledgement() {
        FollowerState state = new FollowerState(epoch, voters);
        int leaderId = 1;
        assertTrue(state.acknowledgeLeader(leaderId));
        assertFalse(state.acknowledgeLeader(leaderId));
        assertTrue(state.hasLeader());
        assertEquals(leaderId, state.leaderId());
    }

    @Test
    public void testUpdateHighWatermarkOnlyPermittedWithLeader() {
        OptionalLong highWatermark = OptionalLong.of(15L);
        FollowerState state = new FollowerState(epoch, voters);
        assertThrows(IllegalArgumentException.class, () -> state.updateHighWatermark(highWatermark));
        int candidateId = 1;
        assertTrue(state.grantVoteTo(candidateId));
        assertThrows(IllegalArgumentException.class, () -> state.updateHighWatermark(highWatermark));
        int leaderId = 2;
        assertTrue(state.acknowledgeLeader(leaderId));
        state.updateHighWatermark(highWatermark);
        assertEquals(highWatermark, state.highWatermark());
    }

    @Test
    public void testMonotonicHighWatermark() {
        OptionalLong highWatermark = OptionalLong.of(15L);
        FollowerState state = new FollowerState(epoch, voters);
        int leaderId = 1;
        assertTrue(state.acknowledgeLeader(leaderId));
        state.updateHighWatermark(highWatermark);
        assertThrows(IllegalArgumentException.class, () -> state.updateHighWatermark(OptionalLong.empty()));
        assertThrows(IllegalArgumentException.class, () -> state.updateHighWatermark(OptionalLong.of(14L)));
        state.updateHighWatermark(highWatermark);
        assertEquals(highWatermark, state.highWatermark());
    }

}