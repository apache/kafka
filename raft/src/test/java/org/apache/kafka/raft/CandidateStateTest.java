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

import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Utils;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CandidateStateTest {
    private final int localId = 0;
    private final int epoch = 5;
    private final MockTime time = new MockTime();
    private final int electionTimeoutMs = 5000;

    @Test
    public void testSingleNodeQuorum() {
        CandidateState state = new CandidateState(time, localId, epoch,
            Collections.singleton(localId), Optional.empty(), 0, electionTimeoutMs);
        assertTrue(state.isVoteGranted());
        assertFalse(state.isVoteRejected());
        assertEquals(Collections.emptySet(), state.unrecordedVoters());
    }

    @Test
    public void testTwoNodeQuorumVoteRejected() {
        int otherNodeId = 1;
        CandidateState state = new CandidateState(time, localId, epoch,
            Utils.mkSet(localId, otherNodeId), Optional.empty(), 0, electionTimeoutMs);
        assertFalse(state.isVoteGranted());
        assertFalse(state.isVoteRejected());
        assertEquals(Collections.singleton(otherNodeId), state.unrecordedVoters());
        assertTrue(state.recordRejectedVote(otherNodeId));
        assertFalse(state.isVoteGranted());
        assertTrue(state.isVoteRejected());
    }

    @Test
    public void testTwoNodeQuorumVoteGranted() {
        int otherNodeId = 1;
        CandidateState state = new CandidateState(time, localId, epoch,
            Utils.mkSet(localId, otherNodeId), Optional.empty(), 0, electionTimeoutMs);
        assertFalse(state.isVoteGranted());
        assertFalse(state.isVoteRejected());
        assertEquals(Collections.singleton(otherNodeId), state.unrecordedVoters());
        assertTrue(state.recordGrantedVote(otherNodeId));
        assertEquals(Collections.emptySet(), state.unrecordedVoters());
        assertFalse(state.isVoteRejected());
        assertTrue(state.isVoteGranted());
    }

    @Test
    public void testThreeNodeQuorumVoteGranted() {
        int node1 = 1;
        int node2 = 2;
        CandidateState state = new CandidateState(time, localId, epoch,
            Utils.mkSet(localId, node1, node2), Optional.empty(), 0, electionTimeoutMs);
        assertFalse(state.isVoteGranted());
        assertFalse(state.isVoteRejected());
        assertEquals(Utils.mkSet(node1, node2), state.unrecordedVoters());
        assertTrue(state.recordGrantedVote(node1));
        assertEquals(Collections.singleton(node2), state.unrecordedVoters());
        assertTrue(state.isVoteGranted());
        assertFalse(state.isVoteRejected());
        assertTrue(state.recordRejectedVote(node2));
        assertEquals(Collections.emptySet(), state.unrecordedVoters());
        assertTrue(state.isVoteGranted());
        assertFalse(state.isVoteRejected());
    }

    @Test
    public void testThreeNodeQuorumVoteRejected() {
        int node1 = 1;
        int node2 = 2;
        CandidateState state = new CandidateState(time, localId, epoch,
            Utils.mkSet(localId, node1, node2), Optional.empty(), 0, electionTimeoutMs);
        assertFalse(state.isVoteGranted());
        assertFalse(state.isVoteRejected());
        assertEquals(Utils.mkSet(node1, node2), state.unrecordedVoters());
        assertTrue(state.recordRejectedVote(node1));
        assertEquals(Collections.singleton(node2), state.unrecordedVoters());
        assertFalse(state.isVoteGranted());
        assertFalse(state.isVoteRejected());
        assertTrue(state.recordRejectedVote(node2));
        assertEquals(Collections.emptySet(), state.unrecordedVoters());
        assertFalse(state.isVoteGranted());
        assertTrue(state.isVoteRejected());
    }

    @Test
    public void testCannotRejectVoteFromLocalId() {
        int otherNodeId = 1;
        CandidateState state = new CandidateState(time, localId, epoch,
            Utils.mkSet(localId, otherNodeId), Optional.empty(), 0, electionTimeoutMs);
        assertThrows(IllegalArgumentException.class, () -> state.recordRejectedVote(localId));
    }

    @Test
    public void testCannotChangeVoteGrantedToRejected() {
        int otherNodeId = 1;
        CandidateState state = new CandidateState(time, localId, epoch,
            Utils.mkSet(localId, otherNodeId), Optional.empty(), 0, electionTimeoutMs);
        assertTrue(state.recordGrantedVote(otherNodeId));
        assertThrows(IllegalArgumentException.class, () -> state.recordRejectedVote(otherNodeId));
        assertTrue(state.isVoteGranted());
    }

    @Test
    public void testCannotChangeVoteRejectedToGranted() {
        int otherNodeId = 1;
        CandidateState state = new CandidateState(time, localId, epoch,
            Utils.mkSet(localId, otherNodeId), Optional.empty(), 0, electionTimeoutMs);
        assertTrue(state.recordRejectedVote(otherNodeId));
        assertThrows(IllegalArgumentException.class, () -> state.recordGrantedVote(otherNodeId));
        assertTrue(state.isVoteRejected());
    }

    @Test
    public void testCannotGrantOrRejectNonVoters() {
        int nonVoterId = 1;
        CandidateState state = new CandidateState(time, localId, epoch,
            Collections.singleton(localId), Optional.empty(), 0, electionTimeoutMs);
        assertThrows(IllegalArgumentException.class, () -> state.recordGrantedVote(nonVoterId));
        assertThrows(IllegalArgumentException.class, () -> state.recordRejectedVote(nonVoterId));
    }

    @Test
    public void testIdempotentGrant() {
        int otherNodeId = 1;
        CandidateState state = new CandidateState(time, localId, epoch,
            Utils.mkSet(localId, otherNodeId), Optional.empty(), 0, electionTimeoutMs);
        assertTrue(state.recordGrantedVote(otherNodeId));
        assertFalse(state.recordGrantedVote(otherNodeId));
    }

    @Test
    public void testIdempotentReject() {
        int otherNodeId = 1;
        CandidateState state = new CandidateState(time, localId, epoch,
            Utils.mkSet(localId, otherNodeId), Optional.empty(), 0, electionTimeoutMs);
        assertTrue(state.recordRejectedVote(otherNodeId));
        assertFalse(state.recordRejectedVote(otherNodeId));
    }

}
