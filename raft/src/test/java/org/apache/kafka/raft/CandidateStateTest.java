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
import org.apache.kafka.raft.internals.ReplicaKey;
import org.apache.kafka.raft.internals.VoterSet;
import org.apache.kafka.raft.internals.VoterSetTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CandidateStateTest {
    private final VoterSet.VoterNode localNode = VoterSetTest.voterNode(0, true);
    private final int epoch = 5;
    private final MockTime time = new MockTime();
    private final int electionTimeoutMs = 5000;
    private final LogContext logContext = new LogContext();

    private CandidateState newCandidateState(VoterSet voters) {
        return new CandidateState(
                time,
                localNode.voterKey().id(),
                localNode.voterKey().directoryId().get(),
                epoch,
                voters,
                Optional.empty(),
                0,
                electionTimeoutMs,
                logContext
        );
    }

    @Test
    public void testSingleNodeQuorum() {
        CandidateState state = newCandidateState(voterSetWithLocal(Collections.emptyList()));
        assertTrue(state.isVoteGranted());
        assertFalse(state.isVoteRejected());
        assertEquals(Collections.emptySet(), state.unrecordedVoters());
    }

    @Test
    public void testTwoNodeQuorumVoteRejected() {
        int otherNodeId = 1;
        CandidateState state = newCandidateState(
            voterSetWithLocal(Collections.singletonList(otherNodeId))
        );
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
        CandidateState state = newCandidateState(
            voterSetWithLocal(Collections.singletonList(otherNodeId))
        );
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
        CandidateState state = newCandidateState(
            voterSetWithLocal(Arrays.asList(node1, node2))
        );
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
        CandidateState state = newCandidateState(
            voterSetWithLocal(Arrays.asList(node1, node2))
        );
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
        CandidateState state = newCandidateState(
            voterSetWithLocal(Collections.singletonList(otherNodeId))
        );
        assertThrows(
            IllegalArgumentException.class,
            () -> state.recordRejectedVote(localNode.voterKey().id())
        );
    }

    @Test
    public void testCannotChangeVoteGrantedToRejected() {
        int otherNodeId = 1;
        CandidateState state = newCandidateState(
            voterSetWithLocal(Collections.singletonList(otherNodeId))
        );
        assertTrue(state.recordGrantedVote(otherNodeId));
        assertThrows(IllegalArgumentException.class, () -> state.recordRejectedVote(otherNodeId));
        assertTrue(state.isVoteGranted());
    }

    @Test
    public void testCannotChangeVoteRejectedToGranted() {
        int otherNodeId = 1;
        CandidateState state = newCandidateState(
            voterSetWithLocal(Collections.singletonList(otherNodeId))
        );
        assertTrue(state.recordRejectedVote(otherNodeId));
        assertThrows(IllegalArgumentException.class, () -> state.recordGrantedVote(otherNodeId));
        assertTrue(state.isVoteRejected());
    }

    @Test
    public void testCannotGrantOrRejectNonVoters() {
        int nonVoterId = 1;
        CandidateState state = newCandidateState(voterSetWithLocal(Collections.emptyList()));
        assertThrows(IllegalArgumentException.class, () -> state.recordGrantedVote(nonVoterId));
        assertThrows(IllegalArgumentException.class, () -> state.recordRejectedVote(nonVoterId));
    }

    @Test
    public void testIdempotentGrant() {
        int otherNodeId = 1;
        CandidateState state = newCandidateState(
            voterSetWithLocal(Collections.singletonList(otherNodeId))
        );
        assertTrue(state.recordGrantedVote(otherNodeId));
        assertFalse(state.recordGrantedVote(otherNodeId));
    }

    @Test
    public void testIdempotentReject() {
        int otherNodeId = 1;
        CandidateState state = newCandidateState(
            voterSetWithLocal(Collections.singletonList(otherNodeId))
        );
        assertTrue(state.recordRejectedVote(otherNodeId));
        assertFalse(state.recordRejectedVote(otherNodeId));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testGrantVote(boolean isLogUpToDate) {
        CandidateState state = newCandidateState(
            voterSetWithLocal(Arrays.asList(1, 2, 3))
        );

        assertFalse(state.canGrantVote(ReplicaKey.of(0, Optional.empty()), isLogUpToDate));
        assertFalse(state.canGrantVote(ReplicaKey.of(1, Optional.empty()), isLogUpToDate));
        assertFalse(state.canGrantVote(ReplicaKey.of(2, Optional.empty()), isLogUpToDate));
        assertFalse(state.canGrantVote(ReplicaKey.of(3, Optional.empty()), isLogUpToDate));
    }

    @Test
    public void testElectionState() {
        VoterSet voters = voterSetWithLocal(Arrays.asList(1, 2, 3));
        CandidateState state = newCandidateState(voters);
        assertEquals(
            ElectionState.withVotedCandidate(
                epoch,
                localNode.voterKey(),
                voters.voterIds()
            ),
            state.election()
        );
    }

    @Test
    public void testInvalidVoterSet() {
        assertThrows(
            IllegalArgumentException.class,
            () -> newCandidateState(VoterSetTest.voterSet(VoterSetTest.voterMap(Arrays.asList(1, 2, 3), true)))
        );
    }

    private VoterSet voterSetWithLocal(Collection<Integer> remoteVoters) {
        Map<Integer, VoterSet.VoterNode> voterMap = VoterSetTest.voterMap(remoteVoters, true);
        voterMap.put(localNode.voterKey().id(), localNode);

        return VoterSetTest.voterSet(voterMap);
    }
}
