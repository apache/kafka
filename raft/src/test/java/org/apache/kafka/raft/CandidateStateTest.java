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
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CandidateStateTest {
    private final ReplicaKey localReplicaKey = ReplicaKey.of(0, Uuid.randomUuid());
    private final int epoch = 5;
    private final MockTime time = new MockTime();
    private final int electionTimeoutMs = 5000;
    private final LogContext logContext = new LogContext();

    private CandidateState newCandidateState(VoterSet voters) {
        return new CandidateState(
                time,
                localReplicaKey.id(),
                localReplicaKey.directoryId().get(),
                epoch,
                voters,
                Optional.empty(),
                0,
                electionTimeoutMs,
                logContext
        );
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testSingleNodeQuorum(boolean withDirectoryId) {
        CandidateState state = newCandidateState(voterSetWithLocal(IntStream.empty(), withDirectoryId));
        assertTrue(state.isVoteGranted());
        assertFalse(state.isVoteRejected());
        assertEquals(Collections.emptySet(), state.unrecordedVoters());
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testTwoNodeQuorumVoteRejected(boolean withDirectoryId) {
        ReplicaKey otherNode = replicaKey(1, withDirectoryId);
        CandidateState state = newCandidateState(
            voterSetWithLocal(Stream.of(otherNode), withDirectoryId)
        );
        assertFalse(state.isVoteGranted());
        assertFalse(state.isVoteRejected());
        assertEquals(Collections.singleton(otherNode), state.unrecordedVoters());
        assertTrue(state.recordRejectedVote(otherNode.id()));
        assertFalse(state.isVoteGranted());
        assertTrue(state.isVoteRejected());
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testTwoNodeQuorumVoteGranted(boolean withDirectoryId) {
        ReplicaKey otherNode = replicaKey(1, withDirectoryId);
        CandidateState state = newCandidateState(
            voterSetWithLocal(Stream.of(otherNode), withDirectoryId)
        );
        assertFalse(state.isVoteGranted());
        assertFalse(state.isVoteRejected());
        assertEquals(Collections.singleton(otherNode), state.unrecordedVoters());
        assertTrue(state.recordGrantedVote(otherNode.id()));
        assertEquals(Collections.emptySet(), state.unrecordedVoters());
        assertFalse(state.isVoteRejected());
        assertTrue(state.isVoteGranted());
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testThreeNodeQuorumVoteGranted(boolean withDirectoryId) {
        ReplicaKey node1 = replicaKey(1, withDirectoryId);
        ReplicaKey node2 = replicaKey(2, withDirectoryId);
        CandidateState state = newCandidateState(
            voterSetWithLocal(Stream.of(node1, node2), withDirectoryId)
        );
        assertFalse(state.isVoteGranted());
        assertFalse(state.isVoteRejected());
        assertEquals(Set.of(node1, node2), state.unrecordedVoters());
        assertTrue(state.recordGrantedVote(node1.id()));
        assertEquals(Collections.singleton(node2), state.unrecordedVoters());
        assertTrue(state.isVoteGranted());
        assertFalse(state.isVoteRejected());
        assertTrue(state.recordRejectedVote(node2.id()));
        assertEquals(Collections.emptySet(), state.unrecordedVoters());
        assertTrue(state.isVoteGranted());
        assertFalse(state.isVoteRejected());
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testThreeNodeQuorumVoteRejected(boolean withDirectoryId) {
        ReplicaKey node1 = replicaKey(1, withDirectoryId);
        ReplicaKey node2 = replicaKey(2, withDirectoryId);
        CandidateState state = newCandidateState(
            voterSetWithLocal(Stream.of(node1, node2), withDirectoryId)
        );
        assertFalse(state.isVoteGranted());
        assertFalse(state.isVoteRejected());
        assertEquals(Set.of(node1, node2), state.unrecordedVoters());
        assertTrue(state.recordRejectedVote(node1.id()));
        assertEquals(Collections.singleton(node2), state.unrecordedVoters());
        assertFalse(state.isVoteGranted());
        assertFalse(state.isVoteRejected());
        assertTrue(state.recordRejectedVote(node2.id()));
        assertEquals(Collections.emptySet(), state.unrecordedVoters());
        assertFalse(state.isVoteGranted());
        assertTrue(state.isVoteRejected());
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testCannotRejectVoteFromLocalId(boolean withDirectoryId) {
        int otherNodeId = 1;
        CandidateState state = newCandidateState(
            voterSetWithLocal(IntStream.of(otherNodeId), withDirectoryId)
        );
        assertThrows(
            IllegalArgumentException.class,
            () -> state.recordRejectedVote(localReplicaKey.id())
        );
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testCannotChangeVoteGrantedToRejected(boolean withDirectoryId) {
        int otherNodeId = 1;
        CandidateState state = newCandidateState(
            voterSetWithLocal(IntStream.of(otherNodeId), withDirectoryId)
        );
        assertTrue(state.recordGrantedVote(otherNodeId));
        assertThrows(IllegalArgumentException.class, () -> state.recordRejectedVote(otherNodeId));
        assertTrue(state.isVoteGranted());
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testCannotChangeVoteRejectedToGranted(boolean withDirectoryId) {
        int otherNodeId = 1;
        CandidateState state = newCandidateState(
            voterSetWithLocal(IntStream.of(otherNodeId), withDirectoryId)
        );
        assertTrue(state.recordRejectedVote(otherNodeId));
        assertThrows(IllegalArgumentException.class, () -> state.recordGrantedVote(otherNodeId));
        assertTrue(state.isVoteRejected());
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testCannotGrantOrRejectNonVoters(boolean withDirectoryId) {
        int nonVoterId = 1;
        CandidateState state = newCandidateState(voterSetWithLocal(IntStream.empty(), withDirectoryId));
        assertThrows(IllegalArgumentException.class, () -> state.recordGrantedVote(nonVoterId));
        assertThrows(IllegalArgumentException.class, () -> state.recordRejectedVote(nonVoterId));
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testIdempotentGrant(boolean withDirectoryId) {
        int otherNodeId = 1;
        CandidateState state = newCandidateState(
            voterSetWithLocal(IntStream.of(otherNodeId), withDirectoryId)
        );
        assertTrue(state.recordGrantedVote(otherNodeId));
        assertFalse(state.recordGrantedVote(otherNodeId));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testIdempotentReject(boolean withDirectoryId) {
        int otherNodeId = 1;
        CandidateState state = newCandidateState(
            voterSetWithLocal(IntStream.of(otherNodeId), withDirectoryId)
        );
        assertTrue(state.recordRejectedVote(otherNodeId));
        assertFalse(state.recordRejectedVote(otherNodeId));
    }

    @ParameterizedTest
    @CsvSource({ "true,true", "true,false", "false,true", "false,false" })
    public void testGrantVote(boolean isLogUpToDate, boolean withDirectoryId) {
        ReplicaKey node0 = replicaKey(0, withDirectoryId);
        ReplicaKey node1 = replicaKey(1, withDirectoryId);
        ReplicaKey node2 = replicaKey(2, withDirectoryId);
        ReplicaKey node3 = replicaKey(3, withDirectoryId);

        CandidateState state = newCandidateState(
            voterSetWithLocal(Stream.of(node1, node2, node3), withDirectoryId)
        );

        assertEquals(isLogUpToDate, state.canGrantVote(node0, isLogUpToDate, true));
        assertEquals(isLogUpToDate, state.canGrantVote(node1, isLogUpToDate, true));
        assertEquals(isLogUpToDate, state.canGrantVote(node2, isLogUpToDate, true));
        assertEquals(isLogUpToDate, state.canGrantVote(node3, isLogUpToDate, true));

        assertFalse(state.canGrantVote(node0, isLogUpToDate, false));
        assertFalse(state.canGrantVote(node1, isLogUpToDate, false));
        assertFalse(state.canGrantVote(node2, isLogUpToDate, false));
        assertFalse(state.canGrantVote(node3, isLogUpToDate, false));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testElectionState(boolean withDirectoryId) {
        VoterSet voters = voterSetWithLocal(IntStream.of(1, 2, 3), withDirectoryId);
        CandidateState state = newCandidateState(voters);
        assertEquals(
            ElectionState.withVotedCandidate(
                epoch,
                localReplicaKey,
                voters.voterIds()
            ),
            state.election()
        );
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testInvalidVoterSet(boolean withDirectoryId) {
        assertThrows(
            IllegalArgumentException.class,
            () -> newCandidateState(
                VoterSetTest.voterSet(VoterSetTest.voterMap(IntStream.of(1, 2, 3), withDirectoryId))
            )
        );
    }

    @Test
    void testLeaderEndpoints() {
        CandidateState state = newCandidateState(
            voterSetWithLocal(IntStream.of(1, 2, 3), true)
        );

        assertEquals(Endpoints.empty(), state.leaderEndpoints());
    }

    private ReplicaKey replicaKey(int id, boolean withDirectoryId) {
        Uuid directoryId = withDirectoryId ? Uuid.randomUuid() : ReplicaKey.NO_DIRECTORY_ID;
        return ReplicaKey.of(id, directoryId);
    }

    private VoterSet voterSetWithLocal(IntStream remoteVoterIds, boolean withDirectoryId) {
        Stream<ReplicaKey> remoteVoterKeys = remoteVoterIds
            .boxed()
            .map(id -> replicaKey(id, withDirectoryId));

        return voterSetWithLocal(remoteVoterKeys, withDirectoryId);
    }

    private VoterSet voterSetWithLocal(Stream<ReplicaKey> remoteVoterKeys, boolean withDirectoryId) {
        ReplicaKey actualLocalVoter = withDirectoryId ?
            localReplicaKey :
            ReplicaKey.of(localReplicaKey.id(), ReplicaKey.NO_DIRECTORY_ID);

        return VoterSetTest.voterSet(
            Stream.concat(Stream.of(actualLocalVoter), remoteVoterKeys)
        );
    }
}
