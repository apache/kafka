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
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ProspectiveStateTest {
    private final ReplicaKey localReplicaKey = ReplicaKey.of(0, Uuid.randomUuid());
    private final Endpoints leaderEndpoints = Endpoints.fromInetSocketAddresses(
        Collections.singletonMap(
            ListenerName.normalised("CONTROLLER"),
            InetSocketAddress.createUnresolved("mock-host-3", 1234)
        )
    );
    private final int epoch = 5;
    private final MockTime time = new MockTime();
    private final int electionTimeoutMs = 10000;
    private final LogContext logContext = new LogContext();
    private final int localId = 0;
    private final int votedId = 1;
    private final Uuid votedDirectoryId = Uuid.randomUuid();
    private final ReplicaKey votedKeyWithDirectoryId = ReplicaKey.of(votedId, votedDirectoryId);
    private final ReplicaKey votedKeyWithoutDirectoryId = ReplicaKey.of(votedId, ReplicaKey.NO_DIRECTORY_ID);

    private ProspectiveState newProspectiveState(
        VoterSet voters,
        OptionalInt leaderId,
        Optional<ReplicaKey> votedKey
    ) {
        return new ProspectiveState(
            time,
            localReplicaKey.id(),
            epoch,
            leaderId,
            leaderId.isPresent() ? leaderEndpoints : Endpoints.empty(),
            votedKey,
            voters,
            Optional.empty(),
            1,
            electionTimeoutMs,
            logContext
        );
    }

    private ProspectiveState newProspectiveState(VoterSet voters) {
        return new ProspectiveState(
            time,
            localReplicaKey.id(),
            epoch,
            OptionalInt.empty(),
            Endpoints.empty(),
            Optional.empty(),
            voters,
            Optional.empty(),
            1,
            electionTimeoutMs,
            logContext
        );
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testSingleNodeQuorum(boolean withDirectoryId) {
        ProspectiveState state = newProspectiveState(voterSetWithLocal(IntStream.empty(), withDirectoryId));
        assertTrue(state.epochElection().isVoteGranted());
        assertFalse(state.epochElection().isVoteRejected());
        assertEquals(Collections.emptySet(), state.epochElection().unrecordedVoters());
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testTwoNodeQuorumVoteRejected(boolean withDirectoryId) {
        ReplicaKey otherNode = replicaKey(1, withDirectoryId);
        ProspectiveState state = newProspectiveState(
            voterSetWithLocal(Stream.of(otherNode), withDirectoryId)
        );
        assertFalse(state.epochElection().isVoteGranted());
        assertFalse(state.epochElection().isVoteRejected());
        assertEquals(Collections.singleton(otherNode), state.epochElection().unrecordedVoters());
        assertTrue(state.recordRejectedVote(otherNode.id()));
        assertFalse(state.epochElection().isVoteGranted());
        assertTrue(state.epochElection().isVoteRejected());
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testTwoNodeQuorumVoteGranted(boolean withDirectoryId) {
        ReplicaKey otherNode = replicaKey(1, withDirectoryId);
        ProspectiveState state = newProspectiveState(
            voterSetWithLocal(Stream.of(otherNode), withDirectoryId)
        );
        assertFalse(state.epochElection().isVoteGranted());
        assertFalse(state.epochElection().isVoteRejected());
        assertEquals(Collections.singleton(otherNode), state.epochElection().unrecordedVoters());
        assertTrue(state.recordGrantedVote(otherNode.id()));
        assertEquals(Collections.emptySet(), state.epochElection().unrecordedVoters());
        assertFalse(state.epochElection().isVoteRejected());
        assertTrue(state.epochElection().isVoteGranted());
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testThreeNodeQuorumVoteGranted(boolean withDirectoryId) {
        ReplicaKey node1 = replicaKey(1, withDirectoryId);
        ReplicaKey node2 = replicaKey(2, withDirectoryId);
        ProspectiveState state = newProspectiveState(
            voterSetWithLocal(Stream.of(node1, node2), withDirectoryId)
        );
        assertFalse(state.epochElection().isVoteGranted());
        assertFalse(state.epochElection().isVoteRejected());
        assertEquals(Set.of(node1, node2), state.epochElection().unrecordedVoters());
        assertTrue(state.recordGrantedVote(node1.id()));
        assertEquals(Collections.singleton(node2), state.epochElection().unrecordedVoters());
        assertTrue(state.epochElection().isVoteGranted());
        assertFalse(state.epochElection().isVoteRejected());
        assertTrue(state.recordRejectedVote(node2.id()));
        assertEquals(Collections.emptySet(), state.epochElection().unrecordedVoters());
        assertTrue(state.epochElection().isVoteGranted());
        assertFalse(state.epochElection().isVoteRejected());
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testThreeNodeQuorumVoteRejected(boolean withDirectoryId) {
        ReplicaKey node1 = replicaKey(1, withDirectoryId);
        ReplicaKey node2 = replicaKey(2, withDirectoryId);
        ProspectiveState state = newProspectiveState(
            voterSetWithLocal(Stream.of(node1, node2), withDirectoryId)
        );
        assertFalse(state.epochElection().isVoteGranted());
        assertFalse(state.epochElection().isVoteRejected());
        assertEquals(Set.of(node1, node2), state.epochElection().unrecordedVoters());
        assertTrue(state.recordRejectedVote(node1.id()));
        assertEquals(Collections.singleton(node2), state.epochElection().unrecordedVoters());
        assertFalse(state.epochElection().isVoteGranted());
        assertFalse(state.epochElection().isVoteRejected());
        assertTrue(state.recordRejectedVote(node2.id()));
        assertEquals(Collections.emptySet(), state.epochElection().unrecordedVoters());
        assertFalse(state.epochElection().isVoteGranted());
        assertTrue(state.epochElection().isVoteRejected());
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testCanChangePreVote(boolean withDirectoryId) {
        int voter1 = 1;
        int voter2 = 2;
        ProspectiveState state = newProspectiveState(
            voterSetWithLocal(IntStream.of(voter1, voter2), withDirectoryId)
        );
        assertTrue(state.recordGrantedVote(voter1));
        assertTrue(state.epochElection().isVoteGranted());
        assertFalse(state.recordRejectedVote(voter1));
        assertFalse(state.epochElection().isVoteGranted());

        assertTrue(state.recordRejectedVote(voter2));
        assertTrue(state.epochElection().isVoteRejected());
        assertFalse(state.recordGrantedVote(voter2));
        assertFalse(state.epochElection().isVoteRejected());
    }


    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testCannotGrantOrRejectNonVoters(boolean withDirectoryId) {
        int nonVoterId = 1;
        ProspectiveState state = newProspectiveState(voterSetWithLocal(IntStream.empty(), withDirectoryId));
        assertThrows(IllegalArgumentException.class, () -> state.recordGrantedVote(nonVoterId));
        assertThrows(IllegalArgumentException.class, () -> state.recordRejectedVote(nonVoterId));
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testConsecutiveGrant(boolean withDirectoryId) {
        int otherNodeId = 1;
        ProspectiveState state = newProspectiveState(
            voterSetWithLocal(IntStream.of(otherNodeId), withDirectoryId)
        );
        assertTrue(state.recordGrantedVote(otherNodeId));
        assertFalse(state.recordGrantedVote(otherNodeId));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testConsecutiveReject(boolean withDirectoryId) {
        int otherNodeId = 1;
        ProspectiveState state = newProspectiveState(
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

        ProspectiveState state = newProspectiveState(
            voterSetWithLocal(Stream.of(node1, node2), withDirectoryId)
        );

        assertEquals(isLogUpToDate, state.canGrantVote(node0, isLogUpToDate, true));
        assertEquals(isLogUpToDate, state.canGrantVote(node1, isLogUpToDate, true));
        assertEquals(isLogUpToDate, state.canGrantVote(node2, isLogUpToDate, true));

        assertEquals(isLogUpToDate, state.canGrantVote(node0, isLogUpToDate, false));
        assertEquals(isLogUpToDate, state.canGrantVote(node1, isLogUpToDate, false));
        assertEquals(isLogUpToDate, state.canGrantVote(node2, isLogUpToDate, false));
    }

    @ParameterizedTest
    @CsvSource({ "true,true", "true,false", "false,true", "false,false" })
    public void testGrantVoteWithVotedKey(boolean isLogUpToDate, boolean withDirectoryId) {
        ReplicaKey node0 = replicaKey(0, withDirectoryId);
        ReplicaKey node1 = replicaKey(1, withDirectoryId);
        ReplicaKey node2 = replicaKey(2, withDirectoryId);

        ProspectiveState state = newProspectiveState(
            voterSetWithLocal(Stream.of(node1, node2), withDirectoryId),
            OptionalInt.empty(),
            Optional.of(node1)
        );

        assertEquals(isLogUpToDate, state.canGrantVote(node0, isLogUpToDate, true));
        assertEquals(isLogUpToDate, state.canGrantVote(node1, isLogUpToDate, true));
        assertEquals(isLogUpToDate, state.canGrantVote(node2, isLogUpToDate, true));

        assertFalse(state.canGrantVote(node0, isLogUpToDate, false));
        assertTrue(state.canGrantVote(node1, isLogUpToDate, false));
        assertFalse(state.canGrantVote(node2, isLogUpToDate, false));
    }

    @ParameterizedTest
    @CsvSource({ "true,true", "true,false", "false,true", "false,false" })
    public void testGrantVoteWithLeader(boolean isLogUpToDate, boolean withDirectoryId) {
        ReplicaKey node0 = replicaKey(0, withDirectoryId);
        ReplicaKey node1 = replicaKey(1, withDirectoryId);
        ReplicaKey node2 = replicaKey(2, withDirectoryId);

        ProspectiveState state = newProspectiveState(
            voterSetWithLocal(Stream.of(node1, node2), withDirectoryId),
            OptionalInt.of(node1.id()),
            Optional.empty()
        );

        assertEquals(isLogUpToDate, state.canGrantVote(node0, isLogUpToDate, true));
        assertEquals(isLogUpToDate, state.canGrantVote(node1, isLogUpToDate, true));
        assertEquals(isLogUpToDate, state.canGrantVote(node2, isLogUpToDate, true));

        assertFalse(state.canGrantVote(node0, isLogUpToDate, false));
        assertFalse(state.canGrantVote(node1, isLogUpToDate, false));
        assertFalse(state.canGrantVote(node2, isLogUpToDate, false));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testElectionState(boolean withDirectoryId) {
        VoterSet voters = voterSetWithLocal(IntStream.of(1, 2, 3), withDirectoryId);
        ProspectiveState state = newProspectiveState(voters);
        assertEquals(
            ElectionState.withUnknownLeader(
                epoch,
                voters.voterIds()
            ),
            state.election()
        );

        // with leader
        state = newProspectiveState(voters, OptionalInt.of(1), Optional.empty());
        assertEquals(
            ElectionState.withElectedLeader(
                epoch,
                1,
                Optional.empty(), voters.voterIds()
            ),
            state.election()
        );

        // with voted key
        ReplicaKey votedKey = replicaKey(1, withDirectoryId);
        state = newProspectiveState(voters, OptionalInt.empty(), Optional.of(votedKey));
        assertEquals(
            ElectionState.withVotedCandidate(
                epoch,
                votedKey,
                voters.voterIds()
            ),
            state.election()
        );

        // with both
        state = newProspectiveState(voters, OptionalInt.of(1), Optional.of(votedKey));
        assertEquals(
            ElectionState.withElectedLeader(
                epoch,
                1,
                Optional.of(votedKey),
                voters.voterIds()
            ),
            state.election()
        );
    }

    @Test
    public void testElectionTimeout() {
        ProspectiveState state = newProspectiveState(
            voterSetWithLocal(IntStream.empty(), true),
            OptionalInt.empty(),
            Optional.of(votedKeyWithDirectoryId)
        );

        assertEquals(epoch, state.epoch());
        assertEquals(votedKeyWithDirectoryId, state.votedKey().get());
        assertEquals(
            ElectionState.withVotedCandidate(epoch, votedKeyWithDirectoryId, Collections.singleton(localId)),
            state.election()
        );
        assertEquals(electionTimeoutMs, state.remainingElectionTimeMs(time.milliseconds()));
        assertFalse(state.hasElectionTimeoutExpired(time.milliseconds()));

        time.sleep(5000);
        assertEquals(electionTimeoutMs - 5000, state.remainingElectionTimeMs(time.milliseconds()));
        assertFalse(state.hasElectionTimeoutExpired(time.milliseconds()));

        time.sleep(5000);
        assertEquals(0, state.remainingElectionTimeMs(time.milliseconds()));
        assertTrue(state.hasElectionTimeoutExpired(time.milliseconds()));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testCanGrantVoteWithoutDirectoryId(boolean isLogUpToDate) {
        ProspectiveState state = newProspectiveState(
            voterSetWithLocal(IntStream.empty(), true),
            OptionalInt.empty(),
            Optional.of(votedKeyWithoutDirectoryId));

        assertEquals(
            isLogUpToDate,
            state.canGrantVote(ReplicaKey.of(votedId, ReplicaKey.NO_DIRECTORY_ID), isLogUpToDate, true)
        );
        assertTrue(state.canGrantVote(ReplicaKey.of(votedId, ReplicaKey.NO_DIRECTORY_ID), isLogUpToDate, false));

        assertEquals(
            isLogUpToDate,
            state.canGrantVote(ReplicaKey.of(votedId, Uuid.randomUuid()), isLogUpToDate, true)
        );
        assertTrue(state.canGrantVote(ReplicaKey.of(votedId, Uuid.randomUuid()), isLogUpToDate, false));

        // Can grant PreVote to other replicas even if we have granted a standard vote to another replica
        assertEquals(
            isLogUpToDate,
            state.canGrantVote(ReplicaKey.of(votedId + 1, ReplicaKey.NO_DIRECTORY_ID), isLogUpToDate, true)
        );
        assertFalse(state.canGrantVote(ReplicaKey.of(votedId + 1, ReplicaKey.NO_DIRECTORY_ID), isLogUpToDate, false));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testCanGrantVoteWithDirectoryId(boolean isLogUpToDate) {
        ProspectiveState state = newProspectiveState(
            voterSetWithLocal(IntStream.empty(), true),
            OptionalInt.empty(),
            Optional.of(votedKeyWithDirectoryId));

        // Same voterKey
        // We will not grant PreVote for a replica we have already granted a standard vote to if their log is behind
        assertEquals(
            isLogUpToDate,
            state.canGrantVote(votedKeyWithDirectoryId, isLogUpToDate, true)
        );
        assertTrue(state.canGrantVote(votedKeyWithDirectoryId, isLogUpToDate, false));

        // Different directoryId
        // We can grant PreVote for a replica we have already granted a standard vote to if their log is up-to-date,
        // even if the directoryId is different
        assertEquals(
            isLogUpToDate,
            state.canGrantVote(ReplicaKey.of(votedId, Uuid.randomUuid()), isLogUpToDate, true)
        );
        assertFalse(state.canGrantVote(ReplicaKey.of(votedId, Uuid.randomUuid()), isLogUpToDate, false));

        // Missing directoryId
        assertEquals(
            isLogUpToDate,
            state.canGrantVote(ReplicaKey.of(votedId, ReplicaKey.NO_DIRECTORY_ID), isLogUpToDate, true)
        );
        assertFalse(state.canGrantVote(ReplicaKey.of(votedId, ReplicaKey.NO_DIRECTORY_ID), isLogUpToDate, false));

        // Different voterId
        assertEquals(
            isLogUpToDate,
            state.canGrantVote(ReplicaKey.of(votedId + 1, votedDirectoryId), isLogUpToDate, true)
        );
        assertEquals(
            isLogUpToDate,
            state.canGrantVote(ReplicaKey.of(votedId + 1, ReplicaKey.NO_DIRECTORY_ID), isLogUpToDate, true)
        );
        assertFalse(state.canGrantVote(ReplicaKey.of(votedId + 1, votedDirectoryId), true, false));
        assertFalse(state.canGrantVote(ReplicaKey.of(votedId + 1, ReplicaKey.NO_DIRECTORY_ID), true, false));
    }

    @Test
    public void testLeaderEndpoints() {
        ProspectiveState state = newProspectiveState(
            voterSetWithLocal(IntStream.of(1, 2, 3), true),
            OptionalInt.empty(),
            Optional.of(ReplicaKey.of(1, Uuid.randomUuid()))
        );
        assertEquals(Endpoints.empty(), state.leaderEndpoints());

        state = newProspectiveState(
            voterSetWithLocal(IntStream.of(1, 2, 3), true),
            OptionalInt.of(3),
            Optional.of(ReplicaKey.of(1, Uuid.randomUuid()))
        );
        assertEquals(leaderEndpoints, state.leaderEndpoints());
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
