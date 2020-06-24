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
import org.apache.kafka.common.utils.Utils;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class QuorumStateTest {
    private final int localId = 0;
    private final int logEndEpoch = 0;
    private final MockQuorumStateStore store = new MockQuorumStateStore();

    @Test
    public void testInitializePrimordialEpoch() throws IOException {
        Set<Integer> voters = Utils.mkSet(localId);
        assertNull(store.readElectionState());

        QuorumState state = initializeEmptyState(voters, store);
        assertTrue(state.isFollower());
        assertEquals(0, state.epoch());
        state.becomeCandidate();
        CandidateState candidateState = state.candidateStateOrThrow();
        assertTrue(candidateState.isVoteGranted());
        assertEquals(1, candidateState.epoch());
    }

    @Test
    public void testInitializeAsFollowerWithElectedLeader() throws IOException {
        int node1 = 1;
        int node2 = 2;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(localId, node1, node2);
        store.writeElectionState(ElectionState.withElectedLeader(epoch, node1, voters));

        QuorumState state = new QuorumState(localId, voters, store, new LogContext());
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        assertTrue(state.isFollower());
        assertEquals(epoch, state.epoch());

        FollowerState followerState = state.followerStateOrThrow();
        assertTrue(followerState.hasLeader());
        assertEquals(epoch, followerState.epoch());
        assertEquals(node1, followerState.leaderId());
    }

    @Test
    public void testInitializeAsFollowerWithVotedCandidate() throws IOException {
        int node1 = 1;
        int node2 = 2;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(localId, node1, node2);
        store.writeElectionState(ElectionState.withVotedCandidate(epoch, node1, voters));

        QuorumState state = new QuorumState(localId, voters, store, new LogContext());

        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        assertTrue(state.isFollower());
        assertEquals(epoch, state.epoch());

        FollowerState followerState = state.followerStateOrThrow();
        assertTrue(followerState.hasVoted());
        assertEquals(epoch, followerState.epoch());
        assertTrue(followerState.hasVotedFor(node1));
    }

    @Test
    public void testInitializeAsFormerCandidate() throws IOException {
        int node1 = 1;
        int node2 = 2;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(localId, node1, node2);
        store.writeElectionState(ElectionState.withVotedCandidate(epoch, localId, voters));

        QuorumState state = new QuorumState(localId, voters, store, new LogContext());
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        assertTrue(state.isCandidate());
        assertEquals(epoch, state.epoch());

        CandidateState candidateState = state.candidateStateOrThrow();
        assertEquals(epoch, candidateState.epoch());
        assertEquals(Utils.mkSet(node1, node2), candidateState.unrecordedVoters());
    }

    @Test
    public void testInitializeAsFormerLeader() throws IOException {
        int node1 = 1;
        int node2 = 2;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(localId, node1, node2);
        store.writeElectionState(ElectionState.withElectedLeader(epoch, localId, voters));

        QuorumState state = new QuorumState(localId, voters, store, new LogContext());
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        assertTrue(state.isLeader());
        assertEquals(epoch, state.epoch());

        LeaderState leaderState = state.leaderStateOrThrow();
        assertEquals(epoch, leaderState.epoch());
        assertEquals(Utils.mkSet(node1, node2), leaderState.nonEndorsingFollowers());
    }

    @Test
    public void testBecomeLeader() throws IOException {
        Set<Integer> voters = Utils.mkSet(localId);
        assertNull(store.readElectionState());

        QuorumState state = initializeEmptyState(voters, store);
        state.becomeCandidate();
        assertTrue(state.isCandidate());

        LeaderState leaderState = state.becomeLeader(0L);
        assertTrue(state.isLeader());
        assertEquals(1, leaderState.epoch());
        assertEquals(Optional.empty(), leaderState.highWatermark());
    }

    @Test
    public void testCannotBecomeLeaderIfAlreadyLeader() throws IOException {
        Set<Integer> voters = Utils.mkSet(localId);
        assertNull(store.readElectionState());

        store.writeElectionState(ElectionState.withUnknownLeader(1, voters));

        QuorumState state = new QuorumState(localId, voters, store, new LogContext());
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.becomeCandidate();
        state.becomeLeader(0L);
        assertTrue(state.isLeader());
        assertThrows(IllegalStateException.class, () -> state.becomeLeader(0L));
        assertTrue(state.isLeader());
    }

    @Test
    public void testCannotBecomeFollowerOfSelf() throws IOException {
        Set<Integer> voters = Utils.mkSet(localId);
        assertNull(store.readElectionState());
        QuorumState state = initializeEmptyState(voters, store);

        assertThrows(IllegalArgumentException.class, () -> state.becomeFetchingFollower(0, localId));
        assertThrows(IllegalArgumentException.class, () -> state.becomeVotedFollower(0, localId));
    }

    @Test
    public void testCannotBecomeLeaderIfCurrentlyFollowing() throws IOException {
        int leaderId = 1;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(localId, leaderId);
        store.writeElectionState(ElectionState.withVotedCandidate(epoch, leaderId, voters));
        QuorumState state = initializeEmptyState(voters, store);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        assertTrue(state.isFollower());
        assertThrows(IllegalStateException.class, () -> state.becomeLeader(0L));
    }

    @Test
    public void testCannotBecomeCandidateIfCurrentlyLeading() throws IOException {
        Set<Integer> voters = Utils.mkSet(localId);
        QuorumState state = initializeEmptyState(voters, store);
        state.becomeCandidate();
        state.becomeLeader(0L);
        assertTrue(state.isLeader());
        assertThrows(IllegalStateException.class, state::becomeCandidate);
    }

    @Test
    public void testCannotBecomeLeaderWithoutGrantedVote() throws IOException {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = initializeEmptyState(voters, store);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.becomeCandidate();
        assertFalse(state.candidateStateOrThrow().isVoteGranted());
        assertThrows(IllegalStateException.class, () -> state.becomeLeader(0L));
        state.candidateStateOrThrow().recordGrantedVote(otherNodeId);
        assertTrue(state.candidateStateOrThrow().isVoteGranted());
        state.becomeLeader(0L);
        assertTrue(state.isLeader());
    }

    @Test
    public void testLeaderToFollowerOfElectedLeader() throws IOException {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);

        QuorumState state = initializeEmptyState(voters, store);

        state.becomeCandidate();
        state.candidateStateOrThrow().recordGrantedVote(otherNodeId);
        state.becomeLeader(0L);
        assertTrue(state.becomeFetchingFollower(5, otherNodeId));
        assertEquals(5, state.epoch());
        assertEquals(OptionalInt.of(otherNodeId), state.leaderId());
        assertEquals(ElectionState.withElectedLeader(5, otherNodeId, voters), store.readElectionState());
    }

    private QuorumState initializeEmptyState(Set<Integer> voters,
                                             MockQuorumStateStore store) throws IOException {
        QuorumState state = new QuorumState(localId, voters, store, new LogContext());
        store.writeElectionState(ElectionState.withUnknownLeader(0, voters));
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        return state;
    }

    @Test
    public void testLeaderToUnattachedFollower() throws IOException {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = initializeEmptyState(voters, store);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.becomeCandidate();
        state.candidateStateOrThrow().recordGrantedVote(otherNodeId);
        state.becomeLeader(0L);
        assertTrue(state.becomeUnattachedFollower(5));
        assertEquals(5, state.epoch());
        assertEquals(OptionalInt.empty(), state.leaderId());
        assertEquals(ElectionState.withUnknownLeader(5, voters), store.readElectionState());
    }

    @Test
    public void testLeaderToFollowerOfVotedCandidate() throws IOException {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = initializeEmptyState(voters, store);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.becomeCandidate();
        state.candidateStateOrThrow().recordGrantedVote(otherNodeId);
        state.becomeLeader(0L);
        assertTrue(state.becomeVotedFollower(5, otherNodeId));
        assertEquals(5, state.epoch());
        assertEquals(OptionalInt.empty(), state.leaderId());
        FollowerState followerState = state.followerStateOrThrow();
        assertTrue(followerState.hasVoted());
        assertTrue(followerState.hasVotedFor(otherNodeId));
        assertEquals(ElectionState.withVotedCandidate(5, otherNodeId, voters), store.readElectionState());
    }

    @Test
    public void testCandidateToFollowerOfElectedLeader() throws IOException {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = initializeEmptyState(voters, store);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        assertTrue(state.becomeFetchingFollower(5, otherNodeId));
        assertEquals(5, state.epoch());
        assertEquals(OptionalInt.of(otherNodeId), state.leaderId());
        assertEquals(ElectionState.withElectedLeader(5, otherNodeId, voters), store.readElectionState());
    }

    @Test
    public void testCandidateToUnattachedFollower() throws IOException {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = initializeEmptyState(voters, store);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        assertTrue(state.becomeUnattachedFollower(5));
        assertEquals(5, state.epoch());
        assertEquals(OptionalInt.empty(), state.leaderId());
        assertEquals(ElectionState.withUnknownLeader(5, voters), store.readElectionState());
    }

    @Test
    public void testCandidateToFollowerOfVotedCandidate() throws IOException {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = initializeEmptyState(voters, store);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        assertTrue(state.becomeVotedFollower(5, otherNodeId));
        assertEquals(5, state.epoch());
        assertEquals(OptionalInt.empty(), state.leaderId());
        FollowerState followerState = state.followerStateOrThrow();
        assertTrue(followerState.hasVoted());
        assertTrue(followerState.hasVotedFor(otherNodeId));
        assertEquals(ElectionState.withVotedCandidate(5, otherNodeId, voters), store.readElectionState());
    }

    @Test
    public void testUnattachedFollowerToFollowerOfVotedCandidateSameEpoch() throws IOException {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = initializeEmptyState(voters, store);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.becomeUnattachedFollower(5);
        state.becomeVotedFollower(5, otherNodeId);
        FollowerState followerState = state.followerStateOrThrow();
        assertEquals(5, followerState.epoch());
        assertTrue(followerState.hasVoted());
        assertTrue(followerState.hasVotedFor(otherNodeId));
        assertEquals(ElectionState.withVotedCandidate(5, otherNodeId, voters), store.readElectionState());
    }

    @Test
    public void testUnattachedFollowerToFollowerOfVotedCandidateHigherEpoch() throws IOException {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = initializeEmptyState(voters, store);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.becomeUnattachedFollower(5);
        state.becomeVotedFollower(8, otherNodeId);
        FollowerState followerState = state.followerStateOrThrow();
        assertEquals(8, followerState.epoch());
        assertTrue(followerState.hasVoted());
        assertTrue(followerState.hasVotedFor(otherNodeId));
        assertEquals(ElectionState.withVotedCandidate(8, otherNodeId, voters), store.readElectionState());
    }

    @Test
    public void testVotedFollowerToFollowerOfElectedLeaderSameEpoch() throws IOException {
        int node1 = 1;
        int node2 = 2;
        Set<Integer> voters = Utils.mkSet(localId, node1, node2);
        QuorumState state = initializeEmptyState(voters, store);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.becomeVotedFollower(5, node1);
        state.becomeFetchingFollower(5, node2);
        FollowerState followerState = state.followerStateOrThrow();
        assertEquals(5, followerState.epoch());
        assertTrue(followerState.hasLeader());
        assertFalse(followerState.hasVoted());
        assertEquals(node2, followerState.leaderId());
        assertEquals(ElectionState.withElectedLeader(5, node2, voters), store.readElectionState());
    }

    @Test
    public void testVotedFollowerToFollowerOfElectedLeaderHigherEpoch() throws IOException {
        int node1 = 1;
        int node2 = 2;
        Set<Integer> voters = Utils.mkSet(localId, node1, node2);
        QuorumState state = initializeEmptyState(voters, store);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.becomeVotedFollower(5, node1);
        state.becomeFetchingFollower(8, node2);
        FollowerState followerState = state.followerStateOrThrow();
        assertEquals(8, followerState.epoch());
        assertTrue(followerState.hasLeader());
        assertFalse(followerState.hasVoted());
        assertEquals(node2, followerState.leaderId());
        assertEquals(ElectionState.withElectedLeader(8, node2, voters), store.readElectionState());
    }

    @Test
    public void testFollowerCannotChangeVotesInSameEpoch() throws IOException {
        int node1 = 1;
        int node2 = 2;
        Set<Integer> voters = Utils.mkSet(localId, node1, node2);
        QuorumState state = initializeEmptyState(voters, store);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.becomeVotedFollower(5, node1);
        assertThrows(IllegalArgumentException.class, () -> state.becomeVotedFollower(5, node2));
        FollowerState followerState = state.followerStateOrThrow();
        assertFalse(followerState.hasLeader());
        assertTrue(followerState.hasVoted());
        assertTrue(followerState.hasVotedFor(node1));
        assertEquals(ElectionState.withVotedCandidate(5, node1, voters), store.readElectionState());
    }

    @Test
    public void testFollowerCannotChangeLeadersInSameEpoch() throws IOException {
        int node1 = 1;
        int node2 = 2;
        Set<Integer> voters = Utils.mkSet(localId, node1, node2);
        QuorumState state = initializeEmptyState(voters, store);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.becomeFetchingFollower(8, node2);
        assertThrows(IllegalArgumentException.class, () -> state.becomeFetchingFollower(8, node1));
        FollowerState followerState = state.followerStateOrThrow();
        assertEquals(8, followerState.epoch());
        assertTrue(followerState.hasLeader());
        assertFalse(followerState.hasVoted());
        assertEquals(node2, followerState.leaderId());
        assertEquals(ElectionState.withElectedLeader(8, node2, voters), store.readElectionState());
    }

    @Test
    public void testFollowerOfElectedLeaderHigherEpoch() throws IOException {
        int node1 = 1;
        int node2 = 2;
        Set<Integer> voters = Utils.mkSet(localId, node1, node2);
        QuorumState state = initializeEmptyState(voters, store);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.becomeFetchingFollower(8, node2);
        assertThrows(IllegalArgumentException.class, () -> state.becomeFetchingFollower(8, node1));
        FollowerState followerState = state.followerStateOrThrow();
        assertEquals(8, followerState.epoch());
        assertTrue(followerState.hasLeader());
        assertFalse(followerState.hasVoted());
        assertEquals(node2, followerState.leaderId());
        assertEquals(ElectionState.withElectedLeader(8, node2, voters), store.readElectionState());
    }

    @Test
    public void testCannotTransitionFromFollowerToLowerEpoch() throws IOException {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = initializeEmptyState(voters, store);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.becomeUnattachedFollower(5);
        assertThrows(IllegalArgumentException.class, () -> state.becomeUnattachedFollower(4));
        assertThrows(IllegalArgumentException.class, () -> state.becomeVotedFollower(4, otherNodeId));
        assertThrows(IllegalArgumentException.class, () -> state.becomeFetchingFollower(4, otherNodeId));
        assertEquals(5, state.epoch());
        assertEquals(ElectionState.withUnknownLeader(5, voters), store.readElectionState());
    }

    @Test
    public void testCannotTransitionFromCandidateToLowerEpoch() throws IOException {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = initializeEmptyState(voters, store);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.becomeUnattachedFollower(5);
        state.becomeCandidate();
        assertThrows(IllegalArgumentException.class, () -> state.becomeUnattachedFollower(4));
        assertThrows(IllegalArgumentException.class, () -> state.becomeVotedFollower(4, otherNodeId));
        assertThrows(IllegalArgumentException.class, () -> state.becomeFetchingFollower(4, otherNodeId));
        assertEquals(6, state.epoch());
        assertEquals(ElectionState.withVotedCandidate(6, localId, voters), store.readElectionState());
    }

    @Test
    public void testCannotTransitionFromLeaderToLowerEpoch() throws IOException {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = initializeEmptyState(voters, store);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.becomeUnattachedFollower(5);
        state.becomeCandidate();
        state.candidateStateOrThrow().recordGrantedVote(otherNodeId);
        state.becomeLeader(0L);
        assertThrows(IllegalArgumentException.class, () -> state.becomeUnattachedFollower(4));
        assertThrows(IllegalArgumentException.class, () -> state.becomeVotedFollower(4, otherNodeId));
        assertThrows(IllegalArgumentException.class, () -> state.becomeFetchingFollower(4, otherNodeId));
        assertEquals(6, state.epoch());
        assertEquals(ElectionState.withElectedLeader(6, localId, voters), store.readElectionState());
    }

    @Test
    public void testCannotBecomeFollowerOfNonVoter() throws IOException {
        int otherNodeId = 1;
        int nonVoterId = 2;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = initializeEmptyState(voters, store);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        assertThrows(IllegalArgumentException.class, () -> state.becomeVotedFollower(4, nonVoterId));
        assertThrows(IllegalArgumentException.class, () -> state.becomeFetchingFollower(4, nonVoterId));
    }

    @Test
    public void testObserverCannotBecomeCandidateCandidateOrLeader() throws IOException {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(otherNodeId);
        QuorumState state = initializeEmptyState(voters, store);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        assertTrue(state.isObserver());
        assertTrue(state.isFollower());
        assertThrows(IllegalStateException.class, state::becomeCandidate);
        assertThrows(IllegalStateException.class, () -> state.becomeLeader(0L));
    }

    @Test
    public void testObserverDetachLeader() throws IOException {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(otherNodeId);
        QuorumState state = initializeEmptyState(voters, store);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        assertTrue(state.isObserver());
        assertTrue(state.isFollower());
        state.becomeFetchingFollower(1, otherNodeId);
        assertEquals(1, state.epoch());
        // If we disconnect from the leader, we may become an unattached follower with the
        // current epoch so that we can discover the new leader.
        state.becomeUnattachedFollower(1);
        assertEquals(1, state.epoch());
    }

    @Test
    public void testInitializeWithCorruptedStore() throws IOException {
        QuorumStateStore stateStore = Mockito.mock(QuorumStateStore.class);
        Mockito.doThrow(IOException.class).when(stateStore).readElectionState();
        QuorumState state = new QuorumState(localId, Utils.mkSet(1), stateStore, new LogContext());

        int epoch = 2;
        state.initialize(new OffsetAndEpoch(0L, epoch));
        assertEquals(epoch, state.epoch());
        assertTrue(state.isFollower());
        assertFalse(state.hasLeader());
    }

    @Test
    public void testInconsistentVotersBetweenConfigAndState() throws IOException {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);

        QuorumState state = initializeEmptyState(voters, store);

        int unknownVoterId = 2;
        Set<Integer> stateVoters = Utils.mkSet(localId, otherNodeId, unknownVoterId);

        int epoch = 5;
        store.writeElectionState(ElectionState.withElectedLeader(epoch, localId, stateVoters));
        assertThrows(IllegalStateException.class,
            () -> state.initialize(new OffsetAndEpoch(0L, logEndEpoch)));
    }

    @Test
    public void testHasRemoteLeader() throws IOException {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);

        QuorumState state = initializeEmptyState(voters, store);
        assertFalse(state.hasRemoteLeader());

        state.becomeCandidate();
        assertFalse(state.hasRemoteLeader());

        state.candidateStateOrThrow().recordGrantedVote(otherNodeId);
        state.becomeLeader(0L);
        assertFalse(state.hasRemoteLeader());

        state.becomeUnattachedFollower(state.epoch() + 1);
        assertFalse(state.hasRemoteLeader());

        state.becomeVotedFollower(state.epoch() + 1, otherNodeId);
        assertFalse(state.hasRemoteLeader());

        state.becomeFetchingFollower(state.epoch() + 1, otherNodeId);
        assertTrue(state.hasRemoteLeader());
    }

}
