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
import org.apache.kafka.raft.internals.BatchAccumulator;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class QuorumStateTest {
    private final int localId = 0;
    private final int logEndEpoch = 0;
    private final MockQuorumStateStore store = new MockQuorumStateStore();
    private final MockTime time = new MockTime();
    private final int electionTimeoutMs = 5000;
    private final int fetchTimeoutMs = 10000;
    private final MockableRandom random = new MockableRandom(1L);

    private BatchAccumulator<?> accumulator = Mockito.mock(BatchAccumulator.class);

    private QuorumState buildQuorumState(Set<Integer> voters) {
        return buildQuorumState(OptionalInt.of(localId), voters);
    }

    private QuorumState buildQuorumState(
        OptionalInt localId,
        Set<Integer> voters
    ) {
        return new QuorumState(
            localId,
            voters,
            electionTimeoutMs,
            fetchTimeoutMs,
            store,
            time,
            new LogContext(),
            random
        );
    }

    @Test
    public void testInitializePrimordialEpoch() throws IOException {
        Set<Integer> voters = Utils.mkSet(localId);
        assertNull(store.readElectionState());

        QuorumState state = initializeEmptyState(voters);
        assertTrue(state.isUnattached());
        assertEquals(0, state.epoch());
        state.transitionToCandidate();
        CandidateState candidateState = state.candidateStateOrThrow();
        assertTrue(candidateState.isVoteGranted());
        assertEquals(1, candidateState.epoch());
    }

    @Test
    public void testInitializeAsUnattached() throws IOException {
        int node1 = 1;
        int node2 = 2;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(localId, node1, node2);
        store.writeElectionState(ElectionState.withUnknownLeader(epoch, voters));

        int jitterMs = 2500;
        random.mockNextInt(jitterMs);

        QuorumState state = buildQuorumState(voters);
        state.initialize(new OffsetAndEpoch(0L, 0));

        assertTrue(state.isUnattached());
        UnattachedState unattachedState = state.unattachedStateOrThrow();
        assertEquals(epoch, unattachedState.epoch());
        assertEquals(electionTimeoutMs + jitterMs,
            unattachedState.remainingElectionTimeMs(time.milliseconds()));
    }

    @Test
    public void testInitializeAsFollower() throws IOException {
        int node1 = 1;
        int node2 = 2;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(localId, node1, node2);
        store.writeElectionState(ElectionState.withElectedLeader(epoch, node1, voters));

        QuorumState state = buildQuorumState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        assertTrue(state.isFollower());
        assertEquals(epoch, state.epoch());

        FollowerState followerState = state.followerStateOrThrow();
        assertEquals(epoch, followerState.epoch());
        assertEquals(node1, followerState.leaderId());
        assertEquals(fetchTimeoutMs, followerState.remainingFetchTimeMs(time.milliseconds()));
    }

    @Test
    public void testInitializeAsVoted() throws IOException {
        int node1 = 1;
        int node2 = 2;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(localId, node1, node2);
        store.writeElectionState(ElectionState.withVotedCandidate(epoch, node1, voters));

        int jitterMs = 2500;
        random.mockNextInt(jitterMs);

        QuorumState state = buildQuorumState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        assertTrue(state.isVoted());
        assertEquals(epoch, state.epoch());

        VotedState votedState = state.votedStateOrThrow();
        assertEquals(epoch, votedState.epoch());
        assertEquals(node1, votedState.votedId());
        assertEquals(electionTimeoutMs + jitterMs,
            votedState.remainingElectionTimeMs(time.milliseconds()));
    }

    @Test
    public void testInitializeAsResignedCandidate() throws IOException {
        int node1 = 1;
        int node2 = 2;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(localId, node1, node2);
        ElectionState election = ElectionState.withVotedCandidate(epoch, localId, voters);
        store.writeElectionState(election);

        int jitterMs = 2500;
        random.mockNextInt(jitterMs);

        QuorumState state = buildQuorumState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        assertTrue(state.isCandidate());
        assertEquals(epoch, state.epoch());

        CandidateState candidateState = state.candidateStateOrThrow();
        assertEquals(epoch, candidateState.epoch());
        assertEquals(election, candidateState.election());
        assertEquals(Utils.mkSet(node1, node2), candidateState.unrecordedVoters());
        assertEquals(Utils.mkSet(localId), candidateState.grantingVoters());
        assertEquals(Collections.emptySet(), candidateState.rejectingVoters());
        assertEquals(electionTimeoutMs + jitterMs,
            candidateState.remainingElectionTimeMs(time.milliseconds()));
    }

    @Test
    public void testInitializeAsResignedLeader() throws IOException {
        int node1 = 1;
        int node2 = 2;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(localId, node1, node2);
        ElectionState election = ElectionState.withElectedLeader(epoch, localId, voters);
        store.writeElectionState(election);

        // If we were previously a leader, we will start as resigned in order to ensure
        // a new leader gets elected. This ensures that records are always uniquely
        // defined by epoch and offset even accounting for the loss of unflushed data.

        // The election timeout should be reset after we become a candidate again
        int jitterMs = 2500;
        random.mockNextInt(jitterMs);

        QuorumState state = buildQuorumState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        assertFalse(state.isLeader());
        assertEquals(epoch, state.epoch());

        ResignedState resignedState = state.resignedStateOrThrow();
        assertEquals(epoch, resignedState.epoch());
        assertEquals(election, resignedState.election());
        assertEquals(Utils.mkSet(node1, node2), resignedState.unackedVoters());
        assertEquals(electionTimeoutMs + jitterMs,
            resignedState.remainingElectionTimeMs(time.milliseconds()));
    }

    @Test
    public void testCandidateToCandidate() throws IOException {
        int node1 = 1;
        int node2 = 2;
        Set<Integer> voters = Utils.mkSet(localId, node1, node2);
        assertNull(store.readElectionState());

        QuorumState state = initializeEmptyState(voters);
        state.transitionToCandidate();
        assertTrue(state.isCandidate());
        assertEquals(1, state.epoch());

        CandidateState candidate1 = state.candidateStateOrThrow();
        candidate1.recordRejectedVote(node2);

        // Check backoff behavior before transitioning
        int backoffMs = 500;
        candidate1.startBackingOff(time.milliseconds(), backoffMs);
        assertTrue(candidate1.isBackingOff());
        assertFalse(candidate1.isBackoffComplete(time.milliseconds()));

        time.sleep(backoffMs - 1);
        assertTrue(candidate1.isBackingOff());
        assertFalse(candidate1.isBackoffComplete(time.milliseconds()));

        time.sleep(1);
        assertTrue(candidate1.isBackingOff());
        assertTrue(candidate1.isBackoffComplete(time.milliseconds()));

        // The election timeout should be reset after we become a candidate again
        int jitterMs = 2500;
        random.mockNextInt(jitterMs);

        state.transitionToCandidate();
        assertTrue(state.isCandidate());
        CandidateState candidate2 = state.candidateStateOrThrow();
        assertEquals(2, state.epoch());
        assertEquals(Collections.singleton(localId), candidate2.grantingVoters());
        assertEquals(Collections.emptySet(), candidate2.rejectingVoters());
        assertEquals(electionTimeoutMs + jitterMs,
            candidate2.remainingElectionTimeMs(time.milliseconds()));
    }

    @Test
    public void testCandidateToResigned() throws IOException {
        int node1 = 1;
        int node2 = 2;
        Set<Integer> voters = Utils.mkSet(localId, node1, node2);
        assertNull(store.readElectionState());

        QuorumState state = initializeEmptyState(voters);
        state.transitionToCandidate();
        assertTrue(state.isCandidate());
        assertEquals(1, state.epoch());

        assertThrows(IllegalStateException.class, () ->
            state.transitionToResigned(Collections.singletonList(localId)));
        assertTrue(state.isCandidate());
    }

    @Test
    public void testCandidateToLeader() throws IOException {
        Set<Integer> voters = Utils.mkSet(localId);
        assertNull(store.readElectionState());

        QuorumState state = initializeEmptyState(voters);
        state.transitionToCandidate();
        assertTrue(state.isCandidate());
        assertEquals(1, state.epoch());

        state.transitionToLeader(0L, accumulator);
        LeaderState leaderState = state.leaderStateOrThrow();
        assertTrue(state.isLeader());
        assertEquals(1, leaderState.epoch());
        assertEquals(Optional.empty(), leaderState.highWatermark());
    }

    @Test
    public void testCandidateToLeaderWithoutGrantedVote() throws IOException {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToCandidate();
        assertFalse(state.candidateStateOrThrow().isVoteGranted());
        assertThrows(IllegalStateException.class, () -> state.transitionToLeader(0L, accumulator));
        state.candidateStateOrThrow().recordGrantedVote(otherNodeId);
        assertTrue(state.candidateStateOrThrow().isVoteGranted());
        state.transitionToLeader(0L, accumulator);
        assertTrue(state.isLeader());
    }

    @Test
    public void testCandidateToFollower() throws IOException {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToCandidate();

        state.transitionToFollower(5, otherNodeId);
        assertEquals(5, state.epoch());
        assertEquals(OptionalInt.of(otherNodeId), state.leaderId());
        assertEquals(ElectionState.withElectedLeader(5, otherNodeId, voters), store.readElectionState());
    }

    @Test
    public void testCandidateToUnattached() throws IOException {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToCandidate();

        state.transitionToUnattached(5);
        assertEquals(5, state.epoch());
        assertEquals(OptionalInt.empty(), state.leaderId());
        assertEquals(ElectionState.withUnknownLeader(5, voters), store.readElectionState());
    }

    @Test
    public void testCandidateToVoted() throws IOException {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToCandidate();

        state.transitionToVoted(5, otherNodeId);
        assertEquals(5, state.epoch());
        assertEquals(OptionalInt.empty(), state.leaderId());

        VotedState followerState = state.votedStateOrThrow();
        assertEquals(otherNodeId, followerState.votedId());
        assertEquals(ElectionState.withVotedCandidate(5, otherNodeId, voters), store.readElectionState());
    }

    @Test
    public void testCandidateToAnyStateLowerEpoch() throws IOException {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToUnattached(5);
        state.transitionToCandidate();
        assertThrows(IllegalStateException.class, () -> state.transitionToUnattached(4));
        assertThrows(IllegalStateException.class, () -> state.transitionToVoted(4, otherNodeId));
        assertThrows(IllegalStateException.class, () -> state.transitionToFollower(4, otherNodeId));
        assertEquals(6, state.epoch());
        assertEquals(ElectionState.withVotedCandidate(6, localId, voters), store.readElectionState());
    }

    @Test
    public void testLeaderToLeader() throws IOException {
        Set<Integer> voters = Utils.mkSet(localId);
        assertNull(store.readElectionState());

        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToCandidate();
        state.transitionToLeader(0L, accumulator);
        assertTrue(state.isLeader());
        assertEquals(1, state.epoch());

        assertThrows(IllegalStateException.class, () -> state.transitionToLeader(0L, accumulator));
        assertTrue(state.isLeader());
        assertEquals(1, state.epoch());
    }

    @Test
    public void testLeaderToResigned() throws IOException {
        Set<Integer> voters = Utils.mkSet(localId);
        assertNull(store.readElectionState());

        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToCandidate();
        state.transitionToLeader(0L, accumulator);
        assertTrue(state.isLeader());
        assertEquals(1, state.epoch());

        state.transitionToResigned(Collections.singletonList(localId));
        assertTrue(state.isResigned());
        ResignedState resignedState = state.resignedStateOrThrow();
        assertEquals(ElectionState.withElectedLeader(1, localId, voters),
            resignedState.election());
        assertEquals(1, resignedState.epoch());
        assertEquals(Collections.emptySet(), resignedState.unackedVoters());
    }

    @Test
    public void testLeaderToCandidate() throws IOException {
        Set<Integer> voters = Utils.mkSet(localId);
        assertNull(store.readElectionState());

        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToCandidate();
        state.transitionToLeader(0L, accumulator);
        assertTrue(state.isLeader());
        assertEquals(1, state.epoch());

        assertThrows(IllegalStateException.class, state::transitionToCandidate);
        assertTrue(state.isLeader());
        assertEquals(1, state.epoch());
    }

    @Test
    public void testLeaderToFollower() throws IOException {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);

        QuorumState state = initializeEmptyState(voters);

        state.transitionToCandidate();
        state.candidateStateOrThrow().recordGrantedVote(otherNodeId);
        state.transitionToLeader(0L, accumulator);
        state.transitionToFollower(5, otherNodeId);

        assertEquals(5, state.epoch());
        assertEquals(OptionalInt.of(otherNodeId), state.leaderId());
        assertEquals(ElectionState.withElectedLeader(5, otherNodeId, voters), store.readElectionState());
    }

    @Test
    public void testLeaderToUnattached() throws IOException {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToCandidate();
        state.candidateStateOrThrow().recordGrantedVote(otherNodeId);
        state.transitionToLeader(0L, accumulator);
        state.transitionToUnattached(5);
        assertEquals(5, state.epoch());
        assertEquals(OptionalInt.empty(), state.leaderId());
        assertEquals(ElectionState.withUnknownLeader(5, voters), store.readElectionState());
    }

    @Test
    public void testLeaderToVoted() throws IOException {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToCandidate();
        state.candidateStateOrThrow().recordGrantedVote(otherNodeId);
        state.transitionToLeader(0L, accumulator);
        state.transitionToVoted(5, otherNodeId);

        assertEquals(5, state.epoch());
        assertEquals(OptionalInt.empty(), state.leaderId());
        VotedState votedState = state.votedStateOrThrow();
        assertEquals(otherNodeId, votedState.votedId());
        assertEquals(ElectionState.withVotedCandidate(5, otherNodeId, voters), store.readElectionState());
    }

    @Test
    public void testLeaderToAnyStateLowerEpoch() throws IOException {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToUnattached(5);
        state.transitionToCandidate();
        state.candidateStateOrThrow().recordGrantedVote(otherNodeId);
        state.transitionToLeader(0L, accumulator);
        assertThrows(IllegalStateException.class, () -> state.transitionToUnattached(4));
        assertThrows(IllegalStateException.class, () -> state.transitionToVoted(4, otherNodeId));
        assertThrows(IllegalStateException.class, () -> state.transitionToFollower(4, otherNodeId));
        assertEquals(6, state.epoch());
        assertEquals(ElectionState.withElectedLeader(6, localId, voters), store.readElectionState());
    }

    @Test
    public void testCannotFollowOrVoteForSelf() throws IOException {
        Set<Integer> voters = Utils.mkSet(localId);
        assertNull(store.readElectionState());
        QuorumState state = initializeEmptyState(voters);

        assertThrows(IllegalStateException.class, () -> state.transitionToFollower(0, localId));
        assertThrows(IllegalStateException.class, () -> state.transitionToVoted(0, localId));
    }

    @Test
    public void testUnattachedToLeaderOrResigned() throws IOException {
        int leaderId = 1;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(localId, leaderId);
        store.writeElectionState(ElectionState.withVotedCandidate(epoch, leaderId, voters));
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        assertTrue(state.isUnattached());
        assertThrows(IllegalStateException.class, () -> state.transitionToLeader(0L, accumulator));
        assertThrows(IllegalStateException.class, () -> state.transitionToResigned(Collections.emptyList()));
    }

    @Test
    public void testUnattachedToVotedSameEpoch() throws IOException {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToUnattached(5);

        int jitterMs = 2500;
        random.mockNextInt(electionTimeoutMs, jitterMs);
        state.transitionToVoted(5, otherNodeId);

        VotedState votedState = state.votedStateOrThrow();
        assertEquals(5, votedState.epoch());
        assertEquals(otherNodeId, votedState.votedId());
        assertEquals(ElectionState.withVotedCandidate(5, otherNodeId, voters), store.readElectionState());

        // Verify election timeout is reset when we vote for a candidate
        assertEquals(electionTimeoutMs + jitterMs,
            votedState.remainingElectionTimeMs(time.milliseconds()));
    }

    @Test
    public void testUnattachedToVotedHigherEpoch() throws IOException {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToUnattached(5);
        state.transitionToVoted(8, otherNodeId);

        VotedState votedState = state.votedStateOrThrow();
        assertEquals(8, votedState.epoch());
        assertEquals(otherNodeId, votedState.votedId());
        assertEquals(ElectionState.withVotedCandidate(8, otherNodeId, voters), store.readElectionState());
    }

    @Test
    public void testUnattachedToCandidate() throws IOException {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToUnattached(5);

        int jitterMs = 2500;
        random.mockNextInt(electionTimeoutMs, jitterMs);
        state.transitionToCandidate();

        assertTrue(state.isCandidate());
        CandidateState candidateState = state.candidateStateOrThrow();
        assertEquals(6, candidateState.epoch());
        assertEquals(electionTimeoutMs + jitterMs,
            candidateState.remainingElectionTimeMs(time.milliseconds()));
    }

    @Test
    public void testUnattachedToUnattached() throws IOException {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToUnattached(5);

        long remainingElectionTimeMs = state.unattachedStateOrThrow().remainingElectionTimeMs(time.milliseconds());
        time.sleep(1000);

        state.transitionToUnattached(6);
        UnattachedState unattachedState = state.unattachedStateOrThrow();
        assertEquals(6, unattachedState.epoch());

        // Verify that the election timer does not get reset
        assertEquals(remainingElectionTimeMs - 1000,
            unattachedState.remainingElectionTimeMs(time.milliseconds()));
    }

    @Test
    public void testUnattachedToFollowerSameEpoch() throws IOException {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToUnattached(5);

        state.transitionToFollower(5, otherNodeId);
        assertTrue(state.isFollower());
        FollowerState followerState = state.followerStateOrThrow();
        assertEquals(5, followerState.epoch());
        assertEquals(otherNodeId, followerState.leaderId());
        assertEquals(fetchTimeoutMs, followerState.remainingFetchTimeMs(time.milliseconds()));
    }

    @Test
    public void testUnattachedToFollowerHigherEpoch() throws IOException {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToUnattached(5);

        state.transitionToFollower(8, otherNodeId);
        assertTrue(state.isFollower());
        FollowerState followerState = state.followerStateOrThrow();
        assertEquals(8, followerState.epoch());
        assertEquals(otherNodeId, followerState.leaderId());
        assertEquals(fetchTimeoutMs, followerState.remainingFetchTimeMs(time.milliseconds()));
    }

    @Test
    public void testUnattachedToAnyStateLowerEpoch() throws IOException {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToUnattached(5);
        assertThrows(IllegalStateException.class, () -> state.transitionToUnattached(4));
        assertThrows(IllegalStateException.class, () -> state.transitionToVoted(4, otherNodeId));
        assertThrows(IllegalStateException.class, () -> state.transitionToFollower(4, otherNodeId));
        assertEquals(5, state.epoch());
        assertEquals(ElectionState.withUnknownLeader(5, voters), store.readElectionState());
    }

    @Test
    public void testVotedToInvalidLeaderOrResigned() throws IOException {
        int node1 = 1;
        int node2 = 2;
        Set<Integer> voters = Utils.mkSet(localId, node1, node2);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToVoted(5, node1);
        assertThrows(IllegalStateException.class, () -> state.transitionToLeader(0, accumulator));
        assertThrows(IllegalStateException.class, () -> state.transitionToResigned(Collections.emptyList()));
    }

    @Test
    public void testVotedToCandidate() throws IOException {
        int node1 = 1;
        int node2 = 2;
        Set<Integer> voters = Utils.mkSet(localId, node1, node2);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToVoted(5, node1);

        int jitterMs = 2500;
        random.mockNextInt(electionTimeoutMs, jitterMs);
        state.transitionToCandidate();
        assertTrue(state.isCandidate());
        CandidateState candidateState = state.candidateStateOrThrow();
        assertEquals(6, candidateState.epoch());
        assertEquals(electionTimeoutMs + jitterMs,
            candidateState.remainingElectionTimeMs(time.milliseconds()));
    }

    @Test
    public void testVotedToVotedSameEpoch() throws IOException {
        int node1 = 1;
        int node2 = 2;
        Set<Integer> voters = Utils.mkSet(localId, node1, node2);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToUnattached(5);
        state.transitionToVoted(8, node1);
        assertThrows(IllegalStateException.class, () -> state.transitionToVoted(8, node1));
        assertThrows(IllegalStateException.class, () -> state.transitionToVoted(8, node2));
    }

    @Test
    public void testVotedToFollowerSameEpoch() throws IOException {
        int node1 = 1;
        int node2 = 2;
        Set<Integer> voters = Utils.mkSet(localId, node1, node2);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToVoted(5, node1);
        state.transitionToFollower(5, node2);

        FollowerState followerState = state.followerStateOrThrow();
        assertEquals(5, followerState.epoch());
        assertEquals(node2, followerState.leaderId());
        assertEquals(ElectionState.withElectedLeader(5, node2, voters), store.readElectionState());
    }

    @Test
    public void testVotedToFollowerHigherEpoch() throws IOException {
        int node1 = 1;
        int node2 = 2;
        Set<Integer> voters = Utils.mkSet(localId, node1, node2);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToVoted(5, node1);
        state.transitionToFollower(8, node2);

        FollowerState followerState = state.followerStateOrThrow();
        assertEquals(8, followerState.epoch());
        assertEquals(node2, followerState.leaderId());
        assertEquals(ElectionState.withElectedLeader(8, node2, voters), store.readElectionState());
    }

    @Test
    public void testVotedToUnattachedSameEpoch() throws IOException {
        int node1 = 1;
        int node2 = 2;
        Set<Integer> voters = Utils.mkSet(localId, node1, node2);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToVoted(5, node1);
        assertThrows(IllegalStateException.class, () -> state.transitionToUnattached(5));
    }

    @Test
    public void testVotedToUnattachedHigherEpoch() throws IOException {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToVoted(5, otherNodeId);

        long remainingElectionTimeMs = state.votedStateOrThrow().remainingElectionTimeMs(time.milliseconds());
        time.sleep(1000);

        state.transitionToUnattached(6);
        UnattachedState unattachedState = state.unattachedStateOrThrow();
        assertEquals(6, unattachedState.epoch());

        // Verify that the election timer does not get reset
        assertEquals(remainingElectionTimeMs - 1000,
            unattachedState.remainingElectionTimeMs(time.milliseconds()));
    }

    @Test
    public void testVotedToAnyStateLowerEpoch() throws IOException {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToVoted(5, otherNodeId);
        assertThrows(IllegalStateException.class, () -> state.transitionToUnattached(4));
        assertThrows(IllegalStateException.class, () -> state.transitionToVoted(4, otherNodeId));
        assertThrows(IllegalStateException.class, () -> state.transitionToFollower(4, otherNodeId));
        assertEquals(5, state.epoch());
        assertEquals(ElectionState.withVotedCandidate(5, otherNodeId, voters), store.readElectionState());
    }

    @Test
    public void testFollowerToFollowerSameEpoch() throws IOException {
        int node1 = 1;
        int node2 = 2;
        Set<Integer> voters = Utils.mkSet(localId, node1, node2);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToFollower(8, node2);
        assertThrows(IllegalStateException.class, () -> state.transitionToFollower(8, node1));
        assertThrows(IllegalStateException.class, () -> state.transitionToFollower(8, node2));

        FollowerState followerState = state.followerStateOrThrow();
        assertEquals(8, followerState.epoch());
        assertEquals(node2, followerState.leaderId());
        assertEquals(ElectionState.withElectedLeader(8, node2, voters), store.readElectionState());
    }

    @Test
    public void testFollowerToFollowerHigherEpoch() throws IOException {
        int node1 = 1;
        int node2 = 2;
        Set<Integer> voters = Utils.mkSet(localId, node1, node2);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToFollower(8, node2);
        state.transitionToFollower(9, node1);

        FollowerState followerState = state.followerStateOrThrow();
        assertEquals(9, followerState.epoch());
        assertEquals(node1, followerState.leaderId());
        assertEquals(ElectionState.withElectedLeader(9, node1, voters), store.readElectionState());
    }

    @Test
    public void testFollowerToLeaderOrResigned() throws IOException {
        int node1 = 1;
        int node2 = 2;
        Set<Integer> voters = Utils.mkSet(localId, node1, node2);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToFollower(8, node2);
        assertThrows(IllegalStateException.class, () -> state.transitionToLeader(0, accumulator));
        assertThrows(IllegalStateException.class, () -> state.transitionToResigned(Collections.emptyList()));
    }

    @Test
    public void testFollowerToCandidate() throws IOException {
        int node1 = 1;
        int node2 = 2;
        Set<Integer> voters = Utils.mkSet(localId, node1, node2);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToFollower(8, node2);

        int jitterMs = 2500;
        random.mockNextInt(electionTimeoutMs, jitterMs);
        state.transitionToCandidate();
        assertTrue(state.isCandidate());
        CandidateState candidateState = state.candidateStateOrThrow();
        assertEquals(9, candidateState.epoch());
        assertEquals(electionTimeoutMs + jitterMs,
            candidateState.remainingElectionTimeMs(time.milliseconds()));
    }

    @Test
    public void testFollowerToUnattachedSameEpoch() throws IOException {
        int node1 = 1;
        int node2 = 2;
        Set<Integer> voters = Utils.mkSet(localId, node1, node2);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToFollower(8, node2);
        assertThrows(IllegalStateException.class, () -> state.transitionToUnattached(8));
    }

    @Test
    public void testFollowerToUnattachedHigherEpoch() throws IOException {
        int node1 = 1;
        int node2 = 2;
        Set<Integer> voters = Utils.mkSet(localId, node1, node2);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToFollower(8, node2);

        int jitterMs = 2500;
        random.mockNextInt(electionTimeoutMs, jitterMs);
        state.transitionToUnattached(9);
        assertTrue(state.isUnattached());
        UnattachedState unattachedState = state.unattachedStateOrThrow();
        assertEquals(9, unattachedState.epoch());
        assertEquals(electionTimeoutMs + jitterMs,
            unattachedState.remainingElectionTimeMs(time.milliseconds()));
    }

    @Test
    public void testFollowerToVotedSameEpoch() throws IOException {
        int node1 = 1;
        int node2 = 2;
        Set<Integer> voters = Utils.mkSet(localId, node1, node2);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToFollower(8, node2);

        assertThrows(IllegalStateException.class, () -> state.transitionToVoted(8, node1));
        assertThrows(IllegalStateException.class, () -> state.transitionToVoted(8, localId));
        assertThrows(IllegalStateException.class, () -> state.transitionToVoted(8, node2));
    }

    @Test
    public void testFollowerToVotedHigherEpoch() throws IOException {
        int node1 = 1;
        int node2 = 2;
        Set<Integer> voters = Utils.mkSet(localId, node1, node2);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToFollower(8, node2);

        int jitterMs = 2500;
        random.mockNextInt(electionTimeoutMs, jitterMs);
        state.transitionToVoted(9, node1);
        assertTrue(state.isVoted());
        VotedState votedState = state.votedStateOrThrow();
        assertEquals(9, votedState.epoch());
        assertEquals(node1, votedState.votedId());
        assertEquals(electionTimeoutMs + jitterMs,
            votedState.remainingElectionTimeMs(time.milliseconds()));
    }

    @Test
    public void testFollowerToAnyStateLowerEpoch() throws IOException {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToFollower(5, otherNodeId);
        assertThrows(IllegalStateException.class, () -> state.transitionToUnattached(4));
        assertThrows(IllegalStateException.class, () -> state.transitionToVoted(4, otherNodeId));
        assertThrows(IllegalStateException.class, () -> state.transitionToFollower(4, otherNodeId));
        assertEquals(5, state.epoch());
        assertEquals(ElectionState.withElectedLeader(5, otherNodeId, voters), store.readElectionState());
    }

    @Test
    public void testCannotBecomeFollowerOfNonVoter() throws IOException {
        int otherNodeId = 1;
        int nonVoterId = 2;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        assertThrows(IllegalStateException.class, () -> state.transitionToVoted(4, nonVoterId));
        assertThrows(IllegalStateException.class, () -> state.transitionToFollower(4, nonVoterId));
    }

    @Test
    public void testObserverCannotBecomeCandidateOrLeaderOrVoted() throws IOException {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(otherNodeId);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        assertTrue(state.isObserver());
        assertThrows(IllegalStateException.class, state::transitionToCandidate);
        assertThrows(IllegalStateException.class, () -> state.transitionToLeader(0L, accumulator));
        assertThrows(IllegalStateException.class, () -> state.transitionToVoted(5, otherNodeId));
    }

    @Test
    public void testObserverFollowerToUnattached() throws IOException {
        int node1 = 1;
        int node2 = 2;
        Set<Integer> voters = Utils.mkSet(node1, node2);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        assertTrue(state.isObserver());

        state.transitionToFollower(2, node1);
        state.transitionToUnattached(3);
        assertTrue(state.isUnattached());
        UnattachedState unattachedState = state.unattachedStateOrThrow();
        assertEquals(3, unattachedState.epoch());

        // Observers can remain in the unattached state indefinitely until a leader is found
        assertEquals(Long.MAX_VALUE, unattachedState.electionTimeoutMs());
    }

    @Test
    public void testObserverUnattachedToFollower() throws IOException {
        int node1 = 1;
        int node2 = 2;
        Set<Integer> voters = Utils.mkSet(node1, node2);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        assertTrue(state.isObserver());

        state.transitionToUnattached(2);
        state.transitionToFollower(3, node1);
        assertTrue(state.isFollower());
        FollowerState followerState = state.followerStateOrThrow();
        assertEquals(3, followerState.epoch());
        assertEquals(node1, followerState.leaderId());
        assertEquals(fetchTimeoutMs, followerState.remainingFetchTimeMs(time.milliseconds()));
    }

    @Test
    public void testInitializeWithCorruptedStore() {
        QuorumStateStore stateStore = Mockito.mock(QuorumStateStore.class);
        Mockito.doThrow(UncheckedIOException.class).when(stateStore).readElectionState();

        QuorumState state = buildQuorumState(Utils.mkSet(localId));

        int epoch = 2;
        state.initialize(new OffsetAndEpoch(0L, epoch));
        assertEquals(epoch, state.epoch());
        assertTrue(state.isUnattached());
        assertFalse(state.hasLeader());
    }

    @Test
    public void testInconsistentVotersBetweenConfigAndState() throws IOException {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);

        QuorumState state = initializeEmptyState(voters);

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

        QuorumState state = initializeEmptyState(voters);
        assertFalse(state.hasRemoteLeader());

        state.transitionToCandidate();
        assertFalse(state.hasRemoteLeader());

        state.candidateStateOrThrow().recordGrantedVote(otherNodeId);
        state.transitionToLeader(0L, accumulator);
        assertFalse(state.hasRemoteLeader());

        state.transitionToUnattached(state.epoch() + 1);
        assertFalse(state.hasRemoteLeader());

        state.transitionToVoted(state.epoch() + 1, otherNodeId);
        assertFalse(state.hasRemoteLeader());

        state.transitionToFollower(state.epoch() + 1, otherNodeId);
        assertTrue(state.hasRemoteLeader());
    }

    @Test
    public void testHighWatermarkRetained() throws IOException {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);

        QuorumState state = initializeEmptyState(voters);
        state.transitionToFollower(5, otherNodeId);

        FollowerState followerState = state.followerStateOrThrow();
        followerState.updateHighWatermark(OptionalLong.of(10L));

        Optional<LogOffsetMetadata> highWatermark = Optional.of(new LogOffsetMetadata(10L));
        assertEquals(highWatermark, state.highWatermark());

        state.transitionToUnattached(6);
        assertEquals(highWatermark, state.highWatermark());

        state.transitionToVoted(7, otherNodeId);
        assertEquals(highWatermark, state.highWatermark());

        state.transitionToCandidate();
        assertEquals(highWatermark, state.highWatermark());

        CandidateState candidateState = state.candidateStateOrThrow();
        candidateState.recordGrantedVote(otherNodeId);
        assertTrue(candidateState.isVoteGranted());

        state.transitionToLeader(10L, accumulator);
        assertEquals(Optional.empty(), state.highWatermark());
    }

    @Test
    public void testInitializeWithEmptyLocalId() throws IOException {
        QuorumState state = buildQuorumState(OptionalInt.empty(), Utils.mkSet(0, 1));
        state.initialize(new OffsetAndEpoch(0L, 0));

        assertTrue(state.isObserver());
        assertFalse(state.isVoter());

        assertThrows(IllegalStateException.class, state::transitionToCandidate);
        assertThrows(IllegalStateException.class, () -> state.transitionToVoted(1, 1));
        assertThrows(IllegalStateException.class, () -> state.transitionToLeader(0L, accumulator));

        state.transitionToFollower(1, 1);
        assertTrue(state.isFollower());

        state.transitionToUnattached(2);
        assertTrue(state.isUnattached());
    }

    @Test
    public void testObserverInitializationFailsIfElectionStateHasVotedCandidate() {
        Set<Integer> voters = Utils.mkSet(0, 1);
        int epoch = 5;
        int votedId = 1;

        store.writeElectionState(ElectionState.withVotedCandidate(epoch, votedId, voters));

        QuorumState state1 = buildQuorumState(OptionalInt.of(2), voters);
        assertThrows(IllegalStateException.class, () -> state1.initialize(new OffsetAndEpoch(0, 0)));

        QuorumState state2 = buildQuorumState(OptionalInt.empty(), voters);
        assertThrows(IllegalStateException.class, () -> state2.initialize(new OffsetAndEpoch(0, 0)));
    }

    private QuorumState initializeEmptyState(Set<Integer> voters) throws IOException {
        QuorumState state = buildQuorumState(voters);
        store.writeElectionState(ElectionState.withUnknownLeader(0, voters));
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        return state;
    }
}
