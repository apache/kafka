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
import org.apache.kafka.raft.internals.BatchAccumulator;
import org.apache.kafka.raft.internals.KRaftControlRecordStateMachine;
import org.apache.kafka.server.common.Feature;
import org.apache.kafka.server.common.KRaftVersion;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mockito;

import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class QuorumStateTest {
    private final int localId = 0;
    private final Uuid localDirectoryId = Uuid.randomUuid();
    private final ReplicaKey localVoterKey = ReplicaKey.of(localId, localDirectoryId);
    private final int logEndEpoch = 0;
    private final MockQuorumStateStore store = new MockQuorumStateStore();
    private final MockTime time = new MockTime();
    private final int electionTimeoutMs = 5000;
    private final int fetchTimeoutMs = 10000;
    private final MockableRandom random = new MockableRandom(1L);
    private final BatchAccumulator<?> accumulator = Mockito.mock(BatchAccumulator.class);

    private QuorumState buildQuorumState(
        OptionalInt localId,
        VoterSet voterSet,
        KRaftVersion kraftVersion
    ) {
        KRaftControlRecordStateMachine mockPartitionState = Mockito.mock(KRaftControlRecordStateMachine.class);

        Mockito
            .when(mockPartitionState.lastVoterSet())
            .thenReturn(voterSet);
        Mockito
            .when(mockPartitionState.lastVoterSetOffset())
            .thenReturn(kraftVersion.isReconfigSupported() ? OptionalLong.of(0) : OptionalLong.empty());
        Mockito
            .when(mockPartitionState.lastKraftVersion())
            .thenReturn(kraftVersion);

        return new QuorumState(
            localId,
            localDirectoryId,
            mockPartitionState,
            localId.isPresent() ? voterSet.listeners(localId.getAsInt()) : Endpoints.empty(),
            Feature.KRAFT_VERSION.supportedVersionRange(),
            electionTimeoutMs,
            fetchTimeoutMs,
            store,
            time,
            new LogContext(),
            random
        );
    }

    private QuorumState initializeEmptyState(VoterSet voters, KRaftVersion kraftVersion) {
        QuorumState state = buildQuorumState(OptionalInt.of(localId), voters, kraftVersion);
        store.writeElectionState(ElectionState.withUnknownLeader(0, voters.voterIds()), kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        return state;
    }

    private Set<Integer> persistedVoters(Set<Integer> voters, KRaftVersion kraftVersion) {
        if (kraftVersion.featureLevel() == 1) {
            return Collections.emptySet();
        }

        return voters;
    }

    private ReplicaKey persistedVotedKey(ReplicaKey replicaKey, KRaftVersion kraftVersion) {
        if (kraftVersion.featureLevel() == 1) {
            return replicaKey;
        }

        return ReplicaKey.of(replicaKey.id(), ReplicaKey.NO_DIRECTORY_ID);
    }

    private VoterSet localStandaloneVoterSet() {
        return VoterSetTest.voterSet(
            Collections.singletonMap(localId, VoterSetTest.voterNode(localVoterKey))
        );
    }

    private VoterSet localWithRemoteVoterSet(IntStream remoteIds, KRaftVersion kraftVersion) {
        boolean withDirectoryId = kraftVersion.featureLevel() > 0;
        Stream<ReplicaKey> remoteKeys = remoteIds
            .boxed()
            .map(id -> replicaKey(id, withDirectoryId));

        return localWithRemoteVoterSet(remoteKeys, kraftVersion);
    }

    private VoterSet localWithRemoteVoterSet(Stream<ReplicaKey> remoteKeys, KRaftVersion kraftVersion) {
        boolean withDirectoryId = kraftVersion.featureLevel() > 0;

        ReplicaKey actualLocalVoter = withDirectoryId ?
            localVoterKey :
            ReplicaKey.of(localVoterKey.id(), ReplicaKey.NO_DIRECTORY_ID);

        return VoterSetTest.voterSet(
            Stream.concat(Stream.of(actualLocalVoter), remoteKeys)
        );
    }

    private VoterSet withRemoteVoterSet(IntStream remoteIds, KRaftVersion kraftVersion) {
        boolean withDirectoryid = kraftVersion.featureLevel() > 0;

        Stream<ReplicaKey> remoteKeys = remoteIds
            .boxed()
            .map(id -> replicaKey(id, withDirectoryid));

        return VoterSetTest.voterSet(remoteKeys);
    }

    private ReplicaKey replicaKey(int id, boolean withDirectoryId) {
        Uuid directoryId = withDirectoryId ? Uuid.randomUuid() : ReplicaKey.NO_DIRECTORY_ID;
        return ReplicaKey.of(id, directoryId);
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testInitializePrimordialEpoch(KRaftVersion kraftVersion) {
        VoterSet voters = localStandaloneVoterSet();
        assertEquals(Optional.empty(), store.readElectionState());

        QuorumState state = initializeEmptyState(voters, kraftVersion);
        assertTrue(state.isUnattached());
        assertEquals(0, state.epoch());
        state.transitionToCandidate();
        CandidateState candidateState = state.candidateStateOrThrow();
        assertTrue(candidateState.isVoteGranted());
        assertEquals(1, candidateState.epoch());
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testInitializeAsUnattached(KRaftVersion kraftVersion) {
        int node1 = 1;
        int node2 = 2;
        int epoch = 5;
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(node1, node2), kraftVersion);
        store.writeElectionState(ElectionState.withUnknownLeader(epoch, voters.voterIds()), kraftVersion);

        int jitterMs = 2500;
        random.mockNextInt(jitterMs);

        QuorumState state = buildQuorumState(OptionalInt.of(localId), voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, 0));

        assertTrue(state.isUnattachedNotVoted());
        UnattachedState unattachedState = state.unattachedStateOrThrow();
        assertEquals(epoch, unattachedState.epoch());
        assertEquals(
            electionTimeoutMs + jitterMs,
            unattachedState.remainingElectionTimeMs(time.milliseconds())
        );
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testInitializeAsFollower(KRaftVersion kraftVersion) {
        int node1 = 1;
        int node2 = 2;
        int epoch = 5;
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(node1, node2), kraftVersion);
        store.writeElectionState(ElectionState.withElectedLeader(epoch, node1, voters.voterIds()), kraftVersion);

        QuorumState state = buildQuorumState(OptionalInt.of(localId), voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        assertTrue(state.isFollower());
        assertEquals(epoch, state.epoch());

        FollowerState followerState = state.followerStateOrThrow();
        assertEquals(epoch, followerState.epoch());
        assertEquals(node1, followerState.leaderId());
        assertEquals(fetchTimeoutMs, followerState.remainingFetchTimeMs(time.milliseconds()));
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testInitializeAsUnattachedWhenMissingEndpoints(KRaftVersion kraftVersion) {
        int node1 = 1;
        int node2 = 2;
        int leader = 3;
        int epoch = 5;
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(node1, node2), kraftVersion);
        store.writeElectionState(ElectionState.withElectedLeader(epoch, leader, voters.voterIds()), kraftVersion);

        QuorumState state = buildQuorumState(OptionalInt.of(localId), voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        assertTrue(state.isUnattached());
        assertEquals(epoch, state.epoch());

        UnattachedState unattachedState = state.unattachedStateOrThrow();
        assertEquals(epoch, unattachedState.epoch());
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testInitializeAsVoted(KRaftVersion kraftVersion) {
        ReplicaKey nodeKey1 = ReplicaKey.of(1, Uuid.randomUuid());
        ReplicaKey nodeKey2 = ReplicaKey.of(2, Uuid.randomUuid());

        int epoch = 5;
        VoterSet voters = VoterSetTest.voterSet(Stream.of(localVoterKey, nodeKey1, nodeKey2));
        store.writeElectionState(
            ElectionState.withVotedCandidate(epoch, nodeKey1, voters.voterIds()),
            kraftVersion
        );

        int jitterMs = 2500;
        random.mockNextInt(jitterMs);

        QuorumState state = buildQuorumState(OptionalInt.of(localId), voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        assertTrue(state.isUnattachedAndVoted());
        assertEquals(epoch, state.epoch());

        UnattachedState votedState = state.unattachedStateOrThrow();
        assertEquals(epoch, votedState.epoch());
        assertEquals(persistedVotedKey(nodeKey1, kraftVersion), votedState.votedKey().get());

        assertEquals(
            electionTimeoutMs + jitterMs,
            votedState.remainingElectionTimeMs(time.milliseconds())
        );
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testInitializeAsResignedCandidate(KRaftVersion kraftVersion) {
        boolean withDirectoryId = kraftVersion.featureLevel() > 0;
        ReplicaKey node1 = replicaKey(1, withDirectoryId);
        ReplicaKey node2 = replicaKey(2, withDirectoryId);
        int epoch = 5;
        VoterSet voters = localWithRemoteVoterSet(Stream.of(node1, node2), kraftVersion);
        ElectionState election = ElectionState.withVotedCandidate(
            epoch,
            localVoterKey,
            voters.voterIds()
        );
        store.writeElectionState(election, kraftVersion);

        int jitterMs = 2500;
        random.mockNextInt(jitterMs);

        QuorumState state = buildQuorumState(OptionalInt.of(localId), voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        assertTrue(state.isCandidate());
        assertEquals(epoch, state.epoch());

        CandidateState candidateState = state.candidateStateOrThrow();
        assertEquals(epoch, candidateState.epoch());
        assertEquals(
            ElectionState.withVotedCandidate(epoch, localVoterKey, voters.voterIds()),
            candidateState.election()
        );
        assertEquals(Set.of(node1, node2), candidateState.unrecordedVoters());
        assertEquals(Set.of(localId), candidateState.grantingVoters());
        assertEquals(Collections.emptySet(), candidateState.rejectingVoters());
        assertEquals(
            electionTimeoutMs + jitterMs,
            candidateState.remainingElectionTimeMs(time.milliseconds())
        );
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testInitializeAsResignedLeader(KRaftVersion kraftVersion) {
        int node1 = 1;
        int node2 = 2;
        int epoch = 5;
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(node1, node2), kraftVersion);
        ElectionState election = ElectionState.withElectedLeader(epoch, localId, voters.voterIds());
        store.writeElectionState(election, kraftVersion);

        // If we were previously a leader, we will start as resigned in order to ensure
        // a new leader gets elected. This ensures that records are always uniquely
        // defined by epoch and offset even accounting for the loss of unflushed data.

        // The election timeout should be reset after we become a candidate again
        int jitterMs = 2500;
        random.mockNextInt(jitterMs);

        QuorumState state = buildQuorumState(OptionalInt.of(localId), voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        assertFalse(state.isLeader());
        assertEquals(epoch, state.epoch());

        ResignedState resignedState = state.resignedStateOrThrow();
        assertEquals(epoch, resignedState.epoch());
        assertEquals(election, resignedState.election());
        assertEquals(Set.of(node1, node2), resignedState.unackedVoters());
        assertEquals(electionTimeoutMs + jitterMs,
            resignedState.remainingElectionTimeMs(time.milliseconds()));
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testCandidateToCandidate(KRaftVersion kraftVersion) {
        int node1 = 1;
        int node2 = 2;
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(node1, node2), kraftVersion);
        assertEquals(Optional.empty(), store.readElectionState());

        QuorumState state = initializeEmptyState(voters, kraftVersion);
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

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testCandidateToResigned(KRaftVersion kraftVersion) {
        int node1 = 1;
        int node2 = 2;
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(node1, node2), kraftVersion);
        assertEquals(Optional.empty(), store.readElectionState());

        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.transitionToCandidate();
        assertTrue(state.isCandidate());
        assertEquals(1, state.epoch());

        assertThrows(
            IllegalStateException.class, () ->
            state.transitionToResigned(Collections.emptyList())
        );
        assertTrue(state.isCandidate());
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testCandidateToLeader(KRaftVersion kraftVersion)  {
        VoterSet voters = localStandaloneVoterSet();
        assertEquals(Optional.empty(), store.readElectionState());

        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.transitionToCandidate();
        assertTrue(state.isCandidate());
        assertEquals(1, state.epoch());

        state.transitionToLeader(0L, accumulator);
        LeaderState<?> leaderState = state.leaderStateOrThrow();
        assertTrue(state.isLeader());
        assertEquals(1, leaderState.epoch());
        assertEquals(Optional.empty(), leaderState.highWatermark());
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testCandidateToLeaderWithoutGrantedVote(KRaftVersion kraftVersion) {
        int otherNodeId = 1;
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(otherNodeId), kraftVersion);
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToCandidate();
        assertFalse(state.candidateStateOrThrow().isVoteGranted());
        assertThrows(IllegalStateException.class, () -> state.transitionToLeader(0L, accumulator));
        state.candidateStateOrThrow().recordGrantedVote(otherNodeId);
        assertTrue(state.candidateStateOrThrow().isVoteGranted());
        state.transitionToLeader(0L, accumulator);
        assertTrue(state.isLeader());
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testCandidateToFollower(KRaftVersion kraftVersion) {
        int otherNodeId = 1;

        VoterSet voters = localWithRemoteVoterSet(IntStream.of(otherNodeId), kraftVersion);
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToCandidate();

        state.transitionToFollower(5, otherNodeId, voters.listeners(otherNodeId));
        assertEquals(5, state.epoch());
        assertEquals(OptionalInt.of(otherNodeId), state.leaderId());
        assertEquals(
            Optional.of(
                ElectionState.withElectedLeader(
                    5,
                    otherNodeId,
                    persistedVoters(voters.voterIds(), kraftVersion)
                )
            ),
            store.readElectionState()
        );
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testCandidateToUnattached(KRaftVersion kraftVersion) {
        int otherNodeId = 1;
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(otherNodeId), kraftVersion);
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToCandidate();

        state.transitionToUnattached(5);
        assertEquals(5, state.epoch());
        assertEquals(OptionalInt.empty(), state.leaderId());
        assertEquals(
            Optional.of(
                ElectionState.withUnknownLeader(
                    5,
                    persistedVoters(voters.voterIds(), kraftVersion)
                )
            ),
            store.readElectionState()
        );
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testCandidateToUnattachedVoted(KRaftVersion kraftVersion) {
        ReplicaKey otherNodeKey = ReplicaKey.of(1, Uuid.randomUuid());
        VoterSet voters = VoterSetTest.voterSet(Stream.of(localVoterKey, otherNodeKey));
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToCandidate();

        state.transitionToUnattachedVotedState(5, otherNodeKey);
        assertEquals(5, state.epoch());
        assertEquals(OptionalInt.empty(), state.leaderId());

        UnattachedState votedState = state.unattachedStateOrThrow();
        assertEquals(otherNodeKey, votedState.votedKey().get());

        assertEquals(
            Optional.of(
                ElectionState.withVotedCandidate(
                    5,
                    persistedVotedKey(otherNodeKey, kraftVersion),
                    persistedVoters(voters.voterIds(), kraftVersion))
            ),
            store.readElectionState()
        );
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testCandidateToAnyStateLowerEpoch(KRaftVersion kraftVersion) {
        ReplicaKey otherNodeKey = ReplicaKey.of(1, Uuid.randomUuid());
        VoterSet voters = VoterSetTest.voterSet(Stream.of(localVoterKey, otherNodeKey));
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToUnattached(5);
        state.transitionToCandidate();
        assertThrows(IllegalStateException.class, () -> state.transitionToUnattached(4));
        assertThrows(IllegalStateException.class, () -> state.transitionToUnattachedVotedState(4, otherNodeKey));
        assertThrows(
            IllegalStateException.class,
            () -> state.transitionToFollower(
                4,
                otherNodeKey.id(),
                voters.listeners(otherNodeKey.id())
            )
        );
        assertEquals(6, state.epoch());
        assertEquals(
            Optional.of(
                ElectionState.withVotedCandidate(
                    6,
                    persistedVotedKey(localVoterKey, kraftVersion),
                    persistedVoters(voters.voterIds(), kraftVersion)
                )
            ),
            store.readElectionState()
        );
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testLeaderToLeader(KRaftVersion kraftVersion) {
        VoterSet voters = localStandaloneVoterSet();
        assertEquals(Optional.empty(), store.readElectionState());

        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToCandidate();
        state.transitionToLeader(0L, accumulator);
        assertTrue(state.isLeader());
        assertEquals(1, state.epoch());

        assertThrows(IllegalStateException.class, () -> state.transitionToLeader(0L, accumulator));
        assertTrue(state.isLeader());
        assertEquals(1, state.epoch());
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testLeaderToResigned(KRaftVersion kraftVersion) {
        VoterSet voters = localStandaloneVoterSet();
        assertEquals(Optional.empty(), store.readElectionState());

        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToCandidate();
        state.transitionToLeader(0L, accumulator);
        assertTrue(state.isLeader());
        assertEquals(1, state.epoch());

        state.transitionToResigned(Collections.singletonList(localVoterKey));
        assertTrue(state.isResigned());
        ResignedState resignedState = state.resignedStateOrThrow();
        assertEquals(
            ElectionState.withElectedLeader(1, localId, voters.voterIds()),
            resignedState.election()
        );
        assertEquals(1, resignedState.epoch());
        assertEquals(Collections.emptySet(), resignedState.unackedVoters());
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testLeaderToCandidate(KRaftVersion kraftVersion) {
        VoterSet voters = localStandaloneVoterSet();
        assertEquals(Optional.empty(), store.readElectionState());

        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToCandidate();
        state.transitionToLeader(0L, accumulator);
        assertTrue(state.isLeader());
        assertEquals(1, state.epoch());

        assertThrows(IllegalStateException.class, state::transitionToCandidate);
        assertTrue(state.isLeader());
        assertEquals(1, state.epoch());
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testLeaderToFollower(KRaftVersion kraftVersion) {
        int otherNodeId = 1;
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(otherNodeId), kraftVersion);

        QuorumState state = initializeEmptyState(voters, kraftVersion);

        state.transitionToCandidate();
        state.candidateStateOrThrow().recordGrantedVote(otherNodeId);
        state.transitionToLeader(0L, accumulator);
        state.transitionToFollower(5, otherNodeId, voters.listeners(otherNodeId));

        assertEquals(5, state.epoch());
        assertEquals(OptionalInt.of(otherNodeId), state.leaderId());
        assertEquals(
            Optional.of(
                ElectionState.withElectedLeader(
                    5,
                    otherNodeId,
                    persistedVoters(voters.voterIds(), kraftVersion)
                )
            ),
            store.readElectionState()
        );
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testLeaderToUnattached(KRaftVersion kraftVersion) {
        int otherNodeId = 1;
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(otherNodeId), kraftVersion);
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToCandidate();
        state.candidateStateOrThrow().recordGrantedVote(otherNodeId);
        state.transitionToLeader(0L, accumulator);
        state.transitionToUnattached(5);
        assertEquals(5, state.epoch());
        assertEquals(OptionalInt.empty(), state.leaderId());
        assertEquals(
            Optional.of(
                ElectionState.withUnknownLeader(
                    5,
                    persistedVoters(voters.voterIds(), kraftVersion)
                )
            ),
            store.readElectionState()
        );
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testLeaderToUnattachedVoted(KRaftVersion kraftVersion) {
        ReplicaKey otherNodeKey = ReplicaKey.of(1, Uuid.randomUuid());
        VoterSet voters = VoterSetTest.voterSet(Stream.of(localVoterKey, otherNodeKey));
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToCandidate();
        state.candidateStateOrThrow().recordGrantedVote(otherNodeKey.id());
        state.transitionToLeader(0L, accumulator);
        state.transitionToUnattachedVotedState(5, otherNodeKey);

        assertEquals(5, state.epoch());
        assertEquals(OptionalInt.empty(), state.leaderId());

        UnattachedState votedState = state.unattachedStateOrThrow();
        assertEquals(otherNodeKey, votedState.votedKey().get());

        assertEquals(
            Optional.of(
                ElectionState.withVotedCandidate(
                    5,
                    persistedVotedKey(otherNodeKey, kraftVersion),
                    persistedVoters(voters.voterIds(), kraftVersion)
                )
            ),
            store.readElectionState()
        );
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testLeaderToAnyStateLowerEpoch(KRaftVersion kraftVersion) {
        ReplicaKey otherNodeKey = ReplicaKey.of(1, Uuid.randomUuid());
        VoterSet voters = VoterSetTest.voterSet(Stream.of(localVoterKey, otherNodeKey));
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToUnattached(5);
        state.transitionToCandidate();
        state.candidateStateOrThrow().recordGrantedVote(otherNodeKey.id());
        state.transitionToLeader(0L, accumulator);
        assertThrows(IllegalStateException.class, () -> state.transitionToUnattached(4));
        assertThrows(IllegalStateException.class, () -> state.transitionToUnattachedVotedState(4, otherNodeKey));
        assertThrows(
            IllegalStateException.class,
            () -> state.transitionToFollower(
                4,
                otherNodeKey.id(),
                voters.listeners(otherNodeKey.id())
            )
        );
        assertEquals(6, state.epoch());
        assertEquals(
            Optional.of(
                ElectionState.withElectedLeader(
                    6,
                    localId,
                    persistedVoters(voters.voterIds(), kraftVersion)
                )
            ),
            store.readElectionState()
        );
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testCannotFollowOrVoteForSelf(KRaftVersion kraftVersion) {
        VoterSet voters = localStandaloneVoterSet();
        assertEquals(Optional.empty(), store.readElectionState());
        QuorumState state = initializeEmptyState(voters, kraftVersion);

        assertThrows(
            IllegalStateException.class,
            () -> state.transitionToFollower(
                0,
                localId,
                voters.listeners(localId)
            )
        );
        assertThrows(IllegalStateException.class, () -> state.transitionToUnattachedVotedState(0, localVoterKey));
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testUnattachedToLeaderOrResigned(KRaftVersion kraftVersion) {
        ReplicaKey leaderKey = ReplicaKey.of(1, Uuid.randomUuid());
        int epoch = 5;
        VoterSet voters = VoterSetTest.voterSet(Stream.of(localVoterKey, leaderKey));
        store.writeElectionState(
            ElectionState.withVotedCandidate(epoch, leaderKey, voters.voterIds()),
            kraftVersion
        );
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        assertTrue(state.isUnattached());
        assertThrows(IllegalStateException.class, () -> state.transitionToLeader(0L, accumulator));
        assertThrows(IllegalStateException.class, () -> state.transitionToResigned(Collections.emptyList()));
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testUnattachedVotedSameEpoch(KRaftVersion kraftVersion) {
        ReplicaKey otherNodeKey = ReplicaKey.of(1, Uuid.randomUuid());
        VoterSet voters = VoterSetTest.voterSet(Stream.of(localVoterKey, otherNodeKey));
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToUnattached(5);

        int jitterMs = 2500;
        random.mockNextInt(electionTimeoutMs, jitterMs);
        state.transitionToUnattachedVotedState(5, otherNodeKey);

        UnattachedState votedState = state.unattachedStateOrThrow();
        assertEquals(5, votedState.epoch());
        assertEquals(otherNodeKey, votedState.votedKey().get());

        assertEquals(
            Optional.of(
                ElectionState.withVotedCandidate(
                    5,
                    persistedVotedKey(otherNodeKey, kraftVersion),
                    persistedVoters(voters.voterIds(), kraftVersion)
                )
            ),
            store.readElectionState()
        );

        // Verify election timeout is reset when we vote for a candidate
        assertEquals(electionTimeoutMs + jitterMs,
            votedState.remainingElectionTimeMs(time.milliseconds()));
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testUnattachedVotedHigherEpoch(KRaftVersion kraftVersion) {
        ReplicaKey otherNodeKey = ReplicaKey.of(1, Uuid.randomUuid());
        VoterSet voters = VoterSetTest.voterSet(Stream.of(localVoterKey, otherNodeKey));
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToUnattached(5);
        assertTrue(state.isUnattachedNotVoted());

        state.transitionToUnattachedVotedState(8, otherNodeKey);
        assertTrue(state.isUnattachedAndVoted());

        UnattachedState votedState = state.unattachedStateOrThrow();
        assertEquals(8, votedState.epoch());
        assertEquals(otherNodeKey, votedState.votedKey().get());

        assertEquals(
            Optional.of(
                ElectionState.withVotedCandidate(
                    8,
                    persistedVotedKey(otherNodeKey, kraftVersion),
                    persistedVoters(voters.voterIds(), kraftVersion)
                )
            ),
            store.readElectionState()
        );
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testUnattachedToCandidate(KRaftVersion kraftVersion) {
        ReplicaKey otherNodeKey = ReplicaKey.of(1, Uuid.randomUuid());
        VoterSet voters = VoterSetTest.voterSet(Stream.of(localVoterKey, otherNodeKey));
        QuorumState state = initializeEmptyState(voters, kraftVersion);
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

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testUnattachedToUnattached(KRaftVersion kraftVersion) {
        ReplicaKey otherNodeKey = ReplicaKey.of(1, Uuid.randomUuid());
        VoterSet voters = VoterSetTest.voterSet(Stream.of(localVoterKey, otherNodeKey));
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToUnattached(5);
        assertTrue(state.isUnattachedNotVoted());

        long remainingElectionTimeMs = state.unattachedStateOrThrow().remainingElectionTimeMs(time.milliseconds());
        time.sleep(1000);

        state.transitionToUnattached(6);
        assertTrue(state.isUnattachedNotVoted());
        UnattachedState unattachedState = state.unattachedStateOrThrow();
        assertEquals(6, unattachedState.epoch());

        // Verify that the election timer does not get reset
        assertEquals(remainingElectionTimeMs - 1000,
            unattachedState.remainingElectionTimeMs(time.milliseconds()));
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testUnattachedToFollowerSameEpoch(KRaftVersion kraftVersion) {
        ReplicaKey otherNodeKey = ReplicaKey.of(1, Uuid.randomUuid());
        VoterSet voters = VoterSetTest.voterSet(Stream.of(localVoterKey, otherNodeKey));
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToUnattached(5);

        state.transitionToFollower(
            5,
            otherNodeKey.id(),
            voters.listeners(otherNodeKey.id())
        );
        assertTrue(state.isFollower());
        FollowerState followerState = state.followerStateOrThrow();
        assertEquals(5, followerState.epoch());
        assertEquals(
            voters.listeners(otherNodeKey.id()),
            followerState.leaderEndpoints()
        );
        assertEquals(fetchTimeoutMs, followerState.remainingFetchTimeMs(time.milliseconds()));
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testUnattachedToFollowerHigherEpoch(KRaftVersion kraftVersion) {
        ReplicaKey otherNodeKey = ReplicaKey.of(1, Uuid.randomUuid());
        VoterSet voters = VoterSetTest.voterSet(Stream.of(localVoterKey, otherNodeKey));
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToUnattached(5);

        state.transitionToFollower(
            8,
            otherNodeKey.id(),
            voters.listeners(otherNodeKey.id())
        );
        assertTrue(state.isFollower());
        FollowerState followerState = state.followerStateOrThrow();
        assertEquals(8, followerState.epoch());
        assertEquals(
            voters.listeners(otherNodeKey.id()),
            followerState.leaderEndpoints()
        );
        assertEquals(fetchTimeoutMs, followerState.remainingFetchTimeMs(time.milliseconds()));
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testUnattachedToAnyStateLowerEpoch(KRaftVersion kraftVersion) {
        ReplicaKey otherNodeKey = ReplicaKey.of(1, Uuid.randomUuid());
        VoterSet voters = VoterSetTest.voterSet(Stream.of(localVoterKey, otherNodeKey));
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToUnattached(5);
        assertThrows(IllegalStateException.class, () -> state.transitionToUnattached(4));
        assertThrows(IllegalStateException.class, () -> state.transitionToUnattachedVotedState(4, otherNodeKey));
        assertThrows(
            IllegalStateException.class,
            () -> state.transitionToFollower(
                4,
                otherNodeKey.id(),
                voters.listeners(otherNodeKey.id())
            )
        );
        assertEquals(5, state.epoch());
        assertEquals(
            Optional.of(
                ElectionState.withUnknownLeader(
                    5,
                    persistedVoters(voters.voterIds(), kraftVersion)
                )
            ),
            store.readElectionState()
        );
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testUnattachedVotedToInvalidLeaderOrResigned(KRaftVersion kraftVersion) {
        int node1 = 1;
        int node2 = 2;
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(node1, node2), kraftVersion);
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToUnattachedVotedState(5, ReplicaKey.of(node1, ReplicaKey.NO_DIRECTORY_ID));
        assertThrows(IllegalStateException.class, () -> state.transitionToLeader(0, accumulator));
        assertThrows(IllegalStateException.class, () -> state.transitionToResigned(Collections.emptyList()));
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testUnattachedVotedToCandidate(KRaftVersion kraftVersion) {
        int node1 = 1;
        int node2 = 2;
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(node1, node2), kraftVersion);
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToUnattachedVotedState(5, ReplicaKey.of(node1, ReplicaKey.NO_DIRECTORY_ID));

        int jitterMs = 2500;
        random.mockNextInt(electionTimeoutMs, jitterMs);
        state.transitionToCandidate();
        assertTrue(state.isCandidate());
        CandidateState candidateState = state.candidateStateOrThrow();
        assertEquals(6, candidateState.epoch());
        assertEquals(electionTimeoutMs + jitterMs,
            candidateState.remainingElectionTimeMs(time.milliseconds()));
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testObserverFromUnattachedVotedToCandidate(KRaftVersion kraftVersion) {
        int voter1 = 1;
        int voter2 = 2;
        VoterSet voters = withRemoteVoterSet(IntStream.of(voter1, voter2), kraftVersion);
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToUnattachedVotedState(5, ReplicaKey.of(voter1, ReplicaKey.NO_DIRECTORY_ID));

        assertThrows(IllegalStateException.class, () -> state.transitionToCandidate());
        assertTrue(state.isUnattached());
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testUnattachedVotedToUnattachedVotedSameEpoch(KRaftVersion kraftVersion) {
        int node1 = 1;
        int node2 = 2;
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(node1, node2), kraftVersion);
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToUnattached(5);
        state.transitionToUnattachedVotedState(8, ReplicaKey.of(node1, Uuid.randomUuid()));
        assertThrows(
            IllegalStateException.class,
            () -> state.transitionToUnattachedVotedState(8, ReplicaKey.of(node1, ReplicaKey.NO_DIRECTORY_ID))
        );
        assertThrows(
            IllegalStateException.class,
            () -> state.transitionToUnattachedVotedState(8, ReplicaKey.of(node2, ReplicaKey.NO_DIRECTORY_ID))
        );
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testUnattachedVotedToFollowerSameEpoch(KRaftVersion kraftVersion) {
        int node1 = 1;
        int node2 = 2;
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(node1, node2), kraftVersion);
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToUnattachedVotedState(5, ReplicaKey.of(node1, ReplicaKey.NO_DIRECTORY_ID));
        state.transitionToFollower(
            5,
            node2,
            voters.listeners(node2)
        );

        FollowerState followerState = state.followerStateOrThrow();
        assertEquals(5, followerState.epoch());
        assertEquals(
            voters.listeners(node2),
            followerState.leaderEndpoints()
        );
        assertEquals(
            Optional.of(
                ElectionState.withElectedLeader(
                    5,
                    node2,
                    persistedVoters(voters.voterIds(), kraftVersion)
                )
            ),
            store.readElectionState()
        );
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testUnattachedVotedToFollowerHigherEpoch(KRaftVersion kraftVersion) {
        int node1 = 1;
        int node2 = 2;
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(node1, node2), kraftVersion);
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToUnattachedVotedState(5, ReplicaKey.of(node1, ReplicaKey.NO_DIRECTORY_ID));
        state.transitionToFollower(
            8,
            node2,
            voters.listeners(node2)
        );

        FollowerState followerState = state.followerStateOrThrow();
        assertEquals(8, followerState.epoch());
        assertEquals(
            voters.listeners(node2),
            followerState.leaderEndpoints()
        );
        assertEquals(
            Optional.of(
                ElectionState.withElectedLeader(
                    8,
                    node2,
                    persistedVoters(voters.voterIds(), kraftVersion)
                )
            ),
            store.readElectionState()
        );
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testUnattachedVotedToUnattachedSameEpoch(KRaftVersion kraftVersion) {
        int node1 = 1;
        int node2 = 2;
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(node1, node2), kraftVersion);
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToUnattachedVotedState(5, ReplicaKey.of(node1, ReplicaKey.NO_DIRECTORY_ID));
        assertThrows(IllegalStateException.class, () -> state.transitionToUnattached(5));
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testUnattachedVotedToUnattachedHigherEpoch(KRaftVersion kraftVersion) {
        int otherNodeId = 1;
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(otherNodeId), kraftVersion);
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToUnattachedVotedState(5, ReplicaKey.of(otherNodeId, ReplicaKey.NO_DIRECTORY_ID));

        long remainingElectionTimeMs = state.unattachedStateOrThrow().remainingElectionTimeMs(time.milliseconds());
        time.sleep(1000);

        state.transitionToUnattached(6);
        UnattachedState unattachedState = state.unattachedStateOrThrow();
        assertEquals(6, unattachedState.epoch());

        // Verify that the election timer does not get reset
        assertEquals(remainingElectionTimeMs - 1000,
            unattachedState.remainingElectionTimeMs(time.milliseconds()));
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testUnattachedVotedToAnyStateLowerEpoch(KRaftVersion kraftVersion) {
        ReplicaKey otherNodeKey = ReplicaKey.of(1, Uuid.randomUuid());
        VoterSet voters = VoterSetTest.voterSet(Stream.of(localVoterKey, otherNodeKey));
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToUnattachedVotedState(5, otherNodeKey);
        assertThrows(IllegalStateException.class, () -> state.transitionToUnattached(4));
        assertThrows(IllegalStateException.class, () -> state.transitionToUnattachedVotedState(4, otherNodeKey));
        assertThrows(
            IllegalStateException.class,
            () -> state.transitionToFollower(
                4,
                otherNodeKey.id(),
                voters.listeners(otherNodeKey.id())
            )
        );
        assertEquals(5, state.epoch());
        assertEquals(
            Optional.of(
                ElectionState.withVotedCandidate(
                    5,
                    persistedVotedKey(otherNodeKey, kraftVersion),
                    persistedVoters(voters.voterIds(), kraftVersion)
                )
            ),
            store.readElectionState()
        );
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testAllStatesToUnattachedFailInSameEpoch(KRaftVersion kraftVersion) {
        ReplicaKey voter1 = ReplicaKey.of(1, Uuid.randomUuid());
        ReplicaKey voter2 = ReplicaKey.of(2, Uuid.randomUuid());
        VoterSet voters = VoterSetTest.voterSet(Stream.of(localVoterKey, voter1, voter2));
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));

        // unattached to unattached
        state.unattachedStateOrThrow();
        state.transitionToUnattachedVotedState(5, voter1);
        // cannot vote for same or different node in same epoch
        assertThrows(IllegalStateException.class, () -> state.transitionToUnattachedVotedState(5, voter1));
        assertThrows(IllegalStateException.class, () -> state.transitionToUnattachedVotedState(5, voter2));
        // can vote for same or different node in larger epoch
        state.transitionToUnattachedVotedState(10, voter1);
        state.transitionToUnattachedVotedState(15, voter2);

        // follower to unattached
        state.transitionToFollower(20, voter1.id(), voters.listeners(voter1.id()));
        assertThrows(IllegalStateException.class, () -> state.transitionToUnattachedVotedState(state.epoch(), voter1));
        state.transitionToUnattachedVotedState(state.epoch() + 1, voter1);

        // candidate
        state.transitionToCandidate();
        assertThrows(IllegalStateException.class, () -> state.transitionToUnattachedVotedState(state.epoch(), voter1));
        state.transitionToUnattachedVotedState(state.epoch() + 1, voter1);

        // leader
        state.transitionToCandidate();
        state.candidateStateOrThrow().recordGrantedVote(voter1.id());
        state.transitionToLeader(0L, accumulator);
        assertThrows(IllegalStateException.class, () -> state.transitionToUnattachedVotedState(state.epoch(), voter1));
        state.transitionToUnattachedVotedState(state.epoch() + 1, voter1);
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testFollowerToFollowerSameEpoch(KRaftVersion kraftVersion) {
        int node1 = 1;
        int node2 = 2;
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(node1, node2), kraftVersion);
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToFollower(
            8,
            node2,
            voters.listeners(node2)
        );
        assertThrows(
            IllegalStateException.class,
            () -> state.transitionToFollower(
                8,
                node1,
                voters.listeners(node1)
            )
        );
        assertThrows(
            IllegalStateException.class,
            () -> state.transitionToFollower(
                8,
                node2,
                voters.listeners(node2)
            )
        );

        FollowerState followerState = state.followerStateOrThrow();
        assertEquals(8, followerState.epoch());
        assertEquals(
            voters.listeners(node2),
            followerState.leaderEndpoints()
        );
        assertEquals(
            Optional.of(
                ElectionState.withElectedLeader(
                    8,
                    node2,
                    persistedVoters(voters.voterIds(), kraftVersion)
                )
            ),
            store.readElectionState()
        );
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testFollowerToFollowerSameEpochAndMoreEndpoints(KRaftVersion kraftVersion) {
        int node1 = 1;
        int node2 = 2;
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(node1, node2), kraftVersion);
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToFollower(
            8,
            node2,
            voters.listeners(node2)
        );

        HashMap<ListenerName, InetSocketAddress> newNode2ListenersMap = new HashMap<>(2);
        newNode2ListenersMap.put(
            VoterSetTest.DEFAULT_LISTENER_NAME,
            InetSocketAddress.createUnresolved("localhost", 9990 + node2)
        );
        newNode2ListenersMap.put(
            ListenerName.normalised("ANOTHER_LISTENER"),
            InetSocketAddress.createUnresolved("localhost", 8990 + node2)
        );
        Endpoints newNode2Endpoints = Endpoints.fromInetSocketAddresses(newNode2ListenersMap);

        state.transitionToFollower(
            8,
            node2,
            newNode2Endpoints
        );
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testFollowerToFollowerHigherEpoch(KRaftVersion kraftVersion) {
        int node1 = 1;
        int node2 = 2;
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(node1, node2), kraftVersion);
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToFollower(
            8,
            node2,
            voters.listeners(node2)
        );
        state.transitionToFollower(
            9,
            node1,
            voters.listeners(node1)
        );

        FollowerState followerState = state.followerStateOrThrow();
        assertEquals(9, followerState.epoch());
        assertEquals(
            voters.listeners(node1),
            followerState.leaderEndpoints()
        );
        assertEquals(
            Optional.of(
                ElectionState.withElectedLeader(
                    9,
                    node1,
                    persistedVoters(voters.voterIds(), kraftVersion)
                )
            ),
            store.readElectionState()
        );
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testFollowerToLeaderOrResigned(KRaftVersion kraftVersion) {
        int node1 = 1;
        int node2 = 2;
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(node1, node2), kraftVersion);
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToFollower(
            8,
            node2,
            voters.listeners(node2)
        );
        assertThrows(IllegalStateException.class, () -> state.transitionToLeader(0, accumulator));
        assertThrows(IllegalStateException.class, () -> state.transitionToResigned(Collections.emptyList()));
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testFollowerToCandidate(KRaftVersion kraftVersion) {
        int node1 = 1;
        int node2 = 2;
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(node1, node2), kraftVersion);
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToFollower(
            8,
            node2,
            voters.listeners(node2)
        );

        int jitterMs = 2500;
        random.mockNextInt(electionTimeoutMs, jitterMs);
        state.transitionToCandidate();
        assertTrue(state.isCandidate());
        CandidateState candidateState = state.candidateStateOrThrow();
        assertEquals(9, candidateState.epoch());
        assertEquals(electionTimeoutMs + jitterMs,
            candidateState.remainingElectionTimeMs(time.milliseconds()));
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testFollowerToUnattachedSameEpoch(KRaftVersion kraftVersion) {
        int node1 = 1;
        int node2 = 2;
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(node1, node2), kraftVersion);
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToFollower(
            8,
            node2,
            voters.listeners(node2)
        );
        assertThrows(IllegalStateException.class, () -> state.transitionToUnattached(8));
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testFollowerToUnattachedHigherEpoch(KRaftVersion kraftVersion) {
        int node1 = 1;
        int node2 = 2;
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(node1, node2), kraftVersion);
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToFollower(
            8,
            node2,
            voters.listeners(node2)
        );

        int jitterMs = 2500;
        random.mockNextInt(electionTimeoutMs, jitterMs);
        state.transitionToUnattached(9);
        assertTrue(state.isUnattached());
        UnattachedState unattachedState = state.unattachedStateOrThrow();
        assertEquals(9, unattachedState.epoch());
        assertEquals(electionTimeoutMs + jitterMs,
            unattachedState.remainingElectionTimeMs(time.milliseconds()));
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testFollowerToUnattachedVotedSameEpoch(KRaftVersion kraftVersion) {
        int node1 = 1;
        int node2 = 2;
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(node1, node2), kraftVersion);
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToFollower(
            8,
            node2,
            voters.listeners(node2)
        );

        assertThrows(
            IllegalStateException.class,
            () -> state.transitionToUnattachedVotedState(8, ReplicaKey.of(node1, ReplicaKey.NO_DIRECTORY_ID))
        );
        assertThrows(
            IllegalStateException.class,
            () -> state.transitionToUnattachedVotedState(8, ReplicaKey.of(localId, ReplicaKey.NO_DIRECTORY_ID))
        );
        assertThrows(
            IllegalStateException.class,
            () -> state.transitionToUnattachedVotedState(8, ReplicaKey.of(node2, ReplicaKey.NO_DIRECTORY_ID))
        );
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testFollowerToUnattachedVotedHigherEpoch(KRaftVersion kraftVersion) {
        ReplicaKey nodeKey1 = ReplicaKey.of(1, Uuid.randomUuid());
        ReplicaKey nodeKey2 = ReplicaKey.of(2, Uuid.randomUuid());

        VoterSet voters = VoterSetTest.voterSet(Stream.of(localVoterKey, nodeKey1, nodeKey2));
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToFollower(
            8,
            nodeKey2.id(),
            voters.listeners(nodeKey2.id())
        );

        int jitterMs = 2500;
        random.mockNextInt(electionTimeoutMs, jitterMs);

        state.transitionToUnattachedVotedState(9, nodeKey1);
        assertTrue(state.isUnattachedAndVoted());

        UnattachedState votedState = state.unattachedStateOrThrow();
        assertEquals(9, votedState.epoch());
        assertEquals(nodeKey1, votedState.votedKey().get());

        assertEquals(electionTimeoutMs + jitterMs,
            votedState.remainingElectionTimeMs(time.milliseconds()));
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testFollowerToAnyStateLowerEpoch(KRaftVersion kraftVersion) {
        int otherNodeId = 1;
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(otherNodeId), kraftVersion);
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToFollower(
            5,
            otherNodeId,
            voters.listeners(otherNodeId)
        );
        assertThrows(IllegalStateException.class, () -> state.transitionToUnattached(4));
        assertThrows(
            IllegalStateException.class,
            () -> state.transitionToUnattachedVotedState(4, ReplicaKey.of(otherNodeId, ReplicaKey.NO_DIRECTORY_ID))
        );
        assertThrows(
            IllegalStateException.class,
            () -> state.transitionToFollower(
                4,
                otherNodeId,
                voters.listeners(otherNodeId)
            )
        );
        assertEquals(5, state.epoch());
        assertEquals(
            Optional.of(
                ElectionState.withElectedLeader(
                    5,
                    otherNodeId,
                    persistedVoters(voters.voterIds(), kraftVersion)
                )
            ),
            store.readElectionState()
        );
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testCanBecomeFollowerOfNonVoter(KRaftVersion kraftVersion) {
        int otherNodeId = 1;
        ReplicaKey nonVoterKey = ReplicaKey.of(2, Uuid.randomUuid());
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(otherNodeId), kraftVersion);
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));

        // Add voted state
        state.transitionToUnattachedVotedState(4, nonVoterKey);
        assertTrue(state.isUnattachedAndVoted());

        UnattachedState votedState = state.unattachedStateOrThrow();
        assertEquals(4, votedState.epoch());
        assertEquals(nonVoterKey, votedState.votedKey().get());

        // Transition to follower
        state.transitionToFollower(
            4,
            nonVoterKey.id(),
            Endpoints.fromInetSocketAddresses(
                Collections.singletonMap(
                    VoterSetTest.DEFAULT_LISTENER_NAME,
                    InetSocketAddress.createUnresolved("non-voter-host", 1234)
                )
            )
        );
        assertEquals(
            new LeaderAndEpoch(OptionalInt.of(nonVoterKey.id()), 4),
            state.leaderAndEpoch()
        );
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testObserverCannotBecomeCandidateOrLeader(KRaftVersion kraftVersion) {
        boolean withDirectoryId = kraftVersion.featureLevel() > 0;
        int otherNodeId = 1;
        VoterSet voters = VoterSetTest.voterSet(
            VoterSetTest.voterMap(IntStream.of(otherNodeId), withDirectoryId)
        );
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        assertTrue(state.isObserver());
        assertThrows(IllegalStateException.class, state::transitionToCandidate);
        assertThrows(IllegalStateException.class, () -> state.transitionToLeader(0L, accumulator));
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testObserverWithIdCanVote(KRaftVersion kraftVersion) {
        ReplicaKey otherNodeKey = ReplicaKey.of(1, Uuid.randomUuid());
        VoterSet voters = VoterSetTest.voterSet(Stream.of(otherNodeKey));

        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        assertTrue(state.isObserver());

        state.transitionToUnattachedVotedState(5, otherNodeKey);
        assertTrue(state.isUnattachedAndVoted());

        UnattachedState votedState = state.unattachedStateOrThrow();
        assertEquals(5, votedState.epoch());
        assertEquals(otherNodeKey, votedState.votedKey().get());
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testObserverFollowerToUnattached(KRaftVersion kraftVersion) {
        boolean withDirectoryId = kraftVersion.featureLevel() > 0;
        int node1 = 1;
        int node2 = 2;
        VoterSet voters = VoterSetTest.voterSet(
            VoterSetTest.voterMap(IntStream.of(node1, node2), withDirectoryId)
        );
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        assertTrue(state.isObserver());

        state.transitionToFollower(
            2,
            node1,
            voters.listeners(node1)
        );
        state.transitionToUnattached(3);
        assertTrue(state.isUnattached());
        UnattachedState unattachedState = state.unattachedStateOrThrow();
        assertEquals(3, unattachedState.epoch());

        // Observers can remain in the unattached state indefinitely until a leader is found
        assertEquals(Long.MAX_VALUE, unattachedState.electionTimeoutMs());
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testObserverUnattachedToFollower(KRaftVersion kraftVersion) {
        boolean withDirectoryId = kraftVersion.featureLevel() > 0;
        int node1 = 1;
        int node2 = 2;
        VoterSet voters = VoterSetTest.voterSet(
            VoterSetTest.voterMap(IntStream.of(node1, node2), withDirectoryId)
        );
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        assertTrue(state.isObserver());

        state.transitionToUnattached(2);
        state.transitionToFollower(3, node1, voters.listeners(node1));
        assertTrue(state.isFollower());
        FollowerState followerState = state.followerStateOrThrow();
        assertEquals(3, followerState.epoch());
        assertEquals(
            voters.listeners(node1),
            followerState.leaderEndpoints()
        );
        assertEquals(fetchTimeoutMs, followerState.remainingFetchTimeMs(time.milliseconds()));
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testInitializeWithCorruptedStore(KRaftVersion kraftVersion) {
        QuorumStateStore stateStore = Mockito.mock(QuorumStateStore.class);
        Mockito.doThrow(UncheckedIOException.class).when(stateStore).readElectionState();

        QuorumState state = buildQuorumState(
            OptionalInt.of(localId),
            localStandaloneVoterSet(),
            kraftVersion
        );

        int epoch = 2;
        state.initialize(new OffsetAndEpoch(0L, epoch));
        assertEquals(epoch, state.epoch());
        assertTrue(state.isUnattached());
        assertFalse(state.hasLeader());
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testHasRemoteLeader(KRaftVersion kraftVersion) {
        ReplicaKey otherNodeKey = ReplicaKey.of(1, Uuid.randomUuid());
        VoterSet voters = VoterSetTest.voterSet(Stream.of(localVoterKey, otherNodeKey));

        QuorumState state = initializeEmptyState(voters, kraftVersion);
        assertFalse(state.hasRemoteLeader());

        state.transitionToCandidate();
        assertFalse(state.hasRemoteLeader());

        state.candidateStateOrThrow().recordGrantedVote(otherNodeKey.id());
        state.transitionToLeader(0L, accumulator);
        assertFalse(state.hasRemoteLeader());

        state.transitionToUnattached(state.epoch() + 1);
        assertFalse(state.hasRemoteLeader());

        state.transitionToUnattachedVotedState(state.epoch() + 1, otherNodeKey);
        assertFalse(state.hasRemoteLeader());

        state.transitionToFollower(
            state.epoch() + 1,
            otherNodeKey.id(),
            voters.listeners(otherNodeKey.id())
        );
        assertTrue(state.hasRemoteLeader());
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testHighWatermarkRetained(KRaftVersion kraftVersion) {
        ReplicaKey otherNodeKey = ReplicaKey.of(1, Uuid.randomUuid());
        VoterSet voters = VoterSetTest.voterSet(Stream.of(localVoterKey, otherNodeKey));

        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.transitionToFollower(
            5,
            otherNodeKey.id(),
            voters.listeners(otherNodeKey.id())
        );

        FollowerState followerState = state.followerStateOrThrow();
        followerState.updateHighWatermark(OptionalLong.of(10L));

        Optional<LogOffsetMetadata> highWatermark = Optional.of(new LogOffsetMetadata(10L));
        assertEquals(highWatermark, state.highWatermark());

        state.transitionToUnattached(6);
        assertEquals(highWatermark, state.highWatermark());

        state.transitionToUnattachedVotedState(7, otherNodeKey);
        assertEquals(highWatermark, state.highWatermark());

        state.transitionToCandidate();
        assertEquals(highWatermark, state.highWatermark());

        CandidateState candidateState = state.candidateStateOrThrow();
        candidateState.recordGrantedVote(otherNodeKey.id());
        assertTrue(candidateState.isVoteGranted());

        state.transitionToLeader(10L, accumulator);
        assertEquals(Optional.empty(), state.highWatermark());
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testInitializeWithEmptyLocalId(KRaftVersion kraftVersion) {
        boolean withDirectoryId = kraftVersion.featureLevel() > 0;
        VoterSet voters = VoterSetTest.voterSet(
            VoterSetTest.voterMap(IntStream.of(0, 1), withDirectoryId)
        );
        QuorumState state = buildQuorumState(OptionalInt.empty(), voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, 0));

        assertTrue(state.isObserver());
        assertFalse(state.isVoter());

        assertThrows(IllegalStateException.class, state::transitionToCandidate);
        assertThrows(
            IllegalStateException.class,
            () -> state.transitionToUnattachedVotedState(1, ReplicaKey.of(1, ReplicaKey.NO_DIRECTORY_ID))
        );
        assertThrows(IllegalStateException.class, () -> state.transitionToLeader(0L, accumulator));

        state.transitionToFollower(1, 1, voters.listeners(1));
        assertTrue(state.isFollower());

        state.transitionToUnattached(2);
        assertTrue(state.isUnattached());
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testNoLocalIdInitializationFailsIfElectionStateHasVotedCandidate(KRaftVersion kraftVersion) {
        boolean withDirectoryId = kraftVersion.featureLevel() > 0;
        int epoch = 5;
        int votedId = 1;
        VoterSet voters = VoterSetTest.voterSet(
            VoterSetTest.voterMap(IntStream.of(0, votedId), withDirectoryId)
        );

        store.writeElectionState(
            ElectionState.withVotedCandidate(
                epoch,
                ReplicaKey.of(votedId, ReplicaKey.NO_DIRECTORY_ID),
                voters.voterIds()
            ),
            kraftVersion
        );

        QuorumState state2 = buildQuorumState(OptionalInt.empty(), voters, kraftVersion);
        assertThrows(IllegalStateException.class, () -> state2.initialize(new OffsetAndEpoch(0, 0)));
    }
}
