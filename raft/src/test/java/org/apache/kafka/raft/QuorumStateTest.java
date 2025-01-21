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
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.raft.internals.BatchAccumulator;
import org.apache.kafka.raft.internals.KRaftControlRecordStateMachine;
import org.apache.kafka.raft.internals.KafkaRaftMetrics;
import org.apache.kafka.server.common.Feature;
import org.apache.kafka.server.common.KRaftVersion;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mockito;

import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
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
            random,
            new KafkaRaftMetrics(new Metrics(), "raft")
        );
    }

    private QuorumState initializeEmptyState(VoterSet voters, KRaftVersion kraftVersion) {
        QuorumState state = buildQuorumState(OptionalInt.of(localId), voters, kraftVersion);
        store.writeElectionState(ElectionState.withUnknownLeader(0, voters.voterIds()), kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        return state;
    }

    private Set<Integer> persistedVoters(Set<Integer> voters, KRaftVersion kraftVersion) {
        if (kraftVersion.isReconfigSupported()) {
            return Collections.emptySet();
        }

        return voters;
    }

    private ReplicaKey persistedVotedKey(ReplicaKey replicaKey, KRaftVersion kraftVersion) {
        if (kraftVersion.isReconfigSupported()) {
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
    public void testHasRemoteLeader(KRaftVersion kraftVersion) {
        ReplicaKey otherNodeKey = ReplicaKey.of(1, Uuid.randomUuid());
        VoterSet voters = VoterSetTest.voterSet(Stream.of(localVoterKey, otherNodeKey));

        QuorumState state = initializeEmptyState(voters, kraftVersion);
        assertFalse(state.hasRemoteLeader());

        state.transitionToProspective();
        assertFalse(state.hasRemoteLeader());
        state.transitionToCandidate();
        assertFalse(state.hasRemoteLeader());

        state.candidateStateOrThrow().recordGrantedVote(otherNodeKey.id());
        state.transitionToLeader(0L, accumulator);
        assertFalse(state.hasRemoteLeader());

        state.transitionToUnattached(state.epoch() + 1, OptionalInt.empty());
        assertFalse(state.hasRemoteLeader());

        state.unattachedAddVotedState(state.epoch(), otherNodeKey);
        assertFalse(state.hasRemoteLeader());

        state.transitionToFollower(
            state.epoch(),
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

        state.transitionToUnattached(6, OptionalInt.empty());
        assertEquals(highWatermark, state.highWatermark());

        state.unattachedAddVotedState(6, otherNodeKey);
        assertEquals(highWatermark, state.highWatermark());

        state.transitionToProspective();
        assertEquals(highWatermark, state.highWatermark());

        state.transitionToCandidate();
        assertEquals(highWatermark, state.highWatermark());

        CandidateState candidateState = state.candidateStateOrThrow();
        candidateState.recordGrantedVote(otherNodeKey.id());
        assertTrue(candidateState.epochElection().isVoteGranted());

        state.transitionToLeader(10L, accumulator);
        assertEquals(Optional.empty(), state.highWatermark());
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testCanBecomeFollowerOfNonVoter(KRaftVersion kraftVersion) {
        int otherNodeId = 1;
        ReplicaKey nonVoterKey = ReplicaKey.of(2, Uuid.randomUuid());
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(otherNodeId), kraftVersion);
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, 4));

        // Add voted state
        state.unattachedAddVotedState(4, nonVoterKey);
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
        assertEquals(
            ElectionState.withElectedLeader(
                4,
                nonVoterKey.id(),
                Optional.of(persistedVotedKey(nonVoterKey, kraftVersion)),
                persistedVoters(voters.voterIds(), kraftVersion)
            ),
            store.readElectionState().get()
        );
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testCannotFollowSelf(KRaftVersion kraftVersion) {
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
        assertThrows(IllegalStateException.class, () -> state.unattachedAddVotedState(0, localVoterKey));
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testCannotTransitionToFollowerWithNoLeaderEndpoint(KRaftVersion kraftVersion) {
        int leaderId = 1;
        int followerId = 2;
        int epoch = 5;
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(leaderId, followerId), kraftVersion);
        store.writeElectionState(ElectionState.withUnknownLeader(epoch, voters.voterIds()), kraftVersion);
        QuorumState state = buildQuorumState(OptionalInt.of(localId), voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));

        assertThrows(
            NullPointerException.class,
            () -> state.transitionToFollower(epoch + 1, leaderId, null)
        );
        assertThrows(
            IllegalArgumentException.class,
            () -> state.transitionToFollower(epoch + 2, leaderId, Endpoints.empty())
        );
    }

    /**
     * Initialization tests
     */

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testInitializePrimordialEpoch(KRaftVersion kraftVersion) {
        VoterSet voters = localStandaloneVoterSet();
        assertEquals(Optional.empty(), store.readElectionState());

        QuorumState state = initializeEmptyState(voters, kraftVersion);
        assertTrue(state.isUnattached());
        assertEquals(0, state.epoch());
        state.transitionToProspective();
        state.transitionToCandidate();
        CandidateState candidateState = state.candidateStateOrThrow();
        assertTrue(candidateState.epochElection().isVoteGranted());
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
        store.writeElectionState(
            ElectionState.withElectedLeader(epoch, node1, Optional.empty(), voters.voterIds()),
            kraftVersion
        );

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
    public void testInitializeVotedAndLeaderAreSame(KRaftVersion kraftVersion) {
        int epoch = 5;
        ReplicaKey leaderKey = ReplicaKey.of(1, Uuid.randomUuid());
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(leaderKey.id(), 2), kraftVersion);
        store.writeElectionState(
            ElectionState.withElectedLeader(epoch, leaderKey.id(), Optional.of(leaderKey), voters.voterIds()),
            kraftVersion
        );

        QuorumState state = buildQuorumState(OptionalInt.of(localId), voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        assertTrue(state.isFollower());
        assertEquals(epoch, state.epoch());

        FollowerState followerState = state.followerStateOrThrow();
        assertEquals(epoch, followerState.epoch());
        assertEquals(leaderKey.id(), followerState.leaderId());
        assertEquals(fetchTimeoutMs, followerState.remainingFetchTimeMs(time.milliseconds()));
        assertEquals(
            ElectionState.withElectedLeader(
                epoch,
                leaderKey.id(),
                Optional.of(persistedVotedKey(leaderKey, kraftVersion)),
                persistedVoters(voters.voterIds(), kraftVersion)
            ),
            store.readElectionState().get()
        );
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testInitializeVotedAndLeaderAreDifferent(KRaftVersion kraftVersion) {
        int leader = 1;
        int epoch = 5;
        ReplicaKey votedKey = ReplicaKey.of(2, Uuid.randomUuid());
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(leader, votedKey.id()), kraftVersion);
        store.writeElectionState(
            ElectionState.withElectedLeader(epoch, leader, Optional.of(votedKey), voters.voterIds()),
            kraftVersion
        );

        QuorumState state = buildQuorumState(OptionalInt.of(localId), voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        assertTrue(state.isFollower());
        assertEquals(epoch, state.epoch());

        FollowerState followerState = state.followerStateOrThrow();
        assertEquals(epoch, followerState.epoch());
        assertEquals(leader, followerState.leaderId());
        assertEquals(fetchTimeoutMs, followerState.remainingFetchTimeMs(time.milliseconds()));
        assertEquals(
            ElectionState.withElectedLeader(
                epoch,
                leader,
                Optional.of(persistedVotedKey(votedKey, kraftVersion)),
                persistedVoters(voters.voterIds(), kraftVersion)
            ),
            store.readElectionState().get()
        );
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testInitializeAsUnattachedWhenMissingEndpoints(KRaftVersion kraftVersion) {
        int node1 = 1;
        int node2 = 2;
        int leader = 3;
        int epoch = 5;
        ReplicaKey votedKey = ReplicaKey.of(leader, Uuid.randomUuid());
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(node1, node2), kraftVersion);
        store.writeElectionState(
            ElectionState.withElectedLeader(epoch, leader, Optional.of(votedKey), voters.voterIds()),
            kraftVersion
        );

        QuorumState state = buildQuorumState(OptionalInt.of(localId), voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        assertTrue(state.isUnattached());
        assertEquals(epoch, state.epoch());

        UnattachedState unattachedState = state.unattachedStateOrThrow();
        assertEquals(epoch, unattachedState.epoch());
        assertEquals(
            ElectionState.withElectedLeader(
                epoch,
                leader,
                Optional.of(persistedVotedKey(votedKey, kraftVersion)),
                persistedVoters(voters.voterIds(), kraftVersion)
            ),
            store.readElectionState().get()
        );
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testInitializeAsVotedNoLeader(KRaftVersion kraftVersion) {
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
        assertEquals(Set.of(node1, node2), candidateState.epochElection().unrecordedVoters());
        assertEquals(Set.of(localId), candidateState.epochElection().grantingVoters());
        assertEquals(Collections.emptySet(), candidateState.epochElection().rejectingVoters());
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
        ElectionState election = ElectionState.withElectedLeader(epoch, localId, Optional.empty(), voters.voterIds());
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
        assertEquals(
            electionTimeoutMs + jitterMs,
            resignedState.remainingElectionTimeMs(time.milliseconds())
        );
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testInitializeAsOnlyVoter(KRaftVersion kraftVersion) {
        VoterSet voters = localStandaloneVoterSet();
        store.writeElectionState(ElectionState.withUnknownLeader(0, voters.voterIds()), kraftVersion);

        QuorumState state = buildQuorumState(OptionalInt.of(localId), voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        // start as unattached, in KafkaRaftClient initialization the local replica then transitions to candidate
        assertTrue(state.isUnattached());
        assertEquals(0, state.epoch());
        assertEquals(OptionalInt.empty(), state.leaderId());
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
    public void testInitializeWithEmptyLocalId(KRaftVersion kraftVersion) {
        boolean withDirectoryId = kraftVersion.featureLevel() > 0;
        VoterSet voters = VoterSetTest.voterSet(
            VoterSetTest.voterMap(IntStream.of(0, 1), withDirectoryId)
        );
        QuorumState state = buildQuorumState(OptionalInt.empty(), voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, 0));

        assertTrue(state.isObserver());
        assertFalse(state.isVoter());

        assertThrows(IllegalStateException.class, state::transitionToProspective);

        assertThrows(
            IllegalStateException.class,
            () -> state.unattachedAddVotedState(1, ReplicaKey.of(1, ReplicaKey.NO_DIRECTORY_ID))
        );
        assertThrows(IllegalStateException.class, () -> state.transitionToLeader(0L, accumulator));

        state.transitionToFollower(1, 1, voters.listeners(1));
        assertTrue(state.isFollower());

        state.transitionToUnattached(2, OptionalInt.empty());
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

    /**
     * Test transitions from Unattached
     */
    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testUnattachedToUnattached(KRaftVersion kraftVersion) {
        ReplicaKey otherNodeKey = ReplicaKey.of(1, Uuid.randomUuid());
        VoterSet voters = VoterSetTest.voterSet(Stream.of(localVoterKey, otherNodeKey));
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToUnattached(5, OptionalInt.empty());
        assertTrue(state.isUnattachedNotVoted());

        long remainingElectionTimeMs = state.unattachedStateOrThrow().remainingElectionTimeMs(time.milliseconds());
        time.sleep(1000);

        // cannot transition to unattached in same epoch
        assertThrows(IllegalStateException.class, () -> state.transitionToUnattached(state.epoch(), OptionalInt.empty()));

        // can transition to unattached in higher epoch
        state.transitionToUnattached(6, OptionalInt.empty());
        assertTrue(state.isUnattachedNotVoted());
        UnattachedState unattachedState = state.unattachedStateOrThrow();
        assertEquals(6, unattachedState.epoch());

        // Verify that the election timer does not get reset
        assertEquals(
            remainingElectionTimeMs - 1000,
            unattachedState.remainingElectionTimeMs(time.milliseconds())
        );
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testUnattachedCannotAddVotedStateForSelf(KRaftVersion kraftVersion) {
        VoterSet voters = localStandaloneVoterSet();
        assertEquals(Optional.empty(), store.readElectionState());
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        assertThrows(IllegalStateException.class, () -> state.unattachedAddVotedState(0, localVoterKey));
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testUnattachedToUnattachedVotedSameEpoch(KRaftVersion kraftVersion) {
        ReplicaKey otherNodeKey = ReplicaKey.of(1, Uuid.randomUuid());
        VoterSet voters = VoterSetTest.voterSet(Stream.of(localVoterKey, otherNodeKey));
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToUnattached(5, OptionalInt.empty());

        int jitterMs = 2500;
        random.mockNextInt(electionTimeoutMs, jitterMs);
        state.unattachedAddVotedState(5, otherNodeKey);

        UnattachedState votedState = state.unattachedStateOrThrow();
        assertEquals(5, votedState.epoch());
        assertEquals(otherNodeKey, votedState.votedKey().get());

        assertEquals(
                ElectionState.withVotedCandidate(
                    5,
                    persistedVotedKey(otherNodeKey, kraftVersion),
                    persistedVoters(voters.voterIds(), kraftVersion)
            ),
            store.readElectionState().get()
        );

        // Verify election timeout is reset when we vote for a candidate
        assertEquals(
            electionTimeoutMs + jitterMs,
            votedState.remainingElectionTimeMs(time.milliseconds())
        );
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testUnattachedToUnattachedVotedHigherEpoch(KRaftVersion kraftVersion) {
        ReplicaKey otherNodeKey = ReplicaKey.of(1, Uuid.randomUuid());
        VoterSet voters = VoterSetTest.voterSet(Stream.of(localVoterKey, otherNodeKey));
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToUnattached(5, OptionalInt.empty());
        assertTrue(state.isUnattachedNotVoted());

        assertThrows(IllegalStateException.class, () -> state.unattachedAddVotedState(10, otherNodeKey));
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testUnattachedToFollowerSameEpoch(KRaftVersion kraftVersion) {
        ReplicaKey otherNodeKey = ReplicaKey.of(1, Uuid.randomUuid());
        VoterSet voters = VoterSetTest.voterSet(Stream.of(localVoterKey, otherNodeKey));
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToUnattached(5, OptionalInt.empty());

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
        state.transitionToUnattached(5, OptionalInt.empty());

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
    public void testUnattachedToProspective(KRaftVersion kraftVersion) {
        int node1 = 1;
        int node2 = 2;
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(node1, node2), kraftVersion);
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        assertTrue(state.isUnattached());
        assertEquals(logEndEpoch, state.epoch());

        int jitterMs = 2500;
        random.mockNextInt(electionTimeoutMs, jitterMs);
        state.transitionToProspective();
        ProspectiveState prospective = state.prospectiveStateOrThrow();
        assertEquals(electionTimeoutMs + jitterMs,
            prospective.remainingElectionTimeMs(time.milliseconds()));
        assertEquals(
            ElectionState.withUnknownLeader(logEndEpoch, persistedVoters(voters.voterIds(), kraftVersion)),
            store.readElectionState().get()
        );
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testUnattachedToCandidate(KRaftVersion kraftVersion) {
        ReplicaKey otherNodeKey = ReplicaKey.of(1, Uuid.randomUuid());
        VoterSet voters = VoterSetTest.voterSet(Stream.of(localVoterKey, otherNodeKey));
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToUnattached(5, OptionalInt.empty());
        assertThrows(IllegalStateException.class, () -> state.transitionToCandidate());
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
        QuorumState state = buildQuorumState(OptionalInt.of(localId), voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        assertTrue(state.isUnattachedAndVoted());
        assertThrows(IllegalStateException.class, () -> state.transitionToLeader(0L, accumulator));
        assertThrows(IllegalStateException.class, () -> state.transitionToResigned(Collections.emptyList()));
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testUnattachedToAnyStateLowerEpoch(KRaftVersion kraftVersion) {
        ReplicaKey otherNodeKey = ReplicaKey.of(1, Uuid.randomUuid());
        VoterSet voters = VoterSetTest.voterSet(Stream.of(localVoterKey, otherNodeKey));
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToUnattached(5, OptionalInt.empty());
        assertThrows(IllegalStateException.class, () -> state.transitionToUnattached(4, OptionalInt.empty()));
        assertThrows(IllegalStateException.class, () -> state.unattachedAddVotedState(4, otherNodeKey));
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
            ElectionState.withUnknownLeader(5, persistedVoters(voters.voterIds(), kraftVersion)),
            store.readElectionState().get()
        );
    }

    /**
     * Tests transitions from Unattached with votedKey
     */
    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testUnattachedVotedToUnattachedSameEpoch(KRaftVersion kraftVersion) {
        int node1 = 1;
        int node2 = 2;
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(node1, node2), kraftVersion);
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.unattachedAddVotedState(logEndEpoch, ReplicaKey.of(node1, ReplicaKey.NO_DIRECTORY_ID));
        assertThrows(IllegalStateException.class, () -> state.transitionToUnattached(logEndEpoch, OptionalInt.empty()));
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testUnattachedVotedToUnattachedHigherEpoch(KRaftVersion kraftVersion) {
        int otherNodeId = 1;
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(otherNodeId), kraftVersion);
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, 5));
        state.unattachedAddVotedState(5, ReplicaKey.of(otherNodeId, ReplicaKey.NO_DIRECTORY_ID));

        long remainingElectionTimeMs = state.unattachedStateOrThrow().remainingElectionTimeMs(time.milliseconds());
        time.sleep(1000);

        state.transitionToUnattached(6, OptionalInt.empty());
        UnattachedState unattachedState = state.unattachedStateOrThrow();
        assertEquals(6, unattachedState.epoch());

        // Verify that the election timer does not get reset
        assertEquals(
            remainingElectionTimeMs - 1000,
            unattachedState.remainingElectionTimeMs(time.milliseconds())
        );
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testUnattachedVotedToUnattachedVoted(KRaftVersion kraftVersion) {
        int node1 = 1;
        int node2 = 2;
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(node1, node2), kraftVersion);
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, 8));
        state.unattachedAddVotedState(8, ReplicaKey.of(node1, Uuid.randomUuid()));
        // same epoch
        assertThrows(
            IllegalStateException.class,
            () -> state.unattachedAddVotedState(8, ReplicaKey.of(node2, ReplicaKey.NO_DIRECTORY_ID))
        );
        // same votedKey
        assertThrows(
            IllegalStateException.class,
            () -> state.unattachedAddVotedState(8, ReplicaKey.of(node1, ReplicaKey.NO_DIRECTORY_ID))
        );
        // higher epoch
        assertThrows(
            IllegalStateException.class,
            () -> state.unattachedAddVotedState(10, ReplicaKey.of(node1, ReplicaKey.NO_DIRECTORY_ID))
        );
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testUnattachedVotedToFollowerSameEpoch(KRaftVersion kraftVersion) {
        int node1 = 1;
        ReplicaKey node1Key = ReplicaKey.of(node1, ReplicaKey.NO_DIRECTORY_ID);
        int node2 = 2;
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(node1, node2), kraftVersion);
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, 5));
        state.unattachedAddVotedState(5, node1Key);
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
            ElectionState.withElectedLeader(
                5,
                node2,
                Optional.of(persistedVotedKey(node1Key, kraftVersion)),
                persistedVoters(voters.voterIds(), kraftVersion)
            ),
            store.readElectionState().get()
        );
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testUnattachedVotedToFollowerHigherEpoch(KRaftVersion kraftVersion) {
        int node1 = 1;
        int node2 = 2;
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(node1, node2), kraftVersion);
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, 5));
        state.unattachedAddVotedState(5, ReplicaKey.of(node1, ReplicaKey.NO_DIRECTORY_ID));
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
            ElectionState.withElectedLeader(
                8,
                node2,
                Optional.empty(),
                persistedVoters(voters.voterIds(), kraftVersion)
            ),
            store.readElectionState().get()
        );
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testUnattachedVotedToProspective(KRaftVersion kraftVersion) {
        int node1 = 1;
        Uuid node1DirectoryId = Uuid.randomUuid();
        int node2 = 2;
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(node1, node2), kraftVersion);
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, 5));
        state.unattachedAddVotedState(5, ReplicaKey.of(node1, node1DirectoryId));

        int jitterMs = 2500;
        random.mockNextInt(electionTimeoutMs, jitterMs);
        state.transitionToProspective();
        assertTrue(state.isProspective());
        ProspectiveState prospectiveState = state.prospectiveStateOrThrow();
        assertEquals(5, prospectiveState.epoch());
        assertEquals(electionTimeoutMs + jitterMs,
            prospectiveState.remainingElectionTimeMs(time.milliseconds()));
        assertEquals(
            ElectionState.withVotedCandidate(
                5,
                persistedVotedKey(ReplicaKey.of(node1, node1DirectoryId), kraftVersion),
                persistedVoters(voters.voterIds(), kraftVersion)
            ),
            store.readElectionState().get()
        );
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testUnattachedVotedToCandidateOrLeaderOrResigned(KRaftVersion kraftVersion) {
        int node1 = 1;
        int node2 = 2;
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(node1, node2), kraftVersion);
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.unattachedAddVotedState(logEndEpoch, ReplicaKey.of(node1, ReplicaKey.NO_DIRECTORY_ID));
        assertThrows(IllegalStateException.class, () -> state.transitionToCandidate());
        assertThrows(IllegalStateException.class, () -> state.transitionToLeader(0, accumulator));
        assertThrows(IllegalStateException.class, () -> state.transitionToResigned(Collections.emptyList()));
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testUnattachedVotedToAnyStateLowerEpoch(KRaftVersion kraftVersion) {
        ReplicaKey otherNodeKey = ReplicaKey.of(1, Uuid.randomUuid());
        VoterSet voters = VoterSetTest.voterSet(Stream.of(localVoterKey, otherNodeKey));
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, 5));
        state.unattachedAddVotedState(5, otherNodeKey);
        assertThrows(IllegalStateException.class, () -> state.transitionToUnattached(4, OptionalInt.empty()));
        assertThrows(IllegalStateException.class, () -> state.unattachedAddVotedState(4, otherNodeKey));
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
            ElectionState.withVotedCandidate(
                5,
                persistedVotedKey(otherNodeKey, kraftVersion),
                persistedVoters(voters.voterIds(), kraftVersion)
            ),
            store.readElectionState().get()
        );
    }

    /**
     * Test transitions from Unattached with leader
     */

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testUnattachedWithLeaderToProspective(KRaftVersion kraftVersion) {
        int node1 = 1;
        int node2 = 2;
        int epoch = 5;
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(node1, node2), kraftVersion);
        store.writeElectionState(
            ElectionState.withElectedLeader(epoch, node1, Optional.empty(), voters.voterIds()),
            kraftVersion
        );
        QuorumState state = buildQuorumState(OptionalInt.of(localId), voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));

        state.transitionToProspective();
        assertTrue(state.isProspective());
        assertEquals(
            ElectionState.withElectedLeader(
                epoch,
                node1,
                Optional.empty(),
                persistedVoters(voters.voterIds(), kraftVersion)
            ),
            store.readElectionState().get()
        );
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testUnattachedWithLeaderNoEndpointToAndFromProspective(KRaftVersion kraftVersion) {
        int leaderId = 1;
        ReplicaKey leaderKey = ReplicaKey.of(leaderId, Uuid.randomUuid());
        int followerId = 2;
        ReplicaKey followerKey = ReplicaKey.of(followerId, Uuid.randomUuid());
        int epoch = 5;
        Map<Integer, VoterSet.VoterNode> voterMap = new HashMap<>();
        voterMap.put(localId, VoterSetTest.voterNode(localVoterKey));
        voterMap.put(leaderId, VoterSetTest.voterNode(leaderKey, Endpoints.empty()));
        voterMap.put(followerId, VoterSetTest.voterNode(followerKey, Endpoints.empty()));
        VoterSet voters = VoterSetTest.voterSet(voterMap);

        store.writeElectionState(
            ElectionState.withElectedLeader(epoch, leaderId, Optional.empty(), voters.voterIds()),
            kraftVersion
        );
        QuorumState state = buildQuorumState(OptionalInt.of(localId), voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        assertTrue(state.isUnattached());
        assertTrue(state.hasLeader());
        assertTrue(state.leaderEndpoints().isEmpty());

        state.transitionToProspective();
        assertTrue(state.isProspective());
        assertTrue(state.leaderEndpoints().isEmpty());
        assertEquals(
            ElectionState.withElectedLeader(
                5,
                leaderId,
                Optional.empty(),
                persistedVoters(voters.voterIds(), kraftVersion)
            ),
            store.readElectionState().get()
        );

        state.transitionToUnattached(5, OptionalInt.of(leaderId));
        assertTrue(state.isUnattached());
        assertTrue(state.leaderEndpoints().isEmpty());
        assertEquals(
            ElectionState.withElectedLeader(
                5,
                leaderId,
                Optional.empty(),
                persistedVoters(voters.voterIds(), kraftVersion)
            ),
            store.readElectionState().get()
        );
    }

    /**
     * Test transitions from Follower
     */
    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testFollowerToUnattachedSameEpoch(KRaftVersion kraftVersion) {
        int node1 = 1;
        int node2 = 2;
        ReplicaKey votedKey = ReplicaKey.of(2, Uuid.randomUuid());
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(node1, node2), kraftVersion);
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToFollower(
            8,
            node2,
            voters.listeners(node2)
        );
        assertThrows(IllegalStateException.class, () -> state.transitionToUnattached(8, OptionalInt.empty()));
        assertThrows(IllegalStateException.class, () -> state.unattachedAddVotedState(8, votedKey));
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
        state.transitionToUnattached(9, OptionalInt.empty());
        assertTrue(state.isUnattached());
        UnattachedState unattachedState = state.unattachedStateOrThrow();
        assertEquals(9, unattachedState.epoch());
        assertEquals(
            electionTimeoutMs + jitterMs,
            unattachedState.remainingElectionTimeMs(time.milliseconds())
        );
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testFollowerToUnattachedVotedHigherEpoch(KRaftVersion kraftVersion) {
        int node1 = 1;
        int node2 = 2;
        ReplicaKey votedKey = ReplicaKey.of(node2, Uuid.randomUuid());
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(node1, node2), kraftVersion);
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToFollower(
            8,
            node2,
            voters.listeners(node2)
        );

        assertThrows(IllegalStateException.class, () -> state.unattachedAddVotedState(9, votedKey));
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
            ElectionState.withElectedLeader(
                8,
                node2,
                Optional.empty(),
                persistedVoters(voters.voterIds(), kraftVersion)
            ),
            store.readElectionState().get()
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
            ElectionState.withElectedLeader(
                9,
                node1,
                Optional.empty(),
                persistedVoters(voters.voterIds(), kraftVersion)
            ),
            store.readElectionState().get()
        );
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testFollowerToProspective(KRaftVersion kraftVersion) {
        int node1 = 1;
        int node2 = 2;
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(node1, node2), kraftVersion);
        store.writeElectionState(ElectionState.withUnknownLeader(logEndEpoch, voters.voterIds()), kraftVersion);
        QuorumState state = buildQuorumState(OptionalInt.of(localId), voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToFollower(
            8,
            node2,
            voters.listeners(node2)
        );
        int jitterMs = 2500;
        random.mockNextInt(electionTimeoutMs, jitterMs);
        state.transitionToProspective();
        ProspectiveState prospective = state.prospectiveStateOrThrow();
        assertEquals(8, prospective.epoch());
        assertEquals(
            electionTimeoutMs + jitterMs,
            prospective.remainingElectionTimeMs(time.milliseconds())
        );
        assertEquals(
            ElectionState.withElectedLeader(
                8,
                node2,
                Optional.empty(),
                persistedVoters(voters.voterIds(), kraftVersion)
            ),
            store.readElectionState().get()
        );
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
        assertThrows(IllegalStateException.class, state::transitionToCandidate);
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
        assertThrows(IllegalStateException.class, () -> state.transitionToUnattached(4, OptionalInt.empty()));
        assertThrows(
            IllegalStateException.class,
            () -> state.transitionToFollower(
                4,
                otherNodeId,
                voters.listeners(otherNodeId)
            )
        );
        assertThrows(IllegalStateException.class, () -> state.transitionToLeader(4, accumulator));
        assertEquals(5, state.epoch());
        assertEquals(
            ElectionState.withElectedLeader(
                5,
                otherNodeId,
                Optional.empty(),
                persistedVoters(voters.voterIds(), kraftVersion)
            ),
            store.readElectionState().get()
        );
    }

    /**
     * Test transitions from Follower with votedKey
     */

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testFollowerVotedToUnattachedSameEpoch(KRaftVersion kraftVersion) {
        int node1 = 1;
        int node2 = 2;
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(node1, node2), kraftVersion);
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToProspective();
        state.prospectiveStateOrThrow().recordGrantedVote(node1);
        state.transitionToFollower(
            state.epoch(),
            node2,
            voters.listeners(node2)
        );
        assertEquals(0, state.epoch());

        assertThrows(
            IllegalStateException.class,
            () -> state.transitionToUnattached(0, OptionalInt.empty())
        );
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testFollowerVotedToUnattachedHigherEpoch(KRaftVersion kraftVersion) {
        int node1 = 1;
        int node2 = 2;
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(node1, node2), kraftVersion);
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToProspective();
        state.prospectiveStateOrThrow().recordGrantedVote(node1);
        state.transitionToFollower(
            state.epoch(),
            node2,
            voters.listeners(node2)
        );
        assertEquals(0, state.epoch());

        int jitterMs = 2500;
        random.mockNextInt(electionTimeoutMs, jitterMs);

        state.transitionToUnattached(10, OptionalInt.empty());
        assertTrue(state.isUnattachedNotVoted());

        UnattachedState unattached = state.unattachedStateOrThrow();
        assertEquals(10, unattached.epoch());

        assertEquals(
            electionTimeoutMs + jitterMs,
            unattached.remainingElectionTimeMs(time.milliseconds())
        );
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testFollowerVotedToFollowerSameEpoch(KRaftVersion kraftVersion) {
        int node1 = 1;
        int node2 = 2;
        ReplicaKey votedKey = ReplicaKey.of(node1, Uuid.randomUuid());
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(node1, node2), kraftVersion);
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToProspective();
        state.prospectiveAddVotedState(state.epoch(), votedKey);
        state.transitionToFollower(
            state.epoch(),
            node2,
            voters.listeners(node2)
        );
        assertEquals(
            ElectionState.withElectedLeader(
                0,
                node2,
                Optional.of(persistedVotedKey(votedKey, kraftVersion)),
                persistedVoters(voters.voterIds(), kraftVersion)
            ),
            store.readElectionState().get()
        );

        assertThrows(
            IllegalStateException.class,
            () -> state.transitionToFollower(state.epoch(), node1, voters.listeners(node1))
        );
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testFollowerVotedToFollowerHigherEpoch(KRaftVersion kraftVersion) {
        int node1 = 1;
        int node2 = 2;
        ReplicaKey votedKey = ReplicaKey.of(node1, Uuid.randomUuid());
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(node1, node2), kraftVersion);
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToProspective();
        state.prospectiveAddVotedState(state.epoch(), votedKey);
        state.transitionToFollower(
            state.epoch(),
            node2,
            voters.listeners(node2)
        );
        assertEquals(
            ElectionState.withElectedLeader(
                0,
                node2,
                Optional.of(persistedVotedKey(votedKey, kraftVersion)),
                persistedVoters(voters.voterIds(), kraftVersion)
            ),
            store.readElectionState().get()
        );

        state.transitionToFollower(state.epoch() + 1, node1, voters.listeners(node1));
        assertEquals(1, state.epoch());
        assertEquals(
            ElectionState.withElectedLeader(
                1,
                node1,
                Optional.empty(),
                persistedVoters(voters.voterIds(), kraftVersion)
            ),
            store.readElectionState().get()
        );
    }

    /**
     * Test transitions from Prospective
     */
    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testProspectiveToUnattachedInSameEpoch(KRaftVersion kraftVersion) {
        ReplicaKey voter1 = ReplicaKey.of(1, Uuid.randomUuid());
        ReplicaKey voter2 = ReplicaKey.of(2, Uuid.randomUuid());
        VoterSet voters = VoterSetTest.voterSet(Stream.of(localVoterKey, voter1, voter2));
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToProspective();

        state.transitionToUnattached(state.epoch(), OptionalInt.empty());
        assertEquals(
            ElectionState.withUnknownLeader(logEndEpoch, persistedVoters(voters.voterIds(), kraftVersion)),
            store.readElectionState().get()
        );
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testProspectiveToUnattachedInHigherEpoch(KRaftVersion kraftVersion) {
        ReplicaKey voter1 = ReplicaKey.of(1, Uuid.randomUuid());
        ReplicaKey voter2 = ReplicaKey.of(2, Uuid.randomUuid());
        VoterSet voters = VoterSetTest.voterSet(Stream.of(localVoterKey, voter1, voter2));
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToProspective();

        state.transitionToUnattached(20, OptionalInt.empty());
        assertEquals(
            ElectionState.withUnknownLeader(20, persistedVoters(voters.voterIds(), kraftVersion)),
            store.readElectionState().get()
        );
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testProspectiveToUnattachedVoted(KRaftVersion kraftVersion) {
        ReplicaKey voter1 = ReplicaKey.of(1, Uuid.randomUuid());
        ReplicaKey voter2 = ReplicaKey.of(2, Uuid.randomUuid());
        VoterSet voters = VoterSetTest.voterSet(Stream.of(localVoterKey, voter1, voter2));
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToProspective();

        // in same epoch
        assertThrows(IllegalStateException.class, () -> state.unattachedAddVotedState(logEndEpoch, voter1));
        assertThrows(IllegalStateException.class, () -> state.unattachedAddVotedState(logEndEpoch, localVoterKey));

        // in higher epoch
        assertThrows(IllegalStateException.class, () -> state.unattachedAddVotedState(10, voter1));
        assertThrows(IllegalStateException.class, () -> state.unattachedAddVotedState(10, localVoterKey));
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testProspectiveToUnattachedWithLeaderInHigherEpoch(KRaftVersion kraftVersion) {
        ReplicaKey voter1 = ReplicaKey.of(1, Uuid.randomUuid());
        ReplicaKey voter2 = ReplicaKey.of(2, Uuid.randomUuid());
        VoterSet voters = VoterSetTest.voterSet(Stream.of(localVoterKey, voter1, voter2));
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToProspective();

        state.transitionToUnattached(10, OptionalInt.of(voter1.id()));
        assertEquals(
            ElectionState.withElectedLeader(
                10,
                voter1.id(),
                Optional.empty(),
                persistedVoters(voters.voterIds(), kraftVersion)
            ),
            store.readElectionState().get()
        );
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testProspectiveToFollowerSameEpoch(KRaftVersion kraftVersion) {
        ReplicaKey voter1 = ReplicaKey.of(1, Uuid.randomUuid());
        ReplicaKey voter2 = ReplicaKey.of(2, Uuid.randomUuid());
        VoterSet voters = VoterSetTest.voterSet(Stream.of(localVoterKey, voter1, voter2));
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToProspective();
        state.transitionToFollower(state.epoch(), voter1.id(), voters.listeners(voter1.id()));
        assertEquals(
            ElectionState.withElectedLeader(
                0,
                voter1.id(),
                Optional.empty(),
                persistedVoters(voters.voterIds(), kraftVersion)
            ),
            store.readElectionState().get()
        );
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testProspectiveToFollowerHigherEpoch(KRaftVersion kraftVersion) {
        ReplicaKey voter1 = ReplicaKey.of(1, Uuid.randomUuid());
        ReplicaKey voter2 = ReplicaKey.of(2, Uuid.randomUuid());
        VoterSet voters = VoterSetTest.voterSet(Stream.of(localVoterKey, voter1, voter2));
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToProspective();
        state.transitionToFollower(state.epoch() + 1, voter1.id(), voters.listeners(voter1.id()));
        assertEquals(
            ElectionState.withElectedLeader(
                1,
                voter1.id(),
                Optional.empty(),
                persistedVoters(voters.voterIds(), kraftVersion)
            ),
            store.readElectionState().get()
        );
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testProspectiveToProspective(KRaftVersion kraftVersion) {
        int leaderId = 1;
        int followerId = 2;
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(leaderId, followerId), kraftVersion);

        store.writeElectionState(ElectionState.withUnknownLeader(logEndEpoch, voters.voterIds()), kraftVersion);
        QuorumState state = buildQuorumState(OptionalInt.of(localId), voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToProspective();
        assertTrue(state.isProspective());
        assertThrows(IllegalStateException.class, state::transitionToProspective);
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testProspectiveToCandidate(KRaftVersion kraftVersion) {
        int leaderId = 1;
        int followerId = 2;
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(leaderId, followerId), kraftVersion);

        store.writeElectionState(ElectionState.withUnknownLeader(logEndEpoch, voters.voterIds()), kraftVersion);
        QuorumState state = buildQuorumState(OptionalInt.of(localId), voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        int jitterMs1 = 2500;
        random.mockNextInt(electionTimeoutMs, jitterMs1);
        state.transitionToProspective();
        assertFalse(state.hasLeader());
        assertEquals(
            electionTimeoutMs + jitterMs1,
            state.prospectiveStateOrThrow().remainingElectionTimeMs(time.milliseconds())
        );

        int jitterMs2 = 3000;
        random.mockNextInt(electionTimeoutMs, jitterMs2);
        state.transitionToCandidate();
        assertEquals(
            electionTimeoutMs + jitterMs2,
            state.candidateStateOrThrow().remainingElectionTimeMs(time.milliseconds())
        );
        assertEquals(
            ElectionState.withVotedCandidate(
                logEndEpoch + 1,
                persistedVotedKey(localVoterKey, kraftVersion),
                persistedVoters(voters.voterIds(), kraftVersion)
            ),
            store.readElectionState().get()
        );
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testProspectiveToLeaderOrResigned(KRaftVersion kraftVersion) {
        ReplicaKey voter1 = ReplicaKey.of(1, Uuid.randomUuid());
        ReplicaKey voter2 = ReplicaKey.of(2, Uuid.randomUuid());
        VoterSet voters = VoterSetTest.voterSet(Stream.of(localVoterKey, voter1, voter2));
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToProspective();

        // in same epoch
        assertThrows(IllegalStateException.class, () -> state.transitionToLeader(logEndEpoch, accumulator));
        assertThrows(IllegalStateException.class, () -> state.transitionToResigned(Collections.emptyList()));

        // in higher epoch
        assertThrows(IllegalStateException.class, () -> state.transitionToLeader(10, accumulator));
    }

    /**
     * Transitions from Prospective with votedKey
     */
    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testProspectiveVotedToUnattachedInSameEpoch(KRaftVersion kraftVersion) {
        ReplicaKey voter1 = ReplicaKey.of(1, Uuid.randomUuid());
        ReplicaKey voter2 = ReplicaKey.of(2, Uuid.randomUuid());
        VoterSet voters = VoterSetTest.voterSet(Stream.of(localVoterKey, voter1, voter2));
        store.writeElectionState(ElectionState.withUnknownLeader(logEndEpoch, voters.voterIds()), kraftVersion);
        QuorumState state = buildQuorumState(OptionalInt.of(localId), voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToProspective();
        state.prospectiveAddVotedState(logEndEpoch, voter1);

        state.transitionToUnattached(logEndEpoch, OptionalInt.empty());
        assertEquals(
            ElectionState.withVotedCandidate(
                logEndEpoch,
                persistedVotedKey(voter1, kraftVersion),
                persistedVoters(voters.voterIds(), kraftVersion)
            ),
            store.readElectionState().get()
        );
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testProspectiveVotedToAndFromFollowerSameEpoch(KRaftVersion kraftVersion) {
        ReplicaKey voter1 = ReplicaKey.of(1, Uuid.randomUuid());
        ReplicaKey voter2 = ReplicaKey.of(2, Uuid.randomUuid());
        VoterSet voters = VoterSetTest.voterSet(Stream.of(localVoterKey, voter1, voter2));
        store.writeElectionState(ElectionState.withUnknownLeader(logEndEpoch, voters.voterIds()), kraftVersion);
        QuorumState state = buildQuorumState(OptionalInt.of(localId), voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToProspective();
        state.prospectiveAddVotedState(logEndEpoch, voter1);

        // transition to follower of voter2 with votedKey voter1
        state.transitionToFollower(state.epoch(), voter2.id(), voters.listeners(voter2.id()));
        assertEquals(
            ElectionState.withElectedLeader(
                logEndEpoch,
                voter2.id(),
                Optional.of(persistedVotedKey(voter1, kraftVersion)),
                persistedVoters(voters.voterIds(), kraftVersion)
            ),
            store.readElectionState().get()
        );

        // transition back to prospective
        state.transitionToProspective();
        assertEquals(
            ElectionState.withElectedLeader(
                logEndEpoch,
                voter2.id(),
                Optional.of(persistedVotedKey(voter1, kraftVersion)),
                persistedVoters(voters.voterIds(), kraftVersion)
            ),
            store.readElectionState().get()
        );
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testProspectiveVotedToCandidate(KRaftVersion kraftVersion) {
        int node1 = 1;
        Uuid node1DirectoryId = Uuid.randomUuid();
        int node2 = 2;
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(node1, node2), kraftVersion);
        store.writeElectionState(
            ElectionState.withVotedCandidate(
                logEndEpoch,
                ReplicaKey.of(node1, node1DirectoryId),
                voters.voterIds()
            ),
            kraftVersion
        );
        QuorumState state = buildQuorumState(OptionalInt.of(localId), voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        assertTrue(state.isUnattachedAndVoted());
        int jitterMs1 = 2500;
        random.mockNextInt(electionTimeoutMs, jitterMs1);
        state.transitionToProspective();
        ProspectiveState prospectiveState = state.prospectiveStateOrThrow();
        assertEquals(
            electionTimeoutMs + jitterMs1,
            prospectiveState.remainingElectionTimeMs(time.milliseconds())
        );
        assertTrue(prospectiveState.votedKey().isPresent());

        int jitterMs2 = 3000;
        random.mockNextInt(electionTimeoutMs, jitterMs2);
        state.transitionToCandidate();
        assertTrue(state.isCandidate());
        assertEquals(
            electionTimeoutMs + jitterMs2,
            state.candidateStateOrThrow().remainingElectionTimeMs(time.milliseconds())
        );
        assertEquals(logEndEpoch + 1, state.epoch());
        assertEquals(
            ElectionState.withVotedCandidate(
                logEndEpoch + 1,
                persistedVotedKey(localVoterKey, kraftVersion),
                persistedVoters(voters.voterIds(), kraftVersion)
            ),
            store.readElectionState().get()
        );
    }

    /**
     * Test transitions from Prospective with leader
     */

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testProspectiveWithLeaderToUnattachedInSameEpoch(KRaftVersion kraftVersion) {
        ReplicaKey voter1 = ReplicaKey.of(1, Uuid.randomUuid());
        ReplicaKey voter2 = ReplicaKey.of(2, Uuid.randomUuid());
        VoterSet voters = VoterSetTest.voterSet(Stream.of(localVoterKey, voter1, voter2));
        store.writeElectionState(
            ElectionState.withElectedLeader(logEndEpoch, voter1.id(), Optional.empty(), voters.voterIds()),
            kraftVersion
        );
        QuorumState state = buildQuorumState(OptionalInt.of(localId), voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToProspective();
        assertEquals(
            ElectionState.withElectedLeader(
                logEndEpoch,
                voter1.id(),
                Optional.empty(),
                persistedVoters(voters.voterIds(), kraftVersion)
            ),
            store.readElectionState().get()
        );

        state.transitionToUnattached(state.epoch(), OptionalInt.of(voter1.id()));
        assertEquals(
            ElectionState.withElectedLeader(
                logEndEpoch,
                voter1.id(),
                Optional.empty(),
                persistedVoters(voters.voterIds(), kraftVersion)
            ),
            store.readElectionState().get()
        );
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testProspectiveWithLeaderToUnattachedInHigherEpoch(KRaftVersion kraftVersion) {
        ReplicaKey voter1 = ReplicaKey.of(1, Uuid.randomUuid());
        ReplicaKey voter2 = ReplicaKey.of(2, Uuid.randomUuid());
        VoterSet voters = VoterSetTest.voterSet(Stream.of(localVoterKey, voter1, voter2));
        store.writeElectionState(
            ElectionState.withElectedLeader(logEndEpoch, voter1.id(), Optional.empty(), voters.voterIds()),
            kraftVersion
        );
        QuorumState state = buildQuorumState(OptionalInt.of(localId), voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToProspective();
        assertEquals(
            ElectionState.withElectedLeader(
                logEndEpoch,
                voter1.id(),
                Optional.empty(),
                persistedVoters(voters.voterIds(), kraftVersion)
            ),
            store.readElectionState().get()
        );

        // transition to unattached
        state.transitionToUnattached(10, OptionalInt.empty());
        assertEquals(
            ElectionState.withUnknownLeader(10, persistedVoters(voters.voterIds(), kraftVersion)),
            store.readElectionState().get()
        );
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testProspectiveWithLeaderToUnattachedWithLeaderInHigherEpoch(KRaftVersion kraftVersion) {
        ReplicaKey voter1 = ReplicaKey.of(1, Uuid.randomUuid());
        ReplicaKey voter2 = ReplicaKey.of(2, Uuid.randomUuid());
        VoterSet voters = VoterSetTest.voterSet(Stream.of(localVoterKey, voter1, voter2));
        store.writeElectionState(
            ElectionState.withElectedLeader(logEndEpoch, voter1.id(), Optional.empty(), voters.voterIds()),
            kraftVersion
        );
        QuorumState state = buildQuorumState(OptionalInt.of(localId), voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToProspective();
        assertEquals(
            ElectionState.withElectedLeader(
                logEndEpoch,
                voter1.id(),
                Optional.empty(),
                persistedVoters(voters.voterIds(), kraftVersion)
            ),
            store.readElectionState().get()
        );

        // transition to unattached with different leader state
        state.transitionToUnattached(10, OptionalInt.of(voter2.id()));
        assertEquals(
            ElectionState.withElectedLeader(
                10,
                voter2.id(),
                Optional.empty(),
                persistedVoters(voters.voterIds(), kraftVersion)
            ),
            store.readElectionState().get()
        );
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testProspectiveWithLeaderToFollowerSameEpoch(KRaftVersion kraftVersion) {
        ReplicaKey voter1 = ReplicaKey.of(1, Uuid.randomUuid());
        ReplicaKey voter2 = ReplicaKey.of(2, Uuid.randomUuid());
        VoterSet voters = VoterSetTest.voterSet(Stream.of(localVoterKey, voter1, voter2));
        store.writeElectionState(
            ElectionState.withElectedLeader(logEndEpoch, voter1.id(), Optional.empty(), voters.voterIds()),
            kraftVersion
        );
        QuorumState state = buildQuorumState(OptionalInt.of(localId), voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToProspective();

        state.transitionToFollower(state.epoch(), voter2.id(), voters.listeners(voter2.id()));
        assertEquals(
            ElectionState.withElectedLeader(
                logEndEpoch,
                voter2.id(),
                Optional.empty(),
                persistedVoters(voters.voterIds(), kraftVersion)
            ),
            store.readElectionState().get()
        );
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testProspectiveWithLeaderToFollowerHigherEpoch(KRaftVersion kraftVersion) {
        ReplicaKey voter1 = ReplicaKey.of(1, Uuid.randomUuid());
        ReplicaKey voter2 = ReplicaKey.of(2, Uuid.randomUuid());
        VoterSet voters = VoterSetTest.voterSet(Stream.of(localVoterKey, voter1, voter2));
        store.writeElectionState(
            ElectionState.withElectedLeader(logEndEpoch, voter1.id(), Optional.empty(), voters.voterIds()),
            kraftVersion
        );
        QuorumState state = buildQuorumState(OptionalInt.of(localId), voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToProspective();

        state.transitionToFollower(10, voter2.id(), voters.listeners(voter2.id()));
        assertEquals(
            ElectionState.withElectedLeader(
                10,
                voter2.id(),
                Optional.empty(),
                persistedVoters(voters.voterIds(), kraftVersion)
            ),
            store.readElectionState().get()
        );
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testProspectiveWithLeaderToCandidate(KRaftVersion kraftVersion) {
        int leaderId = 1;
        int followerId = 2;
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(leaderId, followerId), kraftVersion);

        store.writeElectionState(
            ElectionState.withElectedLeader(logEndEpoch, leaderId, Optional.empty(), voters.voterIds()),
            kraftVersion
        );
        QuorumState state = buildQuorumState(OptionalInt.of(localId), voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToProspective();
        assertTrue(state.isProspective());
        assertTrue(state.hasLeader());

        state.transitionToCandidate();
        assertTrue(state.isCandidate());
        assertEquals(logEndEpoch + 1, state.epoch());
        assertEquals(
            ElectionState.withVotedCandidate(
                logEndEpoch + 1,
                persistedVotedKey(localVoterKey, kraftVersion),
                persistedVoters(voters.voterIds(), kraftVersion)
            ),
            store.readElectionState().get()
        );
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testProspectiveVotedWithLeaderToUnattachedInSameEpoch(KRaftVersion kraftVersion) {
        ReplicaKey leader = ReplicaKey.of(1, Uuid.randomUuid());
        ReplicaKey candidate = ReplicaKey.of(2, Uuid.randomUuid());
        VoterSet voters = VoterSetTest.voterSet(Stream.of(localVoterKey, leader, candidate));
        store.writeElectionState(
            ElectionState.withElectedLeader(logEndEpoch, leader.id(), Optional.empty(), voters.voterIds()),
            kraftVersion
        );
        QuorumState state = buildQuorumState(OptionalInt.of(localId), voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToProspective();
        state.prospectiveAddVotedState(logEndEpoch, candidate);

        // transition to unattached with different leader state
        state.transitionToUnattached(state.epoch(), OptionalInt.of(candidate.id()));

        assertEquals(
            ElectionState.withElectedLeader(
                logEndEpoch,
                candidate.id(),
                Optional.of(persistedVotedKey(candidate, kraftVersion)),
                persistedVoters(voters.voterIds(), kraftVersion)
            ),
            store.readElectionState().get()
        );
    }

    /**
     * Test transitions from Candidate
     */
    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testCandidateToUnattachedSameEpoch(KRaftVersion kraftVersion) {
        int otherNodeId = 1;
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(otherNodeId), kraftVersion);
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToProspective();
        state.transitionToCandidate();

        assertThrows(IllegalStateException.class, () -> state.transitionToUnattached(state.epoch(), OptionalInt.empty()));
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testCandidateToUnattachedHigherEpoch(KRaftVersion kraftVersion) {
        int otherNodeId = 1;
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(otherNodeId), kraftVersion);
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToProspective();
        state.transitionToCandidate();

        state.transitionToUnattached(5, OptionalInt.empty());
        assertEquals(5, state.epoch());
        assertEquals(OptionalInt.empty(), state.leaderId());
        assertEquals(
            ElectionState.withUnknownLeader(5, persistedVoters(voters.voterIds(), kraftVersion)),
            store.readElectionState().get()
        );
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testCandidateToFollowerSameEpoch(KRaftVersion kraftVersion) {
        int otherNodeId = 1;

        VoterSet voters = localWithRemoteVoterSet(IntStream.of(otherNodeId), kraftVersion);
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToProspective();
        state.transitionToCandidate();

        state.transitionToFollower(state.epoch(), otherNodeId, voters.listeners(otherNodeId));
        assertEquals(1, state.epoch());
        assertEquals(OptionalInt.of(otherNodeId), state.leaderId());
        assertEquals(
            ElectionState.withElectedLeader(
                1,
                otherNodeId,
                Optional.of(persistedVotedKey(localVoterKey, kraftVersion)),
                persistedVoters(voters.voterIds(), kraftVersion)
            ),
            store.readElectionState().get()
        );
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testCandidateToFollowerHigherEpoch(KRaftVersion kraftVersion) {
        int otherNodeId = 1;

        VoterSet voters = localWithRemoteVoterSet(IntStream.of(otherNodeId), kraftVersion);
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToProspective();
        state.transitionToCandidate();

        state.transitionToFollower(5, otherNodeId, voters.listeners(otherNodeId));
        assertEquals(5, state.epoch());
        assertEquals(OptionalInt.of(otherNodeId), state.leaderId());
        assertEquals(
            ElectionState.withElectedLeader(
                5,
                otherNodeId,
                Optional.empty(),
                persistedVoters(voters.voterIds(), kraftVersion)
            ),
            store.readElectionState().get()
        );
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testCandidateToProspective(KRaftVersion kraftVersion) {
        int node1 = 1;
        int node2 = 2;
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(node1, node2), kraftVersion);
        assertEquals(Optional.empty(), store.readElectionState());

        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.transitionToProspective();
        int jitterMs1 = 2500;
        random.mockNextInt(electionTimeoutMs, jitterMs1);
        state.transitionToCandidate();
        assertTrue(state.isCandidate());
        assertEquals(1, state.epoch());
        CandidateState candidate = state.candidateStateOrThrow();
        assertEquals(
            electionTimeoutMs + jitterMs1,
            candidate.remainingElectionTimeMs(time.milliseconds())
        );

        // The election timeout should be reset after we transition to prospective
        time.sleep(candidate.remainingElectionTimeMs(time.milliseconds()));
        assertEquals(0, candidate.remainingElectionTimeMs(time.milliseconds()));
        int jitterMs2 = 3000;
        random.mockNextInt(electionTimeoutMs, jitterMs2);
        state.transitionToProspective();
        ProspectiveState prospective = state.prospectiveStateOrThrow();
        assertEquals(
            electionTimeoutMs + jitterMs2,
            prospective.remainingElectionTimeMs(time.milliseconds())
        );
        assertEquals(
            ElectionState.withVotedCandidate(
                1,
                persistedVotedKey(localVoterKey, kraftVersion),
                persistedVoters(voters.voterIds(), kraftVersion)
            ),
            store.readElectionState().get()
        );
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testCandidateToCandidate(KRaftVersion kraftVersion) {
        int node1 = 1;
        int node2 = 2;
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(node1, node2), kraftVersion);
        assertEquals(Optional.empty(), store.readElectionState());

        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.transitionToProspective();
        state.transitionToCandidate();
        assertTrue(state.isCandidate());
        assertThrows(IllegalStateException.class, () -> state.transitionToCandidate());
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testCandidateToLeader(KRaftVersion kraftVersion)  {
        VoterSet voters = localStandaloneVoterSet();
        assertEquals(Optional.empty(), store.readElectionState());

        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.transitionToProspective();
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
        state.transitionToProspective();
        state.transitionToCandidate();
        assertFalse(state.candidateStateOrThrow().epochElection().isVoteGranted());
        assertThrows(IllegalStateException.class, () -> state.transitionToLeader(0L, accumulator));
        state.candidateStateOrThrow().recordGrantedVote(otherNodeId);
        assertTrue(state.candidateStateOrThrow().epochElection().isVoteGranted());
        state.transitionToLeader(0L, accumulator);
        assertTrue(state.isLeader());
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testCandidateToResigned(KRaftVersion kraftVersion) {
        int node1 = 1;
        int node2 = 2;
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(node1, node2), kraftVersion);
        assertEquals(Optional.empty(), store.readElectionState());

        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.transitionToProspective();
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
    public void testCandidateToAnyStateLowerEpoch(KRaftVersion kraftVersion) {
        ReplicaKey otherNodeKey = ReplicaKey.of(1, Uuid.randomUuid());
        VoterSet voters = VoterSetTest.voterSet(Stream.of(localVoterKey, otherNodeKey));
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToUnattached(5, OptionalInt.empty());
        state.transitionToProspective();
        state.transitionToCandidate();
        assertThrows(IllegalStateException.class, () -> state.transitionToUnattached(4, OptionalInt.empty()));
        assertThrows(
            IllegalStateException.class,
            () -> state.transitionToFollower(4, otherNodeKey.id(), voters.listeners(otherNodeKey.id()))
        );
        assertEquals(6, state.epoch());
        assertEquals(
            ElectionState.withVotedCandidate(
                6,
                persistedVotedKey(localVoterKey, kraftVersion),
                persistedVoters(voters.voterIds(), kraftVersion)
            ),
            store.readElectionState().get()
        );
    }

    /**
     * Test transitions from Leader
     */
    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testLeaderToUnattachedSameEpoch(KRaftVersion kraftVersion) {
        int otherNodeId = 1;
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(otherNodeId), kraftVersion);
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToProspective();
        state.transitionToCandidate();
        state.candidateStateOrThrow().recordGrantedVote(otherNodeId);
        state.transitionToLeader(0L, accumulator);
        assertThrows(IllegalStateException.class, () -> state.transitionToUnattached(state.epoch(), OptionalInt.empty()));
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testLeaderToUnattachedHigherEpoch(KRaftVersion kraftVersion) {
        int otherNodeId = 1;
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(otherNodeId), kraftVersion);
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToProspective();
        state.transitionToCandidate();
        state.candidateStateOrThrow().recordGrantedVote(otherNodeId);
        state.transitionToLeader(0L, accumulator);
        state.transitionToUnattached(5, OptionalInt.empty());
        assertEquals(5, state.epoch());
        assertEquals(OptionalInt.empty(), state.leaderId());
        assertEquals(
            ElectionState.withUnknownLeader(5, persistedVoters(voters.voterIds(), kraftVersion)),
            store.readElectionState().get()
        );
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testLeaderToFollowerSameEpoch(KRaftVersion kraftVersion) {
        int otherNodeId = 1;
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(otherNodeId), kraftVersion);

        QuorumState state = initializeEmptyState(voters, kraftVersion);

        state.transitionToProspective();
        state.transitionToCandidate();
        state.candidateStateOrThrow().recordGrantedVote(otherNodeId);
        state.transitionToLeader(0L, accumulator);
        assertThrows(
            IllegalStateException.class,
            () -> state.transitionToFollower(state.epoch(), otherNodeId, voters.listeners(otherNodeId))
        );
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testLeaderToFollowerHigherEpoch(KRaftVersion kraftVersion) {
        int otherNodeId = 1;
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(otherNodeId), kraftVersion);

        QuorumState state = initializeEmptyState(voters, kraftVersion);

        state.transitionToProspective();
        state.transitionToCandidate();
        state.candidateStateOrThrow().recordGrantedVote(otherNodeId);
        state.transitionToLeader(0L, accumulator);
        state.transitionToFollower(5, otherNodeId, voters.listeners(otherNodeId));

        assertEquals(5, state.epoch());
        assertEquals(OptionalInt.of(otherNodeId), state.leaderId());
        assertEquals(
            ElectionState.withElectedLeader(
                5,
                otherNodeId,
                Optional.empty(),
                persistedVoters(voters.voterIds(), kraftVersion)
            ),
            store.readElectionState().get()
        );
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testLeaderToProspective(KRaftVersion kraftVersion) {
        VoterSet voters = localStandaloneVoterSet();
        assertEquals(Optional.empty(), store.readElectionState());

        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToProspective();
        state.transitionToCandidate();
        state.transitionToLeader(0L, accumulator);
        assertTrue(state.isLeader());
        assertEquals(1, state.epoch());

        assertThrows(IllegalStateException.class, state::transitionToProspective);
        assertTrue(state.isLeader());
        assertEquals(1, state.epoch());
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testLeaderToCandidate(KRaftVersion kraftVersion) {
        VoterSet voters = localStandaloneVoterSet();
        assertEquals(Optional.empty(), store.readElectionState());

        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToProspective();
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
    public void testLeaderToLeader(KRaftVersion kraftVersion) {
        VoterSet voters = localStandaloneVoterSet();
        assertEquals(Optional.empty(), store.readElectionState());

        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToProspective();
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
        state.transitionToProspective();
        state.transitionToCandidate();
        state.transitionToLeader(0L, accumulator);
        assertTrue(state.isLeader());
        assertEquals(1, state.epoch());

        state.transitionToResigned(Collections.singletonList(localVoterKey));
        assertTrue(state.isResigned());
        ResignedState resignedState = state.resignedStateOrThrow();
        assertEquals(
            ElectionState.withElectedLeader(1, localId, Optional.empty(), voters.voterIds()),
            resignedState.election()
        );
        assertEquals(1, resignedState.epoch());
        assertEquals(Collections.emptySet(), resignedState.unackedVoters());
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testLeaderToAnyStateLowerEpoch(KRaftVersion kraftVersion) {
        ReplicaKey otherNodeKey = ReplicaKey.of(1, Uuid.randomUuid());
        VoterSet voters = VoterSetTest.voterSet(Stream.of(localVoterKey, otherNodeKey));
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToUnattached(5, OptionalInt.empty());
        state.transitionToProspective();
        state.transitionToCandidate();
        state.candidateStateOrThrow().recordGrantedVote(otherNodeKey.id());
        state.transitionToLeader(0L, accumulator);
        assertThrows(IllegalStateException.class, () -> state.transitionToUnattached(4, OptionalInt.empty()));
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
            ElectionState.withElectedLeader(
                6,
                localId,
                Optional.empty(),
                persistedVoters(voters.voterIds(), kraftVersion)
            ),
            store.readElectionState().get()
        );
    }

    /**
     * Test transitions from Resigned
     */
    // KAFKA-18379 to fill in the rest of the cases
    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testResignedToFollowerInSameEpoch(KRaftVersion kraftVersion) {
        int node1 = 1;
        int node2 = 2;
        int epoch = 5;
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(node1, node2), kraftVersion);
        ElectionState election = ElectionState.withElectedLeader(epoch, localId, Optional.empty(), voters.voterIds());
        store.writeElectionState(election, kraftVersion);
        QuorumState state = buildQuorumState(OptionalInt.of(localId), voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        assertTrue(state.isResigned());
        assertThrows(IllegalStateException.class, () -> state.transitionToFollower(epoch, localId, voters.listeners(localId)));
        // KAFKA-18379 will fix this
        state.transitionToFollower(epoch, node1, voters.listeners(node1));
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testResignedToUnattachedInHigherEpoch(KRaftVersion kraftVersion) {
        int node1 = 1;
        int node2 = 2;
        int epoch = 5;
        VoterSet voters = localWithRemoteVoterSet(IntStream.of(node1, node2), kraftVersion);
        ElectionState election = ElectionState.withElectedLeader(epoch, localId, Optional.empty(), voters.voterIds());
        store.writeElectionState(election, kraftVersion);
        QuorumState state = buildQuorumState(OptionalInt.of(localId), voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        assertTrue(state.isResigned());
        state.transitionToUnattached(epoch + 1, OptionalInt.empty());
    }

    /**
     * Test transitions from Observer as Unattached
     */
    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testObserverUnattachedToUnattachedVoted(KRaftVersion kraftVersion) {
        ReplicaKey otherNodeKey = ReplicaKey.of(1, Uuid.randomUuid());
        VoterSet voters = VoterSetTest.voterSet(Stream.of(otherNodeKey));

        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, 5));
        assertTrue(state.isObserver());

        state.unattachedAddVotedState(5, otherNodeKey);

        UnattachedState votedState = state.unattachedStateOrThrow();
        assertTrue(state.isUnattachedAndVoted());
        assertTrue(state.isObserver());
        assertEquals(5, votedState.epoch());
        assertEquals(otherNodeKey, votedState.votedKey().get());
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

        state.transitionToUnattached(2, OptionalInt.empty());
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
    public void testObserverUnattachedToProspective(KRaftVersion kraftVersion) {
        int voter1 = 1;
        int voter2 = 2;
        VoterSet voters = withRemoteVoterSet(IntStream.of(voter1, voter2), kraftVersion);
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));

        assertTrue(state.isObserver());
        assertTrue(state.isUnattachedNotVoted());
        assertThrows(IllegalStateException.class, state::transitionToProspective);

        state.unattachedAddVotedState(logEndEpoch, ReplicaKey.of(voter1, ReplicaKey.NO_DIRECTORY_ID));
        assertTrue(state.isUnattachedAndVoted());
        assertTrue(state.isObserver());
        assertThrows(IllegalStateException.class, state::transitionToProspective);

        state.transitionToFollower(logEndEpoch, voter2, voters.listeners(voter2));
        assertTrue(state.isFollower());
        assertTrue(state.isObserver());
        assertTrue(state.votedKey().isPresent());
        assertTrue(state.hasLeader());
        assertThrows(IllegalStateException.class, state::transitionToProspective);
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testObserverUnattachedToCandidateOrLeaderOrResigned(KRaftVersion kraftVersion) {
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
        assertThrows(IllegalStateException.class, () -> state.transitionToResigned(Collections.emptyList()));
    }

    /**
     * Test transitions from Observer as Follower
     */
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
        state.transitionToUnattached(3, OptionalInt.empty());
        assertTrue(state.isUnattached());
        UnattachedState unattachedState = state.unattachedStateOrThrow();
        assertEquals(3, unattachedState.epoch());

        // Observers can remain in the unattached state indefinitely until a leader is found
        assertEquals(Long.MAX_VALUE, unattachedState.electionTimeoutMs());
    }

    @ParameterizedTest
    @EnumSource(value = KRaftVersion.class)
    public void testObserverFollowerToProspectiveOrCandidateOrLeaderOrResigned(KRaftVersion kraftVersion) {
        int voter1 = 1;
        int voter2 = 2;
        VoterSet voters = withRemoteVoterSet(IntStream.of(voter1, voter2), kraftVersion);
        QuorumState state = initializeEmptyState(voters, kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));

        state.transitionToFollower(logEndEpoch, voter1, voters.listeners(voter1));
        assertTrue(state.isObserver());
        assertEquals(fetchTimeoutMs, state.followerStateOrThrow().remainingFetchTimeMs(time.milliseconds()));

        assertThrows(IllegalStateException.class, state::transitionToProspective);
        assertThrows(IllegalStateException.class, state::transitionToCandidate);
        assertThrows(IllegalStateException.class, () -> state.transitionToLeader(logEndEpoch + 1, accumulator));
        assertThrows(IllegalStateException.class, () -> state.transitionToResigned(Collections.emptyList()));
    }
}
