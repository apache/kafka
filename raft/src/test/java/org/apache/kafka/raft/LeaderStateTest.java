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
import org.apache.kafka.raft.internals.BatchAccumulator;
import org.apache.kafka.server.common.KRaftVersion;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.apache.kafka.raft.LeaderState.CHECK_QUORUM_TIMEOUT_FACTOR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LeaderStateTest {
    private final ReplicaKey localReplicaKey = ReplicaKey.of(0, Uuid.randomUuid());
    private final int epoch = 5;
    private final LogContext logContext = new LogContext();
    private final BatchAccumulator<?> accumulator = Mockito.mock(BatchAccumulator.class);
    private final MockTime time = new MockTime();
    private final int fetchTimeoutMs = 2000;
    private final int checkQuorumTimeoutMs = (int) (fetchTimeoutMs * CHECK_QUORUM_TIMEOUT_FACTOR);
    private final int beginQuorumEpochTimeoutMs = fetchTimeoutMs / 2;
    private final KRaftVersion kraftVersion = KRaftVersion.KRAFT_VERSION_1;

    private LeaderState<?> newLeaderState(
        VoterSet voters,
        long epochStartOffset
    ) {
        return new LeaderState<>(
            time,
            localReplicaKey,
            epoch,
            epochStartOffset,
            voters,
            OptionalLong.of(0L),
            kraftVersion,
            voters.voterIds(),
            accumulator,
            voters.listeners(localReplicaKey.id()),
            fetchTimeoutMs,
            logContext
        );
    }

    private VoterSet localWithRemoteVoterSet(IntStream remoteIds, boolean withDirectoryId) {
        Map<Integer, VoterSet.VoterNode> voters = VoterSetTest.voterMap(remoteIds, withDirectoryId);
        if (withDirectoryId) {
            voters.put(localReplicaKey.id(), VoterSetTest.voterNode(localReplicaKey));
        } else {
            voters.put(
                localReplicaKey.id(),
                VoterSetTest.voterNode(ReplicaKey.of(localReplicaKey.id(), ReplicaKey.NO_DIRECTORY_ID))
            );
        }

        return VoterSetTest.voterSet(voters);
    }

    private VoterSet localWithRemoteVoterSet(Stream<ReplicaKey> remoteReplicaKeys, boolean withDirectoryId) {
        ReplicaKey actualLocalVoter = withDirectoryId ?
            localReplicaKey :
            ReplicaKey.of(localReplicaKey.id(), ReplicaKey.NO_DIRECTORY_ID);

        return VoterSetTest.voterSet(
            Stream.concat(Stream.of(actualLocalVoter), remoteReplicaKeys)
        );
    }

    @Test
    public void testRequireNonNullAccumulator() {
        VoterSet voterSet = VoterSetTest.voterSet(Stream.of(localReplicaKey));
        assertThrows(
            NullPointerException.class,
            () -> new LeaderState<>(
                new MockTime(),
                localReplicaKey,
                epoch,
                0,
                voterSet,
                OptionalLong.of(0),
                kraftVersion,
                Collections.emptySet(),
                null,
                Endpoints.empty(),
                fetchTimeoutMs,
                logContext
            )
        );
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testFollowerAcknowledgement(boolean withDirectoryId) {
        ReplicaKey node1 = replicaKey(1, withDirectoryId);
        ReplicaKey node2 = replicaKey(2, withDirectoryId);
        LeaderState<?> state = newLeaderState(
            localWithRemoteVoterSet(Stream.of(node1, node2), withDirectoryId),
            0L
        );
        assertEquals(Set.of(node1, node2), state.nonAcknowledgingVoters());
        state.addAcknowledgementFrom(node1.id());
        assertEquals(singleton(node2), state.nonAcknowledgingVoters());
        state.addAcknowledgementFrom(node2.id());
        assertEquals(emptySet(), state.nonAcknowledgingVoters());
    }

    @Test
    public void testNonFollowerAcknowledgement() {
        int nonVoterId = 1;
        LeaderState<?> state = newLeaderState(
            VoterSetTest.voterSet(Stream.of(localReplicaKey)),
            0L
        );
        assertThrows(IllegalArgumentException.class, () -> state.addAcknowledgementFrom(nonVoterId));
    }

    @Test
    public void testUpdateHighWatermarkQuorumSizeOne() {
        VoterSet voters = VoterSetTest.voterSet(Stream.of(localReplicaKey));
        LeaderState<?> state = newLeaderState(voters, 15L);

        assertEquals(Optional.empty(), state.highWatermark());
        assertFalse(state.updateLocalState(new LogOffsetMetadata(15L), voters));
        assertEquals(emptySet(), state.nonAcknowledgingVoters());
        assertEquals(Optional.empty(), state.highWatermark());
        assertTrue(state.updateLocalState(new LogOffsetMetadata(16L), voters));
        assertEquals(Optional.of(new LogOffsetMetadata(16L)), state.highWatermark());
        assertTrue(state.updateLocalState(new LogOffsetMetadata(20), voters));
        assertEquals(Optional.of(new LogOffsetMetadata(20L)), state.highWatermark());
    }

    @Test
    public void testNonMonotonicLocalEndOffsetUpdate() {
        VoterSet voters = VoterSetTest.voterSet(Stream.of(localReplicaKey));
        LeaderState<?> state = newLeaderState(voters, 15L);

        assertEquals(Optional.empty(), state.highWatermark());
        assertTrue(state.updateLocalState(new LogOffsetMetadata(16L), voters));
        assertEquals(Optional.of(new LogOffsetMetadata(16L)), state.highWatermark());
        assertThrows(
            IllegalStateException.class,
            () -> state.updateLocalState(new LogOffsetMetadata(15L), voters)
        );
    }

    @Test
    public void testIdempotentEndOffsetUpdate() {
        VoterSet voters = VoterSetTest.voterSet(Stream.of(localReplicaKey));
        LeaderState<?> state = newLeaderState(voters, 15L);
        assertEquals(Optional.empty(), state.highWatermark());
        assertTrue(state.updateLocalState(new LogOffsetMetadata(16L), voters));
        assertFalse(state.updateLocalState(new LogOffsetMetadata(16L), voters));
        assertEquals(Optional.of(new LogOffsetMetadata(16L)), state.highWatermark());
    }

    @Test
    public void testUpdateHighWatermarkMetadata() {
        VoterSet voters = VoterSetTest.voterSet(Stream.of(localReplicaKey));
        LeaderState<?> state = newLeaderState(voters, 15L);
        assertEquals(Optional.empty(), state.highWatermark());

        LogOffsetMetadata initialHw = new LogOffsetMetadata(16L, Optional.of(new MockOffsetMetadata("bar")));
        assertTrue(state.updateLocalState(initialHw, voters));
        assertEquals(Optional.of(initialHw), state.highWatermark());

        LogOffsetMetadata updateHw = new LogOffsetMetadata(16L, Optional.of(new MockOffsetMetadata("baz")));
        assertTrue(state.updateLocalState(updateHw, voters));
        assertEquals(Optional.of(updateHw), state.highWatermark());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testUpdateHighWatermarkQuorumSizeTwo(boolean withDirectoryId) {
        ReplicaKey otherNodeKey = replicaKey(1, withDirectoryId);

        VoterSet voters = localWithRemoteVoterSet(Stream.of(otherNodeKey), withDirectoryId);
        LeaderState<?> state = newLeaderState(voters, 10L);

        assertFalse(state.updateLocalState(new LogOffsetMetadata(13L), voters));
        assertEquals(singleton(otherNodeKey), state.nonAcknowledgingVoters());
        assertEquals(Optional.empty(), state.highWatermark());
        assertFalse(state.updateReplicaState(otherNodeKey, 0, new LogOffsetMetadata(10L)));
        assertEquals(emptySet(), state.nonAcknowledgingVoters());
        assertEquals(Optional.empty(), state.highWatermark());
        assertTrue(state.updateReplicaState(otherNodeKey, 0, new LogOffsetMetadata(11L)));
        assertEquals(Optional.of(new LogOffsetMetadata(11L)), state.highWatermark());
        assertTrue(state.updateReplicaState(otherNodeKey, 0, new LogOffsetMetadata(13L)));
        assertEquals(Optional.of(new LogOffsetMetadata(13L)), state.highWatermark());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testUpdateHighWatermarkQuorumSizeThree(boolean withDirectoryId) {
        ReplicaKey nodeKey1 = replicaKey(1, withDirectoryId);
        ReplicaKey nodeKey2 = replicaKey(2, withDirectoryId);

        VoterSet voters = localWithRemoteVoterSet(Stream.of(nodeKey1, nodeKey2), withDirectoryId);
        LeaderState<?> state = newLeaderState(voters, 10L);

        assertFalse(state.updateLocalState(new LogOffsetMetadata(15L), voters));
        assertEquals(Set.of(nodeKey1, nodeKey2), state.nonAcknowledgingVoters());
        assertEquals(Optional.empty(), state.highWatermark());
        assertFalse(state.updateReplicaState(nodeKey1, 0, new LogOffsetMetadata(10L)));
        assertEquals(singleton(nodeKey2), state.nonAcknowledgingVoters());
        assertEquals(Optional.empty(), state.highWatermark());
        assertFalse(state.updateReplicaState(nodeKey2, 0, new LogOffsetMetadata(10L)));
        assertEquals(emptySet(), state.nonAcknowledgingVoters());
        assertEquals(Optional.empty(), state.highWatermark());
        assertTrue(state.updateReplicaState(nodeKey2, 0, new LogOffsetMetadata(15L)));
        assertEquals(Optional.of(new LogOffsetMetadata(15L)), state.highWatermark());
        assertFalse(state.updateLocalState(new LogOffsetMetadata(20L), voters));
        assertEquals(Optional.of(new LogOffsetMetadata(15L)), state.highWatermark());
        assertTrue(state.updateReplicaState(nodeKey1, 0, new LogOffsetMetadata(20L)));
        assertEquals(Optional.of(new LogOffsetMetadata(20L)), state.highWatermark());
        assertFalse(state.updateReplicaState(nodeKey2, 0, new LogOffsetMetadata(20L)));
        assertEquals(Optional.of(new LogOffsetMetadata(20L)), state.highWatermark());
    }

    @Test
    public void testHighWatermarkDoesIncreaseFromNewVoter() {
        ReplicaKey nodeKey1 = ReplicaKey.of(1, Uuid.randomUuid());
        ReplicaKey nodeKey2 = ReplicaKey.of(2, Uuid.randomUuid());

        VoterSet originalVoters  = localWithRemoteVoterSet(Stream.of(nodeKey1), true);
        LeaderState<?> state = newLeaderState(originalVoters, 5L);

        assertFalse(state.updateLocalState(new LogOffsetMetadata(15L), originalVoters));
        assertTrue(state.updateReplicaState(nodeKey1, 0, new LogOffsetMetadata(10L)));
        assertEquals(Optional.of(new LogOffsetMetadata(10L)), state.highWatermark());

        // updating replica state of node2 before it joins voterSet should not increase HW to 15L
        assertFalse(state.updateReplicaState(nodeKey2, 0, new LogOffsetMetadata(15L)));
        assertEquals(Optional.of(new LogOffsetMetadata(10L)), state.highWatermark());

        // adding node2 to voterSet will cause HW to increase to 15L
        VoterSet votersWithNode2 = originalVoters.addVoter(VoterSetTest.voterNode(nodeKey2)).get();
        assertTrue(state.updateLocalState(new LogOffsetMetadata(15L), votersWithNode2));
        assertEquals(Optional.of(new LogOffsetMetadata(15L)), state.highWatermark());

        // HW will not update to 16L until a majority reaches it
        assertFalse(state.updateLocalState(new LogOffsetMetadata(16L), votersWithNode2));
        assertEquals(Optional.of(new LogOffsetMetadata(15L)), state.highWatermark());
        assertTrue(state.updateReplicaState(nodeKey2, 0, new LogOffsetMetadata(16L)));
        assertEquals(Optional.of(new LogOffsetMetadata(16L)), state.highWatermark());
    }

    @Test
    public void testHighWatermarkDoesNotDecreaseFromNewVoter() {
        ReplicaKey nodeKey1 = ReplicaKey.of(1, Uuid.randomUuid());
        ReplicaKey nodeKey2 = ReplicaKey.of(2, Uuid.randomUuid());
        ReplicaKey nodeKey3 = ReplicaKey.of(3, Uuid.randomUuid());

        // start with three voters with HW at 15L
        VoterSet originalVoters = localWithRemoteVoterSet(Stream.of(nodeKey1, nodeKey2), true);
        LeaderState<?> state = newLeaderState(originalVoters, 5L);

        assertFalse(state.updateLocalState(new LogOffsetMetadata(15L), originalVoters));
        assertTrue(state.updateReplicaState(nodeKey1, 0, new LogOffsetMetadata(15L)));
        assertEquals(Optional.of(new LogOffsetMetadata(15L)), state.highWatermark());
        assertFalse(state.updateReplicaState(nodeKey2, 0, new LogOffsetMetadata(10L)));

        // updating replica state of node3 before it joins voterSet
        assertFalse(state.updateReplicaState(nodeKey3, 0, new LogOffsetMetadata(10L)));

        // adding node3 to voterSet should not cause HW to decrease even if majority is < HW
        VoterSet votersWithNode3 = originalVoters.addVoter(VoterSetTest.voterNode(nodeKey3)).get();
        assertFalse(state.updateLocalState(new LogOffsetMetadata(16L), votersWithNode3));
        assertEquals(Optional.of(new LogOffsetMetadata(15L)), state.highWatermark());

        // HW will not decrease if calculated HW is anything lower than the last HW
        assertFalse(state.updateReplicaState(nodeKey2, 0, new LogOffsetMetadata(13L)));
        assertEquals(Optional.of(new LogOffsetMetadata(15L)), state.highWatermark());
        assertFalse(state.updateReplicaState(nodeKey3, 0, new LogOffsetMetadata(13L)));
        assertEquals(Optional.of(new LogOffsetMetadata(15L)), state.highWatermark());
        assertFalse(state.updateReplicaState(nodeKey1, 0, new LogOffsetMetadata(16L)));
        assertEquals(Optional.of(new LogOffsetMetadata(15L)), state.highWatermark());

        // HW will update to 16L once a majority of the voterSet is at least 16L
        assertTrue(state.updateReplicaState(nodeKey3, 0, new LogOffsetMetadata(16L)));
        assertEquals(Optional.of(new LogOffsetMetadata(16L)), state.highWatermark());
    }

    @Test
    public void testUpdateHighWatermarkRemovingFollowerFromVoterStates() {
        ReplicaKey nodeKey1 = ReplicaKey.of(1, Uuid.randomUuid());
        ReplicaKey nodeKey2 = ReplicaKey.of(2, Uuid.randomUuid());

        VoterSet originalVoters = localWithRemoteVoterSet(Stream.of(nodeKey1, nodeKey2), true);
        LeaderState<?> state = newLeaderState(originalVoters, 10L);

        assertFalse(state.updateLocalState(new LogOffsetMetadata(15L), originalVoters));
        assertTrue(state.updateReplicaState(nodeKey1, 0, new LogOffsetMetadata(15L)));
        assertFalse(state.updateReplicaState(nodeKey2, 0, new LogOffsetMetadata(10L)));
        assertEquals(Optional.of(new LogOffsetMetadata(15L)), state.highWatermark());

        // removing node1 should not decrement HW to 10L
        VoterSet votersWithoutNode1 = originalVoters.removeVoter(nodeKey1).get();
        assertFalse(state.updateLocalState(new LogOffsetMetadata(17L), votersWithoutNode1));
        assertEquals(Optional.of(new LogOffsetMetadata(15L)), state.highWatermark());

        // HW cannot change until after node2 catches up to last HW
        assertFalse(state.updateReplicaState(nodeKey2, 0, new LogOffsetMetadata(14L)));
        assertEquals(Optional.of(new LogOffsetMetadata(15L)), state.highWatermark());
        assertFalse(state.updateLocalState(new LogOffsetMetadata(18L), votersWithoutNode1));
        assertEquals(Optional.of(new LogOffsetMetadata(15L)), state.highWatermark());
        assertFalse(state.updateReplicaState(nodeKey1, 0, new LogOffsetMetadata(18L)));
        assertEquals(Optional.of(new LogOffsetMetadata(15L)), state.highWatermark());
        assertFalse(state.updateReplicaState(nodeKey2, 0, new LogOffsetMetadata(15L)));
        assertEquals(Optional.of(new LogOffsetMetadata(15L)), state.highWatermark());

        // HW should update to 16L
        assertTrue(state.updateReplicaState(nodeKey2, 0, new LogOffsetMetadata(16L)));
        assertEquals(Optional.of(new LogOffsetMetadata(16L)), state.highWatermark());
    }

    @Test
    public void testUpdateHighWatermarkQuorumRemovingLeaderFromVoterStates() {
        ReplicaKey nodeKey1 = ReplicaKey.of(1, Uuid.randomUuid());
        ReplicaKey nodeKey2 = ReplicaKey.of(2, Uuid.randomUuid());

        VoterSet originalVoters = localWithRemoteVoterSet(Stream.of(nodeKey1, nodeKey2), true);
        LeaderState<?> state = newLeaderState(originalVoters, 10L);

        assertFalse(state.updateLocalState(new LogOffsetMetadata(15L), originalVoters));
        assertTrue(state.updateReplicaState(nodeKey1, 0, new LogOffsetMetadata(15L)));
        assertFalse(state.updateReplicaState(nodeKey2, 0, new LogOffsetMetadata(10L)));
        assertEquals(Optional.of(new LogOffsetMetadata(15L)), state.highWatermark());

        // removing leader should not decrement HW to 10L
        VoterSet votersWithoutLeader = originalVoters.removeVoter(localReplicaKey).get();
        assertFalse(state.updateLocalState(new LogOffsetMetadata(17L), votersWithoutLeader));
        assertEquals(Optional.of(new LogOffsetMetadata(15L)), state.highWatermark());

        // HW cannot change until node2 catches up to last HW
        assertFalse(state.updateReplicaState(nodeKey1, 0, new LogOffsetMetadata(16L)));
        assertEquals(Optional.of(new LogOffsetMetadata(15L)), state.highWatermark());
        assertFalse(state.updateLocalState(new LogOffsetMetadata(18L), votersWithoutLeader));
        assertEquals(Optional.of(new LogOffsetMetadata(15L)), state.highWatermark());
        assertFalse(state.updateReplicaState(nodeKey2, 0, new LogOffsetMetadata(14L)));
        assertEquals(Optional.of(new LogOffsetMetadata(15L)), state.highWatermark());
        assertFalse(state.updateReplicaState(nodeKey2, 0, new LogOffsetMetadata(15L)));
        assertEquals(Optional.of(new LogOffsetMetadata(15L)), state.highWatermark());

        // HW will not update to 16L until the majority of remaining voterSet (node1, node2) are at least 16L
        assertTrue(state.updateReplicaState(nodeKey2, 0, new LogOffsetMetadata(16L)));
        assertEquals(Optional.of(new LogOffsetMetadata(16L)), state.highWatermark());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testNonMonotonicHighWatermarkUpdate(boolean withDirectoryId) {
        MockTime time = new MockTime();
        ReplicaKey nodeKey1 = replicaKey(1, withDirectoryId);

        VoterSet voters = localWithRemoteVoterSet(Stream.of(nodeKey1), withDirectoryId);
        LeaderState<?> state = newLeaderState(voters, 0L);

        state.updateLocalState(new LogOffsetMetadata(10L), voters);
        state.updateReplicaState(nodeKey1, time.milliseconds(), new LogOffsetMetadata(10L));
        assertEquals(Optional.of(new LogOffsetMetadata(10L)), state.highWatermark());

        // Follower crashes and disk is lost. It fetches an earlier offset to rebuild state.
        // The leader will report an error in the logs, but will not let the high watermark rewind
        assertFalse(state.updateReplicaState(nodeKey1, time.milliseconds(), new LogOffsetMetadata(5L)));
        assertEquals(Optional.of(new LogOffsetMetadata(10L)), state.highWatermark());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testGetNonLeaderFollowersByFetchOffsetDescending(boolean withDirectoryId) {
        ReplicaKey nodeKey1 = replicaKey(1, withDirectoryId);
        ReplicaKey nodeKey2 = replicaKey(2, withDirectoryId);
        long leaderStartOffset = 10L;
        long leaderEndOffset = 15L;

        VoterSet voters = localWithRemoteVoterSet(Stream.of(nodeKey1, nodeKey2), withDirectoryId);
        LeaderState<?> state = newLeaderState(voters, leaderStartOffset);

        state.updateLocalState(new LogOffsetMetadata(leaderEndOffset), voters);
        assertEquals(Optional.empty(), state.highWatermark());
        state.updateReplicaState(nodeKey1, 0, new LogOffsetMetadata(leaderStartOffset));
        state.updateReplicaState(nodeKey2, 0, new LogOffsetMetadata(leaderEndOffset));

        // Leader should not be included; the follower with larger offset should be prioritized.
        assertEquals(
            Arrays.asList(nodeKey2, nodeKey1),
            state.nonLeaderVotersByDescendingFetchOffset()
        );
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testCheckQuorum(boolean withDirectoryId) {
        ReplicaKey nodeKey1 = replicaKey(1, withDirectoryId);
        ReplicaKey nodeKey2 = replicaKey(2, withDirectoryId);
        ReplicaKey nodeKey3 = replicaKey(3, withDirectoryId);
        ReplicaKey nodeKey4 = replicaKey(4, withDirectoryId);
        ReplicaKey observerKey5 = replicaKey(5, withDirectoryId);

        VoterSet voters = localWithRemoteVoterSet(
            Stream.of(nodeKey1, nodeKey2, nodeKey3, nodeKey4),
            withDirectoryId
        );
        LeaderState<?> state = newLeaderState(voters, 0L);

        assertEquals(checkQuorumTimeoutMs, state.timeUntilCheckQuorumExpires(time.milliseconds()));
        int resignLeadershipTimeout = checkQuorumTimeoutMs;

        // checkQuorum timeout not exceeded, should not expire the timer
        time.sleep(resignLeadershipTimeout / 2);
        assertTrue(state.timeUntilCheckQuorumExpires(time.milliseconds()) > 0);

        // received fetch requests from 2 voter nodes, the timer should be reset
        state.updateCheckQuorumForFollowingVoter(nodeKey1, time.milliseconds());
        state.updateCheckQuorumForFollowingVoter(nodeKey2, time.milliseconds());
        assertEquals(checkQuorumTimeoutMs, state.timeUntilCheckQuorumExpires(time.milliseconds()));

        // Since the timer was reset, it won't expire this time.
        time.sleep(resignLeadershipTimeout / 2);
        long remainingMs = state.timeUntilCheckQuorumExpires(time.milliseconds());
        assertTrue(remainingMs > 0);

        // received fetch requests from 1 voter and 1 observer nodes, the timer should not be reset.
        state.updateCheckQuorumForFollowingVoter(nodeKey3, time.milliseconds());
        state.updateCheckQuorumForFollowingVoter(observerKey5, time.milliseconds());
        assertEquals(remainingMs, state.timeUntilCheckQuorumExpires(time.milliseconds()));

        // This time, the checkQuorum timer will be expired
        time.sleep(resignLeadershipTimeout / 2);
        assertEquals(0, state.timeUntilCheckQuorumExpires(time.milliseconds()));
    }

    @Test
    public void testCheckQuorumAfterVoterSetChanges() {
        ReplicaKey nodeKey1 = ReplicaKey.of(1, Uuid.randomUuid());
        ReplicaKey nodeKey2 = ReplicaKey.of(2, Uuid.randomUuid());
        ReplicaKey nodeKey3 = ReplicaKey.of(3, Uuid.randomUuid());

        VoterSet originalVoters  = localWithRemoteVoterSet(Stream.of(nodeKey1, nodeKey2), true);
        LeaderState<?> state = newLeaderState(originalVoters, 0L);
        assertEquals(checkQuorumTimeoutMs, state.timeUntilCheckQuorumExpires(time.milliseconds()));

        // checkQuorum timeout not exceeded, should not expire the timer
        time.sleep(checkQuorumTimeoutMs / 2);
        assertEquals(checkQuorumTimeoutMs / 2, state.timeUntilCheckQuorumExpires(time.milliseconds()));

        // received fetch request from 1 voter node, the timer should be reset
        state.updateCheckQuorumForFollowingVoter(nodeKey1, time.milliseconds());
        assertEquals(checkQuorumTimeoutMs, state.timeUntilCheckQuorumExpires(time.milliseconds()));

        // Adding 1 new voter to the voter set
        VoterSet votersWithNode3 = originalVoters.addVoter(VoterSetTest.voterNode(nodeKey3)).get();
        state.updateLocalState(new LogOffsetMetadata(1L), votersWithNode3);

        time.sleep(checkQuorumTimeoutMs / 2);
        // received fetch request from 1 voter node, the timer should not be reset because the majority should be 3
        state.updateCheckQuorumForFollowingVoter(nodeKey1, time.milliseconds());
        assertEquals(checkQuorumTimeoutMs / 2, state.timeUntilCheckQuorumExpires(time.milliseconds()));

        // Timer should be reset after receiving another voter's fetch request
        state.updateCheckQuorumForFollowingVoter(nodeKey2, time.milliseconds());
        assertEquals(checkQuorumTimeoutMs, state.timeUntilCheckQuorumExpires(time.milliseconds()));

        // removing leader from the voter set
        VoterSet votersWithoutLeader = votersWithNode3.removeVoter(localReplicaKey).get();
        state.updateLocalState(new LogOffsetMetadata(1L), votersWithoutLeader);

        time.sleep(checkQuorumTimeoutMs / 2);
        // received fetch request from 1 voter, the timer should not be reset.
        state.updateCheckQuorumForFollowingVoter(nodeKey2, time.milliseconds());
        assertEquals(checkQuorumTimeoutMs / 2, state.timeUntilCheckQuorumExpires(time.milliseconds()));

        // received fetch request from another voter, the timer should be reset since the current quorum majority is 2.
        state.updateCheckQuorumForFollowingVoter(nodeKey1, time.milliseconds());
        assertEquals(checkQuorumTimeoutMs, state.timeUntilCheckQuorumExpires(time.milliseconds()));
    }

    @Test
    public void testCheckQuorumWithOneVoter() {
        int observer = 1;

        // Only 1 voter quorum
        LeaderState<?> state = newLeaderState(
            VoterSetTest.voterSet(Stream.of(localReplicaKey)),
            0L
        );
        assertEquals(Long.MAX_VALUE, state.timeUntilCheckQuorumExpires(time.milliseconds()));

        // When checkQuorum timeout not exceeded and got no fetch request from voter, it should not expire the timer
        time.sleep(checkQuorumTimeoutMs);
        assertEquals(Long.MAX_VALUE, state.timeUntilCheckQuorumExpires(time.milliseconds()));

        // received fetch requests from 1 observer node, the timer still return Long.MAX_VALUE.
        state.updateCheckQuorumForFollowingVoter(
            ReplicaKey.of(observer, ReplicaKey.NO_DIRECTORY_ID),
            time.milliseconds()
        );
        assertEquals(Long.MAX_VALUE, state.timeUntilCheckQuorumExpires(time.milliseconds()));
    }

    @Test
    public void testLeaderEndpoints() {
        VoterSet voters = VoterSetTest.voterSet(Stream.of(localReplicaKey));
        LeaderState<?> state = newLeaderState(voters, 0L);

        assertNotEquals(Endpoints.empty(), state.leaderEndpoints());
        assertEquals(voters.listeners(localReplicaKey.id()), state.leaderEndpoints());
    }

    @Test
    public void testUpdateVotersFromNoDirectoryIdToDirectoryId() {
        int node1 = 1;
        int node2 = 2;
        ReplicaKey nodeKey1 = ReplicaKey.of(node1, Uuid.randomUuid());
        ReplicaKey nodeKey2 = ReplicaKey.of(node2, Uuid.randomUuid());

        VoterSet votersBeforeUpgrade = localWithRemoteVoterSet(
            IntStream.of(node1, node2),
            false
        );

        LeaderState<?> state = newLeaderState(votersBeforeUpgrade, 0L);

        assertFalse(state.updateLocalState(new LogOffsetMetadata(10L), votersBeforeUpgrade));
        assertTrue(state.updateReplicaState(nodeKey1, 0L, new LogOffsetMetadata(10L)));
        assertEquals(Optional.of(new LogOffsetMetadata(10L)), state.highWatermark());

        VoterSet votersAfterUpgrade = localWithRemoteVoterSet(Stream.of(nodeKey1, nodeKey2), true);

        assertFalse(state.updateLocalState(new LogOffsetMetadata(15L), votersAfterUpgrade));
        assertTrue(state.updateReplicaState(nodeKey2, 0L, new LogOffsetMetadata(13L)));
        assertEquals(Optional.of(new LogOffsetMetadata(13L)), state.highWatermark());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testGrantVote(boolean isLogUpToDate) {
        LeaderState<?> state = newLeaderState(
            VoterSetTest.voterSet(VoterSetTest.voterMap(IntStream.of(1, 2, 3), false)),
            1
        );

        assertFalse(state.canGrantVote(ReplicaKey.of(1, ReplicaKey.NO_DIRECTORY_ID), isLogUpToDate, true));
        assertFalse(state.canGrantVote(ReplicaKey.of(2, ReplicaKey.NO_DIRECTORY_ID), isLogUpToDate, true));
        assertFalse(state.canGrantVote(ReplicaKey.of(3, ReplicaKey.NO_DIRECTORY_ID), isLogUpToDate, true));

        assertFalse(state.canGrantVote(ReplicaKey.of(1, ReplicaKey.NO_DIRECTORY_ID), isLogUpToDate, false));
        assertFalse(state.canGrantVote(ReplicaKey.of(2, ReplicaKey.NO_DIRECTORY_ID), isLogUpToDate, false));
        assertFalse(state.canGrantVote(ReplicaKey.of(3, ReplicaKey.NO_DIRECTORY_ID), isLogUpToDate, false));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testBeginQuorumEpochTimer(boolean withDirectoryId) {
        int follower1 = 1;
        long epochStartOffset = 10L;

        VoterSet voters = localWithRemoteVoterSet(IntStream.of(follower1), withDirectoryId);
        LeaderState<?> state = newLeaderState(
            voters,
            epochStartOffset
        );
        assertEquals(0, state.timeUntilBeginQuorumEpochTimerExpires(time.milliseconds()));

        time.sleep(5);
        state.resetBeginQuorumEpochTimer(time.milliseconds());
        assertEquals(beginQuorumEpochTimeoutMs, state.timeUntilBeginQuorumEpochTimerExpires(time.milliseconds()));

        time.sleep(5);
        assertEquals(beginQuorumEpochTimeoutMs - 5, state.timeUntilBeginQuorumEpochTimerExpires(time.milliseconds()));

        time.sleep(beginQuorumEpochTimeoutMs);
        assertEquals(0, state.timeUntilBeginQuorumEpochTimerExpires(time.milliseconds()));
    }

    private static class MockOffsetMetadata implements OffsetMetadata {
        private final String value;

        private MockOffsetMetadata(String value) {
            this.value = value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MockOffsetMetadata that = (MockOffsetMetadata) o;
            return Objects.equals(value, that.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(value);
        }
    }

    private ReplicaKey replicaKey(int id, boolean withDirectoryId) {
        Uuid directoryId = withDirectoryId ? Uuid.randomUuid() : ReplicaKey.NO_DIRECTORY_ID;
        return ReplicaKey.of(id, directoryId);
    }
}
