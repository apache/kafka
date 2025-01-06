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
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.OptionalInt;
import java.util.Set;

import static org.apache.kafka.raft.KafkaRaftClientTest.randomReplicaId;
import static org.apache.kafka.raft.KafkaRaftClientTest.replicaKey;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KafkaRaftClientPreVoteTest {
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testHandlePreVoteRequestAsFollowerWithElectedLeader(boolean hasFetchedFromLeader) throws Exception {
        int localId = randomReplicaId();
        int epoch = 2;
        ReplicaKey otherNodeKey = replicaKey(localId + 1, true);
        ReplicaKey observer = replicaKey(localId + 2, true);
        int electedLeaderId = localId + 2;
        Set<Integer> voters = Set.of(localId, otherNodeKey.id(), electedLeaderId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withElectedLeader(epoch, electedLeaderId)
            .withKip853Rpc(true)
            .build();

        if (hasFetchedFromLeader) {
            context.pollUntilRequest();
            RaftRequest.Outbound fetchRequest = context.assertSentFetchRequest();
            context.assertFetchRequestData(fetchRequest, epoch, 0L, 0);

            context.deliverResponse(
                fetchRequest.correlationId(),
                fetchRequest.destination(),
                context.fetchResponse(epoch, electedLeaderId, MemoryRecords.EMPTY, 0L, Errors.NONE)
            );
        }

        // follower should reject pre-vote requests with the same epoch if it has successfully fetched from the leader
        context.deliverRequest(context.preVoteRequest(epoch, otherNodeKey, epoch, 1));
        context.pollUntilResponse();

        boolean voteGranted = !hasFetchedFromLeader;
        context.assertSentVoteResponse(Errors.NONE, epoch, OptionalInt.of(electedLeaderId), voteGranted);
        context.assertElectedLeader(epoch, electedLeaderId);

        // same with observers
        context.deliverRequest(context.preVoteRequest(epoch, observer, epoch, 1));
        context.pollUntilResponse();

        context.assertSentVoteResponse(Errors.NONE, epoch, OptionalInt.of(electedLeaderId), voteGranted);
        context.assertElectedLeader(epoch, electedLeaderId);

        // follower will transition to unattached if pre-vote request has a higher epoch
        context.deliverRequest(context.preVoteRequest(epoch + 1, otherNodeKey, epoch + 1, 1));
        context.pollUntilResponse();

        context.assertSentVoteResponse(Errors.NONE, epoch + 1, OptionalInt.of(-1), true);
        assertEquals(context.currentEpoch(), epoch + 1);
        assertTrue(context.client.quorum().isUnattachedNotVoted());
    }

    @Test
    public void testHandlePreVoteRequestAsCandidate() throws Exception {
        int localId = randomReplicaId();
        ReplicaKey otherNodeKey = replicaKey(localId + 1, true);
        ReplicaKey observer = replicaKey(localId + 2, true);
        int leaderEpoch = 2;
        Set<Integer> voters = Set.of(localId, otherNodeKey.id());

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withVotedCandidate(leaderEpoch, ReplicaKey.of(localId, ReplicaKey.NO_DIRECTORY_ID))
            .withKip853Rpc(true)
            .build();
        assertTrue(context.client.quorum().isCandidate());

        // candidate should grant pre-vote requests with the same epoch if log is up-to-date
        context.deliverRequest(context.preVoteRequest(leaderEpoch, otherNodeKey, leaderEpoch, 1));
        context.pollUntilResponse();

        context.assertSentVoteResponse(Errors.NONE, leaderEpoch, OptionalInt.empty(), true);
        context.assertVotedCandidate(leaderEpoch, localId);
        assertTrue(context.client.quorum().isCandidate());

        // if an observer sends a pre-vote request for the same epoch, it should also be granted
        context.deliverRequest(context.preVoteRequest(leaderEpoch, observer, leaderEpoch, 1));
        context.pollUntilResponse();

        context.assertSentVoteResponse(Errors.NONE, leaderEpoch, OptionalInt.empty(), true);
        context.assertVotedCandidate(leaderEpoch, localId);
        assertTrue(context.client.quorum().isCandidate());

        // candidate will transition to unattached if pre-vote request has a higher epoch
        context.deliverRequest(context.preVoteRequest(leaderEpoch + 1, otherNodeKey, leaderEpoch + 1, 1));
        context.pollUntilResponse();

        context.assertSentVoteResponse(Errors.NONE, leaderEpoch + 1, OptionalInt.of(-1), true);
        assertTrue(context.client.quorum().isUnattached());
    }

    @Test
    public void testHandlePreVoteRequestAsUnattachedObserver() throws Exception {
        int localId = randomReplicaId();
        int epoch = 2;
        ReplicaKey replica1 = replicaKey(localId + 1, true);
        ReplicaKey replica2 = replicaKey(localId + 2, true);
        ReplicaKey observer = replicaKey(localId + 3, true);
        Set<Integer> voters = Set.of(replica1.id(), replica2.id());

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withUnknownLeader(epoch)
            .withKip853Rpc(true)
            .build();

        context.deliverRequest(context.preVoteRequest(epoch, replica1, epoch, 1));
        context.pollUntilResponse();

        assertTrue(context.client.quorum().isUnattached());
        context.assertSentVoteResponse(Errors.NONE, epoch, OptionalInt.empty(), true);

        // if same replica sends another pre-vote request for the same epoch, it should be granted
        context.deliverRequest(context.preVoteRequest(epoch, replica1, epoch, 1));
        context.pollUntilResponse();

        assertTrue(context.client.quorum().isUnattached());
        context.assertSentVoteResponse(Errors.NONE, epoch, OptionalInt.empty(), true);

        // if different replica sends a pre-vote request for the same epoch, it should be granted
        context.deliverRequest(context.preVoteRequest(epoch, replica2, epoch, 1));
        context.pollUntilResponse();

        assertTrue(context.client.quorum().isUnattached());
        context.assertSentVoteResponse(Errors.NONE, epoch, OptionalInt.empty(), true);

        // if an observer sends a pre-vote request for the same epoch, it should also be granted
        context.deliverRequest(context.preVoteRequest(epoch, observer, epoch, 1));
        context.pollUntilResponse();

        assertTrue(context.client.quorum().isUnattached());
        context.assertSentVoteResponse(Errors.NONE, epoch, OptionalInt.empty(), true);
    }

    @Test
    public void testHandlePreVoteRequestAsUnattachedVoted() throws Exception {
        int localId = randomReplicaId();
        int epoch = 2;
        ReplicaKey replica1 = replicaKey(localId + 1, true);
        ReplicaKey replica2 = replicaKey(localId + 2, true);
        ReplicaKey observer = replicaKey(localId + 3, true);
        Set<Integer> voters = Set.of(replica1.id(), replica2.id());

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withVotedCandidate(epoch, replica2)
            .withKip853Rpc(true)
            .build();

        context.deliverRequest(context.preVoteRequest(epoch, replica1, epoch, 1));
        context.pollUntilResponse();

        assertTrue(context.client.quorum().isUnattachedAndVoted());
        context.assertSentVoteResponse(Errors.NONE, epoch, OptionalInt.empty(), true);

        // if same replica sends another pre-vote request for the same epoch, it should be granted
        context.deliverRequest(context.preVoteRequest(epoch, replica1, epoch, 1));
        context.pollUntilResponse();

        assertTrue(context.client.quorum().isUnattached());
        context.assertSentVoteResponse(Errors.NONE, epoch, OptionalInt.empty(), true);

        // if different replica sends a pre-vote request for the same epoch, it should be granted
        context.deliverRequest(context.preVoteRequest(epoch, replica2, epoch, 1));
        context.pollUntilResponse();

        assertTrue(context.client.quorum().isUnattached());
        context.assertSentVoteResponse(Errors.NONE, epoch, OptionalInt.empty(), true);

        // if an observer sends a pre-vote request for the same epoch, it should also be granted
        context.deliverRequest(context.preVoteRequest(epoch, observer, epoch, 1));
        context.pollUntilResponse();

        assertTrue(context.client.quorum().isUnattached());
        context.assertSentVoteResponse(Errors.NONE, epoch, OptionalInt.empty(), true);
    }

    @Test
    public void testHandlePreVoteRequestAsUnattachedWithLeader() throws Exception {
        int localId = randomReplicaId();
        int epoch = 2;
        ReplicaKey replica1 = replicaKey(localId + 1, true);
        ReplicaKey replica2 = replicaKey(localId + 2, true);
        ReplicaKey leader = replicaKey(localId + 3, true);
        ReplicaKey observer = replicaKey(localId + 4, true);
        Set<Integer> voters = Set.of(replica1.id(), replica2.id());

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withElectedLeader(epoch, leader.id())
            .withKip853Rpc(true)
            .build();

        context.deliverRequest(context.preVoteRequest(epoch, replica1, epoch, 1));
        context.pollUntilResponse();

        assertTrue(context.client.quorum().isUnattachedNotVoted());
        context.assertSentVoteResponse(Errors.NONE, epoch, OptionalInt.of(leader.id()), true);

        // if same replica sends another pre-vote request for the same epoch, it should be granted
        context.deliverRequest(context.preVoteRequest(epoch, replica1, epoch, 1));
        context.pollUntilResponse();

        assertTrue(context.client.quorum().isUnattached());
        context.assertSentVoteResponse(Errors.NONE, epoch, OptionalInt.of(leader.id()), true);

        // if different replica sends a pre-vote request for the same epoch, it should be granted
        context.deliverRequest(context.preVoteRequest(epoch, replica2, epoch, 1));
        context.pollUntilResponse();

        assertTrue(context.client.quorum().isUnattached());
        context.assertSentVoteResponse(Errors.NONE, epoch, OptionalInt.of(leader.id()), true);

        // if an observer sends a pre-vote request for the same epoch, it should also be granted
        context.deliverRequest(context.preVoteRequest(epoch, observer, epoch, 1));
        context.pollUntilResponse();

        assertTrue(context.client.quorum().isUnattached());
        context.assertSentVoteResponse(Errors.NONE, epoch, OptionalInt.of(leader.id()), true);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testHandlePreVoteRequestAsFollowerObserver(boolean hasFetchedFromLeader) throws Exception {
        int localId = randomReplicaId();
        int epoch = 2;
        int leaderId = localId + 1;
        ReplicaKey leader = replicaKey(leaderId, true);
        ReplicaKey follower = replicaKey(localId + 2, true);
        Set<Integer> voters = Set.of(leader.id(), follower.id());

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withElectedLeader(epoch, leader.id())
            .withKip853Rpc(true)
            .build();
        context.assertElectedLeader(epoch, leader.id());

        if (hasFetchedFromLeader) {
            context.pollUntilRequest();
            RaftRequest.Outbound fetchRequest = context.assertSentFetchRequest();
            context.assertFetchRequestData(fetchRequest, epoch, 0L, 0);

            context.deliverResponse(
                fetchRequest.correlationId(),
                fetchRequest.destination(),
                context.fetchResponse(epoch, leaderId, MemoryRecords.EMPTY, 0L, Errors.NONE)
            );
        }

        context.deliverRequest(context.preVoteRequest(epoch, follower, epoch, 1));
        context.pollUntilResponse();

        boolean voteGranted = !hasFetchedFromLeader;
        context.assertSentVoteResponse(Errors.NONE, epoch, OptionalInt.of(leaderId), voteGranted);
        assertTrue(context.client.quorum().isFollower());
    }

    @Test
    public void testHandleInvalidPreVoteRequestWithOlderEpoch() throws Exception {
        int localId = randomReplicaId();
        int epoch = 2;
        ReplicaKey otherNodeKey = replicaKey(localId + 1, true);
        Set<Integer> voters = Set.of(localId, otherNodeKey.id());

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withUnknownLeader(epoch)
            .withKip853Rpc(true)
            .build();

        context.deliverRequest(context.preVoteRequest(epoch - 1, otherNodeKey, epoch - 1, 1));
        context.pollUntilResponse();

        context.assertSentVoteResponse(Errors.FENCED_LEADER_EPOCH, epoch, OptionalInt.empty(), false);
        context.assertUnknownLeader(epoch);
    }

    @Test
    public void testLeaderRejectPreVoteRequestOnSameEpoch() throws Exception {
        int localId = randomReplicaId();
        ReplicaKey otherNodeKey = replicaKey(localId + 1, true);
        Set<Integer> voters = Set.of(localId, otherNodeKey.id());

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withUnknownLeader(2)
            .withKip853Rpc(true)
            .build();

        context.becomeLeader();
        int leaderEpoch = context.currentEpoch();

        context.deliverRequest(context.preVoteRequest(leaderEpoch, otherNodeKey, leaderEpoch, 1));

        context.client.poll();

        context.assertSentVoteResponse(Errors.NONE, leaderEpoch, OptionalInt.of(localId), false);
        context.assertElectedLeader(leaderEpoch, localId);
    }

    @Test
    public void testPreVoteRequestClusterIdValidation() throws Exception {
        int localId = randomReplicaId();
        ReplicaKey otherNodeKey = replicaKey(localId + 1, true);
        Set<Integer> voters = Set.of(localId, otherNodeKey.id());

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withKip853Rpc(true)
            .build();

        context.becomeLeader();
        int epoch = context.currentEpoch();

        // valid cluster id is accepted
        context.deliverRequest(context.preVoteRequest(epoch, otherNodeKey, epoch, 0));
        context.pollUntilResponse();
        context.assertSentVoteResponse(Errors.NONE, epoch, OptionalInt.of(localId), false);

        // null cluster id is accepted
        context.deliverRequest(context.voteRequest(null, epoch, otherNodeKey, epoch, 0, true));
        context.pollUntilResponse();
        context.assertSentVoteResponse(Errors.NONE, epoch, OptionalInt.of(localId), false);

        // empty cluster id is rejected
        context.deliverRequest(context.voteRequest("", epoch, otherNodeKey, epoch, 0, true));
        context.pollUntilResponse();
        context.assertSentVoteResponse(Errors.INCONSISTENT_CLUSTER_ID);

        // invalid cluster id is rejected
        context.deliverRequest(context.voteRequest("invalid-uuid", epoch, otherNodeKey, epoch, 0, true));
        context.pollUntilResponse();
        context.assertSentVoteResponse(Errors.INCONSISTENT_CLUSTER_ID);
    }

    @Test
    public void testInvalidVoterReplicaPreVoteRequest() throws Exception {
        int localId = randomReplicaId();
        ReplicaKey otherNodeKey = replicaKey(localId + 1, true);
        Set<Integer> voters = Set.of(localId, otherNodeKey.id());

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withKip853Rpc(true)
            .build();

        context.becomeLeader();
        int epoch = context.currentEpoch();

        // invalid voter id is rejected
        context.deliverRequest(
            context.voteRequest(
                context.clusterId.toString(),
                epoch,
                otherNodeKey,
                ReplicaKey.of(10, Uuid.randomUuid()),
                epoch,
                100,
                true
            )
        );
        context.pollUntilResponse();
        context.assertSentVoteResponse(Errors.INVALID_VOTER_KEY, epoch, OptionalInt.of(localId), false);

        // invalid voter directory id is rejected
        context.deliverRequest(
            context.voteRequest(
                context.clusterId.toString(),
                epoch,
                otherNodeKey,
                ReplicaKey.of(0, Uuid.randomUuid()),
                epoch,
                100,
                true
            )
        );
        context.pollUntilResponse();
        context.assertSentVoteResponse(Errors.INVALID_VOTER_KEY, epoch, OptionalInt.of(localId), false);
    }

    @Test
    public void testLeaderAcceptPreVoteFromObserver() throws Exception {
        int localId = randomReplicaId();
        int otherNodeId = localId + 1;
        Set<Integer> voters = Set.of(localId, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withUnknownLeader(4)
            .withKip853Rpc(true)
            .build();

        context.becomeLeader();
        int epoch = context.currentEpoch();

        ReplicaKey observerKey = replicaKey(localId + 2, true);
        context.deliverRequest(context.preVoteRequest(epoch - 1, observerKey, 0, 0));
        context.client.poll();
        context.assertSentVoteResponse(Errors.FENCED_LEADER_EPOCH, epoch, OptionalInt.of(localId), false);

        context.deliverRequest(context.preVoteRequest(epoch, observerKey, 0, 0));
        context.client.poll();
        context.assertSentVoteResponse(Errors.NONE, epoch, OptionalInt.of(localId), false);
    }

    @Test
    public void testHandlePreVoteRequestAsResigned() throws Exception {
        int localId = randomReplicaId();
        ReplicaKey otherNodeKey = replicaKey(localId + 1, true);
        Set<Integer> voters = Set.of(localId, otherNodeKey.id());

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withKip853Rpc(true)
            .build();
        context.becomeLeader();
        context.client.quorum().transitionToResigned(Collections.emptyList());
        assertTrue(context.client.quorum().isResigned());

        // resigned should grant pre-vote requests with the same epoch if log is up-to-date
        int epoch = context.currentEpoch();
        context.deliverRequest(context.preVoteRequest(epoch, otherNodeKey, epoch, 1));
        context.pollUntilResponse();
        context.assertSentVoteResponse(Errors.NONE, epoch, OptionalInt.of(localId), true);

        // resigned will transition to unattached if pre-vote request has a higher epoch
        context.deliverRequest(context.preVoteRequest(epoch + 1, otherNodeKey, epoch + 1, 1));
        context.pollUntilResponse();
        context.assertSentVoteResponse(Errors.NONE, epoch + 1, OptionalInt.of(-1), true);
        assertTrue(context.client.quorum().isUnattached());
    }

    @Test
    public void testInvalidVoteRequest() throws Exception {
        int localId = randomReplicaId();
        ReplicaKey otherNodeKey = replicaKey(localId + 1, true);
        int epoch = 5;
        Set<Integer> voters = Set.of(localId, otherNodeKey.id());

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withElectedLeader(epoch, otherNodeKey.id())
            .withKip853Rpc(true)
            .build();
        assertEquals(epoch, context.currentEpoch());
        context.assertElectedLeader(epoch, otherNodeKey.id());

        context.deliverRequest(context.preVoteRequest(epoch, otherNodeKey, 0, -5L));
        context.pollUntilResponse();
        context.assertSentVoteResponse(
            Errors.INVALID_REQUEST,
            epoch,
            OptionalInt.of(otherNodeKey.id()),
            false
        );
        assertEquals(epoch, context.currentEpoch());
        context.assertElectedLeader(epoch, otherNodeKey.id());

        context.deliverRequest(context.preVoteRequest(epoch, otherNodeKey, -1, 0L));
        context.pollUntilResponse();
        context.assertSentVoteResponse(
            Errors.INVALID_REQUEST,
            epoch,
            OptionalInt.of(otherNodeKey.id()),
            false
        );
        assertEquals(epoch, context.currentEpoch());
        context.assertElectedLeader(epoch, otherNodeKey.id());

        context.deliverRequest(context.preVoteRequest(epoch, otherNodeKey, epoch + 1, 0L));
        context.pollUntilResponse();
        context.assertSentVoteResponse(
            Errors.INVALID_REQUEST,
            epoch,
            OptionalInt.of(otherNodeKey.id()),
            false
        );
        assertEquals(epoch, context.currentEpoch());
        context.assertElectedLeader(epoch, otherNodeKey.id());
    }

    @Test
    public void testFollowerGrantsPreVoteIfHasNotFetchedYet() throws Exception {
        int localId = randomReplicaId();
        int epoch = 2;
        ReplicaKey replica1 = replicaKey(localId + 1, true);
        ReplicaKey replica2 = replicaKey(localId + 2, true);
        Set<Integer> voters = Set.of(replica1.id(), replica2.id());

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withElectedLeader(epoch, replica1.id())
            .withKip853Rpc(true)
            .build();
        assertTrue(context.client.quorum().isFollower());

        // We will grant PreVotes before fetching successfully from the leader, it will NOT contain the leaderId
        context.deliverRequest(context.preVoteRequest(epoch, replica2, epoch, 1));
        context.pollUntilResponse();

        assertTrue(context.client.quorum().isFollower());
        context.assertSentVoteResponse(Errors.NONE, epoch, OptionalInt.of(replica1.id()), true);

        // After fetching successfully from the leader once, we will no longer grant PreVotes
        context.pollUntilRequest();
        RaftRequest.Outbound fetchRequest = context.assertSentFetchRequest();
        context.assertFetchRequestData(fetchRequest, epoch, 0L, 0);

        context.deliverResponse(
            fetchRequest.correlationId(),
            fetchRequest.destination(),
            context.fetchResponse(epoch, replica1.id(), MemoryRecords.EMPTY, 0L, Errors.NONE)
        );
        assertTrue(context.client.quorum().isFollower());

        context.deliverRequest(context.preVoteRequest(epoch, replica2, epoch, 1));
        context.pollUntilResponse();

        assertTrue(context.client.quorum().isFollower());
    }

    @Test
    public void testRejectPreVoteIfRemoteLogIsNotUpToDate() throws Exception {
        int localId = randomReplicaId();
        int epoch = 2;
        ReplicaKey replica1 = replicaKey(localId + 1, true);
        ReplicaKey replica2 = replicaKey(localId + 2, true);
        Set<Integer> voters = Set.of(localId, replica1.id(), replica2.id());

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withUnknownLeader(epoch)
            .withKip853Rpc(true)
            .appendToLog(epoch, Arrays.asList("a", "b", "c"))
            .build();
        assertTrue(context.client.quorum().isUnattached());
        assertEquals(3, context.log.endOffset().offset());

        // older epoch
        context.deliverRequest(context.preVoteRequest(epoch - 1, replica1, epoch - 1, 0));
        context.pollUntilResponse();

        assertTrue(context.client.quorum().isUnattached());
        context.assertSentVoteResponse(Errors.FENCED_LEADER_EPOCH, epoch, OptionalInt.empty(), false);

        // older offset
        context.deliverRequest(context.preVoteRequest(epoch, replica1, epoch - 1, context.log.endOffset().offset() - 1));
        context.pollUntilResponse();

        assertTrue(context.client.quorum().isUnattached());
        context.assertSentVoteResponse(Errors.NONE, epoch, OptionalInt.empty(), false);
    }
}
