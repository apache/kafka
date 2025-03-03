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
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.RecordBatchTooLargeException;
import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.common.message.BeginQuorumEpochResponseData;
import org.apache.kafka.common.message.DescribeQuorumResponseData;
import org.apache.kafka.common.message.DescribeQuorumResponseData.ReplicaState;
import org.apache.kafka.common.message.EndQuorumEpochResponseData;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.VoteRequestData;
import org.apache.kafka.common.message.VoteResponseData;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.raft.errors.BufferAllocationException;
import org.apache.kafka.raft.errors.NotLeaderException;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.singletonList;
import static org.apache.kafka.raft.RaftClientTestContext.Builder.DEFAULT_ELECTION_TIMEOUT_MS;
import static org.apache.kafka.test.TestUtils.assertFutureThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KafkaRaftClientTest {
    @Test
    public void testNodeDirectoryId() {
        int localId = randomReplicaId();
        assertThrows(
            IllegalArgumentException.class,
            new RaftClientTestContext.Builder(localId, Uuid.ZERO_UUID)::build
        );
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testInitializeSingleMemberQuorum(boolean withKip853Rpc) throws IOException {
        int localId = randomReplicaId();
        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, Collections.singleton(localId))
            .withKip853Rpc(withKip853Rpc)
            .build();
        context.assertElectedLeader(1, localId);
        assertEquals(context.log.endOffset().offset(), context.client.logEndOffset());
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testInitializeAsLeaderFromStateStoreSingleMemberQuorum(boolean withKip853Rpc) throws Exception {
        // Start off as leader. We should still bump the epoch after initialization
        int localId = randomReplicaId();
        int initialEpoch = 2;
        Set<Integer> voters = Collections.singleton(localId);
        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withKip853Rpc(withKip853Rpc)
            .withElectedLeader(initialEpoch, localId)
            .build();

        context.pollUntil(() -> context.log.endOffset().offset() == 1L);
        assertEquals(1L, context.log.endOffset().offset());
        assertEquals(initialEpoch + 1, context.log.lastFetchedEpoch());
        assertEquals(new LeaderAndEpoch(OptionalInt.of(localId), initialEpoch + 1),
            context.currentLeaderAndEpoch());
        context.assertElectedLeader(initialEpoch + 1, localId);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testRejectVotesFromSameEpochAfterResigningLeadership(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        int remoteId = localId + 1;
        ReplicaKey remoteKey = replicaKey(remoteId, withKip853Rpc);
        Set<Integer> voters = Set.of(localId, remoteKey.id());
        int epoch = 2;

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .updateRandom(r -> r.mockNextInt(DEFAULT_ELECTION_TIMEOUT_MS, 0))
            .withElectedLeader(epoch, localId)
            .withKip853Rpc(withKip853Rpc)
            .build();

        assertEquals(0L, context.log.endOffset().offset());
        context.assertElectedLeader(epoch, localId);

        // Since we were the leader in epoch 2, we should ensure that we will not vote for any
        // other voter in the same epoch, even if it has caught up to the same position.
        context.deliverRequest(
            context.voteRequest(
                epoch,
                remoteKey,
                context.log.lastFetchedEpoch(),
                context.log.endOffset().offset()
            )
        );
        context.pollUntilResponse();
        context.assertSentVoteResponse(Errors.NONE, epoch, OptionalInt.of(localId), false);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testRejectVotesFromSameEpochAfterResigningCandidacy(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        int remoteId = localId + 1;
        ReplicaKey remoteKey = replicaKey(remoteId, withKip853Rpc);
        Set<Integer> voters = Set.of(localId, remoteKey.id());
        int epoch = 2;

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .updateRandom(r -> r.mockNextInt(DEFAULT_ELECTION_TIMEOUT_MS, 0))
            .withVotedCandidate(epoch, ReplicaKey.of(localId, ReplicaKey.NO_DIRECTORY_ID))
            .withKip853Rpc(withKip853Rpc)
            .build();

        assertEquals(0L, context.log.endOffset().offset());
        context.assertVotedCandidate(epoch, localId);

        // Since we were the leader in epoch 2, we should ensure that we will not vote for any
        // other voter in the same epoch, even if it has caught up to the same position.
        context.deliverRequest(
            context.voteRequest(
                epoch,
                remoteKey,
                context.log.lastFetchedEpoch(),
                context.log.endOffset().offset()
            )
        );
        context.pollUntilResponse();
        context.assertSentVoteResponse(Errors.NONE, epoch, OptionalInt.empty(), false);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testGrantVotesFromHigherEpochAfterResigningLeadership(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        int remoteId = localId + 1;
        ReplicaKey remoteKey = replicaKey(remoteId, withKip853Rpc);
        Set<Integer> voters = Set.of(localId, remoteKey.id());
        int epoch = 2;

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .updateRandom(r -> r.mockNextInt(DEFAULT_ELECTION_TIMEOUT_MS, 0))
            .withElectedLeader(epoch, localId)
            .withKip853Rpc(withKip853Rpc)
            .build();

        // Resign from leader, will restart in resigned state
        assertTrue(context.client.quorum().isResigned());
        assertEquals(0L, context.log.endOffset().offset());
        context.assertElectedLeader(epoch, localId);

        // Send vote request with higher epoch
        context.deliverRequest(
            context.voteRequest(
                epoch + 1,
                remoteKey,
                context.log.lastFetchedEpoch(),
                context.log.endOffset().offset()
            )
        );
        context.client.poll();

        // We will first transition to unattached and then grant vote and then transition to voted
        assertTrue(context.client.quorum().isUnattachedAndVoted());
        context.assertVotedCandidate(epoch + 1, remoteKey.id());
        context.assertSentVoteResponse(Errors.NONE, epoch + 1, OptionalInt.empty(), true);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testGrantVotesFromHigherEpochAfterResigningCandidacy(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        int remoteId = localId + 1;
        ReplicaKey remoteKey = replicaKey(remoteId, withKip853Rpc);
        Set<Integer> voters = Set.of(localId, remoteKey.id());
        int epoch = 2;

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .updateRandom(r -> r.mockNextInt(DEFAULT_ELECTION_TIMEOUT_MS, 0))
            .withVotedCandidate(epoch, ReplicaKey.of(localId, ReplicaKey.NO_DIRECTORY_ID))
            .withKip853Rpc(withKip853Rpc)
            .build();

        // Resign from candidate, will restart in candidate state
        assertTrue(context.client.quorum().isCandidate());
        assertEquals(0L, context.log.endOffset().offset());
        context.assertVotedCandidate(epoch, localId);

        // Send vote request with higher epoch
        context.deliverRequest(
            context.voteRequest(
                epoch + 1,
                remoteKey,
                context.log.lastFetchedEpoch(),
                context.log.endOffset().offset()
            )
        );
        context.client.poll();

        // We will first transition to unattached and then grant vote and then transition to voted
        assertTrue(context.client.quorum().isUnattachedAndVoted());
        context.assertVotedCandidate(epoch + 1, remoteKey.id());
        context.assertSentVoteResponse(Errors.NONE, epoch + 1, OptionalInt.empty(), true);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testGrantVotesWhenShuttingDown(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        int remoteId = localId + 1;
        ReplicaKey remoteKey = replicaKey(remoteId, withKip853Rpc);
        Set<Integer> voters = Set.of(localId, remoteKey.id());

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.becomeLeader();
        int epoch = context.currentEpoch();

        // Beginning shutdown
        context.client.shutdown(1000);
        assertTrue(context.client.isShuttingDown());

        // Send vote request with higher epoch
        context.deliverRequest(
            context.voteRequest(
                epoch + 1,
                remoteKey,
                context.log.lastFetchedEpoch(),
                context.log.endOffset().offset()
            )
        );
        context.client.poll();

        // We will first transition to unattached and then grant vote and then transition to voted
        assertTrue(
            context.client.quorum().isUnattachedAndVoted(),
            "Local Id: " + localId +
            " Remote Id: " + remoteId +
            " Quorum local Id: " + context.client.quorum().localIdOrSentinel() +
            " Quorum leader Id: " + context.client.quorum().leaderIdOrSentinel()
        );
        context.assertVotedCandidate(epoch + 1, remoteKey.id());
        context.assertSentVoteResponse(Errors.NONE, epoch + 1, OptionalInt.empty(), true);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testInitializeAsResignedAndBecomeCandidate(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        int remoteId = localId + 1;
        Set<Integer> voters = Set.of(localId, remoteId);
        int epoch = 2;

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .updateRandom(r -> r.mockNextInt(DEFAULT_ELECTION_TIMEOUT_MS, 0))
            .withElectedLeader(epoch, localId)
            .withKip853Rpc(withKip853Rpc)
            .build();

        // Resign from leader, will restart in resigned state
        assertTrue(context.client.quorum().isResigned());
        assertEquals(0L, context.log.endOffset().offset());
        context.assertElectedLeader(epoch, localId);

        // Election timeout
        context.time.sleep(context.electionTimeoutMs());
        context.client.poll();

        // Become candidate in a new epoch
        assertTrue(context.client.quorum().isCandidate());
        context.assertVotedCandidate(epoch + 1, localId);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testInitializeAsResignedLeaderFromStateStore(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        int remoteId = localId + 1;
        Set<Integer> voters = Set.of(localId, remoteId);
        int epoch = 2;

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .updateRandom(r -> r.mockNextInt(DEFAULT_ELECTION_TIMEOUT_MS, 0))
            .withKip853Rpc(withKip853Rpc)
            .withElectedLeader(epoch, localId)
            .build();

        // The node will remain elected, but start up in a resigned state
        // in which no additional writes are accepted.
        assertEquals(0L, context.log.endOffset().offset());
        context.assertElectedLeader(epoch, localId);
        context.client.poll();
        assertThrows(NotLeaderException.class, () -> context.client.prepareAppend(epoch, Arrays.asList("a", "b")));

        context.pollUntilRequest();
        RaftRequest.Outbound request = context.assertSentEndQuorumEpochRequest(epoch, remoteId);
        context.deliverResponse(
            request.correlationId(),
            request.destination(),
            context.endEpochResponse(epoch, OptionalInt.of(localId))
        );
        context.client.poll();

        context.time.sleep(context.electionTimeoutMs());
        context.pollUntilRequest();
        context.assertVotedCandidate(epoch + 1, localId);
        context.assertSentVoteRequest(epoch + 1, 0, 0L, 1);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testAppendFailedWithNotLeaderException(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        int remoteId = localId + 1;
        Set<Integer> voters = Set.of(localId, remoteId);
        int epoch = 2;

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withUnknownLeader(epoch)
            .withKip853Rpc(withKip853Rpc)
            .build();

        assertThrows(NotLeaderException.class, () -> context.client.prepareAppend(epoch, Arrays.asList("a", "b")));
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testAppendFailedWithBufferAllocationException(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        int otherNodeId = localId + 1;
        Set<Integer> voters = Set.of(localId, otherNodeId);

        MemoryPool memoryPool = Mockito.mock(MemoryPool.class);
        ByteBuffer buffer = ByteBuffer.allocate(KafkaRaftClient.MAX_BATCH_SIZE_BYTES);
        // Return null when allocation error
        Mockito.when(memoryPool.tryAllocate(KafkaRaftClient.MAX_BATCH_SIZE_BYTES))
            .thenReturn(buffer) // Buffer for the leader message control record
            .thenReturn(null); // Buffer for the prepareAppend call

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withMemoryPool(memoryPool)
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.becomeLeader();
        assertEquals(OptionalInt.of(localId), context.currentLeader());
        int epoch = context.currentEpoch();

        assertThrows(BufferAllocationException.class, () -> context.client.prepareAppend(epoch, singletonList("a")));
        Mockito.verify(memoryPool).release(buffer);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testAppendFailedWithFencedEpoch(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        int otherNodeId = localId + 1;
        Set<Integer> voters = Set.of(localId, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.becomeLeader();
        assertEquals(OptionalInt.of(localId), context.currentLeader());
        int epoch = context.currentEpoch();

        // Throws IllegalArgumentException on higher epoch
        assertThrows(IllegalArgumentException.class, () -> context.client.prepareAppend(epoch + 1, singletonList("a")));
        // Throws NotLeaderException on smaller epoch
        assertThrows(NotLeaderException.class, () -> context.client.prepareAppend(epoch - 1, singletonList("a")));
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testAppendFailedWithRecordBatchTooLargeException(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        int otherNodeId = localId + 1;
        Set<Integer> voters = Set.of(localId, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.becomeLeader();
        assertEquals(OptionalInt.of(localId), context.currentLeader());
        int epoch = context.currentEpoch();

        int size = KafkaRaftClient.MAX_BATCH_SIZE_BYTES / 8 + 1; // 8 is the estimate min size of each record
        List<String> batchToLarge = new ArrayList<>(size + 1);
        for (int i = 0; i < size; i++)
            batchToLarge.add("a");

        assertThrows(
            RecordBatchTooLargeException.class,
            () -> context.client.prepareAppend(epoch, batchToLarge)
        );
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testEndQuorumEpochRetriesWhileResigned(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        int voter1 = localId + 1;
        int voter2 = localId + 2;
        Set<Integer> voters = Set.of(localId, voter1, voter2);
        int epoch = 19;

        // Start off as leader so that we will initialize in the Resigned state.
        // Note that we intentionally set a request timeout which is smaller than
        // the election timeout so that we can still in the Resigned state and
        // verify retry behavior.
        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withElectionTimeoutMs(10000)
            .withRequestTimeoutMs(5000)
            .withElectedLeader(epoch, localId)
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.pollUntilRequest();
        List<RaftRequest.Outbound> requests = context.collectEndQuorumRequests(
            epoch, Set.of(voter1, voter2), Optional.empty());
        assertEquals(2, requests.size());

        // Respond to one of the requests so that we can verify that no additional
        // request to this node is sent.
        RaftRequest.Outbound endEpochOutbound = requests.get(0);
        context.deliverResponse(
            endEpochOutbound.correlationId(),
            endEpochOutbound.destination(),
            context.endEpochResponse(epoch, OptionalInt.of(localId))
        );
        context.client.poll();
        assertEquals(Collections.emptyList(), context.channel.drainSendQueue());

        // Now sleep for the request timeout and verify that we get only one
        // retried request from the voter that hasn't responded yet.
        int nonRespondedId = requests.get(1).destination().id();
        context.time.sleep(6000);
        context.pollUntilRequest();
        List<RaftRequest.Outbound> retries = context.collectEndQuorumRequests(
            epoch, Set.of(nonRespondedId), Optional.empty());
        assertEquals(1, retries.size());
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testResignWillCompleteFetchPurgatory(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        int remoteId = localId + 1;
        ReplicaKey otherNodeKey = replicaKey(remoteId, withKip853Rpc);
        Set<Integer> voters = Set.of(localId, otherNodeKey.id());

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.becomeLeader();
        assertEquals(OptionalInt.of(localId), context.currentLeader());

        // send fetch request when become leader
        int epoch = context.currentEpoch();
        context.deliverRequest(context.fetchRequest(epoch, otherNodeKey, context.log.endOffset().offset(), epoch, 1000));
        context.client.poll();

        // append some record, but the fetch in purgatory will still fail
        context.log.appendAsLeader(
            context.buildBatch(context.log.endOffset().offset(), epoch, singletonList("raft")),
            epoch
        );

        // when transition to resign, all request in fetchPurgatory will fail
        context.client.shutdown(1000);
        context.client.poll();
        context.assertSentFetchPartitionResponse(Errors.NOT_LEADER_OR_FOLLOWER, epoch, OptionalInt.of(localId));
        context.assertResignedLeader(epoch, localId);

        // shutting down finished
        context.time.sleep(1000);
        context.client.poll();
        assertFalse(context.client.isRunning());
        assertFalse(context.client.isShuttingDown());
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testResignInOlderEpochIgnored(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        int otherNodeId = localId + 1;
        Set<Integer> voters = Set.of(localId, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.becomeLeader();
        assertEquals(OptionalInt.of(localId), context.currentLeader());

        int currentEpoch = context.currentEpoch();
        context.client.resign(currentEpoch - 1);
        context.client.poll();

        // Ensure we are still leader even after expiration of the election timeout.
        context.time.sleep(context.electionTimeoutMs() * 2L);
        context.client.poll();
        context.assertElectedLeader(currentEpoch, localId);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testHandleBeginQuorumEpochAfterUserInitiatedResign(
        boolean withKip853Rpc
    ) throws Exception {
        int localId = randomReplicaId();
        int remoteId1 = localId + 1;
        int remoteId2 = localId + 2;
        Set<Integer> voters = Set.of(localId, remoteId1, remoteId2);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.becomeLeader();
        assertEquals(OptionalInt.of(localId), context.currentLeader());

        int resignedEpoch = context.currentEpoch();

        context.client.resign(resignedEpoch);
        context.pollUntil(context.client.quorum()::isResigned);

        context.deliverRequest(context.beginEpochRequest(resignedEpoch + 1, remoteId1));
        context.pollUntilResponse();
        context.assertSentBeginQuorumEpochResponse(Errors.NONE);
        context.assertElectedLeader(resignedEpoch + 1, remoteId1);
        assertEquals(new LeaderAndEpoch(OptionalInt.of(remoteId1), resignedEpoch + 1),
            context.listener.currentLeaderAndEpoch());
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testBeginQuorumEpochHeartbeat(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        int remoteId1 = localId + 1;
        int remoteId2 = localId + 2;
        Set<Integer> voters = Set.of(localId, remoteId1, remoteId2);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.becomeLeader();
        int epoch = context.currentEpoch();
        assertEquals(OptionalInt.of(localId), context.currentLeader());

        // begin epoch requests should be sent out every beginQuorumEpochTimeoutMs
        context.time.sleep(context.beginQuorumEpochTimeoutMs);
        context.client.poll();
        context.assertSentBeginQuorumEpochRequest(epoch, Set.of(remoteId1, remoteId2));

        int partialDelay = context.beginQuorumEpochTimeoutMs / 2;
        context.time.sleep(partialDelay);
        context.client.poll();
        context.assertSentBeginQuorumEpochRequest(epoch, Set.of());

        context.time.sleep(context.beginQuorumEpochTimeoutMs - partialDelay);
        context.client.poll();
        context.assertSentBeginQuorumEpochRequest(epoch, Set.of(remoteId1, remoteId2));
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testLeaderShouldResignLeadershipIfNotGetFetchRequestFromMajorityVoters(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        int remoteId1 = localId + 1;
        int remoteId2 = localId + 2;
        int observerId = localId + 3;
        ReplicaKey remoteKey1 = replicaKey(remoteId1, withKip853Rpc);
        ReplicaKey remoteKey2 = replicaKey(remoteId2, withKip853Rpc);
        ReplicaKey observerKey3 = replicaKey(observerId, withKip853Rpc);
        Set<Integer> voters = Set.of(localId, remoteKey1.id(), remoteKey2.id());

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withKip853Rpc(withKip853Rpc)
            .build();
        int resignLeadershipTimeout = context.checkQuorumTimeoutMs;

        context.becomeLeader();
        int epoch = context.currentEpoch();
        assertEquals(OptionalInt.of(localId), context.currentLeader());

        // fetch timeout is not expired, the leader should not get resigned
        context.time.sleep(resignLeadershipTimeout / 2);
        context.client.poll();

        assertFalse(context.client.quorum().isResigned());

        // Received fetch request from a voter, the fetch timer should be reset.
        context.deliverRequest(context.fetchRequest(epoch, remoteKey1, 0, 0, 0));
        context.pollUntilRequest();

        // Since the fetch timer is reset, the leader should not get resigned
        context.time.sleep(resignLeadershipTimeout / 2);
        context.client.poll();

        assertFalse(context.client.quorum().isResigned());

        // Received fetch request from another voter, the fetch timer should be reset.
        context.deliverRequest(context.fetchRequest(epoch, remoteKey2, 0, 0, 0));
        context.pollUntilRequest();

        // Since the fetch timer is reset, the leader should not get resigned
        context.time.sleep(resignLeadershipTimeout / 2);
        context.client.poll();

        assertFalse(context.client.quorum().isResigned());

        // Received fetch request from an observer, but the fetch timer should not be reset.
        context.deliverRequest(context.fetchRequest(epoch, observerKey3, 0, 0, 0));
        context.pollUntilRequest();

        // After this sleep, the fetch timeout should expire since we don't receive fetch request from the majority voters within fetchTimeoutMs
        context.time.sleep(resignLeadershipTimeout / 2);
        context.client.poll();

        // The leadership should get resigned now
        assertTrue(context.client.quorum().isResigned());
        context.assertResignedLeader(epoch, localId);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testLeaderShouldNotResignLeadershipIfOnlyOneVoters(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        Set<Integer> voters = Set.of(localId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withKip853Rpc(withKip853Rpc)
            .build();
        assertEquals(OptionalInt.of(localId), context.currentLeader());

        // checkQuorum timeout is expired without receiving fetch request from other voters, but since there is only 1 voter,
        // the leader should not get resigned
        context.time.sleep(context.checkQuorumTimeoutMs);
        context.client.poll();

        assertFalse(context.client.quorum().isResigned());
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testElectionTimeoutAfterUserInitiatedResign(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        int otherNodeId = localId + 1;
        Set<Integer> voters = Set.of(localId, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.becomeLeader();
        assertEquals(OptionalInt.of(localId), context.currentLeader());

        int resignedEpoch = context.currentEpoch();

        context.client.resign(resignedEpoch);
        context.pollUntil(context.client.quorum()::isResigned);

        context.pollUntilRequest();
        RaftRequest.Outbound request = context.assertSentEndQuorumEpochRequest(resignedEpoch, otherNodeId);

        EndQuorumEpochResponseData response = context.endEpochResponse(
            resignedEpoch,
            OptionalInt.of(localId)
        );

        context.deliverResponse(request.correlationId(), request.destination(), response);
        context.client.poll();

        // We do not resend `EndQuorumRequest` once the other voter has acknowledged it.
        context.time.sleep(context.retryBackoffMs);
        context.client.poll();
        assertFalse(context.channel.hasSentRequests());

        // Any `Fetch` received in the resigned state should result in a NOT_LEADER error.
        ReplicaKey observer = replicaKey(-1, withKip853Rpc);
        context.deliverRequest(context.fetchRequest(1, observer, 0, 0, 0));
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(
            Errors.NOT_LEADER_OR_FOLLOWER,
            resignedEpoch,
            OptionalInt.of(localId)
        );

        // After the election timer, we should become a candidate.
        context.time.sleep(2L * context.electionTimeoutMs());
        context.pollUntil(context.client.quorum()::isCandidate);
        assertEquals(resignedEpoch + 1, context.currentEpoch());
        assertEquals(new LeaderAndEpoch(OptionalInt.empty(), resignedEpoch + 1),
            context.listener.currentLeaderAndEpoch());
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testCannotResignWithLargerEpochThanCurrentEpoch(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        int otherNodeId = localId + 1;
        Set<Integer> voters = Set.of(localId, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withKip853Rpc(withKip853Rpc)
            .build();
        context.becomeLeader();

        assertThrows(IllegalArgumentException.class,
            () -> context.client.resign(context.currentEpoch() + 1));
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testCannotResignIfNotLeader(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        int otherNodeId = localId + 1;
        int leaderEpoch = 2;
        Set<Integer> voters = Set.of(localId, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withElectedLeader(leaderEpoch, otherNodeId)
            .withKip853Rpc(withKip853Rpc)
            .build();

        assertEquals(OptionalInt.of(otherNodeId), context.currentLeader());
        assertThrows(IllegalArgumentException.class, () -> context.client.resign(leaderEpoch));
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testCannotResignIfObserver(boolean withKip853Rpc) throws Exception {
        int leaderId = randomReplicaId();
        int otherNodeId = randomReplicaId() + 1;
        int epoch = 5;
        Set<Integer> voters = Set.of(leaderId, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(OptionalInt.empty(), voters)
            .withKip853Rpc(withKip853Rpc)
            .build();
        context.pollUntilRequest();

        RaftRequest.Outbound fetchRequest = context.assertSentFetchRequest();
        assertTrue(voters.contains(fetchRequest.destination().id()));
        context.assertFetchRequestData(fetchRequest, 0, 0L, 0);

        context.deliverResponse(
            fetchRequest.correlationId(),
            fetchRequest.destination(),
            context.fetchResponse(epoch, leaderId, MemoryRecords.EMPTY, 0L, Errors.FENCED_LEADER_EPOCH)
        );

        context.client.poll();
        context.assertElectedLeader(epoch, leaderId);
        assertThrows(IllegalStateException.class, () -> context.client.resign(epoch));
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testInitializeAsCandidateFromStateStore(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        // Need 3 node to require a 2-node majority
        Set<Integer> voters = Set.of(localId, localId + 1, localId + 2);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withVotedCandidate(2, ReplicaKey.of(localId, ReplicaKey.NO_DIRECTORY_ID))
            .withKip853Rpc(withKip853Rpc)
            .build();
        context.assertVotedCandidate(2, localId);
        assertEquals(0L, context.log.endOffset().offset());

        // The candidate will resume the election after reinitialization
        context.pollUntilRequest();
        List<RaftRequest.Outbound> voteRequests = context.collectVoteRequests(2, 0, 0);
        assertEquals(2, voteRequests.size());
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testInitializeAsCandidateAndBecomeLeader(boolean withKip853Rpc) throws Exception {
        final int localId = randomReplicaId();
        final int otherNodeId = localId + 1;
        Set<Integer> voters = Set.of(localId, otherNodeId);
        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.assertUnknownLeader(0);
        context.pollUntilRequest();
        RaftRequest.Outbound request = context.assertSentFetchRequest(0, 0L, 0);
        assertTrue(context.client.quorum().isUnattached());
        assertTrue(context.client.quorum().isVoter());

        // receives a fetch response which does not specify who the leader is
        context.time.sleep(context.electionTimeoutMs() / 2);
        context.deliverResponse(
            request.correlationId(),
            request.destination(),
            context.fetchResponse(0, -1, MemoryRecords.EMPTY, -1, Errors.NOT_LEADER_OR_FOLLOWER)
        );

        // should remain unattached voter
        context.client.poll();
        assertTrue(context.client.quorum().isUnattached());
        assertTrue(context.client.quorum().isVoter());

        // after election timeout should become candidate
        context.time.sleep(context.electionTimeoutMs() * 2L);
        context.pollUntilRequest();
        assertTrue(context.client.quorum().isCandidate());

        context.pollUntilRequest();
        context.assertVotedCandidate(1, localId);

        request = context.assertSentVoteRequest(1, 0, 0L, 1);
        context.deliverResponse(
            request.correlationId(),
            request.destination(),
            context.voteResponse(true, OptionalInt.empty(), 1)
        );

        // Become leader after receiving the vote
        context.pollUntil(() -> context.log.endOffset().offset() == 1L);
        context.assertElectedLeader(1, localId);
        long electionTimestamp = context.time.milliseconds();

        // Leader change record appended
        assertEquals(1L, context.log.endOffset().offset());
        assertEquals(1L, context.log.firstUnflushedOffset());

        // Send BeginQuorumEpoch to voters
        context.client.poll();
        context.assertSentBeginQuorumEpochRequest(1, Set.of(otherNodeId));

        Records records = context.log.read(0, Isolation.UNCOMMITTED).records;
        RecordBatch batch = records.batches().iterator().next();
        assertTrue(batch.isControlBatch());

        Record record = batch.iterator().next();
        assertEquals(electionTimestamp, record.timestamp());
        RaftClientTestContext.verifyLeaderChangeMessage(localId, Arrays.asList(localId, otherNodeId),
            Arrays.asList(otherNodeId, localId), record.key(), record.value());
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testInitializeAsCandidateAndBecomeLeaderQuorumOfThree(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        final int firstNodeId = localId + 1;
        final int secondNodeId = localId + 2;
        Set<Integer> voters = Set.of(localId, firstNodeId, secondNodeId);
        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.assertUnknownLeader(0);
        context.time.sleep(2L * context.electionTimeoutMs());

        context.pollUntilRequest();
        context.assertVotedCandidate(1, localId);

        RaftRequest.Outbound request = context.assertSentVoteRequest(1, 0, 0L, 2);
        context.deliverResponse(
            request.correlationId(),
            request.destination(),
            context.voteResponse(true, OptionalInt.empty(), 1)
        );

        VoteRequestData voteRequest = (VoteRequestData) request.data();
        int voterId = voteRequest.voterId();
        assertNotEquals(localId, voterId);

        // Become leader after receiving the vote
        context.pollUntil(() -> context.log.endOffset().offset() == 1L);
        context.assertElectedLeader(1, localId);
        long electionTimestamp = context.time.milliseconds();

        // Leader change record appended
        assertEquals(1L, context.log.endOffset().offset());
        assertEquals(1L, context.log.firstUnflushedOffset());

        // Send BeginQuorumEpoch to voters
        context.client.poll();
        context.assertSentBeginQuorumEpochRequest(1, Set.of(firstNodeId, secondNodeId));

        Records records = context.log.read(0, Isolation.UNCOMMITTED).records;
        RecordBatch batch = records.batches().iterator().next();
        assertTrue(batch.isControlBatch());

        Record record = batch.iterator().next();
        assertEquals(electionTimestamp, record.timestamp());
        RaftClientTestContext.verifyLeaderChangeMessage(localId, Arrays.asList(localId, firstNodeId, secondNodeId),
            Arrays.asList(voterId, localId), record.key(), record.value());
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testHandleBeginQuorumRequest(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        ReplicaKey otherNodeKey = replicaKey(localId + 1, withKip853Rpc);
        int votedCandidateEpoch = 2;
        Set<Integer> voters = Set.of(localId, otherNodeKey.id());

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withVotedCandidate(votedCandidateEpoch, otherNodeKey)
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.deliverRequest(context.beginEpochRequest(votedCandidateEpoch, otherNodeKey.id()));
        context.pollUntilResponse();

        context.assertElectedLeader(votedCandidateEpoch, otherNodeKey.id());

        context.assertSentBeginQuorumEpochResponse(
            Errors.NONE,
            votedCandidateEpoch,
            OptionalInt.of(otherNodeKey.id())
        );
    }

    @Test
    public void testHandleBeginQuorumRequestMoreEndpoints() throws Exception {
        ReplicaKey local = replicaKey(randomReplicaId(), true);
        ReplicaKey leader = replicaKey(local.id() + 1, true);
        int leaderEpoch = 3;

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, leader));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withStaticVoters(voters)
            .withElectedLeader(leaderEpoch, leader.id())
            .withKip853Rpc(true)
            .build();

        context.client.poll();

        HashMap<ListenerName, InetSocketAddress> leaderListenersMap = new HashMap<>(2);
        leaderListenersMap.put(
            VoterSetTest.DEFAULT_LISTENER_NAME,
            InetSocketAddress.createUnresolved("localhost", 9990 + leader.id())
        );
        leaderListenersMap.put(
            ListenerName.normalised("ANOTHER_LISTENER"),
            InetSocketAddress.createUnresolved("localhost", 8990 + leader.id())
        );
        Endpoints leaderEndpoints = Endpoints.fromInetSocketAddresses(leaderListenersMap);

        context.deliverRequest(context.beginEpochRequest(leaderEpoch, leader.id(), leaderEndpoints));
        context.pollUntilResponse();

        context.assertElectedLeader(leaderEpoch, leader.id());

        context.assertSentBeginQuorumEpochResponse(
            Errors.NONE,
            leaderEpoch,
            OptionalInt.of(leader.id())
        );
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testHandleBeginQuorumResponse(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        int otherNodeId = localId + 1;
        int leaderEpoch = 2;
        Set<Integer> voters = Set.of(localId, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withElectedLeader(leaderEpoch, localId)
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.deliverRequest(context.beginEpochRequest(leaderEpoch + 1, otherNodeId));
        context.pollUntilResponse();

        context.assertElectedLeader(leaderEpoch + 1, otherNodeId);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testEndQuorumIgnoredAsCandidateIfOlderEpoch(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        int otherNodeId = localId + 1;
        int epoch = 5;
        int jitterMs = 85;
        Set<Integer> voters = Set.of(localId, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .updateRandom(r -> r.mockNextInt(jitterMs))
            .withUnknownLeader(epoch - 1)
            .withKip853Rpc(withKip853Rpc)
            .build();

        // Sleep a little to ensure that we become a candidate
        context.time.sleep(context.electionTimeoutMs() + jitterMs);
        context.client.poll();
        context.assertVotedCandidate(epoch, localId);

        context.deliverRequest(
            context.endEpochRequest(
                epoch - 2,
                otherNodeId,
                Collections.singletonList(context.localReplicaKey())
            )
        );

        context.client.poll();
        context.assertSentEndQuorumEpochResponse(Errors.FENCED_LEADER_EPOCH, epoch, OptionalInt.empty());

        // We should still be candidate until expiration of election timeout
        context.time.sleep(context.electionTimeoutMs() + jitterMs - 1);
        context.client.poll();
        context.assertVotedCandidate(epoch, localId);

        // Enter the backoff period
        context.time.sleep(1);
        context.client.poll();
        context.assertVotedCandidate(epoch, localId);

        // After backoff, we will become a candidate again
        context.time.sleep(context.electionBackoffMaxMs);
        context.client.poll();
        context.assertVotedCandidate(epoch + 1, localId);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testEndQuorumIgnoredAsLeaderIfOlderEpoch(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        int voter2 = localId + 1;
        ReplicaKey voter3 = replicaKey(localId + 2, withKip853Rpc);
        Set<Integer> voters = Set.of(localId, voter2, voter3.id());

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withUnknownLeader(6)
            .build();

        context.becomeLeader();
        int epoch = context.currentEpoch();

        // One of the voters may have sent EndQuorumEpoch from an earlier epoch
        context.deliverRequest(
            context.endEpochRequest(epoch - 2, voter2, Arrays.asList(context.localReplicaKey(), voter3))
        );

        context.pollUntilResponse();
        context.assertSentEndQuorumEpochResponse(Errors.FENCED_LEADER_EPOCH, epoch, OptionalInt.of(localId));

        // We should still be leader as long as fetch timeout has not expired
        context.time.sleep(context.fetchTimeoutMs - 1);
        context.client.poll();
        context.assertElectedLeader(epoch, localId);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testEndQuorumStartsNewElectionImmediatelyIfFollowerUnattached(
        boolean withKip853Rpc
    ) throws Exception {
        int localId = randomReplicaId();
        int voter2 = localId + 1;
        ReplicaKey voter3 = replicaKey(localId + 2, withKip853Rpc);
        int epoch = 2;
        Set<Integer> voters = Set.of(localId, voter2, voter3.id());

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withUnknownLeader(epoch)
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.deliverRequest(
            context.endEpochRequest(
                epoch,
                voter2,
                Arrays.asList(context.localReplicaKey(), voter3)
            )
        );

        context.pollUntilResponse();
        context.assertSentEndQuorumEpochResponse(Errors.NONE, epoch, OptionalInt.of(voter2));

        // Should become a candidate immediately
        context.client.poll();
        context.assertVotedCandidate(epoch + 1, localId);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testAccumulatorClearedAfterBecomingFollower(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        int otherNodeId = localId + 1;
        int lingerMs = 50;
        Set<Integer> voters = Set.of(localId, otherNodeId);

        MemoryPool memoryPool = Mockito.mock(MemoryPool.class);
        ByteBuffer buffer = ByteBuffer.allocate(KafkaRaftClient.MAX_BATCH_SIZE_BYTES);
        Mockito.when(memoryPool.tryAllocate(KafkaRaftClient.MAX_BATCH_SIZE_BYTES))
            .thenReturn(buffer);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withAppendLingerMs(lingerMs)
            .withMemoryPool(memoryPool)
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.becomeLeader();
        assertEquals(OptionalInt.of(localId), context.currentLeader());
        int epoch = context.currentEpoch();

        assertEquals(1L, context.client.prepareAppend(epoch, singletonList("a")));
        context.client.schedulePreparedAppend();
        context.deliverRequest(context.beginEpochRequest(epoch + 1, otherNodeId));
        context.pollUntilResponse();

        context.assertElectedLeader(epoch + 1, otherNodeId);
        // Expect two calls one for the leader change control batch and one for the data batch
        Mockito.verify(memoryPool, Mockito.times(2)).release(buffer);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testAccumulatorClearedAfterBecomingVoted(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        ReplicaKey otherNodeKey = replicaKey(localId + 1, withKip853Rpc);
        int lingerMs = 50;
        Set<Integer> voters = Set.of(localId, otherNodeKey.id());

        MemoryPool memoryPool = Mockito.mock(MemoryPool.class);
        ByteBuffer buffer = ByteBuffer.allocate(KafkaRaftClient.MAX_BATCH_SIZE_BYTES);
        Mockito.when(memoryPool.tryAllocate(KafkaRaftClient.MAX_BATCH_SIZE_BYTES))
            .thenReturn(buffer);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withAppendLingerMs(lingerMs)
            .withMemoryPool(memoryPool)
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.becomeLeader();
        assertEquals(OptionalInt.of(localId), context.currentLeader());
        int epoch = context.currentEpoch();

        assertEquals(1L, context.client.prepareAppend(epoch, singletonList("a")));
        context.client.schedulePreparedAppend();
        context.deliverRequest(
            context.voteRequest(epoch + 1, otherNodeKey, epoch, context.log.endOffset().offset())
        );
        context.pollUntilResponse();

        context.assertVotedCandidate(epoch + 1, otherNodeKey.id());
        Mockito.verify(memoryPool, Mockito.times(2)).release(buffer);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testAccumulatorClearedAfterBecomingUnattached(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        ReplicaKey otherNodeKey = replicaKey(localId + 1, withKip853Rpc);
        int lingerMs = 50;
        Set<Integer> voters = Set.of(localId, otherNodeKey.id());

        MemoryPool memoryPool = Mockito.mock(MemoryPool.class);
        ByteBuffer buffer = ByteBuffer.allocate(KafkaRaftClient.MAX_BATCH_SIZE_BYTES);
        Mockito.when(memoryPool.tryAllocate(KafkaRaftClient.MAX_BATCH_SIZE_BYTES))
            .thenReturn(buffer);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withAppendLingerMs(lingerMs)
            .withMemoryPool(memoryPool)
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.becomeLeader();
        assertEquals(OptionalInt.of(localId), context.currentLeader());
        int epoch = context.currentEpoch();

        assertEquals(1L, context.client.prepareAppend(epoch, singletonList("a")));
        context.client.schedulePreparedAppend();
        context.deliverRequest(context.voteRequest(epoch + 1, otherNodeKey, epoch, 0L));
        context.pollUntilResponse();

        context.assertUnknownLeader(epoch + 1);
        // Expect two calls one for the leader change control batch and one for the data batch
        Mockito.verify(memoryPool, Mockito.times(2)).release(buffer);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testChannelWokenUpIfLingerTimeoutReachedWithoutAppend(boolean withKip853Rpc) throws Exception {
        // This test verifies that the client will set its poll timeout accounting
        // for the lingerMs of a pending append
        int localId = randomReplicaId();
        int otherNodeId = localId + 1;
        int lingerMs = 50;
        Set<Integer> voters = Set.of(localId, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withAppendLingerMs(lingerMs)
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.becomeLeader();
        assertEquals(OptionalInt.of(localId), context.currentLeader());
        assertEquals(1L, context.log.endOffset().offset());

        int epoch = context.currentEpoch();
        assertEquals(1L, context.client.prepareAppend(epoch, singletonList("a")));
        context.client.schedulePreparedAppend();
        assertTrue(context.messageQueue.wakeupRequested());

        context.client.poll();
        assertEquals(OptionalLong.of(lingerMs), context.messageQueue.lastPollTimeoutMs());

        context.time.sleep(20);
        context.client.poll();
        assertEquals(OptionalLong.of(30), context.messageQueue.lastPollTimeoutMs());

        context.time.sleep(30);
        context.client.poll();
        assertEquals(2L, context.log.endOffset().offset());
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testChannelWokenUpIfLingerTimeoutReachedDuringAppend(boolean withKip853Rpc) throws Exception {
        // This test verifies that the client will get woken up immediately
        // if the linger timeout has expired during an append
        int localId = randomReplicaId();
        int otherNodeId = localId + 1;
        int lingerMs = 50;
        Set<Integer> voters = Set.of(localId, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withAppendLingerMs(lingerMs)
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.becomeLeader();
        assertEquals(OptionalInt.of(localId), context.currentLeader());
        assertEquals(1L, context.log.endOffset().offset());

        int epoch = context.currentEpoch();
        assertEquals(1L, context.client.prepareAppend(epoch, singletonList("a")));
        context.client.schedulePreparedAppend();
        assertTrue(context.messageQueue.wakeupRequested());

        context.client.poll();
        assertFalse(context.messageQueue.wakeupRequested());
        assertEquals(OptionalLong.of(lingerMs), context.messageQueue.lastPollTimeoutMs());

        context.time.sleep(lingerMs);
        assertEquals(2L, context.client.prepareAppend(epoch, singletonList("b")));
        context.client.schedulePreparedAppend();
        assertTrue(context.messageQueue.wakeupRequested());

        context.client.poll();
        assertEquals(3L, context.log.endOffset().offset());
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testHandleEndQuorumRequest(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        int oldLeaderId = localId + 1;
        int leaderEpoch = 2;
        Set<Integer> voters = Set.of(localId, oldLeaderId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withElectedLeader(leaderEpoch, oldLeaderId)
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.deliverRequest(
            context.endEpochRequest(
                leaderEpoch,
                oldLeaderId,
                Collections.singletonList(context.localReplicaKey())
            )
        );

        context.pollUntilResponse();
        context.assertSentEndQuorumEpochResponse(Errors.NONE, leaderEpoch, OptionalInt.of(oldLeaderId));

        context.client.poll();
        context.assertVotedCandidate(leaderEpoch + 1, localId);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testHandleEndQuorumRequestWithLowerPriorityToBecomeLeader(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        ReplicaKey oldLeaderKey = replicaKey(localId + 1, withKip853Rpc);
        int leaderEpoch = 2;
        ReplicaKey preferredNextLeader = replicaKey(localId + 2, withKip853Rpc);
        Set<Integer> voters = Set.of(localId, oldLeaderKey.id(), preferredNextLeader.id());

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withElectedLeader(leaderEpoch, oldLeaderKey.id())
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.deliverRequest(
            context.endEpochRequest(
                leaderEpoch,
                oldLeaderKey.id(),
                Arrays.asList(preferredNextLeader, context.localReplicaKey())
            )
        );

        context.pollUntilResponse();
        context.assertSentEndQuorumEpochResponse(Errors.NONE, leaderEpoch, OptionalInt.of(oldLeaderKey.id()));

        // The election won't trigger by one round retry backoff
        context.time.sleep(1);

        context.pollUntilRequest();

        context.assertSentFetchRequest(leaderEpoch, 0, 0);

        context.time.sleep(context.retryBackoffMs);

        context.pollUntilRequest();

        List<RaftRequest.Outbound> voteRequests = context.collectVoteRequests(leaderEpoch + 1, 0, 0);
        assertEquals(2, voteRequests.size());

        // Should have already done self-voting
        context.assertVotedCandidate(leaderEpoch + 1, localId);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testVoteRequestTimeout(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        int otherNodeId = localId + 1;
        int epoch = 1;
        Set<Integer> voters = Set.of(localId, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withKip853Rpc(withKip853Rpc)
            .build();
        context.assertUnknownLeader(0);

        context.time.sleep(2L * context.electionTimeoutMs());
        context.pollUntilRequest();
        context.assertVotedCandidate(epoch, localId);

        RaftRequest.Outbound request = context.assertSentVoteRequest(epoch, 0, 0L, 1);

        context.time.sleep(context.requestTimeoutMs());
        context.client.poll();
        RaftRequest.Outbound retryRequest = context.assertSentVoteRequest(epoch, 0, 0L, 1);

        // We will ignore the timed out response if it arrives late
        context.deliverResponse(
            request.correlationId(),
            request.destination(),
            context.voteResponse(true, OptionalInt.empty(), 1)
        );
        context.client.poll();
        context.assertVotedCandidate(epoch, localId);

        // Become leader after receiving the retry response
        context.deliverResponse(
            retryRequest.correlationId(),
            retryRequest.destination(),
            context.voteResponse(true, OptionalInt.empty(), 1)
        );
        context.client.poll();
        context.assertElectedLeader(epoch, localId);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testHandleValidVoteRequestAsFollower(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        int epoch = 2;
        ReplicaKey otherNodeKey = replicaKey(localId + 1, withKip853Rpc);
        Set<Integer> voters = Set.of(localId, otherNodeKey.id());

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withUnknownLeader(epoch)
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.deliverRequest(context.voteRequest(epoch, otherNodeKey, epoch - 1, 1));
        context.pollUntilResponse();

        context.assertSentVoteResponse(Errors.NONE, epoch, OptionalInt.empty(), true);

        context.assertVotedCandidate(epoch, otherNodeKey.id());
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testHandleVoteRequestAsFollowerWithElectedLeader(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        int epoch = 2;
        ReplicaKey otherNodeKey = replicaKey(localId + 1, withKip853Rpc);
        int electedLeaderId = localId + 2;
        Set<Integer> voters = Set.of(localId, otherNodeKey.id(), electedLeaderId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withElectedLeader(epoch, electedLeaderId)
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.deliverRequest(context.voteRequest(epoch, otherNodeKey, epoch - 1, 1));
        context.pollUntilResponse();

        context.assertSentVoteResponse(Errors.NONE, epoch, OptionalInt.of(electedLeaderId), false);

        context.assertElectedLeader(epoch, electedLeaderId);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testHandleVoteRequestAsFollowerWithVotedCandidate(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        int epoch = 2;
        ReplicaKey otherNodeKey = replicaKey(localId + 1, withKip853Rpc);
        ReplicaKey votedCandidateKey = replicaKey(localId + 2, withKip853Rpc);
        Set<Integer> voters = Set.of(localId, otherNodeKey.id(), votedCandidateKey.id());

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withVotedCandidate(epoch, votedCandidateKey)
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.deliverRequest(context.voteRequest(epoch, otherNodeKey, epoch - 1, 1));
        context.pollUntilResponse();

        context.assertSentVoteResponse(Errors.NONE, epoch, OptionalInt.empty(), false);
        context.assertVotedCandidate(epoch, votedCandidateKey.id());
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testHandleInvalidVoteRequestWithOlderEpoch(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        int epoch = 2;
        ReplicaKey otherNodeKey = replicaKey(localId + 1, withKip853Rpc);
        Set<Integer> voters = Set.of(localId, otherNodeKey.id());

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withUnknownLeader(epoch)
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.deliverRequest(context.voteRequest(epoch - 1, otherNodeKey, epoch - 2, 1));
        context.pollUntilResponse();

        context.assertSentVoteResponse(Errors.FENCED_LEADER_EPOCH, epoch, OptionalInt.empty(), false);
        context.assertUnknownLeader(epoch);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testHandleVoteRequestAsObserver(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        int epoch = 2;
        ReplicaKey otherNodeKey = replicaKey(localId + 1, withKip853Rpc);
        int otherNodeId2 = localId + 2;
        Set<Integer> voters = Set.of(otherNodeKey.id(), otherNodeId2);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withUnknownLeader(epoch)
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.deliverRequest(context.voteRequest(epoch + 1, otherNodeKey, epoch, 1));
        context.pollUntilResponse();

        context.assertSentVoteResponse(Errors.NONE, epoch + 1, OptionalInt.empty(), true);
        context.assertVotedCandidate(epoch + 1, otherNodeKey.id());
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testLeaderIgnoreVoteRequestOnSameEpoch(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        ReplicaKey otherNodeKey = replicaKey(localId + 1, withKip853Rpc);
        Set<Integer> voters = Set.of(localId, otherNodeKey.id());

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withUnknownLeader(2)
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.becomeLeader();
        int leaderEpoch = context.currentEpoch();

        context.deliverRequest(context.voteRequest(leaderEpoch, otherNodeKey, leaderEpoch - 1, 1));

        context.client.poll();

        context.assertSentVoteResponse(Errors.NONE, leaderEpoch, OptionalInt.of(localId), false);
        context.assertElectedLeader(leaderEpoch, localId);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testListenerCommitCallbackAfterLeaderWrite(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        ReplicaKey otherNodeKey = replicaKey(localId + 1, withKip853Rpc);
        Set<Integer> voters = Set.of(localId, otherNodeKey.id());

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withUnknownLeader(4)
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.becomeLeader();
        int epoch = context.currentEpoch();

        // First poll has no high watermark advance
        context.client.poll();
        assertEquals(OptionalLong.empty(), context.client.highWatermark());
        assertEquals(1L, context.log.endOffset().offset());

        // Let follower send a fetch to initialize the high watermark,
        // note the offset 0 would be a control message for becoming the leader
        context.deliverRequest(context.fetchRequest(epoch, otherNodeKey, 1L, epoch, 0));
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(localId));
        assertEquals(OptionalLong.of(1L), context.client.highWatermark());

        List<String> records = Arrays.asList("a", "b", "c");
        long offset = context.client.prepareAppend(epoch, records);
        context.client.schedulePreparedAppend();
        context.client.poll();
        assertEquals(OptionalLong.of(0L), context.listener.lastCommitOffset());

        // Let the follower send a fetch, it should advance the high watermark
        context.deliverRequest(context.fetchRequest(epoch, otherNodeKey, 1L, epoch, 500));
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(localId));
        assertEquals(OptionalLong.of(1L), context.client.highWatermark());
        assertEquals(OptionalLong.of(0L), context.listener.lastCommitOffset());

        // Let the follower send another fetch from offset 4
        context.deliverRequest(context.fetchRequest(epoch, otherNodeKey, 4L, epoch, 500));
        context.pollUntil(() -> context.client.highWatermark().equals(OptionalLong.of(4L)));
        assertEquals(records, context.listener.commitWithLastOffset(offset));
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testLeaderImmediatelySendsDivergingEpoch(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        ReplicaKey otherNodeKey = replicaKey(localId + 1, withKip853Rpc);
        Set<Integer> voters = Set.of(localId, otherNodeKey.id());

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withUnknownLeader(5)
            .withKip853Rpc(withKip853Rpc)
            .appendToLog(1, Arrays.asList("a", "b", "c"))
            .appendToLog(3, Arrays.asList("d", "e", "f"))
            .appendToLog(5, Arrays.asList("g", "h", "i"))
            .build();

        // Start off as the leader
        context.becomeLeader();
        int epoch = context.currentEpoch();

        // Send a fetch request for an end offset and epoch which has diverged
        context.deliverRequest(context.fetchRequest(epoch, otherNodeKey, 6, 2, 500));
        context.client.poll();

        // Expect that the leader replies immediately with a diverging epoch
        FetchResponseData.PartitionData partitionResponse = context.assertSentFetchPartitionResponse();
        assertEquals(Errors.NONE, Errors.forCode(partitionResponse.errorCode()));
        assertEquals(epoch, partitionResponse.currentLeader().leaderEpoch());
        assertEquals(localId, partitionResponse.currentLeader().leaderId());
        assertEquals(1, partitionResponse.divergingEpoch().epoch());
        assertEquals(3, partitionResponse.divergingEpoch().endOffset());
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testCandidateIgnoreVoteRequestOnSameEpoch(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        ReplicaKey otherNodeKey = replicaKey(localId + 1, withKip853Rpc);
        int leaderEpoch = 2;
        Set<Integer> voters = Set.of(localId, otherNodeKey.id());

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withVotedCandidate(leaderEpoch, ReplicaKey.of(localId, ReplicaKey.NO_DIRECTORY_ID))
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.pollUntilRequest();

        context.deliverRequest(context.voteRequest(leaderEpoch, otherNodeKey, leaderEpoch - 1, 1));
        context.client.poll();
        context.assertSentVoteResponse(Errors.NONE, leaderEpoch, OptionalInt.empty(), false);
        context.assertVotedCandidate(leaderEpoch, localId);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testRetryElection(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        int otherNodeId = localId + 1;
        int epoch = 1;
        int exponentialFactor = 85;  // set it large enough so that we will bound on jitter
        Set<Integer> voters = Set.of(localId, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .updateRandom(r -> r.mockNextInt(exponentialFactor))
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.assertUnknownLeader(0);

        context.time.sleep(2L * context.electionTimeoutMs());
        context.pollUntilRequest();
        context.assertVotedCandidate(epoch, localId);

        // Quorum size is two. If the other member rejects, then we need to schedule a revote.
        RaftRequest.Outbound request = context.assertSentVoteRequest(epoch, 0, 0L, 1);
        context.deliverResponse(
            request.correlationId(),
            request.destination(),
            context.voteResponse(false, OptionalInt.empty(), 1)
        );

        context.client.poll();

        // All nodes have rejected our candidacy, but we should still remember that we had voted
        context.assertVotedCandidate(epoch, localId);

        // Even though our candidacy was rejected, we will backoff for jitter period
        // before we bump the epoch and start a new election.
        context.time.sleep(context.electionBackoffMaxMs - 1);
        context.client.poll();
        context.assertVotedCandidate(epoch, localId);

        // After jitter expires, we become a candidate again
        context.time.sleep(1);
        context.client.poll();
        context.pollUntilRequest();
        context.assertVotedCandidate(epoch + 1, localId);
        context.assertSentVoteRequest(epoch + 1, 0, 0L, 1);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testInitializeAsFollowerEmptyLog(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        int otherNodeId = localId + 1;
        int epoch = 5;
        Set<Integer> voters = Set.of(localId, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withElectedLeader(epoch, otherNodeId)
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.assertElectedLeader(epoch, otherNodeId);

        context.pollUntilRequest();

        context.assertSentFetchRequest(epoch, 0L, 0);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testInitializeAsFollowerNonEmptyLog(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        int otherNodeId = localId + 1;
        int epoch = 5;
        int lastEpoch = 3;
        Set<Integer> voters = Set.of(localId, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withElectedLeader(epoch, otherNodeId)
            .appendToLog(lastEpoch, singletonList("foo"))
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.assertElectedLeader(epoch, otherNodeId);

        context.pollUntilRequest();
        context.assertSentFetchRequest(epoch, 1L, lastEpoch);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testVoterBecomeCandidateAfterFetchTimeout(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        int otherNodeId = localId + 1;
        int epoch = 5;
        int lastEpoch = 3;
        Set<Integer> voters = Set.of(localId, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withElectedLeader(epoch, otherNodeId)
            .appendToLog(lastEpoch, singletonList("foo"))
            .withKip853Rpc(withKip853Rpc)
            .build();
        context.assertElectedLeader(epoch, otherNodeId);

        context.pollUntilRequest();
        context.assertSentFetchRequest(epoch, 1L, lastEpoch);

        context.time.sleep(context.fetchTimeoutMs);
        context.pollUntilRequest();
        context.assertSentVoteRequest(epoch + 1, lastEpoch, 1L, 1);
        context.assertVotedCandidate(epoch + 1, localId);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testFollowerAsObserverDoesNotBecomeCandidateAfterFetchTimeout(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        int otherNodeId = localId + 1;
        int epoch = 5;
        int lastEpoch = 3;
        Set<Integer> voters = Set.of(otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withElectedLeader(epoch, otherNodeId)
            .appendToLog(lastEpoch, singletonList("foo"))
            .withKip853Rpc(withKip853Rpc)
            .build();
        context.assertElectedLeader(epoch, otherNodeId);

        context.pollUntilRequest();
        context.assertSentFetchRequest(epoch, 1L, lastEpoch);

        context.time.sleep(context.fetchTimeoutMs);
        context.pollUntilRequest();
        assertTrue(context.client.quorum().isFollower());

        // transitions to unattached
        context.deliverRequest(context.voteRequest(epoch + 1, replicaKey(otherNodeId, withKip853Rpc), epoch, 1));
        context.pollUntilResponse();
        context.assertSentVoteResponse(Errors.NONE, epoch + 1, OptionalInt.empty(), true);
        assertTrue(context.client.quorum().isUnattached());
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testUnattachedAsObserverDoesNotBecomeCandidateAfterElectionTimeout(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        int otherNodeId = localId + 1;
        int epoch = 5;
        Set<Integer> voters = Set.of(otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withUnknownLeader(epoch)
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.pollUntilRequest();
        context.assertSentFetchRequest(epoch, 0L, 0);
        assertTrue(context.client.quorum().isUnattached());

        context.time.sleep(context.electionTimeoutMs() * 2);
        context.pollUntilRequest();
        assertTrue(context.client.quorum().isUnattached());
        context.assertSentFetchRequest(epoch, 0L, 0);
        // confirm no vote request was sent
        assertEquals(0, context.channel.drainSendQueue().size());

        context.deliverRequest(context.voteRequest(epoch + 1, replicaKey(otherNodeId, withKip853Rpc), epoch, 0));
        context.pollUntilResponse();
        // observer can vote
        context.assertSentVoteResponse(Errors.NONE, epoch + 1, OptionalInt.empty(), true);

        context.time.sleep(context.electionTimeoutMs() * 2);
        context.pollUntilRequest();
        // observer cannot transition to candidate though
        assertTrue(context.client.quorum().isUnattached());
        context.assertSentFetchRequest(epoch + 1, 0L, 0);
        assertEquals(0, context.channel.drainSendQueue().size());
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testUnattachedAsVoterCanBecomeFollowerAfterFindingLeader(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        int otherNodeId = localId + 1;
        int leaderNodeId = localId + 2;
        int epoch = 5;
        Set<Integer> voters = Set.of(localId, otherNodeId, leaderNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withUnknownLeader(epoch)
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.pollUntilRequest();
        RaftRequest.Outbound request = context.assertSentFetchRequest(epoch, 0L, 0);
        assertTrue(context.client.quorum().isUnattached());
        assertTrue(context.client.quorum().isVoter());

        // receives a fetch response specifying who the leader is
        Errors responseError = (request.destination().id() == otherNodeId) ? Errors.NOT_LEADER_OR_FOLLOWER : Errors.NONE;
        context.deliverResponse(
            request.correlationId(),
            request.destination(),
            context.fetchResponse(epoch, leaderNodeId, MemoryRecords.EMPTY, 0L, responseError)
        );

        context.client.poll();
        assertTrue(context.client.quorum().isFollower());
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testInitializeObserverNoPreviousState(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        int leaderId = localId + 1;
        int otherNodeId = localId + 2;
        int epoch = 5;
        Set<Integer> voters = Set.of(leaderId, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.pollUntilRequest();
        RaftRequest.Outbound fetchRequest = context.assertSentFetchRequest();
        assertTrue(voters.contains(fetchRequest.destination().id()));
        context.assertFetchRequestData(fetchRequest, 0, 0L, 0);

        context.deliverResponse(
            fetchRequest.correlationId(),
            fetchRequest.destination(),
            context.fetchResponse(epoch, leaderId, MemoryRecords.EMPTY, 0L, Errors.FENCED_LEADER_EPOCH)
        );

        context.client.poll();
        context.assertElectedLeader(epoch, leaderId);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testObserverQuorumDiscoveryFailure(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        int leaderId = localId + 1;
        int epoch = 5;
        Set<Integer> voters = Set.of(leaderId);
        List<InetSocketAddress> bootstrapServers = voters
            .stream()
            .map(RaftClientTestContext::mockAddress)
            .collect(Collectors.toList());

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withBootstrapServers(Optional.of(bootstrapServers))
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.pollUntilRequest();
        RaftRequest.Outbound fetchRequest = context.assertSentFetchRequest();
        assertTrue(context.bootstrapIds.contains(fetchRequest.destination().id()));
        context.assertFetchRequestData(fetchRequest, 0, 0L, 0);

        context.deliverResponse(
            fetchRequest.correlationId(),
            fetchRequest.destination(),
            context.fetchResponse(-1, -1, MemoryRecords.EMPTY, -1, Errors.UNKNOWN_SERVER_ERROR)
        );
        context.client.poll();

        context.time.sleep(context.retryBackoffMs);
        context.pollUntilRequest();

        fetchRequest = context.assertSentFetchRequest();
        assertTrue(context.bootstrapIds.contains(fetchRequest.destination().id()));
        context.assertFetchRequestData(fetchRequest, 0, 0L, 0);

        context.deliverResponse(
            fetchRequest.correlationId(),
            fetchRequest.destination(),
            context.fetchResponse(epoch, leaderId, MemoryRecords.EMPTY, 0L, Errors.FENCED_LEADER_EPOCH)
        );
        context.client.poll();

        context.assertElectedLeader(epoch, leaderId);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testObserverSendDiscoveryFetchAfterFetchTimeout(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        int leaderId = localId + 1;
        int otherNodeId = localId + 2;
        int epoch = 5;
        Set<Integer> voters = Set.of(leaderId, otherNodeId);
        List<InetSocketAddress> bootstrapServers = voters
            .stream()
            .map(RaftClientTestContext::mockAddress)
            .collect(Collectors.toList());

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withBootstrapServers(Optional.of(bootstrapServers))
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.pollUntilRequest();
        RaftRequest.Outbound fetchRequest = context.assertSentFetchRequest();
        assertTrue(context.bootstrapIds.contains(fetchRequest.destination().id()));
        context.assertFetchRequestData(fetchRequest, 0, 0L, 0);

        context.deliverResponse(
            fetchRequest.correlationId(),
            fetchRequest.destination(),
            context.fetchResponse(epoch, leaderId, MemoryRecords.EMPTY, 0L, Errors.FENCED_LEADER_EPOCH)
        );

        context.client.poll();
        context.assertElectedLeader(epoch, leaderId);

        context.time.sleep(context.fetchTimeoutMs);

        context.pollUntilRequest();
        fetchRequest = context.assertSentFetchRequest();
        assertNotEquals(leaderId, fetchRequest.destination().id());
        assertTrue(context.bootstrapIds.contains(fetchRequest.destination().id()));
        context.assertFetchRequestData(fetchRequest, epoch, 0L, 0);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testObserverHandleRetryFetchtToBootstrapServer(boolean withKip853Rpc) throws Exception {
        // This test tries to check that KRaft is able to handle a retrying Fetch request to
        // a boostrap server after a Fetch request to the leader.
        int localId = randomReplicaId();
        int leaderId = localId + 1;
        int otherNodeId = localId + 2;
        int epoch = 5;
        Set<Integer> voters = Set.of(leaderId, otherNodeId);
        List<InetSocketAddress> bootstrapServers = voters
            .stream()
            .map(RaftClientTestContext::mockAddress)
            .collect(Collectors.toList());

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withBootstrapServers(Optional.of(bootstrapServers))
            .withKip853Rpc(withKip853Rpc)
            .build();

        // Expect a fetch request to one of the bootstrap servers
        context.pollUntilRequest();
        RaftRequest.Outbound discoveryFetchRequest = context.assertSentFetchRequest();
        assertFalse(voters.contains(discoveryFetchRequest.destination().id()));
        assertTrue(context.bootstrapIds.contains(discoveryFetchRequest.destination().id()));
        context.assertFetchRequestData(discoveryFetchRequest, 0, 0L, 0);

        // Send a response with the leader and epoch
        context.deliverResponse(
            discoveryFetchRequest.correlationId(),
            discoveryFetchRequest.destination(),
            context.fetchResponse(epoch, leaderId, MemoryRecords.EMPTY, 0L, Errors.FENCED_LEADER_EPOCH)
        );

        context.client.poll();
        context.assertElectedLeader(epoch, leaderId);

        // Expect a fetch request to the leader
        context.pollUntilRequest();
        RaftRequest.Outbound toLeaderFetchRequest = context.assertSentFetchRequest();
        assertEquals(leaderId, toLeaderFetchRequest.destination().id());
        context.assertFetchRequestData(toLeaderFetchRequest, epoch, 0L, 0);

        context.time.sleep(context.requestTimeoutMs());

        // After the fetch timeout expect a request to a bootstrap server
        context.pollUntilRequest();
        RaftRequest.Outbound retryToBootstrapServerFetchRequest = context.assertSentFetchRequest();
        assertFalse(voters.contains(retryToBootstrapServerFetchRequest.destination().id()));
        assertTrue(context.bootstrapIds.contains(retryToBootstrapServerFetchRequest.destination().id()));
        context.assertFetchRequestData(retryToBootstrapServerFetchRequest, epoch, 0L, 0);

        // Deliver the delayed responses from the leader
        Records records = context.buildBatch(0L, 3, Arrays.asList("a", "b"));
        context.deliverResponse(
            toLeaderFetchRequest.correlationId(),
            toLeaderFetchRequest.destination(),
            context.fetchResponse(epoch, leaderId, records, 0L, Errors.NONE)
        );

        context.client.poll();

        // Deliver the same delayed responses from the bootstrap server and assume that it is the leader
        records = context.buildBatch(0L, 3, Arrays.asList("a", "b"));
        context.deliverResponse(
            retryToBootstrapServerFetchRequest.correlationId(),
            retryToBootstrapServerFetchRequest.destination(),
            context.fetchResponse(epoch, leaderId, records, 0L, Errors.NONE)
        );

        // This poll should not fail when handling the duplicate response from the bootstrap server
        context.client.poll();
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testObserverHandleRetryFetchToLeader(boolean withKip853Rpc) throws Exception {
        // This test tries to check that KRaft is able to handle a retrying Fetch request to
        // the leader after a Fetch request to the bootstrap server.
        int localId = randomReplicaId();
        int leaderId = localId + 1;
        int otherNodeId = localId + 2;
        int epoch = 5;
        Set<Integer> voters = Set.of(leaderId, otherNodeId);
        List<InetSocketAddress> bootstrapServers = voters
            .stream()
            .map(RaftClientTestContext::mockAddress)
            .collect(Collectors.toList());

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withBootstrapServers(Optional.of(bootstrapServers))
            .withKip853Rpc(withKip853Rpc)
            .build();

        // Expect a fetch request to one of the bootstrap servers
        context.pollUntilRequest();
        RaftRequest.Outbound discoveryFetchRequest = context.assertSentFetchRequest();
        assertFalse(voters.contains(discoveryFetchRequest.destination().id()));
        assertTrue(context.bootstrapIds.contains(discoveryFetchRequest.destination().id()));
        context.assertFetchRequestData(discoveryFetchRequest, 0, 0L, 0);

        // Send a response with the leader and epoch
        context.deliverResponse(
            discoveryFetchRequest.correlationId(),
            discoveryFetchRequest.destination(),
            context.fetchResponse(epoch, leaderId, MemoryRecords.EMPTY, 0L, Errors.FENCED_LEADER_EPOCH)
        );

        context.client.poll();
        context.assertElectedLeader(epoch, leaderId);

        // Expect a fetch request to the leader
        context.pollUntilRequest();
        RaftRequest.Outbound toLeaderFetchRequest = context.assertSentFetchRequest();
        assertEquals(leaderId, toLeaderFetchRequest.destination().id());
        context.assertFetchRequestData(toLeaderFetchRequest, epoch, 0L, 0);

        context.time.sleep(context.requestTimeoutMs());

        // After the fetch timeout expect a request to a bootstrap server
        context.pollUntilRequest();
        RaftRequest.Outbound retryToBootstrapServerFetchRequest = context.assertSentFetchRequest();
        assertFalse(voters.contains(retryToBootstrapServerFetchRequest.destination().id()));
        assertTrue(context.bootstrapIds.contains(retryToBootstrapServerFetchRequest.destination().id()));
        context.assertFetchRequestData(retryToBootstrapServerFetchRequest, epoch, 0L, 0);

        // At this point toLeaderFetchRequest has timed out but retryToBootstrapServerFetchRequest
        // is still waiting for a response.
        // Confirm that no new fetch request has been sent
        context.client.poll();
        assertFalse(context.channel.hasSentRequests());
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testInvalidFetchRequest(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        ReplicaKey otherNodeKey = replicaKey(localId + 1, withKip853Rpc);
        Set<Integer> voters = Set.of(localId, otherNodeKey.id());

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withUnknownLeader(4)
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.becomeLeader();
        int epoch = context.currentEpoch();

        context.deliverRequest(context.fetchRequest(epoch, otherNodeKey, -5L, 0, 0));
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.INVALID_REQUEST, epoch, OptionalInt.of(localId));

        context.deliverRequest(context.fetchRequest(epoch, otherNodeKey, 0L, -1, 0));
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.INVALID_REQUEST, epoch, OptionalInt.of(localId));

        context.deliverRequest(context.fetchRequest(epoch, otherNodeKey, 0L, epoch + 1, 0));
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.INVALID_REQUEST, epoch, OptionalInt.of(localId));

        context.deliverRequest(context.fetchRequest(epoch + 1, otherNodeKey, 0L, 0, 0));
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.UNKNOWN_LEADER_EPOCH, epoch, OptionalInt.of(localId));

        context.deliverRequest(context.fetchRequest(epoch, otherNodeKey, 0L, 0, -1));
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.INVALID_REQUEST, epoch, OptionalInt.of(localId));
    }

    private static Stream<Short> validFetchVersions() {
        int minimumSupportedVersion = 13;
        return Stream
            .iterate(minimumSupportedVersion, value -> value + 1)
            .limit(FetchRequestData.HIGHEST_SUPPORTED_VERSION - minimumSupportedVersion + 1)
            .map(Integer::shortValue);
    }

    // This test mainly focuses on whether the leader state is correctly updated under different fetch version.
    @ParameterizedTest
    @MethodSource("validFetchVersions")
    public void testLeaderStateUpdateWithDifferentFetchRequestVersions(short version) throws Exception {
        int localId = randomReplicaId();
        int otherNodeId = localId + 1;
        ReplicaKey otherNodeKey = replicaKey(otherNodeId, false);
        int epoch = 5;
        Set<Integer> voters = Set.of(localId, otherNodeKey.id());

        RaftClientTestContext context = RaftClientTestContext.initializeAsLeader(localId, voters, epoch);

        // First poll has no high watermark advance.
        context.client.poll();
        assertEquals(OptionalLong.empty(), context.client.highWatermark());
        assertEquals(1L, context.log.endOffset().offset());

        // Now we will advance the high watermark with a follower fetch request.
        FetchRequestData fetchRequestData = context.fetchRequest(epoch, otherNodeKey, 1L, epoch, 0);
        FetchRequestData request = new FetchRequest.SimpleBuilder(fetchRequestData).build(version).data();
        assertEquals((version < 15) ? otherNodeId : -1, fetchRequestData.replicaId());
        assertEquals((version < 15) ? -1 : otherNodeId, fetchRequestData.replicaState().replicaId());
        context.deliverRequest(request, version);
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(localId));
        assertEquals(OptionalLong.of(1L), context.client.highWatermark());
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testFetchRequestClusterIdValidation(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        ReplicaKey otherNodeKey = replicaKey(localId + 1, withKip853Rpc);
        Set<Integer> voters = Set.of(localId, otherNodeKey.id());

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withUnknownLeader(4)
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.becomeLeader();
        int epoch = context.currentEpoch();

        // valid cluster id is accepted
        context.deliverRequest(context.fetchRequest(epoch, otherNodeKey, -5L, 0, 0));
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.INVALID_REQUEST, epoch, OptionalInt.of(localId));

        // null cluster id is accepted
        context.deliverRequest(context.fetchRequest(epoch, null, otherNodeKey, -5L, 0, 0));
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.INVALID_REQUEST, epoch, OptionalInt.of(localId));

        // empty cluster id is rejected
        context.deliverRequest(context.fetchRequest(epoch, "", otherNodeKey, -5L, 0, 0));
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.INCONSISTENT_CLUSTER_ID);

        // invalid cluster id is rejected
        context.deliverRequest(context.fetchRequest(epoch, "invalid-uuid", otherNodeKey, -5L, 0, 0));
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.INCONSISTENT_CLUSTER_ID);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testVoteRequestClusterIdValidation(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        ReplicaKey otherNodeKey = replicaKey(localId + 1, withKip853Rpc);
        Set<Integer> voters = Set.of(localId, otherNodeKey.id());

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.becomeLeader();
        int epoch = context.currentEpoch();

        // valid cluster id is accepted
        context.deliverRequest(context.voteRequest(epoch, otherNodeKey, 0, 0));
        context.pollUntilResponse();
        context.assertSentVoteResponse(Errors.NONE, epoch, OptionalInt.of(localId), false);

        // null cluster id is accepted
        context.deliverRequest(context.voteRequest(null, epoch, otherNodeKey, 0, 0, false));
        context.pollUntilResponse();
        context.assertSentVoteResponse(Errors.NONE, epoch, OptionalInt.of(localId), false);

        // empty cluster id is rejected
        context.deliverRequest(context.voteRequest("", epoch, otherNodeKey, 0, 0, false));
        context.pollUntilResponse();
        context.assertSentVoteResponse(Errors.INCONSISTENT_CLUSTER_ID);

        // invalid cluster id is rejected
        context.deliverRequest(context.voteRequest("invalid-uuid", epoch, otherNodeKey, 0, 0, false));
        context.pollUntilResponse();
        context.assertSentVoteResponse(Errors.INCONSISTENT_CLUSTER_ID);
    }

    @Test
    public void testInvalidVoterReplicaVoteRequest() throws Exception {
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
                epoch + 1,
                otherNodeKey,
                ReplicaKey.of(10, Uuid.randomUuid()),
                epoch,
                100,
                false
            )
        );
        context.pollUntilResponse();
        context.assertSentVoteResponse(Errors.INVALID_VOTER_KEY, epoch + 1, OptionalInt.empty(), false);

        // invalid voter directory id is rejected
        context.deliverRequest(
            context.voteRequest(
                context.clusterId.toString(),
                epoch + 2,
                otherNodeKey,
                ReplicaKey.of(0, Uuid.randomUuid()),
                epoch,
                100,
                false
            )
        );
        context.pollUntilResponse();
        context.assertSentVoteResponse(Errors.INVALID_VOTER_KEY, epoch + 2, OptionalInt.empty(), false);
    }

    @Test
    public void testInvalidVoterReplicaBeginQuorumEpochRequest() throws Exception {
        int localId = randomReplicaId();
        int voter2 = localId + 1;
        int voter3 = localId + 2;
        int epoch = 5;
        Set<Integer> voters = Set.of(localId, voter2, voter3);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withUnknownLeader(epoch - 1)
            .withKip853Rpc(true)
            .build();
        context.assertUnknownLeader(epoch - 1);

        // Leader voter3 sends a begin quorum epoch request with incorrect voter id
        context.deliverRequest(
            context.beginEpochRequest(
                context.clusterId.toString(),
                epoch,
                voter3,
                ReplicaKey.of(10, Uuid.randomUuid())
            )
        );
        context.pollUntilResponse();
        context.assertSentBeginQuorumEpochResponse(Errors.INVALID_VOTER_KEY, epoch, OptionalInt.of(voter3));
        context.assertElectedLeader(epoch, voter3);

        // Leader voter3 sends a begin quorum epoch request with incorrect voter directory id
        context.deliverRequest(
            context.beginEpochRequest(
                context.clusterId.toString(),
                epoch,
                voter3,
                ReplicaKey.of(localId, Uuid.randomUuid())
            )
        );
        context.pollUntilResponse();
        context.assertSentBeginQuorumEpochResponse(Errors.INVALID_VOTER_KEY, epoch, OptionalInt.of(voter3));
        context.assertElectedLeader(epoch, voter3);

        // Leader voter3 sends a begin quorum epoch request with incorrect voter directory id
        context.deliverRequest(
            context.beginEpochRequest(
                context.clusterId.toString(),
                epoch,
                voter3,
                context.localReplicaKey()
            )
        );
        context.pollUntilResponse();
        context.assertSentBeginQuorumEpochResponse(Errors.NONE, epoch, OptionalInt.of(voter3));
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testBeginQuorumEpochRequestClusterIdValidation(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        int otherNodeId = localId + 1;
        Set<Integer> voters = Set.of(localId, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withUnknownLeader(4)
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.becomeLeader();
        int epoch = context.currentEpoch();

        // valid cluster id is accepted
        context.deliverRequest(context.beginEpochRequest(context.clusterId.toString(), epoch, localId));
        context.pollUntilResponse();
        context.assertSentBeginQuorumEpochResponse(Errors.NONE, epoch, OptionalInt.of(localId));

        // null cluster id is accepted
        context.deliverRequest(context.beginEpochRequest(epoch, localId));
        context.pollUntilResponse();
        context.assertSentBeginQuorumEpochResponse(Errors.NONE, epoch, OptionalInt.of(localId));

        // empty cluster id is rejected
        context.deliverRequest(context.beginEpochRequest("", epoch, localId));
        context.pollUntilResponse();
        context.assertSentBeginQuorumEpochResponse(Errors.INCONSISTENT_CLUSTER_ID);

        // invalid cluster id is rejected
        context.deliverRequest(context.beginEpochRequest("invalid-uuid", epoch, localId));
        context.pollUntilResponse();
        context.assertSentBeginQuorumEpochResponse(Errors.INCONSISTENT_CLUSTER_ID);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testEndQuorumEpochRequestClusterIdValidation(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        ReplicaKey otherNodeKey = replicaKey(localId + 1, withKip853Rpc);
        Set<Integer> voters = Set.of(localId, otherNodeKey.id());

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withUnknownLeader(4)
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.becomeLeader();
        int epoch = context.currentEpoch();

        // valid cluster id is accepted
        context.deliverRequest(context.endEpochRequest(epoch, localId, Collections.singletonList(otherNodeKey)));
        context.pollUntilResponse();
        context.assertSentEndQuorumEpochResponse(Errors.NONE, epoch, OptionalInt.of(localId));

        // null cluster id is accepted
        context.deliverRequest(context.endEpochRequest(null, epoch, localId, Collections.singletonList(otherNodeKey)));
        context.pollUntilResponse();
        context.assertSentEndQuorumEpochResponse(Errors.NONE, epoch, OptionalInt.of(localId));

        // empty cluster id is rejected
        context.deliverRequest(context.endEpochRequest("", epoch, localId, Collections.singletonList(otherNodeKey)));
        context.pollUntilResponse();
        context.assertSentEndQuorumEpochResponse(Errors.INCONSISTENT_CLUSTER_ID);

        // invalid cluster id is rejected
        context.deliverRequest(context.endEpochRequest("invalid-uuid", epoch, localId, Collections.singletonList(otherNodeKey)));
        context.pollUntilResponse();
        context.assertSentEndQuorumEpochResponse(Errors.INCONSISTENT_CLUSTER_ID);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testLeaderAcceptVoteFromObserver(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        int otherNodeId = localId + 1;
        Set<Integer> voters = Set.of(localId, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withUnknownLeader(4)
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.becomeLeader();
        int epoch = context.currentEpoch();

        ReplicaKey observerKey = replicaKey(localId + 2, withKip853Rpc);
        context.deliverRequest(context.voteRequest(epoch - 1, observerKey, 0, 0));
        context.client.poll();
        context.assertSentVoteResponse(Errors.FENCED_LEADER_EPOCH, epoch, OptionalInt.of(localId), false);

        context.deliverRequest(context.voteRequest(epoch, observerKey, 0, 0));
        context.client.poll();
        context.assertSentVoteResponse(Errors.NONE, epoch, OptionalInt.of(localId), false);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testInvalidVoteRequest(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        ReplicaKey otherNodeKey = replicaKey(localId + 1, withKip853Rpc);
        int epoch = 5;
        Set<Integer> voters = Set.of(localId, otherNodeKey.id());

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withElectedLeader(epoch, otherNodeKey.id())
            .withKip853Rpc(withKip853Rpc)
            .build();
        context.assertElectedLeader(epoch, otherNodeKey.id());

        context.deliverRequest(context.voteRequest(epoch + 1, otherNodeKey, 0, -5L));
        context.pollUntilResponse();
        context.assertSentVoteResponse(
            Errors.INVALID_REQUEST,
            epoch,
            OptionalInt.of(otherNodeKey.id()),
            false
        );
        context.assertElectedLeader(epoch, otherNodeKey.id());

        context.deliverRequest(context.voteRequest(epoch + 1, otherNodeKey, -1, 0L));
        context.pollUntilResponse();
        context.assertSentVoteResponse(
            Errors.INVALID_REQUEST,
            epoch,
            OptionalInt.of(otherNodeKey.id()),
            false
        );
        context.assertElectedLeader(epoch, otherNodeKey.id());

        context.deliverRequest(context.voteRequest(epoch + 1, otherNodeKey, epoch + 1, 0L));
        context.pollUntilResponse();
        context.assertSentVoteResponse(
            Errors.INVALID_REQUEST,
            epoch,
            OptionalInt.of(otherNodeKey.id()),
            false
        );
        context.assertElectedLeader(epoch, otherNodeKey.id());
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testPurgatoryFetchTimeout(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        ReplicaKey otherNodeKey = replicaKey(localId + 1, withKip853Rpc);
        Set<Integer> voters = Set.of(localId, otherNodeKey.id());

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withUnknownLeader(4)
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.becomeLeader();
        int epoch = context.currentEpoch();

        // Follower sends a fetch which cannot be satisfied immediately
        int maxWaitTimeMs = 500;
        context.deliverRequest(context.fetchRequest(epoch, otherNodeKey, 1L, epoch, maxWaitTimeMs));
        context.client.poll();
        assertEquals(0, context.channel.drainSendQueue().size());

        // After expiration of the max wait time, the fetch returns an empty record set
        context.time.sleep(maxWaitTimeMs);
        context.client.poll();
        MemoryRecords fetchedRecords = context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(localId));
        assertEquals(0, fetchedRecords.sizeInBytes());
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testPurgatoryFetchSatisfiedByWrite(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        ReplicaKey otherNodeKey = replicaKey(localId + 1, withKip853Rpc);
        Set<Integer> voters = Set.of(localId, otherNodeKey.id());

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withUnknownLeader(4)
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.becomeLeader();
        int epoch = context.currentEpoch();

        // Follower sends a fetch which cannot be satisfied immediately
        context.deliverRequest(context.fetchRequest(epoch, otherNodeKey, 1L, epoch, 500));
        context.client.poll();
        assertEquals(0, context.channel.drainSendQueue().size());

        // Append some records that can fulfill the Fetch request
        String[] appendRecords = new String[]{"a", "b", "c"};
        context.client.prepareAppend(epoch, Arrays.asList(appendRecords));
        context.client.schedulePreparedAppend();
        context.client.poll();

        MemoryRecords fetchedRecords = context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(localId));
        RaftClientTestContext.assertMatchingRecords(appendRecords, fetchedRecords);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testPurgatoryFetchCompletedByFollowerTransition(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        ReplicaKey voterKey2 = replicaKey(localId + 1, withKip853Rpc);
        int voter3 = localId + 2;
        Set<Integer> voters = Set.of(localId, voterKey2.id(), voter3);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withUnknownLeader(4)
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.becomeLeader();
        int epoch = context.currentEpoch();

        // Follower sends a fetch which cannot be satisfied immediately
        context.deliverRequest(context.fetchRequest(epoch, voterKey2, 1L, epoch, 500));
        context.client.poll();
        assertTrue(context.channel.drainSendQueue().stream()
            .noneMatch(msg -> msg.data() instanceof FetchResponseData));

        // Now we get a BeginEpoch from the other voter and become a follower
        context.deliverRequest(context.beginEpochRequest(epoch + 1, voter3));
        context.pollUntilResponse();
        context.assertElectedLeader(epoch + 1, voter3);

        // We expect the BeginQuorumEpoch response and a failed Fetch response
        context.assertSentBeginQuorumEpochResponse(Errors.NONE, epoch + 1, OptionalInt.of(voter3));

        // The fetch should be satisfied immediately and return an error
        MemoryRecords fetchedRecords = context.assertSentFetchPartitionResponse(
            Errors.NOT_LEADER_OR_FOLLOWER, epoch + 1, OptionalInt.of(voter3));
        assertEquals(0, fetchedRecords.sizeInBytes());
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testFetchResponseIgnoredAfterBecomingCandidate(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        int otherNodeId = localId + 1;
        int epoch = 5;
        // The other node starts out as the leader
        Set<Integer> voters = Set.of(localId, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withElectedLeader(epoch, otherNodeId)
            .withKip853Rpc(withKip853Rpc)
            .build();
        context.assertElectedLeader(epoch, otherNodeId);

        // Wait until we have a Fetch inflight to the leader
        context.pollUntilRequest();
        RaftRequest.Outbound fetchRequest = context.assertSentFetchRequest(epoch, 0L, 0);

        // Now await the fetch timeout and become a candidate
        context.time.sleep(context.fetchTimeoutMs);
        context.client.poll();
        context.assertVotedCandidate(epoch + 1, localId);

        // The fetch response from the old leader returns, but it should be ignored
        Records records = context.buildBatch(0L, 3, Arrays.asList("a", "b"));
        context.deliverResponse(
            fetchRequest.correlationId(),
            fetchRequest.destination(),
            context.fetchResponse(epoch, otherNodeId, records, 0L, Errors.NONE)
        );

        context.client.poll();
        assertEquals(0, context.log.endOffset().offset());
        context.assertVotedCandidate(epoch + 1, localId);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testFetchResponseIgnoredAfterBecomingFollowerOfDifferentLeader(
        boolean withKip853Rpc
    ) throws Exception {
        int localId = randomReplicaId();
        int voter2 = localId + 1;
        int voter3 = localId + 2;
        int epoch = 5;
        // Start out with `voter2` as the leader
        Set<Integer> voters = Set.of(localId, voter2, voter3);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withElectedLeader(epoch, voter2)
            .withKip853Rpc(withKip853Rpc)
            .build();
        context.assertElectedLeader(epoch, voter2);

        // Wait until we have a Fetch inflight to the leader
        context.pollUntilRequest();
        RaftRequest.Outbound fetchRequest = context.assertSentFetchRequest(epoch, 0L, 0);

        // Now receive a BeginEpoch from `voter3`
        context.deliverRequest(context.beginEpochRequest(epoch + 1, voter3));
        context.client.poll();
        context.assertElectedLeader(epoch + 1, voter3);

        // The fetch response from the old leader returns, but it should be ignored
        Records records = context.buildBatch(0L, 3, Arrays.asList("a", "b"));
        FetchResponseData response = context.fetchResponse(epoch, voter2, records, 0L, Errors.NONE);
        context.deliverResponse(
            fetchRequest.correlationId(),
            fetchRequest.destination(),
            response
        );

        context.client.poll();
        assertEquals(0, context.log.endOffset().offset());
        context.assertElectedLeader(epoch + 1, voter3);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testVoteResponseIgnoredAfterBecomingFollower(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        int voter2 = localId + 1;
        int voter3 = localId + 2;
        int epoch = 5;
        Set<Integer> voters = Set.of(localId, voter2, voter3);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withUnknownLeader(epoch - 1)
            .withKip853Rpc(withKip853Rpc)
            .build();
        context.assertUnknownLeader(epoch - 1);

        // Sleep a little to ensure that we become a candidate
        context.time.sleep(context.electionTimeoutMs() * 2L);

        // Wait until the vote requests are inflight
        context.pollUntilRequest();
        context.assertVotedCandidate(epoch, localId);
        List<RaftRequest.Outbound> voteRequests = context.collectVoteRequests(epoch, 0, 0);
        assertEquals(2, voteRequests.size());

        // While the vote requests are still inflight, we receive a BeginEpoch for the same epoch
        context.deliverRequest(context.beginEpochRequest(epoch, voter3));
        context.client.poll();
        context.assertElectedLeader(epoch, voter3);

        // The vote requests now return and should be ignored
        VoteResponseData voteResponse1 = context.voteResponse(false, OptionalInt.empty(), epoch);
        context.deliverResponse(
            voteRequests.get(0).correlationId(),
            voteRequests.get(0).destination(),
            voteResponse1
        );

        VoteResponseData voteResponse2 = context.voteResponse(false, OptionalInt.of(voter3), epoch);
        context.deliverResponse(
            voteRequests.get(1).correlationId(),
            voteRequests.get(1).destination(),
            voteResponse2
        );

        context.client.poll();
        context.assertElectedLeader(epoch, voter3);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testFollowerLeaderRediscoveryAfterBrokerNotAvailableError(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        int leaderId = localId + 1;
        int otherNodeId = localId + 2;
        int epoch = 5;
        Set<Integer> voters = Set.of(leaderId, localId, otherNodeId);
        List<InetSocketAddress> bootstrapServers = voters
            .stream()
            .map(RaftClientTestContext::mockAddress)
            .collect(Collectors.toList());

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withBootstrapServers(Optional.of(bootstrapServers))
            .withKip853Rpc(withKip853Rpc)
            .withElectedLeader(epoch, leaderId)
            .build();

        context.pollUntilRequest();
        RaftRequest.Outbound fetchRequest1 = context.assertSentFetchRequest();
        assertEquals(leaderId, fetchRequest1.destination().id());
        context.assertFetchRequestData(fetchRequest1, epoch, 0L, 0);

        context.deliverResponse(
            fetchRequest1.correlationId(),
            fetchRequest1.destination(),
            context.fetchResponse(epoch, -1, MemoryRecords.EMPTY, -1, Errors.BROKER_NOT_AVAILABLE)
        );
        context.pollUntilRequest();

        // We should retry the Fetch against the other voter since the original
        // voter connection will be backing off.
        RaftRequest.Outbound fetchRequest2 = context.assertSentFetchRequest();
        assertNotEquals(leaderId, fetchRequest2.destination().id());
        assertTrue(context.bootstrapIds.contains(fetchRequest2.destination().id()));
        context.assertFetchRequestData(fetchRequest2, epoch, 0L, 0);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testFollowerLeaderRediscoveryAfterRequestTimeout(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        int leaderId = localId + 1;
        int otherNodeId = localId + 2;
        int epoch = 5;
        Set<Integer> voters = Set.of(leaderId, localId, otherNodeId);
        List<InetSocketAddress> bootstrapServers = voters
            .stream()
            .map(RaftClientTestContext::mockAddress)
            .collect(Collectors.toList());

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withBootstrapServers(Optional.of(bootstrapServers))
            .withKip853Rpc(withKip853Rpc)
            .withElectedLeader(epoch, leaderId)
            .build();

        context.pollUntilRequest();
        RaftRequest.Outbound fetchRequest1 = context.assertSentFetchRequest();
        assertEquals(leaderId, fetchRequest1.destination().id());
        context.assertFetchRequestData(fetchRequest1, epoch, 0L, 0);

        context.time.sleep(context.requestTimeoutMs());
        context.pollUntilRequest();

        // We should retry the Fetch against the other voter since the original
        // voter connection will be backing off.
        RaftRequest.Outbound fetchRequest2 = context.assertSentFetchRequest();
        assertNotEquals(leaderId, fetchRequest2.destination().id());
        assertTrue(context.bootstrapIds.contains(fetchRequest2.destination().id()));
        context.assertFetchRequestData(fetchRequest2, epoch, 0L, 0);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testObserverLeaderRediscoveryAfterBrokerNotAvailableError(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        int leaderId = localId + 1;
        int otherNodeId = localId + 2;
        int epoch = 5;
        Set<Integer> voters = Set.of(leaderId, otherNodeId);
        List<InetSocketAddress> bootstrapServers = voters
            .stream()
            .map(RaftClientTestContext::mockAddress)
            .collect(Collectors.toList());

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withBootstrapServers(Optional.of(bootstrapServers))
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.discoverLeaderAsObserver(leaderId, epoch);

        context.pollUntilRequest();
        RaftRequest.Outbound fetchRequest1 = context.assertSentFetchRequest();
        assertEquals(leaderId, fetchRequest1.destination().id());
        context.assertFetchRequestData(fetchRequest1, epoch, 0L, 0);

        context.deliverResponse(
            fetchRequest1.correlationId(),
            fetchRequest1.destination(),
            context.fetchResponse(epoch, -1, MemoryRecords.EMPTY, -1, Errors.BROKER_NOT_AVAILABLE)
        );
        context.pollUntilRequest();

        // We should retry the Fetch against the other voter since the original
        // voter connection will be backing off.
        RaftRequest.Outbound fetchRequest2 = context.assertSentFetchRequest();
        assertNotEquals(leaderId, fetchRequest2.destination().id());
        assertTrue(context.bootstrapIds.contains(fetchRequest2.destination().id()));
        context.assertFetchRequestData(fetchRequest2, epoch, 0L, 0);

        context.deliverResponse(
            fetchRequest2.correlationId(),
            fetchRequest2.destination(),
            context.fetchResponse(epoch, leaderId, MemoryRecords.EMPTY, 0L, Errors.NOT_LEADER_OR_FOLLOWER)
        );
        context.client.poll();

        context.assertElectedLeader(epoch, leaderId);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testObserverLeaderRediscoveryAfterRequestTimeout(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        int leaderId = localId + 1;
        int otherNodeId = localId + 2;
        int epoch = 5;
        Set<Integer> voters = Set.of(leaderId, otherNodeId);
        List<InetSocketAddress> bootstrapServers = voters
            .stream()
            .map(RaftClientTestContext::mockAddress)
            .collect(Collectors.toList());

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withBootstrapServers(Optional.of(bootstrapServers))
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.discoverLeaderAsObserver(leaderId, epoch);

        context.pollUntilRequest();
        RaftRequest.Outbound fetchRequest1 = context.assertSentFetchRequest();
        assertEquals(leaderId, fetchRequest1.destination().id());
        context.assertFetchRequestData(fetchRequest1, epoch, 0L, 0);

        context.time.sleep(context.requestTimeoutMs());
        context.pollUntilRequest();

        // We should retry the Fetch against the other voter since the original
        // voter connection will be backing off.
        RaftRequest.Outbound fetchRequest2 = context.assertSentFetchRequest();
        assertNotEquals(leaderId, fetchRequest2.destination().id());
        assertTrue(context.bootstrapIds.contains(fetchRequest2.destination().id()));
        context.assertFetchRequestData(fetchRequest2, epoch, 0L, 0);

        context.deliverResponse(
            fetchRequest2.correlationId(),
            fetchRequest2.destination(),
            context.fetchResponse(epoch, leaderId, MemoryRecords.EMPTY, 0L, Errors.FENCED_LEADER_EPOCH)
        );
        context.client.poll();

        context.assertElectedLeader(epoch, leaderId);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testLeaderGracefulShutdown(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        ReplicaKey otherNodeKey = replicaKey(localId + 1, withKip853Rpc);
        Set<Integer> voters = Set.of(localId, otherNodeKey.id());

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.becomeLeader();
        int epoch = context.currentEpoch();

        // Now shutdown
        int shutdownTimeoutMs = 5000;
        CompletableFuture<Void> shutdownFuture = context.client.shutdown(shutdownTimeoutMs);

        // We should still be running until we have had a chance to send EndQuorumEpoch
        assertTrue(context.client.isShuttingDown());
        assertTrue(context.client.isRunning());
        assertFalse(shutdownFuture.isDone());

        // Send EndQuorumEpoch request to the other voter
        context.pollUntilRequest();
        assertTrue(context.client.isShuttingDown());
        assertTrue(context.client.isRunning());
        context.assertSentEndQuorumEpochRequest(1, otherNodeKey.id());

        // We should still be able to handle vote requests during graceful shutdown
        // in order to help the new leader get elected
        context.deliverRequest(context.voteRequest(epoch + 1, otherNodeKey, epoch, 1L));
        context.client.poll();
        context.assertSentVoteResponse(Errors.NONE, epoch + 1, OptionalInt.empty(), true);

        // Graceful shutdown completes when a new leader is elected
        context.deliverRequest(context.beginEpochRequest(2, otherNodeKey.id()));

        TestUtils.waitForCondition(() -> {
            context.client.poll();
            return !context.client.isRunning();
        }, 5000, "Client failed to shutdown before expiration of timeout");
        assertFalse(context.client.isShuttingDown());
        assertTrue(shutdownFuture.isDone());
        assertNull(shutdownFuture.get());
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testEndQuorumEpochSentBasedOnFetchOffset(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        ReplicaKey closeFollower = replicaKey(localId + 2, withKip853Rpc);
        ReplicaKey laggingFollower = replicaKey(localId + 1, withKip853Rpc);
        Set<Integer> voters = Set.of(localId, closeFollower.id(), laggingFollower.id());

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.becomeLeader();
        int epoch = context.currentEpoch();

        // The lagging follower fetches first
        context.deliverRequest(context.fetchRequest(1, laggingFollower, 1L, epoch, 0));
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(1L, epoch);

        // Append some records, so that the close follower will be able to advance further.
        context.client.prepareAppend(epoch, Arrays.asList("foo", "bar"));
        context.client.schedulePreparedAppend();
        context.client.poll();

        context.deliverRequest(context.fetchRequest(epoch, closeFollower, 3L, epoch, 0));
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(3L, epoch);

        // Now shutdown
        context.client.shutdown(context.electionTimeoutMs() * 2);

        // We should still be running until we have had a chance to send EndQuorumEpoch
        assertTrue(context.client.isRunning());

        // Send EndQuorumEpoch request to the close follower
        context.pollUntilRequest();
        assertTrue(context.client.isRunning());

        context.collectEndQuorumRequests(
            epoch,
            Set.of(closeFollower.id(), laggingFollower.id()),
            Optional.of(
                Arrays.asList(
                    replicaKey(closeFollower.id(), false),
                    replicaKey(laggingFollower.id(), false)
                )
            )
        );
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testDescribeQuorumNonLeader(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        ReplicaKey voter2 = replicaKey(localId + 1, withKip853Rpc);
        ReplicaKey voter3 = replicaKey(localId + 2, withKip853Rpc);
        int epoch = 2;
        Set<Integer> voters = Set.of(localId, voter2.id(), voter3.id());

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withUnknownLeader(epoch)
            .build();

        context.deliverRequest(context.describeQuorumRequest());
        context.pollUntilResponse();

        DescribeQuorumResponseData responseData = context.collectDescribeQuorumResponse();
        assertEquals(Errors.NONE, Errors.forCode(responseData.errorCode()));
        assertEquals("", responseData.errorMessage());

        assertEquals(1, responseData.topics().size());
        DescribeQuorumResponseData.TopicData topicData = responseData.topics().get(0);
        assertEquals(context.metadataPartition.topic(), topicData.topicName());

        assertEquals(1, topicData.partitions().size());
        DescribeQuorumResponseData.PartitionData partitionData = topicData.partitions().get(0);
        assertEquals(context.metadataPartition.partition(), partitionData.partitionIndex());
        assertEquals(Errors.NOT_LEADER_OR_FOLLOWER, Errors.forCode(partitionData.errorCode()));
        assertEquals(Errors.NOT_LEADER_OR_FOLLOWER.message(), partitionData.errorMessage());
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testDescribeQuorumWithOnlyStaticVoters(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        ReplicaKey local = replicaKey(localId, true);
        ReplicaKey follower1 = replicaKey(localId + 1, true);
        Set<Integer> voters = Set.of(localId, follower1.id());

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, local.directoryId().get())
            .withStaticVoters(voters)
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.becomeLeader();
        int epoch = context.currentEpoch();

        // Describe quorum response will not include directory ids
        context.deliverRequest(context.describeQuorumRequest());
        context.pollUntilResponse();
        List<ReplicaState> expectedVoterStates = Arrays.asList(
            new ReplicaState()
                .setReplicaId(localId)
                .setReplicaDirectoryId(ReplicaKey.NO_DIRECTORY_ID)
                .setLogEndOffset(1L)
                .setLastFetchTimestamp(context.time.milliseconds())
                .setLastCaughtUpTimestamp(context.time.milliseconds()),
            new ReplicaState()
                .setReplicaId(follower1.id())
                .setReplicaDirectoryId(ReplicaKey.NO_DIRECTORY_ID)
                .setLogEndOffset(-1L)
                .setLastFetchTimestamp(-1)
                .setLastCaughtUpTimestamp(-1));
        context.assertSentDescribeQuorumResponse(localId, epoch, -1L, expectedVoterStates, Collections.emptyList());
    }

    @ParameterizedTest
    @CsvSource({ "true, true", "true, false", "false, false" })
    public void testDescribeQuorumWithFollowers(boolean withKip853Rpc, boolean withBootstrapSnapshot) throws Exception {
        int localId = randomReplicaId();
        int followerId1 = localId + 1;
        int followerId2 = localId + 2;
        ReplicaKey local = replicaKey(localId, withBootstrapSnapshot);
        // local directory id must exist
        Uuid localDirectoryId = local.directoryId().orElse(Uuid.randomUuid());
        ReplicaKey bootstrapFollower1 = replicaKey(followerId1, withBootstrapSnapshot);
        // if withBootstrapSnapshot is false, directory ids are still needed by the static voter set
        Uuid followerDirectoryId1 = bootstrapFollower1.directoryId().orElse(withKip853Rpc ? Uuid.randomUuid() : ReplicaKey.NO_DIRECTORY_ID);
        ReplicaKey follower1 = ReplicaKey.of(followerId1, followerDirectoryId1);
        ReplicaKey bootstrapFollower2 = replicaKey(followerId2, withBootstrapSnapshot);
        Uuid followerDirectoryId2 = bootstrapFollower2.directoryId().orElse(withKip853Rpc ? Uuid.randomUuid() : ReplicaKey.NO_DIRECTORY_ID);
        ReplicaKey follower2 = ReplicaKey.of(followerId2, followerDirectoryId2);

        RaftClientTestContext.Builder builder = new RaftClientTestContext.Builder(localId, localDirectoryId)
            .withKip853Rpc(withKip853Rpc);

        if (withBootstrapSnapshot) {
            VoterSet bootstrapVoterSet = VoterSetTest.voterSet(Stream.of(local, bootstrapFollower1, bootstrapFollower2));
            builder.withBootstrapSnapshot(Optional.of(bootstrapVoterSet));
        } else {
            VoterSet staticVoterSet = VoterSetTest.voterSet(Stream.of(local, follower1, follower2));
            builder.withStaticVoters(staticVoterSet);
        }
        RaftClientTestContext context = builder.build();

        context.becomeLeader();
        int epoch = context.currentEpoch();

        // Describe quorum response before any fetches made
        context.deliverRequest(context.describeQuorumRequest());
        context.pollUntilResponse();
        List<ReplicaState> expectedVoterStates = Arrays.asList(
            new ReplicaState()
                .setReplicaId(localId)
                .setReplicaDirectoryId(withBootstrapSnapshot ? context.localReplicaKey().directoryId().get() : ReplicaKey.NO_DIRECTORY_ID)
                .setLogEndOffset(withBootstrapSnapshot ? 3L : 1L)
                .setLastFetchTimestamp(context.time.milliseconds())
                .setLastCaughtUpTimestamp(context.time.milliseconds()),
            new ReplicaState()
                .setReplicaId(followerId1)
                .setReplicaDirectoryId(withBootstrapSnapshot ? follower1.directoryId().get() : ReplicaKey.NO_DIRECTORY_ID)
                .setLogEndOffset(-1L)
                .setLastFetchTimestamp(-1)
                .setLastCaughtUpTimestamp(-1),
            new ReplicaState()
                .setReplicaId(followerId2)
                .setReplicaDirectoryId(withBootstrapSnapshot ? follower2.directoryId().get() : ReplicaKey.NO_DIRECTORY_ID)
                .setLogEndOffset(-1L)
                .setLastFetchTimestamp(-1)
                .setLastCaughtUpTimestamp(-1));
        context.assertSentDescribeQuorumResponse(localId, epoch, -1L, expectedVoterStates, Collections.emptyList());

        context.time.sleep(100);
        long fetchOffset = withBootstrapSnapshot ? 3L : 1L;
        long followerFetchTime1 = context.time.milliseconds();
        context.deliverRequest(context.fetchRequest(1, follower1, fetchOffset, epoch, 0));
        context.pollUntilResponse();
        long expectedHW = fetchOffset;
        context.assertSentFetchPartitionResponse(expectedHW, epoch);

        List<String> records = Arrays.asList("foo", "bar");
        long nextFetchOffset = fetchOffset + records.size();
        context.client.prepareAppend(epoch, records);
        context.client.schedulePreparedAppend();
        context.client.poll();

        context.time.sleep(100);
        context.deliverRequest(context.describeQuorumRequest());
        context.pollUntilResponse();

        expectedVoterStates.get(0)
            .setLogEndOffset(nextFetchOffset)
            .setLastFetchTimestamp(context.time.milliseconds())
            .setLastCaughtUpTimestamp(context.time.milliseconds());
        expectedVoterStates.get(1)
            .setLogEndOffset(fetchOffset)
            .setLastFetchTimestamp(followerFetchTime1)
            .setLastCaughtUpTimestamp(followerFetchTime1);
        context.assertSentDescribeQuorumResponse(localId, epoch, expectedHW, expectedVoterStates, Collections.emptyList());

        // After follower2 catches up to leader
        context.time.sleep(100);
        long followerFetchTime2 = context.time.milliseconds();
        context.deliverRequest(context.fetchRequest(epoch, follower2, nextFetchOffset, epoch, 0));
        context.pollUntilResponse();
        expectedHW = nextFetchOffset;
        context.assertSentFetchPartitionResponse(expectedHW, epoch);

        context.time.sleep(100);
        context.deliverRequest(context.describeQuorumRequest());
        context.pollUntilResponse();

        expectedVoterStates.get(0)
            .setLastFetchTimestamp(context.time.milliseconds())
            .setLastCaughtUpTimestamp(context.time.milliseconds());
        expectedVoterStates.get(2)
            .setLogEndOffset(nextFetchOffset)
            .setLastFetchTimestamp(followerFetchTime2)
            .setLastCaughtUpTimestamp(followerFetchTime2);
        context.assertSentDescribeQuorumResponse(localId, epoch, expectedHW, expectedVoterStates, Collections.emptyList());

        // Describe quorum returns error if leader loses leadership
        context.time.sleep(context.checkQuorumTimeoutMs);
        context.deliverRequest(context.describeQuorumRequest());
        context.pollUntilResponse();
        context.assertSentDescribeQuorumResponse(Errors.NOT_LEADER_OR_FOLLOWER, 0, 0, 0, Collections.emptyList(), Collections.emptyList());
    }

    @ParameterizedTest
    @CsvSource({ "true, true", "true, false", "false, false" })
    public void testDescribeQuorumWithObserver(boolean withKip853Rpc, boolean withBootstrapSnapshot) throws Exception {
        int localId = randomReplicaId();
        int followerId = localId + 1;
        ReplicaKey local = replicaKey(localId, withBootstrapSnapshot);
        Uuid localDirectoryId = local.directoryId().orElse(Uuid.randomUuid());
        ReplicaKey bootstrapFollower = replicaKey(followerId, withBootstrapSnapshot);
        Uuid followerDirectoryId = bootstrapFollower.directoryId().orElse(withKip853Rpc ? Uuid.randomUuid() : ReplicaKey.NO_DIRECTORY_ID);
        ReplicaKey follower = ReplicaKey.of(followerId, followerDirectoryId);

        RaftClientTestContext.Builder builder = new RaftClientTestContext.Builder(localId, localDirectoryId)
            .withKip853Rpc(withKip853Rpc);

        if (withBootstrapSnapshot) {
            VoterSet bootstrapVoterSet = VoterSetTest.voterSet(Stream.of(local, bootstrapFollower));
            builder.withBootstrapSnapshot(Optional.of(bootstrapVoterSet));
        } else {
            VoterSet staticVoterSet = VoterSetTest.voterSet(Stream.of(local, follower));
            builder.withStaticVoters(staticVoterSet);
        }
        RaftClientTestContext context = builder.build();

        context.becomeLeader();
        int epoch = context.currentEpoch();

        // Update HW to non-initial value
        context.time.sleep(100);
        long fetchOffset = withBootstrapSnapshot ? 3L : 1L;
        long followerFetchTime = context.time.milliseconds();
        context.deliverRequest(context.fetchRequest(1, follower, fetchOffset, epoch, 0));
        context.pollUntilResponse();
        long expectedHW = fetchOffset;
        context.assertSentFetchPartitionResponse(expectedHW, epoch);

        // Create observer
        ReplicaKey observer = replicaKey(localId + 2, withKip853Rpc);
        Uuid observerDirectoryId = observer.directoryId().orElse(ReplicaKey.NO_DIRECTORY_ID);
        context.time.sleep(100);
        long observerFetchTime = context.time.milliseconds();
        context.deliverRequest(context.fetchRequest(epoch, observer, 0L, 0, 0));
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(expectedHW, epoch);

        context.time.sleep(100);
        context.deliverRequest(context.describeQuorumRequest());
        context.pollUntilResponse();

        List<ReplicaState> expectedVoterStates = Arrays.asList(
            new ReplicaState()
                .setReplicaId(localId)
                .setReplicaDirectoryId(withBootstrapSnapshot ? localDirectoryId : ReplicaKey.NO_DIRECTORY_ID)
                // As we are appending the records directly to the log,
                // the leader end offset hasn't been updated yet.
                .setLogEndOffset(fetchOffset)
                .setLastFetchTimestamp(context.time.milliseconds())
                .setLastCaughtUpTimestamp(context.time.milliseconds()),
            new ReplicaState()
                .setReplicaId(follower.id())
                .setReplicaDirectoryId(withBootstrapSnapshot ? followerDirectoryId : ReplicaKey.NO_DIRECTORY_ID)
                .setLogEndOffset(fetchOffset)
                .setLastFetchTimestamp(followerFetchTime)
                .setLastCaughtUpTimestamp(followerFetchTime));
        List<ReplicaState> expectedObserverStates = singletonList(
            new ReplicaState()
                .setReplicaId(observer.id())
                .setReplicaDirectoryId(observerDirectoryId)
                .setLogEndOffset(0L)
                .setLastFetchTimestamp(observerFetchTime)
                .setLastCaughtUpTimestamp(-1L));
        context.assertSentDescribeQuorumResponse(localId, epoch, expectedHW, expectedVoterStates, expectedObserverStates);

        // Update observer fetch state
        context.time.sleep(100);
        observerFetchTime = context.time.milliseconds();
        context.deliverRequest(context.fetchRequest(epoch, observer, fetchOffset, epoch, 0));
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(expectedHW, epoch);

        context.time.sleep(100);
        context.deliverRequest(context.describeQuorumRequest());
        context.pollUntilResponse();

        expectedVoterStates.get(0)
            .setLastFetchTimestamp(context.time.milliseconds())
            .setLastCaughtUpTimestamp(context.time.milliseconds());
        expectedObserverStates.get(0)
            .setLogEndOffset(fetchOffset)
            .setLastFetchTimestamp(observerFetchTime)
            .setLastCaughtUpTimestamp(observerFetchTime);
        context.assertSentDescribeQuorumResponse(localId, epoch, expectedHW, expectedVoterStates, expectedObserverStates);

        // Observer falls behind
        context.time.sleep(100);
        List<String> records = Arrays.asList("foo", "bar");
        context.client.prepareAppend(epoch, records);
        context.client.schedulePreparedAppend();
        context.client.poll();

        context.deliverRequest(context.describeQuorumRequest());
        context.pollUntilResponse();

        expectedVoterStates.get(0)
            .setLogEndOffset(fetchOffset + records.size())
            .setLastFetchTimestamp(context.time.milliseconds())
            .setLastCaughtUpTimestamp(context.time.milliseconds());
        context.assertSentDescribeQuorumResponse(localId, epoch, expectedHW, expectedVoterStates, expectedObserverStates);

        // Observer is removed due to inactivity
        long timeToSleep = LeaderState.OBSERVER_SESSION_TIMEOUT_MS;
        while (timeToSleep > 0) {
            // Follower needs to continue polling to keep leader alive
            followerFetchTime = context.time.milliseconds();
            context.deliverRequest(context.fetchRequest(epoch, follower, fetchOffset, epoch, 0));
            context.pollUntilResponse();
            context.assertSentFetchPartitionResponse(expectedHW, epoch);

            context.time.sleep(context.checkQuorumTimeoutMs - 1);
            timeToSleep = timeToSleep - (context.checkQuorumTimeoutMs - 1);
        }
        context.deliverRequest(context.describeQuorumRequest());
        context.pollUntilResponse();

        expectedVoterStates.get(0)
            .setLastFetchTimestamp(context.time.milliseconds())
            .setLastCaughtUpTimestamp(context.time.milliseconds());
        expectedVoterStates.get(1)
            .setLastFetchTimestamp(followerFetchTime);
        context.assertSentDescribeQuorumResponse(localId, epoch, expectedHW, expectedVoterStates, Collections.emptyList());

        // No-op for negative node id
        context.deliverRequest(context.fetchRequest(epoch, ReplicaKey.of(-1, ReplicaKey.NO_DIRECTORY_ID), 0L, 0, 0));
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(expectedHW, epoch);
        context.deliverRequest(context.describeQuorumRequest());
        context.pollUntilResponse();

        expectedVoterStates.get(0)
            .setLastFetchTimestamp(context.time.milliseconds())
            .setLastCaughtUpTimestamp(context.time.milliseconds());
        context.assertSentDescribeQuorumResponse(localId, epoch, expectedHW, expectedVoterStates, Collections.emptyList());
    }

    @ParameterizedTest
    @CsvSource({ "true, true", "true, false", "false, false" })
    public void testDescribeQuorumNonMonotonicFollowerFetch(boolean withKip853Rpc, boolean withBootstrapSnapshot) throws Exception {
        int localId = randomReplicaId();
        ReplicaKey local = replicaKey(localId, withBootstrapSnapshot);
        Uuid localDirectoryId = local.directoryId().orElse(Uuid.randomUuid());
        int followerId = localId + 1;
        ReplicaKey bootstrapFollower = replicaKey(followerId, withBootstrapSnapshot);
        Uuid followerDirectoryId = bootstrapFollower.directoryId().orElse(withKip853Rpc ? Uuid.randomUuid() : ReplicaKey.NO_DIRECTORY_ID);
        ReplicaKey follower = ReplicaKey.of(followerId, followerDirectoryId);

        RaftClientTestContext.Builder builder = new RaftClientTestContext.Builder(localId, localDirectoryId)
            .withKip853Rpc(withKip853Rpc);
        if (withBootstrapSnapshot) {
            VoterSet bootstrapVoterSet = VoterSetTest.voterSet(Stream.of(local, bootstrapFollower));
            builder.withBootstrapSnapshot(Optional.of(bootstrapVoterSet));
        } else {
            VoterSet staticVoterSet = VoterSetTest.voterSet(Stream.of(local, follower));
            builder.withStaticVoters(staticVoterSet);
        }
        RaftClientTestContext context = builder.build();

        context.becomeLeader();
        int epoch = context.currentEpoch();

        // Update HW to non-initial value
        context.time.sleep(100);
        List<String> batch = Arrays.asList("foo", "bar");
        context.client.prepareAppend(epoch, batch);
        context.client.schedulePreparedAppend();
        context.client.poll();
        long fetchOffset = withBootstrapSnapshot ? 5L : 3L;
        long followerFetchTime = context.time.milliseconds();
        context.deliverRequest(context.fetchRequest(epoch, follower, fetchOffset, epoch, 0));
        context.pollUntilResponse();
        long expectedHW = fetchOffset;
        context.assertSentFetchPartitionResponse(expectedHW, epoch);

        context.time.sleep(100);
        context.deliverRequest(context.describeQuorumRequest());
        context.pollUntilResponse();
        List<ReplicaState> expectedVoterStates = Arrays.asList(
            new ReplicaState()
                .setReplicaId(localId)
                .setReplicaDirectoryId(withBootstrapSnapshot ? local.directoryId().get() : ReplicaKey.NO_DIRECTORY_ID)
                .setLogEndOffset(fetchOffset)
                .setLastFetchTimestamp(context.time.milliseconds())
                .setLastCaughtUpTimestamp(context.time.milliseconds()),
            new ReplicaState()
                .setReplicaId(follower.id())
                .setReplicaDirectoryId(withBootstrapSnapshot ? follower.directoryId().get() : ReplicaKey.NO_DIRECTORY_ID)
                .setLogEndOffset(fetchOffset)
                .setLastFetchTimestamp(followerFetchTime)
                .setLastCaughtUpTimestamp(followerFetchTime));
        context.assertSentDescribeQuorumResponse(localId, epoch, expectedHW, expectedVoterStates, Collections.emptyList());

        // Follower crashes and disk is lost. It fetches an earlier offset to rebuild state.
        // The leader will report an error in the logs, but will not let the high watermark rewind
        context.time.sleep(100);
        followerFetchTime = context.time.milliseconds();
        context.deliverRequest(context.fetchRequest(epoch, follower, fetchOffset - 1, epoch, 0));
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(expectedHW, epoch);
        context.time.sleep(100);
        context.deliverRequest(context.describeQuorumRequest());
        context.pollUntilResponse();

        expectedVoterStates.get(0)
            .setLastFetchTimestamp(context.time.milliseconds())
            .setLastCaughtUpTimestamp(context.time.milliseconds());
        expectedVoterStates.get(1)
            .setLogEndOffset(fetchOffset - batch.size())
            .setLastFetchTimestamp(followerFetchTime);
        context.assertSentDescribeQuorumResponse(localId, epoch, expectedHW, expectedVoterStates, Collections.emptyList());
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testStaticVotersIgnoredWithBootstrapSnapshot(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        ReplicaKey local = replicaKey(localId, true);
        ReplicaKey follower = replicaKey(localId + 1, true);
        ReplicaKey follower2 = replicaKey(localId + 2, true);
        // only include one follower in static voter set
        Set<Integer> staticVoters = Set.of(localId, follower.id());
        VoterSet voterSet = VoterSetTest.voterSet(Stream.of(local, follower, follower2));

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, local.directoryId().get())
            .withStaticVoters(staticVoters)
            .withKip853Rpc(withKip853Rpc)
            .withBootstrapSnapshot(Optional.of(voterSet))
            .build();

        context.becomeLeader();
        int epoch = context.currentEpoch();
        // check describe quorum response has both followers
        context.deliverRequest(context.describeQuorumRequest());
        context.pollUntilResponse();
        List<ReplicaState> expectedVoterStates = Arrays.asList(
            new ReplicaState()
                .setReplicaId(localId)
                .setReplicaDirectoryId(withKip853Rpc ? local.directoryId().get() : ReplicaKey.NO_DIRECTORY_ID)
                .setLogEndOffset(3L)
                .setLastFetchTimestamp(context.time.milliseconds())
                .setLastCaughtUpTimestamp(context.time.milliseconds()),
            new ReplicaState()
                .setReplicaId(follower.id())
                .setReplicaDirectoryId(withKip853Rpc ? follower.directoryId().get() : ReplicaKey.NO_DIRECTORY_ID)
                .setLogEndOffset(-1L)
                .setLastFetchTimestamp(-1)
                .setLastCaughtUpTimestamp(-1),
            new ReplicaState()
                .setReplicaId(follower2.id())
                .setReplicaDirectoryId(withKip853Rpc ? follower2.directoryId().get() : ReplicaKey.NO_DIRECTORY_ID)
                .setLogEndOffset(-1L)
                .setLastFetchTimestamp(-1)
                .setLastCaughtUpTimestamp(-1));
        context.assertSentDescribeQuorumResponse(localId, epoch, -1L, expectedVoterStates, Collections.emptyList());
    }


    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testLeaderGracefulShutdownTimeout(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        int otherNodeId = localId + 1;
        Set<Integer> voters = Set.of(localId, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withUnknownLeader(1)
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.becomeLeader();
        int epoch = context.currentEpoch();

        // Now shutdown
        int shutdownTimeoutMs = 5000;
        CompletableFuture<Void> shutdownFuture = context.client.shutdown(shutdownTimeoutMs);

        // We should still be running until we have had a chance to send EndQuorumEpoch
        assertTrue(context.client.isRunning());
        assertFalse(shutdownFuture.isDone());

        // Send EndQuorumEpoch request to the other vote
        context.pollUntilRequest();
        assertTrue(context.client.isRunning());

        context.assertSentEndQuorumEpochRequest(epoch, otherNodeId);

        // The shutdown timeout is hit before we receive any requests or responses indicating an epoch bump
        context.time.sleep(shutdownTimeoutMs);

        context.client.poll();
        assertFalse(context.client.isRunning());
        assertTrue(shutdownFuture.isCompletedExceptionally());
        assertFutureThrows(shutdownFuture, TimeoutException.class);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testFollowerGracefulShutdown(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        int otherNodeId = localId + 1;
        int epoch = 5;
        Set<Integer> voters = Set.of(localId, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withElectedLeader(epoch, otherNodeId)
            .withKip853Rpc(withKip853Rpc)
            .build();
        context.assertElectedLeader(epoch, otherNodeId);

        context.client.poll();

        int shutdownTimeoutMs = 5000;
        CompletableFuture<Void> shutdownFuture = context.client.shutdown(shutdownTimeoutMs);
        assertTrue(context.client.isRunning());
        assertFalse(shutdownFuture.isDone());

        context.client.poll();
        assertFalse(context.client.isRunning());
        assertTrue(shutdownFuture.isDone());
        assertNull(shutdownFuture.get());
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testObserverGracefulShutdown(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        int voter1 = localId + 1;
        int voter2 = localId + 2;
        Set<Integer> voters = Set.of(voter1, voter2);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withUnknownLeader(5)
            .withKip853Rpc(withKip853Rpc)
            .build();
        context.client.poll();
        context.assertUnknownLeader(5);

        // Observer shutdown should complete immediately even if the
        // current leader is unknown
        CompletableFuture<Void> shutdownFuture = context.client.shutdown(5000);
        assertTrue(context.client.isRunning());
        assertFalse(shutdownFuture.isDone());

        context.client.poll();
        assertFalse(context.client.isRunning());
        assertTrue(shutdownFuture.isDone());
        assertNull(shutdownFuture.get());
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testGracefulShutdownSingleMemberQuorum(boolean withKip853Rpc) throws IOException {
        int localId = randomReplicaId();
        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, Collections.singleton(localId))
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.assertElectedLeader(1, localId);
        context.client.poll();
        assertEquals(0, context.channel.drainSendQueue().size());
        int shutdownTimeoutMs = 5000;
        context.client.shutdown(shutdownTimeoutMs);
        assertTrue(context.client.isRunning());
        context.client.poll();
        assertFalse(context.client.isRunning());
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testFollowerReplication(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        int otherNodeId = localId + 1;
        int epoch = 5;
        Set<Integer> voters = Set.of(localId, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withElectedLeader(epoch, otherNodeId)
            .withKip853Rpc(withKip853Rpc)
            .build();
        context.assertElectedLeader(epoch, otherNodeId);

        context.pollUntilRequest();

        RaftRequest.Outbound fetchQuorumRequest = context.assertSentFetchRequest(epoch, 0L, 0);
        Records records = context.buildBatch(0L, 3, Arrays.asList("a", "b"));
        FetchResponseData response = context.fetchResponse(epoch, otherNodeId, records, 0L, Errors.NONE);
        context.deliverResponse(
            fetchQuorumRequest.correlationId(),
            fetchQuorumRequest.destination(),
            response
        );

        context.client.poll();
        assertEquals(2L, context.log.endOffset().offset());
        assertEquals(2L, context.log.firstUnflushedOffset());
    }

    @ParameterizedTest
    @CsvSource({ "true, true", "true, false", "false, true", "false, false" })
    public void testObserverReplication(boolean withKip853Rpc, boolean alwaysFlush) throws Exception {
        int localId = randomReplicaId();
        int otherNodeId = localId + 1;
        int epoch = 5;
        Set<Integer> voters = Set.of(otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withElectedLeader(epoch, otherNodeId)
            .withKip853Rpc(withKip853Rpc)
            .withAlwaysFlush(alwaysFlush)
            .build();
        context.assertElectedLeader(epoch, otherNodeId);

        context.pollUntilRequest();

        RaftRequest.Outbound fetchQuorumRequest = context.assertSentFetchRequest(epoch, 0L, 0);
        Records records = context.buildBatch(0L, 3, Arrays.asList("a", "b"));
        FetchResponseData response = context.fetchResponse(epoch, otherNodeId, records, 0L, Errors.NONE);
        context.deliverResponse(
            fetchQuorumRequest.correlationId(),
            fetchQuorumRequest.destination(),
            response
        );

        context.client.poll();
        assertEquals(2L, context.log.endOffset().offset());
        long firstUnflushedOffset = alwaysFlush ? 2L : 0L;
        assertEquals(firstUnflushedOffset, context.log.firstUnflushedOffset());
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testEmptyRecordSetInFetchResponse(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        int otherNodeId = localId + 1;
        int epoch = 5;
        Set<Integer> voters = Set.of(localId, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withElectedLeader(epoch, otherNodeId)
            .withKip853Rpc(withKip853Rpc)
            .build();
        context.assertElectedLeader(epoch, otherNodeId);

        // Receive an empty fetch response
        context.pollUntilRequest();
        RaftRequest.Outbound fetchQuorumRequest = context.assertSentFetchRequest(epoch, 0L, 0);
        FetchResponseData fetchResponse = context.fetchResponse(
            epoch,
            otherNodeId,
            MemoryRecords.EMPTY,
            0L,
            Errors.NONE
        );
        context.deliverResponse(
            fetchQuorumRequest.correlationId(),
            fetchQuorumRequest.destination(),
            fetchResponse
        );
        context.client.poll();
        assertEquals(0L, context.log.endOffset().offset());
        assertEquals(OptionalLong.of(0L), context.client.highWatermark());

        // Receive some records in the next poll, but do not advance high watermark
        context.pollUntilRequest();
        Records records = context.buildBatch(0L, epoch, Arrays.asList("a", "b"));
        fetchQuorumRequest = context.assertSentFetchRequest(epoch, 0L, 0);
        fetchResponse = context.fetchResponse(epoch, otherNodeId, records, 0L, Errors.NONE);
        context.deliverResponse(
            fetchQuorumRequest.correlationId(),
            fetchQuorumRequest.destination(),
            fetchResponse
        );
        context.client.poll();
        assertEquals(2L, context.log.endOffset().offset());
        assertEquals(OptionalLong.of(0L), context.client.highWatermark());

        // The next fetch response is empty, but should still advance the high watermark
        context.pollUntilRequest();
        fetchQuorumRequest = context.assertSentFetchRequest(epoch, 2L, epoch);
        fetchResponse = context.fetchResponse(
            epoch,
            otherNodeId,
            MemoryRecords.EMPTY,
            2L,
            Errors.NONE
        );
        context.deliverResponse(
            fetchQuorumRequest.correlationId(),
            fetchQuorumRequest.destination(),
            fetchResponse
        );
        context.client.poll();
        assertEquals(2L, context.log.endOffset().offset());
        assertEquals(OptionalLong.of(2L), context.client.highWatermark());
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testFetchShouldBeTreatedAsLeaderAcknowledgement(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        ReplicaKey otherNodeKey = replicaKey(localId + 1, withKip853Rpc);
        int epoch = 5;
        Set<Integer> voters = Set.of(localId, otherNodeKey.id());

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .updateRandom(r -> r.mockNextInt(DEFAULT_ELECTION_TIMEOUT_MS, 0))
            .withUnknownLeader(epoch - 1)
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.time.sleep(context.electionTimeoutMs());
        context.expectAndGrantVotes(epoch);

        context.pollUntilRequest();

        // We send BeginEpoch, but it gets lost and the destination finds the leader through the Fetch API
        context.assertSentBeginQuorumEpochRequest(epoch, Set.of(otherNodeKey.id()));

        context.deliverRequest(context.fetchRequest(epoch, otherNodeKey, 0L, 0, 500));

        context.client.poll();

        // The BeginEpoch request eventually times out. We should not send another one.
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(localId));
        context.time.sleep(context.requestTimeoutMs());

        context.client.poll();

        List<RaftRequest.Outbound> sentMessages = context.channel.drainSendQueue();
        assertEquals(0, sentMessages.size());
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testLeaderAppendSingleMemberQuorum(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        Set<Integer> voters = Collections.singleton(localId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withKip853Rpc(withKip853Rpc)
            .build();
        long now = context.time.milliseconds();

        context.pollUntil(() -> context.log.endOffset().offset() == 1L);
        context.assertElectedLeader(1, localId);

        // We still write the leader change message
        assertEquals(OptionalLong.of(1L), context.client.highWatermark());

        String[] appendRecords = new String[]{"a", "b", "c"};

        // First poll has no high watermark advance
        context.client.poll();
        assertEquals(OptionalLong.of(1L), context.client.highWatermark());

        context.client.prepareAppend(context.currentEpoch(), Arrays.asList(appendRecords));
        context.client.schedulePreparedAppend();

        // Then poll the appended data with leader change record
        context.client.poll();
        assertEquals(OptionalLong.of(4L), context.client.highWatermark());

        // Now try reading it
        ReplicaKey otherNodeKey = replicaKey(localId + 1, withKip853Rpc);
        List<MutableRecordBatch> batches = new ArrayList<>(2);
        boolean appended = true;

        // Continue to fetch until the leader returns an empty response
        while (appended) {
            long fetchOffset = 0;
            int lastFetchedEpoch = 0;
            if (!batches.isEmpty()) {
                MutableRecordBatch lastBatch = batches.get(batches.size() - 1);
                fetchOffset = lastBatch.lastOffset() + 1;
                lastFetchedEpoch = lastBatch.partitionLeaderEpoch();
            }

            context.deliverRequest(context.fetchRequest(1, otherNodeKey, fetchOffset, lastFetchedEpoch, 0));
            context.pollUntilResponse();

            MemoryRecords fetchedRecords = context.assertSentFetchPartitionResponse(Errors.NONE, 1, OptionalInt.of(localId));
            List<MutableRecordBatch> fetchedBatch = Utils.toList(fetchedRecords.batchIterator());
            batches.addAll(fetchedBatch);

            appended = !fetchedBatch.isEmpty();
        }

        assertEquals(2, batches.size());

        MutableRecordBatch leaderChangeBatch = batches.get(0);
        assertTrue(leaderChangeBatch.isControlBatch());
        List<Record> readRecords = Utils.toList(leaderChangeBatch.iterator());
        assertEquals(1, readRecords.size());

        Record record = readRecords.get(0);
        assertEquals(now, record.timestamp());
        RaftClientTestContext.verifyLeaderChangeMessage(localId, Collections.singletonList(localId),
            Collections.singletonList(localId), record.key(), record.value());

        MutableRecordBatch batch = batches.get(1);
        assertEquals(1, batch.partitionLeaderEpoch());
        readRecords = Utils.toList(batch.iterator());
        assertEquals(3, readRecords.size());

        for (int i = 0; i < appendRecords.length; i++) {
            assertEquals(appendRecords[i], Utils.utf8(readRecords.get(i).value()));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testFollowerLogReconciliation(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        int otherNodeId = localId + 1;
        int epoch = 5;
        int lastEpoch = 3;
        Set<Integer> voters = Set.of(localId, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withElectedLeader(epoch, otherNodeId)
            .appendToLog(lastEpoch, Arrays.asList("foo", "bar"))
            .appendToLog(lastEpoch, singletonList("baz"))
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.assertElectedLeader(epoch, otherNodeId);
        assertEquals(3L, context.log.endOffset().offset());

        context.pollUntilRequest();

        RaftRequest.Outbound request = context.assertSentFetchRequest(epoch, 3L, lastEpoch);

        FetchResponseData response = context.divergingFetchResponse(epoch, otherNodeId, 2L,
            lastEpoch, 1L);
        context.deliverResponse(request.correlationId(), request.destination(), response);

        // Poll again to complete truncation
        context.client.poll();
        assertEquals(2L, context.log.endOffset().offset());
        assertEquals(2L, context.log.firstUnflushedOffset());

        // Now we should be fetching
        context.client.poll();
        context.assertSentFetchRequest(epoch, 2L, lastEpoch);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testMetrics(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        int epoch = 1;
        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, Collections.singleton(localId))
            .withKip853Rpc(withKip853Rpc)
            .build();
        context.pollUntil(() -> context.log.endOffset().offset() == 1L);

        assertNotNull(getMetric(context.metrics, "current-state"));
        assertNotNull(getMetric(context.metrics, "current-leader"));
        assertNotNull(getMetric(context.metrics, "current-vote"));
        assertNotNull(getMetric(context.metrics, "current-epoch"));
        assertNotNull(getMetric(context.metrics, "high-watermark"));
        assertNotNull(getMetric(context.metrics, "log-end-offset"));
        assertNotNull(getMetric(context.metrics, "log-end-epoch"));
        assertNotNull(getMetric(context.metrics, "number-unknown-voter-connections"));
        assertNotNull(getMetric(context.metrics, "poll-idle-ratio-avg"));
        assertNotNull(getMetric(context.metrics, "commit-latency-avg"));
        assertNotNull(getMetric(context.metrics, "commit-latency-max"));
        assertNotNull(getMetric(context.metrics, "election-latency-avg"));
        assertNotNull(getMetric(context.metrics, "election-latency-max"));
        assertNotNull(getMetric(context.metrics, "fetch-records-rate"));
        assertNotNull(getMetric(context.metrics, "append-records-rate"));

        assertEquals("leader", getMetric(context.metrics, "current-state").metricValue());
        assertEquals((double) localId, getMetric(context.metrics, "current-leader").metricValue());
        assertEquals((double) localId, getMetric(context.metrics, "current-vote").metricValue());
        assertEquals((double) epoch, getMetric(context.metrics, "current-epoch").metricValue());
        assertEquals((double) 1L, getMetric(context.metrics, "high-watermark").metricValue());
        assertEquals((double) 1L, getMetric(context.metrics, "log-end-offset").metricValue());
        assertEquals((double) epoch, getMetric(context.metrics, "log-end-epoch").metricValue());

        context.client.prepareAppend(epoch, Arrays.asList("a", "b", "c"));
        context.client.schedulePreparedAppend();
        context.client.poll();

        assertEquals((double) 4L, getMetric(context.metrics, "high-watermark").metricValue());
        assertEquals((double) 4L, getMetric(context.metrics, "log-end-offset").metricValue());
        assertEquals((double) epoch, getMetric(context.metrics, "log-end-epoch").metricValue());

        context.client.close();

        // should only have total-metrics-count left
        assertEquals(1, context.metrics.metrics().size());
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testClusterAuthorizationFailedInFetch(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        int otherNodeId = localId + 1;
        int epoch = 5;
        Set<Integer> voters = Set.of(localId, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withKip853Rpc(withKip853Rpc)
            .withElectedLeader(epoch, otherNodeId)
            .build();

        context.assertElectedLeader(epoch, otherNodeId);

        context.pollUntilRequest();

        RaftRequest.Outbound request = context.assertSentFetchRequest(epoch, 0, 0);
        FetchResponseData response = new FetchResponseData()
            .setErrorCode(Errors.CLUSTER_AUTHORIZATION_FAILED.code());
        context.deliverResponse(
            request.correlationId(),
            request.destination(),
            response
        );
        assertThrows(ClusterAuthorizationException.class, context.client::poll);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testClusterAuthorizationFailedInBeginQuorumEpoch(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        int otherNodeId = localId + 1;
        int epoch = 5;
        Set<Integer> voters = Set.of(localId, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .updateRandom(r -> r.mockNextInt(DEFAULT_ELECTION_TIMEOUT_MS, 0))
            .withUnknownLeader(epoch - 1)
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.time.sleep(context.electionTimeoutMs());
        context.expectAndGrantVotes(epoch);

        context.pollUntilRequest();
        List<RaftRequest.Outbound> requests = context.collectBeginEpochRequests(epoch);
        assertEquals(1, requests.size());
        RaftRequest.Outbound request = requests.get(0);
        assertEquals(otherNodeId, request.destination().id());
        BeginQuorumEpochResponseData response = new BeginQuorumEpochResponseData()
            .setErrorCode(Errors.CLUSTER_AUTHORIZATION_FAILED.code());

        context.deliverResponse(request.correlationId(), request.destination(), response);
        assertThrows(ClusterAuthorizationException.class, context.client::poll);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testClusterAuthorizationFailedInVote(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        int otherNodeId = localId + 1;
        int epoch = 5;
        Set<Integer> voters = Set.of(localId, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withUnknownLeader(epoch - 1)
            .withKip853Rpc(withKip853Rpc)
            .build();

        // Sleep a little to ensure that we become a candidate
        context.time.sleep(context.electionTimeoutMs() * 2L);
        context.pollUntilRequest();
        context.assertVotedCandidate(epoch, localId);

        RaftRequest.Outbound request = context.assertSentVoteRequest(epoch, 0, 0L, 1);
        VoteResponseData response = new VoteResponseData()
            .setErrorCode(Errors.CLUSTER_AUTHORIZATION_FAILED.code());

        context.deliverResponse(request.correlationId(), request.destination(), response);
        assertThrows(ClusterAuthorizationException.class, context.client::poll);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testClusterAuthorizationFailedInEndQuorumEpoch(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        int otherNodeId = localId + 1;
        Set<Integer> voters = Set.of(localId, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withUnknownLeader(1)
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.becomeLeader();
        int epoch = context.currentEpoch();

        context.client.shutdown(5000);
        context.pollUntilRequest();

        RaftRequest.Outbound request = context.assertSentEndQuorumEpochRequest(epoch, otherNodeId);
        EndQuorumEpochResponseData response = new EndQuorumEpochResponseData()
            .setErrorCode(Errors.CLUSTER_AUTHORIZATION_FAILED.code());

        context.deliverResponse(request.correlationId(), request.destination(), response);
        assertThrows(ClusterAuthorizationException.class, context.client::poll);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testHandleLeaderChangeFiresAfterListenerReachesEpochStartOffsetOnEmptyLog(
        boolean withKip853Rpc
    ) throws Exception {
        int localId = randomReplicaId();
        ReplicaKey otherNodeKey = replicaKey(localId + 1, withKip853Rpc);
        Set<Integer> voters = Set.of(localId, otherNodeKey.id());

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.becomeLeader();
        context.client.poll();
        int epoch = context.currentEpoch();

        // After becoming leader, we expect the `LeaderChange` record to be appended.
        assertEquals(1L, context.log.endOffset().offset());

        // The high watermark is not known to the leader until the followers
        // begin fetching, so we should not have fired the `handleLeaderChange` callback.
        assertEquals(OptionalInt.empty(), context.listener.currentClaimedEpoch());
        assertEquals(OptionalLong.empty(), context.listener.lastCommitOffset());

        // Deliver a fetch from the other voter. The high watermark will not
        // be exposed until it is able to reach the start of the leader epoch,
        // so we are unable to deliver committed data or fire `handleLeaderChange`.
        context.deliverRequest(context.fetchRequest(epoch, otherNodeKey, 0L, 0, 0));
        context.client.poll();
        assertEquals(OptionalInt.empty(), context.listener.currentClaimedEpoch());
        assertEquals(OptionalLong.empty(), context.listener.lastCommitOffset());

        // Now catch up to the start of the leader epoch so that the high
        // watermark advances and we can start sending committed data to the
        // listener. Note that the `LeaderChange` control record is included
        // in the committed batches.
        context.deliverRequest(context.fetchRequest(epoch, otherNodeKey, 1L, epoch, 0));
        context.client.poll();
        assertEquals(OptionalLong.of(0), context.listener.lastCommitOffset());

        // Poll again now that the listener has caught up to the HWM
        context.client.poll();
        assertEquals(OptionalInt.of(epoch), context.listener.currentClaimedEpoch());
        assertEquals(0, context.listener.claimedEpochStartOffset(epoch));
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testHandleLeaderChangeFiresAfterListenerReachesEpochStartOffset(
        boolean withKip853Rpc
    ) throws Exception {
        int localId = randomReplicaId();
        ReplicaKey otherNodeKey = replicaKey(localId + 1, withKip853Rpc);
        int epoch = 5;
        Set<Integer> voters = Set.of(localId, otherNodeKey.id());

        List<String> batch1 = Arrays.asList("1", "2", "3");
        List<String> batch2 = Arrays.asList("4", "5", "6");
        List<String> batch3 = Arrays.asList("7", "8", "9");

        List<List<String>> expectedBatches = Arrays.asList(batch1, batch2, batch3);
        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .appendToLog(1, batch1)
            .appendToLog(1, batch2)
            .appendToLog(2, batch3)
            .withUnknownLeader(epoch - 1)
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.becomeLeader();
        context.client.poll();

        // After becoming leader, we expect the `LeaderChange` record to be appended
        // in addition to the initial 9 records in the log.
        assertEquals(10L, context.log.endOffset().offset());

        // The high watermark is not known to the leader until the followers
        // begin fetching, so we should not have fired the `handleLeaderChange` callback.
        assertEquals(OptionalInt.empty(), context.listener.currentClaimedEpoch());
        assertEquals(OptionalLong.empty(), context.listener.lastCommitOffset());

        // Deliver a fetch from the other voter. The high watermark will not
        // be exposed until it is able to reach the start of the leader epoch,
        // so we are unable to deliver committed data or fire `handleLeaderChange`.
        context.deliverRequest(context.fetchRequest(epoch, otherNodeKey, 3L, 1, 500));
        context.client.poll();
        assertEquals(OptionalInt.empty(), context.listener.currentClaimedEpoch());
        assertEquals(OptionalLong.empty(), context.listener.lastCommitOffset());

        // Now catch up to the start of the leader epoch so that the high
        // watermark advances and we can start sending committed data to the
        // listener. Note that the `LeaderChange` control record is included
        // in the committed batches.
        context.deliverRequest(context.fetchRequest(epoch, otherNodeKey, 10L, epoch, 500));
        context.pollUntil(() -> {
            int index = 0;
            for (Batch<String> batch : context.listener.committedBatches()) {
                if (index < expectedBatches.size()) {
                    // It must be a data record so compare it
                    assertEquals(expectedBatches.get(index), batch.records());
                }
                index++;
            }
            // The control record must be the last batch committed
            assertEquals(4, index);

            return context.listener.currentClaimedEpoch().isPresent();
        });

        assertEquals(OptionalInt.of(epoch), context.listener.currentClaimedEpoch());
        // Note that last committed offset is inclusive and must include the leader change record.
        assertEquals(OptionalLong.of(9), context.listener.lastCommitOffset());
        assertEquals(9, context.listener.claimedEpochStartOffset(epoch));
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testLateRegisteredListenerCatchesUp(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        ReplicaKey otherNodeKey = replicaKey(localId + 1, withKip853Rpc);
        int epoch = 5;
        Set<Integer> voters = Set.of(localId, otherNodeKey.id());

        List<String> batch1 = Arrays.asList("1", "2", "3");
        List<String> batch2 = Arrays.asList("4", "5", "6");
        List<String> batch3 = Arrays.asList("7", "8", "9");

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .appendToLog(1, batch1)
            .appendToLog(1, batch2)
            .appendToLog(2, batch3)
            .withUnknownLeader(epoch - 1)
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.becomeLeader();
        context.client.poll();
        assertEquals(10L, context.log.endOffset().offset());

        // Let the initial listener catch up
        context.deliverRequest(context.fetchRequest(epoch, otherNodeKey, 10L, epoch, 0));
        context.pollUntil(() -> OptionalInt.of(epoch).equals(context.listener.currentClaimedEpoch()));
        assertEquals(OptionalLong.of(10L), context.client.highWatermark());
        assertEquals(OptionalLong.of(9L), context.listener.lastCommitOffset());
        assertEquals(OptionalInt.of(epoch), context.listener.currentClaimedEpoch());
        // Ensure that the `handleLeaderChange` callback was not fired early
        assertEquals(9L, context.listener.claimedEpochStartOffset(epoch));

        // Register a second listener and allow it to catch up to the high watermark
        RaftClientTestContext.MockListener secondListener = new RaftClientTestContext.MockListener(OptionalInt.of(localId));
        context.client.register(secondListener);
        context.pollUntil(() -> OptionalInt.of(epoch).equals(secondListener.currentClaimedEpoch()));
        assertEquals(OptionalLong.of(9L), secondListener.lastCommitOffset());
        assertEquals(OptionalInt.of(epoch), context.listener.currentClaimedEpoch());
        // Ensure that the `handleLeaderChange` callback was not fired early
        assertEquals(9L, secondListener.claimedEpochStartOffset(epoch));
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testReregistrationChangesListenerContext(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        int otherNodeId = localId + 1;
        int epoch = 5;
        Set<Integer> voters = Set.of(localId, otherNodeId);

        List<String> batch1 = Arrays.asList("1", "2", "3");
        List<String> batch2 = Arrays.asList("4", "5", "6");
        List<String> batch3 = Arrays.asList("7", "8", "9");

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .appendToLog(1, batch1)
            .appendToLog(1, batch2)
            .appendToLog(2, batch3)
            .withUnknownLeader(epoch - 1)
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.becomeLeader();
        context.client.poll();
        assertEquals(10L, context.log.endOffset().offset());

        // Let the initial listener catch up
        context.advanceLocalLeaderHighWatermarkToLogEndOffset();
        context.pollUntil(() -> OptionalLong.of(9).equals(context.listener.lastCommitOffset()));

        // Register a second listener
        RaftClientTestContext.MockListener secondListener = new RaftClientTestContext.MockListener(OptionalInt.of(localId));
        context.client.register(secondListener);
        context.pollUntil(() -> OptionalLong.of(9).equals(secondListener.lastCommitOffset()));
        context.client.unregister(secondListener);

        // Write to the log and show that the default listener gets updated...
        assertEquals(10L, context.client.prepareAppend(epoch, singletonList("a")));
        context.client.schedulePreparedAppend();
        context.client.poll();
        context.advanceLocalLeaderHighWatermarkToLogEndOffset();
        context.pollUntil(() -> OptionalLong.of(10).equals(context.listener.lastCommitOffset()));
        // ... but unregister listener doesn't
        assertEquals(OptionalLong.of(9), secondListener.lastCommitOffset());
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testHandleCommitCallbackFiresAfterFollowerHighWatermarkAdvances(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        int otherNodeId = localId + 1;
        int epoch = 5;
        Set<Integer> voters = Set.of(localId, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withElectedLeader(epoch, otherNodeId)
            .withKip853Rpc(withKip853Rpc)
            .build();
        assertEquals(OptionalLong.empty(), context.client.highWatermark());

        // Poll for our first fetch request
        context.pollUntilRequest();
        RaftRequest.Outbound fetchRequest = context.assertSentFetchRequest();
        assertTrue(voters.contains(fetchRequest.destination().id()));
        context.assertFetchRequestData(fetchRequest, epoch, 0L, 0);

        // The response does not advance the high watermark
        List<String> records1 = Arrays.asList("a", "b", "c");
        MemoryRecords batch1 = context.buildBatch(0L, 3, records1);
        context.deliverResponse(
            fetchRequest.correlationId(),
            fetchRequest.destination(),
            context.fetchResponse(epoch, otherNodeId, batch1, 0L, Errors.NONE)
        );
        context.client.poll();

        // The listener should not have seen any data
        assertEquals(OptionalLong.of(0L), context.client.highWatermark());
        assertEquals(0, context.listener.numCommittedBatches());
        assertEquals(OptionalInt.empty(), context.listener.currentClaimedEpoch());

        // Now look for the next fetch request
        context.pollUntilRequest();
        fetchRequest = context.assertSentFetchRequest();
        assertTrue(voters.contains(fetchRequest.destination().id()));
        context.assertFetchRequestData(fetchRequest, epoch, 3L, 3);

        // The high watermark advances to include the first batch we fetched
        List<String> records2 = Arrays.asList("d", "e", "f");
        MemoryRecords batch2 = context.buildBatch(3L, 3, records2);
        context.deliverResponse(
            fetchRequest.correlationId(),
            fetchRequest.destination(),
            context.fetchResponse(epoch, otherNodeId, batch2, 3L, Errors.NONE)
        );
        context.client.poll();

        // The listener should have seen only the data from the first batch
        assertEquals(OptionalLong.of(3L), context.client.highWatermark());
        assertEquals(1, context.listener.numCommittedBatches());
        assertEquals(OptionalLong.of(2L), context.listener.lastCommitOffset());
        assertEquals(records1, context.listener.lastCommit().records());
        assertEquals(OptionalInt.empty(), context.listener.currentClaimedEpoch());
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testHandleCommitCallbackFiresInVotedState(boolean withKip853Rpc) throws Exception {
        // This test verifies that the state machine can still catch up even while
        // an election is in progress as long as the high watermark is known.
        int localId = randomReplicaId();
        ReplicaKey otherNodeKey = replicaKey(localId + 1, withKip853Rpc);
        int epoch = 7;
        Set<Integer> voters = Set.of(localId, otherNodeKey.id());

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .appendToLog(2, Arrays.asList("a", "b", "c"))
            .appendToLog(4, Arrays.asList("d", "e", "f"))
            .appendToLog(4, Arrays.asList("g", "h", "i"))
            .withUnknownLeader(epoch - 1)
            .withKip853Rpc(withKip853Rpc)
            .build();

        // Start off as the leader and receive a fetch to initialize the high watermark
        context.becomeLeader();
        context.deliverRequest(context.fetchRequest(epoch, otherNodeKey, 10L, epoch, 500));
        context.client.poll();
        assertEquals(OptionalLong.of(10L), context.client.highWatermark());

        // Now we receive a vote request which transitions us to the 'voted' state
        int candidateEpoch = epoch + 1;
        context.deliverRequest(context.voteRequest(candidateEpoch, otherNodeKey, epoch, 10L));
        context.pollUntilResponse();
        context.assertVotedCandidate(candidateEpoch, otherNodeKey.id());
        assertEquals(OptionalLong.of(10L), context.client.highWatermark());

        // Register another listener and verify that it catches up while we remain 'voted'
        RaftClientTestContext.MockListener secondListener = new RaftClientTestContext.MockListener(
            OptionalInt.of(localId)
        );
        context.client.register(secondListener);
        context.client.poll();
        context.assertVotedCandidate(candidateEpoch, otherNodeKey.id());

        // Note the offset is 9 because from offsets 0 to 8 there are data records,
        // at offset 9 there is a control record and the raft client sends control record to the
        // raft listener
        context.pollUntil(() -> secondListener.lastCommitOffset().equals(OptionalLong.of(9L)));
        assertEquals(OptionalLong.of(9L), secondListener.lastCommitOffset());
        assertEquals(OptionalInt.empty(), secondListener.currentClaimedEpoch());
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testHandleCommitCallbackFiresInCandidateState(boolean withKip853Rpc) throws Exception {
        // This test verifies that the state machine can still catch up even while
        // an election is in progress as long as the high watermark is known.
        int localId = randomReplicaId();
        ReplicaKey otherNodeKey = replicaKey(localId + 1, withKip853Rpc);
        int epoch = 7;
        Set<Integer> voters = Set.of(localId, otherNodeKey.id());

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .appendToLog(2, Arrays.asList("a", "b", "c"))
            .appendToLog(4, Arrays.asList("d", "e", "f"))
            .appendToLog(4, Arrays.asList("g", "h", "i"))
            .withUnknownLeader(epoch - 1)
            .withKip853Rpc(withKip853Rpc)
            .build();

        // Start off as the leader and receive a fetch to initialize the high watermark
        context.becomeLeader();
        assertEquals(10L, context.log.endOffset().offset());

        context.deliverRequest(context.fetchRequest(epoch, otherNodeKey, 10L, epoch, 0));
        context.pollUntilResponse();
        assertEquals(OptionalLong.of(10L), context.client.highWatermark());
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(localId));

        // Now we receive a vote request which transitions us to the 'unattached' state
        context.deliverRequest(context.voteRequest(epoch + 1, otherNodeKey, epoch, 9L));
        context.pollUntilResponse();
        context.assertUnknownLeader(epoch + 1);
        assertEquals(OptionalLong.of(10L), context.client.highWatermark());

        // Timeout the election and become candidate
        int candidateEpoch = epoch + 2;
        context.time.sleep(context.electionTimeoutMs() * 2L);
        context.client.poll();
        context.assertVotedCandidate(candidateEpoch, localId);

        // Register another listener and verify that it catches up
        RaftClientTestContext.MockListener secondListener = new RaftClientTestContext.MockListener(
            OptionalInt.of(localId)
        );
        context.client.register(secondListener);
        context.client.poll();
        context.assertVotedCandidate(candidateEpoch, localId);

        // Note the offset is 9 because from offsets 0 to 8 there are data records,
        // at offset 9 there is a control record and the raft client sends control record to the
        // raft listener
        context.pollUntil(() -> secondListener.lastCommitOffset().equals(OptionalLong.of(9L)));
        assertEquals(OptionalLong.of(9L), secondListener.lastCommitOffset());
        assertEquals(OptionalInt.empty(), secondListener.currentClaimedEpoch());
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testHandleLeaderChangeFiresAfterUnattachedRegistration(
        boolean withKip853Rpc
    ) throws Exception {
        // When registering a listener while the replica is unattached, it should get notified
        // with the current epoch
        // When transitioning to follower, expect another notification with the leader and epoch
        int localId = randomReplicaId();
        int otherNodeId = localId + 1;
        int epoch = 7;
        Set<Integer> voters = Set.of(localId, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withUnknownLeader(epoch)
            .withKip853Rpc(withKip853Rpc)
            .build();

        // Register another listener and verify that it is notified of latest epoch
        RaftClientTestContext.MockListener secondListener = new RaftClientTestContext.MockListener(
            OptionalInt.of(localId)
        );
        context.client.register(secondListener);
        context.client.poll();

        // Expected leader change notification
        LeaderAndEpoch expectedLeaderAndEpoch = new LeaderAndEpoch(OptionalInt.empty(), epoch);
        assertEquals(expectedLeaderAndEpoch, secondListener.currentLeaderAndEpoch());

        // Transition to follower and then expect a leader changed notification
        context.deliverRequest(context.beginEpochRequest(epoch, otherNodeId));
        context.pollUntilResponse();

        // Expected leader change notification
        expectedLeaderAndEpoch = new LeaderAndEpoch(OptionalInt.of(otherNodeId), epoch);
        assertEquals(expectedLeaderAndEpoch, secondListener.currentLeaderAndEpoch());
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testHandleLeaderChangeFiresAfterFollowerRegistration(boolean withKip853Rpc) throws Exception {
        // When registering a listener while the replica is a follower, it should get notified with
        // the current leader and epoch
        int localId = randomReplicaId();
        int otherNodeId = localId + 1;
        int epoch = 7;
        Set<Integer> voters = Set.of(localId, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withElectedLeader(epoch, otherNodeId)
            .withKip853Rpc(withKip853Rpc)
            .build();

        // Register another listener and verify that it is notified of latest leader and epoch
        RaftClientTestContext.MockListener secondListener = new RaftClientTestContext.MockListener(
            OptionalInt.of(localId)
        );
        context.client.register(secondListener);
        context.client.poll();

        LeaderAndEpoch expectedLeaderAndEpoch = new LeaderAndEpoch(OptionalInt.of(otherNodeId), epoch);
        assertEquals(expectedLeaderAndEpoch, secondListener.currentLeaderAndEpoch());
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testHandleLeaderChangeFiresAfterResignRegistration(boolean withKip853Rpc) throws Exception {
        // When registering a listener while the replica is resigned, it should not get notified with
        // the current leader and epoch
        int localId = randomReplicaId();
        int otherNodeId = localId + 1;
        int epoch = 7;
        Set<Integer> voters = Set.of(localId, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withElectedLeader(epoch, localId)
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.client.poll();
        assertTrue(context.client.quorum().isResigned());
        assertEquals(LeaderAndEpoch.UNKNOWN, context.listener.currentLeaderAndEpoch());

        // Register another listener and verify that it is not notified of latest leader and epoch
        RaftClientTestContext.MockListener secondListener = new RaftClientTestContext.MockListener(
            OptionalInt.of(localId)
        );
        context.client.register(secondListener);
        context.client.poll();

        assertTrue(context.client.quorum().isResigned());
        assertEquals(LeaderAndEpoch.UNKNOWN, secondListener.currentLeaderAndEpoch());
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testObserverFetchWithNoLocalId(boolean withKip853Rpc) throws Exception {
        // When no `localId` is defined, the client will behave as an observer.
        // This is designed for tooling/debugging use cases.
        int leaderId = randomReplicaId();
        Set<Integer> voters = Set.of(leaderId, leaderId + 1);
        List<InetSocketAddress> bootstrapServers = voters
            .stream()
            .map(RaftClientTestContext::mockAddress)
            .collect(Collectors.toList());

        RaftClientTestContext context = new RaftClientTestContext.Builder(OptionalInt.empty(), voters)
            .withBootstrapServers(Optional.of(bootstrapServers))
            .withKip853Rpc(withKip853Rpc)
            .build();

        // First fetch discovers the current leader and epoch

        context.pollUntilRequest();
        RaftRequest.Outbound fetchRequest1 = context.assertSentFetchRequest();
        assertTrue(context.bootstrapIds.contains(fetchRequest1.destination().id()));
        context.assertFetchRequestData(fetchRequest1, 0, 0L, 0);

        int leaderEpoch = 5;

        context.deliverResponse(
            fetchRequest1.correlationId(),
            fetchRequest1.destination(),
            context.fetchResponse(5, leaderId, MemoryRecords.EMPTY, 0L, Errors.FENCED_LEADER_EPOCH)
        );
        context.client.poll();
        context.assertElectedLeader(leaderEpoch, leaderId);

        // Second fetch goes to the discovered leader

        context.pollUntilRequest();
        RaftRequest.Outbound fetchRequest2 = context.assertSentFetchRequest();
        assertEquals(leaderId, fetchRequest2.destination().id());
        context.assertFetchRequestData(fetchRequest2, leaderEpoch, 0L, 0);

        List<String> records = Arrays.asList("a", "b", "c");
        MemoryRecords batch1 = context.buildBatch(0L, 3, records);
        context.deliverResponse(
            fetchRequest2.correlationId(),
            fetchRequest2.destination(),
            context.fetchResponse(leaderEpoch, leaderId, batch1, 0L, Errors.NONE)
        );
        context.client.poll();
        assertEquals(3L, context.log.endOffset().offset());
        assertEquals(3, context.log.lastFetchedEpoch());
    }

    private static KafkaMetric getMetric(final Metrics metrics, final String name) {
        return metrics.metrics().get(metrics.metricName(name, "raft-metrics"));
    }

    static ReplicaKey replicaKey(int id, boolean withDirectoryId) {
        Uuid directoryId = withDirectoryId ? Uuid.randomUuid() : ReplicaKey.NO_DIRECTORY_ID;
        return ReplicaKey.of(id, directoryId);
    }

    static int randomReplicaId() {
        return ThreadLocalRandom.current().nextInt(1025);
    }
}
