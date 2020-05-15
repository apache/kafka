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

import org.apache.kafka.common.errors.NotLeaderForPartitionException;
import org.apache.kafka.common.message.BeginQuorumEpochRequestData;
import org.apache.kafka.common.message.BeginQuorumEpochResponseData;
import org.apache.kafka.common.message.EndQuorumEpochRequestData;
import org.apache.kafka.common.message.EndQuorumEpochResponseData;
import org.apache.kafka.common.message.FetchQuorumRecordsRequestData;
import org.apache.kafka.common.message.FetchQuorumRecordsResponseData;
import org.apache.kafka.common.message.FindQuorumRequestData;
import org.apache.kafka.common.message.FindQuorumResponseData;
import org.apache.kafka.common.message.LeaderChangeMessage;
import org.apache.kafka.common.message.LeaderChangeMessage.Voter;
import org.apache.kafka.common.message.VoteRequestData;
import org.apache.kafka.common.message.VoteResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.ControlRecordType;
import org.apache.kafka.common.record.ControlRecordUtils;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestUtils;

import org.junit.After;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class KafkaRaftClientTest {
    private final int localId = 0;
    private final int electionTimeoutMs = 10000;
    private final int retryBackoffMs = 50;
    private final int requestTimeoutMs = 5000;
    private final int electionJitterMs = 100;
    private final MockTime time = new MockTime();
    private final MockLog log = new MockLog();
    private final MockNetworkChannel channel = new MockNetworkChannel();
    private final Random random = Mockito.spy(new Random());

    private final NoOpStateMachine stateMachine = new NoOpStateMachine();

    private QuorumStateStore quorumStateStore = new MockQuorumStateStore();

    @After
    public void cleanUp() throws IOException {
        quorumStateStore.clear();
    }

    private InetSocketAddress mockAddress(int id) {
        return new InetSocketAddress("localhost", 9990 + id);
    }

    private KafkaRaftClient buildClient(Set<Integer> voters) throws IOException {
        LogContext logContext = new LogContext();
        QuorumState quorum = new QuorumState(localId, voters, quorumStateStore, logContext);

        List<InetSocketAddress> bootstrapServers = voters.stream()
            .map(this::mockAddress)
            .collect(Collectors.toList());

        KafkaRaftClient client = new KafkaRaftClient(channel, log, quorum, time,
            mockAddress(localId), bootstrapServers, electionTimeoutMs, electionJitterMs,
            retryBackoffMs, requestTimeoutMs, logContext, random);
        client.initialize(stateMachine);

        return client;
    }

    @Test
    public void testInitializeSingleMemberQuorum() throws IOException {
        KafkaRaftClient client = buildClient(Collections.singleton(localId));
        assertEquals(ElectionState.withElectedLeader(1, localId), quorumStateStore.readElectionState());
        client.poll();
        assertEquals(0, channel.drainSendQueue().size());
    }

    @Test
    public void testInitializeAsLeaderFromStateStore() throws IOException {
        Set<Integer> voters = Utils.mkSet(localId, 1);
        int epoch = 2;
        quorumStateStore.writeElectionState(ElectionState.withElectedLeader(epoch, localId));
        KafkaRaftClient client = buildClient(voters);
        assertTrue(stateMachine.isLeader());
        assertEquals(epoch, stateMachine.epoch());

        assertEquals(1L, log.endOffset());

        // Should have sent out connection info query for other node id.
        client.poll();

        assertSentFindQuorumRequest();
    }

    @Test
    public void testInitializeAsCandidateFromStateStore() throws IOException {
        // Need 3 node to require a 2-node majority
        Set<Integer> voters = Utils.mkSet(localId, 1, 2);
        quorumStateStore.writeElectionState(ElectionState.withVotedCandidate(2, localId));
        KafkaRaftClient client = buildClient(voters);
        assertFalse(stateMachine.isLeader());
        assertEquals(0L, log.endOffset());

        // Should have sent out connection info query for other node id.
        client.poll();

        int findQuorumCorrelationId = assertSentFindQuorumRequest();
        channel.mockReceive(new RaftResponse.Inbound(findQuorumCorrelationId,
            findQuorumResponse(OptionalInt.empty(), 1, voters), -1));

        // Consume findQuorum response.
        client.poll();

        assertTrue(channel.drainSendQueue().isEmpty());

        // Send out vote requests.
        client.poll();

        List<RaftRequest.Outbound> voteRequests = collectVoteRequests(2, 0, 0);
        assertEquals(2, voteRequests.size());
    }

    @Test
    public void testInitializeAsCandidateAndBecomeLeader() throws Exception {
        long now = time.milliseconds();
        final int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        KafkaRaftClient client = buildClient(voters);
        assertEquals(ElectionState.withVotedCandidate(1, localId), quorumStateStore.readElectionState());

        pollUntilSend(client);

        int findQuorumCorrelationId = assertSentFindQuorumRequest();
        channel.mockReceive(new RaftResponse.Inbound(findQuorumCorrelationId,
            findQuorumResponse(OptionalInt.empty(), 1, voters), -1));

        pollUntilSend(client);

        int correlationId = assertSentVoteRequest(1, 0, 0L);
        VoteResponseData voteResponse = voteResponse(true, Optional.empty(), 1);
        channel.mockReceive(new RaftResponse.Inbound(correlationId, voteResponse, otherNodeId));

        // Become leader after receiving the vote
        client.poll();
        assertEquals(ElectionState.withElectedLeader(1, localId), quorumStateStore.readElectionState());

        // Leader change record appended
        assertEquals(1, log.endOffset());

        // Send BeginQuorumEpoch to voters
        client.poll();
        assertBeginQuorumEpochRequest(1);

        Records records = log.read(0, OptionalLong.of(1));
        RecordBatch batch = records.batches().iterator().next();
        assertTrue(batch.isControlBatch());

        Record record = batch.iterator().next();
        assertEquals(now, record.timestamp());
        verifyLeaderChangeMessage(localId, Collections.singletonList(otherNodeId),
            record.key(), record.value());
    }

    @Test
    public void testHandleBeginQuorumRequest() throws Exception {
        int otherNodeId = 1;
        int votedCandidateEpoch = 2;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);

        quorumStateStore.writeElectionState(ElectionState.withVotedCandidate(
            votedCandidateEpoch, otherNodeId));

        KafkaRaftClient client = buildClient(voters);

        BeginQuorumEpochRequestData beginQuorumEpochRequest = new BeginQuorumEpochRequestData()
                                          .setLeaderId(otherNodeId)
                                          .setLeaderEpoch(votedCandidateEpoch);
        channel.mockReceive(new RaftRequest.Inbound(channel.newCorrelationId(),
            beginQuorumEpochRequest, time.milliseconds()));

        client.poll();

        assertEquals(ElectionState.withElectedLeader(
            votedCandidateEpoch, otherNodeId), quorumStateStore.readElectionState());

        assertBeginQuorumEpochResponse(votedCandidateEpoch, otherNodeId);
    }

    @Test
    public void testHandleBeginQuorumResponse() throws Exception {
        int otherNodeId = 1;
        int leaderEpoch = 2;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);

        quorumStateStore.writeElectionState(ElectionState.withElectedLeader(leaderEpoch, localId));

        KafkaRaftClient client = buildClient(voters);

        BeginQuorumEpochResponseData beginQuorumEpochRequest = new BeginQuorumEpochResponseData()
                                                                   .setLeaderId(localId)
                                                                   .setLeaderEpoch(leaderEpoch);
        channel.mockReceive(new RaftResponse.Inbound(channel.newCorrelationId(), beginQuorumEpochRequest, otherNodeId));

        client.poll();

        assertEquals(ElectionState.withElectedLeader(
            leaderEpoch, localId), quorumStateStore.readElectionState());
    }

    @Test
    public void testEndQuorumIgnoredIfAlreadyCandidate() throws Exception {
        int otherNodeId = 1;
        int epoch = 2;

        int jitterMs = 85;
        Mockito.doReturn(jitterMs).when(random).nextInt(Mockito.anyInt());

        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        quorumStateStore.writeElectionState(ElectionState.withVotedCandidate(epoch, localId));
        KafkaRaftClient client = buildClient(voters);

        EndQuorumEpochRequestData endQuorumEpochRequest = endEpochRequest(epoch,
            OptionalInt.empty(), otherNodeId);
        channel.mockReceive(new RaftRequest.Inbound(channel.newCorrelationId(),
            endQuorumEpochRequest, time.milliseconds()));
        client.poll();
        assertEndQuorumEpochResponse(Errors.NONE);

        // We should still be candidate until expiration of election timeout
        time.sleep(electionTimeoutMs + jitterMs - 1);
        client.poll();
        assertEquals(ElectionState.withVotedCandidate(epoch, localId), quorumStateStore.readElectionState());

        time.sleep(1);
        client.poll();
        assertEquals(ElectionState.withVotedCandidate(epoch + 1, localId), quorumStateStore.readElectionState());
    }

    @Test
    public void testEndQuorumIgnoredIfAlreadyLeader() throws Exception {
        int voter2 = localId + 1;
        int voter3 = localId + 2;
        int epoch = 2;

        int jitterMs = 85;
        Mockito.doReturn(jitterMs).when(random).nextInt(Mockito.anyInt());

        Set<Integer> voters = Utils.mkSet(localId, voter2, voter3);
        quorumStateStore.writeElectionState(ElectionState.withElectedLeader(epoch, localId));
        KafkaRaftClient client = buildClient(voters);

        // One of the voters may have sent EndEpoch as a candidate because it
        // had not yet been notified that the local node was the leader.
        EndQuorumEpochRequestData endQuorumEpochRequest = endEpochRequest(epoch,
            OptionalInt.empty(), voter2);
        channel.mockReceive(new RaftRequest.Inbound(channel.newCorrelationId(),
            endQuorumEpochRequest, time.milliseconds()));
        client.poll();
        assertEndQuorumEpochResponse(Errors.NONE);

        // We should still be leader even after election timeout has expired
        time.sleep(electionTimeoutMs + jitterMs);
        client.poll();
        assertEquals(ElectionState.withElectedLeader(epoch, localId), quorumStateStore.readElectionState());
    }

    @Test
    public void testEndQuorumStartsNewElectionAfterJitterIfReceivedFromVotedCandidate() throws Exception {
        int otherNodeId = 1;
        int epoch = 2;

        int jitterMs = 85;
        Mockito.doReturn(jitterMs).when(random).nextInt(Mockito.anyInt());

        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        quorumStateStore.writeElectionState(ElectionState.withVotedCandidate(epoch, otherNodeId));
        KafkaRaftClient client = buildClient(voters);

        EndQuorumEpochRequestData endQuorumEpochRequest = endEpochRequest(epoch,
            OptionalInt.empty(), otherNodeId);
        channel.mockReceive(new RaftRequest.Inbound(channel.newCorrelationId(),
            endQuorumEpochRequest, time.milliseconds()));
        client.poll();
        assertEndQuorumEpochResponse(Errors.NONE);

        // The other node will still be considered the voted candidate until expiration of jitter
        time.sleep(jitterMs - 1);
        client.poll();
        assertEquals(ElectionState.withVotedCandidate(epoch, otherNodeId), quorumStateStore.readElectionState());

        time.sleep(1);
        client.poll();
        assertEquals(ElectionState.withVotedCandidate(epoch + 1, localId), quorumStateStore.readElectionState());
    }

    @Test
    public void testEndQuorumStartsNewElectionAfterJitterIfFollowerUnattached() throws Exception {
        int voter2 = localId + 1;
        int voter3 = localId + 2;
        int epoch = 2;

        int jitterMs = 85;
        Mockito.doReturn(jitterMs).when(random).nextInt(Mockito.anyInt());

        Set<Integer> voters = Utils.mkSet(localId, voter2, voter3);
        quorumStateStore.writeElectionState(ElectionState.withUnknownLeader(epoch));
        KafkaRaftClient client = buildClient(voters);

        EndQuorumEpochRequestData endQuorumEpochRequest = endEpochRequest(epoch,
            OptionalInt.of(voter2), voter2);
        channel.mockReceive(new RaftRequest.Inbound(channel.newCorrelationId(),
            endQuorumEpochRequest, time.milliseconds()));
        client.poll();
        assertEndQuorumEpochResponse(Errors.NONE);

        // We should still update the current leader when we receive the EndQuorumEpoch
        time.sleep(jitterMs - 1);
        client.poll();
        assertEquals(ElectionState.withElectedLeader(epoch, voter2), quorumStateStore.readElectionState());

        // Once jitter expires, we should become a candidate
        time.sleep(1);
        client.poll();
        assertEquals(ElectionState.withVotedCandidate(epoch + 1, localId), quorumStateStore.readElectionState());
    }

    @Test
    public void testHandleEndQuorumRequest() throws Exception {
        int oldLeaderId = 1;
        int leaderEpoch = 2;
        Set<Integer> voters = Utils.mkSet(localId, oldLeaderId);

        quorumStateStore.writeElectionState(ElectionState.withElectedLeader(leaderEpoch, oldLeaderId));

        KafkaRaftClient client = buildClient(voters);

        EndQuorumEpochRequestData endQuorumEpochRequest = endEpochRequest(leaderEpoch,
            OptionalInt.of(oldLeaderId), oldLeaderId);
        channel.mockReceive(new RaftRequest.Inbound(channel.newCorrelationId(), endQuorumEpochRequest, time.milliseconds()));

        pollUntilSend(client);

        List<RaftMessage> sentMessages = channel.drainSendQueue();
        assertEquals(2, sentMessages.size());

        RaftMessage findQuorumMessage = getSentMessageByType(sentMessages, ApiKeys.FIND_QUORUM);
        assertTrue(findQuorumMessage.data() instanceof FindQuorumRequestData);

        FindQuorumRequestData findQuorumData = (FindQuorumRequestData) findQuorumMessage.data();
        assertEquals(localId, findQuorumData.replicaId());
        int findQuorumCorrelationId = findQuorumMessage.correlationId();

        RaftMessage endQuorumMessage = getSentMessageByType(sentMessages, ApiKeys.END_QUORUM_EPOCH);
        assertTrue(endQuorumMessage.data() instanceof EndQuorumEpochResponseData);

        channel.mockReceive(new RaftResponse.Inbound(findQuorumCorrelationId,
            findQuorumResponse(OptionalInt.of(localId), leaderEpoch, voters), -1));

        time.sleep(electionJitterMs);

        pollUntilSend(client);

        assertSentVoteRequest(leaderEpoch + 1, 0, 0);

        // Should have already done self-voting
        assertEquals(ElectionState.withVotedCandidate(leaderEpoch + 1, localId),
            quorumStateStore.readElectionState());
    }

    @Test
    public void testVoteRequestTimeout() throws Exception {
        int epoch = 1;
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        KafkaRaftClient client = buildClient(voters);
        assertEquals(ElectionState.withVotedCandidate(epoch, localId), quorumStateStore.readElectionState());

        pollUntilSend(client);

        int findQuorumCorrelationId = assertSentFindQuorumRequest();
        channel.mockReceive(new RaftResponse.Inbound(findQuorumCorrelationId,
            findQuorumResponse(OptionalInt.empty(), epoch, voters), -1));

        pollUntilSend(client);

        int correlationId = assertSentVoteRequest(epoch, 0, 0L);

        time.sleep(requestTimeoutMs);
        client.poll();
        int retryId = assertSentVoteRequest(epoch, 0, 0L);

        // Even though we have resent the request, we should still accept the response to
        // the first request if it arrives late.
        VoteResponseData voteResponse = voteResponse(true, Optional.empty(), 1);
        channel.mockReceive(new RaftResponse.Inbound(correlationId, voteResponse, otherNodeId));
        client.poll();
        assertEquals(ElectionState.withElectedLeader(epoch, localId), quorumStateStore.readElectionState());

        // If the second request arrives later, it should have no effect
        VoteResponseData retryResponse = voteResponse(true, Optional.empty(), 1);
        channel.mockReceive(new RaftResponse.Inbound(retryId, retryResponse, otherNodeId));
        client.poll();
        assertEquals(ElectionState.withElectedLeader(epoch, localId), quorumStateStore.readElectionState());
    }

    @Test
    public void testHandleValidVoteRequestAsFollower() throws Exception {
        int leaderEpoch = 2;
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);

        quorumStateStore.writeElectionState(ElectionState.withUnknownLeader(leaderEpoch));

        KafkaRaftClient client = buildClient(voters);

        handleVoteRequest(leaderEpoch, otherNodeId, client, true);

        assertEquals(ElectionState.withVotedCandidate(leaderEpoch + 1, otherNodeId),
            quorumStateStore.readElectionState());
    }

    @Test
    public void testHandleVoteRequestAsFollowerWithElectedLeader() throws Exception {
        int leaderEpoch = 2;
        int otherNodeId = 1;
        int electedLeaderId = 3;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId, electedLeaderId);

        quorumStateStore.writeElectionState(ElectionState.withElectedLeader(leaderEpoch, electedLeaderId));

        KafkaRaftClient client = buildClient(voters);

        handleVoteRequest(leaderEpoch - 1, otherNodeId, client, false);

        assertEquals(ElectionState.withElectedLeader(leaderEpoch, electedLeaderId),
            quorumStateStore.readElectionState());
    }

    @Test
    public void testHandleVoteRequestAsFollowerWithVotedCandidate() throws Exception {
        int leaderEpoch = 2;
        int otherNodeId = 1;
        int votedCandidateId = 3;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId, votedCandidateId);

        quorumStateStore.writeElectionState(ElectionState.withVotedCandidate(leaderEpoch, votedCandidateId));

        KafkaRaftClient client = buildClient(voters);

        handleVoteRequest(leaderEpoch - 1, otherNodeId, client, false);

        assertEquals(ElectionState.withVotedCandidate(leaderEpoch, votedCandidateId),
            quorumStateStore.readElectionState());
    }

    private void handleVoteRequest(int lastEpoch,
                                   int otherNodeId,
                                   KafkaRaftClient client,
                                   boolean voteGranted) throws Exception {
        VoteRequestData voteRequest = new VoteRequestData()
                                          .setLastEpoch(lastEpoch)
                                          .setLastEpochEndOffset(1)
                                          .setCandidateId(otherNodeId)
                                          .setCandidateEpoch(lastEpoch + 1);
        channel.mockReceive(new RaftRequest.Inbound(
            channel.newCorrelationId(), voteRequest, time.milliseconds()));

        client.poll();

        assertSentVoteResponse(lastEpoch + 1, voteGranted, Errors.NONE);
    }

    @Test
    public void testHandleInvalidVoteRequestWithOlderEpoch() throws Exception {
        int leaderEpoch = 2;
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);

        quorumStateStore.writeElectionState(ElectionState.withUnknownLeader(leaderEpoch));

        KafkaRaftClient client = buildClient(voters);
        handleInvalidVoteRequest(leaderEpoch, leaderEpoch - 2, otherNodeId, client, Errors.FENCED_LEADER_EPOCH);
    }

    @Test
    public void testHandleInvalidVoteRequestAsObserver() throws Exception {
        int leaderEpoch = 2;
        int otherNodeId = 1;
        int otherNodeId2 = 2;
        Set<Integer> voters = Utils.mkSet(otherNodeId, otherNodeId2);

        quorumStateStore.writeElectionState(ElectionState.withUnknownLeader(leaderEpoch));

        KafkaRaftClient client = buildClient(voters);
        handleInvalidVoteRequest(leaderEpoch, leaderEpoch, otherNodeId, client, Errors.INCONSISTENT_VOTER_SET);
        assertEquals(ElectionState.withUnknownLeader(leaderEpoch), quorumStateStore.readElectionState());
    }

    private void handleInvalidVoteRequest(int leaderEpoch,
                                          int lastEpoch,
                                          int otherNodeId,
                                          KafkaRaftClient client,
                                          Errors expectedError) throws Exception {
        VoteRequestData voteRequest = new VoteRequestData()
                                          .setLastEpoch(lastEpoch)
                                          .setLastEpochEndOffset(1)
                                          .setCandidateId(otherNodeId)
                                          .setCandidateEpoch(lastEpoch + 1);
        channel.mockReceive(new RaftRequest.Inbound(
            channel.newCorrelationId(), voteRequest, time.milliseconds()));

        client.poll();

        assertSentVoteResponse(leaderEpoch, false, expectedError);
    }

    @Test
    public void testLeaderIgnoreVoteRequestOnSameEpoch() throws Exception {
        int otherNodeId = 1;
        int leaderEpoch = 2;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);

        quorumStateStore.writeElectionState(ElectionState.withElectedLeader(leaderEpoch, localId));

        KafkaRaftClient client = buildClient(voters);

        VoteRequestData voteRequest = new VoteRequestData()
                                          .setLastEpoch(leaderEpoch - 1)
                                          .setLastEpochEndOffset(1)
                                          .setCandidateId(otherNodeId)
                                          .setCandidateEpoch(leaderEpoch);
        channel.mockReceive(new RaftRequest.Inbound(channel.newCorrelationId(), voteRequest, time.milliseconds()));

        client.poll();
        assertEquals(ElectionState.withElectedLeader(leaderEpoch, localId),
            quorumStateStore.readElectionState());

        List<RaftMessage> unsent = channel.drainSendQueue();
        assertEquals(2, unsent.size());

        RaftMessage findQuorumMessage = getSentMessageByType(unsent, ApiKeys.FIND_QUORUM);
        assertTrue(findQuorumMessage instanceof RaftRequest.Outbound);

        RaftMessage voteResponseMessage = getSentMessageByType(unsent, ApiKeys.VOTE);
        assertTrue(voteResponseMessage instanceof RaftResponse.Outbound);
    }

    @Test
    public void testCandidateIgnoreVoteRequestOnSameEpoch() throws Exception {
        int otherNodeId = 1;
        int leaderEpoch = 2;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);

        quorumStateStore.writeElectionState(ElectionState.withVotedCandidate(leaderEpoch, localId));

        KafkaRaftClient client = buildClient(voters);

        VoteRequestData voteRequest = new VoteRequestData()
                                          .setLastEpoch(leaderEpoch - 1)
                                          .setLastEpochEndOffset(1)
                                          .setCandidateId(otherNodeId)
                                          .setCandidateEpoch(leaderEpoch);
        channel.mockReceive(new RaftRequest.Inbound(channel.newCorrelationId(), voteRequest, time.milliseconds()));

        client.poll();
        assertEquals(ElectionState.withVotedCandidate(leaderEpoch, localId),
            quorumStateStore.readElectionState());

        List<RaftMessage> unsent = channel.drainSendQueue();
        assertEquals(2, unsent.size());

        RaftMessage findQuorumMessage = getSentMessageByType(unsent, ApiKeys.FIND_QUORUM);
        assertTrue(findQuorumMessage instanceof RaftRequest.Outbound);

        RaftMessage voteResponseMessage = getSentMessageByType(unsent, ApiKeys.VOTE);
        assertTrue(voteResponseMessage instanceof RaftResponse.Outbound);
    }

    @Test
    public void testRetryElection() throws Exception {
        int otherNodeId = 1;
        int epoch = 1;

        int jitterMs = 85;
        Mockito.doReturn(jitterMs).when(random).nextInt(Mockito.anyInt());

        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        KafkaRaftClient client = buildClient(voters);
        assertEquals(ElectionState.withVotedCandidate(epoch, localId), quorumStateStore.readElectionState());

        pollUntilSend(client);

        int findQuorumCorrelationId = assertSentFindQuorumRequest();
        channel.mockReceive(new RaftResponse.Inbound(findQuorumCorrelationId,
            findQuorumResponse(OptionalInt.empty(), epoch, voters), -1));

        pollUntilSend(client);

        // Quorum size is two. If the other member rejects, then we need to schedule a revote.
        int correlationId = assertSentVoteRequest(epoch, 0, 0L);
        VoteResponseData voteResponse = voteResponse(false, Optional.empty(), 1);
        channel.mockReceive(new RaftResponse.Inbound(correlationId, voteResponse, otherNodeId));

        client.poll();

        // All nodes have rejected our candidacy, but we should still remember that we had voted
        ElectionState latest = quorumStateStore.readElectionState();
        assertEquals(epoch, latest.epoch);
        assertTrue(latest.hasVoted());
        assertEquals(localId, latest.votedId());

        // Even though our candidacy was rejected, we need to await the expiration of the election
        // timeout (plus jitter) before we bump the epoch and start a new election.
        time.sleep(electionTimeoutMs + jitterMs - 1);
        client.poll();
        assertEquals(epoch, quorumStateStore.readElectionState().epoch);

        // After jitter expires, we become a candidate again
        time.sleep(1);
        client.poll();
        assertEquals(ElectionState.withVotedCandidate(epoch + 1, localId), quorumStateStore.readElectionState());
        assertSentVoteRequest(epoch + 1, 0, 0L);
    }

    @Test
    public void testInitializeAsFollowerEmptyLog() throws Exception {
        int otherNodeId = 1;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        quorumStateStore.writeElectionState(ElectionState.withElectedLeader(epoch, otherNodeId));
        KafkaRaftClient client = buildClient(voters);
        assertEquals(ElectionState.withElectedLeader(epoch, otherNodeId), quorumStateStore.readElectionState());

        pollUntilSend(client);

        int findQuorumCorrelationId = assertSentFindQuorumRequest();
        channel.mockReceive(new RaftResponse.Inbound(findQuorumCorrelationId,
            findQuorumResponse(OptionalInt.of(otherNodeId), epoch, voters), -1));

        pollUntilSend(client);

        assertSentFetchQuorumRecordsRequest(epoch, 0L, 0);
    }

    @Test
    public void testInitializeAsFollowerNonEmptyLog() throws Exception {
        int otherNodeId = 1;
        int epoch = 5;
        int lastEpoch = 3;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        quorumStateStore.writeElectionState(ElectionState.withElectedLeader(epoch, otherNodeId));
        log.appendAsLeader(Collections.singleton(new SimpleRecord("foo".getBytes())), lastEpoch);

        KafkaRaftClient client = buildClient(voters);
        assertEquals(ElectionState.withElectedLeader(epoch, otherNodeId), quorumStateStore.readElectionState());

        pollUntilSend(client);

        int findQuorumCorrelationId = assertSentFindQuorumRequest();
        channel.mockReceive(new RaftResponse.Inbound(findQuorumCorrelationId,
            findQuorumResponse(OptionalInt.of(otherNodeId), epoch, voters), -1));

        pollUntilSend(client);

        assertSentFetchQuorumRecordsRequest(epoch, 1L, lastEpoch);
    }

    @Test
    public void testBecomeCandidateAfterElectionTimeout() throws Exception {
        int otherNodeId = 1;
        int epoch = 5;
        int lastEpoch = 3;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        quorumStateStore.writeElectionState(ElectionState.withElectedLeader(epoch, otherNodeId));
        log.appendAsLeader(Collections.singleton(new SimpleRecord("foo".getBytes())), lastEpoch);

        KafkaRaftClient client = buildClient(voters);
        assertEquals(ElectionState.withElectedLeader(epoch, otherNodeId), quorumStateStore.readElectionState());

        pollUntilSend(client);

        int findQuorumCorrelationId = assertSentFindQuorumRequest();
        channel.mockReceive(new RaftResponse.Inbound(findQuorumCorrelationId,
            findQuorumResponse(OptionalInt.of(otherNodeId), epoch, voters), -1));

        pollUntilSend(client);

        assertSentFetchQuorumRecordsRequest(epoch, 1L, lastEpoch);

        time.sleep(electionTimeoutMs);

        client.poll();
        assertSentVoteRequest(epoch + 1, lastEpoch, 1L);
    }

    @Test
    public void testInitializeObserverNoPreviousState() throws IOException {
        int leaderId = 1;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(leaderId);
        KafkaRaftClient client = buildClient(voters);

        client.poll();
        int correlationId = assertSentFindQuorumRequest();
        channel.mockReceive(new RaftResponse.Inbound(correlationId,
            findQuorumResponse(OptionalInt.of(leaderId), epoch, voters), -1));

        client.poll();
        assertEquals(ElectionState.withElectedLeader(epoch, leaderId), quorumStateStore.readElectionState());
    }

    @Test
    public void testObserverFindQuorumFailure() throws IOException {
        int leaderId = 1;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(leaderId);
        KafkaRaftClient client = buildClient(voters);

        client.poll();
        int correlationId = assertSentFindQuorumRequest();
        channel.mockReceive(new RaftResponse.Inbound(correlationId, findQuorumFailure(Errors.UNKNOWN_SERVER_ERROR), -1));

        client.poll();
        assertEquals(0, channel.drainSendQueue().size());

        time.sleep(retryBackoffMs);

        client.poll();
        int retryId = assertSentFindQuorumRequest();
        channel.mockReceive(new RaftResponse.Inbound(retryId,
            findQuorumResponse(OptionalInt.of(leaderId), epoch, voters), -1));

        client.poll();
        assertEquals(ElectionState.withElectedLeader(epoch, leaderId), quorumStateStore.readElectionState());
    }

    @Test
    public void testObserverFindQuorumAfterElectionTimeout() throws IOException {
        int leaderId = 1;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(leaderId);
        KafkaRaftClient client = buildClient(voters);

        client.poll();
        int correlationId = assertSentFindQuorumRequest();
        channel.mockReceive(new RaftResponse.Inbound(correlationId,
            findQuorumResponse(OptionalInt.of(leaderId), epoch, voters), -1));

        client.poll();
        assertEquals(ElectionState.withElectedLeader(epoch, leaderId), quorumStateStore.readElectionState());

        time.sleep(electionTimeoutMs);

        client.poll();
        assertSentFindQuorumRequest();
    }

    @Test
    public void testInvalidFetchRequest() throws Exception {
        int otherNodeId = 1;
        int epoch = 5;

        quorumStateStore.writeElectionState(ElectionState.withElectedLeader(epoch, localId));
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        KafkaRaftClient client = buildClient(voters);
        assertEquals(ElectionState.withElectedLeader(epoch, localId), quorumStateStore.readElectionState());

        pollUntilSend(client);
        int findQuorumCorrelationId = assertSentFindQuorumRequest();
        channel.mockReceive(new RaftResponse.Inbound(findQuorumCorrelationId,
            findQuorumResponse(OptionalInt.empty(), 1, voters), -1));
        client.poll();
        channel.drainSendQueue();

        channel.mockReceive(new RaftRequest.Inbound(channel.newCorrelationId(),
            fetchQuorumRecordsRequest(epoch, otherNodeId, -5L, 0), time.milliseconds()));
        client.poll();
        assertFailedFetchQuorumRecordsResponse(Errors.INVALID_REQUEST);

        channel.mockReceive(new RaftRequest.Inbound(channel.newCorrelationId(),
            fetchQuorumRecordsRequest(epoch, otherNodeId, 0L, -1), time.milliseconds()));
        client.poll();
        assertFailedFetchQuorumRecordsResponse(Errors.INVALID_REQUEST);

        channel.mockReceive(new RaftRequest.Inbound(channel.newCorrelationId(),
            fetchQuorumRecordsRequest(epoch, otherNodeId, 0L, epoch + 1), time.milliseconds()));
        client.poll();
        assertFailedFetchQuorumRecordsResponse(Errors.INVALID_REQUEST);

        channel.mockReceive(new RaftRequest.Inbound(channel.newCorrelationId(),
            fetchQuorumRecordsRequest(epoch + 1, otherNodeId, 0L, 0), time.milliseconds()));
        client.poll();
        assertFailedFetchQuorumRecordsResponse(Errors.INVALID_REQUEST);
    }

    @Test
    public void testVoterOnlyRequestValidation() throws Exception {
        int otherNodeId = 1;
        int epoch = 5;

        quorumStateStore.writeElectionState(ElectionState.withElectedLeader(epoch, localId));
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        KafkaRaftClient client = buildClient(voters);
        assertEquals(ElectionState.withElectedLeader(epoch, localId), quorumStateStore.readElectionState());

        pollUntilSend(client);
        int findQuorumCorrelationId = assertSentFindQuorumRequest();
        channel.mockReceive(new RaftResponse.Inbound(findQuorumCorrelationId,
            findQuorumResponse(OptionalInt.empty(), 1, voters), -1));
        client.poll();
        channel.drainSendQueue();

        int nonVoterId = 2;
        channel.mockReceive(new RaftRequest.Inbound(channel.newCorrelationId(),
            voteRequest(epoch, nonVoterId, 0, 0), time.milliseconds()));
        client.poll();
        assertFailedVoteResponse(Errors.INCONSISTENT_VOTER_SET);

        channel.mockReceive(new RaftRequest.Inbound(channel.newCorrelationId(),
            beginEpochRequest(epoch, nonVoterId), time.milliseconds()));
        client.poll();
        assertFailedBeginQuorumEpochResponse(Errors.INCONSISTENT_VOTER_SET);

        channel.mockReceive(new RaftRequest.Inbound(channel.newCorrelationId(),
            endEpochRequest(epoch, OptionalInt.of(localId), nonVoterId), time.milliseconds()));
        client.poll();
        assertEndQuorumEpochResponse(Errors.INCONSISTENT_VOTER_SET);
    }

    @Test
    public void testInvalidVoteRequest() throws Exception {
        int otherNodeId = 1;
        int epoch = 5;

        quorumStateStore.writeElectionState(ElectionState.withElectedLeader(epoch, otherNodeId));
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        KafkaRaftClient client = buildClient(voters);
        assertEquals(ElectionState.withElectedLeader(epoch, otherNodeId), quorumStateStore.readElectionState());

        pollUntilSend(client);
        int findQuorumCorrelationId = assertSentFindQuorumRequest();
        channel.mockReceive(new RaftResponse.Inbound(findQuorumCorrelationId,
            findQuorumResponse(OptionalInt.empty(), 1, voters), -1));
        client.poll();
        channel.drainSendQueue();

        channel.mockReceive(new RaftRequest.Inbound(channel.newCorrelationId(),
            voteRequest(epoch + 1, otherNodeId, 0, -5L), time.milliseconds()));
        client.poll();
        assertFailedVoteResponse(Errors.INVALID_REQUEST);

        channel.mockReceive(new RaftRequest.Inbound(channel.newCorrelationId(),
            voteRequest(epoch + 1, otherNodeId, -1, 0L), time.milliseconds()));
        client.poll();
        assertFailedVoteResponse(Errors.INVALID_REQUEST);

        channel.mockReceive(new RaftRequest.Inbound(channel.newCorrelationId(),
            voteRequest(epoch + 1, otherNodeId, epoch + 1, 0L), time.milliseconds()));
        client.poll();
        assertFailedVoteResponse(Errors.INVALID_REQUEST);
    }

    @Test
    public void testFetchResponseIgnoredAfterBecomingCandidate() throws Exception {
        int otherNodeId = 1;
        int epoch = 5;

        // The other node starts out as the leader
        quorumStateStore.writeElectionState(ElectionState.withElectedLeader(epoch, otherNodeId));
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        KafkaRaftClient client = buildClient(voters);
        assertEquals(ElectionState.withElectedLeader(epoch, otherNodeId), quorumStateStore.readElectionState());

        pollUntilSend(client);
        int findQuorumCorrelationId = assertSentFindQuorumRequest();
        channel.mockReceive(new RaftResponse.Inbound(findQuorumCorrelationId,
            findQuorumResponse(OptionalInt.empty(), epoch, voters), -1));

        // Wait until we have a Fetch inflight to the leader
        pollUntilSend(client);
        int fetchCorrelationId = assertSentFetchQuorumRecordsRequest(epoch, 0L, 0);

        // Now await the election timeout and become a candidate
        time.sleep(electionTimeoutMs);
        client.poll();
        assertEquals(ElectionState.withVotedCandidate(epoch + 1, localId), quorumStateStore.readElectionState());

        // The fetch response from the old leader returns, but it should be ignored
        Records records = MemoryRecords.withRecords(0L, CompressionType.NONE,
            3, new SimpleRecord("a".getBytes()), new SimpleRecord("b".getBytes()));
        FetchQuorumRecordsResponseData response = fetchRecordsResponse(epoch, otherNodeId, records, 0L, Errors.NONE);
        channel.mockReceive(new RaftResponse.Inbound(fetchCorrelationId, response, otherNodeId));

        client.poll();
        assertEquals(0, log.endOffset());
        assertEquals(ElectionState.withVotedCandidate(epoch + 1, localId), quorumStateStore.readElectionState());
    }

    @Test
    public void testFetchResponseIgnoredAfterBecomingFollowerOfDifferentLeader() throws Exception {
        int voter1 = localId;
        int voter2 = localId + 1;
        int voter3 = localId + 2;
        int epoch = 5;

        // Start out with `voter2` as the leader
        quorumStateStore.writeElectionState(ElectionState.withElectedLeader(epoch, voter2));
        Set<Integer> voters = Utils.mkSet(voter1, voter2, voter3);
        KafkaRaftClient client = buildClient(voters);
        assertEquals(ElectionState.withElectedLeader(epoch, voter2), quorumStateStore.readElectionState());

        pollUntilSend(client);
        int findQuorumCorrelationId = assertSentFindQuorumRequest();
        channel.mockReceive(new RaftResponse.Inbound(findQuorumCorrelationId,
            findQuorumResponse(OptionalInt.empty(), epoch, voters), -1));

        // Wait until we have a Fetch inflight to the leader
        pollUntilSend(client);
        int fetchCorrelationId = assertSentFetchQuorumRecordsRequest(epoch, 0L, 0);

        // Now receive a BeginEpoch from `voter3`
        BeginQuorumEpochRequestData beginEpochRequest = beginEpochRequest(epoch + 1, voter3);
        channel.mockReceive(new RaftRequest.Inbound(channel.newCorrelationId(), beginEpochRequest, time.milliseconds()));
        client.poll();
        assertEquals(ElectionState.withElectedLeader(epoch + 1, voter3), quorumStateStore.readElectionState());

        // The fetch response from the old leader returns, but it should be ignored
        Records records = MemoryRecords.withRecords(0L, CompressionType.NONE,
            3, new SimpleRecord("a".getBytes()), new SimpleRecord("b".getBytes()));
        FetchQuorumRecordsResponseData response = fetchRecordsResponse(epoch, voter2, records, 0L, Errors.NONE);
        channel.mockReceive(new RaftResponse.Inbound(fetchCorrelationId, response, voter2));

        client.poll();
        assertEquals(0, log.endOffset());
        assertEquals(ElectionState.withElectedLeader(epoch + 1, voter3), quorumStateStore.readElectionState());
    }

    @Test
    public void testVoteResponseIgnoredAfterBecomingFollower() throws Exception {
        int voter1 = localId;
        int voter2 = localId + 1;
        int voter3 = localId + 2;
        int epoch = 5;

        // This node initializes as a candidate
        quorumStateStore.writeElectionState(ElectionState.withVotedCandidate(epoch, voter1));
        Set<Integer> voters = Utils.mkSet(voter1, voter2, voter3);
        KafkaRaftClient client = buildClient(voters);
        assertEquals(ElectionState.withVotedCandidate(epoch, voter1), quorumStateStore.readElectionState());

        pollUntilSend(client);
        int findQuorumCorrelationId = assertSentFindQuorumRequest();
        channel.mockReceive(new RaftResponse.Inbound(findQuorumCorrelationId,
            findQuorumResponse(OptionalInt.empty(), epoch, voters), -1));

        // Wait until the vote requests are inflight
        pollUntilSend(client);
        List<RaftRequest.Outbound> voteRequests = collectVoteRequests(epoch, 0, 0);
        assertEquals(2, voteRequests.size());

        // While the vote requests are still inflight, we receive a BeginEpoch for the same epoch
        BeginQuorumEpochRequestData beginEpochRequest = beginEpochRequest(epoch, voter3);
        channel.mockReceive(new RaftRequest.Inbound(channel.newCorrelationId(), beginEpochRequest, time.milliseconds()));
        client.poll();
        assertEquals(ElectionState.withElectedLeader(epoch, voter3), quorumStateStore.readElectionState());

        // The vote requests now return and should be ignored
        VoteResponseData voteResponse1 = voteResponse(false, Optional.empty(), epoch);
        channel.mockReceive(new RaftResponse.Inbound(voteRequests.get(0).correlationId, voteResponse1, voter2));

        VoteResponseData voteResponse2 = voteResponse(false, Optional.of(voter3), epoch);
        channel.mockReceive(new RaftResponse.Inbound(voteRequests.get(0).correlationId, voteResponse2, voter3));

        client.poll();
        assertEquals(ElectionState.withElectedLeader(epoch, voter3), quorumStateStore.readElectionState());
    }

    @Test
    public void testObserverLeaderRediscoveryAfterBrokerNotAvailableError() throws IOException {
        int leaderId = 1;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(leaderId);
        KafkaRaftClient client = buildClient(voters);

        client.poll();
        int correlationId = assertSentFindQuorumRequest();
        channel.mockReceive(new RaftResponse.Inbound(correlationId,
            findQuorumResponse(OptionalInt.of(leaderId), epoch, voters), -1));

        client.poll();
        assertEquals(ElectionState.withElectedLeader(epoch, leaderId), quorumStateStore.readElectionState());

        client.poll();
        int fetchCorrelationId = assertSentFetchQuorumRecordsRequest(epoch, 0L, 0);

        FetchQuorumRecordsResponseData response = fetchRecordsResponse(epoch, leaderId, MemoryRecords.EMPTY, 0L,
                Errors.BROKER_NOT_AVAILABLE);
        channel.mockReceive(new RaftResponse.Inbound(fetchCorrelationId, response, leaderId));
        client.poll();

        assertEquals(ElectionState.withUnknownLeader(epoch), quorumStateStore.readElectionState());
        client.poll();
        assertSentFindQuorumRequest();
    }

    @Test
    public void testObserverLeaderRediscoveryAfterRequestTimeout() throws Exception {
        int leaderId = 1;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(leaderId);
        KafkaRaftClient client = buildClient(voters);

        client.poll();
        int correlationId = assertSentFindQuorumRequest();
        channel.mockReceive(new RaftResponse.Inbound(correlationId,
            findQuorumResponse(OptionalInt.of(leaderId), epoch, voters), -1));

        pollUntilSend(client);
        assertEquals(ElectionState.withElectedLeader(epoch, leaderId), quorumStateStore.readElectionState());
        assertSentFetchQuorumRecordsRequest(epoch, 0L, 0);

        time.sleep(requestTimeoutMs);
        client.poll();

        assertEquals(ElectionState.withUnknownLeader(epoch), quorumStateStore.readElectionState());
        client.poll();
        assertSentFindQuorumRequest();
    }

    @Test
    public void testLeaderHandlesFindQuorum() throws IOException {
        KafkaRaftClient client = buildClient(Collections.singleton(localId));
        assertEquals(ElectionState.withElectedLeader(1, localId), quorumStateStore.readElectionState());

        int observerId = 1;
        FindQuorumRequestData request = new FindQuorumRequestData().setReplicaId(observerId);
        channel.mockReceive(new RaftRequest.Inbound(channel.newCorrelationId(), request, time.milliseconds()));

        client.poll();
        assertSentFindQuorumResponse(1, Optional.of(localId));
    }

    @Test
    public void testLeaderGracefulShutdown() throws Exception {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        KafkaRaftClient client = buildClient(voters);

        // Elect ourselves as the leader
        assertEquals(ElectionState.withVotedCandidate(1, localId), quorumStateStore.readElectionState());

        pollUntilSend(client);

        int findQuorumCorrelationId = assertSentFindQuorumRequest();
        channel.mockReceive(new RaftResponse.Inbound(findQuorumCorrelationId,
            findQuorumResponse(OptionalInt.empty(), 1, voters), -1));

        pollUntilSend(client);

        int voteCorrelationId = assertSentVoteRequest(1, 0, 0L);
        VoteResponseData voteResponse = voteResponse(true, Optional.empty(), 1);
        channel.mockReceive(new RaftResponse.Inbound(voteCorrelationId, voteResponse, otherNodeId));
        client.poll();
        assertEquals(ElectionState.withElectedLeader(1, localId), quorumStateStore.readElectionState());

        // Now shutdown
        int shutdownTimeoutMs = 5000;
        client.shutdown(shutdownTimeoutMs);

        // We should still be running until we have had a chance to send EndQuorumEpoch
        assertTrue(client.isRunning());

        // Send EndQuorumEpoch request to the other vote
        client.poll();
        assertTrue(client.isRunning());
        assertSentEndQuorumEpochRequest(1, localId);

        // Graceful shutdown completes when the epoch is bumped
        VoteRequestData newVoteRequest = voteRequest(2, otherNodeId, 0, 0L);
        channel.mockReceive(new RaftRequest.Inbound(channel.newCorrelationId(), newVoteRequest, time.milliseconds()));

        client.poll();
        assertFalse(client.isRunning());
    }

    @Test
    public void testLeaderGracefulShutdownTimeout() throws Exception {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        KafkaRaftClient client = buildClient(voters);

        // Elect ourselves as the leader
        assertEquals(ElectionState.withVotedCandidate(1, localId), quorumStateStore.readElectionState());

        pollUntilSend(client);

        int findQuorumCorrelationId = assertSentFindQuorumRequest();
        channel.mockReceive(new RaftResponse.Inbound(findQuorumCorrelationId,
            findQuorumResponse(OptionalInt.empty(), 1, voters), -1));

        pollUntilSend(client);

        int voteCorrelationId = assertSentVoteRequest(1, 0, 0L);
        VoteResponseData voteResponse = voteResponse(true, Optional.empty(), 1);
        channel.mockReceive(new RaftResponse.Inbound(voteCorrelationId, voteResponse, otherNodeId));
        client.poll();
        assertEquals(ElectionState.withElectedLeader(1, localId), quorumStateStore.readElectionState());

        // Now shutdown
        int shutdownTimeoutMs = 5000;
        client.shutdown(shutdownTimeoutMs);

        // We should still be running until we have had a chance to send EndQuorumEpoch
        assertTrue(client.isRunning());

        // Send EndQuorumEpoch request to the other vote
        client.poll();
        assertTrue(client.isRunning());
        assertSentEndQuorumEpochRequest(1, localId);

        // The shutdown timeout is hit before we receive any requests or responses indicating an epoch bump
        time.sleep(shutdownTimeoutMs);

        client.poll();
        assertFalse(client.isRunning());
    }

    @Test
    public void testFollowerGracefulShutdown() throws IOException {
        int otherNodeId = 1;
        int epoch = 5;
        quorumStateStore.writeElectionState(ElectionState.withElectedLeader(epoch, otherNodeId));
        KafkaRaftClient client = buildClient(Utils.mkSet(localId, otherNodeId));
        assertEquals(ElectionState.withElectedLeader(epoch, otherNodeId), quorumStateStore.readElectionState());
        assertTrue(stateMachine.isFollower());
        assertEquals(epoch, stateMachine.epoch());

        client.poll();

        int shutdownTimeoutMs = 5000;
        client.shutdown(shutdownTimeoutMs);
        assertTrue(client.isRunning());
        client.poll();
        assertFalse(client.isRunning());
    }

    @Test
    public void testGracefulShutdownSingleMemberQuorum() throws IOException {
        KafkaRaftClient client = buildClient(Collections.singleton(localId));
        assertEquals(ElectionState.withElectedLeader(1, localId), quorumStateStore.readElectionState());
        client.poll();
        assertEquals(0, channel.drainSendQueue().size());
        int shutdownTimeoutMs = 5000;
        client.shutdown(shutdownTimeoutMs);
        assertTrue(client.isRunning());
        client.poll();
        assertFalse(client.isRunning());
    }

    @Test
    public void testFollowerReplication() throws Exception {
        int otherNodeId = 1;
        int epoch = 5;
        quorumStateStore.writeElectionState(ElectionState.withElectedLeader(epoch, otherNodeId));
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        KafkaRaftClient client = buildClient(voters);
        assertEquals(ElectionState.withElectedLeader(epoch, otherNodeId), quorumStateStore.readElectionState());
        assertTrue(stateMachine.isFollower());
        assertEquals(epoch, stateMachine.epoch());

        pollUntilSend(client);

        int findQuorumCorrelationId = assertSentFindQuorumRequest();
        channel.mockReceive(new RaftResponse.Inbound(findQuorumCorrelationId,
            findQuorumResponse(OptionalInt.of(otherNodeId), epoch, voters), -1));

        pollUntilSend(client);

        int fetchQuorumCorrelationId = assertSentFetchQuorumRecordsRequest(epoch, 0L, 0);
        Records records = MemoryRecords.withRecords(0L, CompressionType.NONE,
            3, new SimpleRecord("a".getBytes()), new SimpleRecord("b".getBytes()));
        FetchQuorumRecordsResponseData response = fetchRecordsResponse(epoch, otherNodeId, records, 0L, Errors.NONE);
        channel.mockReceive(new RaftResponse.Inbound(fetchQuorumCorrelationId, response, otherNodeId));

        client.poll();
        assertEquals(2L, log.endOffset());
    }

    @Test
    public void testAppendToNonLeaderFails() throws IOException {
        int otherNodeId = 1;
        int epoch = 5;
        quorumStateStore.writeElectionState(ElectionState.withElectedLeader(epoch, otherNodeId));
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        KafkaRaftClient client = buildClient(voters);
        assertEquals(ElectionState.withElectedLeader(epoch, otherNodeId), quorumStateStore.readElectionState());
        assertTrue(stateMachine.isFollower());
        assertEquals(epoch, stateMachine.epoch());

        SimpleRecord[] appendRecords = new SimpleRecord[] {
            new SimpleRecord("a".getBytes()),
            new SimpleRecord("b".getBytes()),
            new SimpleRecord("c".getBytes())
        };
        Records records = MemoryRecords.withRecords(0L, CompressionType.NONE, 1, appendRecords);

        CompletableFuture<OffsetAndEpoch> future = client.append(records);
        client.poll();

        assertTrue(future.isCompletedExceptionally());
        TestUtils.assertFutureThrows(future, NotLeaderForPartitionException.class);
    }

    @Test
    public void testFetchShouldBeTreatedAsLeaderEndorsement() throws Exception {
        int otherNodeId = 1;
        int epoch = 5;
        quorumStateStore.writeElectionState(ElectionState.withElectedLeader(epoch, localId));
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        KafkaRaftClient client = buildClient(voters);
        assertEquals(ElectionState.withElectedLeader(epoch, localId), quorumStateStore.readElectionState());

        pollUntilSend(client);

        int findQuorumCorrelationId = assertSentFindQuorumRequest();
        channel.mockReceive(new RaftResponse.Inbound(findQuorumCorrelationId,
            findQuorumResponse(OptionalInt.of(localId), epoch, voters), -1));

        pollUntilSend(client);

        // We send BeginEpoch, but it gets lost and the destination finds the leader through the FindQuorum API
        assertBeginQuorumEpochRequest(epoch);

        FetchQuorumRecordsRequestData fetchRequest = fetchQuorumRecordsRequest(epoch, otherNodeId, 0L, 0);
        channel.mockReceive(new RaftRequest.Inbound(channel.newCorrelationId(), fetchRequest, time.milliseconds()));

        client.poll();

        // The BeginEpoch request eventually times out. We should not send another one.
        assertFetchQuorumRecordsResponse(epoch, localId);
        time.sleep(requestTimeoutMs);

        client.poll();

        List<RaftMessage> sentMessages = channel.drainSendQueue();
        assertEquals(0, sentMessages.size());
    }

    @Test
    public void testLeaderAppendSingleMemberQuorum() throws IOException {
        long now = time.milliseconds();

        KafkaRaftClient client = buildClient(Collections.singleton(localId));
        assertEquals(ElectionState.withElectedLeader(1, localId), quorumStateStore.readElectionState());

        SimpleRecord[] appendRecords = new SimpleRecord[] {
            new SimpleRecord("a".getBytes()),
            new SimpleRecord("b".getBytes()),
            new SimpleRecord("c".getBytes())
        };
        Records records = MemoryRecords.withRecords(1L, CompressionType.NONE, 1, appendRecords);

        // First poll has no high watermark advance
        client.poll();
        assertEquals(OptionalLong.of(0L), client.highWatermark());

        client.append(records);

        // Then poll the appended data with leader change record
        client.poll();
        assertEquals(OptionalLong.of(4L), client.highWatermark());

        // Now try reading it
        int otherNodeId = 1;
        FetchQuorumRecordsRequestData fetchRequest = fetchQuorumRecordsRequest(1, otherNodeId, 0L, 0);
        channel.mockReceive(new RaftRequest.Inbound(channel.newCorrelationId(), fetchRequest, time.milliseconds()));

        client.poll();

        MemoryRecords fetchedRecords = assertFetchQuorumRecordsResponse(1, localId);
        List<MutableRecordBatch> batches = Utils.toList(fetchedRecords.batchIterator());
        assertEquals(2, batches.size());

        MutableRecordBatch leaderChangeBatch = batches.get(0);
        assertTrue(leaderChangeBatch.isControlBatch());
        List<Record> readRecords = Utils.toList(leaderChangeBatch.iterator());
        assertEquals(1, readRecords.size());

        Record record = readRecords.get(0);
        assertEquals(now, record.timestamp());
        verifyLeaderChangeMessage(localId, Collections.emptyList(),
            record.key(), record.value());

        MutableRecordBatch batch = batches.get(1);
        assertEquals(1, batch.partitionLeaderEpoch());
        readRecords = Utils.toList(batch.iterator());
        assertEquals(3, readRecords.size());

        for (int i = 0; i < appendRecords.length; i++) {
            assertEquals(appendRecords[i].value(), readRecords.get(i).value());
        }
    }

    @Test
    public void testFollowerLogReconciliation() throws Exception {
        int otherNodeId = 1;
        int epoch = 5;
        int lastEpoch = 3;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        quorumStateStore.writeElectionState(ElectionState.withElectedLeader(epoch, otherNodeId));
        log.appendAsLeader(Arrays.asList(
                new SimpleRecord("foo".getBytes()),
                new SimpleRecord("bar".getBytes())), lastEpoch);
        log.appendAsLeader(Arrays.asList(
            new SimpleRecord("baz".getBytes())), lastEpoch);

        KafkaRaftClient client = buildClient(voters);

        assertEquals(ElectionState.withElectedLeader(epoch, otherNodeId), quorumStateStore.readElectionState());
        assertEquals(3L, log.endOffset());

        pollUntilSend(client);

        int findQuorumCorrelationId = assertSentFindQuorumRequest();
        channel.mockReceive(new RaftResponse.Inbound(findQuorumCorrelationId,
            findQuorumResponse(OptionalInt.of(otherNodeId), epoch, voters), -1));

        pollUntilSend(client);

        int correlationId = assertSentFetchQuorumRecordsRequest(epoch, 3L, lastEpoch);

        FetchQuorumRecordsResponseData response = outOfRangeFetchRecordsResponse(epoch, otherNodeId, 2L,
            lastEpoch, 1L);
        channel.mockReceive(new RaftResponse.Inbound(correlationId, response, otherNodeId));

        // Poll again to complete truncation
        client.poll();
        assertEquals(2L, log.endOffset());

        // Now we should be fetching
        client.poll();
        assertSentFetchQuorumRecordsRequest(epoch, 2L, lastEpoch);
    }

    private void verifyLeaderChangeMessage(int leaderId,
                                           List<Integer> voters,
                                           ByteBuffer recordKey,
                                           ByteBuffer recordValue) {
        assertEquals(ControlRecordType.LEADER_CHANGE, ControlRecordType.parse(recordKey));

        LeaderChangeMessage leaderChangeMessage = ControlRecordUtils.deserializeLeaderChangeMessage(recordValue);
        assertEquals(leaderId, leaderChangeMessage.leaderId());
        assertEquals(voters.stream().map(voterId -> new Voter().setVoterId(voterId)).collect(Collectors.toList()),
            leaderChangeMessage.voters());
    }

    private int assertSentFindQuorumResponse(int epoch, Optional<Integer> leaderId) {
        List<RaftMessage> sentMessages = channel.drainSendQueue();
        assertEquals(1, sentMessages.size());
        RaftMessage raftMessage = sentMessages.get(0);
        assertTrue(raftMessage.data() instanceof FindQuorumResponseData);
        FindQuorumResponseData response = (FindQuorumResponseData) raftMessage.data();
        assertEquals(Errors.NONE, Errors.forCode(response.errorCode()));
        assertEquals(epoch, response.leaderEpoch());
        assertEquals(leaderId.orElse(-1).intValue(), response.leaderId());
        return raftMessage.correlationId();
    }

    private void assertFailedFetchQuorumRecordsResponse(Errors error) {
        List<RaftResponse.Outbound> sentMessages = channel.drainSentResponses();
        assertEquals(1, sentMessages.size());
        RaftMessage raftMessage = sentMessages.get(0);
        assertTrue(raftMessage.data() instanceof FetchQuorumRecordsResponseData);
        FetchQuorumRecordsResponseData response = (FetchQuorumRecordsResponseData) raftMessage.data();
        assertEquals(error, Errors.forCode(response.errorCode()));
        assertEquals(0, response.records().remaining());
    }

    private void assertFailedVoteResponse(Errors error) {
        List<RaftResponse.Outbound> sentMessages = channel.drainSentResponses();
        assertEquals(1, sentMessages.size());
        RaftMessage raftMessage = sentMessages.get(0);
        assertTrue(raftMessage.data() instanceof VoteResponseData);
        VoteResponseData response = (VoteResponseData) raftMessage.data();
        assertEquals(error, Errors.forCode(response.errorCode()));
    }

    private void assertFailedBeginQuorumEpochResponse(Errors error) {
        List<RaftResponse.Outbound> sentMessages = channel.drainSentResponses();
        assertEquals(1, sentMessages.size());
        RaftMessage raftMessage = sentMessages.get(0);
        assertTrue(raftMessage.data() instanceof BeginQuorumEpochResponseData);
        BeginQuorumEpochResponseData response = (BeginQuorumEpochResponseData) raftMessage.data();
        assertEquals(error, Errors.forCode(response.errorCode()));
    }

    private void assertEndQuorumEpochResponse(Errors error) {
        List<RaftResponse.Outbound> sentMessages = channel.drainSentResponses();
        assertEquals(1, sentMessages.size());
        RaftMessage raftMessage = sentMessages.get(0);
        assertTrue(raftMessage.data() instanceof EndQuorumEpochResponseData);
        EndQuorumEpochResponseData response = (EndQuorumEpochResponseData) raftMessage.data();
        assertEquals(error, Errors.forCode(response.errorCode()));
    }

    private MemoryRecords assertFetchQuorumRecordsResponse(int epoch, int leaderId) {
        List<RaftMessage> sentMessages = channel.drainSendQueue();
        assertEquals(1, sentMessages.size());
        RaftMessage raftMessage = sentMessages.get(0);
        assertTrue(raftMessage.data() instanceof FetchQuorumRecordsResponseData);
        FetchQuorumRecordsResponseData response = (FetchQuorumRecordsResponseData) raftMessage.data();
        assertEquals(Errors.NONE, Errors.forCode(response.errorCode()));
        assertEquals(epoch, response.leaderEpoch());
        assertEquals(leaderId, response.leaderId());
        return MemoryRecords.readableRecords(response.records());
    }

    private int assertSentEndQuorumEpochRequest(int epoch, int leaderId) {
        List<RaftMessage> sentMessages = channel.drainSendQueue();
        assertEquals(1, sentMessages.size());
        RaftMessage raftMessage = sentMessages.get(0);
        assertTrue(raftMessage.data() instanceof EndQuorumEpochRequestData);
        EndQuorumEpochRequestData request = (EndQuorumEpochRequestData) raftMessage.data();
        assertEquals(epoch, request.leaderEpoch());
        assertEquals(leaderId, request.leaderId());
        assertEquals(localId, request.replicaId());
        return raftMessage.correlationId();
    }

    private int assertSentFindQuorumRequest() {
        List<RaftMessage> sentMessages = channel.drainSendQueue();
        assertEquals(1, sentMessages.size());
        RaftMessage raftMessage = sentMessages.get(0);
        assertTrue(raftMessage.data() instanceof FindQuorumRequestData);
        FindQuorumRequestData request = (FindQuorumRequestData) raftMessage.data();
        assertEquals(localId, request.replicaId());
        assertEquals(-1, ((RaftRequest.Outbound) raftMessage).destinationId());
        return raftMessage.correlationId();
    }

    private int assertSentVoteRequest(int epoch, int lastEpoch, long lastEpochOffset) {
        List<RaftRequest.Outbound> voteRequests = collectVoteRequests(epoch, lastEpoch, lastEpochOffset);
        assertEquals(1, voteRequests.size());
        return voteRequests.iterator().next().correlationId();
    }

    private List<RaftRequest.Outbound> collectVoteRequests(int epoch, int lastEpoch, long lastEpochOffset) {
        List<RaftRequest.Outbound> voteRequests = new ArrayList<>();
        for (RaftMessage raftMessage : channel.drainSendQueue()) {
            if (raftMessage.data() instanceof VoteRequestData) {
                VoteRequestData request = (VoteRequestData) raftMessage.data();
                assertEquals(epoch, request.candidateEpoch());
                assertEquals(localId, request.candidateId());
                assertEquals(lastEpoch, request.lastEpoch());
                assertEquals(lastEpochOffset, request.lastEpochEndOffset());
                voteRequests.add((RaftRequest.Outbound) raftMessage);
            }
        }
        return voteRequests;
    }

    private int assertSentVoteResponse(int leaderEpoch, boolean voteGranted, Errors expectedError) {
        List<RaftResponse.Outbound> voteResponses = collectVoteResponses(leaderEpoch, voteGranted, expectedError);
        assertEquals(1, voteResponses.size());
        return voteResponses.iterator().next().correlationId();
    }

    private List<RaftResponse.Outbound> collectVoteResponses(int leaderEpoch, boolean voteGranted, Errors expectedError) {
        List<RaftResponse.Outbound> voteResponses = new ArrayList<>();
        for (RaftMessage raftMessage : channel.drainSendQueue()) {
            if (raftMessage.data() instanceof VoteResponseData) {
                VoteResponseData response = (VoteResponseData) raftMessage.data();
                if (voteGranted) {
                    assertEquals(-1, response.leaderId());
                }
                assertEquals(leaderEpoch, response.leaderEpoch());
                assertEquals(voteGranted, response.voteGranted());
                assertEquals(expectedError, Errors.forCode(response.errorCode()));
                voteResponses.add((RaftResponse.Outbound) raftMessage);
            }
        }
        return voteResponses;
    }

    private int assertBeginQuorumEpochRequest(int epoch) {
        List<RaftMessage> sentMessages = channel.drainSendQueue();
        assertEquals(1, sentMessages.size());
        RaftMessage raftMessage = sentMessages.get(0);
        assertTrue(raftMessage.data() instanceof BeginQuorumEpochRequestData);
        BeginQuorumEpochRequestData request = (BeginQuorumEpochRequestData) raftMessage.data();
        assertEquals(epoch, request.leaderEpoch());
        assertEquals(localId, request.leaderId());
        return raftMessage.correlationId();
    }

    private int assertBeginQuorumEpochResponse(int epoch, int leaderId) {
        List<RaftMessage> sentMessages = channel.drainSendQueue();
        assertEquals(1, sentMessages.size());
        RaftMessage raftMessage = sentMessages.get(0);
        assertTrue(raftMessage.data() instanceof BeginQuorumEpochResponseData);
        BeginQuorumEpochResponseData response = (BeginQuorumEpochResponseData) raftMessage.data();
        assertEquals(epoch, response.leaderEpoch());
        assertEquals(leaderId, response.leaderId());
        return raftMessage.correlationId();
    }

    private int assertSentFetchQuorumRecordsRequest(
        int epoch,
        long fetchOffset,
        int lastFetchedEpoch
    ) {
        List<RaftMessage> sentMessages = channel.drainSendQueue();
        assertEquals(1, sentMessages.size());
        RaftMessage raftMessage = sentMessages.get(0);
        assertTrue("Unexpected request type " + raftMessage.data(),
            raftMessage.data() instanceof FetchQuorumRecordsRequestData);
        FetchQuorumRecordsRequestData request = (FetchQuorumRecordsRequestData) raftMessage.data();
        assertEquals(epoch, request.leaderEpoch());
        assertEquals(fetchOffset, request.fetchOffset());
        assertEquals(lastFetchedEpoch, request.lastFetchedEpoch());
        assertEquals(localId, request.replicaId());
        return raftMessage.correlationId();
    }

    private RaftMessage getSentMessageByType(List<RaftMessage> messages, ApiKeys key) {
        for (RaftMessage message : messages) {
            if (ApiKeys.forId(message.data().apiKey()) == key) {
                return message;
            }
        }
        fail("Didn't find expected message type " + key);
        return null;
    }

    private FetchQuorumRecordsResponseData fetchRecordsResponse(
        int epoch,
        int leaderId,
        Records records,
        long highWatermark,
        Errors error
    ) throws IOException {
        return new FetchQuorumRecordsResponseData()
                .setErrorCode(error.code())
                .setHighWatermark(highWatermark)
                .setLeaderEpoch(epoch)
                .setLeaderId(leaderId)
                .setRecords(RaftUtil.serializeRecords(records));
    }

    private FetchQuorumRecordsResponseData outOfRangeFetchRecordsResponse(
        int epoch,
        int leaderId,
        long nextFetchOffset,
        int nextFetchEpoch,
        long highWatermark
    ) {
        return new FetchQuorumRecordsResponseData()
            .setErrorCode(Errors.NONE.code())
            .setHighWatermark(highWatermark)
            .setNextFetchOffset(nextFetchOffset)
            .setNextFetchOffsetEpoch(nextFetchEpoch)
            .setLeaderEpoch(epoch)
            .setLeaderId(leaderId)
            .setRecords(ByteBuffer.wrap(new byte[0]));
    }

    private VoteResponseData voteResponse(boolean voteGranted, Optional<Integer> leaderId, int epoch) {
        return new VoteResponseData()
                .setVoteGranted(voteGranted)
                .setLeaderId(leaderId.orElse(-1))
                .setLeaderEpoch(epoch)
                .setErrorCode(Errors.NONE.code());
    }

    private VoteRequestData voteRequest(int epoch, int candidateId, int lastEpoch, long lastEpochOffset) {
        return new VoteRequestData()
                .setCandidateEpoch(epoch)
                .setCandidateId(candidateId)
                .setLastEpoch(lastEpoch)
                .setLastEpochEndOffset(lastEpochOffset);
    }

    private BeginQuorumEpochRequestData beginEpochRequest(int epoch, int leaderId) {
        return new BeginQuorumEpochRequestData()
            .setLeaderId(leaderId)
            .setLeaderEpoch(epoch);
    }

    private EndQuorumEpochRequestData endEpochRequest(int epoch, OptionalInt leaderId, int replicaId) {
        return new EndQuorumEpochRequestData()
            .setLeaderId(leaderId.orElse(-1))
            .setLeaderEpoch(epoch)
            .setReplicaId(replicaId);
    }

    private FindQuorumResponseData findQuorumResponse(OptionalInt leaderId, int epoch, Collection<Integer> voters) {
        return new FindQuorumResponseData()
                .setErrorCode(Errors.NONE.code())
                .setLeaderEpoch(epoch)
                .setLeaderId(leaderId.orElse(-1))
            .setVoters(voters.stream().map(voterId -> {
                InetSocketAddress address = mockAddress(voterId);
                return new FindQuorumResponseData.Voter()
                    .setVoterId(voterId)
                    .setBootTimestamp(0)
                    .setHost(address.getHostString())
                    .setPort(address.getPort());
            }).collect(Collectors.toList()));
    }

    private FindQuorumResponseData findQuorumFailure(Errors error) {
        return new FindQuorumResponseData()
                .setErrorCode(error.code())
                .setLeaderEpoch(-1)
                .setLeaderId(-1);
    }

    private FetchQuorumRecordsRequestData fetchQuorumRecordsRequest(
        int epoch,
        int replicaId,
        long fetchOffset,
        int lastFetchedEpoch) {
        return new FetchQuorumRecordsRequestData()
            .setLeaderEpoch(epoch)
            .setFetchOffset(fetchOffset)
            .setLastFetchedEpoch(lastFetchedEpoch)
            .setReplicaId(replicaId);
    }

    private void pollUntilSend(KafkaRaftClient client) throws InterruptedException {
        TestUtils.waitForCondition(() -> {
            client.poll();
            return channel.hasSentMessages();
        }, 5000, "Condition failed to be satisfied before timeout");
    }

}
