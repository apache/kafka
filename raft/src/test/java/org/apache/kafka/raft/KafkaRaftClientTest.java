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
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
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
import java.util.HashSet;
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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class KafkaRaftClientTest {
    private final int localId = 0;
    private final int electionTimeoutMs = 10000;
    private final int electionJitterMs = 100;
    private final int fetchTimeoutMs = 50000;   // fetch timeout is usually larger than election timeout
    private final int retryBackoffMs = 50;
    private final int requestTimeoutMs = 5000;
    private final int fetchMaxWaitMs = 0;
    private final MockTime time = new MockTime();
    private final MockLog log = new MockLog();
    private final MockNetworkChannel channel = new MockNetworkChannel();
    private final Random random = Mockito.spy(new Random());
    private final MockStateMachine stateMachine = new MockStateMachine();
    private final QuorumStateStore quorumStateStore = new MockQuorumStateStore();

    @After
    public void cleanUp() throws IOException {
        quorumStateStore.clear();
        stateMachine.close();
    }

    private InetSocketAddress mockAddress(int id) {
        return new InetSocketAddress("localhost", 9990 + id);
    }

    private KafkaRaftClient buildClient(Set<Integer> voters) throws IOException {
        return buildClient(voters, stateMachine);
    }

    private KafkaRaftClient buildClient(Set<Integer> voters, ReplicatedStateMachine stateMachine) throws IOException {
        return buildClient(voters, stateMachine, new Metrics(time));
    }

    private KafkaRaftClient buildClient(Set<Integer> voters, ReplicatedStateMachine stateMachine, Metrics metrics) throws IOException {
        LogContext logContext = new LogContext();
        QuorumState quorum = new QuorumState(localId, voters, quorumStateStore, logContext);

        List<InetSocketAddress> bootstrapServers = voters.stream()
            .map(this::mockAddress)
            .collect(Collectors.toList());

        KafkaRaftClient client = new KafkaRaftClient(channel, log, quorum, time, metrics,
            new MockFuturePurgatory<>(time), mockAddress(localId), bootstrapServers,
            electionTimeoutMs, electionJitterMs, fetchTimeoutMs, retryBackoffMs, requestTimeoutMs,
            fetchMaxWaitMs, logContext, random);
        client.initialize(stateMachine);
        return client;
    }

    @Test
    public void testInitializeSingleMemberQuorum() throws IOException {
        KafkaRaftClient client = buildClient(Collections.singleton(localId));
        assertEquals(ElectionState.withElectedLeader(1, localId, Collections.singleton(localId)), quorumStateStore.readElectionState());
        client.poll();
        assertEquals(0, channel.drainSendQueue().size());
    }

    @Test
    public void testInitializeAsLeaderFromStateStore() throws IOException {
        Set<Integer> voters = Utils.mkSet(localId, 1);
        int epoch = 2;

        quorumStateStore.writeElectionState(ElectionState.withElectedLeader(epoch, localId, voters));
        KafkaRaftClient client = buildClient(voters, stateMachine);
        assertTrue(stateMachine.isLeader());
        assertEquals(epoch, stateMachine.epoch());
        assertEquals(1L, log.endOffset());

        // Should have sent out connection info query for other node id.
        client.poll();

        assertSentFindQuorumRequest();
    }

    @Test
    public void testInitializeAsCandidateFromStateStore() throws Exception {
        // Need 3 node to require a 2-node majority
        Set<Integer> voters = Utils.mkSet(localId, 1, 2);
        quorumStateStore.writeElectionState(ElectionState.withVotedCandidate(2, localId, voters));

        KafkaRaftClient client = buildClient(voters);
        assertFalse(stateMachine.isLeader());
        assertEquals(0L, log.endOffset());

        initializeVoterConnections(client, voters, 1, OptionalInt.empty());

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
        assertEquals(ElectionState.withVotedCandidate(1, localId, voters), quorumStateStore.readElectionState());

        initializeVoterConnections(client, voters, 1, OptionalInt.empty());

        pollUntilSend(client);

        int correlationId = assertSentVoteRequest(1, 0, 0L);
        deliverResponse(correlationId, otherNodeId, voteResponse(true, Optional.empty(), 1));

        // Become leader after receiving the vote
        client.poll();
        assertEquals(ElectionState.withElectedLeader(1, localId, voters), quorumStateStore.readElectionState());

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
            votedCandidateEpoch, otherNodeId, voters));

        KafkaRaftClient client = buildClient(voters);

        deliverRequest(beginEpochRequest(votedCandidateEpoch, otherNodeId));

        client.poll();

        assertEquals(ElectionState.withElectedLeader(
            votedCandidateEpoch, otherNodeId, voters), quorumStateStore.readElectionState());

        assertSentBeginQuorumEpochResponse(Errors.NONE, votedCandidateEpoch, OptionalInt.of(otherNodeId));
    }

    @Test
    public void testHandleBeginQuorumResponse() throws Exception {
        int otherNodeId = 1;
        int leaderEpoch = 2;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);

        quorumStateStore.writeElectionState(ElectionState.withElectedLeader(leaderEpoch, localId, voters));

        KafkaRaftClient client = buildClient(voters);

        deliverRequest(beginEpochRequest(leaderEpoch + 1, otherNodeId));

        client.poll();

        assertEquals(ElectionState.withElectedLeader(leaderEpoch + 1, otherNodeId, voters),
            quorumStateStore.readElectionState());
    }

    @Test
    public void testEndQuorumIgnoredIfAlreadyCandidate() throws Exception {
        int otherNodeId = 1;
        int epoch = 2;

        int jitterMs = 85;
        Mockito.doReturn(jitterMs).when(random).nextInt(Mockito.anyInt());

        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        quorumStateStore.writeElectionState(ElectionState.withVotedCandidate(epoch, localId, voters));
        KafkaRaftClient client = buildClient(voters);

        deliverRequest(endEpochRequest(epoch, OptionalInt.empty(), otherNodeId, Collections.singletonList(localId)));

        client.poll();
        assertSentEndQuorumEpochResponse(Errors.NONE, epoch, OptionalInt.empty());

        // We should still be candidate until expiration of election timeout
        time.sleep(electionTimeoutMs + jitterMs - 1);
        client.poll();
        assertEquals(ElectionState.withVotedCandidate(epoch, localId, voters), quorumStateStore.readElectionState());

        time.sleep(1);
        client.poll();
        assertEquals(ElectionState.withVotedCandidate(epoch + 1, localId, voters), quorumStateStore.readElectionState());
    }

    @Test
    public void testEndQuorumIgnoredIfAlreadyLeader() throws Exception {
        int voter2 = localId + 1;
        int voter3 = localId + 2;
        int epoch = 2;

        int jitterMs = 85;
        Mockito.doReturn(jitterMs).when(random).nextInt(Mockito.anyInt());

        Set<Integer> voters = Utils.mkSet(localId, voter2, voter3);
        quorumStateStore.writeElectionState(ElectionState.withElectedLeader(epoch, localId, voters));
        KafkaRaftClient client = buildClient(voters);

        // One of the voters may have sent EndEpoch as a candidate because it
        // had not yet been notified that the local node was the leader.
        deliverRequest(endEpochRequest(epoch, OptionalInt.empty(), voter2, Arrays.asList(localId, voter3)));

        client.poll();
        assertSentEndQuorumEpochResponse(Errors.NONE, epoch, OptionalInt.of(localId));

        // We should still be leader as long as fetch timeout has not expired
        time.sleep(fetchTimeoutMs - 1);
        client.poll();
        assertEquals(ElectionState.withElectedLeader(epoch, localId, voters), quorumStateStore.readElectionState());
    }

    @Test
    public void testEndQuorumStartsNewElectionImmediatelyIfReceivedFromVotedCandidate() throws Exception {
        int otherNodeId = 1;
        int epoch = 2;

        int jitterMs = 85;
        Mockito.doReturn(jitterMs).when(random).nextInt(Mockito.anyInt());

        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        quorumStateStore.writeElectionState(ElectionState.withVotedCandidate(epoch, otherNodeId, voters));
        KafkaRaftClient client = buildClient(voters);

        deliverRequest(endEpochRequest(epoch, OptionalInt.empty(), otherNodeId, Collections.singletonList(localId)));

        client.poll();
        assertSentEndQuorumEpochResponse(Errors.NONE, epoch + 1, OptionalInt.empty());
        assertEquals(ElectionState.withVotedCandidate(epoch + 1, localId, voters), quorumStateStore.readElectionState());
    }

    @Test
    public void testEndQuorumStartsNewElectionImmediatelyIfFollowerUnattached() throws Exception {
        int voter2 = localId + 1;
        int voter3 = localId + 2;
        int epoch = 2;

        int jitterMs = 85;
        Mockito.doReturn(jitterMs).when(random).nextInt(Mockito.anyInt());

        Set<Integer> voters = Utils.mkSet(localId, voter2, voter3);
        quorumStateStore.writeElectionState(ElectionState.withUnknownLeader(epoch, voters));
        KafkaRaftClient client = buildClient(voters);

        deliverRequest(endEpochRequest(epoch, OptionalInt.of(voter2), voter2, Arrays.asList(localId, voter3)));

        client.poll();

        // Should become a candidate immediately
        assertSentEndQuorumEpochResponse(Errors.NONE, epoch + 1, OptionalInt.empty());
        assertEquals(ElectionState.withVotedCandidate(epoch + 1, localId, voters), quorumStateStore.readElectionState());
    }

    @Test
    public void testHandleEndQuorumRequest() throws Exception {
        int oldLeaderId = 1;
        int leaderEpoch = 2;
        Set<Integer> voters = Utils.mkSet(localId, oldLeaderId);

        quorumStateStore.writeElectionState(ElectionState.withElectedLeader(leaderEpoch, oldLeaderId, voters));

        KafkaRaftClient client = buildClient(voters);

        initializeVoterConnections(client, voters, leaderEpoch, OptionalInt.of(oldLeaderId));

        deliverRequest(endEpochRequest(leaderEpoch, OptionalInt.of(oldLeaderId), oldLeaderId, Collections.singletonList(localId)));
        client.poll();

        // Should have already done self-voting
        assertSentEndQuorumEpochResponse(Errors.NONE, leaderEpoch + 1, OptionalInt.empty());
        assertEquals(ElectionState.withVotedCandidate(leaderEpoch + 1, localId, voters),
            quorumStateStore.readElectionState());
    }

    @Test
    public void testHandleEndQuorumRequestWithLowerPriorityToBecomeLeader() throws Exception {
        int oldLeaderId = 1;
        int leaderEpoch = 2;
        int preferredNextLeader = 3;
        Set<Integer> voters = Utils.mkSet(localId, oldLeaderId, preferredNextLeader);

        quorumStateStore.writeElectionState(ElectionState.withElectedLeader(leaderEpoch, oldLeaderId, voters));

        KafkaRaftClient client = buildClient(voters);

        deliverRequest(endEpochRequest(leaderEpoch,
            OptionalInt.of(oldLeaderId), oldLeaderId, Arrays.asList(preferredNextLeader, localId)));

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

        deliverResponse(findQuorumCorrelationId, -1, findQuorumResponse(OptionalInt.of(localId), leaderEpoch, voters));

        // The election won't trigger by one round retry backoff
        time.sleep(1);

        pollUntilSend(client);

        assertSentFetchQuorumRecordsRequest(leaderEpoch, 0, 0);

        time.sleep(retryBackoffMs);

        pollUntilSend(client);

        List<RaftRequest.Outbound> voteRequests = collectVoteRequests(leaderEpoch + 1, 0, 0);
        assertEquals(2, voteRequests.size());

        // Should have already done self-voting
        assertEquals(ElectionState.withVotedCandidate(leaderEpoch + 1, localId, voters),
            quorumStateStore.readElectionState());
    }

    @Test
    public void testVoteRequestTimeout() throws Exception {
        int epoch = 1;
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        KafkaRaftClient client = buildClient(voters);
        assertEquals(ElectionState.withVotedCandidate(epoch, localId, voters), quorumStateStore.readElectionState());

        initializeVoterConnections(client, voters, epoch, OptionalInt.empty());

        pollUntilSend(client);

        int correlationId = assertSentVoteRequest(epoch, 0, 0L);

        time.sleep(requestTimeoutMs);
        client.poll();
        int retryCorrelationId = assertSentVoteRequest(epoch, 0, 0L);

        // Even though we have resent the request, we should still accept the response to
        // the first request if it arrives late.
        deliverResponse(correlationId, otherNodeId, voteResponse(true, Optional.empty(), 1));
        client.poll();
        assertEquals(ElectionState.withElectedLeader(epoch, localId, voters), quorumStateStore.readElectionState());

        // If the second request arrives later, it should have no effect
        deliverResponse(retryCorrelationId, otherNodeId, voteResponse(true, Optional.empty(), 1));
        client.poll();
        assertEquals(ElectionState.withElectedLeader(epoch, localId, voters), quorumStateStore.readElectionState());
    }

    @Test
    public void testHandleValidVoteRequestAsFollower() throws Exception {
        int epoch = 2;
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);

        quorumStateStore.writeElectionState(ElectionState.withUnknownLeader(epoch, voters));

        KafkaRaftClient client = buildClient(voters);

        deliverRequest(voteRequest(epoch, otherNodeId, epoch - 1, 1));

        client.poll();

        assertSentVoteResponse(Errors.NONE, epoch, OptionalInt.empty(), true);

        assertEquals(ElectionState.withVotedCandidate(epoch, otherNodeId, voters),
            quorumStateStore.readElectionState());
    }

    @Test
    public void testHandleVoteRequestAsFollowerWithElectedLeader() throws Exception {
        int epoch = 2;
        int otherNodeId = 1;
        int electedLeaderId = 3;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId, electedLeaderId);

        quorumStateStore.writeElectionState(ElectionState.withElectedLeader(epoch, electedLeaderId, voters));

        KafkaRaftClient client = buildClient(voters);

        deliverRequest(voteRequest(epoch, otherNodeId, epoch - 1, 1));

        client.poll();

        assertSentVoteResponse(Errors.NONE, epoch, OptionalInt.of(electedLeaderId), false);

        assertEquals(ElectionState.withElectedLeader(epoch, electedLeaderId, voters),
            quorumStateStore.readElectionState());
    }

    @Test
    public void testHandleVoteRequestAsFollowerWithVotedCandidate() throws Exception {
        int epoch = 2;
        int otherNodeId = 1;
        int votedCandidateId = 3;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId, votedCandidateId);

        quorumStateStore.writeElectionState(ElectionState.withVotedCandidate(epoch, votedCandidateId, voters));

        KafkaRaftClient client = buildClient(voters);

        deliverRequest(voteRequest(epoch, otherNodeId, epoch - 1, 1));

        client.poll();

        assertSentVoteResponse(Errors.NONE, epoch, OptionalInt.empty(), false);
        assertEquals(ElectionState.withVotedCandidate(epoch, votedCandidateId, voters),
            quorumStateStore.readElectionState());
    }

    @Test
    public void testHandleInvalidVoteRequestWithOlderEpoch() throws Exception {
        int epoch = 2;
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);

        quorumStateStore.writeElectionState(ElectionState.withUnknownLeader(epoch, voters));

        KafkaRaftClient client = buildClient(voters);
        deliverRequest(voteRequest(epoch - 1, otherNodeId, epoch - 2, 1));

        client.poll();

        assertSentVoteResponse(Errors.FENCED_LEADER_EPOCH, epoch, OptionalInt.empty(), false);
        assertEquals(ElectionState.withUnknownLeader(epoch, voters), quorumStateStore.readElectionState());
    }

    @Test
    public void testHandleInvalidVoteRequestAsObserver() throws Exception {
        int epoch = 2;
        int otherNodeId = 1;
        int otherNodeId2 = 2;
        Set<Integer> voters = Utils.mkSet(otherNodeId, otherNodeId2);

        quorumStateStore.writeElectionState(ElectionState.withUnknownLeader(epoch, voters));

        KafkaRaftClient client = buildClient(voters);

        deliverRequest(voteRequest(epoch + 1, otherNodeId, epoch, 1));

        client.poll();

        assertSentVoteResponse(Errors.INCONSISTENT_VOTER_SET, epoch, OptionalInt.empty(), false);
        assertEquals(ElectionState.withUnknownLeader(epoch, voters), quorumStateStore.readElectionState());
    }

    @Test
    public void testLeaderIgnoreVoteRequestOnSameEpoch() throws Exception {
        int otherNodeId = 1;
        int leaderEpoch = 2;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);

        quorumStateStore.writeElectionState(ElectionState.withElectedLeader(leaderEpoch, localId, voters));

        KafkaRaftClient client = buildClient(voters);

        pollUntilSend(client);
        assertSentFindQuorumRequest();

        deliverRequest(voteRequest(leaderEpoch, otherNodeId, leaderEpoch - 1, 1));

        client.poll();

        assertSentVoteResponse(Errors.NONE, leaderEpoch, OptionalInt.of(localId), false);
        assertEquals(ElectionState.withElectedLeader(leaderEpoch, localId, voters),
            quorumStateStore.readElectionState());
    }

    @Test
    public void testStateMachineApplyCommittedRecords() throws Exception {
        int otherNodeId = 1;
        int epoch = 5;

        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        KafkaRaftClient client = initializeAsLeader(voters, epoch, stateMachine);

        // First poll has no high watermark advance
        client.poll();
        assertEquals(OptionalLong.empty(), client.highWatermark());

        // Let follower send a fetch to initialize the high watermark,
        // note the offset 0 would be a control message for becoming the leader
        deliverRequest(fetchQuorumRecordsRequest(epoch, otherNodeId, 0L, epoch, 500));
        pollUntilSend(client);
        assertEquals(OptionalLong.of(0L), client.highWatermark());

        // Append some records
        SimpleRecord[] appendRecords = new SimpleRecord[] {
            new SimpleRecord("a".getBytes()),
            new SimpleRecord("b".getBytes()),
            new SimpleRecord("c".getBytes())
        };
        Records records = MemoryRecords.withRecords(0L, CompressionType.NONE, 1, appendRecords);
        CompletableFuture<OffsetAndEpoch> future = stateMachine.append(records);

        client.poll();
        assertTrue(future.isDone());
        assertEquals(new OffsetAndEpoch(3, epoch), future.get());

        // Let follower send a fetch, it should advance the high watermark
        deliverRequest(fetchQuorumRecordsRequest(epoch, otherNodeId, 1L, epoch, 500));
        pollUntilSend(client);
        assertEquals(OptionalLong.of(1L), client.highWatermark());
        assertEquals(new OffsetAndEpoch(1, epoch), stateMachine.position());

        // Let the follower to send another fetch from offset 2
        deliverRequest(fetchQuorumRecordsRequest(epoch, otherNodeId, 2L, epoch, 500));
        pollUntilSend(client);
        assertEquals(OptionalLong.of(2L), client.highWatermark());
        assertEquals(new OffsetAndEpoch(2, epoch), stateMachine.position());

        // Let the follower to send another fetch from offset 4, only then the append future can be satisified
        deliverRequest(fetchQuorumRecordsRequest(epoch, otherNodeId, 4L, epoch, 500));
        client.poll();
        assertEquals(OptionalLong.of(4L), client.highWatermark());
        assertEquals(new OffsetAndEpoch(4, epoch), stateMachine.position());

    }

    @Test
    public void testCandidateIgnoreVoteRequestOnSameEpoch() throws Exception {
        int otherNodeId = 1;
        int leaderEpoch = 2;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        quorumStateStore.writeElectionState(ElectionState.withVotedCandidate(leaderEpoch, localId, voters));
        KafkaRaftClient client = buildClient(voters);

        pollUntilSend(client);
        assertSentFindQuorumRequest();

        deliverRequest(voteRequest(leaderEpoch, otherNodeId, leaderEpoch - 1, 1));
        client.poll();
        assertSentVoteResponse(Errors.NONE, leaderEpoch, OptionalInt.empty(), false);
        assertEquals(ElectionState.withVotedCandidate(leaderEpoch, localId, voters),
            quorumStateStore.readElectionState());
    }

    @Test
    public void testRetryElection() throws Exception {
        int otherNodeId = 1;
        int epoch = 1;

        int jitterMs = 85;
        Mockito.doReturn(jitterMs).when(random).nextInt(Mockito.anyInt());

        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);

        KafkaRaftClient client = buildClient(voters);
        assertEquals(ElectionState.withVotedCandidate(epoch, localId, voters), quorumStateStore.readElectionState());

        initializeVoterConnections(client, voters, epoch, OptionalInt.empty());

        pollUntilSend(client);

        // Quorum size is two. If the other member rejects, then we need to schedule a revote.
        int correlationId = assertSentVoteRequest(epoch, 0, 0L);
        deliverResponse(correlationId, otherNodeId, voteResponse(false, Optional.empty(), 1));

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
        assertEquals(ElectionState.withVotedCandidate(epoch + 1, localId, voters), quorumStateStore.readElectionState());
        assertSentVoteRequest(epoch + 1, 0, 0L);
    }

    @Test
    public void testInitializeAsFollowerEmptyLog() throws Exception {
        int otherNodeId = 1;
        int epoch = 5;

        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        quorumStateStore.writeElectionState(ElectionState.withElectedLeader(epoch, otherNodeId, voters));

        KafkaRaftClient client = buildClient(voters);

        assertEquals(ElectionState.withElectedLeader(epoch, otherNodeId, voters), quorumStateStore.readElectionState());

        initializeVoterConnections(client, voters, epoch, OptionalInt.of(otherNodeId));

        pollUntilSend(client);

        assertSentFetchQuorumRecordsRequest(epoch, 0L, 0);
    }

    @Test
    public void testInitializeAsFollowerNonEmptyLog() throws Exception {
        int otherNodeId = 1;
        int epoch = 5;
        int lastEpoch = 3;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        quorumStateStore.writeElectionState(ElectionState.withElectedLeader(epoch, otherNodeId, voters));

        log.appendAsLeader(Collections.singleton(new SimpleRecord("foo".getBytes())), lastEpoch);

        KafkaRaftClient client = buildClient(voters);
        assertEquals(ElectionState.withElectedLeader(epoch, otherNodeId, voters), quorumStateStore.readElectionState());

        pollUntilSend(client);

        initializeVoterConnections(client, voters, epoch, OptionalInt.of(otherNodeId));

        pollUntilSend(client);

        assertSentFetchQuorumRecordsRequest(epoch, 1L, lastEpoch);
    }

    @Test
    public void testVoterBecomeCandidateAfterFetchTimeout() throws Exception {
        int otherNodeId = 1;
        int epoch = 5;
        int lastEpoch = 3;

        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        quorumStateStore.writeElectionState(ElectionState.withElectedLeader(epoch, otherNodeId, voters));

        log.appendAsLeader(Collections.singleton(new SimpleRecord("foo".getBytes())), lastEpoch);

        KafkaRaftClient client = buildClient(voters);
        assertEquals(ElectionState.withElectedLeader(epoch, otherNodeId, voters), quorumStateStore.readElectionState());

        initializeVoterConnections(client, voters, epoch, OptionalInt.of(otherNodeId));

        pollUntilSend(client);

        assertSentFetchQuorumRecordsRequest(epoch, 1L, lastEpoch);

        time.sleep(fetchTimeoutMs);

        client.poll();
        assertSentVoteRequest(epoch + 1, lastEpoch, 1L);
        assertEquals(ElectionState.withVotedCandidate(epoch + 1, localId, voters), quorumStateStore.readElectionState());
    }

    @Test
    public void testInitializeObserverNoPreviousState() throws Exception {
        int leaderId = 1;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(leaderId);
        KafkaRaftClient client = buildClient(voters);

        initializeVoterConnections(client, voters, epoch, OptionalInt.of(leaderId));

        client.poll();
        assertEquals(ElectionState.withElectedLeader(epoch, leaderId, voters), quorumStateStore.readElectionState());
    }

    @Test
    public void testObserverFindQuorumFailure() throws IOException {
        int leaderId = 1;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(leaderId);
        KafkaRaftClient client = buildClient(voters);

        client.poll();
        RaftRequest.Outbound findQuorumRequest = assertSentFindQuorumRequest();
        deliverResponse(findQuorumRequest.correlationId, findQuorumRequest.destinationId(),
            findQuorumFailure(Errors.UNKNOWN_SERVER_ERROR));

        client.poll();
        assertEquals(0, channel.drainSendQueue().size());

        time.sleep(retryBackoffMs);

        client.poll();
        RaftRequest.Outbound retryFindQuorumRequest = assertSentFindQuorumRequest();
        deliverResponse(retryFindQuorumRequest.correlationId, retryFindQuorumRequest.destinationId(),
            findQuorumResponse(OptionalInt.of(leaderId), epoch, voters));

        client.poll();
        assertEquals(ElectionState.withElectedLeader(epoch, leaderId, voters), quorumStateStore.readElectionState());
    }

    @Test
    public void testLeaderFindQuorumAfterMajorityFetchTimeout() throws Exception {
        int epoch = 1;
        Set<Integer> voters = Utils.mkSet(localId, 1, 2, 3, 4);
        quorumStateStore.writeElectionState(ElectionState.withElectedLeader(epoch, localId, voters));
        KafkaRaftClient client = initializeAsLeader(voters, epoch, stateMachine);
        assertNoSentMessages();

        time.sleep(1L);
        deliverRequest(fetchQuorumRecordsRequest(epoch, 1, 0L, epoch, 0));
        client.poll();
        assertSentFetchQuorumRecordsResponse(Errors.NONE, epoch, OptionalInt.of(localId));

        time.sleep(1L);
        deliverRequest(fetchQuorumRecordsRequest(epoch, 2, 0L, epoch, 0));
        client.poll();
        assertSentFetchQuorumRecordsResponse(Errors.NONE, epoch, OptionalInt.of(localId));

        time.sleep(1L);
        deliverRequest(fetchQuorumRecordsRequest(epoch, 3, 0L, epoch, 0));
        client.poll();
        assertSentFetchQuorumRecordsResponse(Errors.NONE, epoch, OptionalInt.of(localId));

        time.sleep(1L);
        deliverRequest(fetchQuorumRecordsRequest(epoch, 4, 0L, epoch, 0));
        client.poll();
        assertSentFetchQuorumRecordsResponse(Errors.NONE, epoch, OptionalInt.of(localId));

        // we have majority of quorum sent fetch, so should not try to find-quorum
        time.sleep(fetchTimeoutMs - 3);

        client.poll();
        assertNoSentMessages();

        time.sleep(1L);
        client.poll();
        assertSentFindQuorumRequest();
    }

    @Test
    public void testObserverFindQuorumAfterFetchTimeout() throws Exception {
        int leaderId = 1;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(leaderId);
        KafkaRaftClient client = buildClient(voters);

        initializeVoterConnections(client, voters, epoch, OptionalInt.of(leaderId));

        client.poll();
        assertEquals(ElectionState.withElectedLeader(epoch, leaderId, voters), quorumStateStore.readElectionState());

        time.sleep(fetchTimeoutMs);

        client.poll();
        assertSentFindQuorumRequest();
    }

    @Test
    public void testInvalidFetchRequest() throws Exception {
        int otherNodeId = 1;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        KafkaRaftClient client = initializeAsLeader(voters, epoch, stateMachine);

        deliverRequest(fetchQuorumRecordsRequest(
            epoch, otherNodeId, -5L, 0, 0));
        client.poll();
        assertSentFetchQuorumRecordsResponse(Errors.INVALID_REQUEST, epoch, OptionalInt.of(localId));

        deliverRequest(fetchQuorumRecordsRequest(
            epoch, otherNodeId, 0L, -1, 0));
        client.poll();
        assertSentFetchQuorumRecordsResponse(Errors.INVALID_REQUEST, epoch, OptionalInt.of(localId));

        deliverRequest(fetchQuorumRecordsRequest(
            epoch, otherNodeId, 0L, epoch + 1, 0));
        client.poll();
        assertSentFetchQuorumRecordsResponse(Errors.INVALID_REQUEST, epoch, OptionalInt.of(localId));

        deliverRequest(fetchQuorumRecordsRequest(
            epoch + 1, otherNodeId, 0L, 0, 0));
        client.poll();
        assertSentFetchQuorumRecordsResponse(Errors.INVALID_REQUEST, epoch, OptionalInt.of(localId));

        deliverRequest(fetchQuorumRecordsRequest(
            epoch, otherNodeId, 0L, 0, -1));
        client.poll();
        assertSentFetchQuorumRecordsResponse(Errors.INVALID_REQUEST, epoch, OptionalInt.of(localId));
    }

    @Test
    public void testVoterOnlyRequestValidation() throws Exception {
        int otherNodeId = 1;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        KafkaRaftClient client = initializeAsLeader(voters, epoch, stateMachine);

        int nonVoterId = 2;
        deliverRequest(voteRequest(epoch, nonVoterId, 0, 0));
        client.poll();
        assertSentVoteResponse(Errors.INCONSISTENT_VOTER_SET, epoch, OptionalInt.of(localId), false);

        deliverRequest(beginEpochRequest(epoch, nonVoterId));
        client.poll();
        assertSentBeginQuorumEpochResponse(Errors.INCONSISTENT_VOTER_SET, epoch, OptionalInt.of(localId));

        deliverRequest(endEpochRequest(epoch, OptionalInt.of(localId), nonVoterId, Collections.singletonList(otherNodeId)));
        client.poll();

        // The sent request has no localId as a preferable voter.
        assertSentEndQuorumEpochResponse(Errors.INCONSISTENT_VOTER_SET, epoch, OptionalInt.of(localId));
    }

    @Test
    public void testInvalidVoteRequest() throws Exception {
        int otherNodeId = 1;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);

        quorumStateStore.writeElectionState(ElectionState.withElectedLeader(epoch, otherNodeId, voters));
        KafkaRaftClient client = buildClient(voters);
        assertEquals(ElectionState.withElectedLeader(epoch, otherNodeId, voters), quorumStateStore.readElectionState());

        initializeVoterConnections(client, voters, epoch, OptionalInt.empty());

        deliverRequest(voteRequest(epoch + 1, otherNodeId, 0, -5L));
        client.poll();
        assertSentVoteResponse(Errors.INVALID_REQUEST, epoch, OptionalInt.of(otherNodeId), false);
        assertEquals(ElectionState.withElectedLeader(epoch, otherNodeId, voters), quorumStateStore.readElectionState());

        deliverRequest(voteRequest(epoch + 1, otherNodeId, -1, 0L));
        client.poll();
        assertSentVoteResponse(Errors.INVALID_REQUEST, epoch, OptionalInt.of(otherNodeId), false);
        assertEquals(ElectionState.withElectedLeader(epoch, otherNodeId, voters), quorumStateStore.readElectionState());

        deliverRequest(voteRequest(epoch + 1, otherNodeId, epoch + 1, 0L));
        client.poll();
        assertSentVoteResponse(Errors.INVALID_REQUEST, epoch, OptionalInt.of(otherNodeId), false);
        assertEquals(ElectionState.withElectedLeader(epoch, otherNodeId, voters), quorumStateStore.readElectionState());
    }

    @Test
    public void testPurgatoryFetchTimeout() throws Exception {
        int otherNodeId = 1;
        int epoch = 5;

        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        KafkaRaftClient client = initializeAsLeader(voters, epoch, stateMachine);

        // Follower sends a fetch which cannot be satisfied immediately
        int maxWaitTimeMs = 500;
        deliverRequest(fetchQuorumRecordsRequest(epoch, otherNodeId, 1L, epoch, maxWaitTimeMs));
        client.poll();
        assertEquals(0, channel.drainSendQueue().size());

        // After expiration of the max wait time, the fetch returns an empty record set
        time.sleep(maxWaitTimeMs);
        client.poll();
        MemoryRecords fetchedRecords = assertSentFetchQuorumRecordsResponse(Errors.NONE, epoch, OptionalInt.of(localId));
        assertEquals(0, fetchedRecords.sizeInBytes());
    }

    @Test
    public void testPurgatoryFetchSatisfiedByWrite() throws Exception {
        int otherNodeId = 1;
        int epoch = 5;

        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        KafkaRaftClient client = initializeAsLeader(voters, epoch, stateMachine);

        // Follower sends a fetch which cannot be satisfied immediately
        deliverRequest(fetchQuorumRecordsRequest(epoch, otherNodeId, 1L, epoch, 500));
        client.poll();
        assertEquals(0, channel.drainSendQueue().size());

        // Append some records that can fulfill the Fetch request
        SimpleRecord[] appendRecords = new SimpleRecord[] {
            new SimpleRecord("a".getBytes()),
            new SimpleRecord("b".getBytes()),
            new SimpleRecord("c".getBytes())
        };
        Records records = MemoryRecords.withRecords(0L, CompressionType.NONE, 1, appendRecords);
        CompletableFuture<OffsetAndEpoch> future = stateMachine.append(records);
        client.poll();
        assertTrue(future.isDone());

        MemoryRecords fetchedRecords = assertSentFetchQuorumRecordsResponse(Errors.NONE, epoch, OptionalInt.of(localId));
        List<Record> recordList = Utils.toList(fetchedRecords.records());
        assertEquals(appendRecords.length, recordList.size());
        for (int i = 0; i < appendRecords.length; i++) {
            assertEquals(appendRecords[i], new SimpleRecord(recordList.get(i)));
        }
    }

    @Test
    public void testPurgatoryFetchCompletedByFollowerTransition() throws Exception {
        int voter1 = localId;
        int voter2 = localId + 1;
        int voter3 = localId + 2;
        int epoch = 5;

        Set<Integer> voters = Utils.mkSet(voter1, voter2, voter3);
        KafkaRaftClient client = initializeAsLeader(voters, epoch, stateMachine);

        // Follower sends a fetch which cannot be satisfied immediately
        deliverRequest(fetchQuorumRecordsRequest(epoch, voter2, 1L, epoch, 500));
        client.poll();
        assertTrue(channel.drainSendQueue().stream()
            .noneMatch(msg -> msg.data() instanceof FetchQuorumRecordsResponseData));

        // Now we get a BeginEpoch from the other voter and become a follower
        deliverRequest(beginEpochRequest(epoch + 1, voter3));
        client.poll();
        assertEquals(ElectionState.withElectedLeader(epoch + 1, voter3, voters), quorumStateStore.readElectionState());

        // We expect the BeginQuorumEpoch response and a failed FetchQuorum response
        assertSentBeginQuorumEpochResponse(Errors.NONE, epoch + 1, OptionalInt.of(voter3));

        // The fetch should be satisfied immediately and return an error
        MemoryRecords fetchedRecords = assertSentFetchQuorumRecordsResponse(
            Errors.FENCED_LEADER_EPOCH, epoch + 1, OptionalInt.of(voter3));
        assertEquals(0, fetchedRecords.sizeInBytes());
    }

    private void initializeVoterConnections(
        KafkaRaftClient client,
        Set<Integer> voters,
        int epoch,
        OptionalInt leaderId
    ) throws Exception {
        pollUntilSend(client);
        RaftRequest.Outbound findQuorumRequest = assertSentFindQuorumRequest();
        deliverResponse(findQuorumRequest.correlationId, findQuorumRequest.destinationId(),
            findQuorumResponse(leaderId, epoch, voters));
    }

    private KafkaRaftClient initializeAsLeader(Set<Integer> voters, int epoch, MockStateMachine stateMachine) throws Exception {
        ElectionState leaderElectionState = ElectionState.withElectedLeader(epoch, localId, voters);
        quorumStateStore.writeElectionState(leaderElectionState);
        KafkaRaftClient client = buildClient(voters, stateMachine);
        assertEquals(leaderElectionState, quorumStateStore.readElectionState());

        initializeVoterConnections(client, voters, epoch, OptionalInt.of(localId));

        // Handle BeginEpoch
        pollUntilSend(client);
        for (RaftRequest.Outbound request : collectBeginEpochRequests(epoch)) {
            BeginQuorumEpochResponseData beginEpochResponse = beginEpochResponse(epoch, localId);
            deliverResponse(request.correlationId, request.destinationId(), beginEpochResponse);
        }
        client.poll();
        return client;
    }

    @Test
    public void testFetchResponseIgnoredAfterBecomingCandidate() throws Exception {
        int otherNodeId = 1;
        int epoch = 5;

        // The other node starts out as the leader
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        quorumStateStore.writeElectionState(ElectionState.withElectedLeader(epoch, otherNodeId, voters));
        KafkaRaftClient client = buildClient(voters);
        assertEquals(ElectionState.withElectedLeader(epoch, otherNodeId, voters), quorumStateStore.readElectionState());

        initializeVoterConnections(client, voters, epoch, OptionalInt.empty());

        // Wait until we have a Fetch inflight to the leader
        pollUntilSend(client);
        int fetchCorrelationId = assertSentFetchQuorumRecordsRequest(epoch, 0L, 0);

        // Now await the fetch timeout and become a candidate
        time.sleep(fetchTimeoutMs);
        client.poll();
        assertEquals(ElectionState.withVotedCandidate(epoch + 1, localId, voters), quorumStateStore.readElectionState());

        // The fetch response from the old leader returns, but it should be ignored
        Records records = MemoryRecords.withRecords(0L, CompressionType.NONE,
            3, new SimpleRecord("a".getBytes()), new SimpleRecord("b".getBytes()));
        deliverResponse(fetchCorrelationId, otherNodeId,
            fetchRecordsResponse(epoch, otherNodeId, records, 0L, Errors.NONE));

        client.poll();
        assertEquals(0, log.endOffset());
        assertEquals(ElectionState.withVotedCandidate(epoch + 1, localId, voters), quorumStateStore.readElectionState());
    }

    @Test
    public void testFetchResponseIgnoredAfterBecomingFollowerOfDifferentLeader() throws Exception {
        int voter1 = localId;
        int voter2 = localId + 1;
        int voter3 = localId + 2;
        int epoch = 5;

        // Start out with `voter2` as the leader
        Set<Integer> voters = Utils.mkSet(voter1, voter2, voter3);
        quorumStateStore.writeElectionState(ElectionState.withElectedLeader(epoch, voter2, voters));
        KafkaRaftClient client = buildClient(voters);
        assertEquals(ElectionState.withElectedLeader(epoch, voter2, voters), quorumStateStore.readElectionState());

        initializeVoterConnections(client, voters, epoch, OptionalInt.empty());

        // Wait until we have a Fetch inflight to the leader
        pollUntilSend(client);
        int fetchCorrelationId = assertSentFetchQuorumRecordsRequest(epoch, 0L, 0);

        // Now receive a BeginEpoch from `voter3`
        deliverRequest(beginEpochRequest(epoch + 1, voter3));
        client.poll();
        assertEquals(ElectionState.withElectedLeader(epoch + 1, voter3, voters), quorumStateStore.readElectionState());

        // The fetch response from the old leader returns, but it should be ignored
        Records records = MemoryRecords.withRecords(0L, CompressionType.NONE,
            3, new SimpleRecord("a".getBytes()), new SimpleRecord("b".getBytes()));
        FetchQuorumRecordsResponseData response = fetchRecordsResponse(epoch, voter2, records, 0L, Errors.NONE);
        deliverResponse(fetchCorrelationId, voter2, response);

        client.poll();
        assertEquals(0, log.endOffset());
        assertEquals(ElectionState.withElectedLeader(epoch + 1, voter3, voters), quorumStateStore.readElectionState());
    }

    @Test
    public void testVoteResponseIgnoredAfterBecomingFollower() throws Exception {
        int voter1 = localId;
        int voter2 = localId + 1;
        int voter3 = localId + 2;
        int epoch = 5;

        // This node initializes as a candidate
        Set<Integer> voters = Utils.mkSet(voter1, voter2, voter3);
        quorumStateStore.writeElectionState(ElectionState.withVotedCandidate(epoch, voter1, voters));

        KafkaRaftClient client = buildClient(voters);
        assertEquals(ElectionState.withVotedCandidate(epoch, voter1, voters), quorumStateStore.readElectionState());

        initializeVoterConnections(client, voters, epoch, OptionalInt.empty());

        // Wait until the vote requests are inflight
        pollUntilSend(client);
        List<RaftRequest.Outbound> voteRequests = collectVoteRequests(epoch, 0, 0);
        assertEquals(2, voteRequests.size());

        // While the vote requests are still inflight, we receive a BeginEpoch for the same epoch
        deliverRequest(beginEpochRequest(epoch, voter3));
        client.poll();
        assertEquals(ElectionState.withElectedLeader(epoch, voter3, voters), quorumStateStore.readElectionState());

        // The vote requests now return and should be ignored
        VoteResponseData voteResponse1 = voteResponse(false, Optional.empty(), epoch);
        deliverResponse(voteRequests.get(0).correlationId, voter2, voteResponse1);

        VoteResponseData voteResponse2 = voteResponse(false, Optional.of(voter3), epoch);
        deliverResponse(voteRequests.get(1).correlationId, voter3, voteResponse2);

        client.poll();
        assertEquals(ElectionState.withElectedLeader(epoch, voter3, voters), quorumStateStore.readElectionState());
    }

    @Test
    public void testObserverLeaderRediscoveryAfterBrokerNotAvailableError() throws Exception {
        int leaderId = 1;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(leaderId);
        KafkaRaftClient client = buildClient(voters);

        initializeVoterConnections(client, voters, epoch, OptionalInt.of(leaderId));

        client.poll();
        assertEquals(ElectionState.withElectedLeader(epoch, leaderId, voters), quorumStateStore.readElectionState());

        client.poll();
        int fetchCorrelationId = assertSentFetchQuorumRecordsRequest(epoch, 0L, 0);

        FetchQuorumRecordsResponseData response = fetchRecordsResponse(
            epoch, leaderId, MemoryRecords.EMPTY, 0L, Errors.BROKER_NOT_AVAILABLE);
        deliverResponse(fetchCorrelationId, leaderId, response);
        client.poll();

        assertEquals(ElectionState.withUnknownLeader(epoch, voters), quorumStateStore.readElectionState());
        time.sleep(retryBackoffMs);

        client.poll();
        assertSentFindQuorumRequest();
    }

    @Test
    public void testObserverLeaderRediscoveryAfterRequestTimeout() throws Exception {
        int leaderId = 1;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(leaderId);
        KafkaRaftClient client = buildClient(voters);

        initializeVoterConnections(client, voters, epoch, OptionalInt.of(leaderId));

        pollUntilSend(client);
        assertEquals(ElectionState.withElectedLeader(epoch, leaderId, voters), quorumStateStore.readElectionState());
        assertSentFetchQuorumRecordsRequest(epoch, 0L, 0);

        time.sleep(requestTimeoutMs);
        client.poll();

        assertEquals(ElectionState.withUnknownLeader(epoch, voters), quorumStateStore.readElectionState());
        client.poll();
        assertSentFindQuorumRequest();
    }

    @Test
    public void testLeaderHandlesFindQuorum() throws IOException {
        KafkaRaftClient client = buildClient(Collections.singleton(localId));
        assertEquals(ElectionState.withElectedLeader(
            1, localId, Collections.singleton(localId)), quorumStateStore.readElectionState());

        int observerId = 1;
        deliverRequest(new FindQuorumRequestData().setReplicaId(observerId));

        client.poll();
        assertSentFindQuorumResponse(Errors.NONE, 1, Optional.of(localId));
    }

    @Test
    public void testLeaderGracefulShutdown() throws Exception {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        KafkaRaftClient client = buildClient(voters);

        // Elect ourselves as the leader
        assertEquals(ElectionState.withVotedCandidate(1, localId, voters), quorumStateStore.readElectionState());

        pollUntilSend(client);

        initializeVoterConnections(client, voters, 1, OptionalInt.empty());

        pollUntilSend(client);

        int voteCorrelationId = assertSentVoteRequest(1, 0, 0L);
        VoteResponseData voteResponse = voteResponse(true, Optional.empty(), 1);
        deliverResponse(voteCorrelationId, otherNodeId, voteResponse);
        client.poll();
        assertEquals(ElectionState.withElectedLeader(1, localId, voters), quorumStateStore.readElectionState());

        // Now shutdown
        int shutdownTimeoutMs = 5000;
        client.shutdown(shutdownTimeoutMs);

        // We should still be running until we have had a chance to send EndQuorumEpoch
        assertTrue(client.isRunning());

        // Send EndQuorumEpoch request to the other vote
        client.poll();
        assertTrue(client.isRunning());

        assertSentEndQuorumEpochRequest(1, OptionalInt.of(localId), otherNodeId);

        // Graceful shutdown completes when the epoch is bumped
        deliverRequest(voteRequest(2, otherNodeId, 0, 0L));

        client.poll();
        assertFalse(client.isRunning());
    }

    @Test
    public void testEndQuorumEpochSentBasedOnFetchOffset() throws Exception {
        int closeFollower = 2;
        int laggingFollower = 1;
        int epoch = 1;
        Set<Integer> voters = Utils.mkSet(localId, closeFollower, laggingFollower);

        // Bootstrap as the leader
        quorumStateStore.writeElectionState(ElectionState.withElectedLeader(epoch, localId, voters));

        KafkaRaftClient client = buildClient(voters);

        pollUntilSend(client);

        int findQuorumCorrelationId = assertSentFindQuorumRequest().correlationId;

        deliverResponse(findQuorumCorrelationId, -1, findQuorumResponse(OptionalInt.of(localId), 1, voters));

        // Accept connection information for followers
        client.poll();

        assertTrue(channel.drainSendQueue().isEmpty());

        // Send out begin quorum to followers
        client.poll();

        List<RaftRequest.Outbound> beginEpochRequests = collectBeginEpochRequests(epoch);

        assertEquals(2, beginEpochRequests.size());

        for (RaftMessage message : beginEpochRequests) {
            deliverResponse(message.correlationId(), ((RaftRequest.Outbound) message).destinationId(),
                beginQuorumEpochResponse(epoch, localId));
        }

        // The lagging follower fetches first
        deliverRequest(fetchQuorumRecordsRequest(1, laggingFollower, 0L, 0, 0));

        client.poll();

        assertSentFetchQuorumRecordsResponse(-1L, epoch);

        // Append some records, so that the close follower will be able to advance further.
        log.appendAsLeader(Utils.mkSet(new SimpleRecord("foo".getBytes()),
            new SimpleRecord("bar".getBytes())), epoch);

        deliverRequest(fetchQuorumRecordsRequest(epoch, closeFollower, 1L, epoch, 0));

        client.poll();

        assertSentFetchQuorumRecordsResponse(0L, epoch);

        // Now shutdown
        client.shutdown(electionTimeoutMs * 2);

        // We should still be running until we have had a chance to send EndQuorumEpoch
        assertTrue(client.isRunning());

        // Send EndQuorumEpoch request to the close follower
        client.poll();
        assertTrue(client.isRunning());

        List<RaftRequest.Outbound> endQuorumRequests =
            collectEndQuorumRequests(1, OptionalInt.of(localId), Utils.mkSet(closeFollower, laggingFollower));

        assertEquals(2, endQuorumRequests.size());
    }

    @Test
    public void testLeaderGracefulShutdownTimeout() throws Exception {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        KafkaRaftClient client = buildClient(voters);

        // Elect ourselves as the leader
        assertEquals(ElectionState.withVotedCandidate(1, localId, voters), quorumStateStore.readElectionState());
        initializeVoterConnections(client, voters, 1, OptionalInt.empty());

        pollUntilSend(client);

        int voteCorrelationId = assertSentVoteRequest(1, 0, 0L);
        VoteResponseData voteResponse = voteResponse(true, Optional.empty(), 1);
        deliverResponse(voteCorrelationId, otherNodeId, voteResponse);
        client.poll();
        assertEquals(ElectionState.withElectedLeader(1, localId, voters), quorumStateStore.readElectionState());

        // Now shutdown
        int shutdownTimeoutMs = 5000;
        client.shutdown(shutdownTimeoutMs);

        // We should still be running until we have had a chance to send EndQuorumEpoch
        assertTrue(client.isRunning());

        // Send EndQuorumEpoch request to the other vote
        client.poll();
        assertTrue(client.isRunning());

        assertSentEndQuorumEpochRequest(1, OptionalInt.of(localId), otherNodeId);

        // The shutdown timeout is hit before we receive any requests or responses indicating an epoch bump
        time.sleep(shutdownTimeoutMs);

        client.poll();
        assertFalse(client.isRunning());
    }

    @Test
    public void testFollowerGracefulShutdown() throws IOException {
        int otherNodeId = 1;
        int epoch = 5;

        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        quorumStateStore.writeElectionState(ElectionState.withElectedLeader(epoch, otherNodeId, voters));
        KafkaRaftClient client = buildClient(voters, stateMachine);

        assertEquals(ElectionState.withElectedLeader(epoch, otherNodeId, voters), quorumStateStore.readElectionState());
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
        assertEquals(ElectionState.withElectedLeader(
            1, localId, Collections.singleton(localId)), quorumStateStore.readElectionState());
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
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        quorumStateStore.writeElectionState(ElectionState.withElectedLeader(epoch, otherNodeId, voters));
        KafkaRaftClient client = buildClient(voters, stateMachine);

        assertEquals(ElectionState.withElectedLeader(epoch, otherNodeId, voters), quorumStateStore.readElectionState());
        assertTrue(stateMachine.isFollower());
        assertEquals(epoch, stateMachine.epoch());

        initializeVoterConnections(client, voters, epoch, OptionalInt.of(otherNodeId));

        pollUntilSend(client);

        int fetchQuorumCorrelationId = assertSentFetchQuorumRecordsRequest(epoch, 0L, 0);
        Records records = MemoryRecords.withRecords(0L, CompressionType.NONE,
            3, new SimpleRecord("a".getBytes()), new SimpleRecord("b".getBytes()));
        FetchQuorumRecordsResponseData response = fetchRecordsResponse(epoch, otherNodeId, records, 0L, Errors.NONE);
        deliverResponse(fetchQuorumCorrelationId, otherNodeId, response);

        client.poll();
        assertEquals(2L, log.endOffset());
    }

    @Test
    public void testAppendToNonLeaderFails() throws IOException {
        int otherNodeId = 1;
        int epoch = 5;

        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        quorumStateStore.writeElectionState(ElectionState.withElectedLeader(epoch, otherNodeId, voters));

        buildClient(voters, stateMachine);
        assertEquals(ElectionState.withElectedLeader(epoch, otherNodeId, voters), quorumStateStore.readElectionState());
        assertTrue(stateMachine.isFollower());
        assertEquals(epoch, stateMachine.epoch());

        SimpleRecord[] appendRecords = new SimpleRecord[] {
            new SimpleRecord("a".getBytes()),
            new SimpleRecord("b".getBytes()),
            new SimpleRecord("c".getBytes())
        };
        Records records = MemoryRecords.withRecords(0L, CompressionType.NONE, 1, appendRecords);

        assertThrows(IllegalStateException.class, () -> stateMachine.append(records));
    }

    @Test
    public void testFetchShouldBeTreatedAsLeaderEndorsement() throws Exception {
        int otherNodeId = 1;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);

        quorumStateStore.writeElectionState(ElectionState.withElectedLeader(epoch, localId, voters));
        KafkaRaftClient client = buildClient(voters);
        assertEquals(ElectionState.withElectedLeader(epoch, localId, voters), quorumStateStore.readElectionState());

        initializeVoterConnections(client, voters, epoch, OptionalInt.of(localId));

        pollUntilSend(client);

        // We send BeginEpoch, but it gets lost and the destination finds the leader through the FindQuorum API
        assertBeginQuorumEpochRequest(epoch);

        deliverRequest(fetchQuorumRecordsRequest(
            epoch, otherNodeId, 0L, 0, 500));

        client.poll();

        // The BeginEpoch request eventually times out. We should not send another one.
        assertSentFetchQuorumRecordsResponse(Errors.NONE, epoch, OptionalInt.of(localId));
        time.sleep(requestTimeoutMs);

        client.poll();

        List<RaftMessage> sentMessages = channel.drainSendQueue();
        assertEquals(0, sentMessages.size());
    }

    @Test
    public void testLeaderAppendSingleMemberQuorum() throws IOException {
        long now = time.milliseconds();

        MockStateMachine stateMachine = new MockStateMachine();
        Set<Integer> voters = Collections.singleton(localId);
        KafkaRaftClient client = buildClient(voters, stateMachine);
        assertEquals(ElectionState.withElectedLeader(1, localId, voters), quorumStateStore.readElectionState());

        SimpleRecord[] appendRecords = new SimpleRecord[] {
            new SimpleRecord("a".getBytes()),
            new SimpleRecord("b".getBytes()),
            new SimpleRecord("c".getBytes())
        };
        Records records = MemoryRecords.withRecords(1L, CompressionType.NONE, 1, appendRecords);

        // First poll has no high watermark advance
        client.poll();
        assertEquals(OptionalLong.of(0L), client.highWatermark());

        stateMachine.append(records);

        // Then poll the appended data with leader change record
        client.poll();
        assertEquals(OptionalLong.of(4L), client.highWatermark());

        // Now try reading it
        int otherNodeId = 1;
        deliverRequest(fetchQuorumRecordsRequest(
            1, otherNodeId, 0L, 0, 500));

        client.poll();

        MemoryRecords fetchedRecords = assertSentFetchQuorumRecordsResponse(Errors.NONE, 1, OptionalInt.of(localId));
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
        quorumStateStore.writeElectionState(ElectionState.withElectedLeader(epoch, otherNodeId, voters));

        log.appendAsLeader(Arrays.asList(
                new SimpleRecord("foo".getBytes()),
                new SimpleRecord("bar".getBytes())), lastEpoch);
        log.appendAsLeader(Arrays.asList(
            new SimpleRecord("baz".getBytes())), lastEpoch);

        KafkaRaftClient client = buildClient(voters);

        assertEquals(ElectionState.withElectedLeader(epoch, otherNodeId, voters), quorumStateStore.readElectionState());
        assertEquals(3L, log.endOffset());

        initializeVoterConnections(client, voters, epoch, OptionalInt.of(otherNodeId));

        pollUntilSend(client);

        int correlationId = assertSentFetchQuorumRecordsRequest(epoch, 3L, lastEpoch);

        FetchQuorumRecordsResponseData response = outOfRangeFetchRecordsResponse(epoch, otherNodeId, 2L,
            lastEpoch, 1L);
        deliverResponse(correlationId, otherNodeId, response);

        // Poll again to complete truncation
        client.poll();
        assertEquals(2L, log.endOffset());

        // Now we should be fetching
        client.poll();
        assertSentFetchQuorumRecordsRequest(epoch, 2L, lastEpoch);
    }

    @Test
    public void testMetrics() throws Exception {
        Metrics metrics = new Metrics(time);
        int epoch = 1;
        quorumStateStore.writeElectionState(ElectionState.withElectedLeader(epoch, localId, Collections.singleton(localId)));
        KafkaRaftClient client = buildClient(Collections.singleton(localId), stateMachine, metrics);

        assertNotNull(getMetric(metrics, "current-state"));
        assertNotNull(getMetric(metrics, "current-leader"));
        assertNotNull(getMetric(metrics, "current-vote"));
        assertNotNull(getMetric(metrics, "current-epoch"));
        assertNotNull(getMetric(metrics, "high-watermark"));
        assertNotNull(getMetric(metrics, "log-end-offset"));
        assertNotNull(getMetric(metrics, "log-end-epoch"));
        assertNotNull(getMetric(metrics, "boot-timestamp"));
        assertNotNull(getMetric(metrics, "number-unknown-voter-connections"));
        assertNotNull(getMetric(metrics, "poll-idle-ratio-avg"));
        assertNotNull(getMetric(metrics, "commit-latency-avg"));
        assertNotNull(getMetric(metrics, "commit-latency-max"));
        assertNotNull(getMetric(metrics, "election-latency-avg"));
        assertNotNull(getMetric(metrics, "election-latency-max"));
        assertNotNull(getMetric(metrics, "fetch-records-rate"));
        assertNotNull(getMetric(metrics, "append-records-rate"));

        assertEquals("leader", getMetric(metrics, "current-state").metricValue());
        assertEquals((double) localId, getMetric(metrics, "current-leader").metricValue());
        assertEquals((double) localId, getMetric(metrics, "current-vote").metricValue());
        assertEquals((double) epoch, getMetric(metrics, "current-epoch").metricValue());
        assertEquals((double) 0L, getMetric(metrics, "high-watermark").metricValue());
        assertEquals((double) 1L, getMetric(metrics, "log-end-offset").metricValue());
        assertEquals((double) epoch, getMetric(metrics, "log-end-epoch").metricValue());
        assertEquals((double) time.milliseconds(), getMetric(metrics, "boot-timestamp").metricValue());

        SimpleRecord[] appendRecords = new SimpleRecord[] {
            new SimpleRecord("a".getBytes()),
            new SimpleRecord("b".getBytes()),
            new SimpleRecord("c".getBytes())
        };
        Records records = MemoryRecords.withRecords(0L, CompressionType.NONE, 1, appendRecords);
        stateMachine.append(records);
        client.poll();

        assertEquals((double) 4L, getMetric(metrics, "high-watermark").metricValue());
        assertEquals((double) 4L, getMetric(metrics, "log-end-offset").metricValue());
        assertEquals((double) epoch, getMetric(metrics, "log-end-epoch").metricValue());

        client.shutdown(100);

        // should only have total-metrics-count left
        assertEquals(1, metrics.metrics().size());
    }

    private KafkaMetric getMetric(final Metrics metrics, final String name) {
        return metrics.metrics().get(metrics.metricName(name, "raft-metrics"));
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

    private int assertSentFindQuorumResponse(
        Errors error,
        int epoch,
        Optional<Integer> leaderId
    ) {
        List<RaftResponse.Outbound> sentMessages = channel.drainSentResponses(ApiKeys.FIND_QUORUM);
        assertEquals(1, sentMessages.size());
        RaftResponse.Outbound raftMessage = sentMessages.get(0);
        assertTrue(raftMessage.data() instanceof FindQuorumResponseData);
        FindQuorumResponseData response = (FindQuorumResponseData) raftMessage.data();
        assertEquals(error, Errors.forCode(response.errorCode()));
        assertEquals(epoch, response.leaderEpoch());
        assertEquals(leaderId.orElse(-1).intValue(), response.leaderId());
        return raftMessage.correlationId();
    }

    private void assertSentVoteResponse(
        Errors error,
        int epoch,
        OptionalInt leaderId,
        boolean voteGranted
    ) {
        List<RaftResponse.Outbound> sentMessages = channel.drainSentResponses(ApiKeys.VOTE);
        assertEquals(1, sentMessages.size());
        RaftMessage raftMessage = sentMessages.get(0);
        assertTrue(raftMessage.data() instanceof VoteResponseData);
        VoteResponseData response = (VoteResponseData) raftMessage.data();
        assertEquals(voteGranted, response.voteGranted());
        assertEquals(error, Errors.forCode(response.errorCode()));
        assertEquals(epoch, response.leaderEpoch());
        assertEquals(leaderId.orElse(-1), response.leaderId());
    }

    private void assertSentEndQuorumEpochResponse(
        Errors error,
        int epoch,
        OptionalInt leaderId
    ) {
        List<RaftResponse.Outbound> sentMessages = channel.drainSentResponses(ApiKeys.END_QUORUM_EPOCH);
        assertEquals(1, sentMessages.size());
        RaftMessage raftMessage = sentMessages.get(0);
        assertTrue(raftMessage.data() instanceof EndQuorumEpochResponseData);
        EndQuorumEpochResponseData response = (EndQuorumEpochResponseData) raftMessage.data();
        assertEquals(error, Errors.forCode(response.errorCode()));
        assertEquals(epoch, response.leaderEpoch());
        assertEquals(leaderId.orElse(-1), response.leaderId());
    }

    private MemoryRecords assertSentFetchQuorumRecordsResponse(
        Errors error,
        int epoch,
        OptionalInt leaderId
    ) {
        List<RaftResponse.Outbound> sentMessages = channel.drainSentResponses(ApiKeys.FETCH_QUORUM_RECORDS);
        assertEquals("Found unexpected sent messages " + sentMessages,
            1, sentMessages.size());
        RaftResponse.Outbound raftMessage = sentMessages.get(0);
        assertEquals(ApiKeys.FETCH_QUORUM_RECORDS.id, raftMessage.data.apiKey());
        FetchQuorumRecordsResponseData response = (FetchQuorumRecordsResponseData) raftMessage.data();
        assertEquals(error, Errors.forCode(response.errorCode()));
        assertEquals(epoch, response.leaderEpoch());
        assertEquals(leaderId.orElse(-1), response.leaderId());
        return MemoryRecords.readableRecords(response.records());
    }

    private void assertSentBeginQuorumEpochResponse(
        Errors error,
        int epoch,
        OptionalInt leaderId
    ) {
        List<RaftResponse.Outbound> sentMessages = channel.drainSentResponses(ApiKeys.BEGIN_QUORUM_EPOCH);
        assertEquals(1, sentMessages.size());
        RaftMessage raftMessage = sentMessages.get(0);
        assertTrue(raftMessage.data() instanceof BeginQuorumEpochResponseData);
        BeginQuorumEpochResponseData response = (BeginQuorumEpochResponseData) raftMessage.data();
        assertEquals(error, Errors.forCode(response.errorCode()));
        assertEquals(epoch, response.leaderEpoch());
        assertEquals(leaderId.orElse(-1), response.leaderId());
    }

    private int assertSentEndQuorumEpochRequest(int epoch, OptionalInt leaderId, int destinationId) {
        List<RaftRequest.Outbound> endQuorumRequests = collectEndQuorumRequests(
            epoch, leaderId, Collections.singleton(destinationId));
        assertEquals(1, endQuorumRequests.size());
        return endQuorumRequests.get(0).correlationId();
    }

    private List<RaftRequest.Outbound> collectEndQuorumRequests(int epoch, OptionalInt leaderId, Set<Integer> destinationIdSet) {
        List<RaftRequest.Outbound> endQuorumRequests = new ArrayList<>();
        Set<Integer> collectedDestinationIdSet = new HashSet<>();
        for (RaftMessage raftMessage : channel.drainSendQueue()) {
            if (raftMessage.data() instanceof EndQuorumEpochRequestData) {
                EndQuorumEpochRequestData request = (EndQuorumEpochRequestData) raftMessage.data();
                assertEquals(epoch, request.leaderEpoch());
                assertEquals(leaderId.orElse(-1), request.leaderId());
                assertEquals(localId, request.replicaId());

                RaftRequest.Outbound outboundRequest = (RaftRequest.Outbound) raftMessage;
                collectedDestinationIdSet.add(outboundRequest.destinationId());
                endQuorumRequests.add(outboundRequest);
            }
        }
        assertEquals(destinationIdSet, collectedDestinationIdSet);
        return endQuorumRequests;
    }

    private RaftRequest.Outbound assertSentFindQuorumRequest() {
        List<RaftRequest.Outbound> sentMessages = channel.drainSentRequests(ApiKeys.FIND_QUORUM);
        assertEquals(1, sentMessages.size());
        RaftRequest.Outbound raftMessage = sentMessages.get(0);
        assertTrue(raftMessage.data() instanceof FindQuorumRequestData);
        FindQuorumRequestData request = (FindQuorumRequestData) raftMessage.data();
        assertEquals(localId, request.replicaId());
        assertTrue(raftMessage.destinationId() < 0);
        return raftMessage;
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

    private int assertBeginQuorumEpochRequest(int epoch) {
        List<RaftRequest.Outbound> requests = collectBeginEpochRequests(epoch);
        assertEquals(1, requests.size());
        return requests.get(0).correlationId;
    }

    private List<RaftRequest.Outbound> collectBeginEpochRequests(int epoch) {
        List<RaftRequest.Outbound> requests = new ArrayList<>();
        for (RaftRequest.Outbound raftRequest : channel.drainSentRequests(ApiKeys.BEGIN_QUORUM_EPOCH)) {
            assertTrue(raftRequest.data() instanceof BeginQuorumEpochRequestData);
            BeginQuorumEpochRequestData request = (BeginQuorumEpochRequestData) raftRequest.data();
            assertEquals(epoch, request.leaderEpoch());
            assertEquals(localId, request.leaderId());
            requests.add(raftRequest);
        }
        return requests;
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

    private void assertSentFetchQuorumRecordsResponse(
        long highWatermark,
        int lastFetchedEpoch) {
        List<RaftMessage> sentMessages = channel.drainSendQueue();
        assertEquals(1, sentMessages.size());
        RaftMessage raftMessage = sentMessages.get(0);
        assertTrue("Unexpected request type " + raftMessage.data(),
            raftMessage.data() instanceof FetchQuorumRecordsResponseData);
        FetchQuorumRecordsResponseData response = (FetchQuorumRecordsResponseData) raftMessage.data();

        assertEquals(Errors.NONE, Errors.forCode(response.errorCode()));
        assertEquals(lastFetchedEpoch, response.leaderEpoch());
        assertEquals(highWatermark, response.highWatermark());
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

    private EndQuorumEpochRequestData endEpochRequest(
        int epoch,
        OptionalInt leaderId,
        int replicaId,
        List<Integer> preferredSuccessors) {
        return new EndQuorumEpochRequestData()
            .setLeaderId(leaderId.orElse(-1))
            .setLeaderEpoch(epoch)
            .setReplicaId(replicaId)
            .setPreferredSuccessors(preferredSuccessors);
    }

    private BeginQuorumEpochResponseData beginEpochResponse(int epoch, int leaderId) {
        return new BeginQuorumEpochResponseData()
            .setLeaderEpoch(epoch)
            .setLeaderId(leaderId);
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
        int lastFetchedEpoch,
        int maxWaitTimeMs
    ) {
        return new FetchQuorumRecordsRequestData()
            .setLeaderEpoch(epoch)
            .setFetchOffset(fetchOffset)
            .setLastFetchedEpoch(lastFetchedEpoch)
            .setReplicaId(replicaId)
            .setMaxWaitTimeMs(maxWaitTimeMs);
    }

    private BeginQuorumEpochResponseData beginQuorumEpochResponse(int epoch, int leaderId) {
        return new BeginQuorumEpochResponseData()
                   .setLeaderEpoch(epoch)
                   .setLeaderId(leaderId);
    }

    private void pollUntilSend(KafkaRaftClient client) throws InterruptedException {
        TestUtils.waitForCondition(() -> {
            client.poll();
            return channel.hasSentMessages();
        }, 5000, "Condition failed to be satisfied before timeout");
    }

    private void deliverRequest(ApiMessage request) {
        RaftRequest.Inbound message = new RaftRequest.Inbound(
            channel.newCorrelationId(), request, time.milliseconds());
        channel.mockReceive(message);
    }

    private void deliverResponse(int correlationId, int sourceId, ApiMessage response) {
        channel.mockReceive(new RaftResponse.Inbound(correlationId, response, sourceId));
    }

    private void assertNoSentMessages() {
        List<RaftMessage> sent = channel.drainSendQueue();
        assertEquals(Collections.emptyList(), sent);
    }

}
