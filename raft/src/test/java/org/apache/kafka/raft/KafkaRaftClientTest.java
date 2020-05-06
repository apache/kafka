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
import org.apache.kafka.common.message.EndQuorumEpochRequestData;
import org.apache.kafka.common.message.FetchQuorumRecordsRequestData;
import org.apache.kafka.common.message.FetchQuorumRecordsResponseData;
import org.apache.kafka.common.message.FindQuorumRequestData;
import org.apache.kafka.common.message.FindQuorumResponseData;
import org.apache.kafka.common.message.LeaderChangeMessage;
import org.apache.kafka.common.message.LeaderChangeMessage.Voter;
import org.apache.kafka.common.message.VoteRequestData;
import org.apache.kafka.common.message.VoteResponseData;
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
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class KafkaRaftClientTest {
    private final int localId = 0;
    private final int electionTimeoutMs = 10000;
    private final int retryBackoffMs = 50;
    private final int requestTimeoutMs = 5000;
    private final int electionJitterMs = 0;
    private final MockTime time = new MockTime();
    private final MockElectionStore electionStore = new MockElectionStore();
    private final MockLog log = new MockLog();
    private final MockNetworkChannel channel = new MockNetworkChannel();

    private InetSocketAddress mockAddress(int id) {
        return new InetSocketAddress("localhost", 9990 + id);
    }

    private KafkaRaftClient buildClient(Set<Integer> voters) throws IOException {
        LogContext logContext = new LogContext();
        QuorumState quorum = new QuorumState(localId, voters, electionStore, logContext);

        List<InetSocketAddress> bootstrapServers = voters.stream()
            .map(this::mockAddress)
            .collect(Collectors.toList());

        KafkaRaftClient client = new KafkaRaftClient(channel, log, quorum, time,
            mockAddress(localId), bootstrapServers,
            electionTimeoutMs, electionJitterMs, retryBackoffMs, requestTimeoutMs, logContext);
        client.initialize(new NoOpStateMachine());
        return client;
    }

    @Test
    public void testInitializeSingleMemberQuorum() throws IOException {
        KafkaRaftClient client = buildClient(Collections.singleton(localId));
        assertEquals(ElectionState.withElectedLeader(1, localId), electionStore.read());
        client.poll();
        assertEquals(0, channel.drainSendQueue().size());
    }

    @Test
    public void testInitializeAsCandidateAndBecomeLeader() throws Exception {
        long now = time.milliseconds();
        final int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        KafkaRaftClient client = buildClient(voters);
        assertEquals(ElectionState.withVotedCandidate(1, localId), electionStore.read());

        pollUntilSend(client);

        int findQuorumCorrelationId = assertSentFindQuorumRequest();
        channel.mockReceive(new RaftResponse.Inbound(findQuorumCorrelationId,
            findQuorumResponse(-1, 1, voters), -1));

        pollUntilSend(client);

        int correlationId = assertSentVoteRequest(1, 0, 0L);
        VoteResponseData voteResponse = voteResponse(true, Optional.empty(), 1);
        channel.mockReceive(new RaftResponse.Inbound(correlationId, voteResponse, otherNodeId));

        // Become leader after receiving the vote
        client.poll();
        assertEquals(ElectionState.withElectedLeader(1, localId), electionStore.read());

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
    public void testVoteRequestTimeout() throws Exception {
        int epoch = 1;
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        KafkaRaftClient client = buildClient(voters);
        assertEquals(ElectionState.withVotedCandidate(epoch, localId), electionStore.read());

        pollUntilSend(client);

        int findQuorumCorrelationId = assertSentFindQuorumRequest();
        channel.mockReceive(new RaftResponse.Inbound(findQuorumCorrelationId,
            findQuorumResponse(localId, epoch, voters), -1));

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
        assertEquals(ElectionState.withElectedLeader(epoch, localId), electionStore.read());

        // If the second request arrives later, it should have no effect
        VoteResponseData retryResponse = voteResponse(true, Optional.empty(), 1);
        channel.mockReceive(new RaftResponse.Inbound(retryId, retryResponse, otherNodeId));
        client.poll();
        assertEquals(ElectionState.withElectedLeader(epoch, localId), electionStore.read());
    }

    @Test
    public void testRetryElection() throws Exception {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        KafkaRaftClient client = buildClient(voters);
        assertEquals(ElectionState.withVotedCandidate(1, localId), electionStore.read());

        pollUntilSend(client);

        int findQuorumCorrelationId = assertSentFindQuorumRequest();
        channel.mockReceive(new RaftResponse.Inbound(findQuorumCorrelationId,
            findQuorumResponse(-1, 1, voters), -1));

        pollUntilSend(client);

        // Quorum size is two. If the other member rejects, then we need to schedule a revote.
        int correlationId = assertSentVoteRequest(1, 0, 0L);
        VoteResponseData voteResponse = voteResponse(false, Optional.empty(), 1);
        channel.mockReceive(new RaftResponse.Inbound(correlationId, voteResponse, otherNodeId));

        client.poll();
        assertEquals(ElectionState.withUnknownLeader(1), electionStore.read());

        // If no new election is held, we will become a candidate again after awaiting the backoff time
        time.sleep(retryBackoffMs);
        client.poll();
        int retryId = assertSentVoteRequest(2, 0, 0L);
        VoteResponseData retryVoteResponse = voteResponse(true, Optional.empty(), 2);
        channel.mockReceive(new RaftResponse.Inbound(retryId, retryVoteResponse, otherNodeId));

        client.poll();
        assertEquals(ElectionState.withElectedLeader(2, localId), electionStore.read());
    }

    @Test
    public void testInitializeAsFollowerEmptyLog() throws Exception {
        int otherNodeId = 1;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        electionStore.write(ElectionState.withElectedLeader(epoch, otherNodeId));
        KafkaRaftClient client = buildClient(voters);
        assertEquals(ElectionState.withElectedLeader(epoch, otherNodeId), electionStore.read());

        pollUntilSend(client);

        int findQuorumCorrelationId = assertSentFindQuorumRequest();
        channel.mockReceive(new RaftResponse.Inbound(findQuorumCorrelationId,
            findQuorumResponse(otherNodeId, epoch, voters), -1));

        pollUntilSend(client);

        assertSentFetchQuorumRecordsRequest(epoch, 0L, 0);
    }

    @Test
    public void testInitializeAsFollowerNonEmptyLog() throws Exception {
        int otherNodeId = 1;
        int epoch = 5;
        int lastEpoch = 3;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        electionStore.write(ElectionState.withElectedLeader(epoch, otherNodeId));
        log.appendAsLeader(Collections.singleton(new SimpleRecord("foo".getBytes())), lastEpoch);

        KafkaRaftClient client = buildClient(voters);
        assertEquals(ElectionState.withElectedLeader(epoch, otherNodeId), electionStore.read());

        pollUntilSend(client);

        int findQuorumCorrelationId = assertSentFindQuorumRequest();
        channel.mockReceive(new RaftResponse.Inbound(findQuorumCorrelationId,
            findQuorumResponse(otherNodeId, epoch, voters), -1));

        pollUntilSend(client);

        assertSentFetchQuorumRecordsRequest(epoch, 1L, lastEpoch);
    }

    @Test
    public void testBecomeCandidateAfterElectionTimeout() throws Exception {
        int otherNodeId = 1;
        int epoch = 5;
        int lastEpoch = 3;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        electionStore.write(ElectionState.withElectedLeader(epoch, otherNodeId));
        log.appendAsLeader(Collections.singleton(new SimpleRecord("foo".getBytes())), lastEpoch);

        KafkaRaftClient client = buildClient(voters);
        assertEquals(ElectionState.withElectedLeader(epoch, otherNodeId), electionStore.read());

        pollUntilSend(client);

        int findQuorumCorrelationId = assertSentFindQuorumRequest();
        channel.mockReceive(new RaftResponse.Inbound(findQuorumCorrelationId,
            findQuorumResponse(otherNodeId, epoch, voters), -1));

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
            findQuorumResponse(leaderId, epoch, voters), -1));

        client.poll();
        assertEquals(ElectionState.withElectedLeader(epoch, leaderId), electionStore.read());
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
            findQuorumResponse(leaderId, epoch, voters), -1));

        client.poll();
        assertEquals(ElectionState.withElectedLeader(epoch, leaderId), electionStore.read());
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
            findQuorumResponse(leaderId, epoch, voters), -1));

        client.poll();
        assertEquals(ElectionState.withElectedLeader(epoch, leaderId), electionStore.read());

        time.sleep(electionTimeoutMs);

        client.poll();
        assertSentFindQuorumRequest();
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
            findQuorumResponse(leaderId, epoch, voters), -1));

        client.poll();
        assertEquals(ElectionState.withElectedLeader(epoch, leaderId), electionStore.read());

        client.poll();
        int fetchCorrelationId = assertSentFetchQuorumRecordsRequest(epoch, 0L, 0);

        FetchQuorumRecordsResponseData response = fetchRecordsResponse(epoch, leaderId, MemoryRecords.EMPTY, 0L,
                Errors.BROKER_NOT_AVAILABLE);
        channel.mockReceive(new RaftResponse.Inbound(fetchCorrelationId, response, leaderId));
        client.poll();

        assertEquals(ElectionState.withUnknownLeader(epoch), electionStore.read());
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
            findQuorumResponse(leaderId, epoch, voters), -1));

        pollUntilSend(client);
        assertEquals(ElectionState.withElectedLeader(epoch, leaderId), electionStore.read());
        assertSentFetchQuorumRecordsRequest(epoch, 0L, 0);

        time.sleep(requestTimeoutMs);
        client.poll();

        assertEquals(ElectionState.withUnknownLeader(epoch), electionStore.read());
        client.poll();
        assertSentFindQuorumRequest();
    }

    @Test
    public void testLeaderHandlesFindQuorum() throws IOException {
        KafkaRaftClient client = buildClient(Collections.singleton(localId));
        assertEquals(ElectionState.withElectedLeader(1, localId), electionStore.read());

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
        assertEquals(ElectionState.withVotedCandidate(1, localId), electionStore.read());

        pollUntilSend(client);

        int findQuorumCorrelationId = assertSentFindQuorumRequest();
        channel.mockReceive(new RaftResponse.Inbound(findQuorumCorrelationId,
            findQuorumResponse(-1, 1, voters), -1));

        pollUntilSend(client);

        int voteCorrelationId = assertSentVoteRequest(1, 0, 0L);
        VoteResponseData voteResponse = voteResponse(true, Optional.empty(), 1);
        channel.mockReceive(new RaftResponse.Inbound(voteCorrelationId, voteResponse, otherNodeId));
        client.poll();
        assertEquals(ElectionState.withElectedLeader(1, localId), electionStore.read());

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
        assertEquals(ElectionState.withVotedCandidate(1, localId), electionStore.read());

        pollUntilSend(client);

        int findQuorumCorrelationId = assertSentFindQuorumRequest();
        channel.mockReceive(new RaftResponse.Inbound(findQuorumCorrelationId,
            findQuorumResponse(-1, 1, voters), -1));

        pollUntilSend(client);

        int voteCorrelationId = assertSentVoteRequest(1, 0, 0L);
        VoteResponseData voteResponse = voteResponse(true, Optional.empty(), 1);
        channel.mockReceive(new RaftResponse.Inbound(voteCorrelationId, voteResponse, otherNodeId));
        client.poll();
        assertEquals(ElectionState.withElectedLeader(1, localId), electionStore.read());

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
        electionStore.write(ElectionState.withElectedLeader(epoch, otherNodeId));
        KafkaRaftClient client = buildClient(Utils.mkSet(localId, otherNodeId));
        assertEquals(ElectionState.withElectedLeader(epoch, otherNodeId), electionStore.read());

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
        assertEquals(ElectionState.withElectedLeader(1, localId), electionStore.read());
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
        electionStore.write(ElectionState.withElectedLeader(epoch, otherNodeId));
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        KafkaRaftClient client = buildClient(voters);
        assertEquals(ElectionState.withElectedLeader(epoch, otherNodeId), electionStore.read());

        pollUntilSend(client);

        int findQuorumCorrelationId = assertSentFindQuorumRequest();
        channel.mockReceive(new RaftResponse.Inbound(findQuorumCorrelationId,
            findQuorumResponse(otherNodeId, epoch, voters), -1));

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
    public void testLeaderAppendSingleMemberQuorum() throws IOException {
        long now = time.milliseconds();
        KafkaRaftClient client = buildClient(Collections.singleton(localId));
        assertEquals(ElectionState.withElectedLeader(1, localId), electionStore.read());

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
        FetchQuorumRecordsRequestData fetchRequest = fetchRecordsRequest(1, otherNodeId, 0L);
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
        electionStore.write(ElectionState.withElectedLeader(epoch, otherNodeId));
        log.appendAsLeader(Arrays.asList(
                new SimpleRecord("foo".getBytes()),
                new SimpleRecord("bar".getBytes())), lastEpoch);
        log.appendAsLeader(Arrays.asList(
            new SimpleRecord("baz".getBytes())), lastEpoch);

        KafkaRaftClient client = buildClient(voters);
        assertEquals(ElectionState.withElectedLeader(epoch, otherNodeId), electionStore.read());
        assertEquals(3L, log.endOffset());

        pollUntilSend(client);

        int findQuorumCorrelationId = assertSentFindQuorumRequest();
        channel.mockReceive(new RaftResponse.Inbound(findQuorumCorrelationId,
            findQuorumResponse(otherNodeId, epoch, voters), -1));

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
        return raftMessage.correlationId();
    }

    private int assertSentVoteRequest(int epoch, int lastEpoch, long lastEpochOffset) {
        List<RaftMessage> sentMessages = channel.drainSendQueue();
        assertEquals(1, sentMessages.size());
        RaftMessage raftMessage = sentMessages.get(0);
        assertTrue(raftMessage.data() instanceof VoteRequestData);
        VoteRequestData request = (VoteRequestData) raftMessage.data();
        assertEquals(epoch, request.candidateEpoch());
        assertEquals(localId, request.candidateId());
        assertEquals(lastEpoch, request.lastEpoch());
        assertEquals(lastEpochOffset, request.lastEpochEndOffset());
        return raftMessage.correlationId();
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
            .setErrorCode(Errors.OFFSET_OUT_OF_RANGE.code())
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

    private FindQuorumResponseData findQuorumResponse(int leaderId, int epoch, Collection<Integer> voters) {
        return new FindQuorumResponseData()
                .setErrorCode(Errors.NONE.code())
                .setLeaderEpoch(epoch)
                .setLeaderId(leaderId)
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

    private FetchQuorumRecordsRequestData fetchRecordsRequest(int epoch, int replicaId, long fetchOffset) {
        return new FetchQuorumRecordsRequestData()
                .setLeaderEpoch(epoch)
                .setFetchOffset(fetchOffset)
                .setReplicaId(replicaId);
    }

    private void pollUntilSend(KafkaRaftClient client) throws InterruptedException {
        TestUtils.waitForCondition(() -> {
            client.poll();
            return channel.hasSentMessages();
        }, "Condition failed to be satisfied before timeout");
    }

}
