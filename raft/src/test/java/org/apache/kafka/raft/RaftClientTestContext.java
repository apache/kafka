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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
//import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
//import java.util.OptionalLong;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
//import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.common.TopicPartition;
//import org.apache.kafka.common.errors.ClusterAuthorizationException;
//import org.apache.kafka.common.errors.NotLeaderOrFollowerException;
import org.apache.kafka.common.message.BeginQuorumEpochRequestData;
import org.apache.kafka.common.message.BeginQuorumEpochResponseData;
import org.apache.kafka.common.message.DescribeQuorumResponseData.ReplicaState;
import org.apache.kafka.common.message.DescribeQuorumResponseData;
import org.apache.kafka.common.message.EndQuorumEpochRequestData;
import org.apache.kafka.common.message.EndQuorumEpochResponseData;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.LeaderChangeMessage.Voter;
import org.apache.kafka.common.message.LeaderChangeMessage;
import org.apache.kafka.common.message.VoteRequestData;
import org.apache.kafka.common.message.VoteResponseData;
//import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.ControlRecordType;
import org.apache.kafka.common.record.ControlRecordUtils;
import org.apache.kafka.common.record.MemoryRecords;
//import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Record;
//import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.record.SimpleRecord;
//import org.apache.kafka.common.requests.BeginQuorumEpochRequest;
import org.apache.kafka.common.requests.BeginQuorumEpochResponse;
//import org.apache.kafka.common.requests.DescribeQuorumRequest;
import org.apache.kafka.common.requests.DescribeQuorumResponse;
//import org.apache.kafka.common.requests.EndQuorumEpochRequest;
//import org.apache.kafka.common.requests.VoteRequest;
import org.apache.kafka.common.requests.VoteResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestUtils;
import org.mockito.Mockito;
import static org.apache.kafka.raft.RaftUtil.hasValidTopicPartition;
//import static org.apache.kafka.test.TestUtils.assertFutureThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
//import static org.junit.jupiter.api.Assertions.assertFalse;
//import static org.junit.jupiter.api.Assertions.assertNotEquals;
//import static org.junit.jupiter.api.Assertions.assertNotNull;
//import static org.junit.jupiter.api.Assertions.assertNull;
//import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class RaftClientTestContext {
    static final TopicPartition METADATA_PARTITION = new TopicPartition("metadata", 0);
    static final int LOCAL_ID = 0;

    static final int ELECTION_BACKOFF_MAX_MS = 100;
    static final int ELECTION_TIMEOUT_MS = 10000;
    private static final int FETCH_MAX_WAIT_MS = 0;
    // fetch timeout is usually larger than election timeout
    static final int FETCH_TIMEOUT_MS = 50000;
    static final int REQUEST_TIMEOUT_MS = 5000;
    static final int RETRY_BACKOFF_MS = 50;

    private final Random random;

    final KafkaRaftClient client;
    final MockLog log;
    final MockNetworkChannel channel;
    final MockTime time;
    final QuorumStateStore quorumStateStore;
    final Metrics metrics;

    public static final class Builder {
        private final QuorumStateStore quorumStateStore = new MockQuorumStateStore();
        private final Random random = Mockito.spy(new Random(1));
        private final MockLog log = new MockLog(METADATA_PARTITION);

        Builder updateQuorumStateStore(Consumer<QuorumStateStore> consumer) {
            consumer.accept(quorumStateStore);
            return this;
        }

        Builder updateRandom(Consumer<Random> consumer) {
            consumer.accept(random);
            return this;
        }

        Builder updateLog(Consumer<MockLog> consumer) {
            consumer.accept(log);
            return this;
        }

        RaftClientTestContext build(Set<Integer> voters) throws IOException {
            MockTime time = new MockTime();
            Metrics metrics = new Metrics(time);
            MockNetworkChannel channel = new MockNetworkChannel();
            LogContext logContext = new LogContext();
            QuorumState quorum = new QuorumState(LOCAL_ID, voters, ELECTION_TIMEOUT_MS, FETCH_TIMEOUT_MS,
                    quorumStateStore, time, logContext, random);

            Map<Integer, InetSocketAddress> voterAddresses = voters.stream().collect(Collectors.toMap(
                        Function.identity(),
                        RaftClientTestContext::mockAddress
                        ));

            KafkaRaftClient client = new KafkaRaftClient(channel, log, quorum, time, metrics,
                    new MockFuturePurgatory<>(time), new MockFuturePurgatory<>(time), voterAddresses,
                    ELECTION_BACKOFF_MAX_MS, RETRY_BACKOFF_MS, REQUEST_TIMEOUT_MS, FETCH_MAX_WAIT_MS, logContext, random);

            client.initialize();

            return new RaftClientTestContext(client, log, channel, time, quorumStateStore, random, metrics);
        }
    }

    private RaftClientTestContext(
        KafkaRaftClient client,
        MockLog log,
        MockNetworkChannel channel,
        MockTime time,
        QuorumStateStore quorumStateStore,
        Random random,
        Metrics metrics
    ) {
        this.channel = channel;
        this.client = client;
        this.log = log;
        this.quorumStateStore = quorumStateStore;
        this.random = random;
        this.time = time;
        this.metrics = metrics;
    }

    static RaftClientTestContext initializeAsLeader(Set<Integer> voters, int epoch) throws Exception {
        if (epoch <= 0) {
            throw new IllegalArgumentException("Cannot become leader in epoch " + epoch);
        }

        ElectionState electionState = ElectionState.withUnknownLeader(epoch - 1, voters);
        RaftClientTestContext context = new RaftClientTestContext.Builder()
            .updateRandom(random -> {
                Mockito.doReturn(0).when(random).nextInt(ELECTION_TIMEOUT_MS);
            })
            .updateQuorumStateStore(quorumStateStore -> {
                assertDoesNotThrow(() -> {
                    quorumStateStore.writeElectionState(electionState);
                });
            })
            .build(voters);

        assertEquals(electionState, context.quorumStateStore.readElectionState());

        // Advance the clock so that we become a candidate
        context.time.sleep(ELECTION_TIMEOUT_MS);
        context.expectLeaderElection(voters, epoch);

        // Handle BeginEpoch
        context.pollUntilSend();
        for (RaftRequest.Outbound request : context.collectBeginEpochRequests(epoch)) {
            BeginQuorumEpochResponseData beginEpochResponse = beginEpochResponse(epoch, LOCAL_ID);
            context.deliverResponse(request.correlationId, request.destinationId(), beginEpochResponse);
        }

        context.client.poll();
        return context;
    }

    static RaftClientTestContext build(Set<Integer> voters) throws IOException {
        return new Builder().build(voters);
    }

    void expectLeaderElection(
        Set<Integer> voters,
        int epoch
    ) throws Exception {
        pollUntilSend();

        List<RaftRequest.Outbound> voteRequests = collectVoteRequests(epoch,
            log.lastFetchedEpoch(), log.endOffset().offset);

        for (RaftRequest.Outbound request : voteRequests) {
            VoteResponseData voteResponse = voteResponse(true, Optional.empty(), epoch);
            deliverResponse(request.correlationId, request.destinationId(), voteResponse);
        }

        client.poll();
        assertEquals(ElectionState.withElectedLeader(epoch, LOCAL_ID, voters),
            quorumStateStore.readElectionState());
    }

    void pollUntilSend() throws InterruptedException {
        TestUtils.waitForCondition(() -> {
            client.poll();
            return channel.hasSentMessages();
        }, 5000, "Condition failed to be satisfied before timeout");
    }

    int assertSentDescribeQuorumResponse(int leaderId,
                                                 int leaderEpoch,
                                                 long highWatermark,
                                                 List<ReplicaState> voterStates,
                                                 List<ReplicaState> observerStates) {
        List<RaftMessage> sentMessages = channel.drainSendQueue();
        assertEquals(1, sentMessages.size());
        RaftMessage raftMessage = sentMessages.get(0);
        assertTrue(
            raftMessage.data() instanceof DescribeQuorumResponseData,
            "Unexpected request type " + raftMessage.data());
        DescribeQuorumResponseData response = (DescribeQuorumResponseData) raftMessage.data();

        DescribeQuorumResponseData expectedResponse = DescribeQuorumResponse.singletonResponse(
            METADATA_PARTITION,
            leaderId,
            leaderEpoch,
            highWatermark,
            voterStates,
            observerStates);

        assertEquals(expectedResponse, response);
        return raftMessage.correlationId();
    }

    int assertSentVoteRequest(int epoch, int lastEpoch, long lastEpochOffset) {
        List<RaftRequest.Outbound> voteRequests = collectVoteRequests(epoch, lastEpoch, lastEpochOffset);
        assertEquals(1, voteRequests.size());
        return voteRequests.iterator().next().correlationId();
    }

    void assertSentVoteResponse(
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
        assertTrue(hasValidTopicPartition(response, METADATA_PARTITION));

        VoteResponseData.PartitionData partitionResponse = response.topics().get(0).partitions().get(0);

        assertEquals(voteGranted, partitionResponse.voteGranted());
        assertEquals(error, Errors.forCode(partitionResponse.errorCode()));
        assertEquals(epoch, partitionResponse.leaderEpoch());
        assertEquals(leaderId.orElse(-1), partitionResponse.leaderId());
    }

    List<RaftRequest.Outbound> collectVoteRequests(
        int epoch,
        int lastEpoch,
        long lastEpochOffset
    ) {
        List<RaftRequest.Outbound> voteRequests = new ArrayList<>();
        for (RaftMessage raftMessage : channel.drainSendQueue()) {
            if (raftMessage.data() instanceof VoteRequestData) {
                VoteRequestData request = (VoteRequestData) raftMessage.data();
                assertTrue(hasValidTopicPartition(request, METADATA_PARTITION));

                VoteRequestData.PartitionData partitionRequest = request.topics().get(0).partitions().get(0);

                assertEquals(epoch, partitionRequest.candidateEpoch());
                assertEquals(LOCAL_ID, partitionRequest.candidateId());
                assertEquals(lastEpoch, partitionRequest.lastOffsetEpoch());
                assertEquals(lastEpochOffset, partitionRequest.lastOffset());
                voteRequests.add((RaftRequest.Outbound) raftMessage);
            }
        }
        return voteRequests;
    }

    void deliverRequest(ApiMessage request) {
        RaftRequest.Inbound message = new RaftRequest.Inbound(channel.newCorrelationId(), request, time.milliseconds());
        channel.mockReceive(message);
    }

    void deliverResponse(int correlationId, int sourceId, ApiMessage response) {
        channel.mockReceive(new RaftResponse.Inbound(correlationId, response, sourceId));
    }

    int assertSentBeginQuorumEpochRequest(int epoch) {
        List<RaftRequest.Outbound> requests = collectBeginEpochRequests(epoch);
        assertEquals(1, requests.size());
        return requests.get(0).correlationId;
    }

    void assertSentBeginQuorumEpochResponse(
        Errors partitionError,
        int epoch,
        OptionalInt leaderId
    ) {
        List<RaftResponse.Outbound> sentMessages = channel.drainSentResponses(ApiKeys.BEGIN_QUORUM_EPOCH);
        assertEquals(1, sentMessages.size());
        RaftMessage raftMessage = sentMessages.get(0);
        assertTrue(raftMessage.data() instanceof BeginQuorumEpochResponseData);
        BeginQuorumEpochResponseData response = (BeginQuorumEpochResponseData) raftMessage.data();
        assertEquals(Errors.NONE, Errors.forCode(response.errorCode()));

        BeginQuorumEpochResponseData.PartitionData partitionResponse =
            response.topics().get(0).partitions().get(0);

        assertEquals(epoch, partitionResponse.leaderEpoch());
        assertEquals(leaderId.orElse(-1), partitionResponse.leaderId());
        assertEquals(partitionError, Errors.forCode(partitionResponse.errorCode()));
    }
    
    int assertSentEndQuorumEpochRequest(int epoch, OptionalInt leaderId, int destinationId) {
        List<RaftRequest.Outbound> endQuorumRequests = collectEndQuorumRequests(
            epoch, leaderId, Collections.singleton(destinationId));
        assertEquals(1, endQuorumRequests.size());
        return endQuorumRequests.get(0).correlationId();
    }

    void assertSentEndQuorumEpochResponse(
        Errors partitionError,
        int epoch,
        OptionalInt leaderId
    ) {
        List<RaftResponse.Outbound> sentMessages = channel.drainSentResponses(ApiKeys.END_QUORUM_EPOCH);
        assertEquals(1, sentMessages.size());
        RaftMessage raftMessage = sentMessages.get(0);
        assertTrue(raftMessage.data() instanceof EndQuorumEpochResponseData);
        EndQuorumEpochResponseData response = (EndQuorumEpochResponseData) raftMessage.data();
        assertEquals(Errors.NONE, Errors.forCode(response.errorCode()));

        EndQuorumEpochResponseData.PartitionData partitionResponse =
            response.topics().get(0).partitions().get(0);

        assertEquals(epoch, partitionResponse.leaderEpoch());
        assertEquals(leaderId.orElse(-1), partitionResponse.leaderId());
        assertEquals(partitionError, Errors.forCode(partitionResponse.errorCode()));
    }

    RaftRequest.Outbound assertSentFetchRequest() {
        List<RaftRequest.Outbound> sentRequests = channel.drainSentRequests(ApiKeys.FETCH);
        assertEquals(1, sentRequests.size());
        return sentRequests.get(0);
    }

    int assertSentFetchRequest(
        int epoch,
        long fetchOffset,
        int lastFetchedEpoch
    ) {
        List<RaftMessage> sentMessages = channel.drainSendQueue();
        assertEquals(1, sentMessages.size());
        RaftMessage raftMessage = sentMessages.get(0);
        assertFetchRequestData(raftMessage, epoch, fetchOffset, lastFetchedEpoch);
        return raftMessage.correlationId();
    }

    MemoryRecords assertSentFetchResponse(
        Errors error,
        int epoch,
        OptionalInt leaderId
    ) {
        FetchResponseData.FetchablePartitionResponse partitionResponse = assertSentPartitionResponse();
        assertEquals(error, Errors.forCode(partitionResponse.errorCode()));
        assertEquals(epoch, partitionResponse.currentLeader().leaderEpoch());
        assertEquals(leaderId.orElse(-1), partitionResponse.currentLeader().leaderId());
        return (MemoryRecords) partitionResponse.recordSet();
    }

    MemoryRecords assertSentFetchResponse(
        long highWatermark,
        int leaderEpoch
    ) {
        FetchResponseData.FetchablePartitionResponse partitionResponse = assertSentPartitionResponse();
        assertEquals(Errors.NONE, Errors.forCode(partitionResponse.errorCode()));
        assertEquals(leaderEpoch, partitionResponse.currentLeader().leaderEpoch());
        assertEquals(highWatermark, partitionResponse.highWatermark());
        return (MemoryRecords) partitionResponse.recordSet();
    }

    void validateLocalRead(
        OffsetAndEpoch fetchOffsetAndEpoch,
        Isolation isolation,
        SimpleRecord[] expectedRecords
    ) throws Exception {
        CompletableFuture<Records> future = client.read(fetchOffsetAndEpoch, isolation, 0L);
        assertTrue(future.isDone());
        assertMatchingRecords(expectedRecords, future.get());
    }

    void fetchFromLeader(
        int leaderId,
        int epoch,
        OffsetAndEpoch fetchOffsetAndEpoch,
        SimpleRecord[] records,
        long highWatermark
    ) throws Exception {
        pollUntilSend();
        int fetchCorrelationId = assertSentFetchRequest(epoch,
            fetchOffsetAndEpoch.offset, fetchOffsetAndEpoch.epoch);
        Records fetchedRecords = MemoryRecords.withRecords(fetchOffsetAndEpoch.offset,
            CompressionType.NONE, epoch, records);
        FetchResponseData fetchResponse = fetchResponse(
            epoch, leaderId, fetchedRecords, highWatermark, Errors.NONE);
        deliverResponse(fetchCorrelationId, leaderId, fetchResponse);
        client.poll();
    }

    void buildFollowerSet(
        int epoch,
        int closeFollower,
        int laggingFollower
    ) throws Exception {
        // The lagging follower fetches first
        deliverRequest(fetchRequest(1, laggingFollower, 0L, 0, 0));

        client.poll();

        assertSentFetchResponse(0L, epoch);

        // Append some records, so that the close follower will be able to advance further.
        MemoryRecords records = MemoryRecords.withRecords(CompressionType.NONE,
            new SimpleRecord("foo".getBytes()),
            new SimpleRecord("bar".getBytes()));
        client.append(records, AckMode.LEADER, Integer.MAX_VALUE);
        client.poll();

        deliverRequest(fetchRequest(epoch, closeFollower, 1L, epoch, 0));

        client.poll();

        assertSentFetchResponse(1L, epoch);
    }

    List<RaftRequest.Outbound> collectEndQuorumRequests(int epoch, OptionalInt leaderId, Set<Integer> destinationIdSet) {
        List<RaftRequest.Outbound> endQuorumRequests = new ArrayList<>();
        Set<Integer> collectedDestinationIdSet = new HashSet<>();
        for (RaftMessage raftMessage : channel.drainSendQueue()) {
            if (raftMessage.data() instanceof EndQuorumEpochRequestData) {
                EndQuorumEpochRequestData request = (EndQuorumEpochRequestData) raftMessage.data();

                EndQuorumEpochRequestData.PartitionData partitionRequest =
                    request.topics().get(0).partitions().get(0);

                assertEquals(epoch, partitionRequest.leaderEpoch());
                assertEquals(leaderId.orElse(-1), partitionRequest.leaderId());
                assertEquals(LOCAL_ID, partitionRequest.replicaId());

                RaftRequest.Outbound outboundRequest = (RaftRequest.Outbound) raftMessage;
                collectedDestinationIdSet.add(outboundRequest.destinationId());
                endQuorumRequests.add(outboundRequest);
            }
        }
        assertEquals(destinationIdSet, collectedDestinationIdSet);
        return endQuorumRequests;
    }

    void discoverLeaderAsObserver(
        Set<Integer> voters,
        int leaderId,
        int epoch
    ) throws Exception {
        pollUntilSend();
        RaftRequest.Outbound fetchRequest = assertSentFetchRequest();
        assertTrue(voters.contains(fetchRequest.destinationId()));
        assertFetchRequestData(fetchRequest, 0, 0L, 0);

        deliverResponse(fetchRequest.correlationId, fetchRequest.destinationId(),
            fetchResponse(epoch, leaderId, MemoryRecords.EMPTY, 0L, Errors.NONE));
        client.poll();
        assertEquals(ElectionState.withElectedLeader(epoch, leaderId, voters), quorumStateStore.readElectionState());
    }

    private List<RaftRequest.Outbound> collectBeginEpochRequests(int epoch) {
        List<RaftRequest.Outbound> requests = new ArrayList<>();
        for (RaftRequest.Outbound raftRequest : channel.drainSentRequests(ApiKeys.BEGIN_QUORUM_EPOCH)) {
            assertTrue(raftRequest.data() instanceof BeginQuorumEpochRequestData);
            BeginQuorumEpochRequestData request = (BeginQuorumEpochRequestData) raftRequest.data();

            BeginQuorumEpochRequestData.PartitionData partitionRequest =
                request.topics().get(0).partitions().get(0);

            assertEquals(epoch, partitionRequest.leaderEpoch());
            assertEquals(LOCAL_ID, partitionRequest.leaderId());
            requests.add(raftRequest);
        }
        return requests;
    }

    private FetchResponseData.FetchablePartitionResponse assertSentPartitionResponse() {
        List<RaftResponse.Outbound> sentMessages = channel.drainSentResponses(ApiKeys.FETCH);
        assertEquals(
            1, sentMessages.size(), "Found unexpected sent messages " + sentMessages);
        RaftResponse.Outbound raftMessage = sentMessages.get(0);
        assertEquals(ApiKeys.FETCH.id, raftMessage.data.apiKey());
        FetchResponseData response = (FetchResponseData) raftMessage.data();
        assertEquals(Errors.NONE, Errors.forCode(response.errorCode()));

        assertEquals(1, response.responses().size());
        assertEquals(METADATA_PARTITION.topic(), response.responses().get(0).topic());
        assertEquals(1, response.responses().get(0).partitionResponses().size());
        return response.responses().get(0).partitionResponses().get(0);
    }

    private static InetSocketAddress mockAddress(int id) {
        return new InetSocketAddress("localhost", 9990 + id);
    }

    private static BeginQuorumEpochResponseData beginEpochResponse(int epoch, int leaderId) {
        return BeginQuorumEpochResponse.singletonResponse(
            Errors.NONE,
            METADATA_PARTITION,
            Errors.NONE,
            epoch,
            leaderId
        );
    }

    static VoteResponseData voteResponse(boolean voteGranted, Optional<Integer> leaderId, int epoch) {
        return VoteResponse.singletonResponse(
            Errors.NONE,
            METADATA_PARTITION,
            Errors.NONE,
            epoch,
            leaderId.orElse(-1),
            voteGranted
        );
    }

    static void assertMatchingRecords(
        SimpleRecord[] expected,
        Records actual
    ) {
        List<Record> recordList = Utils.toList(actual.records());
        assertEquals(expected.length, recordList.size());
        for (int i = 0; i < expected.length; i++) {
            Record record = recordList.get(i);
            assertEquals(
                expected[i], new SimpleRecord(record),
                "Record at offset " + record.offset() + " does not match expected");
        }
    }

    static void verifyLeaderChangeMessage(
        int leaderId,
        List<Integer> voters,
        ByteBuffer recordKey,
        ByteBuffer recordValue
    ) {
        assertEquals(ControlRecordType.LEADER_CHANGE, ControlRecordType.parse(recordKey));

        LeaderChangeMessage leaderChangeMessage = ControlRecordUtils.deserializeLeaderChangeMessage(recordValue);
        assertEquals(leaderId, leaderChangeMessage.leaderId());
        assertEquals(voters.stream().map(voterId -> new Voter().setVoterId(voterId)).collect(Collectors.toList()),
            leaderChangeMessage.voters());
    }

    static void assertFetchRequestData(
        RaftMessage message,
        int epoch,
        long fetchOffset,
        int lastFetchedEpoch
    ) {
        assertTrue(
            message.data() instanceof FetchRequestData, "Unexpected request type " + message.data());
        FetchRequestData request = (FetchRequestData) message.data();

        assertEquals(1, request.topics().size());
        assertEquals(METADATA_PARTITION.topic(), request.topics().get(0).topic());
        assertEquals(1, request.topics().get(0).partitions().size());

        FetchRequestData.FetchPartition fetchPartition = request.topics().get(0).partitions().get(0);
        assertEquals(epoch, fetchPartition.currentLeaderEpoch());
        assertEquals(fetchOffset, fetchPartition.fetchOffset());
        assertEquals(lastFetchedEpoch, fetchPartition.lastFetchedEpoch());
        assertEquals(LOCAL_ID, request.replicaId());
    }

    static FetchRequestData fetchRequest(
        int epoch,
        int replicaId,
        long fetchOffset,
        int lastFetchedEpoch,
        int maxWaitTimeMs
    ) {
        FetchRequestData request = RaftUtil.singletonFetchRequest(METADATA_PARTITION, fetchPartition -> {
            fetchPartition
                .setCurrentLeaderEpoch(epoch)
                .setLastFetchedEpoch(lastFetchedEpoch)
                .setFetchOffset(fetchOffset);
        });
        return request
            .setMaxWaitMs(maxWaitTimeMs)
            .setReplicaId(replicaId);
    }

    static FetchResponseData fetchResponse(
        int epoch,
        int leaderId,
        Records records,
        long highWatermark,
        Errors error
    ) {
        return RaftUtil.singletonFetchResponse(METADATA_PARTITION, Errors.NONE, partitionData -> {
            partitionData
                .setRecordSet(records)
                .setErrorCode(error.code())
                .setHighWatermark(highWatermark);

            partitionData.currentLeader()
                .setLeaderEpoch(epoch)
                .setLeaderId(leaderId);
        });
    }
}
