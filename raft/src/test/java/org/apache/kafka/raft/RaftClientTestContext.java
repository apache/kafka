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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.common.message.BeginQuorumEpochRequestData;
import org.apache.kafka.common.message.BeginQuorumEpochResponseData;
import org.apache.kafka.common.message.DescribeQuorumResponseData;
import org.apache.kafka.common.message.DescribeQuorumResponseData.ReplicaState;
import org.apache.kafka.common.message.EndQuorumEpochRequestData;
import org.apache.kafka.common.message.EndQuorumEpochResponseData;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.FetchSnapshotResponseData;
import org.apache.kafka.common.message.LeaderChangeMessage;
import org.apache.kafka.common.message.LeaderChangeMessage.Voter;
import org.apache.kafka.common.message.VoteRequestData;
import org.apache.kafka.common.message.VoteResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.ControlRecordType;
import org.apache.kafka.common.record.ControlRecordUtils;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.requests.BeginQuorumEpochRequest;
import org.apache.kafka.common.requests.BeginQuorumEpochResponse;
import org.apache.kafka.common.requests.DescribeQuorumResponse;
import org.apache.kafka.common.requests.EndQuorumEpochRequest;
import org.apache.kafka.common.requests.EndQuorumEpochResponse;
import org.apache.kafka.common.requests.FetchSnapshotResponse;
import org.apache.kafka.common.requests.VoteRequest;
import org.apache.kafka.common.requests.VoteResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.raft.internals.BatchBuilder;
import org.apache.kafka.raft.internals.StringSerde;
import org.apache.kafka.server.common.serialization.RecordSerde;
import org.apache.kafka.snapshot.RawSnapshotWriter;
import org.apache.kafka.snapshot.SnapshotReader;
import org.apache.kafka.test.TestCondition;
import org.apache.kafka.test.TestUtils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.apache.kafka.raft.RaftUtil.hasValidTopicPartition;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public final class RaftClientTestContext {
    public final RecordSerde<String> serde = Builder.SERDE;
    final TopicPartition metadataPartition = Builder.METADATA_PARTITION;
    final Uuid metadataTopicId = Uuid.METADATA_TOPIC_ID;
    final int electionBackoffMaxMs = Builder.ELECTION_BACKOFF_MAX_MS;
    final int fetchMaxWaitMs = Builder.FETCH_MAX_WAIT_MS;
    final int fetchTimeoutMs = Builder.FETCH_TIMEOUT_MS;
    final int retryBackoffMs = Builder.RETRY_BACKOFF_MS;

    private int electionTimeoutMs;
    private int requestTimeoutMs;
    private int appendLingerMs;

    private final QuorumStateStore quorumStateStore;
    final Uuid clusterId;
    private final OptionalInt localId;
    public final KafkaRaftClient<String> client;
    final Metrics metrics;
    public final MockLog log;
    final MockNetworkChannel channel;
    final MockMessageQueue messageQueue;
    final MockTime time;
    final MockListener listener;
    final Set<Integer> voters;

    private final List<RaftResponse.Outbound> sentResponses = new ArrayList<>();

    public static final class Builder {
        static final int DEFAULT_ELECTION_TIMEOUT_MS = 10000;

        private static final RecordSerde<String> SERDE = new StringSerde();
        private static final TopicPartition METADATA_PARTITION = new TopicPartition("metadata", 0);
        private static final int ELECTION_BACKOFF_MAX_MS = 100;
        private static final int FETCH_MAX_WAIT_MS = 0;
        // fetch timeout is usually larger than election timeout
        private static final int FETCH_TIMEOUT_MS = 50000;
        private static final int DEFAULT_REQUEST_TIMEOUT_MS = 5000;
        private static final int RETRY_BACKOFF_MS = 50;
        private static final int DEFAULT_APPEND_LINGER_MS = 0;

        private final MockMessageQueue messageQueue = new MockMessageQueue();
        private final MockTime time = new MockTime();
        private final QuorumStateStore quorumStateStore = new MockQuorumStateStore();
        private final MockableRandom random = new MockableRandom(1L);
        private final LogContext logContext = new LogContext();
        private final MockLog log = new MockLog(METADATA_PARTITION, Uuid.METADATA_TOPIC_ID, logContext);
        private final Set<Integer> voters;
        private final OptionalInt localId;

        private Uuid clusterId = Uuid.randomUuid();
        private int requestTimeoutMs = DEFAULT_REQUEST_TIMEOUT_MS;
        private int electionTimeoutMs = DEFAULT_ELECTION_TIMEOUT_MS;
        private int appendLingerMs = DEFAULT_APPEND_LINGER_MS;
        private MemoryPool memoryPool = MemoryPool.NONE;

        public Builder(int localId, Set<Integer> voters) {
            this(OptionalInt.of(localId), voters);
        }

        public Builder(OptionalInt localId, Set<Integer> voters) {
            this.voters = voters;
            this.localId = localId;
        }

        Builder withElectedLeader(int epoch, int leaderId) throws IOException {
            quorumStateStore.writeElectionState(ElectionState.withElectedLeader(epoch, leaderId, voters));
            return this;
        }

        Builder withUnknownLeader(int epoch) throws IOException {
            quorumStateStore.writeElectionState(ElectionState.withUnknownLeader(epoch, voters));
            return this;
        }

        Builder withVotedCandidate(int epoch, int votedId) throws IOException {
            quorumStateStore.writeElectionState(ElectionState.withVotedCandidate(epoch, votedId, voters));
            return this;
        }

        Builder updateRandom(Consumer<MockableRandom> consumer) {
            consumer.accept(random);
            return this;
        }

        Builder withMemoryPool(MemoryPool pool) {
            this.memoryPool = pool;
            return this;
        }

        Builder withAppendLingerMs(int appendLingerMs) {
            this.appendLingerMs = appendLingerMs;
            return this;
        }

        public Builder appendToLog(int epoch, List<String> records) {
            MemoryRecords batch = buildBatch(
                time.milliseconds(),
                log.endOffset().offset,
                epoch,
                records
            );
            log.appendAsLeader(batch, epoch);
            // Need to flush the log to update the last flushed offset. This is always correct
            // because append operation was done in the Builder which represent the state of the
            // log before the replica starts.
            log.flush(false);

            // Reset the value of this method since "flush" before the replica start should not
            // count when checking for flushes by the KRaft client.
            log.flushedSinceLastChecked();
            return this;
        }

        Builder withEmptySnapshot(OffsetAndEpoch snapshotId) throws IOException {
            try (RawSnapshotWriter snapshot = log.storeSnapshot(snapshotId).get()) {
                snapshot.freeze();
            }
            return this;
        }

        Builder deleteBeforeSnapshot(OffsetAndEpoch snapshotId) throws IOException {
            if (snapshotId.offset() > log.highWatermark().offset) {
                log.updateHighWatermark(new LogOffsetMetadata(snapshotId.offset()));
            }
            log.deleteBeforeSnapshot(snapshotId);

            return this;
        }

        Builder withElectionTimeoutMs(int electionTimeoutMs) {
            this.electionTimeoutMs = electionTimeoutMs;
            return this;
        }

        Builder withRequestTimeoutMs(int requestTimeoutMs) {
            this.requestTimeoutMs = requestTimeoutMs;
            return this;
        }

        Builder withClusterId(Uuid clusterId) {
            this.clusterId = clusterId;
            return this;
        }

        public RaftClientTestContext build() throws IOException {
            Metrics metrics = new Metrics(time);
            MockNetworkChannel channel = new MockNetworkChannel(voters);
            MockListener listener = new MockListener(localId);
            Map<Integer, RaftConfig.AddressSpec> voterAddressMap = voters.stream()
                .collect(Collectors.toMap(id -> id, RaftClientTestContext::mockAddress));
            RaftConfig raftConfig = new RaftConfig(voterAddressMap, requestTimeoutMs, RETRY_BACKOFF_MS, electionTimeoutMs,
                    ELECTION_BACKOFF_MAX_MS, FETCH_TIMEOUT_MS, appendLingerMs);

            KafkaRaftClient<String> client = new KafkaRaftClient<>(
                SERDE,
                channel,
                messageQueue,
                log,
                quorumStateStore,
                memoryPool,
                time,
                metrics,
                new MockExpirationService(time),
                FETCH_MAX_WAIT_MS,
                clusterId.toString(),
                localId,
                logContext,
                random,
                raftConfig
            );

            client.register(listener);
            client.initialize();

            RaftClientTestContext context = new RaftClientTestContext(
                clusterId,
                localId,
                client,
                log,
                channel,
                messageQueue,
                time,
                quorumStateStore,
                voters,
                metrics,
                listener
            );

            context.electionTimeoutMs = electionTimeoutMs;
            context.requestTimeoutMs = requestTimeoutMs;
            context.appendLingerMs = appendLingerMs;

            return context;
        }
    }

    private RaftClientTestContext(
        Uuid clusterId,
        OptionalInt localId,
        KafkaRaftClient<String> client,
        MockLog log,
        MockNetworkChannel channel,
        MockMessageQueue messageQueue,
        MockTime time,
        QuorumStateStore quorumStateStore,
        Set<Integer> voters,
        Metrics metrics,
        MockListener listener
    ) {
        this.clusterId = clusterId;
        this.localId = localId;
        this.client = client;
        this.log = log;
        this.channel = channel;
        this.messageQueue = messageQueue;
        this.time = time;
        this.quorumStateStore = quorumStateStore;
        this.voters = voters;
        this.metrics = metrics;
        this.listener = listener;
    }

    int electionTimeoutMs() {
        return electionTimeoutMs;
    }

    int requestTimeoutMs() {
        return requestTimeoutMs;
    }

    int appendLingerMs() {
        return appendLingerMs;
    }

    MemoryRecords buildBatch(
        long baseOffset,
        int epoch,
        List<String> records
    ) {
        return buildBatch(time.milliseconds(), baseOffset, epoch, records);
    }

    static MemoryRecords buildBatch(
        long timestamp,
        long baseOffset,
        int epoch,
        List<String> records
    ) {
        ByteBuffer buffer = ByteBuffer.allocate(512);
        BatchBuilder<String> builder = new BatchBuilder<>(
            buffer,
            Builder.SERDE,
            CompressionType.NONE,
            baseOffset,
            timestamp,
            false,
            epoch,
            512
        );

        for (String record : records) {
            builder.appendRecord(record, null);
        }

        return builder.build();
    }

    static RaftClientTestContext initializeAsLeader(int localId, Set<Integer> voters, int epoch) throws Exception {
        if (epoch <= 0) {
            throw new IllegalArgumentException("Cannot become leader in epoch " + epoch);
        }

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withUnknownLeader(epoch - 1)
            .build();

        context.assertUnknownLeader(epoch - 1);
        context.becomeLeader();
        return context;
    }

    public void becomeLeader() throws Exception {
        int currentEpoch = currentEpoch();
        time.sleep(electionTimeoutMs * 2);
        expectAndGrantVotes(currentEpoch + 1);
        expectBeginEpoch(currentEpoch + 1);
    }

    public OptionalInt currentLeader() {
        return currentLeaderAndEpoch().leaderId();
    }

    public int currentEpoch() {
        return currentLeaderAndEpoch().epoch();
    }

    LeaderAndEpoch currentLeaderAndEpoch() {
        ElectionState election = quorumStateStore.readElectionState();
        return new LeaderAndEpoch(election.leaderIdOpt, election.epoch);
    }

    void expectAndGrantVotes(int epoch) throws Exception {
        pollUntilRequest();

        List<RaftRequest.Outbound> voteRequests = collectVoteRequests(epoch,
            log.lastFetchedEpoch(), log.endOffset().offset);

        for (RaftRequest.Outbound request : voteRequests) {
            VoteResponseData voteResponse = voteResponse(true, Optional.empty(), epoch);
            deliverResponse(request.correlationId, request.destinationId(), voteResponse);
        }

        client.poll();
        assertElectedLeader(epoch, localIdOrThrow());
    }

    private int localIdOrThrow() {
        return localId.orElseThrow(() -> new AssertionError("Required local id is not defined"));
    }

    private void expectBeginEpoch(int epoch) throws Exception {
        pollUntilRequest();
        for (RaftRequest.Outbound request : collectBeginEpochRequests(epoch)) {
            BeginQuorumEpochResponseData beginEpochResponse = beginEpochResponse(epoch, localIdOrThrow());
            deliverResponse(request.correlationId, request.destinationId(), beginEpochResponse);
        }
        client.poll();
    }

    public void pollUntil(TestCondition condition) throws InterruptedException {
        TestUtils.waitForCondition(() -> {
            client.poll();
            return condition.conditionMet();
        }, 5000, "Condition failed to be satisfied before timeout");
    }

    void pollUntilResponse() throws InterruptedException {
        pollUntil(() -> !sentResponses.isEmpty());
    }

    void pollUntilRequest() throws InterruptedException {
        pollUntil(channel::hasSentRequests);
    }

    void assertVotedCandidate(int epoch, int leaderId) throws IOException {
        assertEquals(ElectionState.withVotedCandidate(epoch, leaderId, voters), quorumStateStore.readElectionState());
    }

    public void assertElectedLeader(int epoch, int leaderId) throws IOException {
        assertEquals(ElectionState.withElectedLeader(epoch, leaderId, voters), quorumStateStore.readElectionState());
    }

    void assertUnknownLeader(int epoch) throws IOException {
        assertEquals(ElectionState.withUnknownLeader(epoch, voters), quorumStateStore.readElectionState());
    }

    void assertResignedLeader(int epoch, int leaderId) throws IOException {
        assertTrue(client.quorum().isResigned());
        assertEquals(ElectionState.withElectedLeader(epoch, leaderId, voters), quorumStateStore.readElectionState());
    }

    DescribeQuorumResponseData collectDescribeQuorumResponse() {
        List<RaftResponse.Outbound> sentMessages = drainSentResponses(ApiKeys.DESCRIBE_QUORUM);
        assertEquals(1, sentMessages.size());
        RaftResponse.Outbound raftMessage = sentMessages.get(0);
        assertTrue(
            raftMessage.data() instanceof DescribeQuorumResponseData,
            "Unexpected request type " + raftMessage.data());
        return (DescribeQuorumResponseData) raftMessage.data();
    }

    void assertSentDescribeQuorumResponse(
        int leaderId,
        int leaderEpoch,
        long highWatermark,
        List<ReplicaState> voterStates,
        List<ReplicaState> observerStates
    ) {
        DescribeQuorumResponseData response = collectDescribeQuorumResponse();

        DescribeQuorumResponseData.PartitionData partitionData = new DescribeQuorumResponseData.PartitionData()
            .setErrorCode(Errors.NONE.code())
            .setLeaderId(leaderId)
            .setLeaderEpoch(leaderEpoch)
            .setHighWatermark(highWatermark)
            .setCurrentVoters(voterStates)
            .setObservers(observerStates);
        DescribeQuorumResponseData expectedResponse = DescribeQuorumResponse.singletonResponse(
            metadataPartition,
            partitionData
        );
        assertEquals(expectedResponse, response);
    }

    int assertSentVoteRequest(int epoch, int lastEpoch, long lastEpochOffset, int numVoteReceivers) {
        List<RaftRequest.Outbound> voteRequests = collectVoteRequests(epoch, lastEpoch, lastEpochOffset);
        assertEquals(numVoteReceivers, voteRequests.size());
        return voteRequests.iterator().next().correlationId();
    }

    void assertSentVoteResponse(
            Errors error
    ) {
        List<RaftResponse.Outbound> sentMessages = drainSentResponses(ApiKeys.VOTE);
        assertEquals(1, sentMessages.size());
        RaftMessage raftMessage = sentMessages.get(0);
        assertTrue(raftMessage.data() instanceof VoteResponseData);
        VoteResponseData response = (VoteResponseData) raftMessage.data();

        assertEquals(error, Errors.forCode(response.errorCode()));
    }

    void assertSentVoteResponse(
        Errors error,
        int epoch,
        OptionalInt leaderId,
        boolean voteGranted
    ) {
        List<RaftResponse.Outbound> sentMessages = drainSentResponses(ApiKeys.VOTE);
        assertEquals(1, sentMessages.size());
        RaftMessage raftMessage = sentMessages.get(0);
        assertTrue(raftMessage.data() instanceof VoteResponseData);
        VoteResponseData response = (VoteResponseData) raftMessage.data();
        assertTrue(hasValidTopicPartition(response, metadataPartition));

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
                VoteRequestData.PartitionData partitionRequest = unwrap(request);

                assertEquals(epoch, partitionRequest.candidateEpoch());
                assertEquals(localIdOrThrow(), partitionRequest.candidateId());
                assertEquals(lastEpoch, partitionRequest.lastOffsetEpoch());
                assertEquals(lastEpochOffset, partitionRequest.lastOffset());
                voteRequests.add((RaftRequest.Outbound) raftMessage);
            }
        }
        return voteRequests;
    }

    void deliverRequest(ApiMessage request) {
        RaftRequest.Inbound inboundRequest = new RaftRequest.Inbound(
            channel.newCorrelationId(), request, time.milliseconds());
        inboundRequest.completion.whenComplete((response, exception) -> {
            if (exception != null) {
                throw new RuntimeException(exception);
            } else {
                sentResponses.add(response);
            }
        });
        client.handle(inboundRequest);
    }

    void deliverResponse(int correlationId, int sourceId, ApiMessage response) {
        channel.mockReceive(new RaftResponse.Inbound(correlationId, response, sourceId));
    }

    int assertSentBeginQuorumEpochRequest(int epoch, int numBeginEpochRequests) {
        List<RaftRequest.Outbound> requests = collectBeginEpochRequests(epoch);
        assertEquals(numBeginEpochRequests, requests.size());
        return requests.get(0).correlationId;
    }

    private List<RaftResponse.Outbound> drainSentResponses(
        ApiKeys apiKey
    ) {
        List<RaftResponse.Outbound> res = new ArrayList<>();
        Iterator<RaftResponse.Outbound> iterator = sentResponses.iterator();
        while (iterator.hasNext()) {
            RaftResponse.Outbound response = iterator.next();
            if (response.data.apiKey() == apiKey.id) {
                res.add(response);
                iterator.remove();
            }
        }
        return res;
    }

    void assertSentBeginQuorumEpochResponse(
            Errors responseError
    ) {
        List<RaftResponse.Outbound> sentMessages = drainSentResponses(ApiKeys.BEGIN_QUORUM_EPOCH);
        assertEquals(1, sentMessages.size());
        RaftMessage raftMessage = sentMessages.get(0);
        assertTrue(raftMessage.data() instanceof BeginQuorumEpochResponseData);
        BeginQuorumEpochResponseData response = (BeginQuorumEpochResponseData) raftMessage.data();
        assertEquals(responseError, Errors.forCode(response.errorCode()));
    }

    void assertSentBeginQuorumEpochResponse(
        Errors partitionError,
        int epoch,
        OptionalInt leaderId
    ) {
        List<RaftResponse.Outbound> sentMessages = drainSentResponses(ApiKeys.BEGIN_QUORUM_EPOCH);
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

    int assertSentEndQuorumEpochRequest(int epoch, int destinationId) {
        List<RaftRequest.Outbound> endQuorumRequests = collectEndQuorumRequests(
            epoch, Collections.singleton(destinationId), Optional.empty());
        assertEquals(1, endQuorumRequests.size());
        return endQuorumRequests.get(0).correlationId();
    }

    void assertSentEndQuorumEpochResponse(
        Errors responseError
    ) {
        List<RaftResponse.Outbound> sentMessages = drainSentResponses(ApiKeys.END_QUORUM_EPOCH);
        assertEquals(1, sentMessages.size());
        RaftMessage raftMessage = sentMessages.get(0);
        assertTrue(raftMessage.data() instanceof EndQuorumEpochResponseData);
        EndQuorumEpochResponseData response = (EndQuorumEpochResponseData) raftMessage.data();
        assertEquals(responseError, Errors.forCode(response.errorCode()));
    }

    void assertSentEndQuorumEpochResponse(
        Errors partitionError,
        int epoch,
        OptionalInt leaderId
    ) {
        List<RaftResponse.Outbound> sentMessages = drainSentResponses(ApiKeys.END_QUORUM_EPOCH);
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
        List<RaftRequest.Outbound> sentRequests = channel.drainSentRequests(Optional.of(ApiKeys.FETCH));
        assertEquals(1, sentRequests.size());
        return sentRequests.get(0);
    }

    int assertSentFetchRequest(
        int epoch,
        long fetchOffset,
        int lastFetchedEpoch
    ) {
        List<RaftRequest.Outbound> sentMessages = channel.drainSendQueue();
        assertEquals(1, sentMessages.size());

        RaftRequest.Outbound raftRequest = sentMessages.get(0);
        assertFetchRequestData(raftRequest, epoch, fetchOffset, lastFetchedEpoch);
        return raftRequest.correlationId();
    }

    FetchResponseData.PartitionData assertSentFetchPartitionResponse() {
        List<RaftResponse.Outbound> sentMessages = drainSentResponses(ApiKeys.FETCH);
        assertEquals(
            1, sentMessages.size(), "Found unexpected sent messages " + sentMessages);
        RaftResponse.Outbound raftMessage = sentMessages.get(0);
        assertEquals(ApiKeys.FETCH.id, raftMessage.data.apiKey());
        FetchResponseData response = (FetchResponseData) raftMessage.data();
        assertEquals(Errors.NONE, Errors.forCode(response.errorCode()));

        assertEquals(1, response.responses().size());
        assertEquals(metadataPartition.topic(), response.responses().get(0).topic());
        assertEquals(1, response.responses().get(0).partitions().size());
        return response.responses().get(0).partitions().get(0);
    }

    void assertSentFetchPartitionResponse(Errors topLevelError) {
        List<RaftResponse.Outbound> sentMessages = drainSentResponses(ApiKeys.FETCH);
        assertEquals(
            1, sentMessages.size(), "Found unexpected sent messages " + sentMessages);
        RaftResponse.Outbound raftMessage = sentMessages.get(0);
        assertEquals(ApiKeys.FETCH.id, raftMessage.data.apiKey());
        FetchResponseData response = (FetchResponseData) raftMessage.data();
        assertEquals(topLevelError, Errors.forCode(response.errorCode()));
    }


    MemoryRecords assertSentFetchPartitionResponse(
        Errors error,
        int epoch,
        OptionalInt leaderId
    ) {
        FetchResponseData.PartitionData partitionResponse = assertSentFetchPartitionResponse();
        assertEquals(error, Errors.forCode(partitionResponse.errorCode()));
        assertEquals(epoch, partitionResponse.currentLeader().leaderEpoch());
        assertEquals(leaderId.orElse(-1), partitionResponse.currentLeader().leaderId());
        assertEquals(-1, partitionResponse.divergingEpoch().endOffset());
        assertEquals(-1, partitionResponse.divergingEpoch().epoch());
        assertEquals(-1, partitionResponse.snapshotId().endOffset());
        assertEquals(-1, partitionResponse.snapshotId().epoch());
        return (MemoryRecords) partitionResponse.records();
    }

    MemoryRecords assertSentFetchPartitionResponse(
        long highWatermark,
        int leaderEpoch
    ) {
        FetchResponseData.PartitionData partitionResponse = assertSentFetchPartitionResponse();
        assertEquals(Errors.NONE, Errors.forCode(partitionResponse.errorCode()));
        assertEquals(leaderEpoch, partitionResponse.currentLeader().leaderEpoch());
        assertEquals(highWatermark, partitionResponse.highWatermark());
        assertEquals(-1, partitionResponse.divergingEpoch().endOffset());
        assertEquals(-1, partitionResponse.divergingEpoch().epoch());
        assertEquals(-1, partitionResponse.snapshotId().endOffset());
        assertEquals(-1, partitionResponse.snapshotId().epoch());
        return (MemoryRecords) partitionResponse.records();
    }

    RaftRequest.Outbound assertSentFetchSnapshotRequest() {
        List<RaftRequest.Outbound> sentRequests = channel.drainSentRequests(Optional.of(ApiKeys.FETCH_SNAPSHOT));
        assertEquals(1, sentRequests.size());

        return sentRequests.get(0);
    }

    void assertSentFetchSnapshotResponse(Errors responseError) {
        List<RaftResponse.Outbound> sentMessages = drainSentResponses(ApiKeys.FETCH_SNAPSHOT);
        assertEquals(1, sentMessages.size());

        RaftMessage message = sentMessages.get(0);
        assertTrue(message.data() instanceof FetchSnapshotResponseData);

        FetchSnapshotResponseData response = (FetchSnapshotResponseData) message.data();
        assertEquals(responseError, Errors.forCode(response.errorCode()));
    }

    Optional<FetchSnapshotResponseData.PartitionSnapshot> assertSentFetchSnapshotResponse(TopicPartition topicPartition) {
        List<RaftResponse.Outbound> sentMessages = drainSentResponses(ApiKeys.FETCH_SNAPSHOT);
        assertEquals(1, sentMessages.size());

        RaftMessage message = sentMessages.get(0);
        assertTrue(message.data() instanceof FetchSnapshotResponseData);

        FetchSnapshotResponseData response = (FetchSnapshotResponseData) message.data();
        assertEquals(Errors.NONE, Errors.forCode(response.errorCode()));

        return FetchSnapshotResponse.forTopicPartition(response, topicPartition);
    }

    List<RaftRequest.Outbound> collectEndQuorumRequests(
        int epoch,
        Set<Integer> destinationIdSet,
        Optional<List<Integer>> preferredSuccessorsOpt
    ) {
        List<RaftRequest.Outbound> endQuorumRequests = new ArrayList<>();
        Set<Integer> collectedDestinationIdSet = new HashSet<>();
        for (RaftMessage raftMessage : channel.drainSendQueue()) {
            if (raftMessage.data() instanceof EndQuorumEpochRequestData) {
                EndQuorumEpochRequestData request = (EndQuorumEpochRequestData) raftMessage.data();

                EndQuorumEpochRequestData.PartitionData partitionRequest =
                    request.topics().get(0).partitions().get(0);

                assertEquals(epoch, partitionRequest.leaderEpoch());
                assertEquals(localIdOrThrow(), partitionRequest.leaderId());
                preferredSuccessorsOpt.ifPresent(preferredSuccessors -> {
                    assertEquals(preferredSuccessors, partitionRequest.preferredSuccessors());
                });

                RaftRequest.Outbound outboundRequest = (RaftRequest.Outbound) raftMessage;
                collectedDestinationIdSet.add(outboundRequest.destinationId());
                endQuorumRequests.add(outboundRequest);
            }
        }
        assertEquals(destinationIdSet, collectedDestinationIdSet);
        return endQuorumRequests;
    }

    void discoverLeaderAsObserver(
        int leaderId,
        int epoch
    ) throws Exception {
        pollUntilRequest();
        RaftRequest.Outbound fetchRequest = assertSentFetchRequest();
        assertTrue(voters.contains(fetchRequest.destinationId()));
        assertFetchRequestData(fetchRequest, 0, 0L, 0);

        deliverResponse(fetchRequest.correlationId, fetchRequest.destinationId(),
            fetchResponse(epoch, leaderId, MemoryRecords.EMPTY, 0L, Errors.NONE));
        client.poll();
        assertElectedLeader(epoch, leaderId);
    }

    private List<RaftRequest.Outbound> collectBeginEpochRequests(int epoch) {
        List<RaftRequest.Outbound> requests = new ArrayList<>();
        for (RaftRequest.Outbound raftRequest : channel.drainSentRequests(Optional.of(ApiKeys.BEGIN_QUORUM_EPOCH))) {
            assertTrue(raftRequest.data() instanceof BeginQuorumEpochRequestData);
            BeginQuorumEpochRequestData request = (BeginQuorumEpochRequestData) raftRequest.data();

            BeginQuorumEpochRequestData.PartitionData partitionRequest =
                request.topics().get(0).partitions().get(0);

            assertEquals(epoch, partitionRequest.leaderEpoch());
            assertEquals(localIdOrThrow(), partitionRequest.leaderId());
            requests.add(raftRequest);
        }
        return requests;
    }

    private static RaftConfig.AddressSpec mockAddress(int id) {
        return new RaftConfig.InetAddressSpec(new InetSocketAddress("localhost", 9990 + id));
    }

    EndQuorumEpochResponseData endEpochResponse(
        int epoch,
        OptionalInt leaderId
    ) {
        return EndQuorumEpochResponse.singletonResponse(
            Errors.NONE,
            metadataPartition,
            Errors.NONE,
            epoch,
            leaderId.orElse(-1)
        );
    }

    EndQuorumEpochRequestData endEpochRequest(
        int epoch,
        int leaderId,
        List<Integer> preferredSuccessors
    ) {
        return EndQuorumEpochRequest.singletonRequest(
            metadataPartition,
            epoch,
            leaderId,
            preferredSuccessors
        );
    }

    EndQuorumEpochRequestData endEpochRequest(
        String clusterId,
        int epoch,
        int leaderId,
        List<Integer> preferredSuccessors
    ) {
        return EndQuorumEpochRequest.singletonRequest(
            metadataPartition,
            clusterId,
            epoch,
            leaderId,
            preferredSuccessors
        );
    }

    BeginQuorumEpochRequestData beginEpochRequest(String clusterId, int epoch, int leaderId) {
        return BeginQuorumEpochRequest.singletonRequest(
            metadataPartition,
            clusterId,
            epoch,
            leaderId
        );
    }

    BeginQuorumEpochRequestData beginEpochRequest(int epoch, int leaderId) {
        return BeginQuorumEpochRequest.singletonRequest(
            metadataPartition,
            epoch,
            leaderId
        );
    }

    private BeginQuorumEpochResponseData beginEpochResponse(int epoch, int leaderId) {
        return BeginQuorumEpochResponse.singletonResponse(
            Errors.NONE,
            metadataPartition,
            Errors.NONE,
            epoch,
            leaderId
        );
    }

    VoteRequestData voteRequest(int epoch, int candidateId, int lastEpoch, long lastEpochOffset) {
        return VoteRequest.singletonRequest(
            metadataPartition,
            clusterId.toString(),
            epoch,
            candidateId,
            lastEpoch,
            lastEpochOffset
        );
    }

    VoteRequestData voteRequest(
        String clusterId,
        int epoch,
        int candidateId,
        int lastEpoch,
        long lastEpochOffset
    ) {
        return VoteRequest.singletonRequest(
                metadataPartition,
                clusterId,
                epoch,
                candidateId,
                lastEpoch,
                lastEpochOffset
        );
    }

    VoteResponseData voteResponse(boolean voteGranted, Optional<Integer> leaderId, int epoch) {
        return VoteResponse.singletonResponse(
            Errors.NONE,
            metadataPartition,
            Errors.NONE,
            epoch,
            leaderId.orElse(-1),
            voteGranted
        );
    }

    private VoteRequestData.PartitionData unwrap(VoteRequestData voteRequest) {
        assertTrue(RaftUtil.hasValidTopicPartition(voteRequest, metadataPartition));
        return voteRequest.topics().get(0).partitions().get(0);
    }

    static void assertMatchingRecords(
        String[] expected,
        Records actual
    ) {
        List<Record> recordList = Utils.toList(actual.records());
        assertEquals(expected.length, recordList.size());
        for (int i = 0; i < expected.length; i++) {
            Record record = recordList.get(i);
            assertEquals(expected[i], Utils.utf8(record.value()),
                "Record at offset " + record.offset() + " does not match expected");
        }
    }

    static void verifyLeaderChangeMessage(
        int leaderId,
        List<Integer> voters,
        List<Integer> grantingVoters,
        ByteBuffer recordKey,
        ByteBuffer recordValue
    ) {
        assertEquals(ControlRecordType.LEADER_CHANGE, ControlRecordType.parse(recordKey));

        LeaderChangeMessage leaderChangeMessage = ControlRecordUtils.deserializeLeaderChangeMessage(recordValue);
        assertEquals(leaderId, leaderChangeMessage.leaderId());
        assertEquals(voters.stream().map(voterId -> new Voter().setVoterId(voterId)).collect(Collectors.toList()),
            leaderChangeMessage.voters());
        assertEquals(grantingVoters.stream().map(voterId -> new Voter().setVoterId(voterId)).collect(Collectors.toSet()),
            new HashSet<>(leaderChangeMessage.grantingVoters()));
    }

    void assertFetchRequestData(
        RaftRequest.Outbound message,
        int epoch,
        long fetchOffset,
        int lastFetchedEpoch
    ) {
        assertTrue(
            message.data() instanceof FetchRequestData,
            "unexpected request type " + message.data()
        );
        FetchRequestData request = (FetchRequestData) message.data();
        assertEquals(KafkaRaftClient.MAX_FETCH_SIZE_BYTES, request.maxBytes());
        assertEquals(fetchMaxWaitMs, request.maxWaitMs());

        assertEquals(1, request.topics().size());
        assertEquals(metadataPartition.topic(), request.topics().get(0).topic());
        assertEquals(1, request.topics().get(0).partitions().size());

        FetchRequestData.FetchPartition fetchPartition = request.topics().get(0).partitions().get(0);
        assertEquals(epoch, fetchPartition.currentLeaderEpoch());
        assertEquals(fetchOffset, fetchPartition.fetchOffset());
        assertEquals(lastFetchedEpoch, fetchPartition.lastFetchedEpoch());
        assertEquals(localId.orElse(-1), request.replicaState().replicaId());

        // Assert that voters have flushed up to the fetch offset
        if (localId.isPresent() && voters.contains(localId.getAsInt())) {
            assertEquals(
                log.firstUnflushedOffset(),
                fetchOffset,
                String.format(
                    "expected voters have the fetch offset (%s) be the same as the unflushed offset (%s)",
                    log.firstUnflushedOffset(),
                    fetchOffset
                )
            );
        } else {
            assertFalse(log.flushedSinceLastChecked(), "KRaft client should not explicitly flush when it is an observer");
        }
    }

    FetchRequestData fetchRequest(
        int epoch,
        int replicaId,
        long fetchOffset,
        int lastFetchedEpoch,
        int maxWaitTimeMs
    ) {
        return fetchRequest(
            epoch,
            clusterId.toString(),
            replicaId,
            fetchOffset,
            lastFetchedEpoch,
            maxWaitTimeMs
        );
    }

    FetchRequestData fetchRequest(
        int epoch,
        String clusterId,
        int replicaId,
        long fetchOffset,
        int lastFetchedEpoch,
        int maxWaitTimeMs
    ) {
        FetchRequestData request = RaftUtil.singletonFetchRequest(metadataPartition, metadataTopicId, fetchPartition -> {
            fetchPartition
                .setCurrentLeaderEpoch(epoch)
                .setLastFetchedEpoch(lastFetchedEpoch)
                .setFetchOffset(fetchOffset);
        });
        return request
            .setMaxWaitMs(maxWaitTimeMs)
            .setClusterId(clusterId)
            .setReplicaState(new FetchRequestData.ReplicaState().setReplicaId(replicaId));
    }

    FetchResponseData fetchResponse(
        int epoch,
        int leaderId,
        Records records,
        long highWatermark,
        Errors error
    ) {
        return RaftUtil.singletonFetchResponse(metadataPartition, metadataTopicId, Errors.NONE, partitionData -> {
            partitionData
                .setRecords(records)
                .setErrorCode(error.code())
                .setHighWatermark(highWatermark);

            partitionData.currentLeader()
                .setLeaderEpoch(epoch)
                .setLeaderId(leaderId);
        });
    }

    FetchResponseData divergingFetchResponse(
        int epoch,
        int leaderId,
        long divergingEpochEndOffset,
        int divergingEpoch,
        long highWatermark
    ) {
        return RaftUtil.singletonFetchResponse(metadataPartition, metadataTopicId, Errors.NONE, partitionData -> {
            partitionData.setHighWatermark(highWatermark);

            partitionData.currentLeader()
                .setLeaderEpoch(epoch)
                .setLeaderId(leaderId);

            partitionData.divergingEpoch()
                .setEpoch(divergingEpoch)
                .setEndOffset(divergingEpochEndOffset);
        });
    }

    public void advanceLocalLeaderHighWatermarkToLogEndOffset() throws InterruptedException {
        assertEquals(localId, currentLeader());
        long localLogEndOffset = log.endOffset().offset;
        Set<Integer> followers = voters.stream().filter(voter -> voter != localId.getAsInt()).collect(Collectors.toSet());

        // Send a request from every follower
        for (int follower : followers) {
            deliverRequest(
                fetchRequest(currentEpoch(), follower, localLogEndOffset, currentEpoch(), 0)
            );
            pollUntilResponse();
            assertSentFetchPartitionResponse(Errors.NONE, currentEpoch(), localId);
        }

        pollUntil(() -> OptionalLong.of(localLogEndOffset).equals(client.highWatermark()));
    }

    static class MockListener implements RaftClient.Listener<String> {
        private final List<Batch<String>> commits = new ArrayList<>();
        private final List<BatchReader<String>> savedBatches = new ArrayList<>();
        private final Map<Integer, Long> claimedEpochStartOffsets = new HashMap<>();
        private LeaderAndEpoch currentLeaderAndEpoch = new LeaderAndEpoch(OptionalInt.empty(), 0);
        private final OptionalInt localId;
        private Optional<SnapshotReader<String>> snapshot = Optional.empty();
        private boolean readCommit = true;

        MockListener(OptionalInt localId) {
            this.localId = localId;
        }

        int numCommittedBatches() {
            return commits.size();
        }

        Long claimedEpochStartOffset(int epoch) {
            return claimedEpochStartOffsets.get(epoch);
        }

        LeaderAndEpoch currentLeaderAndEpoch() {
            return currentLeaderAndEpoch;
        }

        List<Batch<String>> committedBatches() {
            return commits;
        }

        Batch<String> lastCommit() {
            if (commits.isEmpty()) {
                return null;
            } else {
                return commits.get(commits.size() - 1);
            }
        }

        OptionalLong lastCommitOffset() {
            if (commits.isEmpty()) {
                return OptionalLong.empty();
            } else {
                return OptionalLong.of(commits.get(commits.size() - 1).lastOffset());
            }
        }

        OptionalInt currentClaimedEpoch() {
            if (localId.isPresent() && currentLeaderAndEpoch.isLeader(localId.getAsInt())) {
                return OptionalInt.of(currentLeaderAndEpoch.epoch());
            } else {
                return OptionalInt.empty();
            }
        }

        List<String> commitWithLastOffset(long lastOffset) {
            return commits.stream()
                .filter(batch -> batch.lastOffset() == lastOffset)
                .findFirst()
                .map(batch -> batch.records())
                .orElse(null);
        }

        Optional<SnapshotReader<String>> drainHandledSnapshot() {
            Optional<SnapshotReader<String>> temp = snapshot;
            snapshot = Optional.empty();
            return temp;
        }

        void updateReadCommit(boolean readCommit) {
            this.readCommit = readCommit;

            if (readCommit) {
                for (BatchReader<String> batch : savedBatches) {
                    readBatch(batch);
                }

                savedBatches.clear();
            }
        }

        void readBatch(BatchReader<String> reader) {
            try {
                while (reader.hasNext()) {
                    long nextOffset = lastCommitOffset().isPresent() ?
                        lastCommitOffset().getAsLong() + 1 : 0L;
                    Batch<String> batch = reader.next();
                    // We expect monotonic offsets, but not necessarily sequential
                    // offsets since control records will be filtered.
                    assertTrue(batch.baseOffset() >= nextOffset,
                        "Received non-monotonic commit " + batch +
                            ". We expected an offset at least as large as " + nextOffset);
                    commits.add(batch);
                }
            } finally {
                reader.close();
            }
        }

        @Override
        public void handleLeaderChange(LeaderAndEpoch leaderAndEpoch) {
            // We record the current committed offset as the claimed epoch's start
            // offset. This is useful to verify that the `handleLeaderChange` callback
            // was not received early on the leader.
            this.currentLeaderAndEpoch = leaderAndEpoch;

            currentClaimedEpoch().ifPresent(claimedEpoch -> {
                long claimedEpochStartOffset = lastCommitOffset().isPresent() ?
                    lastCommitOffset().getAsLong() : 0L;
                this.claimedEpochStartOffsets.put(leaderAndEpoch.epoch(), claimedEpochStartOffset);
            });
        }

        @Override
        public void handleCommit(BatchReader<String> reader) {
            if (readCommit) {
                readBatch(reader);
            } else {
                savedBatches.add(reader);
            }
        }

        @Override
        public void handleLoadSnapshot(SnapshotReader<String> reader) {
            snapshot.ifPresent(snapshot -> assertDoesNotThrow(snapshot::close));
            commits.clear();
            savedBatches.clear();
            snapshot = Optional.of(reader);
        }
    }
}
