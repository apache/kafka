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
//import java.nio.ByteBuffer;
import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.Collections;
//import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
//import java.util.OptionalInt;
//import java.util.OptionalLong;
import java.util.Random;
import java.util.Set;
//import java.util.concurrent.CompletableFuture;
//import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.common.TopicPartition;
//import org.apache.kafka.common.errors.ClusterAuthorizationException;
//import org.apache.kafka.common.errors.NotLeaderOrFollowerException;
import org.apache.kafka.common.message.BeginQuorumEpochRequestData;
import org.apache.kafka.common.message.BeginQuorumEpochResponseData;
//import org.apache.kafka.common.message.DescribeQuorumResponseData.ReplicaState;
//import org.apache.kafka.common.message.DescribeQuorumResponseData;
//import org.apache.kafka.common.message.EndQuorumEpochRequestData;
//import org.apache.kafka.common.message.EndQuorumEpochResponseData;
//import org.apache.kafka.common.message.FetchRequestData;
//import org.apache.kafka.common.message.FetchResponseData;
//import org.apache.kafka.common.message.LeaderChangeMessage.Voter;
//import org.apache.kafka.common.message.LeaderChangeMessage;
import org.apache.kafka.common.message.VoteRequestData;
import org.apache.kafka.common.message.VoteResponseData;
//import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
//import org.apache.kafka.common.record.CompressionType;
//import org.apache.kafka.common.record.ControlRecordType;
//import org.apache.kafka.common.record.ControlRecordUtils;
//import org.apache.kafka.common.record.MemoryRecords;
//import org.apache.kafka.common.record.MutableRecordBatch;
//import org.apache.kafka.common.record.Record;
//import org.apache.kafka.common.record.RecordBatch;
//import org.apache.kafka.common.record.Records;
//import org.apache.kafka.common.record.SimpleRecord;
//import org.apache.kafka.common.requests.BeginQuorumEpochRequest;
import org.apache.kafka.common.requests.BeginQuorumEpochResponse;
//import org.apache.kafka.common.requests.DescribeQuorumRequest;
//import org.apache.kafka.common.requests.DescribeQuorumResponse;
//import org.apache.kafka.common.requests.EndQuorumEpochRequest;
//import org.apache.kafka.common.requests.VoteRequest;
import org.apache.kafka.common.requests.VoteResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
//import org.apache.kafka.common.utils.Utils;
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
import static org.junit.jupiter.api.Assertions.assertTrue;

final class RaftClientTestContext {
    private static final TopicPartition METADATA_PARTITION = new TopicPartition("metadata", 0);
    static final int LOCAL_ID = 0;

    private static final int ELECTION_BACKOFF_MAX_MS = 100;
    static final int ELECTION_TIMEOUT_MS = 10000;
    private static final int FETCH_MAX_WAIT_MS = 0;
    // fetch timeout is usually larger than election timeout
    private static final int FETCH_TIMEOUT_MS = 50000;
    private static final int REQUEST_TIMEOUT_MS = 5000;
    private static final int RETRY_BACKOFF_MS = 50;

    private final MockNetworkChannel channel;
    private final Random random;

    final KafkaRaftClient client;
    final MockLog log;
    final MockTime time;
    final QuorumStateStore quorumStateStore;

    public static final class Builder {
        private final QuorumStateStore quorumStateStore = new MockQuorumStateStore();
        private final Random random = Mockito.spy(new Random(1));

        Builder updateQuorumStateStore(Consumer<QuorumStateStore> consumer) {
            consumer.accept(quorumStateStore);
            return this;
        }

        Builder updateRandom(Consumer<Random> consumer) {
            consumer.accept(random);
            return this;
        }

        RaftClientTestContext build(Set<Integer> voters) throws IOException {
            MockTime time = new MockTime();
            Metrics metrics = new Metrics(time);
            MockLog log = new MockLog(METADATA_PARTITION);
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

            return new RaftClientTestContext(client, log, channel, time, quorumStateStore, random);
        }
    }

    private RaftClientTestContext(
        KafkaRaftClient client,
        MockLog log,
        MockNetworkChannel channel,
        MockTime time,
        QuorumStateStore quorumStateStore,
        Random random
    ) {
        this.channel = channel;
        this.client = client;
        this.log = log;
        this.quorumStateStore = quorumStateStore;
        this.random = random;
        this.time = time;
    }

    static RaftClientTestContext initializeAsLeader(Set<Integer> voters, int epoch) throws Exception {
        MockTime time = new MockTime();
        Metrics metrics = new Metrics(time);
        MockLog log = new MockLog(METADATA_PARTITION);
        MockNetworkChannel channel = new MockNetworkChannel();
        QuorumStateStore quorumStateStore = new MockQuorumStateStore();
        Random random = Mockito.spy(new Random(1));

        if (epoch <= 0) {
            throw new IllegalArgumentException("Cannot become leader in epoch " + epoch);
        }

        Mockito.doReturn(0).when(random).nextInt(ELECTION_TIMEOUT_MS);

        ElectionState electionState = ElectionState.withUnknownLeader(epoch - 1, voters);
        quorumStateStore.writeElectionState(electionState);
        RaftClientTestContext client = buildClient(
            log,
            channel,
            time,
            quorumStateStore,
            random,
            voters,
            metrics
        );
        assertEquals(electionState, client.quorumStateStore.readElectionState());

        // Advance the clock so that we become a candidate
        time.sleep(ELECTION_TIMEOUT_MS);
        client.expectLeaderElection(voters, epoch);

        // Handle BeginEpoch
        client.pollUntilSend();
        for (RaftRequest.Outbound request : client.collectBeginEpochRequests(epoch)) {
            BeginQuorumEpochResponseData beginEpochResponse = beginEpochResponse(epoch, LOCAL_ID);
            client.deliverResponse(request.correlationId, request.destinationId(), beginEpochResponse);
        }

        client.client.poll();
        return client;
    }

    static RaftClientTestContext buildClient(Set<Integer> voters) throws IOException {
        MockTime time = new MockTime();
        Metrics metrics = new Metrics(time);
        MockLog log = new MockLog(METADATA_PARTITION);
        MockNetworkChannel channel = new MockNetworkChannel();
        QuorumStateStore quorumStateStore = new MockQuorumStateStore();
        Random random = Mockito.spy(new Random(1));

        return buildClient(log, channel, time, quorumStateStore, random, voters, new Metrics(time));
    }

    static RaftClientTestContext buildClient(
        MockLog log,
        MockNetworkChannel channel,
        MockTime time,
        QuorumStateStore quorumStateStore,
        Random random,
        Set<Integer> voters,
        Metrics metrics
    ) throws IOException {
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

        return new RaftClientTestContext(client, log, channel, time, quorumStateStore, random);
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

    int assertSentVoteRequest(int epoch, int lastEpoch, long lastEpochOffset) {
        List<RaftRequest.Outbound> voteRequests = collectVoteRequests(epoch, lastEpoch, lastEpochOffset);
        assertEquals(1, voteRequests.size());
        return voteRequests.iterator().next().correlationId();
    }

    private List<RaftRequest.Outbound> collectVoteRequests(
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

    private void deliverResponse(int correlationId, int sourceId, ApiMessage response) {
        channel.mockReceive(new RaftResponse.Inbound(correlationId, response, sourceId));
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

    private static VoteResponseData voteResponse(boolean voteGranted, Optional<Integer> leaderId, int epoch) {
        return VoteResponse.singletonResponse(
            Errors.NONE,
            METADATA_PARTITION,
            Errors.NONE,
            epoch,
            leaderId.orElse(-1),
            voteGranted
        );
    }
}
