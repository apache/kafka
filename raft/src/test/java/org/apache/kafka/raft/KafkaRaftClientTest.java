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
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.NotLeaderOrFollowerException;
import org.apache.kafka.common.message.BeginQuorumEpochRequestData;
import org.apache.kafka.common.message.BeginQuorumEpochResponseData;
import org.apache.kafka.common.message.DescribeQuorumResponseData;
import org.apache.kafka.common.message.DescribeQuorumResponseData.ReplicaState;
import org.apache.kafka.common.message.EndQuorumEpochRequestData;
import org.apache.kafka.common.message.EndQuorumEpochResponseData;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchResponseData;
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
import org.apache.kafka.common.requests.BeginQuorumEpochRequest;
import org.apache.kafka.common.requests.BeginQuorumEpochResponse;
import org.apache.kafka.common.requests.DescribeQuorumRequest;
import org.apache.kafka.common.requests.DescribeQuorumResponse;
import org.apache.kafka.common.requests.EndQuorumEpochRequest;
import org.apache.kafka.common.requests.VoteRequest;
import org.apache.kafka.common.requests.VoteResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.kafka.raft.RaftUtil.hasValidTopicPartition;
import static org.apache.kafka.test.TestUtils.assertFutureThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KafkaRaftClientTest {
    private static final TopicPartition METADATA_PARTITION = new TopicPartition("metadata", 0);

    private final int localId = 0;
    private final int electionTimeoutMs = 10000;
    private final int electionBackoffMaxMs = 100;
    private final int fetchTimeoutMs = 50000;   // fetch timeout is usually larger than election timeout
    private final int retryBackoffMs = 50;
    private final int requestTimeoutMs = 5000;
    private final int fetchMaxWaitMs = 0;

    private final MockTime time = new MockTime();
    private final MockLog log = new MockLog(METADATA_PARTITION);
    private final MockNetworkChannel channel = new MockNetworkChannel();
    private final Random random = Mockito.spy(new Random(1));
    private final QuorumStateStore quorumStateStore = new MockQuorumStateStore();

    @AfterEach
    public void cleanUp() throws IOException {
        quorumStateStore.clear();
    }

    private InetSocketAddress mockAddress(int id) {
        return new InetSocketAddress("localhost", 9990 + id);
    }

    private KafkaRaftClient buildClient(Set<Integer> voters) throws IOException {
        return buildClient(voters, new Metrics(time));
    }

    private KafkaRaftClient buildClient(Set<Integer> voters, Metrics metrics) throws IOException {
        LogContext logContext = new LogContext();
        QuorumState quorum = new QuorumState(localId, voters, electionTimeoutMs, fetchTimeoutMs,
            quorumStateStore, time, logContext, random);

        Map<Integer, InetSocketAddress> voterAddresses = voters.stream().collect(Collectors.toMap(
            Function.identity(),
            this::mockAddress
        ));

        KafkaRaftClient client = new KafkaRaftClient(channel, log, quorum, time, metrics,
            new MockFuturePurgatory<>(time), new MockFuturePurgatory<>(time), voterAddresses,
            electionBackoffMaxMs, retryBackoffMs, requestTimeoutMs, fetchMaxWaitMs, logContext, random);

        client.initialize();

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
        KafkaRaftClient client = buildClient(voters);
        assertEquals(1L, log.endOffset().offset);

        // FIXME: Is this test useful?
    }

    @Test
    public void testInitializeAsCandidateFromStateStore() throws Exception {
        // Need 3 node to require a 2-node majority
        Set<Integer> voters = Utils.mkSet(localId, 1, 2);
        quorumStateStore.writeElectionState(ElectionState.withVotedCandidate(2, localId, voters));

        KafkaRaftClient client = buildClient(voters);
        assertEquals(0L, log.endOffset().offset);

        // Send out vote requests.
        client.poll();

        List<RaftRequest.Outbound> voteRequests = collectVoteRequests(2, 0, 0);
        assertEquals(2, voteRequests.size());
    }

    @Test
    public void testInitializeAsCandidateAndBecomeLeader() throws Exception {
        final int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        KafkaRaftClient client = buildClient(voters);

        assertEquals(ElectionState.withUnknownLeader(0, voters), quorumStateStore.readElectionState());
        time.sleep(2 * electionTimeoutMs);

        pollUntilSend(client);
        assertEquals(ElectionState.withVotedCandidate(1, localId, voters), quorumStateStore.readElectionState());

        int correlationId = assertSentVoteRequest(1, 0, 0L);
        deliverResponse(correlationId, otherNodeId, voteResponse(true, Optional.empty(), 1));

        // Become leader after receiving the vote
        client.poll();
        assertEquals(ElectionState.withElectedLeader(1, localId, voters), quorumStateStore.readElectionState());
        long electionTimestamp = time.milliseconds();

        // Leader change record appended
        assertEquals(1, log.endOffset().offset);

        // Send BeginQuorumEpoch to voters
        client.poll();
        assertSentBeginQuorumEpochRequest(1);

        Records records = log.read(0, Isolation.UNCOMMITTED).records;
        RecordBatch batch = records.batches().iterator().next();
        assertTrue(batch.isControlBatch());

        Record record = batch.iterator().next();
        assertEquals(electionTimestamp, record.timestamp());
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

        // Enter the backoff period
        time.sleep(1);
        client.poll();
        assertEquals(ElectionState.withVotedCandidate(epoch, localId, voters), quorumStateStore.readElectionState());

        // After backoff, we will become a candidate again
        time.sleep(electionBackoffMaxMs);
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
    public void testEndQuorumStartsNewElectionAfterBackoffIfReceivedFromVotedCandidate() throws Exception {
        int otherNodeId = 1;
        int epoch = 2;

        int jitterMs = 85;
        Mockito.doReturn(jitterMs).when(random).nextInt(Mockito.anyInt());

        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        quorumStateStore.writeElectionState(ElectionState.withVotedCandidate(epoch, otherNodeId, voters));
        KafkaRaftClient client = buildClient(voters);

        deliverRequest(endEpochRequest(epoch, OptionalInt.empty(), otherNodeId, Collections.singletonList(localId)));
        client.poll();
        assertSentEndQuorumEpochResponse(Errors.NONE, epoch, OptionalInt.empty());

        time.sleep(electionBackoffMaxMs);
        client.poll();
        assertEquals(ElectionState.withVotedCandidate(epoch + 1, localId, voters),
            quorumStateStore.readElectionState());
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
        assertSentEndQuorumEpochResponse(Errors.NONE, epoch, OptionalInt.of(voter2));

        // Should become a candidate immediately
        client.poll();
        assertEquals(ElectionState.withVotedCandidate(epoch + 1, localId, voters), quorumStateStore.readElectionState());
    }

    @Test
    public void testLocalReadFromLeader() throws Exception {
        int otherNodeId = 1;
        int epoch = 2;

        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        KafkaRaftClient client = initializeAsLeader(voters, epoch);
        assertEquals(1L, log.endOffset().offset);
        assertEquals(OptionalLong.empty(), client.highWatermark());

        deliverRequest(fetchRequest(epoch, otherNodeId, 1L, epoch, 0));
        client.poll();
        assertEquals(1L, log.endOffset().offset);
        assertEquals(OptionalLong.of(1L), client.highWatermark());
        assertSentFetchResponse(Errors.NONE, epoch, OptionalInt.of(localId));

        SimpleRecord[] records = new SimpleRecord[] {
            new SimpleRecord("a".getBytes()),
            new SimpleRecord("b".getBytes())
        };
        client.append(MemoryRecords.withRecords(CompressionType.NONE, records), AckMode.LEADER, Integer.MAX_VALUE);
        client.poll();
        assertEquals(3L, log.endOffset().offset);
        assertEquals(OptionalLong.of(1L), client.highWatermark());

        validateLocalRead(client, new OffsetAndEpoch(1L, epoch), Isolation.COMMITTED, new SimpleRecord[0]);
        validateLocalRead(client, new OffsetAndEpoch(1L, epoch), Isolation.UNCOMMITTED, records);
        validateLocalRead(client, new OffsetAndEpoch(3L, epoch), Isolation.COMMITTED, new SimpleRecord[0]);
        validateLocalRead(client, new OffsetAndEpoch(3L, epoch), Isolation.UNCOMMITTED, new SimpleRecord[0]);

        deliverRequest(fetchRequest(epoch, otherNodeId, 3L, epoch, 0));
        client.poll();
        assertEquals(OptionalLong.of(3L), client.highWatermark());
        assertSentFetchResponse(Errors.NONE, epoch, OptionalInt.of(localId));

        validateLocalRead(client, new OffsetAndEpoch(1L, epoch), Isolation.COMMITTED, records);
    }

    @Test
    public void testDelayedLocalReadFromLeader() throws Exception {
        int otherNodeId = 1;
        int epoch = 2;

        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        KafkaRaftClient client = initializeAsLeader(voters, epoch);
        assertEquals(1L, log.endOffset().offset);
        assertEquals(OptionalLong.empty(), client.highWatermark());

        deliverRequest(fetchRequest(epoch, otherNodeId, 1L, epoch, 0));
        client.poll();
        assertEquals(1L, log.endOffset().offset);
        assertEquals(OptionalLong.of(1L), client.highWatermark());
        assertSentFetchResponse(Errors.NONE, epoch, OptionalInt.of(localId));

        CompletableFuture<Records> logEndReadFuture = client.read(new OffsetAndEpoch(1L, epoch),
            Isolation.UNCOMMITTED, 500);
        assertFalse(logEndReadFuture.isDone());

        CompletableFuture<Records> highWatermarkReadFuture = client.read(new OffsetAndEpoch(1L, epoch),
            Isolation.COMMITTED, 500);
        assertFalse(logEndReadFuture.isDone());

        SimpleRecord[] records = new SimpleRecord[] {
            new SimpleRecord("a".getBytes()),
            new SimpleRecord("b".getBytes())
        };
        client.append(MemoryRecords.withRecords(CompressionType.NONE, records), AckMode.LEADER, Integer.MAX_VALUE);
        client.poll();
        assertEquals(3L, log.endOffset().offset);
        assertEquals(OptionalLong.of(1L), client.highWatermark());

        assertTrue(logEndReadFuture.isDone());
        assertMatchingRecords(records, logEndReadFuture.get());
        assertFalse(highWatermarkReadFuture.isDone());

        deliverRequest(fetchRequest(epoch, otherNodeId, 3L, epoch, 0));
        client.poll();
        assertEquals(OptionalLong.of(3L), client.highWatermark());
        assertSentFetchResponse(Errors.NONE, epoch, OptionalInt.of(localId));

        assertTrue(highWatermarkReadFuture.isDone());
        assertMatchingRecords(records, highWatermarkReadFuture.get());
    }

    @Test
    public void testLocalReadFromFollower() throws Exception {
        int otherNodeId = 1;
        int epoch = 2;

        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        quorumStateStore.writeElectionState(ElectionState.withElectedLeader(epoch, otherNodeId, voters));
        KafkaRaftClient client = buildClient(voters);

        SimpleRecord[] records1 = new SimpleRecord[] {
            new SimpleRecord("a".getBytes()),
            new SimpleRecord("b".getBytes())
        };
        fetchFromLeader(client, otherNodeId, epoch, new OffsetAndEpoch(0, 0), records1, 2L);
        assertEquals(2L, log.endOffset().offset);
        assertEquals(OptionalLong.of(2L), client.highWatermark());

        validateLocalRead(client, new OffsetAndEpoch(0, 0), Isolation.COMMITTED, records1);
        validateLocalRead(client, new OffsetAndEpoch(0, 0), Isolation.UNCOMMITTED, records1);
        validateLocalRead(client, new OffsetAndEpoch(2L, epoch), Isolation.COMMITTED, new SimpleRecord[0]);
        validateLocalRead(client, new OffsetAndEpoch(2L, epoch), Isolation.UNCOMMITTED, new SimpleRecord[0]);

        SimpleRecord[] records2 = new SimpleRecord[] {
            new SimpleRecord("c".getBytes()),
            new SimpleRecord("d".getBytes()),
            new SimpleRecord("e".getBytes())
        };
        fetchFromLeader(client, otherNodeId, epoch, new OffsetAndEpoch(2L, epoch), records2, 2L);
        assertEquals(5L, log.endOffset().offset);
        assertEquals(OptionalLong.of(2L), client.highWatermark());

        validateLocalRead(client, new OffsetAndEpoch(2L, epoch), Isolation.COMMITTED, new SimpleRecord[0]);
        validateLocalRead(client, new OffsetAndEpoch(2L, epoch), Isolation.UNCOMMITTED, records2);
        validateLocalRead(client, new OffsetAndEpoch(5L, epoch), Isolation.COMMITTED, new SimpleRecord[0]);
        validateLocalRead(client, new OffsetAndEpoch(5L, epoch), Isolation.UNCOMMITTED, new SimpleRecord[0]);

        fetchFromLeader(client, otherNodeId, epoch, new OffsetAndEpoch(5L, epoch), new SimpleRecord[0], 5L);
        assertEquals(5L, log.endOffset().offset);
        assertEquals(OptionalLong.of(5L), client.highWatermark());

        validateLocalRead(client, new OffsetAndEpoch(2L, epoch), Isolation.COMMITTED, records2);
        validateLocalRead(client, new OffsetAndEpoch(5L, epoch), Isolation.COMMITTED, new SimpleRecord[0]);
    }

    @Test
    public void testDelayedLocalReadFromFollowerToHighWatermark() throws Exception {
        int otherNodeId = 1;
        int epoch = 2;

        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        quorumStateStore.writeElectionState(ElectionState.withElectedLeader(epoch, otherNodeId, voters));
        KafkaRaftClient client = buildClient(voters);

        SimpleRecord[] records = new SimpleRecord[] {
            new SimpleRecord("a".getBytes()),
            new SimpleRecord("b".getBytes())
        };
        fetchFromLeader(client, otherNodeId, epoch, new OffsetAndEpoch(0, 0), records, 0L);
        assertEquals(2L, log.endOffset().offset);
        assertEquals(OptionalLong.of(0L), client.highWatermark());

        CompletableFuture<Records> future = client.read(new OffsetAndEpoch(0, 0),
            Isolation.COMMITTED, 500);
        assertFalse(future.isDone());

        fetchFromLeader(client, otherNodeId, epoch, new OffsetAndEpoch(2L, epoch), new SimpleRecord[0], 2L);
        assertEquals(2L, log.endOffset().offset);
        assertEquals(OptionalLong.of(2L), client.highWatermark());
        assertTrue(future.isDone());
        assertMatchingRecords(records, future.get());
    }

    @Test
    public void testDelayedLocalReadFromFollowerToHighWatermarkTimeout() throws Exception {
        int otherNodeId = 1;
        int epoch = 2;

        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        quorumStateStore.writeElectionState(ElectionState.withElectedLeader(epoch, otherNodeId, voters));
        KafkaRaftClient client = buildClient(voters);

        SimpleRecord[] records1 = new SimpleRecord[] {
            new SimpleRecord("a".getBytes()),
            new SimpleRecord("b".getBytes())
        };
        fetchFromLeader(client, otherNodeId, epoch, new OffsetAndEpoch(0, 0), records1, 0L);
        assertEquals(2L, log.endOffset().offset);
        assertEquals(OptionalLong.of(0L), client.highWatermark());

        CompletableFuture<Records> future = client.read(new OffsetAndEpoch(0, 0),
            Isolation.COMMITTED, 500);
        assertFalse(future.isDone());

        time.sleep(500);
        client.poll();
        assertTrue(future.isDone());
        assertFutureThrows(future, org.apache.kafka.common.errors.TimeoutException.class);
    }

    @Test
    public void testLocalReadLogTruncationError() throws Exception {
        int otherNodeId = 1;
        int epoch = 2;

        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        quorumStateStore.writeElectionState(ElectionState.withElectedLeader(epoch, otherNodeId, voters));
        KafkaRaftClient client = buildClient(voters);

        SimpleRecord[] records = new SimpleRecord[] {
            new SimpleRecord("a".getBytes()),
            new SimpleRecord("b".getBytes())
        };
        fetchFromLeader(client, otherNodeId, epoch, new OffsetAndEpoch(0, 0), records, 2L);
        assertEquals(2L, log.endOffset().offset);
        assertEquals(OptionalLong.of(2L), client.highWatermark());

        CompletableFuture<Records> future = client.read(new OffsetAndEpoch(1, 1),
            Isolation.COMMITTED, 0);
        assertTrue(future.isDone());
        assertFutureThrows(future, LogTruncationException.class);
    }

    @Test
    public void testDelayedLocalReadLogTruncationErrorAfterUncleanElection() throws Exception {
        int otherNodeId = 1;
        int epoch = 2;

        // Initialize as leader and append some data that will eventually get truncated
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        KafkaRaftClient client = initializeAsLeader(voters, epoch);

        SimpleRecord[] records = new SimpleRecord[] {
            new SimpleRecord("a".getBytes()),
            new SimpleRecord("b".getBytes())
        };
        client.append(MemoryRecords.withRecords(CompressionType.NONE, records),
            AckMode.LEADER, Integer.MAX_VALUE);
        client.poll();
        assertEquals(3L, log.endOffset().offset);

        // The other node becomes leader
        int newEpoch = 3;
        deliverRequest(beginEpochRequest(newEpoch, otherNodeId));
        client.poll();
        assertSentBeginQuorumEpochResponse(Errors.NONE, newEpoch, OptionalInt.of(otherNodeId));

        CompletableFuture<Records> future = client.read(new OffsetAndEpoch(3L, epoch),
            Isolation.UNCOMMITTED, 500);
        assertFalse(future.isDone());

        // We send a fetch at the current offset and the leader tells us to truncate
        pollUntilSend(client);
        int fetchCorrelationId = assertSentFetchRequest(newEpoch, 3L, epoch);
        FetchResponseData fetchResponse = outOfRangeFetchRecordsResponse(
            newEpoch, otherNodeId, 1L, epoch, 0L);
        deliverResponse(fetchCorrelationId, otherNodeId, fetchResponse);
        client.poll();
        assertEquals(1L, log.endOffset().offset);
        assertTrue(future.isDone());
        assertFutureThrows(future, LogTruncationException.class);
    }

    private void validateLocalRead(
        KafkaRaftClient client,
        OffsetAndEpoch fetchOffsetAndEpoch,
        Isolation isolation,
        SimpleRecord[] expectedRecords
    ) throws Exception {
        CompletableFuture<Records> future = client.read(fetchOffsetAndEpoch, isolation, 0L);
        assertTrue(future.isDone());
        assertMatchingRecords(expectedRecords, future.get());
    }

    private void assertMatchingRecords(
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

    private void fetchFromLeader(
        KafkaRaftClient client,
        int leaderId,
        int epoch,
        OffsetAndEpoch fetchOffsetAndEpoch,
        SimpleRecord[] records,
        long highWatermark
    ) throws Exception {
        pollUntilSend(client);
        int fetchCorrelationId = assertSentFetchRequest(epoch,
            fetchOffsetAndEpoch.offset, fetchOffsetAndEpoch.epoch);
        Records fetchedRecords = MemoryRecords.withRecords(fetchOffsetAndEpoch.offset,
            CompressionType.NONE, epoch, records);
        FetchResponseData fetchResponse = fetchResponse(
            epoch, leaderId, fetchedRecords, highWatermark, Errors.NONE);
        deliverResponse(fetchCorrelationId, leaderId, fetchResponse);
        client.poll();
    }

    @Test
    public void testHandleEndQuorumRequest() throws Exception {
        int oldLeaderId = 1;
        int leaderEpoch = 2;
        Set<Integer> voters = Utils.mkSet(localId, oldLeaderId);

        quorumStateStore.writeElectionState(ElectionState.withElectedLeader(leaderEpoch, oldLeaderId, voters));

        KafkaRaftClient client = buildClient(voters);

        deliverRequest(endEpochRequest(leaderEpoch, OptionalInt.of(oldLeaderId), oldLeaderId, Collections.singletonList(localId)));

        client.poll();
        assertSentEndQuorumEpochResponse(Errors.NONE, leaderEpoch, OptionalInt.of(oldLeaderId));

        client.poll();
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
        assertSentEndQuorumEpochResponse(Errors.NONE, leaderEpoch, OptionalInt.of(oldLeaderId));

        // The election won't trigger by one round retry backoff
        time.sleep(1);

        pollUntilSend(client);

        assertSentFetchRequest(leaderEpoch, 0, 0);

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
        assertEquals(ElectionState.withUnknownLeader(0, voters), quorumStateStore.readElectionState());

        time.sleep(2 * electionTimeoutMs);
        pollUntilSend(client);
        assertEquals(ElectionState.withVotedCandidate(epoch, localId, voters), quorumStateStore.readElectionState());

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
        KafkaRaftClient client = initializeAsLeader(voters, epoch);

        // First poll has no high watermark advance
        client.poll();
        assertEquals(OptionalLong.empty(), client.highWatermark());

        // Let follower send a fetch to initialize the high watermark,
        // note the offset 0 would be a control message for becoming the leader
        deliverRequest(fetchRequest(epoch, otherNodeId, 0L, epoch, 500));
        pollUntilSend(client);
        assertEquals(OptionalLong.of(0L), client.highWatermark());

        // Append some records with leader commit mode
        SimpleRecord[] appendRecords = new SimpleRecord[] {
            new SimpleRecord("a".getBytes()),
            new SimpleRecord("b".getBytes()),
            new SimpleRecord("c".getBytes())
        };
        Records records = MemoryRecords.withRecords(0L, CompressionType.NONE, 1, appendRecords);
        CompletableFuture<OffsetAndEpoch> future = client.append(records,
            AckMode.LEADER, Integer.MAX_VALUE);

        client.poll();
        assertTrue(future.isDone());
        assertEquals(new OffsetAndEpoch(3, epoch), future.get());

        // Let follower send a fetch, it should advance the high watermark
        deliverRequest(fetchRequest(epoch, otherNodeId, 1L, epoch, 500));
        pollUntilSend(client);
        assertEquals(OptionalLong.of(1L), client.highWatermark());

        // Let the follower to send another fetch from offset 4
        deliverRequest(fetchRequest(epoch, otherNodeId, 4L, epoch, 500));
        client.poll();
        assertEquals(OptionalLong.of(4L), client.highWatermark());

        // Append more records with quorum commit mode
        appendRecords = new SimpleRecord[] {
            new SimpleRecord("a".getBytes()),
            new SimpleRecord("b".getBytes()),
            new SimpleRecord("c".getBytes())
        };
        records = MemoryRecords.withRecords(0L, CompressionType.NONE, 1, appendRecords);
        future = client.append(records, AckMode.QUORUM, Integer.MAX_VALUE);

        // Appending locally should not complete the future
        client.poll();
        assertFalse(future.isDone());

        // Let follower send a fetch, it should not yet advance the high watermark
        deliverRequest(fetchRequest(epoch, otherNodeId, 4L, epoch, 500));
        pollUntilSend(client);
        assertFalse(future.isDone());
        assertEquals(OptionalLong.of(4L), client.highWatermark());

        // Let the follower to send another fetch at 7, which should not advance the high watermark and complete the future
        deliverRequest(fetchRequest(epoch, otherNodeId, 7L, epoch, 500));
        client.poll();
        assertEquals(OptionalLong.of(7L), client.highWatermark());

        assertTrue(future.isDone());
        assertEquals(new OffsetAndEpoch(6, epoch), future.get());
    }

    @Test
    public void testStateMachineExpireAppendedRecords() throws Exception {
        int otherNodeId = 1;
        int epoch = 5;

        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        KafkaRaftClient client = initializeAsLeader(voters, epoch);

        // First poll has no high watermark advance
        client.poll();
        assertEquals(OptionalLong.empty(), client.highWatermark());

        // Let follower send a fetch to initialize the high watermark,
        // note the offset 0 would be a control message for becoming the leader
        deliverRequest(fetchRequest(epoch, otherNodeId, 0L, epoch, 500));
        pollUntilSend(client);
        assertEquals(OptionalLong.of(0L), client.highWatermark());

        // Append some records with quorum commit mode
        SimpleRecord[] appendRecords = new SimpleRecord[] {
            new SimpleRecord("a".getBytes()),
            new SimpleRecord("b".getBytes()),
            new SimpleRecord("c".getBytes())
        };

        long requestTimeoutMs = 5000;
        Records records = MemoryRecords.withRecords(0L, CompressionType.NONE, 1, appendRecords);
        CompletableFuture<OffsetAndEpoch> future = client.append(records, AckMode.QUORUM, requestTimeoutMs);

        client.poll();
        assertFalse(future.isDone());

        time.sleep(requestTimeoutMs - 1);
        assertFalse(future.isDone());

        time.sleep(1);
        assertTrue(future.isCompletedExceptionally());
    }

    @Test
    public void testCandidateIgnoreVoteRequestOnSameEpoch() throws Exception {
        int otherNodeId = 1;
        int leaderEpoch = 2;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        quorumStateStore.writeElectionState(ElectionState.withVotedCandidate(leaderEpoch, localId, voters));
        KafkaRaftClient client = buildClient(voters);

        pollUntilSend(client);

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

        int exponentialFactor = 85;  // set it large enough so that we will bound on jitter
        Mockito.doReturn(exponentialFactor).when(random).nextInt(Mockito.anyInt());

        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);

        KafkaRaftClient client = buildClient(voters);
        assertEquals(ElectionState.withUnknownLeader(0, voters), quorumStateStore.readElectionState());

        time.sleep(2 * electionTimeoutMs);
        pollUntilSend(client);
        assertEquals(ElectionState.withVotedCandidate(epoch, localId, voters), quorumStateStore.readElectionState());

        // Quorum size is two. If the other member rejects, then we need to schedule a revote.
        int correlationId = assertSentVoteRequest(epoch, 0, 0L);
        deliverResponse(correlationId, otherNodeId, voteResponse(false, Optional.empty(), 1));

        client.poll();

        // All nodes have rejected our candidacy, but we should still remember that we had voted
        ElectionState latest = quorumStateStore.readElectionState();
        assertEquals(epoch, latest.epoch);
        assertTrue(latest.hasVoted());
        assertEquals(localId, latest.votedId());

        // Even though our candidacy was rejected, we will backoff for jitter period
        // before we bump the epoch and start a new election.
        time.sleep(electionBackoffMaxMs - 1);
        client.poll();
        assertEquals(epoch, quorumStateStore.readElectionState().epoch);

        // After jitter expires, we become a candidate again
        time.sleep(1);
        client.poll();
        pollUntilSend(client);
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

        pollUntilSend(client);

        assertSentFetchRequest(epoch, 0L, 0);
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
        assertSentFetchRequest(epoch, 1L, lastEpoch);
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

        pollUntilSend(client);
        assertSentFetchRequest(epoch, 1L, lastEpoch);

        time.sleep(fetchTimeoutMs);

        pollUntilSend(client);

        assertSentVoteRequest(epoch + 1, lastEpoch, 1L);
        assertEquals(ElectionState.withVotedCandidate(epoch + 1, localId, voters), quorumStateStore.readElectionState());
    }

    @Test
    public void testInitializeObserverNoPreviousState() throws Exception {
        int leaderId = 1;
        int otherNodeId = 2;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(leaderId, otherNodeId);
        KafkaRaftClient client = buildClient(voters);

        pollUntilSend(client);
        RaftRequest.Outbound fetchRequest = assertSentFetchRequest();
        assertTrue(voters.contains(fetchRequest.destinationId()));
        assertFetchRequestData(fetchRequest, 0, 0L, 0);

        deliverResponse(fetchRequest.correlationId, fetchRequest.destinationId(),
            fetchResponse(epoch, leaderId, MemoryRecords.EMPTY, 0L, Errors.FENCED_LEADER_EPOCH));

        client.poll();
        assertEquals(ElectionState.withElectedLeader(epoch, leaderId, voters), quorumStateStore.readElectionState());
    }

    @Test
    public void testObserverQuorumDiscoveryFailure() throws Exception {
        int leaderId = 1;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(leaderId);
        KafkaRaftClient client = buildClient(voters);

        pollUntilSend(client);
        RaftRequest.Outbound fetchRequest = assertSentFetchRequest();
        assertTrue(voters.contains(fetchRequest.destinationId()));
        assertFetchRequestData(fetchRequest, 0, 0L, 0);

        deliverResponse(fetchRequest.correlationId, fetchRequest.destinationId(),
            fetchResponse(-1, -1, MemoryRecords.EMPTY, -1, Errors.UNKNOWN_SERVER_ERROR));
        client.poll();

        time.sleep(retryBackoffMs);
        pollUntilSend(client);

        fetchRequest = assertSentFetchRequest();
        assertTrue(voters.contains(fetchRequest.destinationId()));
        assertFetchRequestData(fetchRequest, 0, 0L, 0);

        deliverResponse(fetchRequest.correlationId, fetchRequest.destinationId(),
            fetchResponse(epoch, leaderId, MemoryRecords.EMPTY, 0L, Errors.FENCED_LEADER_EPOCH));
        client.poll();

        assertEquals(ElectionState.withElectedLeader(epoch, leaderId, voters), quorumStateStore.readElectionState());
    }

    @Test
    public void testObserverSendDiscoveryFetchAfterFetchTimeout() throws Exception {
        int leaderId = 1;
        int otherNodeId = 2;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(leaderId, otherNodeId);
        KafkaRaftClient client = buildClient(voters);

        pollUntilSend(client);
        RaftRequest.Outbound fetchRequest = assertSentFetchRequest();
        assertTrue(voters.contains(fetchRequest.destinationId()));
        assertFetchRequestData(fetchRequest, 0, 0L, 0);

        deliverResponse(fetchRequest.correlationId, fetchRequest.destinationId(),
            fetchResponse(epoch, leaderId, MemoryRecords.EMPTY, 0L, Errors.FENCED_LEADER_EPOCH));
        client.poll();

        assertEquals(ElectionState.withElectedLeader(epoch, leaderId, voters), quorumStateStore.readElectionState());
        time.sleep(fetchTimeoutMs);

        pollUntilSend(client);
        fetchRequest = assertSentFetchRequest();
        assertTrue(voters.contains(fetchRequest.destinationId()));
        assertFetchRequestData(fetchRequest, epoch, 0L, 0);
    }

    @Test
    public void testInvalidFetchRequest() throws Exception {
        int otherNodeId = 1;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        KafkaRaftClient client = initializeAsLeader(voters, epoch);

        deliverRequest(fetchRequest(
            epoch, otherNodeId, -5L, 0, 0));
        client.poll();
        assertSentFetchResponse(Errors.INVALID_REQUEST, epoch, OptionalInt.of(localId));

        deliverRequest(fetchRequest(
            epoch, otherNodeId, 0L, -1, 0));
        client.poll();
        assertSentFetchResponse(Errors.INVALID_REQUEST, epoch, OptionalInt.of(localId));

        deliverRequest(fetchRequest(
            epoch, otherNodeId, 0L, epoch + 1, 0));
        client.poll();
        assertSentFetchResponse(Errors.INVALID_REQUEST, epoch, OptionalInt.of(localId));

        deliverRequest(fetchRequest(
            epoch + 1, otherNodeId, 0L, 0, 0));
        client.poll();
        assertSentFetchResponse(Errors.UNKNOWN_LEADER_EPOCH, epoch, OptionalInt.of(localId));

        deliverRequest(fetchRequest(
            epoch, otherNodeId, 0L, 0, -1));
        client.poll();
        assertSentFetchResponse(Errors.INVALID_REQUEST, epoch, OptionalInt.of(localId));
    }

    @Test
    public void testVoterOnlyRequestValidation() throws Exception {
        int otherNodeId = 1;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        KafkaRaftClient client = initializeAsLeader(voters, epoch);

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
        KafkaRaftClient client = initializeAsLeader(voters, epoch);

        // Follower sends a fetch which cannot be satisfied immediately
        int maxWaitTimeMs = 500;
        deliverRequest(fetchRequest(epoch, otherNodeId, 1L, epoch, maxWaitTimeMs));
        client.poll();
        assertEquals(0, channel.drainSendQueue().size());

        // After expiration of the max wait time, the fetch returns an empty record set
        time.sleep(maxWaitTimeMs);
        client.poll();
        MemoryRecords fetchedRecords = assertSentFetchResponse(Errors.NONE, epoch, OptionalInt.of(localId));
        assertEquals(0, fetchedRecords.sizeInBytes());
    }

    @Test
    public void testPurgatoryFetchSatisfiedByWrite() throws Exception {
        int otherNodeId = 1;
        int epoch = 5;

        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        KafkaRaftClient client = initializeAsLeader(voters, epoch);

        // Follower sends a fetch which cannot be satisfied immediately
        deliverRequest(fetchRequest(epoch, otherNodeId, 1L, epoch, 500));
        client.poll();
        assertEquals(0, channel.drainSendQueue().size());

        // Append some records that can fulfill the Fetch request
        SimpleRecord[] appendRecords = new SimpleRecord[] {
            new SimpleRecord("a".getBytes()),
            new SimpleRecord("b".getBytes()),
            new SimpleRecord("c".getBytes())
        };
        Records records = MemoryRecords.withRecords(0L, CompressionType.NONE, 1, appendRecords);
        CompletableFuture<OffsetAndEpoch> future = client.append(records, AckMode.LEADER, Integer.MAX_VALUE);
        client.poll();
        assertTrue(future.isDone());

        MemoryRecords fetchedRecords = assertSentFetchResponse(Errors.NONE, epoch, OptionalInt.of(localId));
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
        KafkaRaftClient client = initializeAsLeader(voters, epoch);

        // Follower sends a fetch which cannot be satisfied immediately
        deliverRequest(fetchRequest(epoch, voter2, 1L, epoch, 500));
        client.poll();
        assertTrue(channel.drainSendQueue().stream()
            .noneMatch(msg -> msg.data() instanceof FetchResponseData));

        // Now we get a BeginEpoch from the other voter and become a follower
        deliverRequest(beginEpochRequest(epoch + 1, voter3));
        client.poll();
        assertEquals(ElectionState.withElectedLeader(epoch + 1, voter3, voters), quorumStateStore.readElectionState());

        // We expect the BeginQuorumEpoch response and a failed Fetch response
        assertSentBeginQuorumEpochResponse(Errors.NONE, epoch + 1, OptionalInt.of(voter3));

        // The fetch should be satisfied immediately and return an error
        MemoryRecords fetchedRecords = assertSentFetchResponse(
            Errors.NOT_LEADER_OR_FOLLOWER, epoch + 1, OptionalInt.of(voter3));
        assertEquals(0, fetchedRecords.sizeInBytes());
    }

    private KafkaRaftClient initializeAsLeader(Set<Integer> voters, int epoch) throws Exception {
        ElectionState leaderElectionState = ElectionState.withElectedLeader(epoch, localId, voters);
        quorumStateStore.writeElectionState(leaderElectionState);
        KafkaRaftClient client = buildClient(voters);
        assertEquals(leaderElectionState, quorumStateStore.readElectionState());

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

        // Wait until we have a Fetch inflight to the leader
        pollUntilSend(client);
        int fetchCorrelationId = assertSentFetchRequest(epoch, 0L, 0);

        // Now await the fetch timeout and become a candidate
        time.sleep(fetchTimeoutMs);
        client.poll();
        assertEquals(ElectionState.withVotedCandidate(epoch + 1, localId, voters), quorumStateStore.readElectionState());

        // The fetch response from the old leader returns, but it should be ignored
        Records records = MemoryRecords.withRecords(0L, CompressionType.NONE,
            3, new SimpleRecord("a".getBytes()), new SimpleRecord("b".getBytes()));
        deliverResponse(fetchCorrelationId, otherNodeId,
            fetchResponse(epoch, otherNodeId, records, 0L, Errors.NONE));

        client.poll();
        assertEquals(0, log.endOffset().offset);
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

        // Wait until we have a Fetch inflight to the leader
        pollUntilSend(client);
        int fetchCorrelationId = assertSentFetchRequest(epoch, 0L, 0);

        // Now receive a BeginEpoch from `voter3`
        deliverRequest(beginEpochRequest(epoch + 1, voter3));
        client.poll();
        assertEquals(ElectionState.withElectedLeader(epoch + 1, voter3, voters), quorumStateStore.readElectionState());

        // The fetch response from the old leader returns, but it should be ignored
        Records records = MemoryRecords.withRecords(0L, CompressionType.NONE,
            3, new SimpleRecord("a".getBytes()), new SimpleRecord("b".getBytes()));
        FetchResponseData response = fetchResponse(epoch, voter2, records, 0L, Errors.NONE);
        deliverResponse(fetchCorrelationId, voter2, response);

        client.poll();
        assertEquals(0, log.endOffset().offset);
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

    private void discoverLeaderAsObserver(
        KafkaRaftClient client,
        Set<Integer> voters,
        int leaderId,
        int epoch
    ) throws Exception {
        pollUntilSend(client);
        RaftRequest.Outbound fetchRequest = assertSentFetchRequest();
        assertTrue(voters.contains(fetchRequest.destinationId()));
        assertFetchRequestData(fetchRequest, 0, 0L, 0);

        deliverResponse(fetchRequest.correlationId, fetchRequest.destinationId(),
            fetchResponse(epoch, leaderId, MemoryRecords.EMPTY, 0L, Errors.NONE));
        client.poll();
        assertEquals(ElectionState.withElectedLeader(epoch, leaderId, voters), quorumStateStore.readElectionState());
    }

    @Test
    public void testObserverLeaderRediscoveryAfterBrokerNotAvailableError() throws Exception {
        int leaderId = 1;
        int otherNodeId = 2;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(leaderId, otherNodeId);
        KafkaRaftClient client = buildClient(voters);
        discoverLeaderAsObserver(client, voters, leaderId, epoch);

        pollUntilSend(client);
        RaftRequest.Outbound fetchRequest1 = assertSentFetchRequest();
        assertEquals(leaderId, fetchRequest1.destinationId());
        assertFetchRequestData(fetchRequest1, epoch, 0L, 0);

        deliverResponse(fetchRequest1.correlationId, fetchRequest1.destinationId(),
            fetchResponse(epoch, -1, MemoryRecords.EMPTY, -1, Errors.BROKER_NOT_AVAILABLE));
        pollUntilSend(client);

        // We should retry the Fetch against the other voter since the original
        // voter connection will be backing off.
        RaftRequest.Outbound fetchRequest2 = assertSentFetchRequest();
        assertNotEquals(leaderId, fetchRequest2.destinationId());
        assertTrue(voters.contains(fetchRequest2.destinationId()));
        assertFetchRequestData(fetchRequest2, epoch, 0L, 0);

        Errors error = fetchRequest2.destinationId() == leaderId ?
            Errors.NONE : Errors.NOT_LEADER_OR_FOLLOWER;
        deliverResponse(fetchRequest2.correlationId, fetchRequest2.destinationId(),
            fetchResponse(epoch, leaderId, MemoryRecords.EMPTY, 0L, error));
        client.poll();

        assertEquals(ElectionState.withElectedLeader(epoch, leaderId, voters), quorumStateStore.readElectionState());
    }

    @Test
    public void testObserverLeaderRediscoveryAfterRequestTimeout() throws Exception {
        int leaderId = 1;
        int otherNodeId = 2;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(leaderId, otherNodeId);
        KafkaRaftClient client = buildClient(voters);
        discoverLeaderAsObserver(client, voters, leaderId, epoch);

        pollUntilSend(client);
        RaftRequest.Outbound fetchRequest1 = assertSentFetchRequest();
        assertEquals(leaderId, fetchRequest1.destinationId());
        assertFetchRequestData(fetchRequest1, epoch, 0L, 0);

        time.sleep(requestTimeoutMs);
        pollUntilSend(client);

        // We should retry the Fetch against the other voter since the original
        // voter connection will be backing off.
        RaftRequest.Outbound fetchRequest2 = assertSentFetchRequest();
        assertNotEquals(leaderId, fetchRequest2.destinationId());
        assertTrue(voters.contains(fetchRequest2.destinationId()));
        assertFetchRequestData(fetchRequest2, epoch, 0L, 0);

        deliverResponse(fetchRequest2.correlationId, fetchRequest2.destinationId(),
            fetchResponse(epoch, leaderId, MemoryRecords.EMPTY, 0L, Errors.FENCED_LEADER_EPOCH));
        client.poll();

        assertEquals(ElectionState.withElectedLeader(epoch, leaderId, voters), quorumStateStore.readElectionState());
    }

    @Test
    public void testLeaderGracefulShutdown() throws Exception {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        int epoch = 1;
        KafkaRaftClient client = initializeAsLeader(voters, epoch);

        // Now shutdown
        int shutdownTimeoutMs = 5000;
        CompletableFuture<Void> shutdownFuture = client.shutdown(shutdownTimeoutMs);

        // We should still be running until we have had a chance to send EndQuorumEpoch
        assertTrue(client.isShuttingDown());
        assertTrue(client.isRunning());
        assertFalse(shutdownFuture.isDone());

        // Send EndQuorumEpoch request to the other voter
        client.poll();
        assertTrue(client.isShuttingDown());
        assertTrue(client.isRunning());
        assertSentEndQuorumEpochRequest(1, OptionalInt.of(localId), otherNodeId);

        // We should still be able to handle vote requests during graceful shutdown
        // in order to help the new leader get elected
        deliverRequest(voteRequest(epoch + 1, otherNodeId, epoch, 1L));
        client.poll();
        assertSentVoteResponse(Errors.NONE, epoch + 1, OptionalInt.empty(), true);

        // Graceful shutdown completes when a new leader is elected
        deliverRequest(beginEpochRequest(2, otherNodeId));

        TestUtils.waitForCondition(() -> {
            client.poll();
            return !client.isRunning();
        }, 5000, "Client failed to shutdown before expiration of timeout");
        assertFalse(client.isShuttingDown());
        assertTrue(shutdownFuture.isDone());
        assertNull(shutdownFuture.get());
    }

    @Test
    public void testEndQuorumEpochSentBasedOnFetchOffset() throws Exception {
        int closeFollower = 2;
        int laggingFollower = 1;
        int epoch = 1;
        Set<Integer> voters = Utils.mkSet(localId, closeFollower, laggingFollower);
        KafkaRaftClient client = initializeAsLeader(voters, epoch);

        buildFollowerSet(client, epoch, closeFollower, laggingFollower);

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
    public void testDescribeQuorum() throws Exception {
        int closeFollower = 2;
        int laggingFollower = 1;
        int epoch = 1;
        Set<Integer> voters = Utils.mkSet(localId, closeFollower, laggingFollower);

        KafkaRaftClient client = initializeAsLeader(voters, epoch);

        buildFollowerSet(client, epoch, closeFollower, laggingFollower);

        // Create observer
        int observerId = 3;
        deliverRequest(fetchRequest(epoch, observerId, 0L, 0, 0));

        client.poll();

        long highWatermark = 1L;
        assertSentFetchResponse(highWatermark, epoch);

        deliverRequest(DescribeQuorumRequest.singletonRequest(METADATA_PARTITION));

        client.poll();

        assertSentDescribeQuorumResponse(localId, epoch, highWatermark,
            Arrays.asList(
                new ReplicaState()
                    .setReplicaId(localId)
                    // As we are appending the records directly to the log,
                    // the leader end offset hasn't been updated yet.
                    .setLogEndOffset(3L),
                new ReplicaState()
                    .setReplicaId(laggingFollower)
                    .setLogEndOffset(0L),
                new ReplicaState()
                    .setReplicaId(closeFollower)
                    .setLogEndOffset(1L)),
            Collections.singletonList(
                new ReplicaState()
                    .setReplicaId(observerId)
                    .setLogEndOffset(0L)));
    }

    private void buildFollowerSet(KafkaRaftClient client,
                                  int epoch,
                                  int closeFollower,
                                  int laggingFollower) throws Exception {
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

    @Test
    public void testLeaderGracefulShutdownTimeout() throws Exception {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        int epoch = 1;
        KafkaRaftClient client = initializeAsLeader(voters, epoch);

        // Now shutdown
        int shutdownTimeoutMs = 5000;
        CompletableFuture<Void> shutdownFuture = client.shutdown(shutdownTimeoutMs);

        // We should still be running until we have had a chance to send EndQuorumEpoch
        assertTrue(client.isRunning());
        assertFalse(shutdownFuture.isDone());

        // Send EndQuorumEpoch request to the other vote
        client.poll();
        assertTrue(client.isRunning());

        assertSentEndQuorumEpochRequest(epoch, OptionalInt.of(localId), otherNodeId);

        // The shutdown timeout is hit before we receive any requests or responses indicating an epoch bump
        time.sleep(shutdownTimeoutMs);

        client.poll();
        assertFalse(client.isRunning());
        assertTrue(shutdownFuture.isCompletedExceptionally());
        assertFutureThrows(shutdownFuture, TimeoutException.class);
    }

    @Test
    public void testFollowerGracefulShutdown() throws Exception {
        int otherNodeId = 1;
        int epoch = 5;

        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        quorumStateStore.writeElectionState(ElectionState.withElectedLeader(epoch, otherNodeId, voters));
        KafkaRaftClient client = buildClient(voters);
        assertEquals(ElectionState.withElectedLeader(epoch, otherNodeId, voters), quorumStateStore.readElectionState());

        client.poll();

        int shutdownTimeoutMs = 5000;
        CompletableFuture<Void> shutdownFuture = client.shutdown(shutdownTimeoutMs);
        assertTrue(client.isRunning());
        assertFalse(shutdownFuture.isDone());

        client.poll();
        assertFalse(client.isRunning());
        assertTrue(shutdownFuture.isDone());
        assertNull(shutdownFuture.get());
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
        KafkaRaftClient client = buildClient(voters);
        assertEquals(ElectionState.withElectedLeader(epoch, otherNodeId, voters), quorumStateStore.readElectionState());

        pollUntilSend(client);

        int fetchQuorumCorrelationId = assertSentFetchRequest(epoch, 0L, 0);
        Records records = MemoryRecords.withRecords(0L, CompressionType.NONE,
            3, new SimpleRecord("a".getBytes()), new SimpleRecord("b".getBytes()));
        FetchResponseData response = fetchResponse(epoch, otherNodeId, records, 0L, Errors.NONE);
        deliverResponse(fetchQuorumCorrelationId, otherNodeId, response);

        client.poll();
        assertEquals(2L, log.endOffset().offset);
    }

    @Test
    public void testEmptyRecordSetInFetchResponse() throws Exception {
        int otherNodeId = 1;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        quorumStateStore.writeElectionState(ElectionState.withElectedLeader(epoch, otherNodeId, voters));
        KafkaRaftClient client = buildClient(voters);

        assertEquals(ElectionState.withElectedLeader(epoch, otherNodeId, voters), quorumStateStore.readElectionState());

        // Receive an empty fetch response
        pollUntilSend(client);
        int fetchQuorumCorrelationId = assertSentFetchRequest(epoch, 0L, 0);
        FetchResponseData fetchResponse = fetchResponse(epoch, otherNodeId,
            MemoryRecords.EMPTY, 0L, Errors.NONE);
        deliverResponse(fetchQuorumCorrelationId, otherNodeId, fetchResponse);
        client.poll();
        assertEquals(0L, log.endOffset().offset);
        assertEquals(OptionalLong.of(0L), client.highWatermark());

        // Receive some records in the next poll, but do not advance high watermark
        pollUntilSend(client);
        Records records = MemoryRecords.withRecords(0L, CompressionType.NONE,
            epoch, new SimpleRecord("a".getBytes()), new SimpleRecord("b".getBytes()));
        fetchQuorumCorrelationId = assertSentFetchRequest(epoch, 0L, 0);
        fetchResponse = fetchResponse(epoch, otherNodeId,
            records, 0L, Errors.NONE);
        deliverResponse(fetchQuorumCorrelationId, otherNodeId, fetchResponse);
        client.poll();
        assertEquals(2L, log.endOffset().offset);
        assertEquals(OptionalLong.of(0L), client.highWatermark());

        // The next fetch response is empty, but should still advance the high watermark
        pollUntilSend(client);
        fetchQuorumCorrelationId = assertSentFetchRequest(epoch, 2L, epoch);
        fetchResponse = fetchResponse(epoch, otherNodeId,
            MemoryRecords.EMPTY, 2L, Errors.NONE);
        deliverResponse(fetchQuorumCorrelationId, otherNodeId, fetchResponse);
        client.poll();
        assertEquals(2L, log.endOffset().offset);
        assertEquals(OptionalLong.of(2L), client.highWatermark());
    }

    @Test
    public void testAppendEmptyRecordSetNotAllowed() throws Exception {
        int epoch = 5;

        Set<Integer> voters = Collections.singleton(localId);
        quorumStateStore.writeElectionState(ElectionState.withElectedLeader(epoch, localId, voters));

        KafkaRaftClient client = buildClient(voters);
        assertThrows(IllegalArgumentException.class, () ->
            client.append(MemoryRecords.EMPTY, AckMode.LEADER, Integer.MAX_VALUE));
    }

    @Test
    public void testAppendToNonLeaderFails() throws IOException {
        int otherNodeId = 1;
        int epoch = 5;

        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        quorumStateStore.writeElectionState(ElectionState.withElectedLeader(epoch, otherNodeId, voters));

        KafkaRaftClient client = buildClient(voters);
        assertEquals(ElectionState.withElectedLeader(epoch, otherNodeId, voters), quorumStateStore.readElectionState());

        SimpleRecord[] appendRecords = new SimpleRecord[] {
            new SimpleRecord("a".getBytes()),
            new SimpleRecord("b".getBytes()),
            new SimpleRecord("c".getBytes())
        };
        Records records = MemoryRecords.withRecords(0L, CompressionType.NONE, 1, appendRecords);

        CompletableFuture<OffsetAndEpoch> future = client.append(records, AckMode.LEADER, Integer.MAX_VALUE);
        client.poll();

        assertFutureThrows(future, NotLeaderOrFollowerException.class);
    }

    @Test
    public void testFetchShouldBeTreatedAsLeaderEndorsement() throws Exception {
        int otherNodeId = 1;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);

        quorumStateStore.writeElectionState(ElectionState.withElectedLeader(epoch, localId, voters));
        KafkaRaftClient client = buildClient(voters);
        assertEquals(ElectionState.withElectedLeader(epoch, localId, voters), quorumStateStore.readElectionState());

        pollUntilSend(client);

        // We send BeginEpoch, but it gets lost and the destination finds the leader through the Fetch API
        assertSentBeginQuorumEpochRequest(epoch);

        deliverRequest(fetchRequest(
            epoch, otherNodeId, 0L, 0, 500));

        client.poll();

        // The BeginEpoch request eventually times out. We should not send another one.
        assertSentFetchResponse(Errors.NONE, epoch, OptionalInt.of(localId));
        time.sleep(requestTimeoutMs);

        client.poll();

        List<RaftMessage> sentMessages = channel.drainSendQueue();
        assertEquals(0, sentMessages.size());
    }

    @Test
    public void testLeaderAppendSingleMemberQuorum() throws IOException {
        long now = time.milliseconds();

        Set<Integer> voters = Collections.singleton(localId);
        KafkaRaftClient client = buildClient(voters);
        assertEquals(ElectionState.withElectedLeader(1, localId, voters), quorumStateStore.readElectionState());

        // We still write the leader change message
        assertEquals(OptionalLong.of(1L), client.highWatermark());

        SimpleRecord[] appendRecords = new SimpleRecord[] {
            new SimpleRecord("a".getBytes()),
            new SimpleRecord("b".getBytes()),
            new SimpleRecord("c".getBytes())
        };
        Records records = MemoryRecords.withRecords(1L, CompressionType.NONE, 1, appendRecords);

        // First poll has no high watermark advance
        client.poll();
        assertEquals(OptionalLong.of(1L), client.highWatermark());

        client.append(records, AckMode.LEADER, Integer.MAX_VALUE);

        // Then poll the appended data with leader change record
        client.poll();
        assertEquals(OptionalLong.of(4L), client.highWatermark());

        // Now try reading it
        int otherNodeId = 1;
        deliverRequest(fetchRequest(
            1, otherNodeId, 0L, 0, 500));

        client.poll();

        MemoryRecords fetchedRecords = assertSentFetchResponse(Errors.NONE, 1, OptionalInt.of(localId));
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
        assertEquals(3L, log.endOffset().offset);

        pollUntilSend(client);

        int correlationId = assertSentFetchRequest(epoch, 3L, lastEpoch);

        FetchResponseData response = outOfRangeFetchRecordsResponse(epoch, otherNodeId, 2L,
            lastEpoch, 1L);
        deliverResponse(correlationId, otherNodeId, response);

        // Poll again to complete truncation
        client.poll();
        assertEquals(2L, log.endOffset().offset);

        // Now we should be fetching
        client.poll();
        assertSentFetchRequest(epoch, 2L, lastEpoch);
    }

    @Test
    public void testMetrics() throws Exception {
        Metrics metrics = new Metrics(time);
        int epoch = 1;
        quorumStateStore.writeElectionState(ElectionState.withElectedLeader(epoch, localId, Collections.singleton(localId)));
        KafkaRaftClient client = buildClient(Collections.singleton(localId), metrics);

        assertNotNull(getMetric(metrics, "current-state"));
        assertNotNull(getMetric(metrics, "current-leader"));
        assertNotNull(getMetric(metrics, "current-vote"));
        assertNotNull(getMetric(metrics, "current-epoch"));
        assertNotNull(getMetric(metrics, "high-watermark"));
        assertNotNull(getMetric(metrics, "log-end-offset"));
        assertNotNull(getMetric(metrics, "log-end-epoch"));
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
        assertEquals((double) 1L, getMetric(metrics, "high-watermark").metricValue());
        assertEquals((double) 1L, getMetric(metrics, "log-end-offset").metricValue());
        assertEquals((double) epoch, getMetric(metrics, "log-end-epoch").metricValue());

        SimpleRecord[] appendRecords = new SimpleRecord[] {
            new SimpleRecord("a".getBytes()),
            new SimpleRecord("b".getBytes()),
            new SimpleRecord("c".getBytes())
        };
        Records records = MemoryRecords.withRecords(0L, CompressionType.NONE, 1, appendRecords);
        client.append(records, AckMode.LEADER, Integer.MAX_VALUE);
        client.poll();

        assertEquals((double) 4L, getMetric(metrics, "high-watermark").metricValue());
        assertEquals((double) 4L, getMetric(metrics, "log-end-offset").metricValue());
        assertEquals((double) epoch, getMetric(metrics, "log-end-epoch").metricValue());

        CompletableFuture<Void> shutdownFuture = client.shutdown(100);
        client.poll();
        assertTrue(shutdownFuture.isDone());
        assertNull(shutdownFuture.get());

        // should only have total-metrics-count left
        assertEquals(1, metrics.metrics().size());
    }

    @Test
    public void testClusterAuthorizationFailedInFetch() throws Exception {
        int otherNodeId = 1;
        int epoch = 5;

        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        quorumStateStore.writeElectionState(ElectionState.withElectedLeader(epoch, otherNodeId, voters));
        KafkaRaftClient client = buildClient(voters);
        assertEquals(ElectionState.withElectedLeader(epoch, otherNodeId, voters), quorumStateStore.readElectionState());

        pollUntilSend(client);

        int correlationId = assertSentFetchRequest(epoch, 0, 0);
        FetchResponseData response = new FetchResponseData()
            .setErrorCode(Errors.CLUSTER_AUTHORIZATION_FAILED.code());
        deliverResponse(correlationId, otherNodeId, response);
        assertThrows(ClusterAuthorizationException.class, client::poll);
    }

    @Test
    public void testClusterAuthorizationFailedInBeginQuorumEpoch() throws Exception {
        int otherNodeId = 1;
        int epoch = 5;

        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        quorumStateStore.writeElectionState(ElectionState.withElectedLeader(epoch, localId, voters));
        KafkaRaftClient client = buildClient(voters);
        assertEquals(ElectionState.withElectedLeader(epoch, localId, voters), quorumStateStore.readElectionState());

        pollUntilSend(client);
        int correlationId = assertSentBeginQuorumEpochRequest(epoch);
        BeginQuorumEpochResponseData response = new BeginQuorumEpochResponseData()
            .setErrorCode(Errors.CLUSTER_AUTHORIZATION_FAILED.code());

        deliverResponse(correlationId, otherNodeId, response);
        assertThrows(ClusterAuthorizationException.class, client::poll);
    }

    @Test
    public void testClusterAuthorizationFailedInVote() throws Exception {
        int otherNodeId = 1;
        int epoch = 5;

        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        quorumStateStore.writeElectionState(ElectionState.withVotedCandidate(epoch, localId, voters));
        KafkaRaftClient client = buildClient(voters);
        assertEquals(ElectionState.withVotedCandidate(epoch, localId, voters), quorumStateStore.readElectionState());

        pollUntilSend(client);
        int correlationId = assertSentVoteRequest(epoch, 0, 0L);
        VoteResponseData response = new VoteResponseData()
            .setErrorCode(Errors.CLUSTER_AUTHORIZATION_FAILED.code());

        deliverResponse(correlationId, otherNodeId, response);
        assertThrows(ClusterAuthorizationException.class, client::poll);
    }

    @Test
    public void testClusterAuthorizationFailedInEndQuorumEpoch() throws Exception {
        int otherNodeId = 1;
        int epoch = 2;

        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        KafkaRaftClient client = initializeAsLeader(voters, epoch);

        client.shutdown(5000);
        pollUntilSend(client);

        int correlationId = assertSentEndQuorumEpochRequest(epoch, OptionalInt.of(localId), otherNodeId);
        EndQuorumEpochResponseData response = new EndQuorumEpochResponseData()
            .setErrorCode(Errors.CLUSTER_AUTHORIZATION_FAILED.code());

        deliverResponse(correlationId, otherNodeId, response);
        assertThrows(ClusterAuthorizationException.class, client::poll);
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
        assertTrue(hasValidTopicPartition(response, METADATA_PARTITION));

        VoteResponseData.PartitionData partitionResponse = response.topics().get(0).partitions().get(0);

        assertEquals(voteGranted, partitionResponse.voteGranted());
        assertEquals(error, Errors.forCode(partitionResponse.errorCode()));
        assertEquals(epoch, partitionResponse.leaderEpoch());
        assertEquals(leaderId.orElse(-1), partitionResponse.leaderId());
    }

    private void assertSentEndQuorumEpochResponse(
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

    private MemoryRecords assertSentFetchResponse(
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

    private MemoryRecords assertSentFetchResponse(
        long highWatermark,
        int leaderEpoch
    ) {
        FetchResponseData.FetchablePartitionResponse partitionResponse = assertSentPartitionResponse();
        assertEquals(Errors.NONE, Errors.forCode(partitionResponse.errorCode()));
        assertEquals(leaderEpoch, partitionResponse.currentLeader().leaderEpoch());
        assertEquals(highWatermark, partitionResponse.highWatermark());
        return (MemoryRecords) partitionResponse.recordSet();
    }

    private void assertSentBeginQuorumEpochResponse(
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

                EndQuorumEpochRequestData.PartitionData partitionRequest =
                    request.topics().get(0).partitions().get(0);

                assertEquals(epoch, partitionRequest.leaderEpoch());
                assertEquals(leaderId.orElse(-1), partitionRequest.leaderId());
                assertEquals(localId, partitionRequest.replicaId());

                RaftRequest.Outbound outboundRequest = (RaftRequest.Outbound) raftMessage;
                collectedDestinationIdSet.add(outboundRequest.destinationId());
                endQuorumRequests.add(outboundRequest);
            }
        }
        assertEquals(destinationIdSet, collectedDestinationIdSet);
        return endQuorumRequests;
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
                assertTrue(hasValidTopicPartition(request, METADATA_PARTITION));

                VoteRequestData.PartitionData partitionRequest = request.topics().get(0).partitions().get(0);

                assertEquals(epoch, partitionRequest.candidateEpoch());
                assertEquals(localId, partitionRequest.candidateId());
                assertEquals(lastEpoch, partitionRequest.lastOffsetEpoch());
                assertEquals(lastEpochOffset, partitionRequest.lastOffset());
                voteRequests.add((RaftRequest.Outbound) raftMessage);
            }
        }
        return voteRequests;
    }

    private int assertSentBeginQuorumEpochRequest(int epoch) {
        List<RaftRequest.Outbound> requests = collectBeginEpochRequests(epoch);
        assertEquals(1, requests.size());
        return requests.get(0).correlationId;
    }

    private List<RaftRequest.Outbound> collectBeginEpochRequests(int epoch) {
        List<RaftRequest.Outbound> requests = new ArrayList<>();
        for (RaftRequest.Outbound raftRequest : channel.drainSentRequests(ApiKeys.BEGIN_QUORUM_EPOCH)) {
            assertTrue(raftRequest.data() instanceof BeginQuorumEpochRequestData);
            BeginQuorumEpochRequestData request = (BeginQuorumEpochRequestData) raftRequest.data();

            BeginQuorumEpochRequestData.PartitionData partitionRequest =
                request.topics().get(0).partitions().get(0);

            assertEquals(epoch, partitionRequest.leaderEpoch());
            assertEquals(localId, partitionRequest.leaderId());
            requests.add(raftRequest);
        }
        return requests;
    }

    private RaftRequest.Outbound assertSentFetchRequest() {
        List<RaftRequest.Outbound> sentRequests = channel.drainSentRequests(ApiKeys.FETCH);
        assertEquals(1, sentRequests.size());
        return sentRequests.get(0);
    }

    private void assertFetchRequestData(
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
        assertEquals(localId, request.replicaId());
    }

    private int assertSentFetchRequest(
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

    private FetchResponseData fetchResponse(
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

    private FetchResponseData outOfRangeFetchRecordsResponse(
        int epoch,
        int leaderId,
        long divergingEpochEndOffset,
        int divergingEpoch,
        long highWatermark
    ) {
        return RaftUtil.singletonFetchResponse(METADATA_PARTITION, Errors.NONE, partitionData -> {
            partitionData
                .setErrorCode(Errors.NONE.code())
                .setHighWatermark(highWatermark);

            partitionData.currentLeader()
                .setLeaderEpoch(epoch)
                .setLeaderId(leaderId);

            partitionData.divergingEpoch()
                .setEpoch(divergingEpoch)
                .setEndOffset(divergingEpochEndOffset);
        });
    }

    private VoteResponseData voteResponse(boolean voteGranted, Optional<Integer> leaderId, int epoch) {
        return VoteResponse.singletonResponse(
            Errors.NONE,
            METADATA_PARTITION,
            Errors.NONE,
            epoch,
            leaderId.orElse(-1),
            voteGranted
        );
    }

    private VoteRequestData voteRequest(int epoch, int candidateId, int lastEpoch, long lastEpochOffset) {
        return VoteRequest.singletonRequest(
            METADATA_PARTITION,
            epoch,
            candidateId,
            lastEpoch,
            lastEpochOffset
        );
    }

    private BeginQuorumEpochRequestData beginEpochRequest(int epoch, int leaderId) {
        return BeginQuorumEpochRequest.singletonRequest(
            METADATA_PARTITION,
            epoch,
            leaderId
        );
    }

    private EndQuorumEpochRequestData endEpochRequest(
        int epoch,
        OptionalInt leaderId,
        int replicaId,
        List<Integer> preferredSuccessors) {
        return EndQuorumEpochRequest.singletonRequest(
            METADATA_PARTITION,
            replicaId,
            epoch,
            leaderId.orElse(-1),
            preferredSuccessors
        );
    }

    private BeginQuorumEpochResponseData beginEpochResponse(int epoch, int leaderId) {
        return BeginQuorumEpochResponse.singletonResponse(
            Errors.NONE,
            METADATA_PARTITION,
            Errors.NONE,
            epoch,
            leaderId
        );
    }

    private int assertSentDescribeQuorumResponse(int leaderId,
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

    private FetchRequestData fetchRequest(
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

}
