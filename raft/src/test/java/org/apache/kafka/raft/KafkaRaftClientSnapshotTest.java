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
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.FetchSnapshotRequestData;
import org.apache.kafka.common.message.FetchSnapshotResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.requests.FetchSnapshotRequest;
import org.apache.kafka.common.requests.FetchSnapshotResponse;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.raft.internals.StringSerde;
import org.apache.kafka.snapshot.RawSnapshotReader;
import org.apache.kafka.snapshot.RawSnapshotWriter;
import org.apache.kafka.snapshot.SnapshotWriter;
import org.apache.kafka.snapshot.SnapshotWriterTest;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final public class KafkaRaftClientSnapshotTest {
    @Test
    public void testMissingFetchSnapshotRequest() throws Exception {
        int localId = 0;
        int epoch = 2;
        Set<Integer> voters = Utils.mkSet(localId, localId + 1);

        RaftClientTestContext context = RaftClientTestContext.initializeAsLeader(localId, voters, epoch);

        context.deliverRequest(
            fetchSnapshotRequest(
                context.metadataPartition,
                epoch,
                new OffsetAndEpoch(0, 0),
                Integer.MAX_VALUE,
                0
            )
        );

        context.client.poll();

        FetchSnapshotResponseData.PartitionSnapshot response = context.assertSentFetchSnapshotResponse(context.metadataPartition).get();
        assertEquals(Errors.SNAPSHOT_NOT_FOUND, Errors.forCode(response.errorCode()));
    }

    @Test
    public void testUnknownFetchSnapshotRequest() throws Exception {
        int localId = 0;
        Set<Integer> voters = Utils.mkSet(localId, localId + 1);
        int epoch = 2;
        TopicPartition topicPartition = new TopicPartition("unknown", 0);

        RaftClientTestContext context = RaftClientTestContext.initializeAsLeader(localId, voters, epoch);

        context.deliverRequest(
            fetchSnapshotRequest(
                topicPartition,
                epoch,
                new OffsetAndEpoch(0, 0),
                Integer.MAX_VALUE,
                0
            )
        );

        context.client.poll();

        FetchSnapshotResponseData.PartitionSnapshot response = context.assertSentFetchSnapshotResponse(topicPartition).get();
        assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION, Errors.forCode(response.errorCode()));
    }

    @Test
    public void testFetchSnapshotRequestAsLeader() throws Exception {
        int localId = 0;
        Set<Integer> voters = Utils.mkSet(localId, localId + 1);
        int epoch = 2;
        OffsetAndEpoch snapshotId = new OffsetAndEpoch(0, 0);
        List<String> records = Arrays.asList("foo", "bar");

        RaftClientTestContext context = RaftClientTestContext.initializeAsLeader(localId, voters, epoch);

        try (SnapshotWriter<String> snapshot = context.client.createSnapshot(snapshotId)) {
            snapshot.append(records);
            snapshot.freeze();
        }

        try (RawSnapshotReader snapshot = context.log.readSnapshot(snapshotId).get()) {
            context.deliverRequest(
                fetchSnapshotRequest(
                    context.metadataPartition,
                    epoch,
                    snapshotId,
                    Integer.MAX_VALUE,
                    0
                )
            );

            context.client.poll();

            FetchSnapshotResponseData.PartitionSnapshot response = context
                .assertSentFetchSnapshotResponse(context.metadataPartition)
                .get();

            assertEquals(Errors.NONE, Errors.forCode(response.errorCode()));
            assertEquals(snapshot.sizeInBytes(), response.size());
            assertEquals(0, response.position());
            assertEquals(snapshot.sizeInBytes(), response.bytes().remaining());

            ByteBuffer buffer = ByteBuffer.allocate(Math.toIntExact(snapshot.sizeInBytes()));
            snapshot.read(buffer, 0);
            buffer.flip();

            assertEquals(buffer.slice(), response.bytes());
        }
    }

    @Test
    public void testPartialFetchSnapshotRequestAsLeader() throws Exception {
        int localId = 0;
        Set<Integer> voters = Utils.mkSet(localId, localId + 1);
        int epoch = 2;
        OffsetAndEpoch snapshotId = new OffsetAndEpoch(0, 0);
        List<String> records = Arrays.asList("foo", "bar");

        RaftClientTestContext context = RaftClientTestContext.initializeAsLeader(localId, voters, epoch);

        try (SnapshotWriter<String> snapshot = context.client.createSnapshot(snapshotId)) {
            snapshot.append(records);
            snapshot.freeze();
        }

        try (RawSnapshotReader snapshot = context.log.readSnapshot(snapshotId).get()) {
            // Fetch half of the snapshot
            context.deliverRequest(
                fetchSnapshotRequest(
                    context.metadataPartition,
                    epoch,
                    snapshotId,
                    Math.toIntExact(snapshot.sizeInBytes() / 2),
                    0
                )
            );

            context.client.poll();

            FetchSnapshotResponseData.PartitionSnapshot response = context
                .assertSentFetchSnapshotResponse(context.metadataPartition)
                .get();

            assertEquals(Errors.NONE, Errors.forCode(response.errorCode()));
            assertEquals(snapshot.sizeInBytes(), response.size());
            assertEquals(0, response.position());
            assertEquals(snapshot.sizeInBytes() / 2, response.bytes().remaining());

            ByteBuffer snapshotBuffer = ByteBuffer.allocate(Math.toIntExact(snapshot.sizeInBytes()));
            snapshot.read(snapshotBuffer, 0);
            snapshotBuffer.flip();

            ByteBuffer responseBuffer = ByteBuffer.allocate(Math.toIntExact(snapshot.sizeInBytes()));
            responseBuffer.put(response.bytes());

            ByteBuffer expectedBytes = snapshotBuffer.duplicate();
            expectedBytes.limit(Math.toIntExact(snapshot.sizeInBytes() / 2));

            assertEquals(expectedBytes, responseBuffer.duplicate().flip());

            // Fetch the remainder of the snapshot
            context.deliverRequest(
                fetchSnapshotRequest(
                    context.metadataPartition,
                    epoch,
                    snapshotId,
                    Integer.MAX_VALUE,
                    responseBuffer.position()
                )
            );

            context.client.poll();

            response = context.assertSentFetchSnapshotResponse(context.metadataPartition).get();
            assertEquals(Errors.NONE, Errors.forCode(response.errorCode()));
            assertEquals(snapshot.sizeInBytes(), response.size());
            assertEquals(responseBuffer.position(), response.position());
            assertEquals(snapshot.sizeInBytes() - (snapshot.sizeInBytes() / 2), response.bytes().remaining());

            responseBuffer.put(response.bytes());
            assertEquals(snapshotBuffer, responseBuffer.flip());
        }
    }

    @Test
    public void testFetchSnapshotRequestAsFollower() throws IOException {
        int localId = 0;
        int leaderId = localId + 1;
        Set<Integer> voters = Utils.mkSet(localId, leaderId);
        int epoch = 2;
        OffsetAndEpoch snapshotId = new OffsetAndEpoch(0, 0);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withElectedLeader(epoch, leaderId)
            .build();

        context.deliverRequest(
            fetchSnapshotRequest(
                context.metadataPartition,
                epoch,
                snapshotId,
                Integer.MAX_VALUE,
                0
            )
        );

        context.client.poll();

        FetchSnapshotResponseData.PartitionSnapshot response = context.assertSentFetchSnapshotResponse(context.metadataPartition).get();
        assertEquals(Errors.NOT_LEADER_OR_FOLLOWER, Errors.forCode(response.errorCode()));
        assertEquals(epoch, response.currentLeader().leaderEpoch());
        assertEquals(leaderId, response.currentLeader().leaderId());
    }

    @Test
    public void testFetchSnapshotRequestWithInvalidPosition() throws Exception {
        int localId = 0;
        Set<Integer> voters = Utils.mkSet(localId, localId + 1);
        int epoch = 2;
        OffsetAndEpoch snapshotId = new OffsetAndEpoch(0, 0);
        List<String> records = Arrays.asList("foo", "bar");

        RaftClientTestContext context = RaftClientTestContext.initializeAsLeader(localId, voters, epoch);

        try (SnapshotWriter<String> snapshot = context.client.createSnapshot(snapshotId)) {
            snapshot.append(records);
            snapshot.freeze();
        }

        context.deliverRequest(
            fetchSnapshotRequest(
                context.metadataPartition,
                epoch,
                snapshotId,
                Integer.MAX_VALUE,
                -1
            )
        );

        context.client.poll();

        FetchSnapshotResponseData.PartitionSnapshot response = context.assertSentFetchSnapshotResponse(context.metadataPartition).get();
        assertEquals(Errors.POSITION_OUT_OF_RANGE, Errors.forCode(response.errorCode()));
        assertEquals(epoch, response.currentLeader().leaderEpoch());
        assertEquals(localId, response.currentLeader().leaderId());

        try (RawSnapshotReader snapshot = context.log.readSnapshot(snapshotId).get()) {
            context.deliverRequest(
                fetchSnapshotRequest(
                    context.metadataPartition,
                    epoch,
                    snapshotId,
                    Integer.MAX_VALUE,
                    snapshot.sizeInBytes()
                )
            );

            context.client.poll();

            response = context.assertSentFetchSnapshotResponse(context.metadataPartition).get();
            assertEquals(Errors.POSITION_OUT_OF_RANGE, Errors.forCode(response.errorCode()));
            assertEquals(epoch, response.currentLeader().leaderEpoch());
            assertEquals(localId, response.currentLeader().leaderId());
        }
    }

    @Test
    public void testFetchSnapshotRequestWithOlderEpoch() throws Exception {
        int localId = 0;
        Set<Integer> voters = Utils.mkSet(localId, localId + 1);
        int epoch = 2;
        OffsetAndEpoch snapshotId = new OffsetAndEpoch(0, 0);

        RaftClientTestContext context = RaftClientTestContext.initializeAsLeader(localId, voters, epoch);

        context.deliverRequest(
            fetchSnapshotRequest(
                context.metadataPartition,
                epoch - 1,
                snapshotId,
                Integer.MAX_VALUE,
                0
            )
        );

        context.client.poll();

        FetchSnapshotResponseData.PartitionSnapshot response = context.assertSentFetchSnapshotResponse(context.metadataPartition).get();
        assertEquals(Errors.FENCED_LEADER_EPOCH, Errors.forCode(response.errorCode()));
        assertEquals(epoch, response.currentLeader().leaderEpoch());
        assertEquals(localId, response.currentLeader().leaderId());
    }

    @Test
    public void testFetchSnapshotRequestWithNewerEpoch() throws Exception {
        int localId = 0;
        Set<Integer> voters = Utils.mkSet(localId, localId + 1);
        int epoch = 2;
        OffsetAndEpoch snapshotId = new OffsetAndEpoch(0, 0);

        RaftClientTestContext context = RaftClientTestContext.initializeAsLeader(localId, voters, epoch);

        context.deliverRequest(
            fetchSnapshotRequest(
                context.metadataPartition,
                epoch + 1,
                snapshotId,
                Integer.MAX_VALUE,
                0
            )
        );

        context.client.poll();

        FetchSnapshotResponseData.PartitionSnapshot response = context.assertSentFetchSnapshotResponse(context.metadataPartition).get();
        assertEquals(Errors.UNKNOWN_LEADER_EPOCH, Errors.forCode(response.errorCode()));
        assertEquals(epoch, response.currentLeader().leaderEpoch());
        assertEquals(localId, response.currentLeader().leaderId());
    }

    @Test
    public void testFetchResponseWithInvalidSnapshotId() throws Exception {
        int localId = 0;
        int leaderId = localId + 1;
        Set<Integer> voters = Utils.mkSet(localId, leaderId);
        int epoch = 2;
        OffsetAndEpoch invalidEpoch = new OffsetAndEpoch(100L, -1);
        OffsetAndEpoch invalidEndOffset = new OffsetAndEpoch(-1L, 1);
        int slept = 0;

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withElectedLeader(epoch, leaderId)
            .build();

        context.pollUntilRequest();
        RaftRequest.Outbound fetchRequest = context.assertSentFetchRequest();
        context.assertFetchRequestData(fetchRequest, epoch, 0L, 0);

        context.deliverResponse(
            fetchRequest.correlationId,
            fetchRequest.destinationId(),
            snapshotFetchResponse(context.metadataPartition, epoch, leaderId, invalidEpoch, 200L)
        );

        // Handle the invalid response
        context.client.poll();

        // Expect another fetch request after backoff has expired
        context.time.sleep(context.retryBackoffMs);
        slept += context.retryBackoffMs;

        context.pollUntilRequest();
        fetchRequest = context.assertSentFetchRequest();
        context.assertFetchRequestData(fetchRequest, epoch, 0L, 0);

        context.deliverResponse(
            fetchRequest.correlationId,
            fetchRequest.destinationId(),
            snapshotFetchResponse(context.metadataPartition, epoch, leaderId, invalidEndOffset, 200L)
        );

        // Handle the invalid response
        context.client.poll();

        // Expect another fetch request after backoff has expired
        context.time.sleep(context.retryBackoffMs);
        slept += context.retryBackoffMs;

        context.pollUntilRequest();
        fetchRequest = context.assertSentFetchRequest();
        context.assertFetchRequestData(fetchRequest, epoch, 0L, 0);

        // Fetch timer is not reset; sleeping for remainder should transition to candidate
        context.time.sleep(context.fetchTimeoutMs - slept);

        context.pollUntilRequest();

        context.assertSentVoteRequest(epoch + 1, 0, 0L, 1);
        context.assertVotedCandidate(epoch + 1, localId);
    }

    @Test
    public void testFetchResponseWithSnapshotId() throws Exception {
        int localId = 0;
        int leaderId = localId + 1;
        Set<Integer> voters = Utils.mkSet(localId, leaderId);
        int epoch = 2;
        OffsetAndEpoch snapshotId = new OffsetAndEpoch(100L, 1);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withElectedLeader(epoch, leaderId)
            .build();

        context.pollUntilRequest();
        RaftRequest.Outbound fetchRequest = context.assertSentFetchRequest();
        context.assertFetchRequestData(fetchRequest, epoch, 0L, 0);

        context.deliverResponse(
            fetchRequest.correlationId,
            fetchRequest.destinationId(),
            snapshotFetchResponse(context.metadataPartition, epoch, leaderId, snapshotId, 200L)
        );

        context.pollUntilRequest();
        RaftRequest.Outbound snapshotRequest = context.assertSentFetchSnapshotRequest();
        FetchSnapshotRequestData.PartitionSnapshot request = assertFetchSnapshotRequest(
                snapshotRequest,
                context.metadataPartition,
                localId,
                Integer.MAX_VALUE
        ).get();
        assertEquals(snapshotId.offset, request.snapshotId().endOffset());
        assertEquals(snapshotId.epoch, request.snapshotId().epoch());
        assertEquals(0, request.position());

        List<String> records = Arrays.asList("foo", "bar");
        MemorySnapshotWriter memorySnapshot = new MemorySnapshotWriter(snapshotId);
        try (SnapshotWriter<String> snapshotWriter = snapshotWriter(context, memorySnapshot)) {
            snapshotWriter.append(records);
            snapshotWriter.freeze();
        }

        context.deliverResponse(
            snapshotRequest.correlationId,
            snapshotRequest.destinationId(),
            fetchSnapshotResponse(
                context.metadataPartition,
                epoch,
                leaderId,
                snapshotId,
                memorySnapshot.buffer().remaining(),
                0L,
                memorySnapshot.buffer().slice()
            )
        );

        context.pollUntilRequest();

        try (RawSnapshotReader snapshot = context.log.readSnapshot(snapshotId).get()) {
            assertEquals(memorySnapshot.buffer().remaining(), snapshot.sizeInBytes());
            SnapshotWriterTest.assertSnapshot(Arrays.asList(records), snapshot);
        }
    }

    @Test
    public void testFetchSnapshotResponsePartialData() throws Exception {
        int localId = 0;
        int leaderId = localId + 1;
        Set<Integer> voters = Utils.mkSet(localId, leaderId);
        int epoch = 2;
        OffsetAndEpoch snapshotId = new OffsetAndEpoch(100L, 1);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withElectedLeader(epoch, leaderId)
            .build();

        context.pollUntilRequest();
        RaftRequest.Outbound fetchRequest = context.assertSentFetchRequest();
        context.assertFetchRequestData(fetchRequest, epoch, 0L, 0);

        context.deliverResponse(
            fetchRequest.correlationId,
            fetchRequest.destinationId(),
            snapshotFetchResponse(context.metadataPartition, epoch, leaderId, snapshotId, 200L)
        );

        context.pollUntilRequest();
        RaftRequest.Outbound snapshotRequest = context.assertSentFetchSnapshotRequest();
        FetchSnapshotRequestData.PartitionSnapshot request = assertFetchSnapshotRequest(
                snapshotRequest,
                context.metadataPartition,
                localId,
                Integer.MAX_VALUE
        ).get();
        assertEquals(snapshotId.offset, request.snapshotId().endOffset());
        assertEquals(snapshotId.epoch, request.snapshotId().epoch());
        assertEquals(0, request.position());

        List<String> records = Arrays.asList("foo", "bar");
        MemorySnapshotWriter memorySnapshot = new MemorySnapshotWriter(snapshotId);
        try (SnapshotWriter<String> snapshotWriter = snapshotWriter(context, memorySnapshot)) {
            snapshotWriter.append(records);
            snapshotWriter.freeze();
        }

        ByteBuffer sendingBuffer = memorySnapshot.buffer().slice();
        sendingBuffer.limit(sendingBuffer.limit() / 2);

        context.deliverResponse(
            snapshotRequest.correlationId,
            snapshotRequest.destinationId(),
            fetchSnapshotResponse(
                context.metadataPartition,
                epoch,
                leaderId,
                snapshotId,
                memorySnapshot.buffer().remaining(),
                0L,
                sendingBuffer
            )
        );

        context.pollUntilRequest();
        snapshotRequest = context.assertSentFetchSnapshotRequest();
        request = assertFetchSnapshotRequest(
                snapshotRequest,
                context.metadataPartition,
                localId,
                Integer.MAX_VALUE
        ).get();
        assertEquals(snapshotId.offset, request.snapshotId().endOffset());
        assertEquals(snapshotId.epoch, request.snapshotId().epoch());
        assertEquals(sendingBuffer.limit(), request.position());

        sendingBuffer = memorySnapshot.buffer().slice();
        sendingBuffer.position(Math.toIntExact(request.position()));

        context.deliverResponse(
            snapshotRequest.correlationId,
            snapshotRequest.destinationId(),
            fetchSnapshotResponse(
                context.metadataPartition,
                epoch,
                leaderId,
                snapshotId,
                memorySnapshot.buffer().remaining(),
                request.position(),
                sendingBuffer
            )
        );

        context.pollUntilRequest();

        try (RawSnapshotReader snapshot = context.log.readSnapshot(snapshotId).get()) {
            assertEquals(memorySnapshot.buffer().remaining(), snapshot.sizeInBytes());
            SnapshotWriterTest.assertSnapshot(Arrays.asList(records), snapshot);
        }
    }

    @Test
    public void testFetchSnapshotResponseMissingSnapshot() throws Exception {
        int localId = 0;
        int leaderId = localId + 1;
        Set<Integer> voters = Utils.mkSet(localId, leaderId);
        int epoch = 2;
        OffsetAndEpoch snapshotId = new OffsetAndEpoch(100L, 1);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withElectedLeader(epoch, leaderId)
            .build();

        context.pollUntilRequest();
        RaftRequest.Outbound fetchRequest = context.assertSentFetchRequest();
        context.assertFetchRequestData(fetchRequest, epoch, 0L, 0);

        context.deliverResponse(
            fetchRequest.correlationId,
            fetchRequest.destinationId(),
            snapshotFetchResponse(context.metadataPartition, epoch, leaderId, snapshotId, 200L)
        );

        context.pollUntilRequest();
        RaftRequest.Outbound snapshotRequest = context.assertSentFetchSnapshotRequest();
        FetchSnapshotRequestData.PartitionSnapshot request = assertFetchSnapshotRequest(
                snapshotRequest,
                context.metadataPartition,
                localId,
                Integer.MAX_VALUE
        ).get();
        assertEquals(snapshotId.offset, request.snapshotId().endOffset());
        assertEquals(snapshotId.epoch, request.snapshotId().epoch());
        assertEquals(0, request.position());

        // Reply with a snapshot not found error
        context.deliverResponse(
            snapshotRequest.correlationId,
            snapshotRequest.destinationId(),
            FetchSnapshotResponse.singleton(
                context.metadataPartition,
                responsePartitionSnapshot -> {
                    responsePartitionSnapshot
                        .currentLeader()
                        .setLeaderEpoch(epoch)
                        .setLeaderId(leaderId);

                    return responsePartitionSnapshot
                        .setErrorCode(Errors.SNAPSHOT_NOT_FOUND.code());
                }
            )
        );

        context.pollUntilRequest();
        fetchRequest = context.assertSentFetchRequest();
        context.assertFetchRequestData(fetchRequest, epoch, 0L, 0);
    }

    @Test
    public void testFetchSnapshotResponseFromNewerEpochNotLeader() throws Exception {
        int localId = 0;
        int firstLeaderId = localId + 1;
        int secondLeaderId = firstLeaderId + 1;
        Set<Integer> voters = Utils.mkSet(localId, firstLeaderId, secondLeaderId);
        int epoch = 2;
        OffsetAndEpoch snapshotId = new OffsetAndEpoch(100L, 1);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withElectedLeader(epoch, firstLeaderId)
            .build();

        context.pollUntilRequest();
        RaftRequest.Outbound fetchRequest = context.assertSentFetchRequest();
        context.assertFetchRequestData(fetchRequest, epoch, 0L, 0);

        context.deliverResponse(
            fetchRequest.correlationId,
            fetchRequest.destinationId(),
            snapshotFetchResponse(context.metadataPartition, epoch, firstLeaderId, snapshotId, 200L)
        );

        context.pollUntilRequest();
        RaftRequest.Outbound snapshotRequest = context.assertSentFetchSnapshotRequest();
        FetchSnapshotRequestData.PartitionSnapshot request = assertFetchSnapshotRequest(
                snapshotRequest,
                context.metadataPartition,
                localId,
                Integer.MAX_VALUE
        ).get();
        assertEquals(snapshotId.offset, request.snapshotId().endOffset());
        assertEquals(snapshotId.epoch, request.snapshotId().epoch());
        assertEquals(0, request.position());

        // Reply with new leader response
        context.deliverResponse(
            snapshotRequest.correlationId,
            snapshotRequest.destinationId(),
            FetchSnapshotResponse.singleton(
                context.metadataPartition,
                responsePartitionSnapshot -> {
                    responsePartitionSnapshot
                        .currentLeader()
                        .setLeaderEpoch(epoch + 1)
                        .setLeaderId(secondLeaderId);

                    return responsePartitionSnapshot
                        .setErrorCode(Errors.FENCED_LEADER_EPOCH.code());
                }
            )
        );

        context.pollUntilRequest();
        fetchRequest = context.assertSentFetchRequest();
        context.assertFetchRequestData(fetchRequest, epoch + 1, 0L, 0);
    }

    @Test
    public void testFetchSnapshotResponseFromNewerEpochLeader() throws Exception {
        int localId = 0;
        int leaderId = localId + 1;
        Set<Integer> voters = Utils.mkSet(localId, leaderId);
        int epoch = 2;
        OffsetAndEpoch snapshotId = new OffsetAndEpoch(100L, 1);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withElectedLeader(epoch, leaderId)
            .build();

        context.pollUntilRequest();
        RaftRequest.Outbound fetchRequest = context.assertSentFetchRequest();
        context.assertFetchRequestData(fetchRequest, epoch, 0L, 0);

        context.deliverResponse(
            fetchRequest.correlationId,
            fetchRequest.destinationId(),
            snapshotFetchResponse(context.metadataPartition, epoch, leaderId, snapshotId, 200L)
        );

        context.pollUntilRequest();
        RaftRequest.Outbound snapshotRequest = context.assertSentFetchSnapshotRequest();
        FetchSnapshotRequestData.PartitionSnapshot request = assertFetchSnapshotRequest(
                snapshotRequest,
                context.metadataPartition,
                localId,
                Integer.MAX_VALUE
        ).get();
        assertEquals(snapshotId.offset, request.snapshotId().endOffset());
        assertEquals(snapshotId.epoch, request.snapshotId().epoch());
        assertEquals(0, request.position());

        // Reply with new leader epoch
        context.deliverResponse(
            snapshotRequest.correlationId,
            snapshotRequest.destinationId(),
            FetchSnapshotResponse.singleton(
                context.metadataPartition,
                responsePartitionSnapshot -> {
                    responsePartitionSnapshot
                        .currentLeader()
                        .setLeaderEpoch(epoch + 1)
                        .setLeaderId(leaderId);

                    return responsePartitionSnapshot
                        .setErrorCode(Errors.FENCED_LEADER_EPOCH.code());
                }
            )
        );

        context.pollUntilRequest();
        fetchRequest = context.assertSentFetchRequest();
        context.assertFetchRequestData(fetchRequest, epoch + 1, 0L, 0);
    }

    @Test
    public void testFetchSnapshotResponseFromOlderEpoch() throws Exception {
        int localId = 0;
        int leaderId = localId + 1;
        Set<Integer> voters = Utils.mkSet(localId, leaderId);
        int epoch = 2;
        OffsetAndEpoch snapshotId = new OffsetAndEpoch(100L, 1);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withElectedLeader(epoch, leaderId)
            .build();

        context.pollUntilRequest();
        RaftRequest.Outbound fetchRequest = context.assertSentFetchRequest();
        context.assertFetchRequestData(fetchRequest, epoch, 0L, 0);

        context.deliverResponse(
            fetchRequest.correlationId,
            fetchRequest.destinationId(),
            snapshotFetchResponse(context.metadataPartition, epoch, leaderId, snapshotId, 200L)
        );

        context.pollUntilRequest();
        RaftRequest.Outbound snapshotRequest = context.assertSentFetchSnapshotRequest();
        FetchSnapshotRequestData.PartitionSnapshot request = assertFetchSnapshotRequest(
                snapshotRequest,
                context.metadataPartition,
                localId,
                Integer.MAX_VALUE
        ).get();
        assertEquals(snapshotId.offset, request.snapshotId().endOffset());
        assertEquals(snapshotId.epoch, request.snapshotId().epoch());
        assertEquals(0, request.position());

        // Reply with unknown leader epoch
        context.deliverResponse(
            snapshotRequest.correlationId,
            snapshotRequest.destinationId(),
            FetchSnapshotResponse.singleton(
                context.metadataPartition,
                responsePartitionSnapshot -> {
                    responsePartitionSnapshot
                        .currentLeader()
                        .setLeaderEpoch(epoch - 1)
                        .setLeaderId(leaderId + 1);

                    return responsePartitionSnapshot
                        .setErrorCode(Errors.UNKNOWN_LEADER_EPOCH.code());
                }
            )
        );

        context.pollUntilRequest();

        // Follower should resend the fetch snapshot request
        snapshotRequest = context.assertSentFetchSnapshotRequest();
        request = assertFetchSnapshotRequest(
                snapshotRequest,
                context.metadataPartition,
                localId,
                Integer.MAX_VALUE
        ).get();
        assertEquals(snapshotId.offset, request.snapshotId().endOffset());
        assertEquals(snapshotId.epoch, request.snapshotId().epoch());
        assertEquals(0, request.position());
    }

    @Test
    public void testFetchSnapshotResponseWithInvalidId() throws Exception {
        int localId = 0;
        int leaderId = localId + 1;
        Set<Integer> voters = Utils.mkSet(localId, leaderId);
        int epoch = 2;
        OffsetAndEpoch snapshotId = new OffsetAndEpoch(100L, 1);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withElectedLeader(epoch, leaderId)
            .build();

        context.pollUntilRequest();
        RaftRequest.Outbound fetchRequest = context.assertSentFetchRequest();
        context.assertFetchRequestData(fetchRequest, epoch, 0L, 0);

        context.deliverResponse(
            fetchRequest.correlationId,
            fetchRequest.destinationId(),
            snapshotFetchResponse(context.metadataPartition, epoch, leaderId, snapshotId, 200L)
        );

        context.pollUntilRequest();
        RaftRequest.Outbound snapshotRequest = context.assertSentFetchSnapshotRequest();
        FetchSnapshotRequestData.PartitionSnapshot request = assertFetchSnapshotRequest(
                snapshotRequest,
                context.metadataPartition,
                localId,
                Integer.MAX_VALUE
        ).get();
        assertEquals(snapshotId.offset, request.snapshotId().endOffset());
        assertEquals(snapshotId.epoch, request.snapshotId().epoch());
        assertEquals(0, request.position());

        // Reply with an invalid snapshot id endOffset
        context.deliverResponse(
            snapshotRequest.correlationId,
            snapshotRequest.destinationId(),
            FetchSnapshotResponse.singleton(
                context.metadataPartition,
                responsePartitionSnapshot -> {
                    responsePartitionSnapshot
                        .currentLeader()
                        .setLeaderEpoch(epoch)
                        .setLeaderId(leaderId);

                    responsePartitionSnapshot
                        .snapshotId()
                        .setEndOffset(-1)
                        .setEpoch(snapshotId.epoch);

                    return responsePartitionSnapshot;
                }
            )
        );

        context.pollUntilRequest();

        // Follower should send a fetch request
        fetchRequest = context.assertSentFetchRequest();
        context.assertFetchRequestData(fetchRequest, epoch, 0L, 0);

        context.deliverResponse(
            fetchRequest.correlationId,
            fetchRequest.destinationId(),
            snapshotFetchResponse(context.metadataPartition, epoch, leaderId, snapshotId, 200L)
        );

        context.pollUntilRequest();

        snapshotRequest = context.assertSentFetchSnapshotRequest();
        request = assertFetchSnapshotRequest(
                snapshotRequest,
                context.metadataPartition,
                localId,
                Integer.MAX_VALUE
        ).get();
        assertEquals(snapshotId.offset, request.snapshotId().endOffset());
        assertEquals(snapshotId.epoch, request.snapshotId().epoch());
        assertEquals(0, request.position());

        // Reply with an invalid snapshot id epoch
        context.deliverResponse(
            snapshotRequest.correlationId,
            snapshotRequest.destinationId(),
            FetchSnapshotResponse.singleton(
                context.metadataPartition,
                responsePartitionSnapshot -> {
                    responsePartitionSnapshot
                        .currentLeader()
                        .setLeaderEpoch(epoch)
                        .setLeaderId(leaderId);

                    responsePartitionSnapshot
                        .snapshotId()
                        .setEndOffset(snapshotId.offset)
                        .setEpoch(-1);

                    return responsePartitionSnapshot;
                }
            )
        );

        context.pollUntilRequest();

        // Follower should send a fetch request
        fetchRequest = context.assertSentFetchRequest();
        context.assertFetchRequestData(fetchRequest, epoch, 0L, 0);
    }

    @Test
    public void testFetchSnapshotResponseToNotFollower() throws Exception {
        int localId = 0;
        int leaderId = localId + 1;
        Set<Integer> voters = Utils.mkSet(localId, leaderId);
        int epoch = 2;
        OffsetAndEpoch snapshotId = new OffsetAndEpoch(100L, 1);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withElectedLeader(epoch, leaderId)
            .build();

        context.pollUntilRequest();
        RaftRequest.Outbound fetchRequest = context.assertSentFetchRequest();
        context.assertFetchRequestData(fetchRequest, epoch, 0L, 0);

        context.deliverResponse(
            fetchRequest.correlationId,
            fetchRequest.destinationId(),
            snapshotFetchResponse(context.metadataPartition, epoch, leaderId, snapshotId, 200L)
        );

        context.pollUntilRequest();

        RaftRequest.Outbound snapshotRequest = context.assertSentFetchSnapshotRequest();
        FetchSnapshotRequestData.PartitionSnapshot request = assertFetchSnapshotRequest(
                snapshotRequest,
                context.metadataPartition,
                localId,
                Integer.MAX_VALUE
        ).get();
        assertEquals(snapshotId.offset, request.snapshotId().endOffset());
        assertEquals(snapshotId.epoch, request.snapshotId().epoch());
        assertEquals(0, request.position());

        // Sleeping for fetch timeout should transition to candidate
        context.time.sleep(context.fetchTimeoutMs);

        context.pollUntilRequest();

        context.assertSentVoteRequest(epoch + 1, 0, 0L, 1);
        context.assertVotedCandidate(epoch + 1, localId);

        // Send the response late
        context.deliverResponse(
            snapshotRequest.correlationId,
            snapshotRequest.destinationId(),
            FetchSnapshotResponse.singleton(
                context.metadataPartition,
                responsePartitionSnapshot -> {
                    responsePartitionSnapshot
                        .currentLeader()
                        .setLeaderEpoch(epoch)
                        .setLeaderId(leaderId);

                    responsePartitionSnapshot
                        .snapshotId()
                        .setEndOffset(snapshotId.offset)
                        .setEpoch(-1);

                    return responsePartitionSnapshot;
                }
            )
        );

        // Assert that the response is ignored and the replicas stays as a candidate
        context.client.poll();
        context.assertVotedCandidate(epoch + 1, localId);
    }

    private static FetchSnapshotRequestData fetchSnapshotRequest(
        TopicPartition topicPartition,
        int epoch,
        OffsetAndEpoch offsetAndEpoch,
        int maxBytes,
        long position
    ) {
        FetchSnapshotRequestData.SnapshotId snapshotId = new FetchSnapshotRequestData.SnapshotId()
            .setEndOffset(offsetAndEpoch.offset)
            .setEpoch(offsetAndEpoch.epoch);

        FetchSnapshotRequestData request = FetchSnapshotRequest.singleton(
            topicPartition,
            snapshotPartition -> {
                return snapshotPartition
                    .setCurrentLeaderEpoch(epoch)
                    .setSnapshotId(snapshotId)
                    .setPosition(position);
            }
        );

        return request.setMaxBytes(maxBytes);
    }

    private static FetchSnapshotResponseData fetchSnapshotResponse(
        TopicPartition topicPartition,
        int leaderEpoch,
        int leaderId,
        OffsetAndEpoch snapshotId,
        long size,
        long position,
        ByteBuffer buffer
    ) {
        return FetchSnapshotResponse.singleton(
            topicPartition,
            partitionSnapshot -> {
                partitionSnapshot.currentLeader()
                    .setLeaderEpoch(leaderEpoch)
                    .setLeaderId(leaderId);

                partitionSnapshot.snapshotId()
                    .setEndOffset(snapshotId.offset)
                    .setEpoch(snapshotId.epoch);

                return partitionSnapshot
                    .setSize(size)
                    .setPosition(position)
                    .setBytes(buffer);
            }
        );
    }

    private static FetchResponseData snapshotFetchResponse(
        TopicPartition topicPartition,
        int epoch,
        int leaderId,
        OffsetAndEpoch snapshotId,
        long highWatermark
    ) {
        return RaftUtil.singletonFetchResponse(topicPartition, Errors.NONE, partitionData -> {
            partitionData
                .setErrorCode(Errors.NONE.code())
                .setHighWatermark(highWatermark);

            partitionData.currentLeader()
                .setLeaderEpoch(epoch)
                .setLeaderId(leaderId);

            partitionData.snapshotId()
                .setEpoch(snapshotId.epoch)
                .setEndOffset(snapshotId.offset);
        });
    }

    private static Optional<FetchSnapshotRequestData.PartitionSnapshot> assertFetchSnapshotRequest(
        RaftRequest.Outbound request,
        TopicPartition topicPartition,
        int replicaId,
        int maxBytes
    ) {
        assertTrue(request.data() instanceof FetchSnapshotRequestData);

        FetchSnapshotRequestData data = (FetchSnapshotRequestData) request.data();

        assertEquals(replicaId, data.replicaId());
        assertEquals(maxBytes, data.maxBytes());

        return FetchSnapshotRequest.forTopicPartition(data, topicPartition);
    }

    private static SnapshotWriter<String> snapshotWriter(RaftClientTestContext context, RawSnapshotWriter snapshot) {
        return new SnapshotWriter<>(
            snapshot,
            4 * 1024,
            MemoryPool.NONE,
            context.time,
            CompressionType.NONE,
            new StringSerde()
        );
    }

    private final static class MemorySnapshotWriter implements RawSnapshotWriter {
        private final OffsetAndEpoch snapshotId;
        private ByteBuffer data;
        private boolean frozen;

        public MemorySnapshotWriter(OffsetAndEpoch snapshotId) {
            this.snapshotId = snapshotId;
            this.data = ByteBuffer.allocate(0);
            this.frozen = false;
        }

        @Override
        public OffsetAndEpoch snapshotId() {
            return snapshotId;
        }

        @Override
        public long sizeInBytes() {
            if (frozen) {
                throw new RuntimeException("Snapshot is already frozen " + snapshotId);
            }

            return data.position();
        }

        @Override
        public void append(ByteBuffer buffer) {
            if (frozen) {
                throw new RuntimeException("Snapshot is already frozen " + snapshotId);
            }

            if (!(data.remaining() >= buffer.remaining())) {
                ByteBuffer old = data;
                old.flip();

                int newSize = Math.max(data.capacity() * 2, data.capacity() + buffer.remaining());
                data = ByteBuffer.allocate(newSize);

                data.put(old);
            }
            data.put(buffer);
        }

        @Override
        public boolean isFrozen() {
            return frozen;
        }

        @Override
        public void freeze() {
            if (frozen) {
                throw new RuntimeException("Snapshot is already frozen " + snapshotId);
            }

            frozen = true;
            data.flip();
        }

        @Override
        public void close() {}

        public ByteBuffer buffer() {
            return data;
        }
    }
}
