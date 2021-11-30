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
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.FetchSnapshotRequestData;
import org.apache.kafka.common.message.FetchSnapshotResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.UnalignedMemoryRecords;
import org.apache.kafka.common.requests.FetchSnapshotRequest;
import org.apache.kafka.common.requests.FetchSnapshotResponse;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.raft.internals.StringSerde;
import org.apache.kafka.snapshot.RawSnapshotReader;
import org.apache.kafka.snapshot.RawSnapshotWriter;
import org.apache.kafka.snapshot.SnapshotWriter;
import org.apache.kafka.snapshot.SnapshotReader;
import org.apache.kafka.snapshot.RecordsSnapshotWriter;
import org.apache.kafka.snapshot.SnapshotWriterReaderTest;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.OptionalInt;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

final public class KafkaRaftClientSnapshotTest {
    @Test
    public void testLeaderListenerNotified() throws Exception {
        int localId = 0;
        int otherNodeId = localId + 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        OffsetAndEpoch snapshotId = new OffsetAndEpoch(3, 1);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .appendToLog(snapshotId.epoch, Arrays.asList("a", "b", "c"))
            .appendToLog(snapshotId.epoch, Arrays.asList("d", "e", "f"))
            .withEmptySnapshot(snapshotId)
            .deleteBeforeSnapshot(snapshotId)
            .build();

        context.becomeLeader();
        int epoch = context.currentEpoch();

        // Advance the highWatermark
        long localLogEndOffset = context.log.endOffset().offset;
        context.deliverRequest(context.fetchRequest(epoch, otherNodeId, localLogEndOffset, epoch, 0));
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(localId));
        assertEquals(localLogEndOffset, context.client.highWatermark().getAsLong());

        // Check that listener was notified of the new snapshot
        try (SnapshotReader<String> snapshot = context.listener.drainHandledSnapshot().get()) {
            assertEquals(snapshotId, snapshot.snapshotId());
            SnapshotWriterReaderTest.assertSnapshot(Arrays.asList(), snapshot);
        }
    }

    @Test
    public void testFollowerListenerNotified() throws Exception {
        int localId = 0;
        int leaderId = localId + 1;
        Set<Integer> voters = Utils.mkSet(localId, leaderId);
        int epoch = 2;
        OffsetAndEpoch snapshotId = new OffsetAndEpoch(3, 1);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .appendToLog(snapshotId.epoch, Arrays.asList("a", "b", "c"))
            .appendToLog(snapshotId.epoch, Arrays.asList("d", "e", "f"))
            .withEmptySnapshot(snapshotId)
            .deleteBeforeSnapshot(snapshotId)
            .withElectedLeader(epoch, leaderId)
            .build();

        // Advance the highWatermark
        long localLogEndOffset = context.log.endOffset().offset;
        context.pollUntilRequest();
        RaftRequest.Outbound fetchRequest = context.assertSentFetchRequest();
        context.assertFetchRequestData(fetchRequest, epoch, localLogEndOffset, snapshotId.epoch);
        context.deliverResponse(
            fetchRequest.correlationId,
            fetchRequest.destinationId(),
            context.fetchResponse(epoch, leaderId, MemoryRecords.EMPTY, localLogEndOffset, Errors.NONE)
        );

        context.pollUntilRequest();
        context.assertSentFetchRequest(epoch, localLogEndOffset, snapshotId.epoch);

        // Check that listener was notified of the new snapshot
        try (SnapshotReader<String> snapshot = context.listener.drainHandledSnapshot().get()) {
            assertEquals(snapshotId, snapshot.snapshotId());
            SnapshotWriterReaderTest.assertSnapshot(Arrays.asList(), snapshot);
        }
    }

    @Test
    public void testSecondListenerNotified() throws Exception {
        int localId = 0;
        int leaderId = localId + 1;
        Set<Integer> voters = Utils.mkSet(localId, leaderId);
        int epoch = 2;
        OffsetAndEpoch snapshotId = new OffsetAndEpoch(3, 1);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .appendToLog(snapshotId.epoch, Arrays.asList("a", "b", "c"))
            .appendToLog(snapshotId.epoch, Arrays.asList("d", "e", "f"))
            .withEmptySnapshot(snapshotId)
            .deleteBeforeSnapshot(snapshotId)
            .withElectedLeader(epoch, leaderId)
            .build();

        // Advance the highWatermark
        long localLogEndOffset = context.log.endOffset().offset;
        context.pollUntilRequest();
        RaftRequest.Outbound fetchRequest = context.assertSentFetchRequest();
        context.assertFetchRequestData(fetchRequest, epoch, localLogEndOffset, snapshotId.epoch);
        context.deliverResponse(
            fetchRequest.correlationId,
            fetchRequest.destinationId(),
            context.fetchResponse(epoch, leaderId, MemoryRecords.EMPTY, localLogEndOffset, Errors.NONE)
        );

        context.pollUntilRequest();
        context.assertSentFetchRequest(epoch, localLogEndOffset, snapshotId.epoch);

        RaftClientTestContext.MockListener secondListener = new RaftClientTestContext.MockListener(OptionalInt.of(localId));
        context.client.register(secondListener);
        context.client.poll();

        // Check that the second listener was notified of the new snapshot
        try (SnapshotReader<String> snapshot = secondListener.drainHandledSnapshot().get()) {
            assertEquals(snapshotId, snapshot.snapshotId());
            SnapshotWriterReaderTest.assertSnapshot(Arrays.asList(), snapshot);
        }
    }

    @Test
    public void testListenerRenotified() throws Exception {
        int localId = 0;
        int otherNodeId = localId + 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        OffsetAndEpoch snapshotId = new OffsetAndEpoch(3, 1);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .appendToLog(snapshotId.epoch, Arrays.asList("a", "b", "c"))
            .appendToLog(snapshotId.epoch, Arrays.asList("d", "e", "f"))
            .appendToLog(snapshotId.epoch, Arrays.asList("g", "h", "i"))
            .withEmptySnapshot(snapshotId)
            .deleteBeforeSnapshot(snapshotId)
            .build();

        context.becomeLeader();
        int epoch = context.currentEpoch();

        // Stop the listener from reading commit batches
        context.listener.updateReadCommit(false);

        // Advance the highWatermark
        long localLogEndOffset = context.log.endOffset().offset;
        context.deliverRequest(context.fetchRequest(epoch, otherNodeId, localLogEndOffset, epoch, 0));
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(localId));
        assertEquals(localLogEndOffset, context.client.highWatermark().getAsLong());

        // Check that listener was notified of the new snapshot
        try (SnapshotReader<String> snapshot = context.listener.drainHandledSnapshot().get()) {
            assertEquals(snapshotId, snapshot.snapshotId());
            SnapshotWriterReaderTest.assertSnapshot(Arrays.asList(), snapshot);
        }

        // Generate a new snapshot
        OffsetAndEpoch secondSnapshotId = new OffsetAndEpoch(localLogEndOffset, epoch);
        try (SnapshotWriter<String> snapshot = context.client.createSnapshot(secondSnapshotId.offset - 1, secondSnapshotId.epoch, 0).get()) {
            assertEquals(secondSnapshotId, snapshot.snapshotId());
            snapshot.freeze();
        }
        context.log.deleteBeforeSnapshot(secondSnapshotId);
        context.client.poll();

        // Resume the listener from reading commit batches
        context.listener.updateReadCommit(true);

        context.client.poll();
        // Check that listener was notified of the second snapshot
        try (SnapshotReader<String> snapshot = context.listener.drainHandledSnapshot().get()) {
            assertEquals(secondSnapshotId, snapshot.snapshotId());
            SnapshotWriterReaderTest.assertSnapshot(Arrays.asList(), snapshot);
        }
    }

    @Test
    public void testFetchRequestOffsetLessThanLogStart() throws Exception {
        int localId = 0;
        int otherNodeId = localId + 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withAppendLingerMs(1)
            .build();

        context.becomeLeader();
        int epoch = context.currentEpoch();

        List<String> appendRecords = Arrays.asList("a", "b", "c");
        context.client.scheduleAppend(epoch, appendRecords);
        context.time.sleep(context.appendLingerMs());
        context.client.poll();

        long localLogEndOffset = context.log.endOffset().offset;
        assertTrue(
            appendRecords.size() <= localLogEndOffset,
            String.format("Record length = %s, log end offset = %s", appendRecords.size(), localLogEndOffset)
        );

        // Advance the highWatermark
        context.advanceLocalLeaderHighWatermarkToLogEndOffset();

        OffsetAndEpoch snapshotId = new OffsetAndEpoch(localLogEndOffset, epoch);
        try (SnapshotWriter<String> snapshot = context.client.createSnapshot(snapshotId.offset - 1, snapshotId.epoch, 0).get()) {
            assertEquals(snapshotId, snapshot.snapshotId());
            snapshot.freeze();
        }
        context.log.deleteBeforeSnapshot(snapshotId);
        context.client.poll();

        // Send Fetch request less than start offset
        context.deliverRequest(context.fetchRequest(epoch, otherNodeId, 0, epoch, 0));
        context.pollUntilResponse();
        FetchResponseData.PartitionData partitionResponse = context.assertSentFetchPartitionResponse();
        assertEquals(Errors.NONE, Errors.forCode(partitionResponse.errorCode()));
        assertEquals(epoch, partitionResponse.currentLeader().leaderEpoch());
        assertEquals(localId, partitionResponse.currentLeader().leaderId());
        assertEquals(snapshotId.epoch, partitionResponse.snapshotId().epoch());
        assertEquals(snapshotId.offset, partitionResponse.snapshotId().endOffset());
    }

    @Test
    public void testFetchRequestWithLargerLastFetchedEpoch() throws Exception {
        int localId = 0;
        int otherNodeId = localId + 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);

        OffsetAndEpoch oldestSnapshotId = new OffsetAndEpoch(3, 2);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .appendToLog(oldestSnapshotId.epoch, Arrays.asList("a", "b", "c"))
            .appendToLog(oldestSnapshotId.epoch, Arrays.asList("d", "e", "f"))
            .withAppendLingerMs(1)
            .build();

        context.becomeLeader();
        int epoch = context.currentEpoch();
        assertEquals(oldestSnapshotId.epoch + 1, epoch);

        // Advance the highWatermark
        context.advanceLocalLeaderHighWatermarkToLogEndOffset();

        // Create a snapshot at the high watermark
        try (SnapshotWriter<String> snapshot = context.client.createSnapshot(oldestSnapshotId.offset - 1, oldestSnapshotId.epoch, 0).get()) {
            assertEquals(oldestSnapshotId, snapshot.snapshotId());
            snapshot.freeze();
        }
        context.client.poll();

        context.client.scheduleAppend(epoch, Arrays.asList("g", "h", "i"));
        context.time.sleep(context.appendLingerMs());
        context.client.poll();

        // It is an invalid request to send an last fetched epoch greater than the current epoch
        context.deliverRequest(context.fetchRequest(epoch, otherNodeId, oldestSnapshotId.offset + 1, epoch + 1, 0));
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.INVALID_REQUEST, epoch, OptionalInt.of(localId));
    }

    @Test
    public void testFetchRequestTruncateToLogStart() throws Exception {
        int localId = 0;
        int otherNodeId = localId + 1;
        int syncNodeId = otherNodeId + 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId, syncNodeId);

        OffsetAndEpoch oldestSnapshotId = new OffsetAndEpoch(3, 2);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .appendToLog(oldestSnapshotId.epoch, Arrays.asList("a", "b", "c"))
            .appendToLog(oldestSnapshotId.epoch + 2, Arrays.asList("d", "e", "f"))
            .withAppendLingerMs(1)
            .build();

        context.becomeLeader();
        int epoch = context.currentEpoch();
        assertEquals(oldestSnapshotId.epoch + 2 + 1, epoch);

        // Advance the highWatermark
        context.advanceLocalLeaderHighWatermarkToLogEndOffset();

        // Create a snapshot at the high watermark
        try (SnapshotWriter<String> snapshot = context.client.createSnapshot(oldestSnapshotId.offset - 1, oldestSnapshotId.epoch, 0).get()) {
            assertEquals(oldestSnapshotId, snapshot.snapshotId());
            snapshot.freeze();
        }
        context.client.poll();

        // This should truncate to the old snapshot
        context.deliverRequest(
            context.fetchRequest(epoch, otherNodeId, oldestSnapshotId.offset + 1, oldestSnapshotId.epoch + 1, 0)
        );
        context.pollUntilResponse();
        FetchResponseData.PartitionData partitionResponse = context.assertSentFetchPartitionResponse();
        assertEquals(Errors.NONE, Errors.forCode(partitionResponse.errorCode()));
        assertEquals(epoch, partitionResponse.currentLeader().leaderEpoch());
        assertEquals(localId, partitionResponse.currentLeader().leaderId());
        assertEquals(oldestSnapshotId.epoch, partitionResponse.divergingEpoch().epoch());
        assertEquals(oldestSnapshotId.offset, partitionResponse.divergingEpoch().endOffset());
    }

    @Test
    public void testFetchRequestAtLogStartOffsetWithValidEpoch() throws Exception {
        int localId = 0;
        int otherNodeId = localId + 1;
        int syncNodeId = otherNodeId + 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId, syncNodeId);

        OffsetAndEpoch oldestSnapshotId = new OffsetAndEpoch(3, 2);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .appendToLog(oldestSnapshotId.epoch, Arrays.asList("a", "b", "c"))
            .appendToLog(oldestSnapshotId.epoch, Arrays.asList("d", "e", "f"))
            .appendToLog(oldestSnapshotId.epoch + 2, Arrays.asList("g", "h", "i"))
            .withAppendLingerMs(1)
            .build();

        context.becomeLeader();
        int epoch = context.currentEpoch();
        assertEquals(oldestSnapshotId.epoch + 2 + 1, epoch);

        // Advance the highWatermark
        context.advanceLocalLeaderHighWatermarkToLogEndOffset();

        // Create a snapshot at the high watermark
        try (SnapshotWriter<String> snapshot = context.client.createSnapshot(oldestSnapshotId.offset - 1, oldestSnapshotId.epoch, 0).get()) {
            assertEquals(oldestSnapshotId, snapshot.snapshotId());
            snapshot.freeze();
        }
        context.client.poll();

        // Send fetch request at log start offset with valid last fetched epoch
        context.deliverRequest(
            context.fetchRequest(epoch, otherNodeId, oldestSnapshotId.offset, oldestSnapshotId.epoch, 0)
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(localId));
    }

    @Test
    public void testFetchRequestAtLogStartOffsetWithInvalidEpoch() throws Exception {
        int localId = 0;
        int otherNodeId = localId + 1;
        int syncNodeId = otherNodeId + 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId, syncNodeId);

        OffsetAndEpoch oldestSnapshotId = new OffsetAndEpoch(3, 2);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .appendToLog(oldestSnapshotId.epoch, Arrays.asList("a", "b", "c"))
            .appendToLog(oldestSnapshotId.epoch, Arrays.asList("d", "e", "f"))
            .appendToLog(oldestSnapshotId.epoch + 2, Arrays.asList("g", "h", "i"))
            .withAppendLingerMs(1)
            .build();

        context.becomeLeader();
        int epoch = context.currentEpoch();
        assertEquals(oldestSnapshotId.epoch + 2 + 1, epoch);

        // Advance the highWatermark
        context.advanceLocalLeaderHighWatermarkToLogEndOffset();

        // Create a snapshot at the high watermark
        try (SnapshotWriter<String> snapshot = context.client.createSnapshot(oldestSnapshotId.offset - 1, oldestSnapshotId.epoch, 0).get()) {
            assertEquals(oldestSnapshotId, snapshot.snapshotId());
            snapshot.freeze();
        }
        context.log.deleteBeforeSnapshot(oldestSnapshotId);
        context.client.poll();

        // Send fetch with log start offset and invalid last fetched epoch
        context.deliverRequest(
            context.fetchRequest(epoch, otherNodeId, oldestSnapshotId.offset, oldestSnapshotId.epoch + 1, 0)
        );
        context.pollUntilResponse();
        FetchResponseData.PartitionData partitionResponse = context.assertSentFetchPartitionResponse();
        assertEquals(Errors.NONE, Errors.forCode(partitionResponse.errorCode()));
        assertEquals(epoch, partitionResponse.currentLeader().leaderEpoch());
        assertEquals(localId, partitionResponse.currentLeader().leaderId());
        assertEquals(oldestSnapshotId.epoch, partitionResponse.snapshotId().epoch());
        assertEquals(oldestSnapshotId.offset, partitionResponse.snapshotId().endOffset());
    }

    @Test
    public void testFetchRequestWithLastFetchedEpochLessThanOldestSnapshot() throws Exception {
        int localId = 0;
        int otherNodeId = localId + 1;
        int syncNodeId = otherNodeId + 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId, syncNodeId);

        OffsetAndEpoch oldestSnapshotId = new OffsetAndEpoch(3, 2);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .appendToLog(oldestSnapshotId.epoch, Arrays.asList("a", "b", "c"))
            .appendToLog(oldestSnapshotId.epoch, Arrays.asList("d", "e", "f"))
            .appendToLog(oldestSnapshotId.epoch + 2, Arrays.asList("g", "h", "i"))
            .withAppendLingerMs(1)
            .build();

        context.becomeLeader();
        int epoch = context.currentEpoch();
        assertEquals(oldestSnapshotId.epoch + 2 + 1, epoch);

        // Advance the highWatermark
        context.advanceLocalLeaderHighWatermarkToLogEndOffset();

        // Create a snapshot at the high watermark
        try (SnapshotWriter<String> snapshot = context.client.createSnapshot(oldestSnapshotId.offset - 1, oldestSnapshotId.epoch, 0).get()) {
            assertEquals(oldestSnapshotId, snapshot.snapshotId());
            snapshot.freeze();
        }
        context.client.poll();

        // Send a epoch less than the oldest snapshot
        context.deliverRequest(
            context.fetchRequest(
                epoch,
                otherNodeId,
                context.log.endOffset().offset,
                oldestSnapshotId.epoch - 1,
                0
            )
        );
        context.pollUntilResponse();
        FetchResponseData.PartitionData partitionResponse = context.assertSentFetchPartitionResponse();
        assertEquals(Errors.NONE, Errors.forCode(partitionResponse.errorCode()));
        assertEquals(epoch, partitionResponse.currentLeader().leaderEpoch());
        assertEquals(localId, partitionResponse.currentLeader().leaderId());
        assertEquals(oldestSnapshotId.epoch, partitionResponse.snapshotId().epoch());
        assertEquals(oldestSnapshotId.offset, partitionResponse.snapshotId().endOffset());
    }

    @Test
    public void testFetchSnapshotRequestMissingSnapshot() throws Exception {
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
    public void testFetchSnapshotRequestUnknownPartition() throws Exception {
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
        OffsetAndEpoch snapshotId = new OffsetAndEpoch(1, 1);
        List<String> records = Arrays.asList("foo", "bar");

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .appendToLog(snapshotId.epoch, Arrays.asList("a"))
            .build();

        context.becomeLeader();
        int epoch = context.currentEpoch();

        context.advanceLocalLeaderHighWatermarkToLogEndOffset();

        try (SnapshotWriter<String> snapshot = context.client.createSnapshot(snapshotId.offset - 1, snapshotId.epoch, 0).get()) {
            assertEquals(snapshotId, snapshot.snapshotId());
            snapshot.append(records);
            snapshot.freeze();
        }

        RawSnapshotReader snapshot = context.log.readSnapshot(snapshotId).get();
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
        assertEquals(snapshot.sizeInBytes(), response.unalignedRecords().sizeInBytes());

        UnalignedMemoryRecords memoryRecords = (UnalignedMemoryRecords) snapshot.slice(0, Math.toIntExact(snapshot.sizeInBytes()));

        assertEquals(memoryRecords.buffer(), ((UnalignedMemoryRecords) response.unalignedRecords()).buffer());
    }

    @Test
    public void testPartialFetchSnapshotRequestAsLeader() throws Exception {
        int localId = 0;
        Set<Integer> voters = Utils.mkSet(localId, localId + 1);
        OffsetAndEpoch snapshotId = new OffsetAndEpoch(2, 1);
        List<String> records = Arrays.asList("foo", "bar");

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .appendToLog(snapshotId.epoch, records)
            .build();

        context.becomeLeader();
        int epoch = context.currentEpoch();

        context.advanceLocalLeaderHighWatermarkToLogEndOffset();

        try (SnapshotWriter<String> snapshot = context.client.createSnapshot(snapshotId.offset - 1, snapshotId.epoch, 0).get()) {
            assertEquals(snapshotId, snapshot.snapshotId());
            snapshot.append(records);
            snapshot.freeze();
        }

        RawSnapshotReader snapshot = context.log.readSnapshot(snapshotId).get();
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
        assertEquals(snapshot.sizeInBytes() / 2, response.unalignedRecords().sizeInBytes());

        UnalignedMemoryRecords memoryRecords = (UnalignedMemoryRecords) snapshot.slice(0, Math.toIntExact(snapshot.sizeInBytes()));
        ByteBuffer snapshotBuffer = memoryRecords.buffer();

        ByteBuffer responseBuffer = ByteBuffer.allocate(Math.toIntExact(snapshot.sizeInBytes()));
        responseBuffer.put(((UnalignedMemoryRecords) response.unalignedRecords()).buffer());

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
        assertEquals(snapshot.sizeInBytes() - (snapshot.sizeInBytes() / 2), response.unalignedRecords().sizeInBytes());

        responseBuffer.put(((UnalignedMemoryRecords) response.unalignedRecords()).buffer());
        assertEquals(snapshotBuffer, responseBuffer.flip());
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
        OffsetAndEpoch snapshotId = new OffsetAndEpoch(1, 1);
        List<String> records = Arrays.asList("foo", "bar");

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .appendToLog(snapshotId.epoch, Arrays.asList("a"))
            .build();

        context.becomeLeader();
        int epoch = context.currentEpoch();

        context.advanceLocalLeaderHighWatermarkToLogEndOffset();

        try (SnapshotWriter<String> snapshot = context.client.createSnapshot(snapshotId.offset - 1, snapshotId.epoch, 0).get()) {
            assertEquals(snapshotId, snapshot.snapshotId());
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

        RawSnapshotReader snapshot = context.log.readSnapshot(snapshotId).get();
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
            snapshotFetchResponse(context.metadataPartition, context.metadataTopicId, epoch, leaderId, invalidEpoch, 200L)
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
            snapshotFetchResponse(context.metadataPartition, context.metadataTopicId, epoch, leaderId, invalidEndOffset, 200L)
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
            snapshotFetchResponse(context.metadataPartition, context.metadataTopicId, epoch, leaderId, snapshotId, 200L)
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
        fetchRequest = context.assertSentFetchRequest();
        context.assertFetchRequestData(fetchRequest, epoch, snapshotId.offset, snapshotId.epoch);

        // Check that the snapshot was written to the log
        RawSnapshotReader snapshot = context.log.readSnapshot(snapshotId).get();
        assertEquals(memorySnapshot.buffer().remaining(), snapshot.sizeInBytes());
        SnapshotWriterReaderTest.assertSnapshot(Arrays.asList(records), snapshot);

        // Check that listener was notified of the new snapshot
        try (SnapshotReader<String> reader = context.listener.drainHandledSnapshot().get()) {
            assertEquals(snapshotId, reader.snapshotId());
            SnapshotWriterReaderTest.assertSnapshot(Arrays.asList(records), reader);
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
            snapshotFetchResponse(context.metadataPartition, context.metadataTopicId, epoch, leaderId, snapshotId, 200L)
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
        fetchRequest = context.assertSentFetchRequest();
        context.assertFetchRequestData(fetchRequest, epoch, snapshotId.offset, snapshotId.epoch);

        // Check that the snapshot was written to the log
        RawSnapshotReader snapshot = context.log.readSnapshot(snapshotId).get();
        assertEquals(memorySnapshot.buffer().remaining(), snapshot.sizeInBytes());
        SnapshotWriterReaderTest.assertSnapshot(Arrays.asList(records), snapshot);

        // Check that listener was notified of the new snapshot
        try (SnapshotReader<String> reader = context.listener.drainHandledSnapshot().get()) {
            assertEquals(snapshotId, reader.snapshotId());
            SnapshotWriterReaderTest.assertSnapshot(Arrays.asList(records), reader);
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
            snapshotFetchResponse(context.metadataPartition, context.metadataTopicId, epoch, leaderId, snapshotId, 200L)
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
            snapshotFetchResponse(context.metadataPartition, context.metadataTopicId, epoch, firstLeaderId, snapshotId, 200L)
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
            snapshotFetchResponse(context.metadataPartition, context.metadataTopicId, epoch, leaderId, snapshotId, 200L)
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
            snapshotFetchResponse(context.metadataPartition, context.metadataTopicId, epoch, leaderId, snapshotId, 200L)
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
            snapshotFetchResponse(context.metadataPartition, context.metadataTopicId, epoch, leaderId, snapshotId, 200L)
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
            snapshotFetchResponse(context.metadataPartition, context.metadataTopicId, epoch, leaderId, snapshotId, 200L)
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
            snapshotFetchResponse(context.metadataPartition, context.metadataTopicId, epoch, leaderId, snapshotId, 200L)
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
                        .setEpoch(snapshotId.epoch);

                    return responsePartitionSnapshot;
                }
            )
        );

        // Assert that the response is ignored and the replicas stays as a candidate
        context.client.poll();
        context.assertVotedCandidate(epoch + 1, localId);
    }

    @Test
    public void testFetchSnapshotRequestClusterIdValidation() throws Exception {
        int localId = 0;
        int otherNodeId = 1;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);

        RaftClientTestContext context = RaftClientTestContext.initializeAsLeader(localId, voters, epoch);

        // null cluster id is accepted
        context.deliverRequest(
            fetchSnapshotRequest(
                context.clusterId.toString(),
                context.metadataPartition,
                epoch,
                new OffsetAndEpoch(0, 0),
                Integer.MAX_VALUE,
                0
            )
        );
        context.pollUntilResponse();
        context.assertSentFetchSnapshotResponse(context.metadataPartition);

        // null cluster id is accepted
        context.deliverRequest(
            fetchSnapshotRequest(
                null,
                context.metadataPartition,
                epoch,
                new OffsetAndEpoch(0, 0),
                Integer.MAX_VALUE,
                0
            )
        );
        context.pollUntilResponse();
        context.assertSentFetchSnapshotResponse(context.metadataPartition);

        // empty cluster id is rejected
        context.deliverRequest(
            fetchSnapshotRequest(
                "",
                context.metadataPartition,
                epoch,
                new OffsetAndEpoch(0, 0),
                Integer.MAX_VALUE,
                0
            )
        );
        context.pollUntilResponse();
        context.assertSentFetchSnapshotResponse(Errors.INCONSISTENT_CLUSTER_ID);

        // invalid cluster id is rejected
        context.deliverRequest(
            fetchSnapshotRequest(
                "invalid-uuid",
                context.metadataPartition,
                epoch,
                new OffsetAndEpoch(0, 0),
                Integer.MAX_VALUE,
                0
            )
        );
        context.pollUntilResponse();
        context.assertSentFetchSnapshotResponse(Errors.INCONSISTENT_CLUSTER_ID);
    }

    @Test
    public void testCreateSnapshotAsLeaderWithInvalidSnapshotId() throws Exception {
        int localId = 0;
        int otherNodeId = localId + 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        int epoch = 2;

        List<String> appendRecords = Arrays.asList("a", "b", "c");
        OffsetAndEpoch invalidSnapshotId1 = new OffsetAndEpoch(3, epoch);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
                .appendToLog(epoch, appendRecords)
                .withAppendLingerMs(1)
                .build();

        context.becomeLeader();
        int currentEpoch = context.currentEpoch();

        // When leader creating snapshot:
        // 1.1 high watermark cannot be empty
        assertEquals(OptionalLong.empty(), context.client.highWatermark());
        assertThrows(IllegalArgumentException.class, () -> context.client.createSnapshot(invalidSnapshotId1.offset, invalidSnapshotId1.epoch, 0));

        // 1.2 high watermark must larger than or equal to the snapshotId's endOffset
        context.advanceLocalLeaderHighWatermarkToLogEndOffset();
        // append some more records to make the LEO > high watermark
        List<String> newRecords = Arrays.asList("d", "e", "f");
        context.client.scheduleAppend(currentEpoch, newRecords);
        context.time.sleep(context.appendLingerMs());
        context.client.poll();
        assertEquals(context.log.endOffset().offset, context.client.highWatermark().getAsLong() + newRecords.size());

        OffsetAndEpoch invalidSnapshotId2 = new OffsetAndEpoch(context.client.highWatermark().getAsLong() + 1, currentEpoch);
        assertThrows(IllegalArgumentException.class, () -> context.client.createSnapshot(invalidSnapshotId2.offset, invalidSnapshotId2.epoch, 0));

        // 2 the quorum epoch must larger than or equal to the snapshotId's epoch
        OffsetAndEpoch invalidSnapshotId3 = new OffsetAndEpoch(context.client.highWatermark().getAsLong() - 2, currentEpoch + 1);
        assertThrows(IllegalArgumentException.class, () -> context.client.createSnapshot(invalidSnapshotId3.offset, invalidSnapshotId3.epoch, 0));

        // 3 the snapshotId should be validated against endOffsetForEpoch
        OffsetAndEpoch endOffsetForEpoch = context.log.endOffsetForEpoch(epoch);
        assertEquals(epoch, endOffsetForEpoch.epoch);
        OffsetAndEpoch invalidSnapshotId4 = new OffsetAndEpoch(endOffsetForEpoch.offset + 1, epoch);
        assertThrows(IllegalArgumentException.class, () -> context.client.createSnapshot(invalidSnapshotId4.offset, invalidSnapshotId4.epoch, 0));
    }

    @Test
    public void testCreateSnapshotAsFollowerWithInvalidSnapshotId() throws Exception {
        int localId = 0;
        int leaderId = 1;
        int otherFollowerId = 2;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(localId, leaderId, otherFollowerId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
                .withElectedLeader(epoch, leaderId)
                .build();
        context.assertElectedLeader(epoch, leaderId);

        // When follower creating snapshot:
        // 1) The high watermark cannot be empty
        assertEquals(OptionalLong.empty(), context.client.highWatermark());
        OffsetAndEpoch invalidSnapshotId1 = new OffsetAndEpoch(0, 0);
        assertThrows(IllegalArgumentException.class, () -> context.client.createSnapshot(invalidSnapshotId1.offset, invalidSnapshotId1.epoch, 0));

        // Poll for our first fetch request
        context.pollUntilRequest();
        RaftRequest.Outbound fetchRequest = context.assertSentFetchRequest();
        assertTrue(voters.contains(fetchRequest.destinationId()));
        context.assertFetchRequestData(fetchRequest, epoch, 0L, 0);

        // The response does not advance the high watermark
        List<String> records1 = Arrays.asList("a", "b", "c");
        MemoryRecords batch1 = context.buildBatch(0L, 3, records1);
        context.deliverResponse(fetchRequest.correlationId, fetchRequest.destinationId(),
                context.fetchResponse(epoch, leaderId, batch1, 0L, Errors.NONE));
        context.client.poll();

        // 2) The high watermark must be larger than or equal to the snapshotId's endOffset
        int currentEpoch = context.currentEpoch();
        OffsetAndEpoch invalidSnapshotId2 = new OffsetAndEpoch(context.client.highWatermark().getAsLong() + 1, currentEpoch);
        assertThrows(IllegalArgumentException.class, () -> context.client.createSnapshot(invalidSnapshotId2.offset, invalidSnapshotId2.epoch, 0));

        // 3) The quorum epoch must be larger than or equal to the snapshotId's epoch
        OffsetAndEpoch invalidSnapshotId3 = new OffsetAndEpoch(context.client.highWatermark().getAsLong(), currentEpoch + 1);
        assertThrows(IllegalArgumentException.class, () -> context.client.createSnapshot(invalidSnapshotId3.offset, invalidSnapshotId3.epoch, 0));

        // The high watermark advances to be larger than log.endOffsetForEpoch(3), to test the case 3
        context.pollUntilRequest();
        fetchRequest = context.assertSentFetchRequest();
        assertTrue(voters.contains(fetchRequest.destinationId()));
        context.assertFetchRequestData(fetchRequest, epoch, 3L, 3);

        List<String> records2 = Arrays.asList("d", "e", "f");
        MemoryRecords batch2 = context.buildBatch(3L, 4, records2);
        context.deliverResponse(fetchRequest.correlationId, fetchRequest.destinationId(),
                context.fetchResponse(epoch, leaderId, batch2, 6L, Errors.NONE));
        context.client.poll();
        assertEquals(6L, context.client.highWatermark().getAsLong());

        // 4) The snapshotId should be validated against endOffsetForEpoch
        OffsetAndEpoch endOffsetForEpoch = context.log.endOffsetForEpoch(3);
        assertEquals(3, endOffsetForEpoch.epoch);
        OffsetAndEpoch invalidSnapshotId4 = new OffsetAndEpoch(endOffsetForEpoch.offset + 1, epoch);
        assertThrows(IllegalArgumentException.class, () -> context.client.createSnapshot(invalidSnapshotId4.offset, invalidSnapshotId4.epoch, 0));
    }

    private static FetchSnapshotRequestData fetchSnapshotRequest(
            TopicPartition topicPartition,
            int epoch,
            OffsetAndEpoch offsetAndEpoch,
            int maxBytes,
            long position
    ) {
        return fetchSnapshotRequest(null, topicPartition, epoch, offsetAndEpoch, maxBytes, position);
    }

    private static FetchSnapshotRequestData fetchSnapshotRequest(
        String clusterId,
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
            clusterId,
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
                    .setUnalignedRecords(MemoryRecords.readableRecords(buffer.slice()));
            }
        );
    }

    private static FetchResponseData snapshotFetchResponse(
        TopicPartition topicPartition,
        Uuid topicId,
        int epoch,
        int leaderId,
        OffsetAndEpoch snapshotId,
        long highWatermark
    ) {
        return RaftUtil.singletonFetchResponse(topicPartition, topicId, Errors.NONE, partitionData -> {
            partitionData.setHighWatermark(highWatermark);

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
        return RecordsSnapshotWriter.createWithHeader(
            () -> Optional.of(snapshot),
            4 * 1024,
            MemoryPool.NONE,
            context.time,
            0,
            CompressionType.NONE,
            new StringSerde()
        ).get();
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
        public void append(UnalignedMemoryRecords records) {
            if (frozen) {
                throw new RuntimeException("Snapshot is already frozen " + snapshotId);
            }
            append(records.buffer());
        }

        @Override
        public void append(MemoryRecords records) {
            if (frozen) {
                throw new RuntimeException("Snapshot is already frozen " + snapshotId);
            }
            append(records.buffer());
        }

        private void append(ByteBuffer buffer) {
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
