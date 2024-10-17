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
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.FetchSnapshotRequestData;
import org.apache.kafka.common.message.FetchSnapshotResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.UnalignedMemoryRecords;
import org.apache.kafka.common.requests.FetchSnapshotRequest;
import org.apache.kafka.raft.internals.StringSerde;
import org.apache.kafka.snapshot.RawSnapshotReader;
import org.apache.kafka.snapshot.RawSnapshotWriter;
import org.apache.kafka.snapshot.RecordsSnapshotWriter;
import org.apache.kafka.snapshot.SnapshotReader;
import org.apache.kafka.snapshot.SnapshotWriter;
import org.apache.kafka.snapshot.SnapshotWriterReaderTest;
import org.apache.kafka.snapshot.Snapshots;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public final class KafkaRaftClientSnapshotTest {
    @Test
    public void testLatestSnapshotId() throws Exception {
        int localId = randomReplicaId();
        int leaderId = localId + 1;
        Set<Integer> voters = Set.of(localId, leaderId);
        int epoch = 2;
        OffsetAndEpoch snapshotId = new OffsetAndEpoch(3, 1);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .appendToLog(snapshotId.epoch(), Arrays.asList("a", "b", "c"))
            .appendToLog(snapshotId.epoch(), Arrays.asList("d", "e", "f"))
            .withEmptySnapshot(snapshotId)
            .withElectedLeader(epoch, leaderId)
            .build();

        assertEquals(Optional.of(snapshotId), context.client.latestSnapshotId());
    }

    @Test
    public void testLatestSnapshotIdMissing() throws Exception {
        int localId = randomReplicaId();
        int leaderId = localId + 1;
        Set<Integer> voters = Set.of(localId, leaderId);
        int epoch = 2;
        OffsetAndEpoch snapshotId = new OffsetAndEpoch(3, 1);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .appendToLog(snapshotId.epoch(), Arrays.asList("a", "b", "c"))
            .appendToLog(snapshotId.epoch(), Arrays.asList("d", "e", "f"))
            .withElectedLeader(epoch, leaderId)
            .build();

        assertEquals(Optional.empty(), context.client.latestSnapshotId());
    }

    @ParameterizedTest
    @CsvSource({ "false,false", "false,true", "true,false", "true,true" })
    public void testLeaderListenerNotified(boolean entireLog, boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        ReplicaKey otherNodeKey = replicaKey(localId + 1, false);
        Set<Integer> voters = Set.of(localId, otherNodeKey.id());
        OffsetAndEpoch snapshotId = new OffsetAndEpoch(3, 1);

        RaftClientTestContext.Builder contextBuilder = new RaftClientTestContext.Builder(localId, voters)
            .withKip853Rpc(withKip853Rpc)
            .appendToLog(snapshotId.epoch(), Arrays.asList("a", "b", "c"))
            .appendToLog(snapshotId.epoch(), Arrays.asList("d", "e", "f"))
            .withEmptySnapshot(snapshotId);

        if (!entireLog) {
            contextBuilder.deleteBeforeSnapshot(snapshotId);
        }

        RaftClientTestContext context = contextBuilder.build();

        context.becomeLeader();
        int epoch = context.currentEpoch();

        // Advance the highWatermark
        long localLogEndOffset = context.log.endOffset().offset();
        context.deliverRequest(context.fetchRequest(epoch, otherNodeKey, localLogEndOffset, epoch, 0));
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(localId));
        assertEquals(localLogEndOffset, context.client.highWatermark().getAsLong());

        // Check that listener was notified of the new snapshot
        try (SnapshotReader<String> snapshot = context.listener.drainHandledSnapshot().get()) {
            assertEquals(snapshotId, snapshot.snapshotId());
            SnapshotWriterReaderTest.assertDataSnapshot(Collections.emptyList(), snapshot);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testFollowerListenerNotified(boolean entireLog) throws Exception {
        int localId = randomReplicaId();
        int leaderId = localId + 1;
        Set<Integer> voters = Set.of(localId, leaderId);
        int epoch = 2;
        OffsetAndEpoch snapshotId = new OffsetAndEpoch(3, 1);

        RaftClientTestContext.Builder contextBuilder = new RaftClientTestContext.Builder(localId, voters)
            .appendToLog(snapshotId.epoch(), Arrays.asList("a", "b", "c"))
            .appendToLog(snapshotId.epoch(), Arrays.asList("d", "e", "f"))
            .withEmptySnapshot(snapshotId)
            .withElectedLeader(epoch, leaderId);

        if (!entireLog) {
            contextBuilder.deleteBeforeSnapshot(snapshotId);
        }

        RaftClientTestContext context = contextBuilder.build();

        // Advance the highWatermark
        long localLogEndOffset = context.log.endOffset().offset();
        context.pollUntilRequest();
        RaftRequest.Outbound fetchRequest = context.assertSentFetchRequest();
        context.assertFetchRequestData(fetchRequest, epoch, localLogEndOffset, snapshotId.epoch());
        context.deliverResponse(
            fetchRequest.correlationId(),
            fetchRequest.destination(),
            context.fetchResponse(epoch, leaderId, MemoryRecords.EMPTY, localLogEndOffset, Errors.NONE)
        );

        context.pollUntilRequest();
        context.assertSentFetchRequest(epoch, localLogEndOffset, snapshotId.epoch());

        // Check that listener was notified of the new snapshot
        try (SnapshotReader<String> snapshot = context.listener.drainHandledSnapshot().get()) {
            assertEquals(snapshotId, snapshot.snapshotId());
            SnapshotWriterReaderTest.assertDataSnapshot(Collections.emptyList(), snapshot);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testSecondListenerNotified(boolean entireLog) throws Exception {
        int localId = randomReplicaId();
        int leaderId = localId + 1;
        Set<Integer> voters = Set.of(localId, leaderId);
        int epoch = 2;
        OffsetAndEpoch snapshotId = new OffsetAndEpoch(3, 1);

        RaftClientTestContext.Builder contextBuilder = new RaftClientTestContext.Builder(localId, voters)
            .appendToLog(snapshotId.epoch(), Arrays.asList("a", "b", "c"))
            .appendToLog(snapshotId.epoch(), Arrays.asList("d", "e", "f"))
            .withEmptySnapshot(snapshotId)
            .withElectedLeader(epoch, leaderId);

        if (!entireLog) {
            contextBuilder.deleteBeforeSnapshot(snapshotId);
        }

        RaftClientTestContext context = contextBuilder.build();

        // Advance the highWatermark
        long localLogEndOffset = context.log.endOffset().offset();
        context.pollUntilRequest();
        RaftRequest.Outbound fetchRequest = context.assertSentFetchRequest();
        context.assertFetchRequestData(fetchRequest, epoch, localLogEndOffset, snapshotId.epoch());
        context.deliverResponse(
            fetchRequest.correlationId(),
            fetchRequest.destination(),
            context.fetchResponse(epoch, leaderId, MemoryRecords.EMPTY, localLogEndOffset, Errors.NONE)
        );

        context.pollUntilRequest();
        context.assertSentFetchRequest(epoch, localLogEndOffset, snapshotId.epoch());

        RaftClientTestContext.MockListener secondListener = new RaftClientTestContext.MockListener(OptionalInt.of(localId));
        context.client.register(secondListener);
        context.client.poll();

        // Check that the second listener was notified of the new snapshot
        try (SnapshotReader<String> snapshot = secondListener.drainHandledSnapshot().get()) {
            assertEquals(snapshotId, snapshot.snapshotId());
            SnapshotWriterReaderTest.assertDataSnapshot(Collections.emptyList(), snapshot);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    public void testListenerRenotified(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        ReplicaKey otherNodeKey = replicaKey(localId + 1, withKip853Rpc);
        Set<Integer> voters = Set.of(localId, otherNodeKey.id());
        OffsetAndEpoch snapshotId = new OffsetAndEpoch(3, 1);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withKip853Rpc(withKip853Rpc)
            .appendToLog(snapshotId.epoch(), Arrays.asList("a", "b", "c"))
            .appendToLog(snapshotId.epoch(), Arrays.asList("d", "e", "f"))
            .appendToLog(snapshotId.epoch(), Arrays.asList("g", "h", "i"))
            .withEmptySnapshot(snapshotId)
            .deleteBeforeSnapshot(snapshotId)
            .build();

        context.becomeLeader();
        int epoch = context.currentEpoch();

        // Stop the listener from reading commit batches
        context.listener.updateReadCommit(false);

        // Advance the highWatermark
        long localLogEndOffset = context.log.endOffset().offset();
        context.deliverRequest(context.fetchRequest(epoch, otherNodeKey, localLogEndOffset, epoch, 0));
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(localId));
        assertEquals(localLogEndOffset, context.client.highWatermark().getAsLong());

        // Check that listener was notified of the new snapshot
        try (SnapshotReader<String> snapshot = context.listener.drainHandledSnapshot().get()) {
            assertEquals(snapshotId, snapshot.snapshotId());
            SnapshotWriterReaderTest.assertDataSnapshot(Collections.emptyList(), snapshot);
        }

        // Generate a new snapshot
        OffsetAndEpoch secondSnapshotId = new OffsetAndEpoch(localLogEndOffset, epoch);
        try (SnapshotWriter<String> snapshot = context.client.createSnapshot(secondSnapshotId, 0).get()) {
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
            SnapshotWriterReaderTest.assertDataSnapshot(Collections.emptyList(), snapshot);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    public void testLeaderImmediatelySendsSnapshotId(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        ReplicaKey otherNodeKey = replicaKey(localId + 1, withKip853Rpc);
        Set<Integer> voters = Set.of(localId, otherNodeKey.id());
        OffsetAndEpoch snapshotId = new OffsetAndEpoch(3, 4);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withUnknownLeader(snapshotId.epoch())
            .withKip853Rpc(withKip853Rpc)
            .appendToLog(snapshotId.epoch(), Arrays.asList("a", "b", "c"))
            .appendToLog(snapshotId.epoch(), Arrays.asList("d", "e", "f"))
            .appendToLog(snapshotId.epoch(), Arrays.asList("g", "h", "i"))
            .withEmptySnapshot(snapshotId)
            .deleteBeforeSnapshot(snapshotId)
            .build();

        context.becomeLeader();
        int epoch = context.currentEpoch();

        // Send a fetch request for an end offset and epoch which has been snapshotted
        context.deliverRequest(context.fetchRequest(epoch, otherNodeKey, 6, 2, 500));
        context.client.poll();

        // Expect that the leader replies immediately with a snapshot id
        FetchResponseData.PartitionData partitionResponse = context.assertSentFetchPartitionResponse();
        assertEquals(Errors.NONE, Errors.forCode(partitionResponse.errorCode()));
        assertEquals(epoch, partitionResponse.currentLeader().leaderEpoch());
        assertEquals(localId, partitionResponse.currentLeader().leaderId());
        assertEquals(snapshotId.epoch(), partitionResponse.snapshotId().epoch());
        assertEquals(snapshotId.offset(), partitionResponse.snapshotId().endOffset());
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    public void testFetchRequestOffsetLessThanLogStart(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        ReplicaKey otherNodeKey = replicaKey(localId + 1, withKip853Rpc);
        Set<Integer> voters = Set.of(localId, otherNodeKey.id());

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withAppendLingerMs(1)
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.becomeLeader();
        int epoch = context.currentEpoch();

        List<String> appendRecords = Arrays.asList("a", "b", "c");
        context.client.prepareAppend(epoch, appendRecords);
        context.client.schedulePreparedAppend();
        context.time.sleep(context.appendLingerMs());
        context.client.poll();

        long localLogEndOffset = context.log.endOffset().offset();
        assertTrue(
            appendRecords.size() <= localLogEndOffset,
            String.format("Record length = %s, log end offset = %s", appendRecords.size(), localLogEndOffset)
        );

        // Advance the highWatermark
        context.advanceLocalLeaderHighWatermarkToLogEndOffset();

        OffsetAndEpoch snapshotId = new OffsetAndEpoch(localLogEndOffset, epoch);
        try (SnapshotWriter<String> snapshot = context.client.createSnapshot(snapshotId, 0).get()) {
            assertEquals(snapshotId, snapshot.snapshotId());
            snapshot.freeze();
        }
        context.log.deleteBeforeSnapshot(snapshotId);
        context.client.poll();

        // Send Fetch request less than start offset
        context.deliverRequest(context.fetchRequest(epoch, otherNodeKey, snapshotId.offset() - 2, snapshotId.epoch(), 0));
        context.pollUntilResponse();
        FetchResponseData.PartitionData partitionResponse = context.assertSentFetchPartitionResponse();
        assertEquals(Errors.NONE, Errors.forCode(partitionResponse.errorCode()));
        assertEquals(epoch, partitionResponse.currentLeader().leaderEpoch());
        assertEquals(localId, partitionResponse.currentLeader().leaderId());
        assertEquals(snapshotId.epoch(), partitionResponse.snapshotId().epoch());
        assertEquals(snapshotId.offset(), partitionResponse.snapshotId().endOffset());
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    public void testFetchRequestOffsetAtZero(boolean withKip853Rpc) throws Exception {
        // When the follower sends a FETCH request at offset 0, reply with snapshot id if it exists
        int localId = randomReplicaId();
        ReplicaKey otherNodeKey = replicaKey(localId + 1, withKip853Rpc);
        Set<Integer> voters = Set.of(localId, otherNodeKey.id());

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withAppendLingerMs(1)
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.becomeLeader();
        int epoch = context.currentEpoch();

        List<String> appendRecords = Arrays.asList("a", "b", "c");
        context.client.prepareAppend(epoch, appendRecords);
        context.client.schedulePreparedAppend();
        context.time.sleep(context.appendLingerMs());
        context.client.poll();

        long localLogEndOffset = context.log.endOffset().offset();
        assertTrue(
            appendRecords.size() <= localLogEndOffset,
            String.format("Record length = %s, log end offset = %s", appendRecords.size(), localLogEndOffset)
        );

        // Advance the highWatermark
        context.advanceLocalLeaderHighWatermarkToLogEndOffset();

        // Generate a snapshot at the LEO
        OffsetAndEpoch snapshotId = new OffsetAndEpoch(localLogEndOffset, epoch);
        try (SnapshotWriter<String> snapshot = context.client.createSnapshot(snapshotId, 0).get()) {
            assertEquals(snapshotId, snapshot.snapshotId());
            snapshot.freeze();
        }

        // Send Fetch request for offset 0
        context.deliverRequest(context.fetchRequest(epoch, otherNodeKey, 0, 0, 0));
        context.pollUntilResponse();
        FetchResponseData.PartitionData partitionResponse = context.assertSentFetchPartitionResponse();
        assertEquals(Errors.NONE, Errors.forCode(partitionResponse.errorCode()));
        assertEquals(epoch, partitionResponse.currentLeader().leaderEpoch());
        assertEquals(localId, partitionResponse.currentLeader().leaderId());
        assertEquals(snapshotId.epoch(), partitionResponse.snapshotId().epoch());
        assertEquals(snapshotId.offset(), partitionResponse.snapshotId().endOffset());
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    public void testFetchRequestWithLargerLastFetchedEpoch(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        ReplicaKey otherNodeKey = replicaKey(localId + 1, withKip853Rpc);
        Set<Integer> voters = Set.of(localId, otherNodeKey.id());

        OffsetAndEpoch oldestSnapshotId = new OffsetAndEpoch(3, 2);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .appendToLog(oldestSnapshotId.epoch(), Arrays.asList("a", "b", "c"))
            .appendToLog(oldestSnapshotId.epoch(), Arrays.asList("d", "e", "f"))
            .withAppendLingerMs(1)
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.becomeLeader();
        int epoch = context.currentEpoch();
        assertEquals(oldestSnapshotId.epoch() + 1, epoch);

        // Advance the highWatermark
        context.advanceLocalLeaderHighWatermarkToLogEndOffset();

        // Create a snapshot at the high watermark
        try (SnapshotWriter<String> snapshot = context.client.createSnapshot(oldestSnapshotId, 0).get()) {
            assertEquals(oldestSnapshotId, snapshot.snapshotId());
            snapshot.freeze();
        }
        context.client.poll();

        context.client.prepareAppend(epoch, Arrays.asList("g", "h", "i"));
        context.client.schedulePreparedAppend();
        context.time.sleep(context.appendLingerMs());
        context.client.poll();

        // It is an invalid request to send an last fetched epoch greater than the current epoch
        context.deliverRequest(context.fetchRequest(epoch, otherNodeKey, oldestSnapshotId.offset() + 1, epoch + 1, 0));
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.INVALID_REQUEST, epoch, OptionalInt.of(localId));
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    public void testFetchRequestTruncateToLogStart(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        ReplicaKey otherNodeKey = replicaKey(localId + 1, withKip853Rpc);
        int syncNodeId = otherNodeKey.id() + 1;
        Set<Integer> voters = Set.of(localId, otherNodeKey.id(), syncNodeId);

        OffsetAndEpoch oldestSnapshotId = new OffsetAndEpoch(3, 2);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .appendToLog(oldestSnapshotId.epoch(), Arrays.asList("a", "b", "c"))
            .appendToLog(oldestSnapshotId.epoch() + 2, Arrays.asList("d", "e", "f"))
            .withAppendLingerMs(1)
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.becomeLeader();
        int epoch = context.currentEpoch();
        assertEquals(oldestSnapshotId.epoch() + 2 + 1, epoch);

        // Advance the highWatermark
        context.advanceLocalLeaderHighWatermarkToLogEndOffset();

        // Create a snapshot at the high watermark
        try (SnapshotWriter<String> snapshot = context.client.createSnapshot(oldestSnapshotId, 0).get()) {
            assertEquals(oldestSnapshotId, snapshot.snapshotId());
            snapshot.freeze();
        }
        context.client.poll();

        // This should truncate to the old snapshot
        context.deliverRequest(
            context.fetchRequest(
                epoch,
                otherNodeKey,
                oldestSnapshotId.offset() + 1,
                oldestSnapshotId.epoch() + 1,
                0
            )
        );
        context.pollUntilResponse();
        FetchResponseData.PartitionData partitionResponse = context.assertSentFetchPartitionResponse();
        assertEquals(Errors.NONE, Errors.forCode(partitionResponse.errorCode()));
        assertEquals(epoch, partitionResponse.currentLeader().leaderEpoch());
        assertEquals(localId, partitionResponse.currentLeader().leaderId());
        assertEquals(oldestSnapshotId.epoch(), partitionResponse.divergingEpoch().epoch());
        assertEquals(oldestSnapshotId.offset(), partitionResponse.divergingEpoch().endOffset());
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    public void testFetchRequestAtLogStartOffsetWithValidEpoch(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        ReplicaKey otherNodeKey = replicaKey(localId + 1, withKip853Rpc);
        int syncNodeId = otherNodeKey.id() + 1;
        Set<Integer> voters = Set.of(localId, otherNodeKey.id(), syncNodeId);

        OffsetAndEpoch oldestSnapshotId = new OffsetAndEpoch(3, 2);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .appendToLog(oldestSnapshotId.epoch(), Arrays.asList("a", "b", "c"))
            .appendToLog(oldestSnapshotId.epoch(), Arrays.asList("d", "e", "f"))
            .appendToLog(oldestSnapshotId.epoch() + 2, Arrays.asList("g", "h", "i"))
            .withAppendLingerMs(1)
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.becomeLeader();
        int epoch = context.currentEpoch();
        assertEquals(oldestSnapshotId.epoch() + 2 + 1, epoch);

        // Advance the highWatermark
        context.advanceLocalLeaderHighWatermarkToLogEndOffset();

        // Create a snapshot at the high watermark
        try (SnapshotWriter<String> snapshot = context.client.createSnapshot(oldestSnapshotId, 0).get()) {
            assertEquals(oldestSnapshotId, snapshot.snapshotId());
            snapshot.freeze();
        }
        context.client.poll();

        // Send fetch request at log start offset with valid last fetched epoch
        context.deliverRequest(
            context.fetchRequest(
                epoch,
                otherNodeKey,
                oldestSnapshotId.offset(),
                oldestSnapshotId.epoch(),
                0
            )
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(localId));
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    public void testFetchRequestAtLogStartOffsetWithInvalidEpoch(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        ReplicaKey otherNodeKey = replicaKey(localId + 1, withKip853Rpc);
        int syncNodeId = otherNodeKey.id() + 1;
        Set<Integer> voters = Set.of(localId, otherNodeKey.id(), syncNodeId);

        OffsetAndEpoch oldestSnapshotId = new OffsetAndEpoch(3, 2);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .appendToLog(oldestSnapshotId.epoch(), Arrays.asList("a", "b", "c"))
            .appendToLog(oldestSnapshotId.epoch(), Arrays.asList("d", "e", "f"))
            .appendToLog(oldestSnapshotId.epoch() + 2, Arrays.asList("g", "h", "i"))
            .withAppendLingerMs(1)
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.becomeLeader();
        int epoch = context.currentEpoch();
        assertEquals(oldestSnapshotId.epoch() + 2 + 1, epoch);

        // Advance the highWatermark
        context.advanceLocalLeaderHighWatermarkToLogEndOffset();

        // Create a snapshot at the high watermark
        try (SnapshotWriter<String> snapshot = context.client.createSnapshot(oldestSnapshotId, 0).get()) {
            assertEquals(oldestSnapshotId, snapshot.snapshotId());
            snapshot.freeze();
        }
        context.log.deleteBeforeSnapshot(oldestSnapshotId);
        context.client.poll();

        // Send fetch with log start offset and invalid last fetched epoch
        context.deliverRequest(
            context.fetchRequest(
                epoch,
                otherNodeKey,
                oldestSnapshotId.offset(),
                oldestSnapshotId.epoch() + 1,
                0
            )
        );
        context.pollUntilResponse();
        FetchResponseData.PartitionData partitionResponse = context.assertSentFetchPartitionResponse();
        assertEquals(Errors.NONE, Errors.forCode(partitionResponse.errorCode()));
        assertEquals(epoch, partitionResponse.currentLeader().leaderEpoch());
        assertEquals(localId, partitionResponse.currentLeader().leaderId());
        assertEquals(oldestSnapshotId.epoch(), partitionResponse.snapshotId().epoch());
        assertEquals(oldestSnapshotId.offset(), partitionResponse.snapshotId().endOffset());
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    public void testFetchRequestWithLastFetchedEpochLessThanOldestSnapshot(
        boolean withKip853Rpc
    ) throws Exception {
        int localId = randomReplicaId();
        ReplicaKey otherNodeKey = replicaKey(localId + 1, withKip853Rpc);
        int syncNodeId = otherNodeKey.id() + 1;
        Set<Integer> voters = Set.of(localId, otherNodeKey.id(), syncNodeId);

        OffsetAndEpoch oldestSnapshotId = new OffsetAndEpoch(3, 2);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .appendToLog(oldestSnapshotId.epoch(), Arrays.asList("a", "b", "c"))
            .appendToLog(oldestSnapshotId.epoch(), Arrays.asList("d", "e", "f"))
            .appendToLog(oldestSnapshotId.epoch() + 2, Arrays.asList("g", "h", "i"))
            .withAppendLingerMs(1)
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.becomeLeader();
        int epoch = context.currentEpoch();
        assertEquals(oldestSnapshotId.epoch() + 2 + 1, epoch);

        // Advance the highWatermark
        context.advanceLocalLeaderHighWatermarkToLogEndOffset();

        // Create a snapshot at the high watermark
        try (SnapshotWriter<String> snapshot = context.client.createSnapshot(oldestSnapshotId, 0).get()) {
            assertEquals(oldestSnapshotId, snapshot.snapshotId());
            snapshot.freeze();
        }
        context.client.poll();

        // Send a epoch less than the oldest snapshot
        context.deliverRequest(
            context.fetchRequest(
                epoch,
                otherNodeKey,
                context.log.endOffset().offset(),
                oldestSnapshotId.epoch() - 1,
                0
            )
        );
        context.pollUntilResponse();
        FetchResponseData.PartitionData partitionResponse = context.assertSentFetchPartitionResponse();
        assertEquals(Errors.NONE, Errors.forCode(partitionResponse.errorCode()));
        assertEquals(epoch, partitionResponse.currentLeader().leaderEpoch());
        assertEquals(localId, partitionResponse.currentLeader().leaderId());
        assertEquals(oldestSnapshotId.epoch(), partitionResponse.snapshotId().epoch());
        assertEquals(oldestSnapshotId.offset(), partitionResponse.snapshotId().endOffset());
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    public void testFetchSnapshotRequestMissingSnapshot(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        Set<Integer> voters = Set.of(localId, localId + 1);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withUnknownLeader(3)
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.becomeLeader();
        int epoch = context.currentEpoch();

        context.deliverRequest(
            fetchSnapshotRequest(
                context.metadataPartition,
                epoch,
                Snapshots.BOOTSTRAP_SNAPSHOT_ID,
                Integer.MAX_VALUE,
                0
            )
        );

        context.client.poll();

        FetchSnapshotResponseData.PartitionSnapshot response = context.assertSentFetchSnapshotResponse(context.metadataPartition).get();
        assertEquals(Errors.SNAPSHOT_NOT_FOUND, Errors.forCode(response.errorCode()));
    }

    @Test
    public void testFetchSnapshotRequestBootstrapSnapshot() throws Exception {
        ReplicaKey localKey = replicaKey(0, true);
        VoterSet voters = VoterSetTest.voterSet(
            Stream.of(localKey, replicaKey(localKey.id() + 1, true))
        );

        RaftClientTestContext context = new RaftClientTestContext
            .Builder(localKey.id(), localKey.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .withUnknownLeader(3)
            .build();

        context.becomeLeader();
        int epoch = context.currentEpoch();

        context.deliverRequest(
            fetchSnapshotRequest(
                context.metadataPartition,
                epoch,
                Snapshots.BOOTSTRAP_SNAPSHOT_ID,
                Integer.MAX_VALUE,
                0
            )
        );

        context.client.poll();

        FetchSnapshotResponseData.PartitionSnapshot response = context.assertSentFetchSnapshotResponse(context.metadataPartition).get();
        assertEquals(Errors.SNAPSHOT_NOT_FOUND, Errors.forCode(response.errorCode()));
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    public void testFetchSnapshotRequestUnknownPartition(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        Set<Integer> voters = Set.of(localId, localId + 1);
        TopicPartition topicPartition = new TopicPartition("unknown", 0);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withUnknownLeader(3)
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.becomeLeader();
        int epoch = context.currentEpoch();

        context.deliverRequest(
            fetchSnapshotRequest(
                topicPartition,
                epoch,
                Snapshots.BOOTSTRAP_SNAPSHOT_ID,
                Integer.MAX_VALUE,
                0
            )
        );

        context.client.poll();

        FetchSnapshotResponseData.PartitionSnapshot response = context.assertSentFetchSnapshotResponse(topicPartition).get();
        assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION, Errors.forCode(response.errorCode()));
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    public void testFetchSnapshotRequestAsLeader(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        Set<Integer> voters = Set.of(localId, localId + 1);
        OffsetAndEpoch snapshotId = new OffsetAndEpoch(1, 1);
        List<String> records = Arrays.asList("foo", "bar");

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .appendToLog(snapshotId.epoch(), Collections.singletonList("a"))
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.becomeLeader();
        int epoch = context.currentEpoch();

        context.advanceLocalLeaderHighWatermarkToLogEndOffset();

        try (SnapshotWriter<String> snapshot = context.client.createSnapshot(snapshotId, 0).get()) {
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

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    public void testLeaderShouldResignLeadershipIfNotGetFetchSnapshotRequestFromMajorityVoters(
        boolean withKip853Rpc
    ) throws Exception {
        int localId = randomReplicaId();
        ReplicaKey voter1 = replicaKey(localId + 1, withKip853Rpc);
        ReplicaKey voter2 = replicaKey(localId + 2, withKip853Rpc);
        ReplicaKey observer3 = replicaKey(localId + 3, withKip853Rpc);
        Set<Integer> voters = Set.of(localId, voter1.id(), voter2.id());
        OffsetAndEpoch snapshotId = new OffsetAndEpoch(1, 1);
        List<String> records = Arrays.asList("foo", "bar");

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
                .appendToLog(snapshotId.epoch(), Collections.singletonList("a"))
                .withKip853Rpc(withKip853Rpc)
                .build();

        int resignLeadershipTimeout = context.checkQuorumTimeoutMs;
        context.becomeLeader();
        int epoch = context.currentEpoch();

        FetchSnapshotRequestData voter1FetchSnapshotRequest = fetchSnapshotRequest(
                context.clusterId.toString(),
                voter1,
                context.metadataPartition,
                epoch,
                snapshotId,
                Integer.MAX_VALUE,
                0
        );

        FetchSnapshotRequestData voter2FetchSnapshotRequest = fetchSnapshotRequest(
                context.clusterId.toString(),
                voter2,
                context.metadataPartition,
                epoch,
                snapshotId,
                Integer.MAX_VALUE,
                0
        );

        FetchSnapshotRequestData observerFetchSnapshotRequest = fetchSnapshotRequest(
                context.clusterId.toString(),
                observer3,
                context.metadataPartition,
                epoch,
                snapshotId,
                Integer.MAX_VALUE,
                0
        );

        context.advanceLocalLeaderHighWatermarkToLogEndOffset();
        try (SnapshotWriter<String> snapshot = context.client.createSnapshot(snapshotId, 0).get()) {
            assertEquals(snapshotId, snapshot.snapshotId());
            snapshot.append(records);
            snapshot.freeze();
        }

        // fetch timeout is not expired, the leader should not get resigned
        context.time.sleep(resignLeadershipTimeout / 2);
        context.client.poll();
        assertFalse(context.client.quorum().isResigned());

        // voter1 sends fetchSnapshotRequest, the fetch timer should be reset
        context.deliverRequest(voter1FetchSnapshotRequest);
        context.client.poll();
        context.assertSentFetchSnapshotResponse(context.metadataPartition);

        // Since the fetch timer is reset, the leader should not get resigned
        context.time.sleep(resignLeadershipTimeout / 2);
        context.client.poll();
        assertFalse(context.client.quorum().isResigned());

        // voter2 sends fetchSnapshotRequest, the fetch timer should be reset
        context.deliverRequest(voter2FetchSnapshotRequest);
        context.client.poll();
        context.assertSentFetchSnapshotResponse(context.metadataPartition);

        // Since the fetch timer is reset, the leader should not get resigned
        context.time.sleep(resignLeadershipTimeout / 2);
        context.client.poll();
        assertFalse(context.client.quorum().isResigned());

        // An observer sends fetchSnapshotRequest, but the fetch timer should not be reset.
        context.deliverRequest(observerFetchSnapshotRequest);
        context.client.poll();
        context.assertSentFetchSnapshotResponse(context.metadataPartition);

        // After this sleep, the fetch timeout should expire since we don't receive fetch request from the majority voters within fetchTimeoutMs
        context.time.sleep(resignLeadershipTimeout / 2);
        context.client.poll();
        assertTrue(context.client.quorum().isResigned());
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    public void testPartialFetchSnapshotRequestAsLeader(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        Set<Integer> voters = Set.of(localId, localId + 1);
        OffsetAndEpoch snapshotId = new OffsetAndEpoch(2, 1);
        List<String> records = Arrays.asList("foo", "bar");

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .appendToLog(snapshotId.epoch(), records)
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.becomeLeader();
        int epoch = context.currentEpoch();

        context.advanceLocalLeaderHighWatermarkToLogEndOffset();

        try (SnapshotWriter<String> snapshot = context.client.createSnapshot(snapshotId, 0).get()) {
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

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    public void testFetchSnapshotRequestAsFollower(boolean withKip853Rpc) throws IOException {
        int localId = randomReplicaId();
        int leaderId = localId + 1;
        Set<Integer> voters = Set.of(localId, leaderId);
        int epoch = 2;
        OffsetAndEpoch snapshotId = Snapshots.BOOTSTRAP_SNAPSHOT_ID;

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withElectedLeader(epoch, leaderId)
            .withKip853Rpc(withKip853Rpc)
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

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    public void testFetchSnapshotRequestWithInvalidPosition(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        Set<Integer> voters = Set.of(localId, localId + 1);
        OffsetAndEpoch snapshotId = new OffsetAndEpoch(1, 1);
        List<String> records = Arrays.asList("foo", "bar");

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .appendToLog(snapshotId.epoch(), Collections.singletonList("a"))
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.becomeLeader();
        int epoch = context.currentEpoch();

        context.advanceLocalLeaderHighWatermarkToLogEndOffset();

        try (SnapshotWriter<String> snapshot = context.client.createSnapshot(snapshotId, 0).get()) {
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

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    public void testFetchSnapshotRequestWithOlderEpoch(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        Set<Integer> voters = Set.of(localId, localId + 1);
        OffsetAndEpoch snapshotId = Snapshots.BOOTSTRAP_SNAPSHOT_ID;

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withUnknownLeader(1)
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.becomeLeader();
        int epoch = context.currentEpoch();

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

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    public void testFetchSnapshotRequestWithNewerEpoch(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        Set<Integer> voters = Set.of(localId, localId + 1);
        OffsetAndEpoch snapshotId = Snapshots.BOOTSTRAP_SNAPSHOT_ID;

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withUnknownLeader(1)
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.becomeLeader();
        int epoch = context.currentEpoch();

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

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testFetchResponseWithInvalidSnapshotId(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        int leaderId = localId + 1;
        Set<Integer> voters = Set.of(localId, leaderId);
        int epoch = 2;
        OffsetAndEpoch invalidEpoch = new OffsetAndEpoch(100L, -1);
        OffsetAndEpoch invalidEndOffset = new OffsetAndEpoch(-1L, 1);
        int slept = 0;

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withElectedLeader(epoch, leaderId)
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.pollUntilRequest();
        RaftRequest.Outbound fetchRequest = context.assertSentFetchRequest();
        context.assertFetchRequestData(fetchRequest, epoch, 0L, 0);

        context.deliverResponse(
            fetchRequest.correlationId(),
            fetchRequest.destination(),
            context.snapshotFetchResponse(epoch, leaderId, invalidEpoch, 200L)
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
            fetchRequest.correlationId(),
            fetchRequest.destination(),
            context.snapshotFetchResponse(epoch, leaderId, invalidEndOffset, 200L)
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

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testFetchResponseWithSnapshotId(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        int leaderId = localId + 1;
        Set<Integer> voters = Set.of(localId, leaderId);
        int epoch = 2;
        OffsetAndEpoch snapshotId = new OffsetAndEpoch(100L, 1);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withElectedLeader(epoch, leaderId)
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.pollUntilRequest();
        RaftRequest.Outbound fetchRequest = context.assertSentFetchRequest();
        context.assertFetchRequestData(fetchRequest, epoch, 0L, 0);

        context.deliverResponse(
            fetchRequest.correlationId(),
            fetchRequest.destination(),
            context.snapshotFetchResponse(epoch, leaderId, snapshotId, 200L)
        );

        context.pollUntilRequest();
        RaftRequest.Outbound snapshotRequest = context.assertSentFetchSnapshotRequest();
        FetchSnapshotRequestData.PartitionSnapshot request = assertFetchSnapshotRequest(
            snapshotRequest,
            context.metadataPartition,
            localId,
            KafkaRaftClient.MAX_FETCH_SIZE_BYTES
        ).get();
        assertEquals(snapshotId.offset(), request.snapshotId().endOffset());
        assertEquals(snapshotId.epoch(), request.snapshotId().epoch());
        assertEquals(0, request.position());

        List<String> records = Arrays.asList("foo", "bar");
        MemorySnapshotWriter memorySnapshot = new MemorySnapshotWriter(snapshotId);
        try (SnapshotWriter<String> snapshotWriter = snapshotWriter(context, memorySnapshot)) {
            snapshotWriter.append(records);
            snapshotWriter.freeze();
        }

        context.deliverResponse(
            snapshotRequest.correlationId(),
            snapshotRequest.destination(),
            fetchSnapshotResponse(
                context,
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
        context.assertFetchRequestData(fetchRequest, epoch, snapshotId.offset(), snapshotId.epoch());

        // Check that the snapshot was written to the log
        RawSnapshotReader snapshot = context.log.readSnapshot(snapshotId).get();
        assertEquals(memorySnapshot.buffer().remaining(), snapshot.sizeInBytes());
        SnapshotWriterReaderTest.assertDataSnapshot(Collections.singletonList(records), snapshot);

        // Check that listener was notified of the new snapshot
        try (SnapshotReader<String> reader = context.listener.drainHandledSnapshot().get()) {
            assertEquals(snapshotId, reader.snapshotId());
            SnapshotWriterReaderTest.assertDataSnapshot(Collections.singletonList(records), reader);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testFetchSnapshotResponsePartialData(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        int leaderId = localId + 1;
        Set<Integer> voters = Set.of(localId, leaderId);
        int epoch = 2;
        OffsetAndEpoch snapshotId = new OffsetAndEpoch(100L, 1);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withElectedLeader(epoch, leaderId)
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.pollUntilRequest();
        RaftRequest.Outbound fetchRequest = context.assertSentFetchRequest();
        context.assertFetchRequestData(fetchRequest, epoch, 0L, 0);

        context.deliverResponse(
            fetchRequest.correlationId(),
            fetchRequest.destination(),
            context.snapshotFetchResponse(epoch, leaderId, snapshotId, 200L)
        );

        context.pollUntilRequest();
        RaftRequest.Outbound snapshotRequest = context.assertSentFetchSnapshotRequest();
        FetchSnapshotRequestData.PartitionSnapshot request = assertFetchSnapshotRequest(
            snapshotRequest,
            context.metadataPartition,
            localId,
            KafkaRaftClient.MAX_FETCH_SIZE_BYTES
        ).get();
        assertEquals(snapshotId.offset(), request.snapshotId().endOffset());
        assertEquals(snapshotId.epoch(), request.snapshotId().epoch());
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
            snapshotRequest.correlationId(),
            snapshotRequest.destination(),
            fetchSnapshotResponse(
                context,
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
            KafkaRaftClient.MAX_FETCH_SIZE_BYTES
        ).get();
        assertEquals(snapshotId.offset(), request.snapshotId().endOffset());
        assertEquals(snapshotId.epoch(), request.snapshotId().epoch());
        assertEquals(sendingBuffer.limit(), request.position());

        sendingBuffer = memorySnapshot.buffer().slice();
        sendingBuffer.position(Math.toIntExact(request.position()));

        context.deliverResponse(
            snapshotRequest.correlationId(),
            snapshotRequest.destination(),
            fetchSnapshotResponse(
                context,
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
        context.assertFetchRequestData(fetchRequest, epoch, snapshotId.offset(), snapshotId.epoch());

        // Check that the snapshot was written to the log
        RawSnapshotReader snapshot = context.log.readSnapshot(snapshotId).get();
        assertEquals(memorySnapshot.buffer().remaining(), snapshot.sizeInBytes());
        SnapshotWriterReaderTest.assertDataSnapshot(Collections.singletonList(records), snapshot);

        // Check that listener was notified of the new snapshot
        try (SnapshotReader<String> reader = context.listener.drainHandledSnapshot().get()) {
            assertEquals(snapshotId, reader.snapshotId());
            SnapshotWriterReaderTest.assertDataSnapshot(Collections.singletonList(records), reader);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testFetchSnapshotResponseMissingSnapshot(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        int leaderId = localId + 1;
        Set<Integer> voters = Set.of(localId, leaderId);
        int epoch = 2;
        OffsetAndEpoch snapshotId = new OffsetAndEpoch(100L, 1);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withElectedLeader(epoch, leaderId)
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.pollUntilRequest();
        RaftRequest.Outbound fetchRequest = context.assertSentFetchRequest();
        context.assertFetchRequestData(fetchRequest, epoch, 0L, 0);

        context.deliverResponse(
            fetchRequest.correlationId(),
            fetchRequest.destination(),
            context.snapshotFetchResponse(epoch, leaderId, snapshotId, 200L)
        );

        context.pollUntilRequest();
        RaftRequest.Outbound snapshotRequest = context.assertSentFetchSnapshotRequest();
        FetchSnapshotRequestData.PartitionSnapshot request = assertFetchSnapshotRequest(
            snapshotRequest,
            context.metadataPartition,
            localId,
            KafkaRaftClient.MAX_FETCH_SIZE_BYTES
        ).get();
        assertEquals(snapshotId.offset(), request.snapshotId().endOffset());
        assertEquals(snapshotId.epoch(), request.snapshotId().epoch());
        assertEquals(0, request.position());

        // Reply with a snapshot not found error
        context.deliverResponse(
            snapshotRequest.correlationId(),
            snapshotRequest.destination(),
            context.fetchSnapshotResponse(
                leaderId,
                responsePartitionSnapshot -> {
                    responsePartitionSnapshot
                        .currentLeader()
                        .setLeaderEpoch(epoch)
                        .setLeaderId(leaderId);

                    responsePartitionSnapshot.setUnalignedRecords(UnalignedMemoryRecords.empty());

                    return responsePartitionSnapshot
                        .setErrorCode(Errors.SNAPSHOT_NOT_FOUND.code());
                }
            )
        );

        context.pollUntilRequest();
        fetchRequest = context.assertSentFetchRequest();
        context.assertFetchRequestData(fetchRequest, epoch, 0L, 0);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testFetchSnapshotResponseFromNewerEpochNotLeader(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        int firstLeaderId = localId + 1;
        int secondLeaderId = firstLeaderId + 1;
        Set<Integer> voters = Set.of(localId, firstLeaderId, secondLeaderId);
        int epoch = 2;
        OffsetAndEpoch snapshotId = new OffsetAndEpoch(100L, 1);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withElectedLeader(epoch, firstLeaderId)
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.pollUntilRequest();
        RaftRequest.Outbound fetchRequest = context.assertSentFetchRequest();
        context.assertFetchRequestData(fetchRequest, epoch, 0L, 0);

        context.deliverResponse(
            fetchRequest.correlationId(),
            fetchRequest.destination(),
            context.snapshotFetchResponse(epoch, firstLeaderId, snapshotId, 200L)
        );

        context.pollUntilRequest();
        RaftRequest.Outbound snapshotRequest = context.assertSentFetchSnapshotRequest();
        FetchSnapshotRequestData.PartitionSnapshot request = assertFetchSnapshotRequest(
            snapshotRequest,
            context.metadataPartition,
            localId,
            KafkaRaftClient.MAX_FETCH_SIZE_BYTES
        ).get();
        assertEquals(snapshotId.offset(), request.snapshotId().endOffset());
        assertEquals(snapshotId.epoch(), request.snapshotId().epoch());
        assertEquals(0, request.position());

        // Reply with new leader response
        context.deliverResponse(
            snapshotRequest.correlationId(),
            snapshotRequest.destination(),
            context.fetchSnapshotResponse(
                secondLeaderId,
                responsePartitionSnapshot -> {
                    responsePartitionSnapshot
                        .currentLeader()
                        .setLeaderEpoch(epoch + 1)
                        .setLeaderId(secondLeaderId);

                    responsePartitionSnapshot.setUnalignedRecords(UnalignedMemoryRecords.empty());

                    return responsePartitionSnapshot
                        .setErrorCode(Errors.FENCED_LEADER_EPOCH.code());
                }
            )
        );

        context.pollUntilRequest();
        fetchRequest = context.assertSentFetchRequest();
        context.assertFetchRequestData(fetchRequest, epoch + 1, 0L, 0);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testFetchSnapshotResponseFromNewerEpochLeader(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        int leaderId = localId + 1;
        Set<Integer> voters = Set.of(localId, leaderId);
        int epoch = 2;
        OffsetAndEpoch snapshotId = new OffsetAndEpoch(100L, 1);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withElectedLeader(epoch, leaderId)
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.pollUntilRequest();
        RaftRequest.Outbound fetchRequest = context.assertSentFetchRequest();
        context.assertFetchRequestData(fetchRequest, epoch, 0L, 0);

        context.deliverResponse(
            fetchRequest.correlationId(),
            fetchRequest.destination(),
            context.snapshotFetchResponse(epoch, leaderId, snapshotId, 200L)
        );

        context.pollUntilRequest();
        RaftRequest.Outbound snapshotRequest = context.assertSentFetchSnapshotRequest();
        FetchSnapshotRequestData.PartitionSnapshot request = assertFetchSnapshotRequest(
            snapshotRequest,
            context.metadataPartition,
            localId,
            KafkaRaftClient.MAX_FETCH_SIZE_BYTES
        ).get();
        assertEquals(snapshotId.offset(), request.snapshotId().endOffset());
        assertEquals(snapshotId.epoch(), request.snapshotId().epoch());
        assertEquals(0, request.position());

        // Reply with new leader epoch
        context.deliverResponse(
            snapshotRequest.correlationId(),
            snapshotRequest.destination(),
            context.fetchSnapshotResponse(
                leaderId,
                responsePartitionSnapshot -> {
                    responsePartitionSnapshot
                        .currentLeader()
                        .setLeaderEpoch(epoch + 1)
                        .setLeaderId(leaderId);

                    responsePartitionSnapshot.setUnalignedRecords(UnalignedMemoryRecords.empty());

                    return responsePartitionSnapshot
                        .setErrorCode(Errors.FENCED_LEADER_EPOCH.code());
                }
            )
        );

        context.pollUntilRequest();
        fetchRequest = context.assertSentFetchRequest();
        context.assertFetchRequestData(fetchRequest, epoch + 1, 0L, 0);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testFetchSnapshotResponseFromOlderEpoch(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        int leaderId = localId + 1;
        Set<Integer> voters = Set.of(localId, leaderId);
        int epoch = 2;
        OffsetAndEpoch snapshotId = new OffsetAndEpoch(100L, 1);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withElectedLeader(epoch, leaderId)
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.pollUntilRequest();
        RaftRequest.Outbound fetchRequest = context.assertSentFetchRequest();
        context.assertFetchRequestData(fetchRequest, epoch, 0L, 0);

        context.deliverResponse(
            fetchRequest.correlationId(),
            fetchRequest.destination(),
            context.snapshotFetchResponse(epoch, leaderId, snapshotId, 200L)
        );

        context.pollUntilRequest();
        RaftRequest.Outbound snapshotRequest = context.assertSentFetchSnapshotRequest();
        FetchSnapshotRequestData.PartitionSnapshot request = assertFetchSnapshotRequest(
            snapshotRequest,
            context.metadataPartition,
            localId,
            KafkaRaftClient.MAX_FETCH_SIZE_BYTES
        ).get();
        assertEquals(snapshotId.offset(), request.snapshotId().endOffset());
        assertEquals(snapshotId.epoch(), request.snapshotId().epoch());
        assertEquals(0, request.position());

        // Reply with unknown leader epoch
        context.deliverResponse(
            snapshotRequest.correlationId(),
            snapshotRequest.destination(),
            context.fetchSnapshotResponse(
                leaderId + 1,
                responsePartitionSnapshot -> {
                    responsePartitionSnapshot
                        .currentLeader()
                        .setLeaderEpoch(epoch - 1)
                        .setLeaderId(leaderId + 1);

                    responsePartitionSnapshot.setUnalignedRecords(UnalignedMemoryRecords.empty());

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
            KafkaRaftClient.MAX_FETCH_SIZE_BYTES
        ).get();
        assertEquals(snapshotId.offset(), request.snapshotId().endOffset());
        assertEquals(snapshotId.epoch(), request.snapshotId().epoch());
        assertEquals(0, request.position());
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testFetchSnapshotResponseWithInvalidId(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        int leaderId = localId + 1;
        Set<Integer> voters = Set.of(localId, leaderId);
        int epoch = 2;
        OffsetAndEpoch snapshotId = new OffsetAndEpoch(100L, 1);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withElectedLeader(epoch, leaderId)
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.pollUntilRequest();
        RaftRequest.Outbound fetchRequest = context.assertSentFetchRequest();
        context.assertFetchRequestData(fetchRequest, epoch, 0L, 0);

        context.deliverResponse(
            fetchRequest.correlationId(),
            fetchRequest.destination(),
            context.snapshotFetchResponse(epoch, leaderId, snapshotId, 200L)
        );

        context.pollUntilRequest();
        RaftRequest.Outbound snapshotRequest = context.assertSentFetchSnapshotRequest();
        FetchSnapshotRequestData.PartitionSnapshot request = assertFetchSnapshotRequest(
            snapshotRequest,
            context.metadataPartition,
            localId,
            KafkaRaftClient.MAX_FETCH_SIZE_BYTES
        ).get();
        assertEquals(snapshotId.offset(), request.snapshotId().endOffset());
        assertEquals(snapshotId.epoch(), request.snapshotId().epoch());
        assertEquals(0, request.position());

        // Reply with an invalid snapshot id endOffset
        context.deliverResponse(
            snapshotRequest.correlationId(),
            snapshotRequest.destination(),
            context.fetchSnapshotResponse(
                leaderId,
                responsePartitionSnapshot -> {
                    responsePartitionSnapshot
                        .currentLeader()
                        .setLeaderEpoch(epoch)
                        .setLeaderId(leaderId);

                    responsePartitionSnapshot
                        .snapshotId()
                        .setEndOffset(-1)
                        .setEpoch(snapshotId.epoch());

                    responsePartitionSnapshot.setUnalignedRecords(UnalignedMemoryRecords.empty());

                    return responsePartitionSnapshot;
                }
            )
        );

        context.pollUntilRequest();

        // Follower should send a fetch request
        fetchRequest = context.assertSentFetchRequest();
        context.assertFetchRequestData(fetchRequest, epoch, 0L, 0);

        context.deliverResponse(
            fetchRequest.correlationId(),
            fetchRequest.destination(),
            context.snapshotFetchResponse(epoch, leaderId, snapshotId, 200L)
        );

        context.pollUntilRequest();

        snapshotRequest = context.assertSentFetchSnapshotRequest();
        request = assertFetchSnapshotRequest(
            snapshotRequest,
            context.metadataPartition,
            localId,
            KafkaRaftClient.MAX_FETCH_SIZE_BYTES
        ).get();
        assertEquals(snapshotId.offset(), request.snapshotId().endOffset());
        assertEquals(snapshotId.epoch(), request.snapshotId().epoch());
        assertEquals(0, request.position());

        // Reply with an invalid snapshot id epoch
        context.deliverResponse(
            snapshotRequest.correlationId(),
            snapshotRequest.destination(),
            context.fetchSnapshotResponse(
                leaderId,
                responsePartitionSnapshot -> {
                    responsePartitionSnapshot
                        .currentLeader()
                        .setLeaderEpoch(epoch)
                        .setLeaderId(leaderId);

                    responsePartitionSnapshot
                        .snapshotId()
                        .setEndOffset(snapshotId.offset())
                        .setEpoch(-1);

                    responsePartitionSnapshot.setUnalignedRecords(UnalignedMemoryRecords.empty());

                    return responsePartitionSnapshot;
                }
            )
        );

        context.pollUntilRequest();

        // Follower should send a fetch request
        fetchRequest = context.assertSentFetchRequest();
        context.assertFetchRequestData(fetchRequest, epoch, 0L, 0);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testFetchSnapshotResponseToNotFollower(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        int leaderId = localId + 1;
        Set<Integer> voters = Set.of(localId, leaderId);
        int epoch = 2;
        OffsetAndEpoch snapshotId = new OffsetAndEpoch(100L, 1);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withElectedLeader(epoch, leaderId)
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.pollUntilRequest();
        RaftRequest.Outbound fetchRequest = context.assertSentFetchRequest();
        context.assertFetchRequestData(fetchRequest, epoch, 0L, 0);

        context.deliverResponse(
            fetchRequest.correlationId(),
            fetchRequest.destination(),
            context.snapshotFetchResponse(epoch, leaderId, snapshotId, 200L)
        );

        context.pollUntilRequest();

        RaftRequest.Outbound snapshotRequest = context.assertSentFetchSnapshotRequest();
        FetchSnapshotRequestData.PartitionSnapshot request = assertFetchSnapshotRequest(
            snapshotRequest,
            context.metadataPartition,
            localId,
            KafkaRaftClient.MAX_FETCH_SIZE_BYTES
        ).get();
        assertEquals(snapshotId.offset(), request.snapshotId().endOffset());
        assertEquals(snapshotId.epoch(), request.snapshotId().epoch());
        assertEquals(0, request.position());

        // Sleeping for fetch timeout should transition to candidate
        context.time.sleep(context.fetchTimeoutMs);

        context.pollUntilRequest();

        context.assertSentVoteRequest(epoch + 1, 0, 0L, 1);
        context.assertVotedCandidate(epoch + 1, localId);

        // Send the response late
        context.deliverResponse(
            snapshotRequest.correlationId(),
            snapshotRequest.destination(),
            context.fetchSnapshotResponse(
                leaderId,
                responsePartitionSnapshot -> {
                    responsePartitionSnapshot
                        .currentLeader()
                        .setLeaderEpoch(epoch)
                        .setLeaderId(leaderId);

                    responsePartitionSnapshot
                        .snapshotId()
                        .setEndOffset(snapshotId.offset())
                        .setEpoch(snapshotId.epoch());

                    responsePartitionSnapshot.setUnalignedRecords(UnalignedMemoryRecords.empty());

                    return responsePartitionSnapshot;
                }
            )
        );

        // Assert that the response is ignored and the replicas stays as a candidate
        context.client.poll();
        context.assertVotedCandidate(epoch + 1, localId);
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    public void testFetchSnapshotRequestClusterIdValidation(
        boolean withKip853Rpc
    ) throws Exception {
        int localId = randomReplicaId();
        ReplicaKey otherNode = replicaKey(localId + 1, withKip853Rpc);
        Set<Integer> voters = Set.of(localId, otherNode.id());

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withUnknownLeader(4)
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.becomeLeader();
        int epoch = context.currentEpoch();

        // valid cluster id is accepted
        context.deliverRequest(
            fetchSnapshotRequest(
                context.clusterId.toString(),
                otherNode,
                context.metadataPartition,
                epoch,
                Snapshots.BOOTSTRAP_SNAPSHOT_ID,
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
                otherNode,
                context.metadataPartition,
                epoch,
                Snapshots.BOOTSTRAP_SNAPSHOT_ID,
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
                otherNode,
                context.metadataPartition,
                epoch,
                Snapshots.BOOTSTRAP_SNAPSHOT_ID,
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
                otherNode,
                context.metadataPartition,
                epoch,
                Snapshots.BOOTSTRAP_SNAPSHOT_ID,
                Integer.MAX_VALUE,
                0
            )
        );
        context.pollUntilResponse();
        context.assertSentFetchSnapshotResponse(Errors.INCONSISTENT_CLUSTER_ID);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testCreateSnapshotAsLeaderWithInvalidSnapshotId(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        int otherNodeId = localId + 1;
        Set<Integer> voters = Set.of(localId, otherNodeId);
        int epoch = 2;

        List<String> appendRecords = Arrays.asList("a", "b", "c");
        OffsetAndEpoch invalidSnapshotId1 = new OffsetAndEpoch(4, epoch);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .appendToLog(epoch, appendRecords)
            .withAppendLingerMs(1)
            .withKip853Rpc(withKip853Rpc)
            .build();

        context.becomeLeader();
        int currentEpoch = context.currentEpoch();

        // When leader creating snapshot:
        // 1.1 high watermark cannot be empty
        assertEquals(OptionalLong.empty(), context.client.highWatermark());
        assertThrows(IllegalArgumentException.class, () -> context.client.createSnapshot(invalidSnapshotId1, 0));

        // 1.2 high watermark must larger than or equal to the snapshotId's endOffset
        context.advanceLocalLeaderHighWatermarkToLogEndOffset();
        // append some more records to make the LEO > high watermark
        List<String> newRecords = Arrays.asList("d", "e", "f");
        context.client.prepareAppend(currentEpoch, newRecords);
        context.client.schedulePreparedAppend();
        context.time.sleep(context.appendLingerMs());
        context.client.poll();
        assertEquals(context.log.endOffset().offset(), context.client.highWatermark().getAsLong() + newRecords.size());

        OffsetAndEpoch invalidSnapshotId2 = new OffsetAndEpoch(context.client.highWatermark().getAsLong() + 2, currentEpoch);
        assertThrows(IllegalArgumentException.class, () -> context.client.createSnapshot(invalidSnapshotId2, 0));

        // 2 the quorum epoch must larger than or equal to the snapshotId's epoch
        OffsetAndEpoch invalidSnapshotId3 = new OffsetAndEpoch(context.client.highWatermark().getAsLong(), currentEpoch + 1);
        assertThrows(IllegalArgumentException.class, () -> context.client.createSnapshot(invalidSnapshotId3, 0));

        // 3 the snapshotId should be validated against endOffsetForEpoch
        OffsetAndEpoch endOffsetForEpoch = context.log.endOffsetForEpoch(epoch);
        assertEquals(epoch, endOffsetForEpoch.epoch());
        OffsetAndEpoch invalidSnapshotId4 = new OffsetAndEpoch(endOffsetForEpoch.offset() + 2, epoch);
        assertThrows(IllegalArgumentException.class, () -> context.client.createSnapshot(invalidSnapshotId4, 0));
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testCreateSnapshotAsFollowerWithInvalidSnapshotId(boolean withKip853Rpc) throws Exception {
        int localId = randomReplicaId();
        int leaderId = localId + 1;
        int otherFollowerId = localId + 2;
        int epoch = 5;
        Set<Integer> voters = Set.of(localId, leaderId, otherFollowerId);

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, voters)
            .withElectedLeader(epoch, leaderId)
            .withKip853Rpc(withKip853Rpc)
            .build();
        context.assertElectedLeader(epoch, leaderId);

        // When follower creating snapshot:
        // 1) The high watermark cannot be empty
        assertEquals(OptionalLong.empty(), context.client.highWatermark());
        OffsetAndEpoch invalidSnapshotId1 = new OffsetAndEpoch(1, 0);
        assertThrows(IllegalArgumentException.class, () -> context.client.createSnapshot(invalidSnapshotId1, 0));

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
            context.fetchResponse(epoch, leaderId, batch1, 0L, Errors.NONE)
        );
        context.client.poll();

        // 2) The high watermark must be larger than or equal to the snapshotId's endOffset
        int currentEpoch = context.currentEpoch();
        OffsetAndEpoch invalidSnapshotId2 = new OffsetAndEpoch(context.client.highWatermark().getAsLong() + 1, currentEpoch);
        assertThrows(IllegalArgumentException.class, () -> context.client.createSnapshot(invalidSnapshotId2, 0));

        // 3) The quorum epoch must be larger than or equal to the snapshotId's epoch
        OffsetAndEpoch invalidSnapshotId3 = new OffsetAndEpoch(context.client.highWatermark().getAsLong() + 1, currentEpoch + 1);
        assertThrows(IllegalArgumentException.class, () -> context.client.createSnapshot(invalidSnapshotId3, 0));

        // The high watermark advances to be larger than log.endOffsetForEpoch(3), to test the case 3
        context.pollUntilRequest();
        fetchRequest = context.assertSentFetchRequest();
        assertTrue(voters.contains(fetchRequest.destination().id()));
        context.assertFetchRequestData(fetchRequest, epoch, 3L, 3);

        List<String> records2 = Arrays.asList("d", "e", "f");
        MemoryRecords batch2 = context.buildBatch(3L, 4, records2);
        context.deliverResponse(
            fetchRequest.correlationId(),
            fetchRequest.destination(),
            context.fetchResponse(epoch, leaderId, batch2, 6L, Errors.NONE)
        );
        context.client.poll();
        assertEquals(6L, context.client.highWatermark().getAsLong());

        // 4) The snapshotId should be validated against endOffsetForEpoch
        OffsetAndEpoch endOffsetForEpoch = context.log.endOffsetForEpoch(3);
        assertEquals(3, endOffsetForEpoch.epoch());
        OffsetAndEpoch invalidSnapshotId4 = new OffsetAndEpoch(endOffsetForEpoch.offset() + 1, epoch);
        assertThrows(IllegalArgumentException.class, () -> context.client.createSnapshot(invalidSnapshotId4, 0));
    }

    private static ReplicaKey replicaKey(int id, boolean withDirectoryId) {
        Uuid directoryId = withDirectoryId ? Uuid.randomUuid() : ReplicaKey.NO_DIRECTORY_ID;
        return ReplicaKey.of(id, directoryId);
    }

    private static int randomReplicaId() {
        return ThreadLocalRandom.current().nextInt(1025);
    }

    public static FetchSnapshotRequestData fetchSnapshotRequest(
            TopicPartition topicPartition,
            int epoch,
            OffsetAndEpoch offsetAndEpoch,
            int maxBytes,
            long position
    ) {
        return RaftUtil.singletonFetchSnapshotRequest(
            null,
            ReplicaKey.of(-1, ReplicaKey.NO_DIRECTORY_ID),
            topicPartition,
            epoch,
            offsetAndEpoch,
            maxBytes,
            position
        );
    }

    private static FetchSnapshotRequestData fetchSnapshotRequest(
        String clusterId,
        ReplicaKey replicaKey,
        TopicPartition topicPartition,
        int epoch,
        OffsetAndEpoch offsetAndEpoch,
        int maxBytes,
        long position
    ) {
        return RaftUtil.singletonFetchSnapshotRequest(
            clusterId,
            replicaKey,
            topicPartition,
            epoch,
            offsetAndEpoch,
            maxBytes,
            position
        );
    }

    private static FetchSnapshotResponseData fetchSnapshotResponse(
        RaftClientTestContext context,
        int leaderEpoch,
        int leaderId,
        OffsetAndEpoch snapshotId,
        long size,
        long position,
        ByteBuffer buffer
    ) {
        return context.fetchSnapshotResponse(
            leaderId,
            partitionSnapshot -> {
                partitionSnapshot.currentLeader()
                    .setLeaderEpoch(leaderEpoch)
                    .setLeaderId(leaderId);

                partitionSnapshot.snapshotId()
                    .setEndOffset(snapshotId.offset())
                    .setEpoch(snapshotId.epoch());

                return partitionSnapshot
                    .setSize(size)
                    .setPosition(position)
                    .setUnalignedRecords(MemoryRecords.readableRecords(buffer.slice()));
            }
        );
    }

    private static Optional<FetchSnapshotRequestData.PartitionSnapshot> assertFetchSnapshotRequest(
        RaftRequest.Outbound request,
        TopicPartition topicPartition,
        int replicaId,
        int maxBytes
    ) {
        assertInstanceOf(FetchSnapshotRequestData.class, request.data());

        FetchSnapshotRequestData data = (FetchSnapshotRequestData) request.data();

        assertEquals(replicaId, data.replicaId());
        assertEquals(maxBytes, data.maxBytes());

        return FetchSnapshotRequest.forTopicPartition(data, topicPartition);
    }

    private static SnapshotWriter<String> snapshotWriter(RaftClientTestContext context, RawSnapshotWriter snapshot) {
        return new RecordsSnapshotWriter.Builder()
            .setTime(context.time)
            .setRawSnapshotWriter(snapshot)
            .build(new StringSerde());
    }

    private static final class MemorySnapshotWriter implements RawSnapshotWriter {
        private final OffsetAndEpoch snapshotId;
        private final AtomicLong frozenPosition;
        private ByteBuffer data;

        public MemorySnapshotWriter(OffsetAndEpoch snapshotId) {
            this.snapshotId = snapshotId;
            this.data = ByteBuffer.allocate(0);
            this.frozenPosition = new AtomicLong(-1L);
        }

        @Override
        public OffsetAndEpoch snapshotId() {
            return snapshotId;
        }

        @Override
        public long sizeInBytes() {
            long position = frozenPosition.get();
            return (position < 0) ? data.position() : position;
        }

        @Override
        public void append(UnalignedMemoryRecords records) {
            if (isFrozen()) {
                throw new RuntimeException("Snapshot is already frozen " + snapshotId);
            }
            append(records.buffer());
        }

        @Override
        public void append(MemoryRecords records) {
            if (isFrozen()) {
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
            return frozenPosition.get() >= 0;
        }

        @Override
        public void freeze() {
            if (!frozenPosition.compareAndSet(-1L, data.position())) {
                throw new RuntimeException("Snapshot is already frozen " + snapshotId);
            }
            data.flip();
        }

        @Override
        public void close() {}

        public ByteBuffer buffer() {
            return data;
        }
    }
}
