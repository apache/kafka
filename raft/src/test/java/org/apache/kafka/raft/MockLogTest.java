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
import org.apache.kafka.common.errors.OffsetOutOfRangeException;
import org.apache.kafka.common.message.LeaderChangeMessage;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.ControlRecordUtils;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.snapshot.RawSnapshotReader;
import org.apache.kafka.snapshot.RawSnapshotWriter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MockLogTest {

    private MockLog log;
    private final TopicPartition topicPartition = new TopicPartition("mock-topic", 0);
    private final Uuid topicId = Uuid.randomUuid();

    @BeforeEach
    public void setup() {
        log = new MockLog(topicPartition, topicId);
    }

    @AfterEach
    public void cleanup() {
        log.close();
    }

    @Test
    public void testTopicPartition() {
        assertEquals(topicPartition, log.topicPartition());
    }

    @Test
    public void testTopicId() {
        assertEquals(topicId, log.topicId());
    }

    @Test
    public void testAppendAsLeaderHelper() {
        int epoch = 2;
        SimpleRecord recordOne = new SimpleRecord("one".getBytes());
        appendAsLeader(Collections.singleton(recordOne), epoch);
        assertEquals(epoch, log.lastFetchedEpoch());
        assertEquals(0L, log.startOffset());
        assertEquals(1L, log.endOffset().offset);

        Records records = log.read(0, Isolation.UNCOMMITTED).records;
        List<? extends RecordBatch> batches = Utils.toList(records.batches().iterator());

        RecordBatch batch = batches.get(0);
        assertEquals(0, batch.baseOffset());
        assertEquals(0, batch.lastOffset());

        List<Record> fetchedRecords = Utils.toList(batch.iterator());
        assertEquals(1, fetchedRecords.size());
        assertEquals(recordOne, new SimpleRecord(fetchedRecords.get(0)));
        assertEquals(0, fetchedRecords.get(0).offset());

        SimpleRecord recordTwo = new SimpleRecord("two".getBytes());
        SimpleRecord recordThree = new SimpleRecord("three".getBytes());
        appendAsLeader(Arrays.asList(recordTwo, recordThree), epoch);
        assertEquals(0L, log.startOffset());
        assertEquals(3L, log.endOffset().offset);

        records = log.read(0, Isolation.UNCOMMITTED).records;
        batches = Utils.toList(records.batches().iterator());
        assertEquals(2, batches.size());

        fetchedRecords = Utils.toList(records.records().iterator());
        assertEquals(3, fetchedRecords.size());
        assertEquals(recordOne, new SimpleRecord(fetchedRecords.get(0)));
        assertEquals(0, fetchedRecords.get(0).offset());

        assertEquals(recordTwo, new SimpleRecord(fetchedRecords.get(1)));
        assertEquals(1, fetchedRecords.get(1).offset());

        assertEquals(recordThree, new SimpleRecord(fetchedRecords.get(2)));
        assertEquals(2, fetchedRecords.get(2).offset());
    }

    @Test
    public void testTruncateTo() {
        int epoch = 2;
        SimpleRecord recordOne = new SimpleRecord("one".getBytes());
        SimpleRecord recordTwo = new SimpleRecord("two".getBytes());
        appendAsLeader(Arrays.asList(recordOne, recordTwo), epoch);

        SimpleRecord recordThree = new SimpleRecord("three".getBytes());
        appendAsLeader(Collections.singleton(recordThree), epoch);

        assertEquals(0L, log.startOffset());
        assertEquals(3L, log.endOffset().offset);

        log.truncateTo(2);
        assertEquals(0L, log.startOffset());
        assertEquals(2L, log.endOffset().offset);

        log.truncateTo(1);
        assertEquals(0L, log.startOffset());
        assertEquals(0L, log.endOffset().offset);
    }

    @Test
    public void testTruncateBelowHighWatermark() {
        appendBatch(5, 1);
        LogOffsetMetadata highWatermark = new LogOffsetMetadata(5L);
        log.updateHighWatermark(highWatermark);
        assertEquals(highWatermark, log.highWatermark());
        assertThrows(IllegalArgumentException.class, () -> log.truncateTo(4L));
        assertEquals(highWatermark, log.highWatermark());
    }

    @Test
    public void testUpdateHighWatermark() {
        appendBatch(5, 1);
        LogOffsetMetadata newOffset = new LogOffsetMetadata(5L);
        log.updateHighWatermark(newOffset);
        assertEquals(newOffset.offset, log.highWatermark().offset);
    }

    @Test
    public void testDecrementHighWatermark() {
        appendBatch(5, 1);
        LogOffsetMetadata newOffset = new LogOffsetMetadata(4L);
        log.updateHighWatermark(newOffset);
        assertThrows(IllegalArgumentException.class, () -> log.updateHighWatermark(new LogOffsetMetadata(3L)));
    }

    @Test
    public void testAssignEpochStartOffset() {
        log.initializeLeaderEpoch(2);
        assertEquals(2, log.lastFetchedEpoch());
    }

    @Test
    public void testAppendAsLeader() {
        SimpleRecord recordFoo = new SimpleRecord("foo".getBytes());
        final int currentEpoch = 3;
        final long initialOffset = log.endOffset().offset;

        log.appendAsLeader(
            MemoryRecords.withRecords(initialOffset, CompressionType.NONE, recordFoo),
            currentEpoch
        );

        assertEquals(0, log.startOffset());
        assertEquals(1, log.endOffset().offset);
        assertEquals(currentEpoch, log.lastFetchedEpoch());

        Records records = log.read(0, Isolation.UNCOMMITTED).records;
        List<ByteBuffer> extractRecords = new ArrayList<>();
        for (Record record : records.records()) {
            extractRecords.add(record.value());
        }

        assertEquals(1, extractRecords.size());
        assertEquals(recordFoo.value(), extractRecords.get(0));
        assertEquals(new OffsetAndEpoch(1, currentEpoch), log.endOffsetForEpoch(currentEpoch));
    }

    @Test
    public void testUnexpectedAppendOffset() {
        SimpleRecord recordFoo = new SimpleRecord("foo".getBytes());
        final int currentEpoch = 3;
        final long initialOffset = log.endOffset().offset;

        log.appendAsLeader(
            MemoryRecords.withRecords(initialOffset, CompressionType.NONE, currentEpoch, recordFoo),
            currentEpoch
        );

        // Throw exception for out of order records
        assertThrows(
            RuntimeException.class,
            () -> {
                log.appendAsLeader(
                    MemoryRecords.withRecords(initialOffset, CompressionType.NONE, currentEpoch, recordFoo),
                    currentEpoch
                );
            }
        );

        assertThrows(
            RuntimeException.class,
            () -> {
                log.appendAsFollower(
                    MemoryRecords.withRecords(initialOffset, CompressionType.NONE, currentEpoch, recordFoo)
                );
            }
        );
    }

    @Test
    public void testAppendControlRecord() {
        final long initialOffset = 0;
        final int currentEpoch = 3;
        LeaderChangeMessage messageData =  new LeaderChangeMessage().setLeaderId(0);
        ByteBuffer buffer = ByteBuffer.allocate(256);
        log.appendAsLeader(
            MemoryRecords.withLeaderChangeMessage(initialOffset, 0L, 2, buffer, messageData),
            currentEpoch
        );

        assertEquals(0, log.startOffset());
        assertEquals(1, log.endOffset().offset);
        assertEquals(currentEpoch, log.lastFetchedEpoch());

        Records records = log.read(0, Isolation.UNCOMMITTED).records;
        for (RecordBatch batch : records.batches()) {
            assertTrue(batch.isControlBatch());
        }
        List<ByteBuffer> extractRecords = new ArrayList<>();
        for (Record record : records.records()) {
            LeaderChangeMessage deserializedData = ControlRecordUtils.deserializeLeaderChangeMessage(record);
            assertEquals(deserializedData, messageData);
            extractRecords.add(record.value());
        }

        assertEquals(1, extractRecords.size());
        assertEquals(new OffsetAndEpoch(1, currentEpoch), log.endOffsetForEpoch(currentEpoch));
    }

    @Test
    public void testAppendAsFollower() throws IOException {
        final long initialOffset = 5;
        final int epoch = 3;
        SimpleRecord recordFoo = new SimpleRecord("foo".getBytes());

        try (RawSnapshotWriter snapshot = log.createSnapshot(new OffsetAndEpoch(initialOffset, 0))) {
            snapshot.freeze();
        }
        log.truncateToLatestSnapshot();

        log.appendAsFollower(MemoryRecords.withRecords(initialOffset, CompressionType.NONE, epoch, recordFoo));

        assertEquals(initialOffset, log.startOffset());
        assertEquals(initialOffset + 1, log.endOffset().offset);
        assertEquals(3, log.lastFetchedEpoch());

        Records records = log.read(5L, Isolation.UNCOMMITTED).records;
        List<ByteBuffer> extractRecords = new ArrayList<>();
        for (Record record : records.records()) {
            extractRecords.add(record.value());
        }

        assertEquals(1, extractRecords.size());
        assertEquals(recordFoo.value(), extractRecords.get(0));
        assertEquals(new OffsetAndEpoch(5, 0), log.endOffsetForEpoch(0));
        assertEquals(new OffsetAndEpoch(log.endOffset().offset, epoch), log.endOffsetForEpoch(epoch));
    }

    @Test
    public void testReadRecords() {
        int epoch = 2;

        ByteBuffer recordOneBuffer = ByteBuffer.allocate(4);
        recordOneBuffer.putInt(1);
        SimpleRecord recordOne = new SimpleRecord(recordOneBuffer);

        ByteBuffer recordTwoBuffer = ByteBuffer.allocate(4);
        recordTwoBuffer.putInt(2);
        SimpleRecord recordTwo = new SimpleRecord(recordTwoBuffer);

        appendAsLeader(Arrays.asList(recordOne, recordTwo), epoch);

        Records records = log.read(0, Isolation.UNCOMMITTED).records;

        List<ByteBuffer> extractRecords = new ArrayList<>();
        for (Record record : records.records()) {
            extractRecords.add(record.value());
        }
        assertEquals(Arrays.asList(recordOne.value(), recordTwo.value()), extractRecords);
    }

    @Test
    public void testReadUpToLogEnd() {
        appendBatch(20, 1);
        appendBatch(10, 1);
        appendBatch(30, 1);

        assertEquals(Optional.of(new OffsetRange(0L, 59L)), readOffsets(0L, Isolation.UNCOMMITTED));
        assertEquals(Optional.of(new OffsetRange(0L, 59L)), readOffsets(10L, Isolation.UNCOMMITTED));
        assertEquals(Optional.of(new OffsetRange(20L, 59L)), readOffsets(20L, Isolation.UNCOMMITTED));
        assertEquals(Optional.of(new OffsetRange(20L, 59L)), readOffsets(25L, Isolation.UNCOMMITTED));
        assertEquals(Optional.of(new OffsetRange(30L, 59L)), readOffsets(30L, Isolation.UNCOMMITTED));
        assertEquals(Optional.of(new OffsetRange(30L, 59L)), readOffsets(33L, Isolation.UNCOMMITTED));
        assertEquals(Optional.empty(), readOffsets(60L, Isolation.UNCOMMITTED));
        assertThrows(OffsetOutOfRangeException.class, () -> log.read(61L, Isolation.UNCOMMITTED));

        // Verify range after truncation
        log.truncateTo(20L);
        assertThrows(OffsetOutOfRangeException.class, () -> log.read(21L, Isolation.UNCOMMITTED));
    }

    @Test
    public void testReadUpToHighWatermark() {
        appendBatch(20, 1);
        appendBatch(10, 1);
        appendBatch(30, 1);

        log.updateHighWatermark(new LogOffsetMetadata(0L));
        assertEquals(Optional.empty(), readOffsets(0L, Isolation.COMMITTED));
        assertEquals(Optional.empty(), readOffsets(10L, Isolation.COMMITTED));

        log.updateHighWatermark(new LogOffsetMetadata(20L));
        assertEquals(Optional.of(new OffsetRange(0L, 19L)), readOffsets(0L, Isolation.COMMITTED));
        assertEquals(Optional.of(new OffsetRange(0L, 19L)), readOffsets(10L, Isolation.COMMITTED));
        assertEquals(Optional.empty(), readOffsets(20L, Isolation.COMMITTED));
        assertEquals(Optional.empty(), readOffsets(30L, Isolation.COMMITTED));

        log.updateHighWatermark(new LogOffsetMetadata(30L));
        assertEquals(Optional.of(new OffsetRange(0L, 29L)), readOffsets(0L, Isolation.COMMITTED));
        assertEquals(Optional.of(new OffsetRange(0L, 29L)), readOffsets(10L, Isolation.COMMITTED));
        assertEquals(Optional.of(new OffsetRange(20L, 29L)), readOffsets(20L, Isolation.COMMITTED));
        assertEquals(Optional.of(new OffsetRange(20L, 29L)), readOffsets(25L, Isolation.COMMITTED));
        assertEquals(Optional.empty(), readOffsets(30L, Isolation.COMMITTED));
        assertEquals(Optional.empty(), readOffsets(50L, Isolation.COMMITTED));

        log.updateHighWatermark(new LogOffsetMetadata(60L));
        assertEquals(Optional.of(new OffsetRange(0L, 59L)), readOffsets(0L, Isolation.COMMITTED));
        assertEquals(Optional.of(new OffsetRange(0L, 59L)), readOffsets(10L, Isolation.COMMITTED));
        assertEquals(Optional.of(new OffsetRange(20L, 59L)), readOffsets(20L, Isolation.COMMITTED));
        assertEquals(Optional.of(new OffsetRange(20L, 59L)), readOffsets(25L, Isolation.COMMITTED));
        assertEquals(Optional.of(new OffsetRange(30, 59L)), readOffsets(30L, Isolation.COMMITTED));
        assertEquals(Optional.of(new OffsetRange(30L, 59L)), readOffsets(50L, Isolation.COMMITTED));
        assertEquals(Optional.empty(), readOffsets(60L, Isolation.COMMITTED));
        assertThrows(OffsetOutOfRangeException.class, () -> log.read(61L, Isolation.COMMITTED));
    }

    @Test
    public void testMetadataValidation() {
        appendBatch(5, 1);
        appendBatch(5, 1);
        appendBatch(5, 1);

        LogFetchInfo readInfo = log.read(5, Isolation.UNCOMMITTED);
        assertEquals(5L, readInfo.startOffsetMetadata.offset);
        assertTrue(readInfo.startOffsetMetadata.metadata.isPresent());
        MockLog.MockOffsetMetadata offsetMetadata = (MockLog.MockOffsetMetadata)
            readInfo.startOffsetMetadata.metadata.get();

        // Update to a high watermark with valid offset metadata
        log.updateHighWatermark(readInfo.startOffsetMetadata);
        assertEquals(readInfo.startOffsetMetadata.offset, log.highWatermark().offset);

        // Now update to a high watermark with invalid metadata
        assertThrows(IllegalArgumentException.class, () ->
            log.updateHighWatermark(new LogOffsetMetadata(10L,
                Optional.of(new MockLog.MockOffsetMetadata(98230980L)))));

        // Ensure we can update the high watermark to the end offset
        LogFetchInfo readFromEndInfo = log.read(15L, Isolation.UNCOMMITTED);
        assertEquals(15, readFromEndInfo.startOffsetMetadata.offset);
        assertTrue(readFromEndInfo.startOffsetMetadata.metadata.isPresent());
        log.updateHighWatermark(readFromEndInfo.startOffsetMetadata);

        // Ensure that the end offset metadata is valid after new entries are appended
        appendBatch(5, 1);
        log.updateHighWatermark(readFromEndInfo.startOffsetMetadata);

        // Check handling of a fetch from the middle of a batch
        LogFetchInfo readFromMiddleInfo = log.read(16L, Isolation.UNCOMMITTED);
        assertEquals(readFromEndInfo.startOffsetMetadata, readFromMiddleInfo.startOffsetMetadata);
    }

    @Test
    public void testEndOffsetForEpoch() {
        appendBatch(5, 1);
        appendBatch(10, 1);
        appendBatch(5, 3);
        appendBatch(10, 4);

        assertEquals(new OffsetAndEpoch(0, 0), log.endOffsetForEpoch(0));
        assertEquals(new OffsetAndEpoch(15L, 1), log.endOffsetForEpoch(1));
        assertEquals(new OffsetAndEpoch(15L, 1), log.endOffsetForEpoch(2));
        assertEquals(new OffsetAndEpoch(20L, 3), log.endOffsetForEpoch(3));
        assertEquals(new OffsetAndEpoch(30L, 4), log.endOffsetForEpoch(4));
        assertEquals(new OffsetAndEpoch(30L, 4), log.endOffsetForEpoch(5));
    }

    @Test
    public void testEmptyAppendNotAllowed() {
        assertThrows(IllegalArgumentException.class, () -> log.appendAsFollower(MemoryRecords.EMPTY));
        assertThrows(IllegalArgumentException.class, () -> log.appendAsLeader(MemoryRecords.EMPTY, 1));
    }

    @Test
    public void testReadOutOfRangeOffset() throws IOException {
        final long initialOffset = 5L;
        final int epoch = 3;
        SimpleRecord recordFoo = new SimpleRecord("foo".getBytes());

        try (RawSnapshotWriter snapshot = log.createSnapshot(new OffsetAndEpoch(initialOffset, 0))) {
            snapshot.freeze();
        }
        log.truncateToLatestSnapshot();

        log.appendAsFollower(MemoryRecords.withRecords(initialOffset, CompressionType.NONE, epoch, recordFoo));

        assertThrows(OffsetOutOfRangeException.class, () -> log.read(log.startOffset() - 1,
            Isolation.UNCOMMITTED));
        assertThrows(OffsetOutOfRangeException.class, () -> log.read(log.endOffset().offset + 1,
            Isolation.UNCOMMITTED));
    }

    @Test
    public void testMonotonicEpochStartOffset() {
        appendBatch(5, 1);
        assertEquals(5L, log.endOffset().offset);

        log.initializeLeaderEpoch(2);
        assertEquals(new OffsetAndEpoch(5L, 1), log.endOffsetForEpoch(1));
        assertEquals(new OffsetAndEpoch(5L, 2), log.endOffsetForEpoch(2));

        // Initialize a new epoch at the same end offset. The epoch cache ensures
        // that the start offset of each retained epoch increases monotonically.
        log.initializeLeaderEpoch(3);
        assertEquals(new OffsetAndEpoch(5L, 1), log.endOffsetForEpoch(1));
        assertEquals(new OffsetAndEpoch(5L, 1), log.endOffsetForEpoch(2));
        assertEquals(new OffsetAndEpoch(5L, 3), log.endOffsetForEpoch(3));
    }

    @Test
    public void testUnflushedRecordsLostAfterReopen() {
        appendBatch(5, 1);
        appendBatch(10, 2);
        log.flush();

        appendBatch(5, 3);
        appendBatch(10, 4);
        log.reopen();

        assertEquals(15L, log.endOffset().offset);
        assertEquals(2, log.lastFetchedEpoch());
    }

    @Test
    public void testCreateSnapshot() throws IOException {
        int numberOfRecords = 10;
        int epoch = 0;
        OffsetAndEpoch snapshotId = new OffsetAndEpoch(numberOfRecords, epoch);
        appendBatch(numberOfRecords, epoch);
        log.updateHighWatermark(new LogOffsetMetadata(numberOfRecords));

        try (RawSnapshotWriter snapshot = log.createSnapshot(snapshotId)) {
            snapshot.freeze();
        }

        try (RawSnapshotReader snapshot = log.readSnapshot(snapshotId).get()) {
            assertEquals(0, snapshot.sizeInBytes());
        }
    }

    @Test
    public void testReadMissingSnapshot() {
        assertFalse(log.readSnapshot(new OffsetAndEpoch(10, 0)).isPresent());
    }

    @Test
    public void testUpdateLogStartOffset() throws IOException {
        int offset = 10;
        int epoch = 0;
        OffsetAndEpoch snapshotId = new OffsetAndEpoch(offset, epoch);

        appendBatch(offset, epoch);
        log.updateHighWatermark(new LogOffsetMetadata(offset));

        try (RawSnapshotWriter snapshot = log.createSnapshot(snapshotId)) {
            snapshot.freeze();
        }

        assertTrue(log.deleteBeforeSnapshot(snapshotId));
        assertEquals(offset, log.startOffset());
        assertEquals(epoch, log.lastFetchedEpoch());
        assertEquals(offset, log.endOffset().offset);

        int newRecords = 10;
        appendBatch(newRecords, epoch + 1);
        log.updateHighWatermark(new LogOffsetMetadata(offset + newRecords));

        // Start offset should not change since a new snapshot was not generated
        assertFalse(log.deleteBeforeSnapshot(new OffsetAndEpoch(offset + newRecords, epoch)));
        assertEquals(offset, log.startOffset());

        assertEquals(epoch + 1, log.lastFetchedEpoch());
        assertEquals(offset + newRecords, log.endOffset().offset);
        assertEquals(offset + newRecords, log.highWatermark().offset);
    }

    @Test
    public void testUpdateLogStartOffsetWithMissingSnapshot() {
        int offset = 10;
        int epoch = 0;

        appendBatch(offset, epoch);
        log.updateHighWatermark(new LogOffsetMetadata(offset));

        assertFalse(log.deleteBeforeSnapshot(new OffsetAndEpoch(1, epoch)));
        assertEquals(0, log.startOffset());
        assertEquals(epoch, log.lastFetchedEpoch());
        assertEquals(offset, log.endOffset().offset);
        assertEquals(offset, log.highWatermark().offset);
    }

    @Test
    public void testFailToIncreaseLogStartPastHighWatermark() throws IOException {
        int offset = 10;
        int epoch = 0;
        OffsetAndEpoch snapshotId = new OffsetAndEpoch(2 * offset, 1 + epoch);

        appendBatch(offset, epoch);
        log.updateHighWatermark(new LogOffsetMetadata(offset));

        try (RawSnapshotWriter snapshot = log.createSnapshot(snapshotId)) {
            snapshot.freeze();
        }

        assertThrows(
            OffsetOutOfRangeException.class,
            () -> log.deleteBeforeSnapshot(snapshotId)
        );
    }

    @Test
    public void testTruncateFullyToLatestSnapshot() throws IOException {
        int numberOfRecords = 10;
        int epoch = 0;
        OffsetAndEpoch sameEpochSnapshotId = new OffsetAndEpoch(2 * numberOfRecords, epoch);

        appendBatch(numberOfRecords, epoch);

        try (RawSnapshotWriter snapshot = log.createSnapshot(sameEpochSnapshotId)) {
            snapshot.freeze();
        }

        assertTrue(log.truncateToLatestSnapshot());
        assertEquals(sameEpochSnapshotId.offset, log.startOffset());
        assertEquals(sameEpochSnapshotId.epoch, log.lastFetchedEpoch());
        assertEquals(sameEpochSnapshotId.offset, log.endOffset().offset);
        assertEquals(sameEpochSnapshotId.offset, log.highWatermark().offset);

        OffsetAndEpoch greaterEpochSnapshotId = new OffsetAndEpoch(3 * numberOfRecords, epoch + 1);

        appendBatch(numberOfRecords, epoch);

        try (RawSnapshotWriter snapshot = log.createSnapshot(greaterEpochSnapshotId)) {
            snapshot.freeze();
        }

        assertTrue(log.truncateToLatestSnapshot());
        assertEquals(greaterEpochSnapshotId.offset, log.startOffset());
        assertEquals(greaterEpochSnapshotId.epoch, log.lastFetchedEpoch());
        assertEquals(greaterEpochSnapshotId.offset, log.endOffset().offset);
        assertEquals(greaterEpochSnapshotId.offset, log.highWatermark().offset);
    }

    @Test
    public void testDoesntTruncateFully() throws IOException {
        int numberOfRecords = 10;
        int epoch = 1;

        appendBatch(numberOfRecords, epoch);

        OffsetAndEpoch olderEpochSnapshotId = new OffsetAndEpoch(numberOfRecords, epoch - 1);
        try (RawSnapshotWriter snapshot = log.createSnapshot(olderEpochSnapshotId)) {
            snapshot.freeze();
        }

        assertFalse(log.truncateToLatestSnapshot());

        appendBatch(numberOfRecords, epoch);

        OffsetAndEpoch olderOffsetSnapshotId = new OffsetAndEpoch(numberOfRecords, epoch);
        try (RawSnapshotWriter snapshot = log.createSnapshot(olderOffsetSnapshotId)) {
            snapshot.freeze();
        }

        assertFalse(log.truncateToLatestSnapshot());
    }

    @Test
    public void testTruncateWillRemoveOlderSnapshot() throws IOException {
        int numberOfRecords = 10;
        int epoch = 0;

        OffsetAndEpoch sameEpochSnapshotId = new OffsetAndEpoch(numberOfRecords, epoch);
        appendBatch(numberOfRecords, epoch);

        try (RawSnapshotWriter snapshot = log.createSnapshot(sameEpochSnapshotId)) {
            snapshot.freeze();
        }

        OffsetAndEpoch greaterEpochSnapshotId = new OffsetAndEpoch(2 * numberOfRecords, epoch + 1);
        appendBatch(numberOfRecords, epoch);

        try (RawSnapshotWriter snapshot = log.createSnapshot(greaterEpochSnapshotId)) {
            snapshot.freeze();
        }

        assertTrue(log.truncateToLatestSnapshot());
        assertEquals(Optional.empty(), log.readSnapshot(sameEpochSnapshotId));
    }

    @Test
    public void testUpdateLogStartOffsetWillRemoveOlderSnapshot() throws IOException {
        int numberOfRecords = 10;
        int epoch = 0;

        OffsetAndEpoch sameEpochSnapshotId = new OffsetAndEpoch(numberOfRecords, epoch);
        appendBatch(numberOfRecords, epoch);

        try (RawSnapshotWriter snapshot = log.createSnapshot(sameEpochSnapshotId)) {
            snapshot.freeze();
        }

        OffsetAndEpoch greaterEpochSnapshotId = new OffsetAndEpoch(2 * numberOfRecords, epoch + 1);
        appendBatch(numberOfRecords, epoch);

        try (RawSnapshotWriter snapshot = log.createSnapshot(greaterEpochSnapshotId)) {
            snapshot.freeze();
        }

        log.updateHighWatermark(new LogOffsetMetadata(greaterEpochSnapshotId.offset));
        assertTrue(log.deleteBeforeSnapshot(greaterEpochSnapshotId));
        assertEquals(Optional.empty(), log.readSnapshot(sameEpochSnapshotId));
    }

    @Test
    public void testValidateEpochGreaterThanLastKnownEpoch() {
        int numberOfRecords = 1;
        int epoch = 1;

        appendBatch(numberOfRecords, epoch);

        ValidOffsetAndEpoch resultOffsetAndEpoch = log.validateOffsetAndEpoch(numberOfRecords, epoch + 1);
        assertEquals(new ValidOffsetAndEpoch(ValidOffsetAndEpoch.Kind.DIVERGING, new OffsetAndEpoch(log.endOffset().offset, epoch)),
                resultOffsetAndEpoch);
    }

    @Test
    public void testValidateEpochLessThanOldestSnapshotEpoch() throws IOException {
        int offset = 1;
        int epoch = 1;

        OffsetAndEpoch olderEpochSnapshotId = new OffsetAndEpoch(offset, epoch);
        try (RawSnapshotWriter snapshot = log.createSnapshot(olderEpochSnapshotId)) {
            snapshot.freeze();
        }
        log.truncateToLatestSnapshot();

        ValidOffsetAndEpoch resultOffsetAndEpoch = log.validateOffsetAndEpoch(offset, epoch - 1);
        assertEquals(new ValidOffsetAndEpoch(ValidOffsetAndEpoch.Kind.SNAPSHOT, olderEpochSnapshotId),
                resultOffsetAndEpoch);
    }

    @Test
    public void testValidateOffsetLessThanOldestSnapshotOffset() throws IOException {
        int offset = 2;
        int epoch = 1;

        OffsetAndEpoch olderEpochSnapshotId = new OffsetAndEpoch(offset, epoch);
        try (RawSnapshotWriter snapshot = log.createSnapshot(olderEpochSnapshotId)) {
            snapshot.freeze();
        }
        log.truncateToLatestSnapshot();

        ValidOffsetAndEpoch resultOffsetAndEpoch = log.validateOffsetAndEpoch(offset - 1, epoch);
        assertEquals(new ValidOffsetAndEpoch(ValidOffsetAndEpoch.Kind.SNAPSHOT, olderEpochSnapshotId),
                resultOffsetAndEpoch);
    }

    @Test
    public void testValidateOffsetEqualToOldestSnapshotOffset() throws IOException {
        int offset = 2;
        int epoch = 1;

        OffsetAndEpoch olderEpochSnapshotId = new OffsetAndEpoch(offset, epoch);
        try (RawSnapshotWriter snapshot = log.createSnapshot(olderEpochSnapshotId)) {
            snapshot.freeze();
        }
        log.truncateToLatestSnapshot();

        ValidOffsetAndEpoch resultOffsetAndEpoch = log.validateOffsetAndEpoch(offset, epoch);
        assertEquals(new ValidOffsetAndEpoch(ValidOffsetAndEpoch.Kind.VALID, olderEpochSnapshotId),
                resultOffsetAndEpoch);
    }

    @Test
    public void testValidateUnknownEpochLessThanLastKnownGreaterThanOldestSnapshot() throws IOException {
        int numberOfRecords = 5;
        int offset = 10;

        OffsetAndEpoch olderEpochSnapshotId = new OffsetAndEpoch(offset, 1);
        try (RawSnapshotWriter snapshot = log.createSnapshot(olderEpochSnapshotId)) {
            snapshot.freeze();
        }
        log.truncateToLatestSnapshot();

        appendBatch(numberOfRecords, 1);
        appendBatch(numberOfRecords, 2);
        appendBatch(numberOfRecords, 4);

        // offset is not equal to oldest snapshot's offset
        ValidOffsetAndEpoch resultOffsetAndEpoch = log.validateOffsetAndEpoch(100, 3);
        assertEquals(new ValidOffsetAndEpoch(ValidOffsetAndEpoch.Kind.DIVERGING, new OffsetAndEpoch(20, 2)),
                resultOffsetAndEpoch);
    }

    @Test
    public void testValidateEpochLessThanFirstEpochInLog() throws IOException {
        int numberOfRecords = 5;
        int offset = 10;

        OffsetAndEpoch olderEpochSnapshotId = new OffsetAndEpoch(offset, 1);
        try (RawSnapshotWriter snapshot = log.createSnapshot(olderEpochSnapshotId)) {
            snapshot.freeze();
        }
        log.truncateToLatestSnapshot();

        appendBatch(numberOfRecords, 3);

        // offset is not equal to oldest snapshot's offset
        ValidOffsetAndEpoch resultOffsetAndEpoch = log.validateOffsetAndEpoch(100, 2);
        assertEquals(new ValidOffsetAndEpoch(ValidOffsetAndEpoch.Kind.DIVERGING, olderEpochSnapshotId),
                resultOffsetAndEpoch);
    }

    @Test
    public void testValidateOffsetGreatThanEndOffset() {
        int numberOfRecords = 1;
        int epoch = 1;

        appendBatch(numberOfRecords, epoch);

        ValidOffsetAndEpoch resultOffsetAndEpoch = log.validateOffsetAndEpoch(numberOfRecords + 1, epoch);
        assertEquals(new ValidOffsetAndEpoch(ValidOffsetAndEpoch.Kind.DIVERGING, new OffsetAndEpoch(log.endOffset().offset, epoch)),
                resultOffsetAndEpoch);
    }

    @Test
    public void testValidateOffsetLessThanLEO() {
        int numberOfRecords = 10;
        int epoch = 1;

        appendBatch(numberOfRecords, epoch);
        appendBatch(numberOfRecords, epoch + 1);

        ValidOffsetAndEpoch resultOffsetAndEpoch = log.validateOffsetAndEpoch(11, epoch);
        assertEquals(new ValidOffsetAndEpoch(ValidOffsetAndEpoch.Kind.DIVERGING, new OffsetAndEpoch(10, epoch)),
                resultOffsetAndEpoch);
    }

    @Test
    public void testValidateValidEpochAndOffset() {
        int numberOfRecords = 5;
        int epoch = 1;

        appendBatch(numberOfRecords, epoch);

        ValidOffsetAndEpoch resultOffsetAndEpoch = log.validateOffsetAndEpoch(numberOfRecords - 1, epoch);
        assertEquals(new ValidOffsetAndEpoch(ValidOffsetAndEpoch.Kind.VALID, new OffsetAndEpoch(numberOfRecords - 1, epoch)),
                resultOffsetAndEpoch);
    }

    private Optional<OffsetRange> readOffsets(long startOffset, Isolation isolation) {
        Records records = log.read(startOffset, isolation).records;
        long firstReadOffset = -1L;
        long lastReadOffset = -1L;
        for (Record record : records.records()) {
            if (firstReadOffset < 0)
                firstReadOffset = record.offset();
            if (record.offset() > lastReadOffset)
                lastReadOffset = record.offset();
        }
        if (firstReadOffset < 0) {
            return Optional.empty();
        } else {
            return Optional.of(new OffsetRange(firstReadOffset, lastReadOffset));
        }
    }

    private static class OffsetRange {
        public final long startOffset;
        public final long endOffset;

        private OffsetRange(long startOffset, long endOffset) {
            this.startOffset = startOffset;
            this.endOffset = endOffset;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            OffsetRange that = (OffsetRange) o;
            return startOffset == that.startOffset &&
                endOffset == that.endOffset;
        }

        @Override
        public int hashCode() {
            return Objects.hash(startOffset, endOffset);
        }
    }

    private void appendAsLeader(Collection<SimpleRecord> records, int epoch) {
        log.appendAsLeader(
            MemoryRecords.withRecords(
                log.endOffset().offset,
                CompressionType.NONE,
                records.toArray(new SimpleRecord[records.size()])
            ),
            epoch
        );
    }

    private void appendBatch(int numRecords, int epoch) {
        List<SimpleRecord> records = new ArrayList<>(numRecords);
        for (int i = 0; i < numRecords; i++) {
            records.add(new SimpleRecord(String.valueOf(i).getBytes()));
        }

        appendAsLeader(records, epoch);
    }
}
