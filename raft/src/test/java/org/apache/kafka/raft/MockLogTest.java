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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MockLogTest {

    private MockLog log;
    private final TopicPartition topicPartition = new TopicPartition("mock-topic", 0);

    @BeforeEach
    public void setup() {
        log = new MockLog(topicPartition);
    }

    @Test
    public void testTopicPartition() {
        assertEquals(topicPartition, log.topicPartition());
    }

    @Test
    public void testAppendAsLeaderHelper() {
        int epoch = 2;
        SimpleRecord recordOne = new SimpleRecord("one".getBytes());
        log.appendAsLeader(Collections.singleton(recordOne), epoch);
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
        log.appendAsLeader(Arrays.asList(recordTwo, recordThree), epoch);
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
        log.appendAsLeader(Arrays.asList(recordOne, recordTwo), epoch);

        SimpleRecord recordThree = new SimpleRecord("three".getBytes());
        log.appendAsLeader(Collections.singleton(recordThree), epoch);

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
    public void testUpdateHighWatermark() {
        appendBatch(5, 1);
        LogOffsetMetadata newOffset = new LogOffsetMetadata(5L);
        log.updateHighWatermark(newOffset);
        assertEquals(newOffset, log.highWatermark());
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
        // The record passed-in offsets are not going to affect the eventual offsets.
        final long initialOffset = 5L;
        SimpleRecord recordFoo = new SimpleRecord("foo".getBytes());
        final int currentEpoch = 3;
        log.appendAsLeader(MemoryRecords.withRecords(initialOffset, CompressionType.NONE, recordFoo), currentEpoch);

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
        assertEquals(Optional.of(new OffsetAndEpoch(1, currentEpoch)), log.endOffsetForEpoch(currentEpoch));
    }

    @Test
    public void testAppendControlRecord() {
        final long initialOffset = 5L;
        final int currentEpoch = 3;
        LeaderChangeMessage messageData =  new LeaderChangeMessage().setLeaderId(0);
        log.appendAsLeader(MemoryRecords.withLeaderChangeMessage(
            initialOffset, 2, messageData), currentEpoch);

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
        assertEquals(Optional.of(new OffsetAndEpoch(1, currentEpoch)), log.endOffsetForEpoch(currentEpoch));
    }

    @Test
    public void testAppendAsFollower() {
        final long initialOffset = 5L;
        final int epoch = 3;
        SimpleRecord recordFoo = new SimpleRecord("foo".getBytes());
        log.appendAsFollower(MemoryRecords.withRecords(initialOffset, CompressionType.NONE, epoch, recordFoo));

        assertEquals(5L, log.startOffset());
        assertEquals(6L, log.endOffset().offset);
        assertEquals(3, log.lastFetchedEpoch());

        Records records = log.read(5L, Isolation.UNCOMMITTED).records;
        List<ByteBuffer> extractRecords = new ArrayList<>();
        for (Record record : records.records()) {
            extractRecords.add(record.value());
        }

        assertEquals(1, extractRecords.size());
        assertEquals(recordFoo.value(), extractRecords.get(0));
        assertEquals(Optional.of(new OffsetAndEpoch(5, 0)), log.endOffsetForEpoch(0));
        assertEquals(Optional.of(new OffsetAndEpoch(log.endOffset().offset, epoch)), log.endOffsetForEpoch(epoch));
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

        log.appendAsLeader(Arrays.asList(recordOne, recordTwo), epoch);

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
        assertEquals(readInfo.startOffsetMetadata, log.highWatermark());

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

        assertEquals(Optional.of(new OffsetAndEpoch(0, 0)), log.endOffsetForEpoch(0));
        assertEquals(Optional.of(new OffsetAndEpoch(15L, 1)), log.endOffsetForEpoch(1));
        assertEquals(Optional.of(new OffsetAndEpoch(15L, 1)), log.endOffsetForEpoch(2));
        assertEquals(Optional.of(new OffsetAndEpoch(20L, 3)), log.endOffsetForEpoch(3));
        assertEquals(Optional.of(new OffsetAndEpoch(30L, 4)), log.endOffsetForEpoch(4));
        assertEquals(Optional.empty(), log.endOffsetForEpoch(5));
    }

    @Test
    public void testEmptyAppendNotAllowed() {
        assertThrows(IllegalArgumentException.class, () -> log.appendAsFollower(MemoryRecords.EMPTY));
        assertThrows(IllegalArgumentException.class, () -> log.appendAsLeader(MemoryRecords.EMPTY, 1));
    }

    @Test
    public void testReadOutOfRangeOffset() {
        final long initialOffset = 5L;
        final int epoch = 3;
        SimpleRecord recordFoo = new SimpleRecord("foo".getBytes());
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
        assertEquals(Optional.of(new OffsetAndEpoch(5L, 1)), log.endOffsetForEpoch(1));
        assertEquals(Optional.of(new OffsetAndEpoch(5L, 2)), log.endOffsetForEpoch(2));

        // Initialize a new epoch at the same end offset. The epoch cache ensures
        // that the start offset of each retained epoch increases monotonically.
        log.initializeLeaderEpoch(3);
        assertEquals(Optional.of(new OffsetAndEpoch(5L, 1)), log.endOffsetForEpoch(1));
        assertEquals(Optional.of(new OffsetAndEpoch(5L, 1)), log.endOffsetForEpoch(2));
        assertEquals(Optional.of(new OffsetAndEpoch(5L, 3)), log.endOffsetForEpoch(3));
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

    private void appendBatch(int numRecords, int epoch) {
        List<SimpleRecord> records = new ArrayList<>(numRecords);
        for (int i = 0; i < numRecords; i++) {
            records.add(new SimpleRecord(String.valueOf(i).getBytes()));
        }
        log.appendAsLeader(records, epoch);
    }
}
