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
package org.apache.kafka.storage.internals.log;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.record.ControlRecordType;
import org.apache.kafka.common.record.EndTransactionMarker;
import org.apache.kafka.common.record.FileLogInputStream;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.transaction.TransactionLogConfig;
import org.apache.kafka.server.util.MockScheduler;
import org.apache.kafka.storage.internals.checkpoint.LeaderEpochCheckpointFile;
import org.apache.kafka.storage.internals.epoch.LeaderEpochFileCache;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LogSegmentTest {
    private final TopicPartition topicPartition = new TopicPartition("topic", 0);
    private final List<LogSegment> segments = new ArrayList<>();
    private File logDir = null;

    /* Create a segment with the given base offset */
    private LogSegment createSegment(long offset, int indexIntervalBytes, Time time) throws IOException {
        LogSegment seg = LogTestUtils.createSegment(offset, logDir, indexIntervalBytes, time);
        segments.add(seg);
        return seg;
    }

    private LogSegment createSegment(long offset) throws IOException {
        return createSegment(offset, 10, Time.SYSTEM);
    }

    private LogSegment createSegment(long offset, Time time) throws IOException {
        return createSegment(offset, 10, time);
    }

    private LogSegment createSegment(long offset, int indexIntervalBytes) throws IOException {
        return createSegment(offset, indexIntervalBytes, Time.SYSTEM);
    }

    /* Create a ByteBufferMessageSet for the given messages starting from the given offset */
    private MemoryRecords v1Records(long offset, String... records) {
        List<SimpleRecord> simpleRecords = new ArrayList<>();
        for (String s : records) {
            simpleRecords.add(new SimpleRecord(offset * 10, s.getBytes()));
        }
        return MemoryRecords.withRecords(
            RecordBatch.MAGIC_VALUE_V1, offset,
            Compression.NONE, TimestampType.CREATE_TIME, simpleRecords.toArray(new SimpleRecord[0]));
    }

    private MemoryRecords v2Records(long offset, String... records) {
        List<SimpleRecord> simpleRecords = new ArrayList<>();
        for (String s : records) {
            simpleRecords.add(new SimpleRecord(offset * 10, s.getBytes()));
        }
        return MemoryRecords.withRecords(
                RecordBatch.MAGIC_VALUE_V2, offset,
                Compression.NONE, TimestampType.CREATE_TIME, simpleRecords.toArray(new SimpleRecord[0]));
    }

    @BeforeEach
    public void setup() {
        logDir = TestUtils.tempDirectory();
    }

    @AfterEach
    public void teardown() throws IOException {
        for (LogSegment segment : segments) {
            segment.close();
        }
        Utils.delete(logDir);
    }

    /**
     * LogSegmentOffsetOverflowException should be thrown while appending the logs if:
     * 1. largestOffset - baseOffset < 0
     * 2. largestOffset - baseOffset > Integer.MAX_VALUE
     */
    @ParameterizedTest
    @CsvSource({
        "0, -2147483648",
        "0, 2147483648",
        "1, 0",
        "100, 10",
        "2147483648, 0",
        "-2147483648, 0",
        "2147483648, 4294967296"
    })
    public void testAppendForLogSegmentOffsetOverflowException(long baseOffset, long largestOffset) throws IOException {
        try (LogSegment seg = createSegment(baseOffset, 10, Time.SYSTEM)) {
            long currentTime = Time.SYSTEM.milliseconds();
            MemoryRecords memoryRecords = v1Records(0, "hello");
            assertThrows(LogSegmentOffsetOverflowException.class, () -> seg.append(largestOffset, currentTime, largestOffset, memoryRecords));
        }
    }

    /**
     * A read on an empty log segment should return null
     */
    @Test
    public void testReadOnEmptySegment() throws IOException {
        try (LogSegment seg = createSegment(40)) {
            FetchDataInfo read = seg.read(40, 300);
            assertNull(read, "Read beyond the last offset in the segment should be null");
        }
    }

    /**
     * Reading from before the first offset in the segment should return messages
     * beginning with the first message in the segment
     */
    @Test
    public void testReadBeforeFirstOffset() throws IOException {
        try (LogSegment seg = createSegment(40)) {
            MemoryRecords ms = v1Records(50, "hello", "there", "little", "bee");
            seg.append(53, RecordBatch.NO_TIMESTAMP, -1L, ms);
            Records read = seg.read(41, 300).records;
            checkEquals(ms.records().iterator(), read.records().iterator());
        }
    }

    /**
     * Reading from an offset in the middle of a batch should return a
     * LogOffsetMetadata offset that points to the batch's base offset
     */
    @Test
    public void testReadFromMiddleOfBatch() throws IOException {
        long batchBaseOffset = 50;
        try (LogSegment seg = createSegment(40)) {
            MemoryRecords ms = v2Records(batchBaseOffset, "hello", "there", "little", "bee");
            seg.append(53, RecordBatch.NO_TIMESTAMP, -1L, ms);
            FetchDataInfo readInfo = seg.read(52, 300);
            assertEquals(batchBaseOffset, readInfo.fetchOffsetMetadata.messageOffset);
        }
    }

    /**
     * If we read from an offset beyond the last offset in the segment we should get null
     */
    @Test
    public void testReadAfterLast() throws IOException {
        try (LogSegment seg = createSegment(40)) {
            MemoryRecords ms = v1Records(50, "hello", "there");
            seg.append(51, RecordBatch.NO_TIMESTAMP, -1L, ms);
            FetchDataInfo read = seg.read(52, 200);
            assertNull(read, "Read beyond the last offset in the segment should give null");
        }
    }

    /**
     * If we read from an offset which doesn't exist we should get a message set beginning
     * with the least offset greater than the given startOffset.
     */
    @Test
    public void testReadFromGap() throws IOException {
        try (LogSegment seg = createSegment(40)) {
            MemoryRecords ms = v1Records(50, "hello", "there");
            seg.append(51, RecordBatch.NO_TIMESTAMP, -1L, ms);
            MemoryRecords ms2 = v1Records(60, "alpha", "beta");
            seg.append(61, RecordBatch.NO_TIMESTAMP, -1L, ms2);
            FetchDataInfo read = seg.read(55, 200);
            checkEquals(ms2.records().iterator(), read.records.records().iterator());
        }
    }

    @ParameterizedTest(name = "testReadWhenNoMaxPosition minOneMessage = {0}")
    @ValueSource(booleans = {true, false})
    public void testReadWhenNoMaxPosition(boolean minOneMessage) throws IOException {
        Optional<Long> maxPosition = Optional.empty();
        int maxSize = 1;
        try (LogSegment seg = createSegment(40)) {
            MemoryRecords ms = v1Records(50, "hello", "there");
            seg.append(51, RecordBatch.NO_TIMESTAMP, -1L, ms);

            // read at first offset
            FetchDataInfo read = seg.read(50, maxSize, maxPosition, minOneMessage);
            assertEquals(new LogOffsetMetadata(50, 40, 0), read.fetchOffsetMetadata);
            assertFalse(read.records.records().iterator().hasNext());

            // read at last offset
            read = seg.read(51, maxSize, maxPosition, minOneMessage);
            assertEquals(new LogOffsetMetadata(51, 40, 39), read.fetchOffsetMetadata);
            assertFalse(read.records.records().iterator().hasNext());

            // read at log-end-offset
            read = seg.read(52, maxSize, maxPosition, minOneMessage);
            assertNull(read);

            // read beyond log-end-offset
            read = seg.read(53, maxSize, maxPosition, minOneMessage);
            assertNull(read);
        }
    }

    /**
     * In a loop append two messages then truncate off the second of those messages and check that we can read
     * the first but not the second message.
     */
    @Test
    public void testTruncate() throws IOException {
        try (LogSegment seg = createSegment(40)) {
            long offset = 40;
            for (int i = 0; i < 30; i++) {
                MemoryRecords ms1 = v1Records(offset, "hello");
                seg.append(offset, RecordBatch.NO_TIMESTAMP, -1L, ms1);
                MemoryRecords ms2 = v1Records(offset + 1, "hello");
                seg.append(offset + 1, RecordBatch.NO_TIMESTAMP, -1L, ms2);

                // check that we can read back both messages
                FetchDataInfo read = seg.read(offset, 10000);
                assertIterableEquals(Arrays.asList(ms1.records().iterator().next(), ms2.records().iterator().next()), read.records.records());

                // Now truncate off the last message
                seg.truncateTo(offset + 1);
                FetchDataInfo read2 = seg.read(offset, 10000);
                assertIterableEquals(ms1.records(), read2.records.records());
                offset += 1;
            }
        }
    }

    @Test
    public void testTruncateEmptySegment() throws IOException {
        // This tests the scenario in which the follower truncates to an empty segment. In this
        // case we must ensure that the index is resized so that the log segment is not mistakenly
        // rolled due to a full index

        long maxSegmentMs = 300000;
        MockTime time = new MockTime();
        try (LogSegment seg = createSegment(0L, time)) {
            seg.timeIndex(); // Force load indexes before closing the segment
            seg.offsetIndex();
            seg.close();

            LogSegment reopened = createSegment(0L, time);
            assertEquals(0, seg.timeIndex().sizeInBytes());
            assertEquals(0, seg.offsetIndex().sizeInBytes());

            time.sleep(500);
            reopened.truncateTo(57);
            assertEquals(0, reopened.timeWaitedForRoll(time.milliseconds(), RecordBatch.NO_TIMESTAMP));
            assertFalse(reopened.timeIndex().isFull());
            assertFalse(reopened.offsetIndex().isFull());

            RollParams rollParams = new RollParams(maxSegmentMs, Integer.MAX_VALUE, RecordBatch.NO_TIMESTAMP,
                100L, 1024, time.milliseconds());
            assertFalse(reopened.shouldRoll(rollParams));

            // the segment should not be rolled even if maxSegmentMs has been exceeded
            time.sleep(maxSegmentMs + 1);
            assertEquals(maxSegmentMs + 1, reopened.timeWaitedForRoll(time.milliseconds(), RecordBatch.NO_TIMESTAMP));
            rollParams = new RollParams(maxSegmentMs, Integer.MAX_VALUE, RecordBatch.NO_TIMESTAMP, 100L, 1024, time.milliseconds());
            assertFalse(reopened.shouldRoll(rollParams));

            // but we should still roll the segment if we cannot fit the next offset
            rollParams = new RollParams(maxSegmentMs, Integer.MAX_VALUE, RecordBatch.NO_TIMESTAMP,
                (long) Integer.MAX_VALUE + 200L, 1024, time.milliseconds());
            assertTrue(reopened.shouldRoll(rollParams));
        }
    }

    @Test
    public void testReloadLargestTimestampAndNextOffsetAfterTruncation() throws IOException {
        int numMessages = 30;
        try (LogSegment seg = createSegment(40, 2 * v1Records(0, "hello").sizeInBytes() - 1)) {
            int offset = 40;
            for (int i = 0; i < numMessages; i++) {
                seg.append(offset, offset, offset, v1Records(offset, "hello"));
                offset++;
            }
            assertEquals(offset, seg.readNextOffset());

            int expectedNumEntries = numMessages / 2 - 1;
            assertEquals(expectedNumEntries, seg.timeIndex().entries(), String.format("Should have %d time indexes", expectedNumEntries));

            seg.truncateTo(41);
            assertEquals(0, seg.timeIndex().entries(), "Should have 0 time indexes");
            assertEquals(400L, seg.largestTimestamp(), "Largest timestamp should be 400");
            assertEquals(41, seg.readNextOffset());
        }
    }

    /**
     * Test truncating the whole segment, and check that we can reappend with the original offset.
     */
    @Test
    public void testTruncateFull() throws IOException {
        MockTime time = new MockTime();
        try (LogSegment seg = createSegment(40, time)) {

            seg.append(41, RecordBatch.NO_TIMESTAMP, -1L, v1Records(40, "hello", "there"));

            // If the segment is empty after truncation, the create time should be reset
            time.sleep(500);
            assertEquals(500, seg.timeWaitedForRoll(time.milliseconds(), RecordBatch.NO_TIMESTAMP));

            seg.truncateTo(0);
            assertEquals(0, seg.timeWaitedForRoll(time.milliseconds(), RecordBatch.NO_TIMESTAMP));
            assertFalse(seg.timeIndex().isFull());
            assertFalse(seg.offsetIndex().isFull());
            assertNull(seg.read(0, 1024), "Segment should be empty.");

            seg.append(41, RecordBatch.NO_TIMESTAMP, -1L, v1Records(40, "hello", "there"));
        }
    }

    /**
     * Append messages with timestamp and search message by timestamp.
     */
    @Test
    public void testFindOffsetByTimestamp() throws IOException {
        int messageSize = v1Records(0, "msg00").sizeInBytes();
        try (LogSegment seg = createSegment(40, messageSize * 2 - 1)) {
            // Produce some messages
            for (int i = 40; i < 50; i++) {
                seg.append(i, i * 10, i, v1Records(i, "msg" + i));
            }

            assertEquals(490, seg.largestTimestamp());
            // Search for an indexed timestamp
            assertEquals(42, seg.findOffsetByTimestamp(420, 0L).get().offset);
            assertEquals(43, seg.findOffsetByTimestamp(421, 0L).get().offset);
            // Search for an un-indexed timestamp
            assertEquals(43, seg.findOffsetByTimestamp(430, 0L).get().offset);
            assertEquals(44, seg.findOffsetByTimestamp(431, 0L).get().offset);
            // Search beyond the last timestamp
            assertEquals(Optional.empty(), seg.findOffsetByTimestamp(491, 0L));
            // Search before the first indexed timestamp
            assertEquals(41, seg.findOffsetByTimestamp(401, 0L).get().offset);
            // Search before the first timestamp
            assertEquals(40, seg.findOffsetByTimestamp(399, 0L).get().offset);
        }
    }

    /**
     * Test that offsets are assigned sequentially and that the nextOffset variable is incremented
     */
    @Test
    public void testNextOffsetCalculation() throws IOException {
        try (LogSegment seg = createSegment(40)) {
            assertEquals(40, seg.readNextOffset());
            seg.append(52, RecordBatch.NO_TIMESTAMP, -1L, v1Records(50, "hello", "there", "you"));
            assertEquals(53, seg.readNextOffset());
        }
    }

    /**
     * Test that we can change the file suffixes for the log and index files
     */
    @Test
    public void testChangeFileSuffixes() throws IOException {
        try (LogSegment seg = createSegment(40)) {
            File logFile = seg.log().file();
            File indexFile = seg.offsetIndexFile();
            File timeIndexFile = seg.timeIndexFile();
            // Ensure that files for offset and time indices have not been created eagerly.
            assertFalse(seg.offsetIndexFile().exists());
            assertFalse(seg.timeIndexFile().exists());
            seg.changeFileSuffixes("", ".deleted");
            // Ensure that attempt to change suffixes for non-existing offset and time indices does not create new files.
            assertFalse(seg.offsetIndexFile().exists());
            assertFalse(seg.timeIndexFile().exists());
            // Ensure that file names are updated accordingly.
            assertEquals(logFile.getAbsolutePath() + ".deleted", seg.log().file().getAbsolutePath());
            assertEquals(indexFile.getAbsolutePath() + ".deleted", seg.offsetIndexFile().getAbsolutePath());
            assertEquals(timeIndexFile.getAbsolutePath() + ".deleted", seg.timeIndexFile().getAbsolutePath());
            assertTrue(seg.log().file().exists());
            // Ensure lazy creation of offset index file upon accessing it.
            seg.offsetIndex();
            assertTrue(seg.offsetIndexFile().exists());
            // Ensure lazy creation of time index file upon accessing it.
            seg.timeIndex();
            assertTrue(seg.timeIndexFile().exists());
        }
    }

    /**
     * Create a segment with some data and an index. Then corrupt the index,
     * and recover the segment, the entries should all be readable.
     */
    @Test
    public void testRecoveryFixesCorruptIndex() throws Exception {
        try (LogSegment seg = createSegment(0)) {
            for (int i = 0; i < 100; i++) {
                seg.append(i, RecordBatch.NO_TIMESTAMP, -1L, v1Records(i, Integer.toString(i)));
            }
            File indexFile = seg.offsetIndexFile();
            writeNonsenseToFile(indexFile, 5, (int) indexFile.length());
            seg.recover(newProducerStateManager(), Optional.empty());
            for (int i = 0; i < 100; i++) {
                Iterable<Record> records = seg.read(i, 1, Optional.of((long) seg.size()), true).records.records();
                assertEquals(i, records.iterator().next().offset());
            }
        }
    }

    @Test
    public void testRecoverTransactionIndex() throws Exception {
        try (LogSegment segment = createSegment(100)) {
            short producerEpoch = 0;
            int partitionLeaderEpoch = 15;
            int sequence = 100;

            long pid1 = 5L;
            long pid2 = 10L;

            // append transactional records from pid1
            segment.append(101L, RecordBatch.NO_TIMESTAMP,
                100L, MemoryRecords.withTransactionalRecords(100L, Compression.NONE,
                    pid1, producerEpoch, sequence, partitionLeaderEpoch, new SimpleRecord("a".getBytes()), new SimpleRecord("b".getBytes())));

            // append transactional records from pid2
            segment.append(103L, RecordBatch.NO_TIMESTAMP,
                102L, MemoryRecords.withTransactionalRecords(102L, Compression.NONE,
                    pid2, producerEpoch, sequence, partitionLeaderEpoch, new SimpleRecord("a".getBytes()), new SimpleRecord("b".getBytes())));

            // append non-transactional records
            segment.append(105L, RecordBatch.NO_TIMESTAMP,
                104L, MemoryRecords.withRecords(104L, Compression.NONE,
                    partitionLeaderEpoch, new SimpleRecord("a".getBytes()), new SimpleRecord("b".getBytes())));

            // abort the transaction from pid2
            segment.append(106L, RecordBatch.NO_TIMESTAMP,
                106L, endTxnRecords(ControlRecordType.ABORT, pid2, producerEpoch, 106L));

            // commit the transaction from pid1
            segment.append(107L, RecordBatch.NO_TIMESTAMP,
                107L, endTxnRecords(ControlRecordType.COMMIT, pid1, producerEpoch, 107L));

            ProducerStateManager stateManager = newProducerStateManager();
            segment.recover(stateManager, Optional.empty());
            assertEquals(108L, stateManager.mapEndOffset());

            List<AbortedTxn> abortedTxns = segment.txnIndex().allAbortedTxns();
            assertEquals(1, abortedTxns.size());
            AbortedTxn abortedTxn = abortedTxns.get(0);
            assertEquals(pid2, abortedTxn.producerId());
            assertEquals(102L, abortedTxn.firstOffset());
            assertEquals(106L, abortedTxn.lastOffset());
            assertEquals(100L, abortedTxn.lastStableOffset());

            // recover again, assuming the transaction from pid2 began on a previous segment
            stateManager = newProducerStateManager();
            stateManager.loadProducerEntry(new ProducerStateEntry(pid2, producerEpoch, 0,
                RecordBatch.NO_TIMESTAMP, OptionalLong.of(75L),
                Optional.of(new BatchMetadata(10, 10L, 5, RecordBatch.NO_TIMESTAMP))));
            segment.recover(stateManager, Optional.empty());
            assertEquals(108L, stateManager.mapEndOffset());

            abortedTxns = segment.txnIndex().allAbortedTxns();
            assertEquals(1, abortedTxns.size());
            abortedTxn = abortedTxns.get(0);
            assertEquals(pid2, abortedTxn.producerId());
            assertEquals(75L, abortedTxn.firstOffset());
            assertEquals(106L, abortedTxn.lastOffset());
            assertEquals(100L, abortedTxn.lastStableOffset());
        }
    }

    /**
     * Create a segment with some data, then recover the segment.
     * The epoch cache entries should reflect the segment.
     */
    @Test
    public void testRecoveryRebuildsEpochCache() throws Exception {
        try (LogSegment seg = createSegment(0)) {
            LeaderEpochCheckpointFile checkpoint = new LeaderEpochCheckpointFile(TestUtils.tempFile(), new LogDirFailureChannel(1));

            LeaderEpochFileCache cache = new LeaderEpochFileCache(topicPartition, checkpoint, new MockScheduler(new MockTime()));
            seg.append(105L, RecordBatch.NO_TIMESTAMP, 104L, MemoryRecords.withRecords(104L, Compression.NONE, 0,
                new SimpleRecord("a".getBytes()), new SimpleRecord("b".getBytes())));

            seg.append(107L, RecordBatch.NO_TIMESTAMP, 106L, MemoryRecords.withRecords(106L, Compression.NONE, 1,
                new SimpleRecord("a".getBytes()), new SimpleRecord("b".getBytes())));

            seg.append(109L, RecordBatch.NO_TIMESTAMP, 108L, MemoryRecords.withRecords(108L, Compression.NONE, 1,
                new SimpleRecord("a".getBytes()), new SimpleRecord("b".getBytes())));

            seg.append(111L, RecordBatch.NO_TIMESTAMP, 110L, MemoryRecords.withRecords(110L, Compression.NONE, 2,
                new SimpleRecord("a".getBytes()), new SimpleRecord("b".getBytes())));

            seg.recover(newProducerStateManager(), Optional.of(cache));
            assertEquals(Arrays.asList(
                new EpochEntry(0, 104L),
                new EpochEntry(1, 106L),
                new EpochEntry(2, 110L)), cache.epochEntries());
        }
    }

    private MemoryRecords endTxnRecords(
        ControlRecordType controlRecordType,
        long producerId,
        short producerEpoch,
        long offset) {

        EndTransactionMarker marker = new EndTransactionMarker(controlRecordType, 0);
        return MemoryRecords.withEndTransactionMarker(
            offset,
            RecordBatch.NO_TIMESTAMP,
            0,
            producerId,
            producerEpoch,
            marker
        );
    }

    /**
     * Create a segment with some data and an index. Then corrupt the index,
     * and recover the segment, the entries should all be readable.
     */
    @Test
    public void testRecoveryFixesCorruptTimeIndex() throws IOException {
        try (LogSegment seg = createSegment(0)) {
            for (int i = 0; i < 100; i++) {
                seg.append(i, i * 10, i, v1Records(i, String.valueOf(i)));
            }
            File timeIndexFile = seg.timeIndexFile();
            writeNonsenseToFile(timeIndexFile, 5, (int) timeIndexFile.length());
            seg.recover(newProducerStateManager(), Optional.empty());
            for (int i = 0; i < 100; i++) {
                assertEquals(i, seg.findOffsetByTimestamp(i * 10, 0L).get().offset);
                if (i < 99) {
                    assertEquals(i + 1, seg.findOffsetByTimestamp(i * 10 + 1, 0L).get().offset);
                }
            }
        }
    }

    /**
     * Randomly corrupt a log a number of times and attempt recovery.
     */
    @Test
    public void testRecoveryWithCorruptMessage() throws IOException {
        int messagesAppended = 20;
        for (int ignore = 0; ignore < 10; ignore++) {
            try (LogSegment seg = createSegment(0)) {
                for (int i = 0; i < messagesAppended; i++) {
                    seg.append(i, RecordBatch.NO_TIMESTAMP, -1L, v1Records(i, String.valueOf(i)));
                }
                int offsetToBeginCorruption = TestUtils.RANDOM.nextInt(messagesAppended);
                // start corrupting somewhere in the middle of the chosen record all the way to the end

                FileRecords.LogOffsetPosition recordPosition = seg.log().searchForOffsetWithSize(offsetToBeginCorruption, 0);
                int position = recordPosition.position + TestUtils.RANDOM.nextInt(15);
                writeNonsenseToFile(seg.log().file(), position, (int) (seg.log().file().length() - position));
                seg.recover(newProducerStateManager(), Optional.empty());

                List<Long> expectList = new ArrayList<>();
                for (long j = 0; j < offsetToBeginCorruption; j++) {
                    expectList.add(j);
                }
                List<Long> actualList = new ArrayList<>();
                for (FileLogInputStream.FileChannelRecordBatch batch : seg.log().batches()) {
                    actualList.add(batch.lastOffset());
                }
                assertEquals(expectList, actualList, "Should have truncated off bad messages.");
                seg.deleteIfExists();
            }
        }
    }

    /* create a segment with   pre allocate, put message to it and verify */
    @Test
    public void testCreateWithInitFileSizeAppendMessage() throws IOException {
        File tempDir = TestUtils.tempDirectory();
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(TopicConfig.INDEX_INTERVAL_BYTES_CONFIG, 10);
        configMap.put(TopicConfig.SEGMENT_INDEX_BYTES_CONFIG, 1000);
        configMap.put(TopicConfig.SEGMENT_JITTER_MS_CONFIG, 0);
        LogConfig logConfig = new LogConfig(configMap);
        try (LogSegment seg = LogSegment.open(tempDir, 40, logConfig, Time.SYSTEM, false,
            512 * 1024 * 1024, true, "")) {
            segments.add(seg);
            MemoryRecords ms = v1Records(50, "hello", "there");
            seg.append(51, RecordBatch.NO_TIMESTAMP, -1L, ms);
            MemoryRecords ms2 = v1Records(60, "alpha", "beta");
            seg.append(61, RecordBatch.NO_TIMESTAMP, -1L, ms2);
            FetchDataInfo read = seg.read(55, 200);
            checkEquals(ms2.records().iterator(), read.records.records().iterator());
        }
    }

    /* create a segment with   pre allocate and clearly shut down*/
    @Test
    public void testCreateWithInitFileSizeClearShutdown() throws IOException {
        // Create a temporary directory
        File tempDir = TestUtils.tempDirectory();

        // Set up the log configuration
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(TopicConfig.INDEX_INTERVAL_BYTES_CONFIG, 10);
        configMap.put(TopicConfig.SEGMENT_INDEX_BYTES_CONFIG, 1000);
        configMap.put(TopicConfig.SEGMENT_JITTER_MS_CONFIG, 0);
        LogConfig logConfig = new LogConfig(configMap);

        try (LogSegment seg = LogSegment.open(tempDir, 40, logConfig, Time.SYSTEM, 512 * 1024 * 1024, true)) {
            MemoryRecords ms = v1Records(50, "hello", "there");
            seg.append(51, RecordBatch.NO_TIMESTAMP, -1L, ms);
            MemoryRecords ms2 = v1Records(60, "alpha", "beta");
            seg.append(61, RecordBatch.NO_TIMESTAMP, -1L, ms2);
            FetchDataInfo read = seg.read(55, 200);
            checkEquals(ms2.records().iterator(), read.records.records().iterator());
            long oldSize = seg.log().sizeInBytes();
            long oldPosition = seg.log().channel().position();
            long oldFileSize = seg.log().file().length();
            assertEquals(512 * 1024 * 1024, oldFileSize);
            seg.close();
            // After close, file should be trimmed
            assertEquals(oldSize, seg.log().file().length());

            LogSegment segReopen = LogSegment.open(tempDir, 40, logConfig, Time.SYSTEM,
                true, 512 * 1024 * 1024, true, "");
            segments.add(segReopen);

            FetchDataInfo readAgain = segReopen.read(55, 200);
            checkEquals(ms2.records().iterator(), readAgain.records.records().iterator());
            long size = segReopen.log().sizeInBytes();
            long position = segReopen.log().channel().position();
            long fileSize = segReopen.log().file().length();
            assertEquals(oldPosition, position);
            assertEquals(oldSize, size);
            assertEquals(size, fileSize);
        }
    }

    private MemoryRecords recordsForTruncateEven(long offset, String record) {
        return MemoryRecords.withRecords(RecordBatch.MAGIC_VALUE_V2, offset, Compression.NONE,
            TimestampType.CREATE_TIME, new SimpleRecord(offset * 1000, record.getBytes()));
    }

    @Test
    public void shouldTruncateEvenIfOffsetPointsToAGapInTheLog() throws IOException {
        try (LogSegment seg = createSegment(40)) {
            long offset = 40;

            // Given two messages with a gap between them (e.g. mid offset compacted away)
            MemoryRecords ms1 = recordsForTruncateEven(offset, "first message");
            seg.append(offset, RecordBatch.NO_TIMESTAMP, -1L, ms1);
            MemoryRecords ms2 = recordsForTruncateEven(offset + 3, "message after gap");
            seg.append(offset + 3, RecordBatch.NO_TIMESTAMP, -1L, ms2);

            // When we truncate to an offset without a corresponding log entry
            seg.truncateTo(offset + 1);

            // Then we should still truncate the record that was present (i.e. offset + 3 is gone)
            FetchDataInfo log = seg.read(offset, 10000);
            Iterator<? extends RecordBatch> iter = log.records.batches().iterator();
            assertEquals(offset, iter.next().baseOffset());
            assertFalse(iter.hasNext());
        }
    }

    private MemoryRecords v2RecordWithSize(long offset, int size) {
        return MemoryRecords.withRecords(RecordBatch.MAGIC_VALUE_V2, offset, Compression.NONE, TimestampType.CREATE_TIME,
            new SimpleRecord(new byte[size]));
    }

    @Test
    public void testAppendFromFile() throws IOException {
        // create a log file in a separate directory to avoid conflicting with created segments
        File tempDir = TestUtils.tempDirectory();
        FileRecords fileRecords = FileRecords.open(LogFileUtils.logFile(tempDir, 0));

        // Simulate a scenario with log offset range exceeding Integer.MAX_VALUE
        fileRecords.append(v2RecordWithSize(0, 1024));
        fileRecords.append(v2RecordWithSize(500, 1024 * 1024 + 1));
        long sizeBeforeOverflow = fileRecords.sizeInBytes();
        fileRecords.append(v2RecordWithSize(Integer.MAX_VALUE + 5L, 1024));
        long sizeAfterOverflow = fileRecords.sizeInBytes();

        try (LogSegment segment = createSegment(0)) {
            long bytesAppended = segment.appendFromFile(fileRecords, 0);
            assertEquals(sizeBeforeOverflow, bytesAppended);
            assertEquals(sizeBeforeOverflow, segment.size());
        }

        try (LogSegment overflowSegment = createSegment(Integer.MAX_VALUE)) {
            long overflowBytesAppended = overflowSegment.appendFromFile(fileRecords, (int) sizeBeforeOverflow);
            assertEquals(sizeAfterOverflow - sizeBeforeOverflow, overflowBytesAppended);
            assertEquals(overflowBytesAppended, overflowSegment.size());
        }

        Utils.delete(tempDir);
    }

    @Test
    public void testGetFirstBatchTimestamp() throws IOException {
        try (LogSegment segment = createSegment(1)) {
            assertEquals(Long.MAX_VALUE, segment.getFirstBatchTimestamp());

            segment.append(1, 1000L, 1, MemoryRecords.withRecords(1, Compression.NONE, new SimpleRecord("one".getBytes())));
            assertEquals(1000L, segment.getFirstBatchTimestamp());
        }
    }

    @Test
    public void testDeleteIfExistsWithGetParentIsNull() throws IOException {
        FileRecords log = mock(FileRecords.class);
        @SuppressWarnings("unchecked")
        LazyIndex<OffsetIndex> lazyOffsetIndex = mock(LazyIndex.class);
        @SuppressWarnings("unchecked")
        LazyIndex<TimeIndex> lazyTimeIndex = mock(LazyIndex.class);
        TransactionIndex transactionIndex = mock(TransactionIndex.class);


        // Use Mockito's when().thenReturn() for stubbing
        when(log.deleteIfExists()).thenReturn(true);
        when(lazyOffsetIndex.deleteIfExists()).thenReturn(true);
        when(lazyTimeIndex.deleteIfExists()).thenReturn(true);
        when(transactionIndex.deleteIfExists()).thenReturn(false);

        File mockFile = mock(File.class);
        when(mockFile.getAbsolutePath()).thenReturn("/test/path");
        when(log.file()).thenReturn(mockFile);
        when(lazyOffsetIndex.file()).thenReturn(mockFile);
        when(lazyTimeIndex.file()).thenReturn(mockFile);

        File transactionIndexFile = new File("/");
        when(transactionIndex.file()).thenReturn(transactionIndexFile);

        try (LogSegment segment = new LogSegment(log, lazyOffsetIndex, lazyTimeIndex, transactionIndex, 0, 10, 100, Time.SYSTEM)) {
            assertDoesNotThrow(
                () -> segment.deleteIfExists(),
                "Should not throw exception when transactionIndex.deleteIfExists() returns false");
        }
    }

    private ProducerStateManager newProducerStateManager() throws IOException {
        return new ProducerStateManager(
            topicPartition,
            logDir,
            (int) (Duration.ofMinutes(5).toMillis()),
            new ProducerStateManagerConfig(TransactionLogConfig.PRODUCER_ID_EXPIRATION_MS_DEFAULT, false),
            new MockTime()
        );
    }

    private void checkEquals(Iterator<?> s1, Iterator<?> s2) {
        while (s1.hasNext() && s2.hasNext()) {
            assertEquals(s1.next(), s2.next());
        }
        assertFalse(s1.hasNext(), "Iterators have uneven length--first has more");
        assertFalse(s2.hasNext(), "Iterators have uneven length--second has more");
    }

    private void writeNonsenseToFile(File file, long position, int size) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            raf.seek(position);
            for (int i = 0; i < size; i++) {
                raf.writeByte(TestUtils.RANDOM.nextInt(255));
            }
        }
    }
}
