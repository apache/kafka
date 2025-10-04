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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.common.record.ControlRecordType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.transaction.TransactionLogConfig;
import org.apache.kafka.server.storage.log.FetchIsolation;
import org.apache.kafka.server.util.MockTime;
import org.apache.kafka.server.util.Scheduler;
import org.apache.kafka.storage.internals.epoch.LeaderEpochFileCache;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class UnifiedLogTest {

    private final File tmpDir = TestUtils.tempDirectory();
    private final File logDir = TestUtils.randomPartitionLogDir(tmpDir);
    private final BrokerTopicStats brokerTopicStats = new BrokerTopicStats(false);
    private final MockTime mockTime = new MockTime();
    private final int maxTransactionTimeoutMs = 60 * 60 * 1000;
    private final ProducerStateManagerConfig producerStateManagerConfig = new ProducerStateManagerConfig(maxTransactionTimeoutMs, false);
    private final List<UnifiedLog> logsToClose = new ArrayList<>();

    private UnifiedLog log;

    @AfterEach
    public void tearDown() throws IOException {
        brokerTopicStats.close();
        for (UnifiedLog log : logsToClose) {
            try {
                // some test like testLogDeletionAfterClose and testLogDeletionAfterClose
                // they are closed from test so KafkaStorageException is expected.
                log.close();
            } catch (KafkaStorageException ignore) {
                // ignore
            }
        }
        Utils.delete(tmpDir);
    }

    @Test
    public void testOffsetFromProducerSnapshotFile() {
        long offset = 23423423L;
        File snapshotFile = LogFileUtils.producerSnapshotFile(tmpDir, offset);
        assertEquals(offset, UnifiedLog.offsetFromFile(snapshotFile));
    }

    @Test
    public void shouldApplyEpochToMessageOnAppendIfLeader() throws IOException {
        SimpleRecord[] records = java.util.stream.IntStream.range(0, 50)
            .mapToObj(id -> new SimpleRecord(String.valueOf(id).getBytes()))
            .toArray(SimpleRecord[]::new);

        // Given this partition is on leader epoch 72
        int epoch = 72;
        try (UnifiedLog log = createLog(logDir, new LogConfig(new Properties()))) {
            log.assignEpochStartOffset(epoch, records.length);

            // When appending messages as a leader (i.e. assignOffsets = true)
            for (SimpleRecord record : records) {
                log.appendAsLeader(MemoryRecords.withRecords(Compression.NONE, record), epoch);
            }

            // Then leader epoch should be set on messages
            for (int i = 0; i < records.length; i++) {
                FetchDataInfo read = log.read(i, 1, FetchIsolation.LOG_END, true);
                RecordBatch batch = read.records.batches().iterator().next();
                assertEquals(epoch, batch.partitionLeaderEpoch(), "Should have set leader epoch");
            }
        }
    }

    @Test
    public void followerShouldSaveEpochInformationFromReplicatedMessagesToTheEpochCache() throws IOException {
        int[] messageIds = java.util.stream.IntStream.range(0, 50).toArray();
        SimpleRecord[] records = Arrays.stream(messageIds)
            .mapToObj(id -> new SimpleRecord(String.valueOf(id).getBytes()))
            .toArray(SimpleRecord[]::new);

        //Given each message has an offset & epoch, as msgs from leader would
        Function<Integer, MemoryRecords> recordsForEpoch = i -> {
            MemoryRecords recs = MemoryRecords.withRecords(messageIds[i], Compression.NONE, records[i]);
            recs.batches().forEach(record -> {
                record.setPartitionLeaderEpoch(42);
                record.setLastOffset(i);
            });
            return recs;
        };

        try (UnifiedLog log = createLog(logDir, new LogConfig(new Properties()))) {
            // Given each message has an offset & epoch, as msgs from leader would
            for (int i = 0; i < records.length; i++) {
                log.appendAsFollower(recordsForEpoch.apply(i), i);
            }

            assertEquals(Optional.of(42), log.latestEpoch());
        }
    }

    @Test
    public void shouldTruncateLeaderEpochsWhenDeletingSegments() throws IOException {
        Supplier<MemoryRecords>  records = () -> TestUtils.singletonRecords("test".getBytes());
        LogConfig config = new LogTestUtils.LogConfigBuilder()
                .withSegmentBytes(records.get().sizeInBytes() * 5)
                .withRetentionBytes(records.get().sizeInBytes() * 10L)
                .build();

        log = createLog(logDir, config);
        LeaderEpochFileCache cache = epochCache(log);

        // Given three segments of 5 messages each
        for (int i = 0; i < 15; i++) {
            log.appendAsLeader(records.get(), 0);
        }

        // Given epochs
        cache.assign(0, 0);
        cache.assign(1, 5);
        cache.assign(2, 10);

        // When first segment is removed
        log.updateHighWatermark(log.logEndOffset());
        log.deleteOldSegments();

        //The oldest epoch entry should have been removed
        assertEquals(List.of(new EpochEntry(1, 5), new EpochEntry(2, 10)), cache.epochEntries());
    }

    @Test
    public void shouldUpdateOffsetForLeaderEpochsWhenDeletingSegments() throws IOException {
        Supplier<MemoryRecords> records = () -> TestUtils.singletonRecords("test".getBytes());
        LogConfig config = new LogTestUtils.LogConfigBuilder()
                .withSegmentBytes(records.get().sizeInBytes() * 5)
                .withRetentionBytes(records.get().sizeInBytes() * 10L)
                .build();

        log = createLog(logDir, config);
        LeaderEpochFileCache cache = epochCache(log);

        // Given three segments of 5 messages each
        for (int i = 0; i < 15; i++) {
            log.appendAsLeader(records.get(), 0);
        }

        // Given epochs
        cache.assign(0, 0);
        cache.assign(1, 7);
        cache.assign(2, 10);

        // When first segment removed (up to offset 5)
        log.updateHighWatermark(log.logEndOffset());
        log.deleteOldSegments();

        //The first entry should have gone from (0,0) => (0,5)
        assertEquals(List.of(new EpochEntry(0, 5), new EpochEntry(1, 7), new EpochEntry(2, 10)), cache.epochEntries());
    }

    @Test
    public void shouldTruncateLeaderEpochCheckpointFileWhenTruncatingLog() throws IOException {
        Supplier<MemoryRecords> records = () -> TestUtils.records(List.of(new SimpleRecord("value".getBytes())), 0, 0);
        LogConfig config = new LogTestUtils.LogConfigBuilder()
                .withSegmentBytes(10 * records.get().sizeInBytes())
                .build();
        log = createLog(logDir, config);
        LeaderEpochFileCache cache = epochCache(log);

        //Given 2 segments, 10 messages per segment
        append(0, 0, 10);
        append(1, 10, 6);
        append(2, 16, 4);

        assertEquals(2, log.numberOfSegments());
        assertEquals(20, log.logEndOffset());

        // When truncate to LEO (no op)
        log.truncateTo(log.logEndOffset());
        // Then no change
        assertEquals(3, cache.epochEntries().size());

        // When truncate
        log.truncateTo(11);
        // Then no change
        assertEquals(2, cache.epochEntries().size());

        // When truncate
        log.truncateTo(10);
        assertEquals(1, cache.epochEntries().size());

        // When truncate all
        log.truncateTo(0);
        assertEquals(0, cache.epochEntries().size());
    }

    @Test
    public void shouldDeleteSizeBasedSegments() throws IOException {
        Supplier<MemoryRecords> records = () -> TestUtils.singletonRecords("test".getBytes());
        LogConfig config = new LogTestUtils.LogConfigBuilder()
                .withSegmentBytes(records.get().sizeInBytes() * 5)
                .withRetentionBytes(records.get().sizeInBytes() * 10L)
                .build();
        log = createLog(logDir, config);

        // append some messages to create some segments
        for (int i = 0; i < 15; i++) {
            log.appendAsLeader(records.get(), 0);
        }

        log.updateHighWatermark(log.logEndOffset());
        log.deleteOldSegments();
        assertEquals(2, log.numberOfSegments(), "should have 2 segments");
    }

    @Test
    public void shouldNotDeleteSizeBasedSegmentsWhenUnderRetentionSize() throws IOException {
        Supplier<MemoryRecords> records = () -> TestUtils.singletonRecords("test".getBytes());
        LogConfig config = new LogTestUtils.LogConfigBuilder()
                .withSegmentBytes(records.get().sizeInBytes() * 5)
                .withRetentionBytes(records.get().sizeInBytes() * 15L)
                .build();

        log = createLog(logDir, config);

        // append some messages to create some segments
        for (int i = 0; i < 15; i++) {
            log.appendAsLeader(records.get(), 0);
        }

        log.updateHighWatermark(log.logEndOffset());
        log.deleteOldSegments();
        assertEquals(3, log.numberOfSegments(), "should have 3 segments");
    }

    @Test
    public void shouldDeleteTimeBasedSegmentsReadyToBeDeleted() throws IOException {
        Supplier<MemoryRecords> records = () -> TestUtils.singletonRecords("test".getBytes(), 10L);
        LogConfig config = new LogTestUtils.LogConfigBuilder()
                .withSegmentBytes(records.get().sizeInBytes() * 15)
                .withRetentionMs(10000L)
                .build();
        log = createLog(logDir, config);

        // append some messages to create some segments
        for (int i = 0; i < 15; i++) {
            log.appendAsLeader(records.get(), 0);
        }

        log.updateHighWatermark(log.logEndOffset());
        log.deleteOldSegments();
        assertEquals(1, log.numberOfSegments(), "There should be 1 segment remaining");
    }

    @Test
    public void shouldNotDeleteTimeBasedSegmentsWhenNoneReadyToBeDeleted() throws IOException {
        Supplier<MemoryRecords> records = () -> TestUtils.singletonRecords("test".getBytes(), mockTime.milliseconds());
        LogConfig logConfig = new LogTestUtils.LogConfigBuilder()
                .withSegmentBytes(records.get().sizeInBytes() * 5)
                .withRetentionMs(10000000)
                .build();
        log = createLog(logDir, logConfig);

        // append some messages to create some segments
        for (int i = 0; i < 15; i++) {
            log.appendAsLeader(records.get(), 0);
        }

        log.updateHighWatermark(log.logEndOffset());
        log.deleteOldSegments();
        assertEquals(3, log.numberOfSegments(), "There should be 3 segments remaining");
    }

    @Test
    public void shouldNotDeleteSegmentsWhenPolicyDoesNotIncludeDelete() throws IOException {
        Supplier<MemoryRecords> records = () -> TestUtils.singletonRecords("test".getBytes(), "test".getBytes(), 10L);
        LogConfig config = new LogTestUtils.LogConfigBuilder()
                .withSegmentBytes(records.get().sizeInBytes() * 5)
                .withRetentionMs(10000)
                .withCleanupPolicy("compact")
                .build();
        log = createLog(logDir, config);

        // append some messages to create some segments
        for (int i = 0; i < 15; i++) {
            log.appendAsLeader(records.get(), 0);
        }

        // mark the oldest segment as older the retention.ms
        log.logSegments().iterator().next().setLastModified(mockTime.milliseconds() - 20000);

        int segments = log.numberOfSegments();
        log.updateHighWatermark(log.logEndOffset());
        log.deleteOldSegments();
        assertEquals(segments, log.numberOfSegments(), "There should be 3 segments remaining");
    }

    @Test
    public void shouldDeleteSegmentsReadyToBeDeletedWhenCleanupPolicyIsCompactAndDelete() throws IOException {
        Supplier<MemoryRecords> records = () -> TestUtils.singletonRecords("test".getBytes(), "test".getBytes(), 10L);
        LogConfig config = new LogTestUtils.LogConfigBuilder()
                .withSegmentBytes(records.get().sizeInBytes() * 5)
                .withRetentionBytes(records.get().sizeInBytes() * 10L)
                .withCleanupPolicy("compact, delete")
                .build();

        log = createLog(logDir, config);

        // append some messages to create some segments
        for (int i = 0; i < 15; i++) {
            log.appendAsLeader(records.get(), 0);
        }

        log.updateHighWatermark(log.logEndOffset());
        log.deleteOldSegments();
        assertEquals(1, log.numberOfSegments(), "There should be 1 segment remaining");
    }

    @Test
    public void shouldDeleteLocalLogSegmentsWhenPolicyIsEmptyWithSizeRetention() throws IOException {
        Supplier<MemoryRecords> records = () -> TestUtils.singletonRecords("test".getBytes(), "test".getBytes(), 10L);
        int recordSize = records.get().sizeInBytes();
        LogConfig config = new LogTestUtils.LogConfigBuilder()
                .withSegmentBytes(recordSize * 2)
                .withRetentionBytes(recordSize / 2)
                .withCleanupPolicy("")
                .withRemoteLogStorageEnable(true)
                .build();
        log = createLog(logDir, config, true);

        for (int i = 0; i < 10; i++) {
            log.appendAsLeader(records.get(), 0);
        }

        int segmentsBefore = log.numberOfSegments();
        log.updateHighWatermark(log.logEndOffset());
        log.updateHighestOffsetInRemoteStorage(log.logEndOffset() - 1);
        int deletedSegments = log.deleteOldSegments();

        assertTrue(log.numberOfSegments() < segmentsBefore, "Some segments should be deleted due to size retention");
        assertTrue(deletedSegments > 0, "At least one segment should be deleted");
    }

    @Test
    public void shouldDeleteLocalLogSegmentsWhenPolicyIsEmptyWithMsRetention() throws IOException {
        long oldTimestamp = mockTime.milliseconds() - 20000;
        Supplier<MemoryRecords> oldRecords = () -> TestUtils.singletonRecords("test".getBytes(), "test".getBytes(), oldTimestamp);
        int recordSize = oldRecords.get().sizeInBytes();
        LogConfig logConfig = new LogTestUtils.LogConfigBuilder()
                .withSegmentBytes(recordSize * 2)
                .withLocalRetentionMs(5000)
                .withCleanupPolicy("")
                .withRemoteLogStorageEnable(true)
                .build();
        log = createLog(logDir, logConfig, true);

        for (int i = 0; i < 10; i++) {
            log.appendAsLeader(oldRecords.get(), 0);
        }

        Supplier<MemoryRecords> newRecords = () -> TestUtils.singletonRecords("test".getBytes(), "test".getBytes(), mockTime.milliseconds());
        for (int i = 0; i < 5; i++) {
            log.appendAsLeader(newRecords.get(), 0);
        }

        int segmentsBefore = log.numberOfSegments();

        log.updateHighWatermark(log.logEndOffset());
        log.updateHighestOffsetInRemoteStorage(log.logEndOffset() - 1);
        int deletedSegments = log.deleteOldSegments();

        assertTrue(log.numberOfSegments() < segmentsBefore, "Some segments should be deleted due to time retention");
        assertTrue(deletedSegments > 0, "At least one segment should be deleted");
    }

    @Test
    public void testLogDeletionAfterDeleteRecords() throws IOException {
        Supplier<MemoryRecords> records = () -> TestUtils.singletonRecords("test".getBytes());
        LogConfig logConfig = new LogTestUtils.LogConfigBuilder()
                .withSegmentBytes(records.get().sizeInBytes() * 5)
                .build();
        log = createLog(logDir, logConfig);

        for (int i = 0; i < 15; i++) {
            log.appendAsLeader(records.get(), 0);
        }
        assertEquals(3, log.numberOfSegments());
        assertEquals(0, log.logStartOffset());
        log.updateHighWatermark(log.logEndOffset());

        log.maybeIncrementLogStartOffset(1, LogStartOffsetIncrementReason.ClientRecordDeletion);
        log.deleteOldSegments();
        assertEquals(3, log.numberOfSegments());
        assertEquals(1, log.logStartOffset());

        log.maybeIncrementLogStartOffset(6, LogStartOffsetIncrementReason.ClientRecordDeletion);
        log.deleteOldSegments();
        assertEquals(2, log.numberOfSegments());
        assertEquals(6, log.logStartOffset());

        log.maybeIncrementLogStartOffset(15, LogStartOffsetIncrementReason.ClientRecordDeletion);
        log.deleteOldSegments();
        assertEquals(1, log.numberOfSegments());
        assertEquals(15, log.logStartOffset());
    }

    @Test
    public void testLogDeletionAfterClose() throws IOException {
        Supplier<MemoryRecords> records = () -> TestUtils.singletonRecords("test".getBytes(), mockTime.milliseconds() - 1000);
        LogConfig logConfig = new LogTestUtils.LogConfigBuilder()
                .withSegmentBytes(records.get().sizeInBytes() * 5)
                .withSegmentIndexBytes(1000)
                .withRetentionMs(999)
                .build();
        log = createLog(logDir, logConfig);

        // append some messages to create some segments
        log.appendAsLeader(records.get(), 0);

        assertEquals(1, log.numberOfSegments(), "The deleted segments should be gone.");
        assertEquals(1, epochCache(log).epochEntries().size(), "Epoch entries should have gone.");

        log.close();
        log.delete();
        assertEquals(0, log.numberOfSegments());
        assertEquals(0, epochCache(log).epochEntries().size(), "Epoch entries should have gone.");
    }

    @Test
    public void testDeleteOldSegments() throws IOException {
        Supplier<MemoryRecords> records = () -> TestUtils.singletonRecords("test".getBytes(), mockTime.milliseconds() - 1000);
        LogConfig logConfig = new LogTestUtils.LogConfigBuilder()
                .withSegmentBytes(records.get().sizeInBytes() * 5)
                .withSegmentIndexBytes(1000)
                .withRetentionMs(999)
                .build();
        log = createLog(logDir, logConfig);

        // append some messages to create some segments
        for (int i = 0; i < 100; i++) {
            log.appendAsLeader(records.get(), 0);
        }

        log.assignEpochStartOffset(0, 40);
        log.assignEpochStartOffset(1, 90);

        // segments are not eligible for deletion if no high watermark has been set
        int numSegments = log.numberOfSegments();
        log.deleteOldSegments();
        assertEquals(numSegments, log.numberOfSegments());
        assertEquals(0L, log.logStartOffset());

        // only segments with offset before the current high watermark are eligible for deletion
        for (long hw = 25; hw <= 30; hw++) {
            log.updateHighWatermark(hw);
            log.deleteOldSegments();
            assertTrue(log.logStartOffset() <= hw);
            long finalHw = hw;
            log.logSegments().forEach(segment -> {
                FetchDataInfo segmentFetchInfo;
                try {
                    segmentFetchInfo = segment.read(segment.baseOffset(), Integer.MAX_VALUE);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                Optional<RecordBatch> lastBatch = Optional.empty();
                for (RecordBatch batch : segmentFetchInfo.records.batches()) {
                    lastBatch = Optional.of(batch);
                }
                lastBatch.ifPresent(batch -> assertTrue(batch.lastOffset() >= finalHw));
            });
        }

        log.updateHighWatermark(log.logEndOffset());
        log.deleteOldSegments();
        assertEquals(1, log.numberOfSegments(), "The deleted segments should be gone.");
        assertEquals(1, epochCache(log).epochEntries().size(), "Epoch entries should have gone.");
        assertEquals(new EpochEntry(1, 100), epochCache(log).epochEntries().get(0), "Epoch entry should be the latest epoch and the leo.");

        for (int i = 0; i < 100; i++) {
            log.appendAsLeader(records.get(), 0);
        }

        log.delete();
        assertEquals(0, log.numberOfSegments(), "The number of segments should be 0");
        assertEquals(0, log.deleteOldSegments(), "The number of deleted segments should be zero.");
        assertEquals(0, epochCache(log).epochEntries().size(), "Epoch entries should have gone.");
    }

    @Test
    public void shouldDeleteStartOffsetBreachedSegmentsWhenPolicyDoesNotIncludeDelete() throws IOException {
        Supplier<MemoryRecords> records = () -> TestUtils.singletonRecords("test".getBytes(), "test".getBytes(), 10L);
        int recordsPerSegment = 5;
        LogConfig logConfig = new LogTestUtils.LogConfigBuilder()
                .withSegmentBytes(records.get().sizeInBytes() * recordsPerSegment)
                .withSegmentIndexBytes(1000)
                .withCleanupPolicy("compact")
                .build();
        log = createLog(logDir, logConfig);

        // append some messages to create some segments
        for (int i = 0; i < 15; i++) {
            log.appendAsLeader(records.get(), 0);
        }

        assertEquals(3, log.numberOfSegments());
        log.updateHighWatermark(log.logEndOffset());
        log.maybeIncrementLogStartOffset(recordsPerSegment, LogStartOffsetIncrementReason.ClientRecordDeletion);

        // The first segment, which is entirely before the log start offset, should be deleted
        // Of the remaining the segments, the first can overlap the log start offset and the rest must have a base offset
        // greater than the start offset.
        log.updateHighWatermark(log.logEndOffset());
        log.deleteOldSegments();
        assertEquals(2, log.numberOfSegments(), "There should be 2 segments remaining");
        assertTrue(log.logSegments().iterator().next().baseOffset() <= log.logStartOffset());
        log.logSegments().forEach(segment -> {
            if (log.logSegments().iterator().next() != segment) {
                assertTrue(segment.baseOffset() > log.logStartOffset());
            }
        });
    }

    @Test
    public void testFirstUnstableOffsetNoTransactionalData() throws IOException {
        LogConfig logConfig = new LogTestUtils.LogConfigBuilder()
                .withSegmentBytes(1024 * 1024 * 5)
                .build();
        log = createLog(logDir, logConfig);

        MemoryRecords records = MemoryRecords.withRecords(Compression.NONE,
            new SimpleRecord("foo".getBytes()),
            new SimpleRecord("bar".getBytes()),
            new SimpleRecord("baz".getBytes()));

        log.appendAsLeader(records, 0);
        assertEquals(Optional.empty(), log.firstUnstableOffset());
    }

    @Test
    public void testFirstUnstableOffsetWithTransactionalData() throws IOException {
        LogConfig logConfig = new LogTestUtils.LogConfigBuilder()
                .withSegmentBytes(1024 * 1024 * 5)
                .build();
        log = createLog(logDir, logConfig);

        long pid = 137L;
        short epoch = 5;
        int seq = 0;

        // add some transactional records
        MemoryRecords records = MemoryRecords.withTransactionalRecords(
                Compression.NONE, pid, epoch, seq,
                new SimpleRecord("foo".getBytes()),
                new SimpleRecord("bar".getBytes()),
                new SimpleRecord("baz".getBytes()));

        LogAppendInfo firstAppendInfo = log.appendAsLeader(records, 0);
        assertEquals(Optional.of(firstAppendInfo.firstOffset()), log.firstUnstableOffset());

        // add more transactional records
        seq += 3;
        log.appendAsLeader(MemoryRecords.withTransactionalRecords(Compression.NONE, pid, epoch, seq,
            new SimpleRecord("blah".getBytes())), 0);
        assertEquals(Optional.of(firstAppendInfo.firstOffset()), log.firstUnstableOffset());

        // now transaction is committed
        LogAppendInfo commitAppendInfo = LogTestUtils.appendEndTxnMarkerAsLeader(log, pid, epoch,
                ControlRecordType.COMMIT, mockTime.milliseconds(), 0, 0);

        // first unstable offset is not updated until the high watermark is advanced
        assertEquals(Optional.of(firstAppendInfo.firstOffset()), log.firstUnstableOffset());
        log.updateHighWatermark(commitAppendInfo.lastOffset() + 1);

        // now there should be no first unstable offset
        assertEquals(Optional.empty(), log.firstUnstableOffset());
    }

    private void append(int epoch, long startOffset, int count) {
        Function<Integer, MemoryRecords> records = i ->
                TestUtils.records(List.of(new SimpleRecord("value".getBytes())), startOffset + i, epoch);
        for (int i = 0; i < count; i++) {
            log.appendAsFollower(records.apply(i), epoch);
        }
    }

    private LeaderEpochFileCache epochCache(UnifiedLog log) {
        return log.leaderEpochCache();
    }

    private UnifiedLog createLog(File dir, LogConfig config) throws IOException {
        return createLog(dir, config, false);
    }

    private UnifiedLog createLog(File dir, LogConfig config, boolean remoteStorageSystemEnable) throws IOException {
        return createLog(dir, config, this.brokerTopicStats, mockTime.scheduler, this.mockTime,
                this.producerStateManagerConfig, Optional.empty(), remoteStorageSystemEnable);
    }

    private UnifiedLog createLog(
            File dir,
            LogConfig config,
            BrokerTopicStats brokerTopicStats,
            Scheduler scheduler,
            MockTime time,
            ProducerStateManagerConfig producerStateManagerConfig,
            Optional<Uuid> topicId,
            boolean remoteStorageSystemEnable) throws IOException {

        UnifiedLog log = LogTestUtils.createLog(dir, config, brokerTopicStats, scheduler, time, 0L, 0L,
                3600000, producerStateManagerConfig,
                TransactionLogConfig.PRODUCER_ID_EXPIRATION_CHECK_INTERVAL_MS_DEFAULT, true, topicId,
            new ConcurrentHashMap<>(), remoteStorageSystemEnable, LogOffsetsListener.NO_OP_OFFSETS_LISTENER);

        this.logsToClose.add(log);
        return log;
    }
}
