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

import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.errors.InconsistentTopicIdException;
import org.apache.kafka.common.errors.OffsetOutOfRangeException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.RecordBatchTooLargeException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.message.DescribeProducersResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.record.internal.ControlRecordType;
import org.apache.kafka.common.record.internal.DefaultRecordBatch;
import org.apache.kafka.common.record.internal.FileRecords;
import org.apache.kafka.common.record.internal.InvalidMemoryRecordsProvider;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.MemoryRecordsBuilder;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.record.internal.Records;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.requests.ListOffsetsResponse;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.transaction.TransactionLogConfig;
import org.apache.kafka.server.common.TransactionVersion;
import org.apache.kafka.server.log.remote.storage.NoOpRemoteLogMetadataManager;
import org.apache.kafka.server.log.remote.storage.NoOpRemoteStorageManager;
import org.apache.kafka.server.log.remote.storage.RemoteLogManager;
import org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig;
import org.apache.kafka.server.metrics.KafkaYammerMetrics;
import org.apache.kafka.server.purgatory.DelayedOperationPurgatory;
import org.apache.kafka.server.purgatory.DelayedRemoteListOffsets;
import org.apache.kafka.server.storage.log.FetchIsolation;
import org.apache.kafka.server.storage.log.UnexpectedAppendOffsetException;
import org.apache.kafka.server.util.KafkaScheduler;
import org.apache.kafka.server.util.MockTime;
import org.apache.kafka.server.util.Scheduler;
import org.apache.kafka.storage.internals.epoch.LeaderEpochFileCache;
import org.apache.kafka.storage.log.metrics.BrokerTopicMetrics;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;
import org.apache.kafka.test.TestUtils;

import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Meter;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.Random;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

public class UnifiedLogTest {

    private static final int ONE_MB = 1024 * 1024;
    private static final int TEN_KB = 2048 * 5;
    private static final long ONE_HOUR = 60 * 60L;

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
            Utils.closeQuietly(log, "UnifiedLog");
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
        Supplier<MemoryRecords>  records = () -> singletonRecords("test".getBytes());
        LogConfig config = new LogTestUtils.LogConfigBuilder()
                .segmentBytes(records.get().sizeInBytes() * 5)
                .retentionBytes(records.get().sizeInBytes() * 10L)
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
        assertTrue(log.deleteOldSegments() > 0, "At least one segment should be deleted");

        //The oldest epoch entry should have been removed
        assertEquals(List.of(new EpochEntry(1, 5), new EpochEntry(2, 10)), cache.epochEntries());
    }

    @Test
    public void shouldUpdateOffsetForLeaderEpochsWhenDeletingSegments() throws IOException {
        Supplier<MemoryRecords> records = () -> singletonRecords("test".getBytes());
        LogConfig config = new LogTestUtils.LogConfigBuilder()
                .segmentBytes(records.get().sizeInBytes() * 5)
                .retentionBytes(records.get().sizeInBytes() * 10L)
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
        assertTrue(log.deleteOldSegments() > 0, "At least one segment should be deleted");

        //The first entry should have gone from (0,0) => (0,5)
        assertEquals(List.of(new EpochEntry(0, 5), new EpochEntry(1, 7), new EpochEntry(2, 10)), cache.epochEntries());
    }

    @Test
    public void shouldTruncateLeaderEpochCheckpointFileWhenTruncatingLog() throws IOException {
        Supplier<MemoryRecords> records = () -> records(List.of(new SimpleRecord("value".getBytes())), 0, 0);
        LogConfig config = new LogTestUtils.LogConfigBuilder()
                .segmentBytes(10 * records.get().sizeInBytes())
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
        Supplier<MemoryRecords> records = () -> singletonRecords("test".getBytes());
        LogConfig config = new LogTestUtils.LogConfigBuilder()
                .segmentBytes(records.get().sizeInBytes() * 5)
                .retentionBytes(records.get().sizeInBytes() * 10L)
                .build();
        log = createLog(logDir, config);

        // append some messages to create some segments
        for (int i = 0; i < 15; i++) {
            log.appendAsLeader(records.get(), 0);
        }

        log.updateHighWatermark(log.logEndOffset());
        assertTrue(log.deleteOldSegments() > 0, "At least one segment should be deleted");
        assertEquals(2, log.numberOfSegments(), "should have 2 segments");
    }

    @Test
    public void shouldNotDeleteSizeBasedSegmentsWhenUnderRetentionSize() throws IOException {
        Supplier<MemoryRecords> records = () -> singletonRecords("test".getBytes());
        LogConfig config = new LogTestUtils.LogConfigBuilder()
                .segmentBytes(records.get().sizeInBytes() * 5)
                .retentionBytes(records.get().sizeInBytes() * 15L)
                .build();

        log = createLog(logDir, config);

        // append some messages to create some segments
        for (int i = 0; i < 15; i++) {
            log.appendAsLeader(records.get(), 0);
        }

        log.updateHighWatermark(log.logEndOffset());
        assertEquals(0, log.deleteOldSegments());
        assertEquals(3, log.numberOfSegments(), "should have 3 segments");
    }

    @Test
    public void shouldDeleteTimeBasedSegmentsReadyToBeDeleted() throws IOException {
        Supplier<MemoryRecords> records = () -> singletonRecords("test".getBytes(), 10L);
        LogConfig config = new LogTestUtils.LogConfigBuilder()
                .segmentBytes(records.get().sizeInBytes() * 15)
                .retentionMs(10000L)
                .build();
        log = createLog(logDir, config);

        // append some messages to create some segments
        for (int i = 0; i < 15; i++) {
            log.appendAsLeader(records.get(), 0);
        }

        log.updateHighWatermark(log.logEndOffset());
        assertTrue(log.deleteOldSegments() > 0, "At least one segment should be deleted");
        assertEquals(1, log.numberOfSegments(), "There should be 1 segment remaining");
    }

    @Test
    public void shouldNotDeleteTimeBasedSegmentsWhenNoneReadyToBeDeleted() throws IOException {
        Supplier<MemoryRecords> records = () -> singletonRecords("test".getBytes(), mockTime.milliseconds());
        LogConfig logConfig = new LogTestUtils.LogConfigBuilder()
                .segmentBytes(records.get().sizeInBytes() * 5)
                .retentionMs(10000000)
                .build();
        log = createLog(logDir, logConfig);

        // append some messages to create some segments
        for (int i = 0; i < 15; i++) {
            log.appendAsLeader(records.get(), 0);
        }

        log.updateHighWatermark(log.logEndOffset());
        assertEquals(0, log.deleteOldSegments());
        assertEquals(3, log.numberOfSegments(), "There should be 3 segments remaining");
    }

    @Test
    public void shouldNotDeleteSegmentsWhenPolicyDoesNotIncludeDelete() throws IOException {
        Supplier<MemoryRecords> records = () -> singletonRecords("test".getBytes(), "test".getBytes(), 10L);
        LogConfig config = new LogTestUtils.LogConfigBuilder()
                .segmentBytes(records.get().sizeInBytes() * 5)
                .retentionMs(10000)
                .cleanupPolicy("compact")
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
        assertEquals(0, log.deleteOldSegments());
        assertEquals(segments, log.numberOfSegments(), "There should be 3 segments remaining");
    }

    @Test
    public void shouldDeleteSegmentsReadyToBeDeletedWhenCleanupPolicyIsCompactAndDelete() throws IOException {
        Supplier<MemoryRecords> records = () -> singletonRecords("test".getBytes(), "test".getBytes(), 10L);
        LogConfig config = new LogTestUtils.LogConfigBuilder()
                .segmentBytes(records.get().sizeInBytes() * 5)
                .retentionBytes(records.get().sizeInBytes() * 10L)
                .cleanupPolicy("compact, delete")
                .build();

        log = createLog(logDir, config);

        // append some messages to create some segments
        for (int i = 0; i < 15; i++) {
            log.appendAsLeader(records.get(), 0);
        }

        log.updateHighWatermark(log.logEndOffset());
        assertTrue(log.deleteOldSegments() > 0, "At least one segment should be deleted");
        assertEquals(1, log.numberOfSegments(), "There should be 1 segment remaining");
    }

    @Test
    public void shouldDeleteLocalLogSegmentsWhenPolicyIsEmptyWithSizeRetention() throws IOException {
        Supplier<MemoryRecords> records = () -> singletonRecords("test".getBytes(), "test".getBytes(), 10L);
        int recordSize = records.get().sizeInBytes();
        LogConfig config = new LogTestUtils.LogConfigBuilder()
                .segmentBytes(recordSize * 2)
                .retentionBytes(recordSize / 2)
                .cleanupPolicy("")
                .remoteLogStorageEnable(true)
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
        Supplier<MemoryRecords> oldRecords = () -> singletonRecords("test".getBytes(), "test".getBytes(), oldTimestamp);
        int recordSize = oldRecords.get().sizeInBytes();
        LogConfig logConfig = new LogTestUtils.LogConfigBuilder()
                .segmentBytes(recordSize * 2)
                .localRetentionMs(5000)
                .cleanupPolicy("")
                .remoteLogStorageEnable(true)
                .build();
        log = createLog(logDir, logConfig, true);

        for (int i = 0; i < 10; i++) {
            log.appendAsLeader(oldRecords.get(), 0);
        }

        Supplier<MemoryRecords> newRecords = () -> singletonRecords("test".getBytes(), "test".getBytes(), mockTime.milliseconds());
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
        Supplier<MemoryRecords> records = () -> singletonRecords("test".getBytes());
        LogConfig logConfig = new LogTestUtils.LogConfigBuilder()
                .segmentBytes(records.get().sizeInBytes() * 5)
                .build();
        log = createLog(logDir, logConfig);

        for (int i = 0; i < 15; i++) {
            log.appendAsLeader(records.get(), 0);
        }
        assertEquals(3, log.numberOfSegments());
        assertEquals(0, log.logStartOffset());
        log.updateHighWatermark(log.logEndOffset());

        // The logStartOffset at the first segment so we did not delete it.
        log.maybeIncrementLogStartOffset(1, LogStartOffsetIncrementReason.ClientRecordDeletion);
        assertEquals(0, log.deleteOldSegments());
        assertEquals(3, log.numberOfSegments());
        assertEquals(1, log.logStartOffset());

        log.maybeIncrementLogStartOffset(6, LogStartOffsetIncrementReason.ClientRecordDeletion);
        assertTrue(log.deleteOldSegments() > 0, "At least one segment should be deleted");
        assertEquals(2, log.numberOfSegments());
        assertEquals(6, log.logStartOffset());

        log.maybeIncrementLogStartOffset(15, LogStartOffsetIncrementReason.ClientRecordDeletion);
        assertTrue(log.deleteOldSegments() > 0, "At least one segment should be deleted");
        assertEquals(1, log.numberOfSegments());
        assertEquals(15, log.logStartOffset());
    }

    @Test
    public void testLogDeletionAfterClose() throws IOException {
        Supplier<MemoryRecords> records = () -> singletonRecords("test".getBytes(), mockTime.milliseconds() - 1000);
        LogConfig logConfig = new LogTestUtils.LogConfigBuilder()
                .segmentBytes(records.get().sizeInBytes() * 5)
                .segmentIndexBytes(1000)
                .retentionMs(999)
                .build();
        log = createLog(logDir, logConfig);
        // avoid close after test because it is closed in this test
        logsToClose.remove(log);

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
        Supplier<MemoryRecords> records = () -> singletonRecords("test".getBytes(), mockTime.milliseconds() - 1000);
        LogConfig logConfig = new LogTestUtils.LogConfigBuilder()
                .segmentBytes(records.get().sizeInBytes() * 5)
                .segmentIndexBytes(1000)
                .retentionMs(999)
                .build();
        log = createLog(logDir, logConfig);
        // avoid close after test because it is closed in this test
        logsToClose.remove(log);

        // append some messages to create some segments
        for (int i = 0; i < 100; i++) {
            log.appendAsLeader(records.get(), 0);
        }

        log.assignEpochStartOffset(0, 40);
        log.assignEpochStartOffset(1, 90);

        // segments are not eligible for deletion if no high watermark has been set
        int numSegments = log.numberOfSegments();
        assertEquals(0, log.deleteOldSegments());
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
        assertTrue(log.deleteOldSegments() > 0, "At least one segment should be deleted");
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
        Supplier<MemoryRecords> records = () -> singletonRecords("test".getBytes(), "test".getBytes(), 10L);
        int recordsPerSegment = 5;
        LogConfig logConfig = new LogTestUtils.LogConfigBuilder()
                .segmentBytes(records.get().sizeInBytes() * recordsPerSegment)
                .segmentIndexBytes(1000)
                .cleanupPolicy("compact")
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
        assertTrue(log.deleteOldSegments() > 0, "At least one segment should be deleted");
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
                .segmentBytes(5 * ONE_MB)
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
                .segmentBytes(5 * ONE_MB)
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
                ControlRecordType.COMMIT, mockTime.milliseconds(), 0, 0, TransactionVersion.TV_0.featureLevel());

        // first unstable offset is not updated until the high watermark is advanced
        assertEquals(Optional.of(firstAppendInfo.firstOffset()), log.firstUnstableOffset());
        log.updateHighWatermark(commitAppendInfo.lastOffset() + 1);

        // now there should be no first unstable offset
        assertEquals(Optional.empty(), log.firstUnstableOffset());
    }

    @Test
    public void testHighWatermarkMetadataUpdatedAfterSegmentRoll() throws IOException {
        LogConfig logConfig = new LogTestUtils.LogConfigBuilder().segmentBytes(ONE_MB).build();
        UnifiedLog log = createLog(logDir, logConfig);

        MemoryRecords records = LogTestUtils.records(List.of(
                new SimpleRecord(mockTime.milliseconds(), "a".getBytes(), "value".getBytes()),
                new SimpleRecord(mockTime.milliseconds(), "b".getBytes(), "value".getBytes()),
                new SimpleRecord(mockTime.milliseconds(), "c".getBytes(), "value".getBytes())
        ));

        log.appendAsLeader(records, 0);
        assertFetchSizeAndOffsets(log, 0L, 0, List.of());

        log.maybeIncrementHighWatermark(log.logEndOffsetMetadata());
        assertFetchSizeAndOffsets(log, 0L, records.sizeInBytes(), List.of(0L, 1L, 2L));

        log.roll();
        assertFetchSizeAndOffsets(log, 0L, records.sizeInBytes(), List.of(0L, 1L, 2L));

        log.appendAsLeader(records, 0);
        assertFetchSizeAndOffsets(log, 3L, 0, List.of());
    }

    private void assertFetchSizeAndOffsets(UnifiedLog log, long fetchOffset, int expectedSize, List<Long> expectedOffsets) throws IOException {
        FetchDataInfo readInfo = log.read(
                fetchOffset,
                2048,
                FetchIsolation.HIGH_WATERMARK,
                false);
        assertEquals(expectedSize, readInfo.records.sizeInBytes());
        List<Long> actualOffsets = new ArrayList<>();
        readInfo.records.records().forEach(record -> actualOffsets.add(record.offset()));
        assertEquals(expectedOffsets, actualOffsets);
    }

    @Test
    public void testAppendAsLeaderWithRaftLeader() throws IOException {
        LogConfig logConfig = new LogTestUtils.LogConfigBuilder().segmentBytes(ONE_MB).build();
        UnifiedLog log = createLog(logDir, logConfig);
        int leaderEpoch = 0;

        Function<Long, MemoryRecords> records = offset -> LogTestUtils.records(List.of(
                new SimpleRecord(mockTime.milliseconds(), "a".getBytes(), "value".getBytes()),
                new SimpleRecord(mockTime.milliseconds(), "b".getBytes(), "value".getBytes()),
                new SimpleRecord(mockTime.milliseconds(), "c".getBytes(), "value".getBytes())
        ), RecordBatch.CURRENT_MAGIC_VALUE, Compression.NONE, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE, offset, leaderEpoch);

        log.appendAsLeader(records.apply(0L), leaderEpoch, AppendOrigin.RAFT_LEADER);
        assertEquals(0, log.logStartOffset());
        assertEquals(3L, log.logEndOffset());

        // Since raft leader is responsible for assigning offsets, and the LogValidator is bypassed from the performance perspective,
        // so the first offset of the MemoryRecords to be appended should equal to the next offset in the log
        assertThrows(UnexpectedAppendOffsetException.class, () -> log.appendAsLeader(records.apply(1L), leaderEpoch, AppendOrigin.RAFT_LEADER));

        // When the first offset of the MemoryRecords to be appended equals to the next offset in the log, append will succeed
        log.appendAsLeader(records.apply(3L), leaderEpoch, AppendOrigin.RAFT_LEADER);
        assertEquals(6, log.logEndOffset());
    }

    @Test
    public void testAppendInfoFirstOffset() throws IOException {
        LogConfig logConfig = new LogTestUtils.LogConfigBuilder().segmentBytes(ONE_MB).build();
        UnifiedLog log = createLog(logDir, logConfig);

        List<SimpleRecord> simpleRecords = List.of(
                new SimpleRecord(mockTime.milliseconds(), "a".getBytes(), "value".getBytes()),
                new SimpleRecord(mockTime.milliseconds(), "b".getBytes(), "value".getBytes()),
                new SimpleRecord(mockTime.milliseconds(), "c".getBytes(), "value".getBytes())
        );

        MemoryRecords records = LogTestUtils.records(simpleRecords);

        LogAppendInfo firstAppendInfo = log.appendAsLeader(records, 0);
        assertEquals(0, firstAppendInfo.firstOffset());

        LogAppendInfo secondAppendInfo = log.appendAsLeader(
                LogTestUtils.records(simpleRecords),
                0
        );
        assertEquals(simpleRecords.size(), secondAppendInfo.firstOffset());

        log.roll();
        LogAppendInfo afterRollAppendInfo =  log.appendAsLeader(LogTestUtils.records(simpleRecords), 0);
        assertEquals(simpleRecords.size() * 2, afterRollAppendInfo.firstOffset());
    }

    @Test
    public void testTruncateBelowFirstUnstableOffset() throws IOException {
        testTruncateBelowFirstUnstableOffset(UnifiedLog::truncateTo);
    }

    @Test
    public void testTruncateFullyAndStartBelowFirstUnstableOffset() throws IOException {
        testTruncateBelowFirstUnstableOffset((log, targetOffset) -> log.truncateFullyAndStartAt(targetOffset, Optional.empty()));
    }

    @Test
    public void testTruncateFullyAndStart() throws IOException {
        LogConfig logConfig = new LogTestUtils.LogConfigBuilder().segmentBytes(ONE_MB).build();
        UnifiedLog log = createLog(logDir, logConfig);
        long producerId = 17L;
        short producerEpoch = 10;
        int sequence = 0;

        log.appendAsLeader(LogTestUtils.records(List.of(
                new SimpleRecord("0".getBytes()),
                new SimpleRecord("1".getBytes()),
                new SimpleRecord("2".getBytes())
        )), 0);
        log.appendAsLeader(MemoryRecords.withTransactionalRecords(
                Compression.NONE,
                producerId,
                producerEpoch,
                sequence,
                new SimpleRecord("3".getBytes()),
                new SimpleRecord("4".getBytes())
        ), 0);
        assertEquals(Optional.of(3L), log.firstUnstableOffset());

        // We close and reopen the log to ensure that the first unstable offset segment
        // position will be undefined when we truncate the log.
        log.close();

        UnifiedLog reopened = createLog(logDir, logConfig);
        assertEquals(Optional.of(new LogOffsetMetadata(3L)), reopened.producerStateManager().firstUnstableOffset());

        reopened.truncateFullyAndStartAt(2L, Optional.of(1L));
        assertEquals(Optional.empty(), reopened.firstUnstableOffset());
        assertEquals(Map.of(), reopened.producerStateManager().activeProducers());
        assertEquals(1L, reopened.logStartOffset());
        assertEquals(2L, reopened.logEndOffset());
    }

    private void testTruncateBelowFirstUnstableOffset(BiConsumer<UnifiedLog, Long> truncateFunc) throws IOException {
        // Verify that truncation below the first unstable offset correctly
        // resets the producer state. Specifically we are testing the case when
        // the segment position of the first unstable offset is unknown.
        LogConfig logConfig = new LogTestUtils.LogConfigBuilder().segmentBytes(ONE_MB).build();
        UnifiedLog log = createLog(logDir, logConfig);
        long producerId = 17L;
        short producerEpoch = 10;
        int sequence = 0;

        log.appendAsLeader(LogTestUtils.records(List.of(
                new SimpleRecord("0".getBytes()),
                new SimpleRecord("1".getBytes()),
                new SimpleRecord("2".getBytes())
        )), 0);
        log.appendAsLeader(MemoryRecords.withTransactionalRecords(
                Compression.NONE,
                producerId,
                producerEpoch,
                sequence,
                new SimpleRecord("3".getBytes()),
                new SimpleRecord("4".getBytes())
        ), 0);
        assertEquals(Optional.of(3L), log.firstUnstableOffset());

        // We close and reopen the log to ensure that the first unstable offset segment
        // position will be undefined when we truncate the log.
        log.close();

        UnifiedLog reopened = createLog(logDir, logConfig);
        assertEquals(Optional.of(new LogOffsetMetadata(3L)), reopened.producerStateManager().firstUnstableOffset());

        truncateFunc.accept(reopened, 0L);
        assertEquals(Optional.empty(), reopened.firstUnstableOffset());
        assertEquals(Map.of(), reopened.producerStateManager().activeProducers());
    }

    @Test
    public void testHighWatermarkMaintenance() throws IOException {
        LogConfig logConfig = new LogTestUtils.LogConfigBuilder().segmentBytes(ONE_MB).build();
        UnifiedLog log = createLog(logDir, logConfig);
        int leaderEpoch = 0;

        Function<Long, MemoryRecords> records = offset -> LogTestUtils.records(List.of(
                new SimpleRecord(mockTime.milliseconds(), "a".getBytes(), "value".getBytes()),
                new SimpleRecord(mockTime.milliseconds(), "b".getBytes(), "value".getBytes()),
                new SimpleRecord(mockTime.milliseconds(), "c".getBytes(), "value".getBytes())
        ), RecordBatch.CURRENT_MAGIC_VALUE, Compression.NONE, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE, offset, leaderEpoch);

        // High watermark initialized to 0
        assertHighWatermark(log, 0L);

        // High watermark not changed by append
        log.appendAsLeader(records.apply(0L), leaderEpoch);
        assertHighWatermark(log, 0L);

        // Update high watermark as leader
        log.maybeIncrementHighWatermark(new LogOffsetMetadata(1L));
        assertHighWatermark(log, 1L);

        // Cannot update past the log end offset
        log.updateHighWatermark(5L);
        assertHighWatermark(log, 3L);

        // Update high watermark as follower
        log.appendAsFollower(records.apply(3L), leaderEpoch);
        log.updateHighWatermark(6L);
        assertHighWatermark(log, 6L);

        // High watermark should be adjusted by truncation
        log.truncateTo(3L);
        assertHighWatermark(log, 3L);

        log.appendAsLeader(records.apply(0L), 0);
        assertHighWatermark(log, 3L);
        assertEquals(6L, log.logEndOffset());
        assertEquals(0L, log.logStartOffset());

        // Full truncation should also reset high watermark
        log.truncateFullyAndStartAt(4L, Optional.empty());
        assertEquals(4L, log.logEndOffset());
        assertEquals(4L, log.logStartOffset());
        assertHighWatermark(log, 4L);
    }

    private void assertHighWatermark(UnifiedLog log, long offset) throws IOException {
        assertEquals(offset, log.highWatermark());
        assertValidLogOffsetMetadata(log, log.fetchOffsetSnapshot().highWatermark());
    }

    private void assertNonEmptyFetch(UnifiedLog log, long offset, FetchIsolation isolation, long batchBaseOffset) throws IOException {
        FetchDataInfo readInfo = log.read(offset, Integer.MAX_VALUE, isolation, true);

        assertFalse(readInfo.firstEntryIncomplete);
        assertTrue(readInfo.records.sizeInBytes() > 0);

        long upperBoundOffset = switch (isolation) {
            case LOG_END -> log.logEndOffset();
            case HIGH_WATERMARK -> log.highWatermark();
            case TXN_COMMITTED -> log.lastStableOffset();
        };

        for (Record record : readInfo.records.records())
            assertTrue(record.offset() < upperBoundOffset);

        assertEquals(batchBaseOffset, readInfo.fetchOffsetMetadata.messageOffset);
        assertValidLogOffsetMetadata(log, readInfo.fetchOffsetMetadata);
    }

    private void assertEmptyFetch(UnifiedLog log, long offset, FetchIsolation isolation, long batchBaseOffset) throws IOException {
        FetchDataInfo readInfo = log.read(offset, Integer.MAX_VALUE, isolation, true);
        assertFalse(readInfo.firstEntryIncomplete);
        assertEquals(0, readInfo.records.sizeInBytes());
        assertEquals(batchBaseOffset, readInfo.fetchOffsetMetadata.messageOffset);
        assertValidLogOffsetMetadata(log, readInfo.fetchOffsetMetadata);
    }

    @Test
    public void testFetchUpToLogEndOffset() throws IOException {
        LogConfig logConfig = new LogTestUtils.LogConfigBuilder().segmentBytes(ONE_MB).build();
        UnifiedLog log = createLog(logDir, logConfig);

        log.appendAsLeader(LogTestUtils.records(List.of(
                new SimpleRecord("0".getBytes()),
                new SimpleRecord("1".getBytes()),
                new SimpleRecord("2".getBytes())
        )), 0);
        log.appendAsLeader(LogTestUtils.records(List.of(
                new SimpleRecord("3".getBytes()),
                new SimpleRecord("4".getBytes())
        )), 0);
        TreeSet<Long> batchBaseOffsets = new TreeSet<>(List.of(0L, 3L, 5L));

        for (long offset = log.logStartOffset(); offset < log.logEndOffset(); offset++) {
            Long batchBaseOffset = batchBaseOffsets.floor(offset);
            assertNotNull(batchBaseOffset);
            assertNonEmptyFetch(log, offset, FetchIsolation.LOG_END, batchBaseOffset);
        }
    }

    @Test
    public void testFetchUpToHighWatermark() throws IOException {
        LogConfig logConfig = new LogTestUtils.LogConfigBuilder().segmentBytes(ONE_MB).build();
        UnifiedLog log = createLog(logDir, logConfig);

        log.appendAsLeader(LogTestUtils.records(List.of(
                new SimpleRecord("0".getBytes()),
                new SimpleRecord("1".getBytes()),
                new SimpleRecord("2".getBytes())
        )), 0);
        log.appendAsLeader(LogTestUtils.records(List.of(
                new SimpleRecord("3".getBytes()),
                new SimpleRecord("4".getBytes())
        )), 0);
        TreeSet<Long> batchBaseOffsets = new TreeSet<>(List.of(0L, 3L, 5L));

        assertHighWatermarkBoundedFetches(log, batchBaseOffsets);

        log.updateHighWatermark(3L);
        assertHighWatermarkBoundedFetches(log, batchBaseOffsets);

        log.updateHighWatermark(5L);
        assertHighWatermarkBoundedFetches(log, batchBaseOffsets);
    }

    private void assertHighWatermarkBoundedFetches(UnifiedLog log, TreeSet<Long> batchBaseOffsets) throws IOException {
        for (long offset = log.logStartOffset(); offset < log.highWatermark(); offset++) {
            Long batchBaseOffset = batchBaseOffsets.floor(offset);
            assertNotNull(batchBaseOffset);
            assertNonEmptyFetch(log, offset, FetchIsolation.HIGH_WATERMARK, batchBaseOffset);
        }

        for (long offset = log.highWatermark(); offset <= log.logEndOffset(); offset++) {
            Long batchBaseOffset = batchBaseOffsets.floor(offset);
            assertNotNull(batchBaseOffset);
            assertEmptyFetch(log, offset, FetchIsolation.HIGH_WATERMARK, batchBaseOffset);
        }
    }

    @Test
    public void testActiveProducers() throws IOException {
        LogConfig logConfig = new LogTestUtils.LogConfigBuilder().segmentBytes(ONE_MB).build();
        UnifiedLog log = createLog(logDir, logConfig);

        // Test transactional producer state (open transaction)
        short producer1Epoch = 5;
        long producerId1 = 1L;
        LogTestUtils.appendTransactionalAsLeader(log, producerId1, producer1Epoch, mockTime).accept(5);
        assertProducerState(
                log,
                producerId1,
                producer1Epoch,
                4,
                Optional.of(0L),
                Optional.empty()
        );

        // Test transactional producer state (closed transaction)
        int coordinatorEpoch = 15;
        LogTestUtils.appendEndTxnMarkerAsLeader(log, producerId1, producer1Epoch, ControlRecordType.COMMIT,
                mockTime.milliseconds(), coordinatorEpoch, 0, TransactionVersion.TV_0.featureLevel());
        assertProducerState(
                log,
                producerId1,
                producer1Epoch,
                4,
                Optional.empty(),
                Optional.of(coordinatorEpoch)
        );

        // Test idempotent producer state
        short producer2Epoch = 5;
        long producerId2 = 2L;
        LogTestUtils.appendIdempotentAsLeader(log, producerId2, producer2Epoch, mockTime, false).accept(3);
        assertProducerState(
                log,
                producerId2,
                producer2Epoch,
                2,
                Optional.empty(),
                Optional.empty()
        );
    }

    private void assertProducerState(
            UnifiedLog log,
            long producerId,
            short producerEpoch,
            int lastSequence,
            Optional<Long> currentTxnStartOffset,
            Optional<Integer> coordinatorEpoch
    ) {
        Optional<DescribeProducersResponseData.ProducerState> producerStateOpt = log.activeProducers().stream().filter(p -> p.producerId() == producerId).findFirst();
        assertTrue(producerStateOpt.isPresent());

        DescribeProducersResponseData.ProducerState producerState = producerStateOpt.get();
        assertEquals(producerEpoch, producerState.producerEpoch());
        assertEquals(lastSequence, producerState.lastSequence());
        assertEquals(currentTxnStartOffset.orElse(-1L), producerState.currentTxnStartOffset());
        assertEquals(coordinatorEpoch.orElse(-1), producerState.coordinatorEpoch());
    }

    @Test
    public void testFetchUpToLastStableOffset() throws IOException {
        LogConfig logConfig = new LogTestUtils.LogConfigBuilder().segmentBytes(ONE_MB).build();
        UnifiedLog log = createLog(logDir, logConfig);
        short epoch = 0;

        long producerId1 = 1L;
        long producerId2 = 2L;

        Consumer<Integer> appendProducer1 = LogTestUtils.appendTransactionalAsLeader(log, producerId1, epoch, mockTime);
        Consumer<Integer> appendProducer2 = LogTestUtils.appendTransactionalAsLeader(log, producerId2, epoch, mockTime);

        appendProducer1.accept(5);
        LogTestUtils.appendNonTransactionalAsLeader(log, 3);
        appendProducer2.accept(2);
        appendProducer1.accept(4);
        LogTestUtils.appendNonTransactionalAsLeader(log, 2);
        appendProducer1.accept(10);

        TreeSet<Long> batchBaseOffsets = new TreeSet<>(List.of(0L, 5L, 8L, 10L, 14L, 16L, 26L, 27L, 28L));

        assertLsoBoundedFetches(log, batchBaseOffsets);

        log.updateHighWatermark(log.logEndOffset());
        assertLsoBoundedFetches(log, batchBaseOffsets);

        LogTestUtils.appendEndTxnMarkerAsLeader(log, producerId1, epoch, ControlRecordType.COMMIT, mockTime.milliseconds(),
                0, 0, TransactionVersion.TV_0.featureLevel());
        assertEquals(0L, log.lastStableOffset());

        log.updateHighWatermark(log.logEndOffset());
        assertEquals(8L, log.lastStableOffset());
        assertLsoBoundedFetches(log, batchBaseOffsets);

        LogTestUtils.appendEndTxnMarkerAsLeader(log, producerId2, epoch, ControlRecordType.ABORT, mockTime.milliseconds(),
                0, 0, TransactionVersion.TV_0.featureLevel());
        assertEquals(8L, log.lastStableOffset());

        log.updateHighWatermark(log.logEndOffset());
        assertEquals(log.logEndOffset(), log.lastStableOffset());
        assertLsoBoundedFetches(log, batchBaseOffsets);
    }

    private void assertLsoBoundedFetches(UnifiedLog log, TreeSet<Long> batchBaseOffsets) throws IOException {
        for (long offset = log.logStartOffset(); offset < log.lastStableOffset(); offset++) {
            Long batchBaseOffset = batchBaseOffsets.floor(offset);
            assertNotNull(batchBaseOffset);
            assertNonEmptyFetch(log, offset, FetchIsolation.TXN_COMMITTED, batchBaseOffset);
        }

        for (long offset = log.lastStableOffset(); offset <= log.logEndOffset(); offset++) {
            Long batchBaseOffset = batchBaseOffsets.floor(offset);
            assertNotNull(batchBaseOffset);
            assertEmptyFetch(log, offset, FetchIsolation.TXN_COMMITTED, batchBaseOffset);
        }
    }

    /**
     * Tests for time based log roll. This test appends messages then changes the time
     * using the mock clock to force the log to roll and checks the number of segments.
     */
    @Test
    public void testTimeBasedLogRollDuringAppend() throws IOException {
        Supplier<MemoryRecords> createRecords = () -> LogTestUtils.records(List.of(new SimpleRecord("test".getBytes())));
        LogConfig logConfig = new LogTestUtils.LogConfigBuilder().segmentMs(ONE_HOUR).build();

        // create a log
        UnifiedLog log = createLog(logDir, logConfig, 0L, 0L, brokerTopicStats, mockTime.scheduler, mockTime,
                new ProducerStateManagerConfig(24 * 60, false), true, Optional.empty(), false);
        assertEquals(1, log.numberOfSegments(), "Log begins with a single empty segment.");
        // Test the segment rolling behavior when messages do not have a timestamp.
        mockTime.sleep(log.config().segmentMs + 1);
        log.appendAsLeader(createRecords.get(), 0);
        assertEquals(1, log.numberOfSegments(), "Log doesn't roll if doing so creates an empty segment.");

        log.appendAsLeader(createRecords.get(), 0);
        assertEquals(2, log.numberOfSegments(), "Log rolls on this append since time has expired.");

        for (int numSegments = 3; numSegments < 5; numSegments++) {
            mockTime.sleep(log.config().segmentMs + 1);
            log.appendAsLeader(createRecords.get(), 0);
            assertEquals(numSegments, log.numberOfSegments(), "Changing time beyond rollMs and appending should create a new segment.");
        }

        // Append a message with timestamp to a segment whose first message do not have a timestamp.
        long timestamp = mockTime.milliseconds() + log.config().segmentMs + 1;
        Supplier<MemoryRecords> recordWithTimestamp = () -> LogTestUtils.records(List.of(new SimpleRecord(timestamp, "test".getBytes())));
        log.appendAsLeader(recordWithTimestamp.get(), 0);
        assertEquals(4, log.numberOfSegments(), "Segment should not have been rolled out because the log rolling should be based on wall clock.");

        // Test the segment rolling behavior when messages have timestamps.
        mockTime.sleep(log.config().segmentMs + 1);
        log.appendAsLeader(recordWithTimestamp.get(), 0);
        assertEquals(5, log.numberOfSegments(), "A new segment should have been rolled out");

        // move the wall clock beyond log rolling time
        mockTime.sleep(log.config().segmentMs + 1);
        log.appendAsLeader(recordWithTimestamp.get(), 0);
        assertEquals(5, log.numberOfSegments(), "Log should not roll because the roll should depend on timestamp of the first message.");

        Supplier<MemoryRecords> recordWithExpiredTimestamp = () -> LogTestUtils.records(List.of(new SimpleRecord(mockTime.milliseconds(), "test".getBytes())));
        log.appendAsLeader(recordWithExpiredTimestamp.get(), 0);
        assertEquals(6, log.numberOfSegments(), "Log should roll because the timestamp in the message should make the log segment expire.");

        int numSegments = log.numberOfSegments();
        mockTime.sleep(log.config().segmentMs + 1);
        log.appendAsLeader(MemoryRecords.withRecords(Compression.NONE), 0);
        assertEquals(numSegments, log.numberOfSegments(), "Appending an empty message set should not roll log even if sufficient time has passed.");
    }

    @Test
    public void testRollSegmentThatAlreadyExists() throws IOException {
        LogConfig logConfig = new LogTestUtils.LogConfigBuilder().segmentMs(ONE_HOUR).build();
        int partitionLeaderEpoch = 0;

        // create a log
        UnifiedLog log = createLog(logDir, logConfig);
        assertEquals(1, log.numberOfSegments(), "Log begins with a single empty segment.");

        // roll active segment with the same base offset of size zero should recreate the segment
        log.roll(Optional.of(0L));
        assertEquals(1, log.numberOfSegments(), "Expect 1 segment after roll() empty segment with base offset.");

        // should be able to append records to active segment
        MemoryRecords records = LogTestUtils.records(
                List.of(new SimpleRecord(mockTime.milliseconds(), "k1".getBytes(), "v1".getBytes())),
                0L, partitionLeaderEpoch);
        log.appendAsFollower(records, partitionLeaderEpoch);
        assertEquals(1, log.numberOfSegments(), "Expect one segment.");
        assertEquals(0L, log.activeSegment().baseOffset());

        // make sure we can append more records
        MemoryRecords records2 = LogTestUtils.records(
                List.of(new SimpleRecord(mockTime.milliseconds() + 10, "k2".getBytes(), "v2".getBytes())),
                1L, partitionLeaderEpoch);
        log.appendAsFollower(records2, partitionLeaderEpoch);

        assertEquals(2, log.logEndOffset(), "Expect two records in the log");
        assertEquals(0, log.read(0, 1, FetchIsolation.LOG_END, true).records.batches().iterator().next().lastOffset());
        assertEquals(1, log.read(1, 1, FetchIsolation.LOG_END, true).records.batches().iterator().next().lastOffset());

        // roll so that active segment is empty
        log.roll();
        assertEquals(2L, log.activeSegment().baseOffset(), "Expect base offset of active segment to be LEO");
        assertEquals(2, log.numberOfSegments(), "Expect two segments.");

        // manually resize offset index to force roll of an empty active segment on next append
        log.activeSegment().offsetIndex().resize(0);
        MemoryRecords records3 = LogTestUtils.records(
                List.of(new SimpleRecord(mockTime.milliseconds() + 12, "k3".getBytes(), "v3".getBytes())),
                2L, partitionLeaderEpoch);
        log.appendAsFollower(records3, partitionLeaderEpoch);
        assertTrue(log.activeSegment().offsetIndex().maxEntries() > 1);
        assertEquals(2, log.read(2, 1, FetchIsolation.LOG_END, true).records.batches().iterator().next().lastOffset());
        assertEquals(2, log.numberOfSegments(), "Expect two segments.");
    }

    @Test
    public void testNonSequentialAppend() throws IOException {
        // create a log
        UnifiedLog log = createLog(logDir, new LogConfig(new Properties()));
        long pid = 1L;
        short epoch = 0;

        MemoryRecords records = LogTestUtils.records(
                List.of(new SimpleRecord(mockTime.milliseconds(), "key".getBytes(), "value".getBytes())),
                pid, epoch, 0, 0L);
        log.appendAsLeader(records, 0);

        MemoryRecords nextRecords = LogTestUtils.records(
                List.of(new SimpleRecord(mockTime.milliseconds(), "key".getBytes(), "value".getBytes())),
                pid, epoch, 2, 0L);
        assertThrows(OutOfOrderSequenceException.class, () -> log.appendAsLeader(nextRecords, 0));
    }

    @Test
    public void testTruncateToEndOffsetClearsEpochCache() throws IOException {
        UnifiedLog log = createLog(logDir, new LogConfig(new Properties()));

        // Seed some initial data in the log
        MemoryRecords records = LogTestUtils.records(List.of(new SimpleRecord("a".getBytes()), new SimpleRecord("b".getBytes())),
                27, RecordBatch.NO_PARTITION_LEADER_EPOCH);
        appendAsFollower(log, records, 19);
        assertEquals(Optional.of(new EpochEntry(19, 27)), log.leaderEpochCache().latestEntry());
        assertEquals(29, log.logEndOffset());

        // Truncations greater than or equal to the log end offset should
        // clear the epoch cache
        verifyTruncationClearsEpochCache(log, 20, log.logEndOffset());
        verifyTruncationClearsEpochCache(log, 24, log.logEndOffset() + 1);
    }

    private void verifyTruncationClearsEpochCache(UnifiedLog log, int epoch, long truncationOffset) {
        // Simulate becoming a leader
        log.assignEpochStartOffset(epoch, log.logEndOffset());
        assertEquals(Optional.of(new EpochEntry(epoch, 29)), log.leaderEpochCache().latestEntry());
        assertEquals(29, log.logEndOffset());

        // Now we become the follower and truncate to an offset greater
        // than or equal to the log end offset. The trivial epoch entry
        // at the end of the log should be gone
        log.truncateTo(truncationOffset);
        assertEquals(Optional.of(new EpochEntry(19, 27)), log.leaderEpochCache().latestEntry());
        assertEquals(29, log.logEndOffset());
    }

    /**
     * Test the values returned by the logSegments call
     */
    @Test
    public void testLogSegmentsCallCorrect() throws IOException {
        // Create 3 segments and make sure we get the right values from various logSegments calls.
        Supplier<MemoryRecords> createRecords = () -> LogTestUtils.records(List.of(new SimpleRecord("test".getBytes())), mockTime.milliseconds());

        int setSize = createRecords.get().sizeInBytes();
        int msgPerSeg = 10;
        int segmentSize = msgPerSeg * setSize;  // each segment will be 10 messages
        // create a log
        LogConfig logConfig = new LogTestUtils.LogConfigBuilder().segmentBytes(segmentSize).build();
        UnifiedLog log = createLog(logDir, logConfig);
        assertEquals(1, log.numberOfSegments(), "There should be exactly 1 segment.");

        // segments expire in size
        for (int i = 1; i <= (2 * msgPerSeg + 2); i++) {
            log.appendAsLeader(createRecords.get(), 0);
        }
        assertEquals(3, log.numberOfSegments(), "There should be exactly 3 segments.");

        // from == to should always be null
        assertEquals(List.of(), getSegmentOffsets(log, 10, 10));
        assertEquals(List.of(), getSegmentOffsets(log, 15, 15));

        assertEquals(List.of(0L, 10L, 20L), getSegmentOffsets(log, 0, 21));

        assertEquals(List.of(0L), getSegmentOffsets(log, 1, 5));
        assertEquals(List.of(10L, 20L), getSegmentOffsets(log, 13, 21));
        assertEquals(List.of(10L), getSegmentOffsets(log, 13, 17));

        // from > to is bad
        assertThrows(IllegalArgumentException.class, () -> log.logSegments(10, 0));
    }

    private List<Long> getSegmentOffsets(UnifiedLog log, long from, long to) {
        return log.logSegments(from, to).stream().map(LogSegment::baseOffset).toList();
    }

    @Test
    public void testInitializationOfProducerSnapshotsUpgradePath() throws IOException {
        // simulate the upgrade path by creating a new log with several segments, deleting the
        // snapshot files, and then reloading the log
        LogConfig logConfig = new LogTestUtils.LogConfigBuilder().segmentBytes(64 * 10).build();
        UnifiedLog log = createLog(logDir, logConfig);
        assertEquals(OptionalLong.empty(), log.oldestProducerSnapshotOffset());

        for (int i = 0; i <= 100; i++) {
            SimpleRecord record = new SimpleRecord(mockTime.milliseconds(), String.valueOf(i).getBytes());
            log.appendAsLeader(LogTestUtils.records(List.of(record)), 0);
        }
        assertTrue(log.logSegments().size() >= 2);
        long logEndOffset = log.logEndOffset();
        log.close();

        LogTestUtils.deleteProducerSnapshotFiles(logDir);

        // Reload after clean shutdown
        log = createLog(logDir, logConfig, 0L, logEndOffset, brokerTopicStats, mockTime.scheduler, mockTime,
                producerStateManagerConfig, true, Optional.empty(), false);
        List<Long> segmentOffsets = log.logSegments().stream()
                .map(LogSegment::baseOffset)
                .toList();
        int size = segmentOffsets.size();
        List<Long> expectedSnapshotOffsets = new ArrayList<>(size >= 2 ? segmentOffsets.subList(size - 2, size) : segmentOffsets);
        expectedSnapshotOffsets.add(log.logEndOffset());
        assertEquals(expectedSnapshotOffsets, LogTestUtils.listProducerSnapshotOffsets(logDir));
        log.close();

        LogTestUtils.deleteProducerSnapshotFiles(logDir);

        // Reload after unclean shutdown with recoveryPoint set to log end offset
        log = createLog(logDir, logConfig, 0L, logEndOffset, brokerTopicStats, mockTime.scheduler, mockTime,
                producerStateManagerConfig, false, Optional.empty(), false);
        assertEquals(expectedSnapshotOffsets, LogTestUtils.listProducerSnapshotOffsets(logDir));
        log.close();

        LogTestUtils.deleteProducerSnapshotFiles(logDir);

        // Reload after unclean shutdown with recoveryPoint set to 0
        log = createLog(logDir, logConfig, 0L, 0L, brokerTopicStats, mockTime.scheduler, mockTime,
                producerStateManagerConfig, false, Optional.empty(), false);
        // We progressively create a snapshot for each segment after the recovery point
        segmentOffsets = log.logSegments().stream()
                .map(LogSegment::baseOffset)
                .toList();
        expectedSnapshotOffsets = new ArrayList<>(segmentOffsets.subList(1, segmentOffsets.size()));
        expectedSnapshotOffsets.add(log.logEndOffset());
        assertEquals(expectedSnapshotOffsets, LogTestUtils.listProducerSnapshotOffsets(logDir));
        log.close();
    }

    @Test
    public void testLogReinitializeAfterManualDelete() throws IOException {
        LogConfig logConfig = new LogTestUtils.LogConfigBuilder().build();
        // simulate a case where log data does not exist but the start offset is non-zero
        UnifiedLog log = createLog(logDir, logConfig, 500L, 0L, brokerTopicStats, mockTime.scheduler, mockTime,
                producerStateManagerConfig, true, Optional.empty(), false);
        assertEquals(500, log.logStartOffset());
        assertEquals(500, log.logEndOffset());
    }

    /**
     * Test that "PeriodicProducerExpirationCheck" scheduled task gets canceled after log
     * is deleted.
     */
    @Test
    public void testProducerExpireCheckAfterDelete() throws Exception {
        KafkaScheduler scheduler = new KafkaScheduler(1);
        try {
            scheduler.startup();
            LogConfig logConfig = new LogTestUtils.LogConfigBuilder().build();
            UnifiedLog log = createLog(logDir, logConfig, 0L, 0L, brokerTopicStats, scheduler, mockTime,
                    producerStateManagerConfig, true, Optional.empty(), false);

            ScheduledFuture<?> producerExpireCheck = log.producerExpireCheck();
            assertTrue(scheduler.taskRunning(producerExpireCheck), "producerExpireCheck isn't as part of scheduled tasks");

            log.delete();
            assertFalse(scheduler.taskRunning(producerExpireCheck),
                    "producerExpireCheck is part of scheduled tasks even after log deletion");
        } finally {
            scheduler.shutdown();
        }
    }

    @Test
    public void testProducerIdMapOffsetUpdatedForNonIdempotentData() throws IOException {
        LogConfig logConfig = new LogTestUtils.LogConfigBuilder().segmentBytes(TEN_KB).build();
        UnifiedLog log = createLog(logDir, logConfig);
        MemoryRecords records = LogTestUtils.records(List.of(new SimpleRecord(mockTime.milliseconds(), "key".getBytes(), "value".getBytes())));
        log.appendAsLeader(records, 0);
        log.takeProducerSnapshot();
        assertEquals(OptionalLong.of(1), log.latestProducerSnapshotOffset());
    }

    @Test
    public void testRebuildProducerIdMapWithCompactedData() throws IOException {
        LogConfig logConfig = new LogTestUtils.LogConfigBuilder().segmentBytes(TEN_KB).build();
        UnifiedLog log = createLog(logDir, logConfig);
        long pid = 1L;
        short producerEpoch = 0;
        int partitionLeaderEpoch = 0;
        int seq = 0;
        long baseOffset = 23L;

        // create a batch with a couple gaps to simulate compaction
        MemoryRecords records = LogTestUtils.records(
                List.of(
                        new SimpleRecord(mockTime.milliseconds(), "a".getBytes()),
                        new SimpleRecord(mockTime.milliseconds(), "key".getBytes(), "b".getBytes()),
                        new SimpleRecord(mockTime.milliseconds(), "c".getBytes()),
                        new SimpleRecord(mockTime.milliseconds(), "key".getBytes(), "d".getBytes())
                ),
                pid, producerEpoch, seq, baseOffset
        );
        records.batches().forEach(b -> b.setPartitionLeaderEpoch(partitionLeaderEpoch));

        ByteBuffer filtered = ByteBuffer.allocate(2048);
        records.filterTo(new MemoryRecords.RecordFilter(0, 0) {
            @Override
            public MemoryRecords.RecordFilter.BatchRetentionResult checkBatchRetention(RecordBatch batch) {
                return new MemoryRecords.RecordFilter.BatchRetentionResult(MemoryRecords.RecordFilter.BatchRetention.DELETE_EMPTY, false);
            }
            @Override 
            public boolean shouldRetainRecord(RecordBatch recordBatch, Record record) {
                return !record.hasKey();
            }
        }, filtered, BufferSupplier.NO_CACHING);
        filtered.flip();
        MemoryRecords filteredRecords = MemoryRecords.readableRecords(filtered);

        log.appendAsFollower(filteredRecords, partitionLeaderEpoch);

        // append some more data and then truncate to force rebuilding of the PID map
        MemoryRecords moreRecords = LogTestUtils.records(
                List.of(
                        new SimpleRecord(mockTime.milliseconds(), "e".getBytes()),
                        new SimpleRecord(mockTime.milliseconds(), "f".getBytes())),
                baseOffset + 4, RecordBatch.NO_PARTITION_LEADER_EPOCH
        );
        appendAsFollower(log, moreRecords, partitionLeaderEpoch);

        log.truncateTo(baseOffset + 4);

        Map<Long, Integer> activeProducers = log.activeProducersWithLastSequence();
        assertTrue(activeProducers.containsKey(pid));

        int lastSeq = activeProducers.get(pid);
        assertEquals(3, lastSeq);
    }

    @Test
    public void testRebuildProducerStateWithEmptyCompactedBatch() throws IOException {
        LogConfig logConfig = new LogTestUtils.LogConfigBuilder().segmentBytes(TEN_KB).build();
        UnifiedLog log = createLog(logDir, logConfig);
        long pid = 1L;
        short producerEpoch = 0;
        int partitionLeaderEpoch = 0;
        int seq = 0;
        long baseOffset = 23L;

        // create an empty batch
        MemoryRecords records = LogTestUtils.records(
                List.of(
                        new SimpleRecord(mockTime.milliseconds(), "key".getBytes(), "a".getBytes()),
                        new SimpleRecord(mockTime.milliseconds(), "key".getBytes(), "b".getBytes())),
                pid, producerEpoch, seq, baseOffset
        );
        records.batches().forEach(b -> b.setPartitionLeaderEpoch(partitionLeaderEpoch));

        ByteBuffer filtered = ByteBuffer.allocate(2048);
        records.filterTo(new MemoryRecords.RecordFilter(0, 0) {
            @Override 
            public MemoryRecords.RecordFilter.BatchRetentionResult checkBatchRetention(RecordBatch batch) {
                return new MemoryRecords.RecordFilter.BatchRetentionResult(MemoryRecords.RecordFilter.BatchRetention.RETAIN_EMPTY, true);
            }
            @Override public boolean shouldRetainRecord(RecordBatch recordBatch, Record record) {
                return false;
            }
        }, filtered, BufferSupplier.NO_CACHING);
        filtered.flip();
        MemoryRecords filteredRecords = MemoryRecords.readableRecords(filtered);

        log.appendAsFollower(filteredRecords, partitionLeaderEpoch);

        // append some more data and then truncate to force rebuilding of the PID map
        MemoryRecords moreRecords = LogTestUtils.records(
                List.of(
                        new SimpleRecord(mockTime.milliseconds(), "e".getBytes()),
                        new SimpleRecord(mockTime.milliseconds(), "f".getBytes())),
                baseOffset + 2, RecordBatch.NO_PARTITION_LEADER_EPOCH
        );
        appendAsFollower(log, moreRecords, partitionLeaderEpoch);

        log.truncateTo(baseOffset + 2);

        Map<Long, Integer> activeProducers = log.activeProducersWithLastSequence();
        assertTrue(activeProducers.containsKey(pid));

        int lastSeq = activeProducers.get(pid);
        assertEquals(1, lastSeq);
    }

    @Test
    public void testUpdateProducerIdMapWithCompactedData() throws IOException {
        LogConfig logConfig = new LogTestUtils.LogConfigBuilder().segmentBytes(TEN_KB).build();
        UnifiedLog log = createLog(logDir, logConfig);
        long pid = 1L;
        short producerEpoch = 0;
        int partitionLeaderEpoch = 0;
        int seq = 0;
        long baseOffset = 23L;

        // create a batch with a couple gaps to simulate compaction
        MemoryRecords records = LogTestUtils.records(
                List.of(
                        new SimpleRecord(mockTime.milliseconds(), "a".getBytes()),
                        new SimpleRecord(mockTime.milliseconds(), "key".getBytes(), "b".getBytes()),
                        new SimpleRecord(mockTime.milliseconds(), "c".getBytes()),
                        new SimpleRecord(mockTime.milliseconds(), "key".getBytes(), "d".getBytes())),
                pid, producerEpoch, seq, baseOffset
        );
        records.batches().forEach(b -> b.setPartitionLeaderEpoch(partitionLeaderEpoch));

        ByteBuffer filtered = ByteBuffer.allocate(2048);
        records.filterTo(new MemoryRecords.RecordFilter(0, 0) {
            @Override public MemoryRecords.RecordFilter.BatchRetentionResult checkBatchRetention(RecordBatch batch) {
                return new MemoryRecords.RecordFilter.BatchRetentionResult(MemoryRecords.RecordFilter.BatchRetention.DELETE_EMPTY, false);
            }
            @Override public boolean shouldRetainRecord(RecordBatch recordBatch, Record record) {
                return !record.hasKey();
            }
        }, filtered, BufferSupplier.NO_CACHING);
        filtered.flip();
        MemoryRecords filteredRecords = MemoryRecords.readableRecords(filtered);

        log.appendAsFollower(filteredRecords, partitionLeaderEpoch);
        Map<Long, Integer> activeProducers = log.activeProducersWithLastSequence();
        assertTrue(activeProducers.containsKey(pid));

        int lastSeq = activeProducers.get(pid);
        assertEquals(3, lastSeq);
    }

    @Test
    public void testProducerIdMapTruncateTo() throws IOException {
        LogConfig logConfig = new LogTestUtils.LogConfigBuilder().segmentBytes(TEN_KB).build();
        UnifiedLog log = createLog(logDir, logConfig);
        log.appendAsLeader(LogTestUtils.records(List.of(new SimpleRecord("a".getBytes()))), 0);
        log.appendAsLeader(LogTestUtils.records(List.of(new SimpleRecord("b".getBytes()))), 0);
        log.takeProducerSnapshot();

        log.appendAsLeader(LogTestUtils.records(List.of(new SimpleRecord("c".getBytes()))), 0);
        log.takeProducerSnapshot();

        log.truncateTo(2);
        assertEquals(OptionalLong.of(2), log.latestProducerSnapshotOffset());
        assertEquals(2, log.latestProducerStateEndOffset());

        log.truncateTo(1);
        assertEquals(OptionalLong.of(1), log.latestProducerSnapshotOffset());
        assertEquals(1, log.latestProducerStateEndOffset());

        log.truncateTo(0);
        assertEquals(OptionalLong.empty(), log.latestProducerSnapshotOffset());
        assertEquals(0, log.latestProducerStateEndOffset());
    }

    @Test
    public void testProducerIdMapTruncateToWithNoSnapshots() throws IOException {
        // This ensures that the upgrade optimization path cannot be hit after initial loading
        LogConfig logConfig = new LogTestUtils.LogConfigBuilder().segmentBytes(TEN_KB).build();
        UnifiedLog log = createLog(logDir, logConfig);
        long pid = 1L;
        short epoch = 0;

        log.appendAsLeader(LogTestUtils.records(
                List.of(new SimpleRecord("a".getBytes())),
                pid, epoch, 0, 0L), 0);
        log.appendAsLeader(LogTestUtils.records(
                List.of(new SimpleRecord("b".getBytes())),
                pid, epoch, 1, 0L), 0);

        LogTestUtils.deleteProducerSnapshotFiles(logDir);

        log.truncateTo(1L);
        assertEquals(1, log.activeProducersWithLastSequence().size());

        int lastSeq = log.activeProducersWithLastSequence().get(pid);
        assertEquals(0, lastSeq);
    }

    @ParameterizedTest(name = "testRetentionDeletesProducerStateSnapshots with createEmptyActiveSegment: {0}")
    @ValueSource(booleans = {true, false})
    public void testRetentionDeletesProducerStateSnapshots(boolean createEmptyActiveSegment) throws IOException {
        LogConfig logConfig = new LogTestUtils.LogConfigBuilder()
                .segmentBytes(TEN_KB)
                .retentionBytes(0)
                .retentionMs(1000 * 60)
                .fileDeleteDelayMs(0)
                .build();
        UnifiedLog log = createLog(logDir, logConfig);
        long pid1 = 1L;
        short epoch = 0;

        log.appendAsLeader(LogTestUtils.records(
                List.of(new SimpleRecord("a".getBytes())),
                pid1, epoch, 0, 0L), 0);
        log.roll();
        log.appendAsLeader(LogTestUtils.records(
                List.of(new SimpleRecord("b".getBytes())),
                pid1, epoch, 1, 0L), 0);
        log.roll();
        log.appendAsLeader(LogTestUtils.records(
                List.of(new SimpleRecord("c".getBytes())),
                pid1, epoch, 2, 0L), 0);
        if (createEmptyActiveSegment) {
            log.roll();
        }

        log.updateHighWatermark(log.logEndOffset());

        int numProducerSnapshots = createEmptyActiveSegment ? 3 : 2;
        assertEquals(numProducerSnapshots, ProducerStateManager.listSnapshotFiles(logDir).size());
        // Sleep to breach the retention period
        mockTime.sleep(1000 * 60 + 1);
        assertTrue(log.deleteOldSegments() > 0, "At least one segment should be deleted");
        // Sleep to breach the file delete delay and run scheduled file deletion tasks
        mockTime.sleep(1);
        assertEquals(1, ProducerStateManager.listSnapshotFiles(logDir).size(),
                "expect a single producer state snapshot remaining");
        assertEquals(3, log.logStartOffset());
    }

    private void assertValidLogOffsetMetadata(UnifiedLog log, LogOffsetMetadata offsetMetadata) throws IOException {
        assertFalse(offsetMetadata.messageOffsetOnly());

        long segmentBaseOffset = offsetMetadata.segmentBaseOffset;
        List<LogSegment> segments = log.logSegments(segmentBaseOffset, segmentBaseOffset + 1);
        assertFalse(segments.isEmpty());

        LogSegment segment = segments.iterator().next();
        assertEquals(segmentBaseOffset, segment.baseOffset());
        assertTrue(offsetMetadata.relativePositionInSegment <= segment.size());

        FetchDataInfo readInfo = segment.read(offsetMetadata.messageOffset,
                2048,
                Optional.of((long) segment.size()),
                false);

        if (offsetMetadata.relativePositionInSegment < segment.size()) {
            assertEquals(offsetMetadata, readInfo.fetchOffsetMetadata);
        } else {
            assertNull(readInfo);
        }
    }

    private void append(int epoch, long startOffset, int count) {
        Function<Integer, MemoryRecords> records = i ->
                records(List.of(new SimpleRecord("value".getBytes())), startOffset + i, epoch);
        for (int i = 0; i < count; i++) {
            log.appendAsFollower(records.apply(i), epoch);
        }
    }

    private void appendAsFollower(UnifiedLog log, MemoryRecords records, int leaderEpoch) {
        records.batches().forEach(b -> b.setPartitionLeaderEpoch(leaderEpoch));
        log.appendAsFollower(records, leaderEpoch);
    }

    private LeaderEpochFileCache epochCache(UnifiedLog log) {
        return log.leaderEpochCache();
    }

    private UnifiedLog createLog(File dir, LogConfig config) throws IOException {
        return createLog(dir, config, false);
    }

    private UnifiedLog createLog(File dir, LogConfig config, boolean remoteStorageSystemEnable) throws IOException {
        return createLog(dir, config, 0L, 0L, brokerTopicStats, mockTime.scheduler, mockTime,
                producerStateManagerConfig, true, Optional.empty(), remoteStorageSystemEnable);
    }

    private UnifiedLog createLog(
            File dir,
            LogConfig config,
            long logStartOffset,
            long recoveryPoint,
            BrokerTopicStats brokerTopicStats,
            Scheduler scheduler,
            MockTime time,
            ProducerStateManagerConfig producerStateManagerConfig,
            boolean lastShutdownClean,
            Optional<Uuid> topicId,
            boolean remoteStorageSystemEnable) throws IOException {

        UnifiedLog log = UnifiedLog.create(
                dir,
                config,
                logStartOffset,
                recoveryPoint,
                scheduler,
                brokerTopicStats,
                time,
                3600000,
                producerStateManagerConfig,
                TransactionLogConfig.PRODUCER_ID_EXPIRATION_CHECK_INTERVAL_MS_DEFAULT,
                new LogDirFailureChannel(10),
                lastShutdownClean,
                topicId,
                new ConcurrentHashMap<>(),
                remoteStorageSystemEnable,
                LogOffsetsListener.NO_OP_OFFSETS_LISTENER
        );

        logsToClose.add(log);
        return log;
    }

    public static MemoryRecords singletonRecords(byte[] value, byte[] key) {
        return singletonRecords(value, key, Compression.NONE, RecordBatch.NO_TIMESTAMP, RecordBatch.CURRENT_MAGIC_VALUE);
    }

    public static MemoryRecords singletonRecords(byte[] value, long timestamp) {
        return singletonRecords(value, null, Compression.NONE, timestamp, RecordBatch.CURRENT_MAGIC_VALUE);
    }

    public static MemoryRecords singletonRecords(
            byte[] value
    ) {
        return records(List.of(new SimpleRecord(RecordBatch.NO_TIMESTAMP, null, value)),
                RecordBatch.CURRENT_MAGIC_VALUE,
                Compression.NONE,
                RecordBatch.NO_PRODUCER_ID,
                RecordBatch.NO_PRODUCER_EPOCH,
                RecordBatch.NO_SEQUENCE,
                0,
                RecordBatch.NO_PARTITION_LEADER_EPOCH
        );
    }

    public static MemoryRecords singletonRecords(
            byte[] value,
            byte[] key,
            Compression codec,
            long timestamp,
            byte magicValue
    ) {
        return records(List.of(new SimpleRecord(timestamp, key, value)),
                magicValue, codec,
                RecordBatch.NO_PRODUCER_ID,
                RecordBatch.NO_PRODUCER_EPOCH,
                RecordBatch.NO_SEQUENCE,
                0,
                RecordBatch.NO_PARTITION_LEADER_EPOCH
        );
    }

    public static MemoryRecords singletonRecords(byte[] value, byte[] key, long timestamp) {
        return singletonRecords(value, key, Compression.NONE, timestamp, RecordBatch.CURRENT_MAGIC_VALUE);
    }

    public static MemoryRecords records(List<SimpleRecord> records) {
        return records(records, RecordBatch.CURRENT_MAGIC_VALUE, Compression.NONE, RecordBatch.NO_PRODUCER_ID,
                RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE, 0L, RecordBatch.NO_PARTITION_LEADER_EPOCH);
    }

    public static MemoryRecords records(List<SimpleRecord> records, long baseOffset) {
        return records(records, RecordBatch.CURRENT_MAGIC_VALUE, Compression.NONE, RecordBatch.NO_PRODUCER_ID,
                RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE, baseOffset, RecordBatch.NO_PARTITION_LEADER_EPOCH);
    }

    public static MemoryRecords records(List<SimpleRecord> records, long baseOffset, int partitionLeaderEpoch) {
        return records(records, RecordBatch.CURRENT_MAGIC_VALUE, Compression.NONE, RecordBatch.NO_PRODUCER_ID,
                RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE, baseOffset, partitionLeaderEpoch);
    }

    public static MemoryRecords records(List<SimpleRecord> records, byte magicValue, Compression compression) {
        return records(records, magicValue, compression, RecordBatch.NO_PRODUCER_ID,
                RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE, 0L, RecordBatch.NO_PARTITION_LEADER_EPOCH);
    }

    public static MemoryRecords records(List<SimpleRecord> records,
                                        byte magicValue,
                                        Compression compression,
                                        long producerId,
                                        short producerEpoch,
                                        int sequence,
                                        long baseOffset,
                                        int partitionLeaderEpoch) {
        ByteBuffer buf = ByteBuffer.allocate(DefaultRecordBatch.sizeInBytes(records));
        MemoryRecordsBuilder builder = MemoryRecords.builder(buf, magicValue, compression, TimestampType.CREATE_TIME, baseOffset,
                System.currentTimeMillis(), producerId, producerEpoch, sequence, false, partitionLeaderEpoch);
        for (SimpleRecord record : records) {
            builder.append(record);
        }
        return builder.build();
    }

    /**
     * Test RetentionSizeInPercent metric for regular (non-tiered) topics.
     * The metric should only be reported for non-tiered topics with size-based retention configured.
     *
     * @param remoteLogStorageEnable whether remote log storage is enabled
     * @param remoteLogCopyDisable whether remote log copy is disabled (only relevant when remote storage is enabled)
     * @param expectedSizeInPercent expected percentage value after retention cleanup
     */
    @ParameterizedTest
    @CsvSource({
        // Remote storage enabled with copy enabled: metric handled by RemoteLogManager, returns 0 here
        "true, false, 0",
        // Remote storage enabled but copy disabled: metric should be calculated (100%)
        "true, true, 100",
        // Remote storage disabled: metric should be calculated (100%)
        "false, false, 100",
        // Remote storage disabled (remoteLogCopyDisable is ignored): metric should be calculated (100%)
        "false, true, 100"
    })
    public void testRetentionSizeInPercentMetric(
            boolean remoteLogStorageEnable,
            boolean remoteLogCopyDisable,
            int expectedSizeInPercent
    ) throws IOException {
        Supplier<MemoryRecords> records = () -> singletonRecords("test".getBytes());
        int recordSize = records.get().sizeInBytes();
        LogConfig logConfig = new LogTestUtils.LogConfigBuilder()
                .segmentBytes(recordSize * 5)
                .retentionBytes(recordSize * 10L)
                .remoteLogStorageEnable(remoteLogStorageEnable)
                .remoteLogCopyDisable(remoteLogCopyDisable)
                .build();
        log = createLog(logDir, logConfig, true);

        String metricName = "name=RetentionSizeInPercent,topic=" + log.topicPartition().topic() + 
                ",partition=" + log.topicPartition().partition();

        // Append some messages to create 3 segments (15 records / 5 records per segment = 3 segments)
        for (int i = 0; i < 15; i++) {
            log.appendAsLeader(records.get(), 0);
        }

        // Before deletion, calculate what the percentage should be
        // Total size = 15 * recordSize, retention = 10 * recordSize
        // Percentage = (15 * 100) / 10 = 150% (for non-tiered topics)
        if (!remoteLogStorageEnable || remoteLogCopyDisable) {
            assertEquals(150, log.calculateRetentionSizeInPercent());
        }

        log.updateHighWatermark(log.logEndOffset());
        // For tiered storage tests, simulate remote storage having the data
        if (remoteLogStorageEnable) {
            log.updateHighestOffsetInRemoteStorage(9);
        }
        log.deleteOldSegments();

        // After deletion: log size should be ~10 * recordSize (2 segments), retention = 10 * recordSize
        // Percentage = (10 * 100) / 10 = 100% (for non-tiered topics)
        // Verify via Yammer metric (JMX)
        assertEquals(expectedSizeInPercent, yammerMetricValue(metricName));
        assertEquals(2, log.numberOfSegments(), "should have 2 segments after deletion");
    }

    @Test
    public void testRetentionSizeInPercentWithInfiniteRetention() throws IOException {
        Supplier<MemoryRecords> records = () -> singletonRecords("test".getBytes());
        // Create log with no retention configured (retentionBytes = -1 means unlimited)
        LogConfig logConfig = new LogTestUtils.LogConfigBuilder()
                .segmentBytes(records.get().sizeInBytes() * 5)
                .retentionBytes(-1L)
                .build();
        log = createLog(logDir, logConfig, false);

        String metricName = "name=RetentionSizeInPercent,topic=" + log.topicPartition().topic() + 
                ",partition=" + log.topicPartition().partition();

        for (int i = 0; i < 10; i++) {
            log.appendAsLeader(records.get(), 0);
        }

        // With unlimited retention, the metric should be 0
        assertEquals(0, log.calculateRetentionSizeInPercent());

        log.updateHighWatermark(log.logEndOffset());
        log.deleteOldSegments();

        // After deleteOldSegments, metric should still be 0
        // Verify via Yammer metric (JMX)
        assertEquals(0, yammerMetricValue(metricName));
    }

    /**
     * Test that verifies the RetentionSizeInPercent metric is always updated in the finally block
     * of deleteOldSegments(), even when an exception is thrown during deletion.
     * This ensures the metric is calculated even when log deletion encounters errors.
     */
    @Test
    public void testRetentionSizeInPercentMetricUpdatedOnDeletionError() throws IOException {
        Supplier<MemoryRecords> records = () -> singletonRecords("test".getBytes());
        int recordSize = records.get().sizeInBytes();

        // Create log with retention smaller than data to force deletion
        LogConfig logConfig = new LogTestUtils.LogConfigBuilder()
                .segmentBytes(recordSize * 5)
                .retentionBytes(recordSize * 10L)
                .build();
        log = createLog(logDir, logConfig, false);

        String metricName = "name=RetentionSizeInPercent,topic=" + log.topicPartition().topic() +
                ",partition=" + log.topicPartition().partition();

        // Append messages to create multiple segments (15 records / 5 per segment = 3 segments)
        for (int i = 0; i < 15; i++) {
            log.appendAsLeader(records.get(), 0);
        }
        assertEquals(3, log.numberOfSegments(), "Should have 3 segments");

        log.updateHighWatermark(log.logEndOffset());

        // First call to initialize the metric normally
        log.deleteOldSegments();
        assertEquals(100, yammerMetricValue(metricName), "Metric should be 100% after initial deletion");

        // Add more data to change the metric value
        for (int i = 0; i < 10; i++) {
            log.appendAsLeader(records.get(), 0);
        }
        log.updateHighWatermark(log.logEndOffset());

        // Create a spy and make config() throw on first call, but work normally on subsequent calls
        // This simulates an error in the try block while allowing the finally block to succeed
        // The config() method is called in both the try block and calculateRetentionSizeInPercent()
        UnifiedLog spyLog = spy(log);
        doThrow(new RuntimeException("Simulated error during deletion"))
                .doCallRealMethod()  // Allow subsequent calls to work (for finally block)
                .when(spyLog).config();

        // Call deleteOldSegments on the spy - it should throw due to config() error
        // But the finally block should still execute and update the metric
        assertThrows(RuntimeException.class, spyLog::deleteOldSegments);

        // Verify the metric was still updated in the finally block despite the exception
        // After adding 10 more records (2 more segments), total = 4 segments = 20 records
        // Percentage = (20 * 100) / 10 = 200%
        assertEquals(200, yammerMetricValue(metricName),
                "Metric should be updated in finally block even when exception occurs");
    }
    
    @Test
    public void testReadWithMinMessage() throws IOException {
        LogConfig logConfig = new LogTestUtils.LogConfigBuilder()
                .segmentBytes(72)
                .build();
        log = createLog(logDir, logConfig);
        int[] messageIds = IntStream.concat(
                IntStream.range(0, 50),
                IntStream.iterate(50, i -> i < 200, i -> i + 7)
        ).toArray();
        SimpleRecord[] records = Arrays.stream(messageIds)
                .mapToObj(id -> new SimpleRecord(String.valueOf(id).getBytes()))
                .toArray(SimpleRecord[]::new);

        // now test the case that we give the offsets and use non-sequential offsets
        for (int i = 0; i < records.length; i++) {
            log.appendAsFollower(
                    MemoryRecords.withRecords(messageIds[i], Compression.NONE, 0, records[i]),
                    Integer.MAX_VALUE
            );
        }

        int maxMessageId = Arrays.stream(messageIds).max().getAsInt();
        for (int i = 50; i < maxMessageId; i++) {
            int offset = i;
            int idx = IntStream.range(0, messageIds.length)
                    .filter(j -> messageIds[j] >= offset)
                    .findFirst()
                    .getAsInt();

            List<FetchDataInfo> fetchResults = List.of(
                    log.read(i, 1, FetchIsolation.LOG_END, true),
                    log.read(i, 100000, FetchIsolation.LOG_END, true),
                    log.read(i, 100, FetchIsolation.LOG_END, true)
            );
            for (FetchDataInfo fetchDataInfo : fetchResults) {
                Record read = fetchDataInfo.records.records().iterator().next();
                assertEquals(messageIds[idx], read.offset(), "Offset read should match message id.");
                assertEquals(records[idx], new SimpleRecord(read), "Message should match appended.");
            }
        }
    }

    @Test
    public void testReadWithTooSmallMaxLength() throws IOException {
        LogConfig logConfig = new LogTestUtils.LogConfigBuilder()
                .segmentBytes(72)
                .build();
        log = createLog(logDir, logConfig);
        int[] messageIds = IntStream.concat(
                IntStream.range(0, 50),
                IntStream.iterate(50, i -> i < 200, i -> i + 7)
        ).toArray();
        SimpleRecord[] records = Arrays.stream(messageIds)
                .mapToObj(id -> new SimpleRecord(String.valueOf(id).getBytes()))
                .toArray(SimpleRecord[]::new);

        // now test the case that we give the offsets and use non-sequential offsets
        for (int i = 0; i < records.length; i++) {
            log.appendAsFollower(
                    MemoryRecords.withRecords(messageIds[i], Compression.NONE, 0, records[i]),
                    Integer.MAX_VALUE
            );
        }

        int maxMessageId = Arrays.stream(messageIds).max().getAsInt();
        for (int i = 50; i < maxMessageId; i++) {
            assertEquals(MemoryRecords.EMPTY, log.read(i, 0, FetchIsolation.LOG_END, false).records);

            // we return an incomplete message instead of an empty one for the case below
            // we use this mechanism to tell consumers of the fetch request version 2 and below that the message size is
            // larger than the fetch size
            // in fetch request version 3, we no longer need this as we return oversized messages from the first non-empty
            // partition
            FetchDataInfo fetchInfo = log.read(i, 1, FetchIsolation.LOG_END, false);
            assertTrue(fetchInfo.firstEntryIncomplete);
            assertInstanceOf(FileRecords.class, fetchInfo.records);
            assertEquals(1, fetchInfo.records.sizeInBytes());
        }
    }

    /**
     * Test reading at the boundary of the log, specifically
     * - reading from the logEndOffset should give an empty message set
     * - reading from the maxOffset should give an empty message set
     * - reading beyond the log end offset should throw an OffsetOutOfRangeException
     */
    @Test
    public void testReadOutOfRange() throws IOException {
        // create empty log files to simulate a log starting at offset 1024
        Files.createFile(LogFileUtils.logFile(logDir, 1024).toPath());
        Files.createFile(LogFileUtils.offsetIndexFile(logDir, 1024).toPath());

        // set up replica log starting with offset 1024 and with one message (at offset 1024)
        LogConfig logConfig = new LogTestUtils.LogConfigBuilder()
                .segmentBytes(1024)
                .build();
        log = createLog(logDir, logConfig);
        log.appendAsLeader(MemoryRecords.withRecords(Compression.NONE, new SimpleRecord("42".getBytes())), 0);

        assertEquals(
                0,
                log.read(1025, 1000, FetchIsolation.LOG_END, true).records.sizeInBytes(),
                "Reading at the log end offset should produce 0 byte read."
        );

        assertThrows(OffsetOutOfRangeException.class, () -> log.read(0, 1000, FetchIsolation.LOG_END, true));
        assertThrows(OffsetOutOfRangeException.class, () -> log.read(1026, 1000, FetchIsolation.LOG_END, true));
    }

    @Test
    public void testFlushingEmptyActiveSegments() throws IOException {
        log = createLog(logDir, new LogConfig(new Properties()));
        MemoryRecords message = MemoryRecords.withRecords(
                Compression.NONE,
                new SimpleRecord(mockTime.milliseconds(), null, "Test".getBytes())
        );
        
        log.appendAsLeader(message, 0);
        log.roll();
        assertEquals(2, logDir.listFiles(f -> f.getName().endsWith(".log")).length);
        assertEquals(1, logDir.listFiles(f -> f.getName().endsWith(".index")).length);
        assertEquals(0, log.activeSegment().size());
        log.flush(true);
        assertEquals(2, logDir.listFiles(f -> f.getName().endsWith(".log")).length);
        assertEquals(2, logDir.listFiles(f -> f.getName().endsWith(".index")).length);
    }

    /**
     * Test that covers reads and writes on a multisegment log. This test appends a bunch of messages
     * and then reads them all back and checks that the message read and offset matches what was appended.
     */
    @Test
    public void testLogRolls() throws IOException, InterruptedException {
        // create a multipart log with 100 messages
        LogConfig logConfig = new LogTestUtils.LogConfigBuilder()
                .segmentBytes(100)
                .build();
        log = createLog(logDir, logConfig);
        int numMessages = 100;
        MemoryRecords[] messageSets = IntStream.range(0, numMessages)
                .mapToObj(i -> MemoryRecords.withRecords(
                        Compression.NONE,
                        new SimpleRecord(mockTime.milliseconds(), null, String.valueOf(i).getBytes()))
                ).toArray(MemoryRecords[]::new);
        for (MemoryRecords messageSet : messageSets) {
            log.appendAsLeader(messageSet, 0);
        }
        log.flush(false);

        // do successive reads to ensure all our messages are there
        long offset = 0L;
        for (int i = 0; i < numMessages; i++) {
            Iterable<? extends RecordBatch> batches = log.read(offset, 1024 * 1024, FetchIsolation.LOG_END, true).records.batches();
            RecordBatch head = batches.iterator().next();
            assertEquals(offset, head.lastOffset(), "Offsets not equal");

            Record expected = messageSets[i].records().iterator().next();
            Record actual = head.iterator().next();
            assertEquals(expected.key(), actual.key(), "Keys not equal at offset " + offset);
            assertEquals(expected.value(), actual.value(), "Values not equal at offset " + offset);
            assertEquals(expected.timestamp(), actual.timestamp(), "Timestamps not equal at offset " + offset);
            offset = head.lastOffset() + 1;
        }
        Records lastRead = log.read(numMessages, 1024 * 1024, FetchIsolation.LOG_END, true).records;
        assertFalse(lastRead.records().iterator().hasNext(), "Should be no more messages");

        // check that rolling the log forced a flush, the flush is async so retry in case of failure
        TestUtils.retryOnExceptionWithTimeout(1000L, () ->
                assertTrue(log.recoveryPoint() >= log.activeSegment().baseOffset(), "Log roll should have forced flush")
        );
    }

    /**
     * Test reads at offsets that fall within compressed message set boundaries.
     */
    @Test
    public void testCompressedMessages() throws IOException {
        // this log should roll after every message set
        LogConfig logConfig = new LogTestUtils.LogConfigBuilder()
                .segmentBytes(110)
                .build();
        log = createLog(logDir, logConfig);

        // append 2 compressed message sets, each with two messages giving offsets 0, 1, 2, 3
        log.appendAsLeader(MemoryRecords.withRecords(Compression.gzip().build(),
                new SimpleRecord("hello".getBytes()), new SimpleRecord("there".getBytes())), 0);
        log.appendAsLeader(MemoryRecords.withRecords(Compression.gzip().build(),
                new SimpleRecord("alpha".getBytes()), new SimpleRecord("beta".getBytes())), 0);

        // we should always get the first message in the compressed set when reading any offset in the set
        assertEquals(0, read(log, 0).iterator().next().offset(), "Read at offset 0 should produce 0");
        assertEquals(0, read(log, 1).iterator().next().offset(), "Read at offset 1 should produce 0");
        assertEquals(2, read(log, 2).iterator().next().offset(), "Read at offset 2 should produce 2");
        assertEquals(2, read(log, 3).iterator().next().offset(), "Read at offset 3 should produce 2");
    }

    private Iterable<Record> read(UnifiedLog log, long offset) throws IOException {
        return log.read(offset, 4096, FetchIsolation.LOG_END, true).records.records();
    }

    /**
     * Test garbage collecting old segments
     */
    @Test
    public void testThatGarbageCollectingSegmentsDoesntChangeOffset() throws IOException {
        for (int messagesToAppend : List.of(0, 1, 25)) {
            logDir.mkdirs();
            // first test a log segment starting at 0
            LogConfig logConfig = new LogTestUtils.LogConfigBuilder()
                    .segmentBytes(100)
                    .retentionMs(0)
                    .build();
            UnifiedLog testLog = createLog(logDir, logConfig);
            for (int i = 0; i < messagesToAppend; i++) {
                testLog.appendAsLeader(MemoryRecords.withRecords(Compression.NONE,
                        new SimpleRecord(mockTime.milliseconds() - 10, null, String.valueOf(i).getBytes())), 0);
            }

            long currOffset = testLog.logEndOffset();
            assertEquals(currOffset, messagesToAppend);

            // time goes by; the log file is deleted
            testLog.updateHighWatermark(currOffset);
            testLog.deleteOldSegments();

            assertEquals(currOffset, testLog.logEndOffset(), "Deleting segments shouldn't have changed the logEndOffset");
            assertEquals(1, testLog.numberOfSegments(), "We should still have one segment left");
            assertEquals(0, testLog.deleteOldSegments(), "Further collection shouldn't delete anything");
            assertEquals(currOffset, testLog.logEndOffset(), "Still no change in the logEndOffset");
            assertEquals(currOffset,
                    testLog.appendAsLeader(MemoryRecords.withRecords(Compression.NONE,
                            new SimpleRecord(mockTime.milliseconds(), null, "hello".getBytes())), 0).firstOffset(),
                    "Should still be able to append and should get the logEndOffset assigned to the new append");

            // cleanup the log
            logsToClose.remove(testLog);
            testLog.delete();
        }
    }

    /**
     * MessageSet size shouldn't exceed the config.segmentSize, check that it is properly enforced by
     * appending a message set larger than the config.segmentSize setting and checking that an exception is thrown.
     */
    @Test
    public void testMessageSetSizeCheck() throws IOException {
        MemoryRecords messageSet = MemoryRecords.withRecords(Compression.NONE,
                new SimpleRecord("You".getBytes()), new SimpleRecord("bethe".getBytes()));
        // append messages to log
        int configSegmentSize = messageSet.sizeInBytes() - 1;
        LogConfig logConfig = new LogTestUtils.LogConfigBuilder()
                .segmentBytes(configSegmentSize)
                .build();
        log = createLog(logDir, logConfig);

        assertThrows(RecordBatchTooLargeException.class, () -> log.appendAsLeader(messageSet, 0));
    }

    @Test
    public void testCompactedTopicConstraints() throws IOException {
        SimpleRecord keyedMessage = new SimpleRecord("and here it is".getBytes(), "this message has a key".getBytes());
        SimpleRecord anotherKeyedMessage = new SimpleRecord("another key".getBytes(), "this message also has a key".getBytes());
        SimpleRecord unkeyedMessage = new SimpleRecord("this message does not have a key".getBytes());

        MemoryRecords messageSetWithUnkeyedMessage = MemoryRecords.withRecords(Compression.NONE, unkeyedMessage, keyedMessage);
        MemoryRecords messageSetWithOneUnkeyedMessage = MemoryRecords.withRecords(Compression.NONE, unkeyedMessage);
        MemoryRecords messageSetWithCompressedKeyedMessage = MemoryRecords.withRecords(Compression.gzip().build(), keyedMessage);
        MemoryRecords messageSetWithCompressedUnkeyedMessage = MemoryRecords.withRecords(Compression.gzip().build(), keyedMessage, unkeyedMessage);
        MemoryRecords messageSetWithKeyedMessage = MemoryRecords.withRecords(Compression.NONE, keyedMessage);
        MemoryRecords messageSetWithKeyedMessages = MemoryRecords.withRecords(Compression.NONE, keyedMessage, anotherKeyedMessage);

        LogConfig logConfig = new LogTestUtils.LogConfigBuilder()
                .cleanupPolicy(TopicConfig.CLEANUP_POLICY_COMPACT)
                .build();
        log = createLog(logDir, logConfig);

        String errorMsgPrefix = "Compacted topic cannot accept message without key";

        RecordValidationException e = assertThrows(RecordValidationException.class,
                () -> log.appendAsLeader(messageSetWithUnkeyedMessage, 0));
        assertInstanceOf(InvalidRecordException.class, e.invalidException());
        assertEquals(1, e.recordErrors().size());
        assertEquals(0, e.recordErrors().get(0).batchIndex);
        assertTrue(e.recordErrors().get(0).message.startsWith(errorMsgPrefix));

        e = assertThrows(RecordValidationException.class,
                () -> log.appendAsLeader(messageSetWithOneUnkeyedMessage, 0));
        assertInstanceOf(InvalidRecordException.class, e.invalidException());
        assertEquals(1, e.recordErrors().size());
        assertEquals(0, e.recordErrors().get(0).batchIndex);
        assertTrue(e.recordErrors().get(0).message.startsWith(errorMsgPrefix));

        e = assertThrows(RecordValidationException.class,
                () -> log.appendAsLeader(messageSetWithCompressedUnkeyedMessage, 0));
        assertInstanceOf(InvalidRecordException.class, e.invalidException());
        assertEquals(1, e.recordErrors().size());
        assertEquals(1, e.recordErrors().get(0).batchIndex);  // batch index is 1
        assertTrue(e.recordErrors().get(0).message.startsWith(errorMsgPrefix));

        // check if metric for NoKeyCompactedTopicRecordsPerSec is logged
        assertEquals(1, KafkaYammerMetrics.defaultRegistry().allMetrics().keySet().stream()
                .filter(k -> k.getMBeanName().endsWith(BrokerTopicMetrics.NO_KEY_COMPACTED_TOPIC_RECORDS_PER_SEC))
                .count());
        assertTrue(meterCount(BrokerTopicMetrics.NO_KEY_COMPACTED_TOPIC_RECORDS_PER_SEC) > 0);

        // the following should succeed without any InvalidMessageException
        log.appendAsLeader(messageSetWithKeyedMessage, 0);
        log.appendAsLeader(messageSetWithKeyedMessages, 0);
        log.appendAsLeader(messageSetWithCompressedKeyedMessage, 0);
    }

    /**
     * We have a max size limit on message appends, check that it is properly enforced by appending a message larger than the
     * setting and checking that an exception is thrown.
     */
    @Test
    public void testMessageSizeCheck() throws IOException {
        MemoryRecords first = MemoryRecords.withRecords(Compression.NONE,
                new SimpleRecord("You".getBytes()), new SimpleRecord("bethe".getBytes()));
        MemoryRecords second = MemoryRecords.withRecords(Compression.NONE,
                new SimpleRecord("change (I need more bytes)... blah blah blah.".getBytes()),
                new SimpleRecord("More padding boo hoo".getBytes()));

        // append messages to log
        int maxMessageSize = second.sizeInBytes() - 1;
        LogConfig logConfig = new LogTestUtils.LogConfigBuilder()
                .maxMessageBytes(maxMessageSize)
                .build();
        log = createLog(logDir, logConfig);

        // should be able to append the small message
        log.appendAsLeader(first, 0);

        assertThrows(
                RecordTooLargeException.class, 
                () -> log.appendAsLeader(second, 0),
                "Second message set should throw MessageSizeTooLargeException."
        );
    }

    @Test
    public void testMessageSizeCheckInAppendAsFollower() throws IOException {
        MemoryRecords first = MemoryRecords.withRecords(0, Compression.NONE, 0,
                new SimpleRecord("You".getBytes()), new SimpleRecord("bethe".getBytes()));
        MemoryRecords second = MemoryRecords.withRecords(5, Compression.NONE, 0,
                new SimpleRecord("change (I need more bytes)... blah blah blah.".getBytes()),
                new SimpleRecord("More padding boo hoo".getBytes()));

        log = createLog(logDir, new LogTestUtils.LogConfigBuilder()
                .maxMessageBytes(second.sizeInBytes() - 1)
                .build());

        log.appendAsFollower(first, Integer.MAX_VALUE);
        // the second record is larger than limit but appendAsFollower does not validate the size.
        log.appendAsFollower(second, Integer.MAX_VALUE);
    }

    @ParameterizedTest
    @ArgumentsSource(InvalidMemoryRecordsProvider.class)
    public void testInvalidMemoryRecords(MemoryRecords records, Optional<Class<Exception>> expectedException) throws IOException {
        log = createLog(logDir, new LogConfig(new Properties()));
        long previousEndOffset = log.logEndOffsetMetadata().messageOffset;

        if (expectedException.isPresent()) {
            assertThrows(expectedException.get(), () -> log.appendAsFollower(records, Integer.MAX_VALUE));
        } else {
            log.appendAsFollower(records, Integer.MAX_VALUE);
        }

        assertEquals(previousEndOffset, log.logEndOffsetMetadata().messageOffset);
    }

    @Test
    public void testRandomRecords() throws IOException {
        Random random = new Random();
        for (int i = 0; i < 100; i++) {
            int size = random.nextInt(128) + 1;
            byte[] bytes = new byte[size];
            random.nextBytes(bytes);
            MemoryRecords records = MemoryRecords.readableRecords(ByteBuffer.wrap(bytes));

            File tempDir = TestUtils.tempDirectory();
            File randomLogDir = TestUtils.randomPartitionLogDir(tempDir);
            UnifiedLog testLog = createLog(randomLogDir, new LogConfig(new Properties()));
            try {
                long previousEndOffset = testLog.logEndOffsetMetadata().messageOffset;

                // Depending on the corruption, unified log sometimes throws and sometimes returns an
                // empty set of batches
                assertThrows(CorruptRecordException.class, () -> {
                    LogAppendInfo info = testLog.appendAsFollower(records, Integer.MAX_VALUE);
                    if (info.firstOffset() == UnifiedLog.UNKNOWN_OFFSET) {
                        throw new CorruptRecordException("Unknown offset is test");
                    }
                });

                assertEquals(previousEndOffset, testLog.logEndOffsetMetadata().messageOffset);
            } finally {
                logsToClose.remove(testLog);
                testLog.close();
                Utils.delete(tempDir);
            }
        }
    }

    @Test
    public void testInvalidLeaderEpoch() throws IOException {
        log = createLog(logDir, new LogConfig(new Properties()));
        long previousEndOffset = log.logEndOffsetMetadata().messageOffset;
        int epoch = log.latestEpoch().orElse(0) + 1;
        int numberOfRecords = 10;

        SimpleRecord[] recordsForBatch = IntStream.range(0, numberOfRecords)
                .mapToObj(n -> new SimpleRecord(String.valueOf(n).getBytes()))
                .toArray(SimpleRecord[]::new);

        MemoryRecords batchWithValidEpoch = MemoryRecords.withRecords(
                previousEndOffset, Compression.NONE, epoch, recordsForBatch);

        MemoryRecords batchWithInvalidEpoch = MemoryRecords.withRecords(
                previousEndOffset + numberOfRecords, Compression.NONE, epoch + 1, recordsForBatch);

        ByteBuffer buffer = ByteBuffer.allocate(batchWithValidEpoch.sizeInBytes() + batchWithInvalidEpoch.sizeInBytes());
        buffer.put(batchWithValidEpoch.buffer());
        buffer.put(batchWithInvalidEpoch.buffer());
        buffer.flip();

        MemoryRecords combinedRecords = MemoryRecords.readableRecords(buffer);
        log.appendAsFollower(combinedRecords, epoch);

        // Check that only the first batch was appended
        assertEquals(previousEndOffset + numberOfRecords, log.logEndOffsetMetadata().messageOffset);
        // Check that the last fetched epoch matches the first batch
        assertEquals(epoch, (int) log.latestEpoch().get());
    }

    @Test
    public void testLogFlushesPartitionMetadataOnAppend() throws IOException {
        log = createLog(logDir, new LogConfig(new Properties()));
        MemoryRecords record = MemoryRecords.withRecords(Compression.NONE, new SimpleRecord("simpleValue".getBytes()));

        Uuid topicId = Uuid.randomUuid();
        log.partitionMetadataFile().get().record(topicId);

        // Should trigger a synchronous flush
        log.appendAsLeader(record, 0);
        assertTrue(log.partitionMetadataFile().get().exists());
        assertEquals(topicId, log.partitionMetadataFile().get().read().topicId());
    }

    @Test
    public void testLogFlushesPartitionMetadataOnClose() throws IOException {
        LogConfig logConfig = new LogConfig(new Properties());
        UnifiedLog firstLog = createLog(logDir, logConfig);
        Uuid topicId = Uuid.randomUuid();
        firstLog.partitionMetadataFile().get().record(topicId);

        // Should trigger a synchronous flush
        firstLog.close();

        // We open the log again, and the partition metadata file should exist with the same ID.
        log = createLog(logDir, logConfig);
        assertTrue(log.partitionMetadataFile().get().exists());
        assertEquals(topicId, log.partitionMetadataFile().get().read().topicId());
    }

    @Test
    public void testLogRecoversTopicId() throws IOException {
        LogConfig logConfig = new LogConfig(new Properties());
        UnifiedLog firstLog = createLog(logDir, logConfig);
        Uuid topicId = Uuid.randomUuid();
        firstLog.assignTopicId(topicId);
        firstLog.close();

        // test recovery case
        log = createLog(logDir, logConfig);
        assertTrue(log.topicId().isPresent());
        assertEquals(topicId, log.topicId().get());
    }

    @Test
    public void testLogFailsWhenInconsistentTopicIdSet() throws IOException {
        LogConfig logConfig = new LogConfig(new Properties());
        UnifiedLog firstLog = createLog(logDir, logConfig);
        Uuid topicId = Uuid.randomUuid();
        firstLog.assignTopicId(topicId);
        firstLog.close();

        // test creating a log with a new ID
        assertThrows(InconsistentTopicIdException.class, () ->
                createLog(logDir, logConfig, 0L, 0L, brokerTopicStats, mockTime.scheduler, mockTime,
                        producerStateManagerConfig, false, Optional.of(Uuid.randomUuid()), false));
    }

    /**
     * Test building the time index on the follower by setting assignOffsets to false.
     */
    @Test
    public void testBuildTimeIndexWhenNotAssigningOffsets() throws IOException {
        int numMessages = 100;
        LogConfig logConfig = new LogTestUtils.LogConfigBuilder()
                .segmentBytes(10000)
                .indexIntervalBytes(1)
                .build();
        log = createLog(logDir, logConfig);

        for (int i = 0; i < numMessages; i++) {
            log.appendAsFollower(
                    MemoryRecords.withRecords(100 + i, Compression.NONE, 0,
                            new SimpleRecord(mockTime.milliseconds() + i, String.valueOf(i).getBytes())),
                    Integer.MAX_VALUE);
        }

        int timeIndexEntries = log.logSegments().stream()
                .mapToInt(segment -> {
                    try {
                        return segment.timeIndex().entries();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }).sum();
        assertEquals(numMessages - 1, timeIndexEntries,
                "There should be " + (numMessages - 1) + " time index entries");
        assertEquals(mockTime.milliseconds() + numMessages - 1,
                log.activeSegment().timeIndex().lastEntry().timestamp(),
                "The last time index entry should have timestamp " + (mockTime.milliseconds() + numMessages - 1));
    }

    @Test
    public void testFetchOffsetByTimestampIncludesLeaderEpoch() throws IOException {
        LogConfig logConfig = new LogTestUtils.LogConfigBuilder()
                .segmentBytes(200)
                .indexIntervalBytes(1)
                .build();
        log = createLog(logDir, logConfig);

        assertEquals(new OffsetResultHolder(Optional.empty()),
                log.fetchOffsetByTimestamp(0L, Optional.empty()));

        long firstTimestamp = mockTime.milliseconds();
        int firstLeaderEpoch = 0;
        log.appendAsLeader(singletonRecords(TestUtils.randomBytes(10), firstTimestamp), firstLeaderEpoch);

        long secondTimestamp = firstTimestamp + 1;
        int secondLeaderEpoch = 1;
        log.appendAsLeader(singletonRecords(TestUtils.randomBytes(10), secondTimestamp), secondLeaderEpoch);

        assertEquals(new OffsetResultHolder(new FileRecords.TimestampAndOffset(firstTimestamp, 0L, Optional.of(firstLeaderEpoch))),
                log.fetchOffsetByTimestamp(firstTimestamp, Optional.empty()));
        assertEquals(new OffsetResultHolder(new FileRecords.TimestampAndOffset(secondTimestamp, 1L, Optional.of(secondLeaderEpoch))),
                log.fetchOffsetByTimestamp(secondTimestamp, Optional.empty()));

        assertEquals(new OffsetResultHolder(new FileRecords.TimestampAndOffset(ListOffsetsResponse.UNKNOWN_TIMESTAMP, 0L, Optional.of(firstLeaderEpoch))),
                log.fetchOffsetByTimestamp(ListOffsetsRequest.EARLIEST_TIMESTAMP, Optional.empty()));
        assertEquals(new OffsetResultHolder(new FileRecords.TimestampAndOffset(ListOffsetsResponse.UNKNOWN_TIMESTAMP, 0L, Optional.of(firstLeaderEpoch))),
                log.fetchOffsetByTimestamp(ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP, Optional.empty()));
        assertEquals(new OffsetResultHolder(new FileRecords.TimestampAndOffset(ListOffsetsResponse.UNKNOWN_TIMESTAMP, 2L, Optional.of(secondLeaderEpoch))),
                log.fetchOffsetByTimestamp(ListOffsetsRequest.LATEST_TIMESTAMP, Optional.empty()));

        // The cache can be updated directly after a leader change.
        // The new latest offset should reflect the updated epoch.
        log.assignEpochStartOffset(2, 2L);

        assertEquals(new OffsetResultHolder(new FileRecords.TimestampAndOffset(ListOffsetsResponse.UNKNOWN_TIMESTAMP, 2L, Optional.of(2))),
                log.fetchOffsetByTimestamp(ListOffsetsRequest.LATEST_TIMESTAMP, Optional.empty()));
    }

    @Test
    public void testFetchOffsetByTimestampWithMaxTimestampIncludesTimestamp() throws IOException {
        LogConfig logConfig = new LogTestUtils.LogConfigBuilder()
                .segmentBytes(200)
                .indexIntervalBytes(1)
                .build();
        log = createLog(logDir, logConfig);

        assertEquals(new OffsetResultHolder(Optional.empty()),
                log.fetchOffsetByTimestamp(0L, Optional.empty()));

        long firstTimestamp = mockTime.milliseconds();
        int leaderEpoch = 0;
        log.appendAsLeader(singletonRecords(TestUtils.randomBytes(10), firstTimestamp), leaderEpoch);

        long secondTimestamp = firstTimestamp + 1;
        log.appendAsLeader(singletonRecords(TestUtils.randomBytes(10), secondTimestamp), leaderEpoch);
        log.appendAsLeader(singletonRecords(TestUtils.randomBytes(10), firstTimestamp), leaderEpoch);

        assertEquals(new OffsetResultHolder(new FileRecords.TimestampAndOffset(secondTimestamp, 1L, Optional.of(leaderEpoch))),
                log.fetchOffsetByTimestamp(ListOffsetsRequest.MAX_TIMESTAMP, Optional.empty()));
    }

    @Test
    public void testFetchOffsetByTimestampFromRemoteStorage() throws Exception {
        DelayedOperationPurgatory<DelayedRemoteListOffsets> purgatory = new DelayedOperationPurgatory<DelayedRemoteListOffsets>("RemoteListOffsets", 0);
        RemoteLogManager remoteLogManager = spy(new RemoteLogManager(createRemoteLogManagerConfig(),
                0,
                logDir.getAbsolutePath(),
                "clusterId",
                mockTime,
                tp -> Optional.empty(),
                (tp, offset) -> { },
                brokerTopicStats,
                new Metrics(),
                Optional.empty()));
        remoteLogManager.setDelayedOperationPurgatory(purgatory);

        LogConfig logConfig = new LogTestUtils.LogConfigBuilder()
                .segmentBytes(200)
                .indexIntervalBytes(1)
                .remoteLogStorageEnable(true)
                .build();
        log = createLog(logDir, logConfig, true);

        // Note that the log is empty, so remote offset read won't happen
        assertEquals(new OffsetResultHolder(Optional.empty()),
                log.fetchOffsetByTimestamp(0L, Optional.of(remoteLogManager)));

        long firstTimestamp = mockTime.milliseconds();
        int firstLeaderEpoch = 0;
        log.appendAsLeader(singletonRecords(TestUtils.randomBytes(10), firstTimestamp), firstLeaderEpoch);

        long secondTimestamp = firstTimestamp + 1;
        int secondLeaderEpoch = 1;
        log.appendAsLeader(singletonRecords(TestUtils.randomBytes(10), secondTimestamp), secondLeaderEpoch);

        doAnswer(ans -> {
            long timestamp = ans.getArgument(1);
            return Optional.of(timestamp)
                    .filter(t -> t == firstTimestamp)
                    .map(t -> new FileRecords.TimestampAndOffset(t, 0L, Optional.of(firstLeaderEpoch)));
        }).when(remoteLogManager).findOffsetByTimestamp(
                eq(log.topicPartition()), anyLong(), anyLong(), eq(log.leaderEpochCache()));
        log.updateLocalLogStartOffset(1);

        // In the assertions below we test that offset 0 (first timestamp) is in remote and offset 1 (second timestamp) is in local storage.
        assertFetchOffsetByTimestamp(remoteLogManager, new FileRecords.TimestampAndOffset(firstTimestamp, 0L, Optional.of(firstLeaderEpoch)), firstTimestamp, log);
        assertFetchOffsetByTimestamp(remoteLogManager, new FileRecords.TimestampAndOffset(secondTimestamp, 1L, Optional.of(secondLeaderEpoch)), secondTimestamp, log);

        assertEquals(new OffsetResultHolder(new FileRecords.TimestampAndOffset(ListOffsetsResponse.UNKNOWN_TIMESTAMP, 0L, Optional.of(firstLeaderEpoch))),
                log.fetchOffsetByTimestamp(ListOffsetsRequest.EARLIEST_TIMESTAMP, Optional.of(remoteLogManager)));
        assertEquals(new OffsetResultHolder(new FileRecords.TimestampAndOffset(ListOffsetsResponse.UNKNOWN_TIMESTAMP, 1L, Optional.of(secondLeaderEpoch))),
                log.fetchOffsetByTimestamp(ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP, Optional.of(remoteLogManager)));
        assertEquals(new OffsetResultHolder(new FileRecords.TimestampAndOffset(ListOffsetsResponse.UNKNOWN_TIMESTAMP, 2L, Optional.of(secondLeaderEpoch))),
                log.fetchOffsetByTimestamp(ListOffsetsRequest.LATEST_TIMESTAMP, Optional.of(remoteLogManager)));

        log.assignEpochStartOffset(2, 2L);
        assertEquals(new OffsetResultHolder(new FileRecords.TimestampAndOffset(ListOffsetsResponse.UNKNOWN_TIMESTAMP, 2L, Optional.of(2))),
                log.fetchOffsetByTimestamp(ListOffsetsRequest.LATEST_TIMESTAMP, Optional.of(remoteLogManager)));
    }

    @Test
    public void testFetchLatestTieredTimestampNoRemoteStorage() throws IOException {
        LogConfig logConfig = new LogTestUtils.LogConfigBuilder()
                .segmentBytes(200)
                .indexIntervalBytes(1)
                .build();
        log = createLog(logDir, logConfig);

        assertEquals(new OffsetResultHolder(new FileRecords.TimestampAndOffset(ListOffsetsResponse.UNKNOWN_TIMESTAMP, -1, Optional.of(-1))),
                log.fetchOffsetByTimestamp(ListOffsetsRequest.LATEST_TIERED_TIMESTAMP, Optional.empty()));

        long firstTimestamp = mockTime.milliseconds();
        int leaderEpoch = 0;
        log.appendAsLeader(singletonRecords(TestUtils.randomBytes(10), firstTimestamp), leaderEpoch);
        log.appendAsLeader(singletonRecords(TestUtils.randomBytes(10), firstTimestamp + 1), leaderEpoch);

        assertEquals(new OffsetResultHolder(new FileRecords.TimestampAndOffset(ListOffsetsResponse.UNKNOWN_TIMESTAMP, -1, Optional.of(-1))),
                log.fetchOffsetByTimestamp(ListOffsetsRequest.LATEST_TIERED_TIMESTAMP, Optional.empty()));
    }

    @Test
    public void testFetchLatestTieredTimestampWithRemoteStorage() throws Exception {
        DelayedOperationPurgatory<DelayedRemoteListOffsets> purgatory = new DelayedOperationPurgatory<DelayedRemoteListOffsets>("RemoteListOffsets", 0);
        RemoteLogManager remoteLogManager = spy(new RemoteLogManager(createRemoteLogManagerConfig(),
                0,
                logDir.getAbsolutePath(),
                "clusterId",
                mockTime,
                tp -> Optional.empty(),
                (tp, offset) -> { },
                brokerTopicStats,
                new Metrics(),
                Optional.empty()));
        remoteLogManager.setDelayedOperationPurgatory(purgatory);

        LogConfig logConfig = new LogTestUtils.LogConfigBuilder()
                .segmentBytes(200)
                .indexIntervalBytes(1)
                .remoteLogStorageEnable(true)
                .build();
        log = createLog(logDir, logConfig, true);

        // Note that the log is empty, so remote offset read won't happen
        assertEquals(new OffsetResultHolder(Optional.empty()),
                log.fetchOffsetByTimestamp(0L, Optional.of(remoteLogManager)));
        assertEquals(new OffsetResultHolder(new FileRecords.TimestampAndOffset(ListOffsetsResponse.UNKNOWN_TIMESTAMP, 0, Optional.empty())),
                log.fetchOffsetByTimestamp(ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP, Optional.of(remoteLogManager)));

        long firstTimestamp = mockTime.milliseconds();
        int firstLeaderEpoch = 0;
        log.appendAsLeader(singletonRecords(TestUtils.randomBytes(10), firstTimestamp), firstLeaderEpoch);

        long secondTimestamp = firstTimestamp + 1;
        int secondLeaderEpoch = 1;
        log.appendAsLeader(singletonRecords(TestUtils.randomBytes(10), secondTimestamp), secondLeaderEpoch);

        doAnswer(ans -> {
            long timestamp = ans.getArgument(1);
            return Optional.of(timestamp)
                    .filter(t -> t == firstTimestamp)
                    .map(t -> new FileRecords.TimestampAndOffset(t, 0L, Optional.of(firstLeaderEpoch)));
        }).when(remoteLogManager).findOffsetByTimestamp(
                eq(log.topicPartition()), anyLong(), anyLong(), eq(log.leaderEpochCache()));
        log.updateLocalLogStartOffset(1);
        log.updateHighestOffsetInRemoteStorage(0);

        // In the assertions below we test that offset 0 (first timestamp) is in remote and offset 1 (second timestamp) is in local storage.
        assertFetchOffsetByTimestamp(remoteLogManager, new FileRecords.TimestampAndOffset(firstTimestamp, 0L, Optional.of(firstLeaderEpoch)), firstTimestamp, log);
        assertFetchOffsetByTimestamp(remoteLogManager, new FileRecords.TimestampAndOffset(secondTimestamp, 1L, Optional.of(secondLeaderEpoch)), secondTimestamp, log);

        assertEquals(new OffsetResultHolder(new FileRecords.TimestampAndOffset(ListOffsetsResponse.UNKNOWN_TIMESTAMP, 0L, Optional.of(firstLeaderEpoch))),
                log.fetchOffsetByTimestamp(ListOffsetsRequest.EARLIEST_TIMESTAMP, Optional.of(remoteLogManager)));
        assertEquals(new OffsetResultHolder(new FileRecords.TimestampAndOffset(ListOffsetsResponse.UNKNOWN_TIMESTAMP, 0L, Optional.of(firstLeaderEpoch))),
                log.fetchOffsetByTimestamp(ListOffsetsRequest.LATEST_TIERED_TIMESTAMP, Optional.of(remoteLogManager)));
        assertEquals(new OffsetResultHolder(new FileRecords.TimestampAndOffset(ListOffsetsResponse.UNKNOWN_TIMESTAMP, 1L, Optional.of(secondLeaderEpoch))),
                log.fetchOffsetByTimestamp(ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP, Optional.of(remoteLogManager)));
        assertEquals(new OffsetResultHolder(new FileRecords.TimestampAndOffset(ListOffsetsResponse.UNKNOWN_TIMESTAMP, 2L, Optional.of(secondLeaderEpoch))),
                log.fetchOffsetByTimestamp(ListOffsetsRequest.LATEST_TIMESTAMP, Optional.of(remoteLogManager)));

        log.assignEpochStartOffset(2, 2L);
        assertEquals(new OffsetResultHolder(new FileRecords.TimestampAndOffset(ListOffsetsResponse.UNKNOWN_TIMESTAMP, 2L, Optional.of(2))),
                log.fetchOffsetByTimestamp(ListOffsetsRequest.LATEST_TIMESTAMP, Optional.of(remoteLogManager)));
    }

    private void assertFetchOffsetByTimestamp(RemoteLogManager remoteLogManager,
                                               FileRecords.TimestampAndOffset expected,
                                               long timestamp,
                                               UnifiedLog testLog) throws Exception {
        OffsetResultHolder offsetResultHolder = testLog.fetchOffsetByTimestamp(timestamp, Optional.of(remoteLogManager));
        assertTrue(offsetResultHolder.futureHolderOpt().isPresent());
        offsetResultHolder.futureHolderOpt().get().taskFuture().get(1, TimeUnit.SECONDS);
        assertTrue(offsetResultHolder.futureHolderOpt().get().taskFuture().isDone());
        assertTrue(offsetResultHolder.futureHolderOpt().get().taskFuture().get().hasTimestampAndOffset());
        assertEquals(expected, offsetResultHolder.futureHolderOpt().get().taskFuture().get().timestampAndOffset().orElse(null));
    }

    private RemoteLogManagerConfig createRemoteLogManagerConfig() {
        Properties props = new Properties();
        props.put(RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP, "true");
        props.put(RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CLASS_NAME_PROP, NoOpRemoteStorageManager.class.getName());
        props.put(RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CLASS_NAME_PROP, NoOpRemoteLogMetadataManager.class.getName());
        props.put(RemoteLogManagerConfig.REMOTE_LOG_READER_THREADS_PROP, "2");
        return new RemoteLogManagerConfig(new AbstractConfig(RemoteLogManagerConfig.configDef(), props));
    }

    private long meterCount(String metricName) {
        return KafkaYammerMetrics.defaultRegistry().allMetrics().entrySet().stream()
                .filter(e -> e.getKey().getMBeanName().endsWith(metricName))
                .map(e -> ((Meter) e.getValue()).count())
                .findFirst()
                .orElseThrow(() -> new AssertionError("Unable to find metric " + metricName));
    }

    @SuppressWarnings("unchecked")
    private Object yammerMetricValue(String name) {
        Gauge<Object> gauge = (Gauge<Object>) KafkaYammerMetrics.defaultRegistry().allMetrics().entrySet().stream()
                .filter(e -> e.getKey().getMBeanName().endsWith(name))
                .findFirst()
                .get()
                .getValue();
        return gauge.value();
    }
}
