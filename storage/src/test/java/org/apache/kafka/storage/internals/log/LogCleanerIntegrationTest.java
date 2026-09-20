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
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.record.internal.CompressionType;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.record.internal.RecordVersion;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.transaction.TransactionLogConfig;
import org.apache.kafka.server.config.ServerConfigs;
import org.apache.kafka.server.metrics.KafkaYammerMetrics;
import org.apache.kafka.server.util.MockTime;
import org.apache.kafka.server.util.ServerTestUtils;
import org.apache.kafka.server.util.ShutdownableThread;
import org.apache.kafka.storage.internals.checkpoint.OffsetCheckpointFile;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;
import org.apache.kafka.test.TestUtils;

import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * This is an integration test that tests the fully integrated log cleaner
 */
public class LogCleanerIntegrationTest {
    private static final Logger log = LoggerFactory.getLogger(LogCleanerIntegrationTest.class);

    private LogCleaner cleaner;
    private final File logDir = TestUtils.tempDirectory();

    private final List<UnifiedLog> logs = new ArrayList<>();
    private static final int DEFAULT_MAX_MESSAGE_SIZE = 128;
    private static final int DEFAULT_DELETE_DELAY = 1000;
    private static final int DEFAULT_SEGMENT_SIZE = 2048;
    private static final long DEFAULT_MIN_COMPACTION_LAG_MS = 0L;
    private static final long DEFAULT_MAX_COMPACTION_LAG_MS = Long.MAX_VALUE;
    private static final long MIN_COMPACTION_LAG = Duration.ofHours(1).toMillis();
    private static final long MAX_COMPACTION_LAG = Duration.ofHours(6).toMillis();
    private static final long CLEANER_BACKOFF_MS = 200L;
    private static final float DEFAULT_MIN_CLEANABLE_DIRTY_RATIO = 0.0F;
    private static final float MIN_CLEANABLE_DIRTY_RATIO = 1.0F;
    private static final int SEGMENT_SIZE = 512;

    private final Compression codec = Compression.lz4().build();

    private int counter;

    private final MockTime time = new MockTime(1400000000000L, 1000L);  // Tue May 13 16:53:20 UTC 2014
    private static final List<TopicPartition> TOPIC_PARTITIONS = List.of(
        new TopicPartition("log", 0),
        new TopicPartition("log", 1),
        new TopicPartition("log", 2)
    );

    private record KeyValueOffset(int key, String value, long firstOffset) { }
    private record ValueAndRecords(String value, MemoryRecords records) { }
    private record LogAndMessages(UnifiedLog log, List<KeyValueOffset> messages) { }

    @BeforeEach
    public void setup() {
        counter = 0;
    }

    @AfterEach
    public void teardown() throws IOException, InterruptedException {
        ServerTestUtils.clearYammerMetrics();
        if (cleaner != null) {
            cleaner.shutdown();
        }
        time.scheduler.shutdown();
        for (UnifiedLog log : logs) {
            log.close();
        }
        Utils.delete(logDir);
    }

    @ParameterizedTest
    @EnumSource(CompressionType.class)
    public void cleanerTest(CompressionType compressionType) throws IOException, InterruptedException {
        Compression codec = Compression.of(compressionType).build();
        cleaner = makeCleaner(TOPIC_PARTITIONS,
            CLEANER_BACKOFF_MS,
            MIN_COMPACTION_LAG,
            SEGMENT_SIZE);
        UnifiedLog theLog = cleaner.logs().get(TOPIC_PARTITIONS.get(0));

        // t = T0
        long t0 = time.milliseconds();
        Map<Integer, Integer> appends0 = writeDupsWithTimestamp(100, 3, theLog, codec, t0);
        long startSizeBlock0 = theLog.size();
        log.debug("total log size at T0: {}", startSizeBlock0);

        LogSegment activeSegAtT0 = theLog.activeSegment();
        log.debug("active segment at T0 has base offset: {}", activeSegAtT0.baseOffset());
        long sizeUpToActiveSegmentAtT0 = calculateSizeUpToOffset(theLog, activeSegAtT0.baseOffset());
        log.debug("log size up to base offset of active segment at T0: {}", sizeUpToActiveSegmentAtT0);

        cleaner.startup();

        // T0 < t < T1
        // advance to a time still less than one compaction lag from start
        time.sleep(MIN_COMPACTION_LAG / 2);
        Thread.sleep(5 * CLEANER_BACKOFF_MS); // give cleaning thread a chance to _not_ clean
        assertEquals(startSizeBlock0, theLog.size(), "There should be no cleaning until the compaction lag has passed");

        // t = T1 > T0 + compactionLag
        // advance to time a bit more than one compaction lag from start
        time.sleep(MIN_COMPACTION_LAG / 2 + 1);
        long t1 = time.milliseconds();

        // write another block of data
        Map<Integer, Integer> appends1 = new HashMap<>(appends0);
        appends1.putAll(writeDupsWithTimestamp(100, 3, theLog, codec, t1));
        long firstBlock1SegmentBaseOffset = activeSegAtT0.baseOffset();

        // the first block should get cleaned
        cleaner.awaitCleaned(new TopicPartition("log", 0), activeSegAtT0.baseOffset(), 60000L);

        // check the data is the same
        Map<Integer, Integer> read1 = readFromLog(theLog);
        assertEquals(appends1, read1, "Contents of the map shouldn't change.");

        long compactedSize = calculateSizeUpToOffset(theLog, activeSegAtT0.baseOffset());
        log.debug("after cleaning the compacted size up to active segment at T0: {}", compactedSize);
        Long lastCleaned = cleaner.cleanerManager().allCleanerCheckpoints().get(new TopicPartition("log", 0));
        assertTrue(lastCleaned >= firstBlock1SegmentBaseOffset,
            String.format("log cleaner should have processed up to offset %d, but lastCleaned=%d",
                firstBlock1SegmentBaseOffset, lastCleaned));
        assertTrue(sizeUpToActiveSegmentAtT0 > compactedSize,
            String.format("log should have been compacted: size up to offset of active segment at T0=%d compacted size=%d",
                sizeUpToActiveSegmentAtT0, compactedSize));
    }

    @Test
    public void testMarksPartitionsAsOfflineAndPopulatesUncleanableMetrics() throws Exception {
        int largeMessageKey = 20;
        ValueAndRecords largeMessage = createLargeSingleMessageSet(largeMessageKey, RecordBatch.CURRENT_MAGIC_VALUE, codec);
        int maxMessageSize = largeMessage.records().sizeInBytes();
        cleaner = makeCleaner(TOPIC_PARTITIONS, maxMessageSize, 100L);

        breakPartitionLog(TOPIC_PARTITIONS.get(0));
        breakPartitionLog(TOPIC_PARTITIONS.get(1));

        cleaner.startup();

        UnifiedLog theLog = cleaner.logs().get(TOPIC_PARTITIONS.get(0));
        UnifiedLog theLog2 = cleaner.logs().get(TOPIC_PARTITIONS.get(1));
        String uncleanableDirectory = theLog.dir().getParent();
        Gauge<Integer> uncleanablePartitionsCountGauge = getGauge("uncleanable-partitions-count", uncleanableDirectory);
        Gauge<Long> uncleanableBytesGauge = getGauge("uncleanable-bytes", uncleanableDirectory);

        TestUtils.waitForCondition(
            () -> uncleanablePartitionsCountGauge.value() == 2,
            2000L,
            "There should be 2 uncleanable partitions");

        List<LogSegment> logSegments = theLog.logSegments();
        LogSegment lastLogSegment = logSegments.get(logSegments.size() - 1);
        List<LogSegment> log2Segments = theLog2.logSegments();
        LogSegment lastLog2Segment = log2Segments.get(log2Segments.size() - 1);

        long expectedTotalUncleanableBytes =
            LogCleanerManager.calculateCleanableBytes(theLog, 0, lastLogSegment.baseOffset()).getValue() +
            LogCleanerManager.calculateCleanableBytes(theLog2, 0, lastLog2Segment.baseOffset()).getValue();
        TestUtils.waitForCondition(
            () -> uncleanableBytesGauge.value() == expectedTotalUncleanableBytes,
            1000L,
            "There should be " + expectedTotalUncleanableBytes + " uncleanable bytes");

        Set<TopicPartition> uncleanablePartitions = cleaner.cleanerManager().uncleanablePartitions(uncleanableDirectory);
        assertTrue(uncleanablePartitions.contains(TOPIC_PARTITIONS.get(0)));
        assertTrue(uncleanablePartitions.contains(TOPIC_PARTITIONS.get(1)));
        assertFalse(uncleanablePartitions.contains(TOPIC_PARTITIONS.get(2)));

        // Delete one partition
        cleaner.logs().remove(TOPIC_PARTITIONS.get(0));
        TestUtils.waitForCondition(
            () -> {
                time.sleep(1000);
                return uncleanablePartitionsCountGauge.value() == 1;
            },
            2000L,
            "There should be 1 uncleanable partition");

        Set<TopicPartition> uncleanablePartitions2 = cleaner.cleanerManager().uncleanablePartitions(uncleanableDirectory);
        assertFalse(uncleanablePartitions2.contains(TOPIC_PARTITIONS.get(0)));
        assertTrue(uncleanablePartitions2.contains(TOPIC_PARTITIONS.get(1)));
        assertFalse(uncleanablePartitions2.contains(TOPIC_PARTITIONS.get(2)));
    }

    @Test
    public void testMaxLogCompactionLag() throws Exception {
        cleaner = makeCleaner(TOPIC_PARTITIONS, CLEANER_BACKOFF_MS, MIN_COMPACTION_LAG, SEGMENT_SIZE,
            MAX_COMPACTION_LAG, MIN_CLEANABLE_DIRTY_RATIO);
        UnifiedLog theLog = cleaner.logs().get(TOPIC_PARTITIONS.get(0));

        long t0 = time.milliseconds();
        writeKeyDups(100, 3, theLog, Compression.NONE, t0, 0, 1);

        long startSizeBlock0 = theLog.size();

        LogSegment activeSegAtT0 = theLog.activeSegment();

        cleaner.startup();

        // advance to a time still less than MAX_COMPACTION_LAG from start
        time.sleep(MAX_COMPACTION_LAG / 2);
        Thread.sleep(5 * CLEANER_BACKOFF_MS); // give cleaning thread a chance to _not_ clean
        assertEquals(startSizeBlock0, theLog.size(), "There should be no cleaning until the max compaction lag has passed");

        // advance to time a bit more than one MAX_COMPACTION_LAG from start
        time.sleep(MAX_COMPACTION_LAG / 2 + 1);
        long t1 = time.milliseconds();

        // write the second block of data: all zero keys
        List<int[]> appends1 = writeKeyDups(100, 1, theLog, Compression.NONE, t1, 0, 0);

        // roll the active segment
        theLog.roll();
        LogSegment activeSegAtT1 = theLog.activeSegment();
        long firstBlockCleanableSegmentOffset = activeSegAtT0.baseOffset();

        // the first block should get cleaned
        cleaner.awaitCleaned(new TopicPartition("log", 0), firstBlockCleanableSegmentOffset, 60000L);

        List<int[]> read1 = readKeyValuePairsFromLog(theLog);
        Long lastCleaned = cleaner.cleanerManager().allCleanerCheckpoints().get(new TopicPartition("log", 0));
        assertTrue(lastCleaned >= firstBlockCleanableSegmentOffset,
            "log cleaner should have processed at least to offset " + firstBlockCleanableSegmentOffset + ", but lastCleaned=" + lastCleaned);

        // minCleanableDirtyRatio will prevent second block of data from compacting
        assertNotEquals(appends1.size(), read1.size(), "log should still contain non-zero keys");

        time.sleep(MAX_COMPACTION_LAG + 1);
        // the second block should get cleaned. only zero keys left
        cleaner.awaitCleaned(new TopicPartition("log", 0), activeSegAtT1.baseOffset(), 60000L);

        List<int[]> read2 = readKeyValuePairsFromLog(theLog);

        assertEquals(appends1.size(), read2.size(), "log should only contain zero keys now");
        for (int i = 0; i < appends1.size(); i++) {
            assertEquals(appends1.get(i)[0], read2.get(i)[0], "key mismatch at index " + i);
            assertEquals(appends1.get(i)[1], read2.get(i)[1], "value mismatch at index " + i);
        }

        Long lastCleaned2 = cleaner.cleanerManager().allCleanerCheckpoints().get(new TopicPartition("log", 0));
        long secondBlockCleanableSegmentOffset = activeSegAtT1.baseOffset();
        assertTrue(lastCleaned2 >= secondBlockCleanableSegmentOffset,
            "log cleaner should have processed at least to offset " + secondBlockCleanableSegmentOffset + ", but lastCleaned=" + lastCleaned2);
    }

    @Test
    public void testIsThreadFailed() throws Exception {
        cleaner = makeCleaner(TOPIC_PARTITIONS, 100000, 100L);
        cleaner.startup();
        assertEquals(0, cleaner.deadThreadCount());
        // we simulate the unexpected error with an interrupt
        cleaner.cleaners().forEach(Thread::interrupt);
        // wait until interruption is propagated to all the threads
        TestUtils.waitForCondition(
            () -> cleaner.cleaners().stream().allMatch(ShutdownableThread::isThreadFailed),
            "Threads didn't terminate unexpectedly");
        assertEquals(cleaner.cleaners().size(), getGauge("DeadThreadCount").value());
        assertEquals(cleaner.cleaners().size(), cleaner.deadThreadCount());
    }

    @ParameterizedTest
    @EnumSource(CompressionType.class)
    public void testCleanerCompaction(CompressionType compressionType) throws Exception {
        Compression codec = Compression.of(compressionType).build();
        int largeMessageKey = 20;
        ValueAndRecords largeMessage = createLargeSingleMessageSet(largeMessageKey, RecordBatch.CURRENT_MAGIC_VALUE, codec);
        int maxMessageSize = largeMessage.records().sizeInBytes();

        cleaner = makeCleaner(TOPIC_PARTITIONS, maxMessageSize, 15000L);
        UnifiedLog theLog = cleaner.logs().get(TOPIC_PARTITIONS.get(0));

        List<KeyValueOffset> appends = writeDups(100, 3, theLog, codec);
        long startSize = theLog.size();
        cleaner.startup();

        long firstDirty = theLog.activeSegment().baseOffset();
        checkLastCleaned("log", 0, firstDirty);
        long compactedSize = theLog.logSegments().stream().mapToLong(LogSegment::size).sum();
        assertTrue(startSize > compactedSize,
            "log should have been compacted: startSize=" + startSize + " compactedSize=" + compactedSize);

        checkLogAfterAppendingDups(theLog, startSize, appends);

        LogAppendInfo appendInfo = theLog.appendAsLeader(largeMessage.records(), 0);
        // move LSO forward to increase compaction bound
        theLog.updateHighWatermark(theLog.logEndOffset());
        long largeMessageOffset = appendInfo.firstOffset();

        List<KeyValueOffset> dups = writeDups(100, 3, theLog, codec, largeMessageKey + 1, RecordBatch.CURRENT_MAGIC_VALUE);
        List<KeyValueOffset> appends2 = new ArrayList<>(appends);
        appends2.add(new KeyValueOffset(largeMessageKey, largeMessage.value(), largeMessageOffset));
        appends2.addAll(dups);
        long firstDirty2 = theLog.activeSegment().baseOffset();
        checkLastCleaned("log", 0, firstDirty2);

        checkLogAfterAppendingDups(theLog, startSize, appends2);

        // simulate deleting a partition, by removing it from logs
        // force a checkpoint
        // and make sure its gone from checkpoint file
        cleaner.logs().remove(TOPIC_PARTITIONS.get(0));
        cleaner.updateCheckpoints(logDir, Optional.of(TOPIC_PARTITIONS.get(0)));
        Map<TopicPartition, Long> checkpoints = new OffsetCheckpointFile(
            new File(logDir, LogCleanerManager.OFFSET_CHECKPOINT_FILE), null).read();
        // we expect partition 0 to be gone
        assertFalse(checkpoints.containsKey(TOPIC_PARTITIONS.get(0)));
    }

    @ParameterizedTest
    @EnumSource(CompressionType.class)
    public void testCleansCombinedCompactAndDeleteTopic(CompressionType compressionType) throws Exception {
        Properties logProps = new Properties();
        int retentionMs = 100000;
        logProps.put(TopicConfig.RETENTION_MS_CONFIG, retentionMs);
        logProps.put(TopicConfig.CLEANUP_POLICY_CONFIG, "compact,delete");

        LogAndMessages result1 = runCleanerAndCheckCompacted(100, compressionType, logProps);
        UnifiedLog theLog = result1.log();

        for (LogSegment segment : theLog.logSegments()) {
            segment.setLastModified(time.milliseconds() - (2L * retentionMs));
        }
        TestUtils.waitForCondition(
            () -> theLog.logStartOffset() == theLog.logEndOffset() && theLog.numberOfSegments() == 1,
            "Timed out waiting for deletion of old segments");

        cleaner.shutdown();
        closeLog(theLog);

        // run the cleaner again to make sure if there are no issues post deletion
        LogAndMessages result2 = runCleanerAndCheckCompacted(20, compressionType, logProps);

        List<KeyValueOffset> read = readFromLogFull(result2.log());
        assertEquals(toMap(result2.messages()), toMap(read), "Contents of the map shouldn't change");
    }

    private LogAndMessages runCleanerAndCheckCompacted(int numKeys, CompressionType compressionType,
                                                       Properties logProps) throws Exception {
        cleaner = makeCleaner(TOPIC_PARTITIONS.subList(0, 1), logProps, 100L);
        UnifiedLog theLog = cleaner.logs().get(TOPIC_PARTITIONS.get(0));

        List<KeyValueOffset> messages = writeDups(numKeys, 3, theLog, Compression.of(compressionType).build());
        long startSize = theLog.size();

        theLog.updateHighWatermark(theLog.logEndOffset());

        long firstDirty = theLog.activeSegment().baseOffset();
        cleaner.startup();

        // should compact the log
        checkLastCleaned("log", 0, firstDirty);
        long compactedSize = theLog.logSegments().stream().mapToLong(LogSegment::size).sum();
        assertTrue(startSize > compactedSize,
            "log should have been compacted: startSize=" + startSize + " compactedSize=" + compactedSize);
        return new LogAndMessages(theLog, messages);
    }

    @ParameterizedTest
    @EnumSource(mode = EnumSource.Mode.EXCLUDE, names = "ZSTD")
    public void testCleanerWithMessageFormatV0V1V2(CompressionType compressionType) throws Exception {
        Compression compression = Compression.of(compressionType).build();
        int largeMessageKey = 20;
        ValueAndRecords largeMessage = createLargeSingleMessageSet(largeMessageKey, RecordBatch.MAGIC_VALUE_V0, compression);
        int maxMessageSize;
        if (compressionType == CompressionType.NONE) {
            maxMessageSize = largeMessage.records().sizeInBytes();
        } else {
            // the broker assigns absolute offsets for message format 0 which potentially causes the compressed size to
            // increase because the broker offsets are larger than the ones assigned by the client
            // adding `6` to the message set size is good enough for this test: it covers the increased message size while
            // still being less than the overhead introduced by the conversion from message format version 0 to 1
            maxMessageSize = largeMessage.records().sizeInBytes() + 6;
        }

        cleaner = makeCleaner(TOPIC_PARTITIONS, maxMessageSize, 15000L);

        UnifiedLog theLog = cleaner.logs().get(TOPIC_PARTITIONS.get(0));
        Properties props = logConfigProperties(maxMessageSize);
        props.put(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, TimestampType.LOG_APPEND_TIME.name);
        LogConfig logConfig = new LogConfig(props);
        theLog.updateConfig(logConfig);

        List<KeyValueOffset> appends1 = writeDups(100, 3, theLog, compression, 0, RecordBatch.MAGIC_VALUE_V0);
        long startSize = theLog.size();
        cleaner.startup();

        long firstDirty = theLog.activeSegment().baseOffset();
        checkLastCleaned("log", 0, firstDirty);
        long compactedSize = theLog.logSegments().stream().mapToLong(LogSegment::size).sum();
        assertTrue(startSize > compactedSize,
            "log should have been compacted: startSize=" + startSize + " compactedSize=" + compactedSize);

        checkLogAfterAppendingDups(theLog, startSize, appends1);

        List<KeyValueOffset> dupsV0 = writeDups(40, 3, theLog, compression, 0, RecordBatch.MAGIC_VALUE_V0);
        LogAppendInfo appendInfo = theLog.appendAsLeaderWithRecordVersion(largeMessage.records(), 0, RecordVersion.V0);
        // move LSO forward to increase compaction bound
        theLog.updateHighWatermark(theLog.logEndOffset());
        long largeMessageOffset = appendInfo.firstOffset();

        // also add some messages with version 1 and version 2 to check that we handle mixed format versions correctly
        List<KeyValueOffset> dupsV1 = writeDups(40, 3, theLog, compression, 30, RecordBatch.MAGIC_VALUE_V1);
        List<KeyValueOffset> dupsV2 = writeDups(5, 3, theLog, compression, 15, RecordBatch.MAGIC_VALUE_V2);

        Set<String> v0RecordKeysWithNoV1V2Updates = new HashSet<>();
        Set<Integer> dupsV1Keys = new HashSet<>();
        for (KeyValueOffset kvo : dupsV1) dupsV1Keys.add(kvo.key());
        Set<Integer> dupsV2Keys = new HashSet<>();
        for (KeyValueOffset kvo : dupsV2) dupsV2Keys.add(kvo.key());
        for (KeyValueOffset kvo : appends1) {
            if (!dupsV1Keys.contains(kvo.key()) && !dupsV2Keys.contains(kvo.key())) {
                v0RecordKeysWithNoV1V2Updates.add(String.valueOf(kvo.key()));
            }
        }

        List<KeyValueOffset> appends2 = new ArrayList<>(appends1);
        appends2.addAll(dupsV0);
        appends2.add(new KeyValueOffset(largeMessageKey, largeMessage.value(), largeMessageOffset));
        appends2.addAll(dupsV1);
        appends2.addAll(dupsV2);

        // roll the log so that all appended messages can be compacted
        theLog.roll();
        long firstDirty2 = theLog.activeSegment().baseOffset();
        checkLastCleaned("log", 0, firstDirty2);

        checkLogAfterAppendingDups(theLog, startSize, appends2);
        checkLogAfterConvertingToV2(compressionType, theLog, logConfig.messageTimestampType, v0RecordKeysWithNoV1V2Updates);
    }

    @ParameterizedTest
    @EnumSource(mode = EnumSource.Mode.EXCLUDE, names = "ZSTD")
    public void testCleaningNestedMessagesWithV0V1(CompressionType compressionType) throws Exception {
        Compression compression = Compression.of(compressionType).build();
        int maxMessageSize = 192;
        cleaner = makeCleaner(TOPIC_PARTITIONS, maxMessageSize, 15000L, 256);

        UnifiedLog theLog = cleaner.logs().get(TOPIC_PARTITIONS.get(0));
        LogConfig logConfig = new LogConfig(logConfigProperties(new Properties(), maxMessageSize,
            DEFAULT_MIN_CLEANABLE_DIRTY_RATIO, DEFAULT_MIN_COMPACTION_LAG_MS,
            DEFAULT_DELETE_DELAY, 256, DEFAULT_MAX_COMPACTION_LAG_MS));
        theLog.updateConfig(logConfig);

        // with compression enabled, these messages will be written as a single message containing all the individual messages
        List<KeyValueOffset> appendsV0 = new ArrayList<>(writeDupsSingleMessageSet(2, 3, theLog, compression, 0, RecordBatch.MAGIC_VALUE_V0));
        appendsV0.addAll(writeDupsSingleMessageSet(2, 2, theLog, compression, 3, RecordBatch.MAGIC_VALUE_V0));

        List<KeyValueOffset> appendsV1 = new ArrayList<>(writeDupsSingleMessageSet(2, 2, theLog, compression, 4, RecordBatch.MAGIC_VALUE_V1));
        appendsV1.addAll(writeDupsSingleMessageSet(2, 2, theLog, compression, 4, RecordBatch.MAGIC_VALUE_V1));
        appendsV1.addAll(writeDupsSingleMessageSet(2, 2, theLog, compression, 6, RecordBatch.MAGIC_VALUE_V1));

        List<KeyValueOffset> appends = new ArrayList<>(appendsV0);
        appends.addAll(appendsV1);

        Set<Integer> appendsV1Keys = new HashSet<>();
        for (KeyValueOffset kvo : appendsV1) appendsV1Keys.add(kvo.key());
        Set<String> v0RecordKeysWithNoV1V2Updates = new HashSet<>();
        for (KeyValueOffset kvo : appendsV0) {
            if (!appendsV1Keys.contains(kvo.key())) {
                v0RecordKeysWithNoV1V2Updates.add(String.valueOf(kvo.key()));
            }
        }

        // roll the log so that all appended messages can be compacted
        theLog.roll();
        long startSize = theLog.size();
        cleaner.startup();

        long firstDirty = theLog.activeSegment().baseOffset();
        assertTrue(firstDirty >= appends.size()); // ensure we clean data from V0 and V1

        checkLastCleaned("log", 0, firstDirty);
        long compactedSize = theLog.logSegments().stream().mapToLong(LogSegment::size).sum();
        assertTrue(startSize > compactedSize,
            "log should have been compacted: startSize=" + startSize + " compactedSize=" + compactedSize);

        checkLogAfterAppendingDups(theLog, startSize, appends);
        checkLogAfterConvertingToV2(compressionType, theLog, logConfig.messageTimestampType, v0RecordKeysWithNoV1V2Updates);
    }

    @ParameterizedTest
    @EnumSource(CompressionType.class)
    @SuppressWarnings("removal")
    public void cleanerConfigUpdateTest(CompressionType compressionType) throws Exception {
        Compression codec = Compression.of(compressionType).build();
        int largeMessageKey = 20;
        ValueAndRecords largeMessage = createLargeSingleMessageSet(largeMessageKey, RecordBatch.CURRENT_MAGIC_VALUE, codec);
        int maxMessageSize = largeMessage.records().sizeInBytes();

        cleaner = makeCleaner(TOPIC_PARTITIONS, 1L, maxMessageSize, 1);
        UnifiedLog theLog = cleaner.logs().get(TOPIC_PARTITIONS.get(0));

        writeDups(100, 3, theLog, codec);
        long startSize = theLog.size();
        cleaner.startup();
        assertEquals(1, cleaner.cleanerCount());

        // Verify no cleaning with LogCleanerIoBufferSizeProp=1
        long firstDirty = theLog.activeSegment().baseOffset();
        TopicPartition topicPartition = new TopicPartition("log", 0);
        cleaner.awaitCleaned(topicPartition, firstDirty, 10);
        assertTrue(cleaner.cleanerManager().allCleanerCheckpoints().isEmpty(), "Should not have cleaned");

        ConfigDef configDef = Utils.mergeConfigs(List.of(
            CleanerConfig.CONFIG_DEF,
            ServerConfigs.CONFIG_DEF
        ));

        CleanerConfig currentConfig = cleaner.currentConfig();
        Map<String, Object> oldConfigMap = new HashMap<>();
        oldConfigMap.put(CleanerConfig.LOG_CLEANER_THREADS_PROP, currentConfig.numThreads);
        oldConfigMap.put(CleanerConfig.LOG_CLEANER_DEDUPE_BUFFER_SIZE_PROP, currentConfig.dedupeBufferSize);
        oldConfigMap.put(CleanerConfig.LOG_CLEANER_DEDUPE_BUFFER_LOAD_FACTOR_PROP, currentConfig.dedupeBufferLoadFactor);
        oldConfigMap.put(CleanerConfig.LOG_CLEANER_IO_BUFFER_SIZE_PROP, currentConfig.ioBufferSize);
        oldConfigMap.put(ServerConfigs.MESSAGE_MAX_BYTES_CONFIG, currentConfig.maxMessageSize);
        oldConfigMap.put(CleanerConfig.LOG_CLEANER_IO_MAX_BYTES_PER_SECOND_PROP, currentConfig.maxIoBytesPerSecond);
        oldConfigMap.put(CleanerConfig.LOG_CLEANER_BACKOFF_MS_PROP, currentConfig.backoffMs);
        oldConfigMap.put(CleanerConfig.LOG_CLEANER_ENABLE_PROP, currentConfig.enableCleaner);
        CleanerConfig oldCleanerConfig = new CleanerConfig(new AbstractConfig(configDef, oldConfigMap));

        Map<String, Object> newConfigMap = new HashMap<>(oldConfigMap);
        newConfigMap.put(CleanerConfig.LOG_CLEANER_THREADS_PROP, 2);
        newConfigMap.put(CleanerConfig.LOG_CLEANER_IO_BUFFER_SIZE_PROP, 100000);
        CleanerConfig newCleanerConfig = new CleanerConfig(new AbstractConfig(configDef, newConfigMap));

        cleaner.reconfigure(oldCleanerConfig, newCleanerConfig);

        assertEquals(2, cleaner.cleanerCount());
        checkLastCleaned("log", 0, firstDirty);
        long compactedSize = theLog.logSegments().stream().mapToLong(LogSegment::size).sum();
        assertTrue(startSize > compactedSize,
            "log should have been compacted: startSize=" + startSize + " compactedSize=" + compactedSize);
    }

    @Test
    public void testGaugeReadsAreNotAffectedByReconfigure() throws Exception {
        cleaner = makeCleaner(TOPIC_PARTITIONS, CLEANER_BACKOFF_MS, MIN_COMPACTION_LAG, SEGMENT_SIZE);
        cleaner.startup();

        CleanerConfig config1Thread = makeReconfigureConfig(1);
        CleanerConfig config2Thread = makeReconfigureConfig(2);

        var checkError = CompletableFuture.runAsync(() -> {
            var endtime = System.currentTimeMillis() + Duration.ofSeconds(5).toMillis();
            while (System.currentTimeMillis() < endtime) {
                cleaner.maxOverCleanerThreads(t -> t.lastStats().bufferUtilization());
                cleaner.deadThreadCount();
            }
        });

        var updateCleaner = CompletableFuture.runAsync(() -> {
            var useOne = true;
            var endtime = System.currentTimeMillis() + Duration.ofSeconds(5).toMillis();
            while (System.currentTimeMillis() < endtime) {
                CleanerConfig oldCfg = useOne ? config2Thread : config1Thread;
                CleanerConfig newCfg = useOne ? config1Thread : config2Thread;
                cleaner.reconfigure(oldCfg, newCfg);
                useOne = !useOne;
            }
        });

        checkError.join();
        updateCleaner.join();
    }

    private CleanerConfig makeReconfigureConfig(int numThreads) {
        // Extend CleanerConfig.CONFIG_DEF with message.max.bytes, which CleanerConfig(AbstractConfig)
        // reads via ServerConfigs.MESSAGE_MAX_BYTES_CONFIG but which is not part of CleanerConfig's own ConfigDef.
        ConfigDef configDef = new ConfigDef(CleanerConfig.CONFIG_DEF)
                .define("message.max.bytes", ConfigDef.Type.INT,
                        DEFAULT_MAX_MESSAGE_SIZE, ConfigDef.Importance.MEDIUM, "");
        Map<String, Object> props = new HashMap<>();
        props.put(CleanerConfig.LOG_CLEANER_THREADS_PROP, numThreads);
        return new CleanerConfig(new AbstractConfig(configDef, props));
    }

    private void checkLastCleaned(String topic, int partitionId, long firstDirty) throws InterruptedException {
        // wait until cleaning up to base_offset, note that cleaning happens only when "log dirty ratio" is higher than
        // TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG
        TopicPartition topicPartition = new TopicPartition(topic, partitionId);
        cleaner.awaitCleaned(topicPartition, firstDirty, 60000L);
        Long lastCleaned = cleaner.cleanerManager().allCleanerCheckpoints().get(topicPartition);
        assertTrue(lastCleaned >= firstDirty,
            "log cleaner should have processed up to offset " + firstDirty + ", but lastCleaned=" + lastCleaned);
    }

    private void checkLogAfterAppendingDups(UnifiedLog log, long startSize, List<KeyValueOffset> appends) {
        List<KeyValueOffset> read = readFromLogFull(log);
        assertEquals(toMap(appends), toMap(read), "Contents of the map shouldn't change");
        assertTrue(startSize > log.size());
    }

    private Map<Integer, KeyValueOffset> toMap(List<KeyValueOffset> messages) {
        Map<Integer, KeyValueOffset> result = new HashMap<>();
        for (KeyValueOffset kvo : messages) {
            result.put(kvo.key(), kvo);
        }
        return result;
    }

    private List<KeyValueOffset> readFromLogFull(UnifiedLog log) {
        List<KeyValueOffset> result = new ArrayList<>();
        for (LogSegment segment : log.logSegments()) {
            for (Record record : segment.log().records()) {
                int key = Integer.parseInt(LogTestUtils.readString(record.key()));
                String value = LogTestUtils.readString(record.value());
                result.add(new KeyValueOffset(key, value, record.offset()));
            }
        }
        return result;
    }

    private List<KeyValueOffset> writeDupsSingleMessageSet(int numKeys, int numDups, UnifiedLog log,
                                                           Compression codec, int startKey,
                                                           byte magicValue) throws IOException {
        List<KeyValueOffset> kvs = new ArrayList<>();
        List<SimpleRecord> records = new ArrayList<>();
        for (int i = 0; i < numDups; i++) {
            for (int key = startKey; key < startKey + numKeys; key++) {
                String value = String.valueOf(counter());
                incCounter();
                kvs.add(new KeyValueOffset(key, value, 0));
                records.add(new SimpleRecord(
                    time.milliseconds(),
                    String.valueOf(key).getBytes(),
                    value.getBytes()));
            }
        }

        LogAppendInfo appendInfo = log.appendAsLeaderWithRecordVersion(
            MemoryRecords.withRecords(magicValue, codec, records.toArray(new SimpleRecord[0])),
            0, RecordVersion.lookup(magicValue));
        // move LSO forward to increase compaction bound
        log.updateHighWatermark(log.logEndOffset());

        long[] offsets = LongStream.rangeClosed(appendInfo.firstOffset(), appendInfo.lastOffset()).toArray();

        List<KeyValueOffset> result = new ArrayList<>();
        for (int i = 0; i < kvs.size(); i++) {
            KeyValueOffset kvo = kvs.get(i);
            result.add(new KeyValueOffset(kvo.key(), kvo.value(), offsets[i]));
        }
        return result;
    }

    private void checkLogAfterConvertingToV2(CompressionType compressionType, UnifiedLog log,
                                             TimestampType timestampType, Set<String> keysForV0RecordsWithNoV1V2Updates) {
        for (LogSegment segment : log.logSegments()) {
            for (RecordBatch recordBatch : segment.log().batches()) {
                // Uncompressed v0/v1 records are always converted into single record v2 batches via compaction if they are retained
                // Compressed v0/v1 record batches are converted into record batches v2 with one or more records (depending on the
                // number of retained records after compaction)
                assertEquals(RecordVersion.V2.value, recordBatch.magic());
                List<Record> recordList = new ArrayList<>();
                recordBatch.forEach(recordList::add);
                if (compressionType == CompressionType.NONE) {
                    assertEquals(1, recordList.size());
                } else {
                    assertFalse(recordList.isEmpty());
                }

                Record firstRecord = recordBatch.iterator().next();
                String firstRecordKey = LogTestUtils.readString(firstRecord.key());
                if (keysForV0RecordsWithNoV1V2Updates.contains(firstRecordKey)) {
                    assertEquals(TimestampType.CREATE_TIME, recordBatch.timestampType());
                } else {
                    assertEquals(timestampType, recordBatch.timestampType());
                }

                for (Record record : recordBatch) {
                    String recordKey = LogTestUtils.readString(record.key());
                    if (keysForV0RecordsWithNoV1V2Updates.contains(recordKey)) {
                        assertEquals(RecordBatch.NO_TIMESTAMP, record.timestamp(),
                            "Record " + recordKey + " with unexpected timestamp ");
                    } else {
                        assertNotEquals(RecordBatch.NO_TIMESTAMP, record.timestamp(),
                            "Record " + recordKey + " with unexpected timestamp " + RecordBatch.NO_TIMESTAMP);
                    }
                }
            }
        }
    }

    private void breakPartitionLog(TopicPartition tp) throws IOException {
        UnifiedLog theLog = cleaner.logs().get(tp);
        writeDups(20, 3, theLog, codec);

        List<LogSegment> segments = theLog.logSegments();
        LogSegment lastSegment = segments.get(segments.size() - 1);
        File partitionFile = lastSegment.log().file();
        try (PrintWriter writer = new PrintWriter(partitionFile)) {
            writer.write("jogeajgoea");
        }

        writeDups(20, 3, theLog, codec);
    }

    @SuppressWarnings("unchecked")
    private <T> Gauge<T> getGauge(String metricName) {
        for (Map.Entry<MetricName, Metric> entry : KafkaYammerMetrics.defaultRegistry().allMetrics().entrySet()) {
            MetricName name = entry.getKey();
            if (name.getName().endsWith(metricName) && name.getScope() == null) {
                return (Gauge<T>) entry.getValue();
            }
        }
        throw new AssertionError("Unable to find metric: " + metricName);
    }

    @SuppressWarnings("unchecked")
    private <T> Gauge<T> getGauge(String metricName, String metricScope) {
        for (Map.Entry<MetricName, Metric> entry : KafkaYammerMetrics.defaultRegistry().allMetrics().entrySet()) {
            MetricName name = entry.getKey();
            if (name.getName().endsWith(metricName) && name.getScope() != null && name.getScope().endsWith(metricScope)) {
                return (Gauge<T>) entry.getValue();
            }
        }
        throw new AssertionError("Unable to find metric: " + metricName + " with scope ending in " + metricScope);
    }

    private long calculateSizeUpToOffset(UnifiedLog log, long offset) {
        long size = 0;
        for (LogSegment segment : log.logSegments(0L, offset)) {
            size += segment.size();
        }
        return size;
    }

    private Map<Integer, Integer> readFromLog(UnifiedLog log) {
        Map<Integer, Integer> result = new HashMap<>();
        for (KeyValueOffset kvo : readFromLogFull(log)) {
            result.put(kvo.key(), Integer.parseInt(kvo.value()));
        }
        return result;
    }

    private List<int[]> readKeyValuePairsFromLog(UnifiedLog log) {
        List<int[]> result = new ArrayList<>();
        for (KeyValueOffset kvo : readFromLogFull(log)) {
            result.add(new int[]{kvo.key(), Integer.parseInt(kvo.value())});
        }
        return result;
    }

    private List<int[]> writeKeyDups(int numKeys, int numDups, UnifiedLog log, Compression codec,
                                     long timestamp, int startValue, int step) throws IOException {
        List<int[]> result = new ArrayList<>();
        int valCounter = startValue;
        for (int i = 0; i < numDups; i++) {
            for (int key = 0; key < numKeys; key++) {
                int curValue = valCounter;
                log.appendAsLeader(
                    LogTestUtils.singletonRecords(
                        String.valueOf(curValue).getBytes(),
                        codec,
                        String.valueOf(key).getBytes(),
                        timestamp),
                    0);
                // move LSO forward to increase compaction bound
                log.updateHighWatermark(log.logEndOffset());
                valCounter += step;
                result.add(new int[]{key, curValue});
            }
        }
        return result;
    }

    private Map<Integer, Integer> writeDupsWithTimestamp(int numKeys, int numDups, UnifiedLog log,
                                                          Compression codec, long timestamp) throws IOException {
        Map<Integer, Integer> result = new HashMap<>();
        for (int i = 0; i < numDups; i++) {
            for (int key = 0; key < numKeys; key++) {
                int count = counter();
                log.appendAsLeader(
                    LogTestUtils.singletonRecords(
                        String.valueOf(count).getBytes(),
                        codec,
                        String.valueOf(key).getBytes(),
                        timestamp),
                    0);
                // move LSO forward to increase compaction bound
                log.updateHighWatermark(log.logEndOffset());
                incCounter();
                result.put(key, count);
            }
        }
        return result;
    }

    private Properties logConfigProperties(Properties propertyOverrides,
                                           int maxMessageSize,
                                           float minCleanableDirtyRatio,
                                           long minCompactionLagMs,
                                           int deleteDelay,
                                           int segmentSize,
                                           long maxCompactionLagMs) {
        Properties props = new Properties();
        props.put(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, maxMessageSize);
        props.put(LogConfig.INTERNAL_SEGMENT_BYTES_CONFIG, segmentSize);
        props.put(TopicConfig.SEGMENT_INDEX_BYTES_CONFIG, 100 * 1024);
        props.put(TopicConfig.FILE_DELETE_DELAY_MS_CONFIG, deleteDelay);
        props.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
        props.put(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, minCleanableDirtyRatio);
        props.put(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG, minCompactionLagMs);
        props.put(TopicConfig.MAX_COMPACTION_LAG_MS_CONFIG, maxCompactionLagMs);
        props.putAll(propertyOverrides);
        return props;
    }

    private Properties logConfigProperties(int maxMessageSize) {
        return logConfigProperties(new Properties(), maxMessageSize,
            DEFAULT_MIN_CLEANABLE_DIRTY_RATIO, DEFAULT_MIN_COMPACTION_LAG_MS,
            DEFAULT_DELETE_DELAY, DEFAULT_SEGMENT_SIZE, DEFAULT_MAX_COMPACTION_LAG_MS);
    }

    private LogCleaner makeCleaner(Iterable<TopicPartition> partitions,
                                   float minCleanableDirtyRatio,
                                   int numThreads,
                                   long backoffMs,
                                   int maxMessageSize,
                                   long minCompactionLagMs,
                                   int deleteDelay,
                                   int segmentSize,
                                   long maxCompactionLagMs,
                                   Integer cleanerIoBufferSize,
                                   Properties propertyOverrides) throws IOException {

        ConcurrentMap<TopicPartition, UnifiedLog> logMap = new ConcurrentHashMap<>();
        for (TopicPartition partition : partitions) {
            File dir = new File(logDir, partition.topic() + "-" + partition.partition());
            Files.createDirectories(dir.toPath());

            Properties props = logConfigProperties(propertyOverrides,
                maxMessageSize,
                minCleanableDirtyRatio,
                minCompactionLagMs,
                deleteDelay,
                segmentSize,
                maxCompactionLagMs);
            LogConfig logConfig = new LogConfig(props);

            UnifiedLog log = UnifiedLog.create(
                dir,
                logConfig,
                0L,
                0L,
                time.scheduler,
                new BrokerTopicStats(),
                time,
                5 * 60 * 1000,
                new ProducerStateManagerConfig(TransactionLogConfig.PRODUCER_ID_EXPIRATION_MS_DEFAULT, false),
                TransactionLogConfig.PRODUCER_ID_EXPIRATION_CHECK_INTERVAL_MS_DEFAULT,
                new LogDirFailureChannel(10),
                true,
                Optional.empty());
            logMap.put(partition, log);
            logs.add(log);
        }

        int ioBufferSize = cleanerIoBufferSize != null ? cleanerIoBufferSize : maxMessageSize / 2;
        CleanerConfig cleanerConfig = new CleanerConfig(
            numThreads,
            4 * 1024 * 1024L,
            0.9,
            ioBufferSize,
            maxMessageSize,
            Double.MAX_VALUE,
            backoffMs,
            true);

        return new LogCleaner(cleanerConfig,
            List.of(logDir),
            logMap,
            new LogDirFailureChannel(1),
            time);
    }

    private LogCleaner makeCleaner(Iterable<TopicPartition> partitions,
                                   long backoffMs,
                                   long minCompactionLagMs,
                                   int segmentSize) throws IOException {
        return makeCleaner(partitions,
            DEFAULT_MIN_CLEANABLE_DIRTY_RATIO,
            1,
            backoffMs,
            DEFAULT_MAX_MESSAGE_SIZE,
            minCompactionLagMs,
            DEFAULT_DELETE_DELAY,
            segmentSize,
            DEFAULT_MAX_COMPACTION_LAG_MS,
            null,
            new Properties());
    }

    private LogCleaner makeCleaner(Iterable<TopicPartition> partitions,
                                   int maxMessageSize,
                                   long backoffMs) throws IOException {
        return makeCleaner(partitions,
            DEFAULT_MIN_CLEANABLE_DIRTY_RATIO,
            1,
            backoffMs,
            maxMessageSize,
            DEFAULT_MIN_COMPACTION_LAG_MS,
            DEFAULT_DELETE_DELAY,
            DEFAULT_SEGMENT_SIZE,
            DEFAULT_MAX_COMPACTION_LAG_MS,
            null,
            new Properties());
    }

    private LogCleaner makeCleaner(Iterable<TopicPartition> partitions,
                                   int maxMessageSize,
                                   long backoffMs,
                                   int segmentSize) throws IOException {
        return makeCleaner(partitions,
            DEFAULT_MIN_CLEANABLE_DIRTY_RATIO,
            1,
            backoffMs,
            maxMessageSize,
            DEFAULT_MIN_COMPACTION_LAG_MS,
            DEFAULT_DELETE_DELAY,
            segmentSize,
            DEFAULT_MAX_COMPACTION_LAG_MS,
            null,
            new Properties());
    }

    private LogCleaner makeCleaner(Iterable<TopicPartition> partitions,
                                   Properties propertyOverrides,
                                   long backoffMs) throws IOException {
        return makeCleaner(partitions,
            DEFAULT_MIN_CLEANABLE_DIRTY_RATIO,
            1,
            backoffMs,
            DEFAULT_MAX_MESSAGE_SIZE,
            DEFAULT_MIN_COMPACTION_LAG_MS,
            DEFAULT_DELETE_DELAY,
            DEFAULT_SEGMENT_SIZE,
            DEFAULT_MAX_COMPACTION_LAG_MS,
            null,
            propertyOverrides);
    }

    private LogCleaner makeCleaner(Iterable<TopicPartition> partitions,
                                   long backoffMs,
                                   int maxMessageSize,
                                   int cleanerIoBufferSize) throws IOException {
        return makeCleaner(partitions,
            DEFAULT_MIN_CLEANABLE_DIRTY_RATIO,
            1,
            backoffMs,
            maxMessageSize,
            DEFAULT_MIN_COMPACTION_LAG_MS,
            DEFAULT_DELETE_DELAY,
            DEFAULT_SEGMENT_SIZE,
            DEFAULT_MAX_COMPACTION_LAG_MS,
            cleanerIoBufferSize,
            new Properties());
    }

    private LogCleaner makeCleaner(Iterable<TopicPartition> partitions,
                                   long backoffMs,
                                   long minCompactionLagMs,
                                   int segmentSize,
                                   long maxCompactionLagMs,
                                   float minCleanableDirtyRatio) throws IOException {
        return makeCleaner(partitions,
            minCleanableDirtyRatio,
            1,
            backoffMs,
            DEFAULT_MAX_MESSAGE_SIZE,
            minCompactionLagMs,
            DEFAULT_DELETE_DELAY,
            segmentSize,
            maxCompactionLagMs,
            null,
            new Properties());
    }

    private int counter() {
        return counter;
    }

    private void incCounter() {
        counter++;
    }

    private List<KeyValueOffset> writeDups(int numKeys, int numDups, UnifiedLog log, Compression codec,
                                           int startKey, byte magicValue) throws IOException {
        List<KeyValueOffset> results = new ArrayList<>();
        for (int i = 0; i < numDups; i++) {
            for (int key = startKey; key < startKey + numKeys; key++) {
                String value = String.valueOf(counter());
                MemoryRecords records = LogTestUtils.singletonRecords(
                    value.getBytes(),
                    codec,
                    String.valueOf(key).getBytes(),
                    RecordBatch.NO_TIMESTAMP,
                    magicValue);
                LogAppendInfo appendInfo = log.appendAsLeaderWithRecordVersion(
                    records, 0, RecordVersion.lookup(magicValue));
                // move LSO forward to increase compaction bound
                log.updateHighWatermark(log.logEndOffset());
                results.add(new KeyValueOffset(key, value, appendInfo.firstOffset()));
                incCounter();
            }
        }
        return results;
    }

    private List<KeyValueOffset> writeDups(int numKeys, int numDups, UnifiedLog log, Compression codec) throws IOException {
        return writeDups(numKeys, numDups, log, codec, 0, RecordBatch.CURRENT_MAGIC_VALUE);
    }

    private ValueAndRecords createLargeSingleMessageSet(int key, byte messageFormatVersion, Compression codec) {
        Random random = new Random(0);
        StringBuilder sb = new StringBuilder(128);
        for (int i = 0; i < 128; i++) {
            sb.append((char) ('a' + random.nextInt(26)));
        }
        String value = sb.toString();
        MemoryRecords records = LogTestUtils.singletonRecords(
            value.getBytes(),
            codec,
            String.valueOf(key).getBytes(),
            RecordBatch.NO_TIMESTAMP,
            messageFormatVersion);
        return new ValueAndRecords(value, records);
    }

    private void closeLog(UnifiedLog log) {
        log.close();
        logs.remove(log);
    }
}
