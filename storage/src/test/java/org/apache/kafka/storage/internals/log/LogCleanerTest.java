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
import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.message.AbortedTxn;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.record.internal.CompressionType;
import org.apache.kafka.common.record.internal.ControlRecordType;
import org.apache.kafka.common.record.internal.EndTransactionMarker;
import org.apache.kafka.common.record.internal.FileRecords;
import org.apache.kafka.common.record.internal.LegacyRecord;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.MemoryRecordsBuilder;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.record.internal.Records;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.transaction.TransactionLogConfig;
import org.apache.kafka.server.common.RequestLocal;
import org.apache.kafka.server.common.TransactionVersion;
import org.apache.kafka.server.config.ServerConfigs;
import org.apache.kafka.server.config.ServerLogConfigs;
import org.apache.kafka.server.metrics.KafkaMetricsGroup;
import org.apache.kafka.server.metrics.KafkaYammerMetrics;
import org.apache.kafka.server.util.MockTime;
import org.apache.kafka.storage.internals.utils.Throttler;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;
import org.apache.kafka.test.TestUtils;

import com.yammer.metrics.core.MetricName;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.security.DigestException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

/**
 * Unit tests for the log cleaning logic
 */
public class LogCleanerTest {

    private static final Logger LOG = LoggerFactory.getLogger(LogCleanerTest.class);

    private final File tmpdir = TestUtils.tempDirectory();
    private final File dir = TestUtils.randomPartitionLogDir(tmpdir);
    private final LogConfig logConfig;
    private final MockTime time = new MockTime();
    private final Throttler throttler = new Throttler(Double.MAX_VALUE, Long.MAX_VALUE, "throttler", "entries", time);
    private final int tombstoneRetentionMs = 86400000;
    private final long largeTimestamp = Long.MAX_VALUE - tombstoneRetentionMs - 1;
    private final ProducerStateManagerConfig producerStateManagerConfig = new ProducerStateManagerConfig(TransactionLogConfig.PRODUCER_ID_EXPIRATION_MS_DEFAULT, false);

    public LogCleanerTest() {
        Properties logProps = new Properties();
        logProps.put(LogConfig.INTERNAL_SEGMENT_BYTES_CONFIG, 1024);
        logProps.put(TopicConfig.SEGMENT_INDEX_BYTES_CONFIG, 1024);
        logProps.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
        logConfig = new LogConfig(logProps);
    }

    @AfterEach
    public void teardown() throws IOException {
        Utils.swallow(LOG, time.scheduler::shutdown);
        Utils.delete(tmpdir);
    }

    @Test
    public void testRemoveMetricsOnClose() {
        try (MockedConstruction<KafkaMetricsGroup> mockMetricsGroupCtor = mockConstruction(KafkaMetricsGroup.class)) {
            LogCleaner logCleaner = new LogCleaner(new CleanerConfig(true),
                List.of(TestUtils.tempDirectory(), TestUtils.tempDirectory()),
                new ConcurrentHashMap<>(),
                new LogDirFailureChannel(1),
                time);
            Map<String, List<Map<String, String>>> metricsToVerify = new HashMap<>();
            logCleaner.cleanerManager().gaugeMetricNameWithTag().forEach((metricName, tagList) -> {
                List<Map<String, String>> tags = List.copyOf(tagList);
                metricsToVerify.put(metricName, tags);
            });
            // shutdown logCleaner so that metrics are removed
            logCleaner.shutdown();

            var mockMetricsGroup = mockMetricsGroupCtor.constructed().get(0);
            int numMetricsRegistered = LogCleaner.METRIC_NAMES.size();
            verify(mockMetricsGroup, times(numMetricsRegistered)).newGauge(anyString(), any());

            // verify that each metric in `LogCleaner` is removed
            LogCleaner.METRIC_NAMES.forEach(name -> verify(mockMetricsGroup).removeMetric(name));

            // verify that each metric in `LogCleanerManager` is removed
            var mockLogCleanerManagerMetricsGroup = mockMetricsGroupCtor.constructed().get(1);
            LogCleanerManager.GAUGE_METRIC_NAME_NO_TAG.forEach(metricName ->
                verify(mockLogCleanerManagerMetricsGroup).newGauge(eq(metricName), any()));
            metricsToVerify.forEach((metricName, tagList) ->
                tagList.forEach(tags ->
                    verify(mockLogCleanerManagerMetricsGroup).newGauge(eq(metricName), any(), eq(tags))));

            LogCleanerManager.GAUGE_METRIC_NAME_NO_TAG.forEach(name ->
                verify(mockLogCleanerManagerMetricsGroup).removeMetric(name));
            metricsToVerify.forEach((metricName, tagList) ->
                tagList.forEach(tags ->
                    verify(mockLogCleanerManagerMetricsGroup).removeMetric(eq(metricName), eq(tags))));

            // assert that we have verified all invocations on
            verifyNoMoreInteractions(mockMetricsGroup);
            verifyNoMoreInteractions(mockLogCleanerManagerMetricsGroup);
        }
    }

    @Test
    public void testMetricsActiveAfterReconfiguration() {
        LogCleaner logCleaner = new LogCleaner(new CleanerConfig(true),
            List.of(TestUtils.tempDirectory()),
            new ConcurrentHashMap<>(),
            new LogDirFailureChannel(1),
            time);

        try {
            logCleaner.startup();
            List<String> registeredMetrics = KafkaYammerMetrics.defaultRegistry()
                .allMetrics().keySet().stream().map(MetricName::getName).toList();
            List<String> nonexistent = LogCleaner.METRIC_NAMES.stream()
                .filter(metric -> !registeredMetrics.contains(metric)).toList();
            assertEquals(0, nonexistent.size(), nonexistent + " should be existent");

            logCleaner.reconfigure(makeReconfigureConfig(Map.of()), makeReconfigureConfig(Map.of()));

            List<String> registeredMetrics2 = KafkaYammerMetrics.defaultRegistry()
                .allMetrics().keySet().stream().map(MetricName::getName).toList();
            List<String> nonexistent2 = LogCleaner.METRIC_NAMES.stream()
                .filter(n -> !registeredMetrics2.contains(n)).toList();
            assertEquals(0, nonexistent2.size(), nonexistent2 + " should be existent");
        } finally {
            logCleaner.shutdown();
        }
    }

    /**
     * Test simple log cleaning
     */
    @Test
    public void testCleanSegments() throws IOException {
        Cleaner cleaner = makeCleaner(Integer.MAX_VALUE);
        Properties logProps = new Properties();
        logProps.put(LogConfig.INTERNAL_SEGMENT_BYTES_CONFIG, 1024);

        UnifiedLog log = makeLog(LogConfig.fromProps(logConfig.originals(), logProps));

        // append messages to the log until we have four segments
        while (log.numberOfSegments() < 4) {
            log.appendAsLeader(record((int) log.logEndOffset(), (int) log.logEndOffset()), 0);
        }
        List<Long> keysFound = LogTestUtils.keysInLog(log);
        assertEquals(LongStream.range(0L, log.logEndOffset()).boxed().toList(), keysFound);

        // pretend we have the following keys
        List<Long> keys = List.of(1L, 3L, 5L, 7L, 9L);
        var map = new LogTestUtils.FakeOffsetMap(Integer.MAX_VALUE);
        keys.forEach(k -> map.put(key(k), Long.MAX_VALUE));

        // clean the log
        List<LogSegment> segments = log.logSegments().stream().limit(3).toList();
        CleanerStats stats = new CleanerStats(Time.SYSTEM);
        long expectedBytesRead = segments.stream().mapToLong(LogSegment::size).sum();
        List<Long> shouldRemain = LogTestUtils.keysInLog(log).stream().filter(key -> !keys.contains(key)).toList();
        log.updateHighWatermark(segments.get(segments.size() - 1).readNextOffset());
        cleaner.cleanSegments(log, segments, map, 0L, stats, new CleanedTransactionMetadata(), -1);
        assertEquals(shouldRemain, LogTestUtils.keysInLog(log));
        assertEquals(expectedBytesRead, stats.bytesRead());
    }

    @Test
    public void testCleanSegmentsWithConcurrentSegmentDeletion() throws IOException, DigestException {
        CountDownLatch deleteStartLatch = new CountDownLatch(1);
        CountDownLatch deleteCompleteLatch = new CountDownLatch(1);

        // Construct a log instance. The replaceSegments() method of the log instance is overridden so that
        // it waits for another thread to execute deleteOldSegments()
        Properties logProps = new Properties();
        logProps.put(LogConfig.INTERNAL_SEGMENT_BYTES_CONFIG, 1024);
        logProps.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT + "," + TopicConfig.CLEANUP_POLICY_DELETE);
        LogConfig config = LogConfig.fromProps(logConfig.originals(), logProps);
        TopicPartition topicPartition = UnifiedLog.parseTopicPartitionName(dir);
        var logDirFailureChannel = new LogDirFailureChannel(10);
        int maxTransactionTimeoutMs = 5 * 60 * 1000;
        int producerIdExpirationCheckIntervalMs = TransactionLogConfig.PRODUCER_ID_EXPIRATION_CHECK_INTERVAL_MS_DEFAULT;
        var logSegments = new LogSegments(topicPartition);
        var leaderEpochCache = UnifiedLog.createLeaderEpochCache(
            dir, topicPartition, logDirFailureChannel, Optional.empty(), time.scheduler);
        var producerStateManager = new ProducerStateManager(topicPartition, dir,
            maxTransactionTimeoutMs, producerStateManagerConfig, time);
        var offsets = new LogLoader(
            dir,
            topicPartition,
            config,
            time.scheduler,
            time,
            logDirFailureChannel,
            true,
            logSegments,
            0L,
            0L,
            leaderEpochCache,
            producerStateManager,
            new ConcurrentHashMap<>(),
            false
        ).load();
        var localLog = new LocalLog(dir, config, logSegments, offsets.recoveryPoint(),
            offsets.nextOffsetMetadata(), time.scheduler, time, topicPartition, logDirFailureChannel);
        var log = new UnifiedLog(offsets.logStartOffset(),
            localLog,
            new BrokerTopicStats(),
            producerIdExpirationCheckIntervalMs,
            leaderEpochCache,
            producerStateManager,
            Optional.empty(),
            false,
            LogOffsetsListener.NO_OP_OFFSETS_LISTENER) {
            @Override
            public void replaceSegments(List<LogSegment> newSegments, List<LogSegment> oldSegments) throws IOException {
                deleteStartLatch.countDown();
                try {
                    if (!deleteCompleteLatch.await(5000, TimeUnit.MILLISECONDS)) {
                        throw new IllegalStateException("Log segment deletion timed out");
                    }
                } catch (InterruptedException e) {
                    throw new IllegalStateException(e);
                }
                super.replaceSegments(newSegments, oldSegments);
            }
        };

        // Start an async task that executes log.deleteOldSegments() right before replaceSegments() is executed.
        CompletableFuture.runAsync(() -> {
            try {
                deleteStartLatch.await(5000, TimeUnit.MILLISECONDS);
                log.updateHighWatermark(log.activeSegment().baseOffset());
                log.maybeIncrementLogStartOffset(log.activeSegment().baseOffset(), LogStartOffsetIncrementReason.LeaderOffsetIncremented);
                log.updateHighWatermark(log.activeSegment().baseOffset());
                log.deleteOldSegments();
                deleteCompleteLatch.countDown();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        // Append records so that segment number increase to 3
        while (log.numberOfSegments() < 3) {
            log.appendAsLeader(record(0, (int) log.logEndOffset()), 0);
            log.roll(Optional.empty());
        }
        assertEquals(3, log.numberOfSegments());

        // Remember reference to the first log and determine its file name expected for async deletion
        FileRecords firstLogFile = log.logSegments().get(0).log();
        String expectedFileName = Utils.replaceSuffix(firstLogFile.file().getPath(), "", LogFileUtils.DELETED_FILE_SUFFIX);

        // Clean the log. This should trigger replaceSegments() and deleteOldSegments();
        var offsetMap = new LogTestUtils.FakeOffsetMap(Integer.MAX_VALUE);
        Cleaner cleaner = makeCleaner(Integer.MAX_VALUE);
        List<LogSegment> segments = List.copyOf(log.logSegments(0, log.activeSegment().baseOffset()));
        CleanerStats stats = new CleanerStats(Time.SYSTEM);
        cleaner.buildOffsetMap(log, 0, log.activeSegment().baseOffset(), offsetMap, stats);
        log.updateHighWatermark(segments.get(segments.size() - 1).readNextOffset());
        cleaner.cleanSegments(log, segments, offsetMap, 0L, stats, new CleanedTransactionMetadata(), -1);

        // Validate based on the file name that log segment file is renamed exactly once for async deletion
        assertEquals(expectedFileName, firstLogFile.file().getPath());
        assertEquals(2, log.numberOfSegments());
    }

    @Test
    public void testSizeTrimmedForPreallocatedAndCompactedTopic() throws IOException, DigestException {
        int originalMaxFileSize = 1024;
        Cleaner cleaner = makeCleaner(2);
        Properties logProps = new Properties();
        logProps.put(LogConfig.INTERNAL_SEGMENT_BYTES_CONFIG, originalMaxFileSize);
        logProps.put(TopicConfig.CLEANUP_POLICY_CONFIG, "compact");
        logProps.put(TopicConfig.PREALLOCATE_CONFIG, "true");
        UnifiedLog log = makeLog(LogConfig.fromProps(logConfig.originals(), logProps));

        log.appendAsLeader(record(0, 0), 0); // offset 0
        log.appendAsLeader(record(1, 1), 0); // offset 1
        log.appendAsLeader(record(0, 0), 0); // offset 2
        log.appendAsLeader(record(1, 1), 0); // offset 3
        log.appendAsLeader(record(0, 0), 0); // offset 4
        // roll the segment, so we can clean the messages already appended
        log.roll();

        // clean the log with only one message removed
        cleaner.clean(new LogToClean(log, 2, log.activeSegment().baseOffset(), false));

        assertTrue(log.logSegments().iterator().next().log().channel().size() < originalMaxFileSize,
            "Cleaned segment file should be trimmed to its real size.");
    }

    @Test
    public void testDuplicateCheckAfterCleaning() throws IOException, DigestException {
        Cleaner cleaner = makeCleaner(Integer.MAX_VALUE);
        Properties logProps = new Properties();
        logProps.put(LogConfig.INTERNAL_SEGMENT_BYTES_CONFIG, 2048);
        UnifiedLog log = makeLog(LogConfig.fromProps(logConfig.originals(), logProps));

        short producerEpoch = 0;
        long pid1 = 1;
        long pid2 = 2;
        long pid3 = 3;
        long pid4 = 4;

        appendIdempotentAsLeader(log, pid1, producerEpoch, List.of(1, 2, 3));
        appendIdempotentAsLeader(log, pid2, producerEpoch, List.of(3, 1, 4));
        appendIdempotentAsLeader(log, pid3, producerEpoch, List.of(1, 4));

        log.roll();
        cleaner.clean(new LogToClean(log, 0L, log.activeSegment().baseOffset(), false));
        assertEquals(List.of(2L, 5L, 7L), lastOffsetsPerBatchInLog(log));
        assertEquals(Map.of(pid1, 2, pid2, 2, pid3, 1), lastSequencesInLog(log));
        assertEquals(List.of(2L, 3L, 1L, 4L), LogTestUtils.keysInLog(log));
        assertEquals(List.of(1L, 3L, 6L, 7L), offsetsInLog(log));

        // we have to reload the log to validate that the cleaner maintained sequence numbers correctly
        log.close();
        log = makeLog(LogConfig.fromProps(logConfig.originals(), logProps));

        // check duplicate append from producer 1
        LogAppendInfo logAppendInfo = appendIdempotentAsLeader(log, pid1, producerEpoch, List.of(1, 2, 3));
        assertEquals(0L, logAppendInfo.firstOffset());
        assertEquals(2L, logAppendInfo.lastOffset());

        // check duplicate append from producer 3
        logAppendInfo = appendIdempotentAsLeader(log, pid3, producerEpoch, List.of(1, 4));
        assertEquals(6L, logAppendInfo.firstOffset());
        assertEquals(7L, logAppendInfo.lastOffset());

        // check duplicate append from producer 2
        logAppendInfo = appendIdempotentAsLeader(log, pid2, producerEpoch, List.of(3, 1, 4));
        assertEquals(3L, logAppendInfo.firstOffset());
        assertEquals(5L, logAppendInfo.lastOffset());

        // do one more append and a round of cleaning to force another deletion from producer 1's batch
        appendIdempotentAsLeader(log, pid4, producerEpoch, List.of(2));
        log.roll();
        cleaner.clean(new LogToClean(log, 0L, log.activeSegment().baseOffset(), false));
        assertEquals(Map.of(pid1, 2, pid2, 2, pid3, 1, pid4, 0), lastSequencesInLog(log));
        assertEquals(List.of(2L, 5L, 7L, 8L), lastOffsetsPerBatchInLog(log));
        assertEquals(List.of(3L, 1L, 4L, 2L), LogTestUtils.keysInLog(log));
        assertEquals(List.of(3L, 6L, 7L, 8L), offsetsInLog(log));

        log.close();
        log = makeLog(LogConfig.fromProps(logConfig.originals(), logProps));

        // duplicate append from producer1 should still be fine
        logAppendInfo = appendIdempotentAsLeader(log, pid1, producerEpoch, List.of(1, 2, 3));
        assertEquals(0L, logAppendInfo.firstOffset());
        assertEquals(2L, logAppendInfo.lastOffset());
    }

    private void assertAllAbortedTxns(List<AbortedTxn> expectedAbortedTxns, UnifiedLog log) {
        List<AbortedTxn> abortedTxns = log.collectAbortedTransactions(0L, log.logEndOffset());
        assertEquals(expectedAbortedTxns, abortedTxns);
    }

    private void assertAllTransactionsComplete(UnifiedLog log) {
        assertTrue(log.activeProducers().stream().allMatch(p -> p.currentTxnStartOffset() == -1));
    }

    @Test
    public void testMultiPassSegmentCleaningWithAbortedTransactions() throws IOException, DigestException {
        // Verify that the log cleaner preserves aborted transaction state (including the index)
        // even if the cleaner cannot clean the whole segment in one pass.

        int deleteRetentionMs = 50000;
        int offsetMapSlots = 4;
        Cleaner cleaner = makeCleaner(Integer.MAX_VALUE);
        Properties logProps = new Properties();
        logProps.put(TopicConfig.DELETE_RETENTION_MS_CONFIG, Integer.toString(deleteRetentionMs));
        UnifiedLog log = makeLog(LogConfig.fromProps(logConfig.originals(), logProps));

        short producerEpoch = 0;
        long producerId1 = 1;
        long producerId2 = 2;

        var appendProducer1 = appendTransactionalAsLeader(log, producerId1, producerEpoch);
        var appendProducer2 = appendTransactionalAsLeader(log, producerId2, producerEpoch);

        Consumer<Long> abort = producerId -> log.appendAsLeader(abortMarker(producerId, producerEpoch), 0, AppendOrigin.REPLICATION,
                RequestLocal.noCaching(), VerificationGuard.SENTINEL, TransactionVersion.TV_UNKNOWN);

        Consumer<Long> commit = producerId -> log.appendAsLeader(commitMarker(producerId, producerEpoch), 0, AppendOrigin.REPLICATION,
                RequestLocal.noCaching(), VerificationGuard.SENTINEL, TransactionVersion.TV_UNKNOWN);

        // Append some transaction data (offset range in parenthesis)
        appendProducer1.append(List.of(1, 2));          // [0, 1]
        appendProducer2.append(List.of(2, 3));          // [2, 3]
        appendProducer1.append(List.of(3, 4));          // [4, 5]
        commit.accept(producerId1);            // [6, 6]
        commit.accept(producerId2);            // [7, 7]
        appendProducer1.append(List.of(2, 3));          // [8, 9]
        abort.accept(producerId1);             // [10, 10]
        appendProducer2.append(List.of(4, 5));          // [11, 12]
        appendProducer1.append(List.of(5, 6));          // [13, 14]
        commit.accept(producerId1);            // [15, 15]
        abort.accept(producerId2);             // [16, 16]
        appendProducer2.append(List.of(6, 7));          // [17, 18]
        commit.accept(producerId2);            // [19, 19]

        log.roll();
        assertEquals(20L, log.logEndOffset());

        List<AbortedTxn> expectedAbortedTxns = List.of(
            new AbortedTxn().setProducerId(producerId1).setFirstOffset(8).setLastOffset(10).setLastStableOffset(11),
            new AbortedTxn().setProducerId(producerId2).setFirstOffset(11).setLastOffset(16).setLastStableOffset(17)
        );

        assertAllTransactionsComplete(log);
        assertAllAbortedTxns(expectedAbortedTxns, log);

        long dirtyOffset = 0L;

        // On the first pass, we should see the data from the aborted transactions deleted,
        // but the markers should remain until the deletion retention time has passed.
        dirtyOffset = cleanSegmentsOnePass(cleaner, log, offsetMapSlots, dirtyOffset);
        assertEquals(4L, dirtyOffset);
        assertEquals(List.of(0L, 2L, 4L, 6L, 7L, 10L, 13L, 15L, 16L, 17L, 19L), batchBaseOffsetsInLog(log));
        assertEquals(List.of(0L, 2L, 3L, 4L, 5L, 6L, 7L, 10L, 13L, 14L, 15L, 16L, 17L, 18L, 19L), offsetsInLog(log));
        assertAllTransactionsComplete(log);
        assertAllAbortedTxns(expectedAbortedTxns, log);

        // On the second pass, no data from the aborted transactions remains. The markers
        // still cannot be removed from the log due to the retention time, but we do not
        // need to record them in the transaction index since they are empty.
        dirtyOffset = cleanSegmentsOnePass(cleaner, log, offsetMapSlots, dirtyOffset);
        assertEquals(14L, dirtyOffset);
        assertEquals(List.of(0L, 2L, 4L, 6L, 7L, 10L, 13L, 15L, 16L, 17L, 19L), batchBaseOffsetsInLog(log));
        assertEquals(List.of(0L, 2L, 4L, 5L, 6L, 7L, 10L, 13L, 14L, 15L, 16L, 17L, 18L, 19L), offsetsInLog(log));
        assertAllTransactionsComplete(log);
        assertAllAbortedTxns(List.of(), log);

        // On the last pass, wait for the retention time to expire. The abort markers
        // (offsets 10 and 16) should be deleted.
        time.sleep(deleteRetentionMs);
        dirtyOffset = cleanSegmentsOnePass(cleaner, log, offsetMapSlots, dirtyOffset);
        assertEquals(20L, dirtyOffset);
        assertEquals(List.of(0L, 2L, 4L, 6L, 7L, 13L, 15L, 17L, 19L), batchBaseOffsetsInLog(log));
        assertEquals(List.of(0L, 2L, 4L, 5L, 6L, 7L, 13L, 15L, 17L, 18L, 19L), offsetsInLog(log));
        assertAllTransactionsComplete(log);
        assertAllAbortedTxns(List.of(), log);
    }

    private long cleanSegmentsOnePass(Cleaner cleaner, UnifiedLog log, int offsetMapSlots, long dirtyOffset) throws IOException, DigestException {
        var offsetMap = new LogTestUtils.FakeOffsetMap(offsetMapSlots);
        List<LogSegment> segments = List.copyOf(log.logSegments(0, log.activeSegment().baseOffset()));
        CleanerStats stats = new CleanerStats(time);
        cleaner.buildOffsetMap(log, dirtyOffset, log.activeSegment().baseOffset(), offsetMap, stats);
        log.updateHighWatermark(segments.get(segments.size() - 1).readNextOffset());
        cleaner.cleanSegments(log, segments, offsetMap, time.milliseconds(), stats, new CleanedTransactionMetadata(), Long.MAX_VALUE);
        return offsetMap.latestOffset() + 1;
    }

    @Test
    public void testBasicTransactionAwareCleaning() throws IOException, DigestException {
        Cleaner cleaner = makeCleaner(Integer.MAX_VALUE);
        Properties logProps = new Properties();
        logProps.put(LogConfig.INTERNAL_SEGMENT_BYTES_CONFIG, 2048);
        UnifiedLog log = makeLog(LogConfig.fromProps(logConfig.originals(), logProps));

        short producerEpoch = 0;
        long pid1 = 1;
        long pid2 = 2;

        var appendProducer1 = appendTransactionalAsLeader(log, pid1, producerEpoch);
        var appendProducer2 = appendTransactionalAsLeader(log, pid2, producerEpoch);

        appendProducer1.append(List.of(1, 2));
        appendProducer2.append(List.of(2, 3));
        appendProducer1.append(List.of(3, 4));
        log.appendAsLeader(abortMarker(pid1, producerEpoch), 0, AppendOrigin.COORDINATOR, RequestLocal.noCaching(),
            VerificationGuard.SENTINEL, TransactionVersion.TV_0.featureLevel());
        log.appendAsLeader(commitMarker(pid2, producerEpoch), 0, AppendOrigin.COORDINATOR,
            RequestLocal.noCaching(), VerificationGuard.SENTINEL, TransactionVersion.TV_0.featureLevel());
        appendProducer1.append(List.of(2));
        log.appendAsLeader(commitMarker(pid1, producerEpoch), 0, AppendOrigin.COORDINATOR, RequestLocal.noCaching(),
            VerificationGuard.SENTINEL, TransactionVersion.TV_0.featureLevel());

        List<AbortedTxn> abortedTransactions = log.collectAbortedTransactions(log.logStartOffset(), log.logEndOffset());

        log.roll();
        cleaner.clean(new LogToClean(log, 0L, log.activeSegment().baseOffset(), false));
        assertEquals(List.of(3L, 2L), LogTestUtils.keysInLog(log));
        assertEquals(List.of(3L, 6L, 7L, 8L, 9L), offsetsInLog(log));

        // ensure the transaction index is still correct
        assertEquals(abortedTransactions, log.collectAbortedTransactions(log.logStartOffset(), log.logEndOffset()));
    }

    @Test
    public void testCleanWithTransactionsSpanningSegments() throws IOException, DigestException {
        Cleaner cleaner = makeCleaner(Integer.MAX_VALUE);
        Properties logProps = new Properties();
        logProps.put(LogConfig.INTERNAL_SEGMENT_BYTES_CONFIG, 1024);
        UnifiedLog log = makeLog(LogConfig.fromProps(logConfig.originals(), logProps));

        short producerEpoch = 0;
        long pid1 = 1;
        long pid2 = 2;
        long pid3 = 3;

        var appendProducer1 = appendTransactionalAsLeader(log, pid1, producerEpoch);
        var appendProducer2 = appendTransactionalAsLeader(log, pid2, producerEpoch);
        var appendProducer3 = appendTransactionalAsLeader(log, pid3, producerEpoch);

        appendProducer1.append(List.of(1, 2));
        appendProducer3.append(List.of(2, 3));
        appendProducer2.append(List.of(3, 4));

        log.roll();

        appendProducer2.append(List.of(5, 6));
        appendProducer3.append(List.of(6, 7));
        appendProducer1.append(List.of(7, 8));
        log.appendAsLeader(abortMarker(pid2, producerEpoch), 0, AppendOrigin.COORDINATOR,
            RequestLocal.noCaching(), VerificationGuard.SENTINEL, TransactionVersion.TV_0.featureLevel());
        appendProducer3.append(List.of(8, 9));
        log.appendAsLeader(commitMarker(pid3, producerEpoch), 0, AppendOrigin.COORDINATOR,
            RequestLocal.noCaching(), VerificationGuard.SENTINEL, TransactionVersion.TV_0.featureLevel());
        appendProducer1.append(List.of(9, 10));
        log.appendAsLeader(abortMarker(pid1, producerEpoch), 0, AppendOrigin.COORDINATOR,
            RequestLocal.noCaching(), VerificationGuard.SENTINEL, TransactionVersion.TV_0.featureLevel());

        // we have only cleaned the records in the first segment
        long dirtyOffset = cleaner.clean(new LogToClean(log, 0L, log.activeSegment().baseOffset(), false)).getKey();
        assertEquals(List.of(2L, 3L, 5L, 6L, 6L, 7L, 7L, 8L, 8L, 9L, 9L, 10L), LogTestUtils.keysInLog(log));

        log.roll();

        // append a couple extra segments in the new segment to ensure we have sequence numbers
        appendProducer2.append(List.of(11));
        appendProducer1.append(List.of(12));

        // finally only the keys from pid3 should remain
        cleaner.clean(new LogToClean(log, dirtyOffset, log.activeSegment().baseOffset(), false));
        assertEquals(List.of(2L, 3L, 6L, 7L, 8L, 9L, 11L, 12L), LogTestUtils.keysInLog(log));
    }

    @Test
    public void testCommitMarkerRemoval() throws IOException, DigestException {
        Cleaner cleaner = makeCleaner(Integer.MAX_VALUE);
        Properties logProps = new Properties();
        logProps.put(LogConfig.INTERNAL_SEGMENT_BYTES_CONFIG, 256);
        UnifiedLog log = makeLog(LogConfig.fromProps(logConfig.originals(), logProps));

        short producerEpoch = 0;
        long producerId = 1L;
        var appendProducer = appendTransactionalAsLeader(log, producerId, producerEpoch);

        appendProducer.append(List.of(1));
        appendProducer.append(List.of(2, 3));
        log.appendAsLeader(commitMarker(producerId, producerEpoch), 0, AppendOrigin.COORDINATOR,
                RequestLocal.noCaching(), VerificationGuard.SENTINEL, TransactionVersion.TV_0.featureLevel());
        appendProducer.append(List.of(2));
        log.appendAsLeader(commitMarker(producerId, producerEpoch), 0, AppendOrigin.COORDINATOR,
                RequestLocal.noCaching(), VerificationGuard.SENTINEL, TransactionVersion.TV_0.featureLevel());
        log.roll();

        // cannot remove the marker in this pass because there are still valid records
        long dirtyOffset = cleaner.doClean(new LogToClean(log, 0L, log.activeSegment().baseOffset(), false), largeTimestamp).getKey();
        assertEquals(List.of(1L, 3L, 2L), LogTestUtils.keysInLog(log));
        assertEquals(List.of(0L, 2L, 3L, 4L, 5L), offsetsInLog(log));

        appendProducer.append(List.of(1, 3));
        log.appendAsLeader(commitMarker(producerId, producerEpoch), 0, AppendOrigin.COORDINATOR, RequestLocal.noCaching(),
            VerificationGuard.SENTINEL, TransactionVersion.TV_0.featureLevel());
        log.roll();

        // the first cleaning preserves the commit marker (at offset 3) since there were still records for the transaction
        dirtyOffset = cleaner.doClean(new LogToClean(log, dirtyOffset, log.activeSegment().baseOffset(), false), largeTimestamp).getKey();
        assertEquals(List.of(2L, 1L, 3L), LogTestUtils.keysInLog(log));
        assertEquals(List.of(3L, 4L, 5L, 6L, 7L, 8L), offsetsInLog(log));

        // clean again with same timestamp to verify marker is not removed early
        dirtyOffset = cleaner.doClean(new LogToClean(log, dirtyOffset, log.activeSegment().baseOffset(), false), largeTimestamp).getKey();
        assertEquals(List.of(2L, 1L, 3L), LogTestUtils.keysInLog(log));
        assertEquals(List.of(3L, 4L, 5L, 6L, 7L, 8L), offsetsInLog(log));

        // clean again with max timestamp to verify the marker is removed
        cleaner.doClean(new LogToClean(log, dirtyOffset, log.activeSegment().baseOffset(), false), Long.MAX_VALUE).getKey();
        assertEquals(List.of(2L, 1L, 3L), LogTestUtils.keysInLog(log));
        assertEquals(List.of(4L, 5L, 6L, 7L, 8L), offsetsInLog(log));
    }

    /**
     * Tests log cleaning with batches that are deleted where no additional messages
     * are available to read in the buffer. Cleaning should continue from the next offset.
     */
    @Test
    public void testDeletedBatchesWithNoMessagesRead() throws IOException, DigestException {
        Cleaner cleaner = makeCleaner(Integer.MAX_VALUE, 100);
        Properties logProps = new Properties();
        logProps.put(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, 100);
        logProps.put(LogConfig.INTERNAL_SEGMENT_BYTES_CONFIG, 1000);
        UnifiedLog log = makeLog(LogConfig.fromProps(logConfig.originals(), logProps));

        short producerEpoch = 0;
        long producerId = 1L;
        var appendProducer = appendTransactionalAsLeader(log, producerId, producerEpoch);

        appendProducer.append(List.of(1));
        log.appendAsLeader(abortMarker(producerId, producerEpoch), 0, AppendOrigin.COORDINATOR, RequestLocal.noCaching(),
            VerificationGuard.SENTINEL, TransactionVersion.TV_0.featureLevel());
        appendProducer.append(List.of(2));
        appendProducer.append(List.of(2));
        log.appendAsLeader(commitMarker(producerId, producerEpoch), 0, AppendOrigin.COORDINATOR, RequestLocal.noCaching(),
            VerificationGuard.SENTINEL, TransactionVersion.TV_0.featureLevel());
        log.roll();

        cleaner.doClean(new LogToClean(log, 0L, log.activeSegment().baseOffset(), false), largeTimestamp);
        assertEquals(List.of(2L), LogTestUtils.keysInLog(log));
        assertEquals(List.of(1L, 3L, 4L), offsetsInLog(log));

        // In the first pass, the deleteHorizon for {Producer2: Commit} is set. In the second pass, it's removed.
        runTwoPassClean(cleaner, new LogToClean(log, 0L, log.activeSegment().baseOffset(), false), largeTimestamp);
        assertEquals(List.of(2L), LogTestUtils.keysInLog(log));
        assertEquals(List.of(3L, 4L), offsetsInLog(log));
    }

    @Test
    public void testCommitMarkerRetentionWithEmptyBatch() throws IOException, DigestException {
        Cleaner cleaner = makeCleaner(Integer.MAX_VALUE);
        Properties logProps = new Properties();
        logProps.put(LogConfig.INTERNAL_SEGMENT_BYTES_CONFIG, 256);
        UnifiedLog log = makeLog(LogConfig.fromProps(logConfig.originals(), logProps));

        short producerEpoch = 0;
        var producer1 = appendTransactionalAsLeader(log, 1L, producerEpoch);
        var producer2 = appendTransactionalAsLeader(log, 2L, producerEpoch);

        // [{Producer1: 2, 3}]
        producer1.append(List.of(2, 3)); // offsets 0, 1
        log.roll();

        // [{Producer1: 2, 3}], [{Producer2: 2, 3}, {Producer2: Commit}]
        producer2.append(List.of(2, 3)); // offsets 2, 3
        log.appendAsLeader(commitMarker(2L, producerEpoch), 0, AppendOrigin.COORDINATOR, RequestLocal.noCaching(),
            VerificationGuard.SENTINEL, TransactionVersion.TV_0.featureLevel()); // offset 4
        log.roll();

        // [{Producer1: 2, 3}], [{Producer2: 2, 3}, {Producer2: Commit}], [{2}, {3}, {Producer1: Commit}]
        //  {0, 1},              {2, 3},            {4},                   {5}, {6}, {7} ==> Offsets
        log.appendAsLeader(record(2, 2), 0); // offset 5
        log.appendAsLeader(record(3, 3), 0); // offset 6
        log.appendAsLeader(commitMarker(1L, producerEpoch), 0, AppendOrigin.COORDINATOR, RequestLocal.noCaching(),
            VerificationGuard.SENTINEL, TransactionVersion.TV_0.featureLevel()); // offset 7
        log.roll();

        // first time through the records are removed
        // Expected State: [{Producer1: EmptyBatch}, {Producer2: EmptyBatch}, {Producer2: Commit}, {2}, {3}, {Producer1: Commit}]
        long dirtyOffset = cleaner.doClean(new LogToClean(log, 0L, log.activeSegment().baseOffset(), false), largeTimestamp).getKey();
        assertEquals(List.of(2L, 3L), LogTestUtils.keysInLog(log));
        assertEquals(List.of(4L, 5L, 6L, 7L), offsetsInLog(log));
        assertEquals(List.of(1L, 3L, 4L, 5L, 6L, 7L), lastOffsetsPerBatchInLog(log));

        // the empty batch remains if cleaned again because it still holds the last sequence
        // Expected State: [{Producer1: EmptyBatch}, {Producer2: EmptyBatch}, {Producer2: Commit}, {2}, {3}, {Producer1: Commit}]
        dirtyOffset = cleaner.doClean(new LogToClean(log, dirtyOffset, log.activeSegment().baseOffset(), false), largeTimestamp).getKey();
        assertEquals(List.of(2L, 3L), LogTestUtils.keysInLog(log));
        assertEquals(List.of(4L, 5L, 6L, 7L), offsetsInLog(log));
        assertEquals(List.of(1L, 3L, 4L, 5L, 6L, 7L), lastOffsetsPerBatchInLog(log));

        // append a new record from the producer to allow cleaning of the empty batch
        // [{Producer1: EmptyBatch}, {Producer2: EmptyBatch}, {Producer2: Commit}, {2}, {3}, {Producer1: Commit}, {Producer2: 1}, {Producer2: Commit}]
        //  {1},                     {3},                     {4},                 {5}, {6}, {7},                 {8},            {9} ==> Offsets
        producer2.append(List.of(1)); // offset 8
        log.appendAsLeader(commitMarker(2L, producerEpoch), 0, AppendOrigin.COORDINATOR, RequestLocal.noCaching(),
            VerificationGuard.SENTINEL, TransactionVersion.TV_0.featureLevel()); // offset 9
        log.roll();

        // Expected State: [{Producer1: EmptyBatch}, {Producer2: Commit}, {2}, {3}, {Producer1: Commit}, {Producer2: 1}, {Producer2: Commit}]
        // The deleteHorizon for {Producer2: Commit} is still not set yet.
        dirtyOffset = cleaner.doClean(new LogToClean(log, dirtyOffset, log.activeSegment().baseOffset(), false), largeTimestamp).getKey();
        assertEquals(List.of(2L, 3L, 1L), LogTestUtils.keysInLog(log));
        assertEquals(List.of(4L, 5L, 6L, 7L, 8L, 9L), offsetsInLog(log));
        assertEquals(List.of(1L, 4L, 5L, 6L, 7L, 8L, 9L), lastOffsetsPerBatchInLog(log));

        // Expected State: [{Producer1: EmptyBatch}, {2}, {3}, {Producer1: Commit}, {Producer2: 1}, {Producer2: Commit}]
        // In the first pass, the deleteHorizon for {Producer2: Commit} is set. In the second pass, it's removed.
        runTwoPassClean(cleaner, new LogToClean(log, dirtyOffset, log.activeSegment().baseOffset(), false), largeTimestamp);
        assertEquals(List.of(2L, 3L, 1L), LogTestUtils.keysInLog(log));
        assertEquals(List.of(5L, 6L, 7L, 8L, 9L), offsetsInLog(log));
        assertEquals(List.of(1L, 5L, 6L, 7L, 8L, 9L), lastOffsetsPerBatchInLog(log));
    }

    @Test
    public void testCleanEmptyControlBatch() throws IOException, DigestException {
        Cleaner cleaner = makeCleaner(Integer.MAX_VALUE);
        Properties logProps = new Properties();
        logProps.put(LogConfig.INTERNAL_SEGMENT_BYTES_CONFIG, 256);
        UnifiedLog log = makeLog(LogConfig.fromProps(logConfig.originals(), logProps));

        short producerEpoch = 0;

        // [{Producer1: Commit}, {2}, {3}]
        log.appendAsLeader(commitMarker(1L, producerEpoch), 0, AppendOrigin.COORDINATOR, RequestLocal.noCaching(),
            VerificationGuard.SENTINEL, TransactionVersion.TV_0.featureLevel()); // offset 1
        log.appendAsLeader(record(2, 2), 0); // offset 2
        log.appendAsLeader(record(3, 3), 0); // offset 3
        log.roll();

        // first time through the control batch is retained as an empty batch
        // Expected State: [{Producer1: EmptyBatch}], [{2}, {3}]
        // In the first pass, the deleteHorizon for the commit marker is set. In the second pass, the commit marker is removed
        // but the empty batch is retained for preserving the producer epoch.
        long dirtyOffset = runTwoPassClean(cleaner, new LogToClean(log, 0L, log.activeSegment().baseOffset(), false), largeTimestamp);
        assertEquals(List.of(2L, 3L), LogTestUtils.keysInLog(log));
        assertEquals(List.of(1L, 2L), offsetsInLog(log));
        assertEquals(List.of(0L, 1L, 2L), lastOffsetsPerBatchInLog(log));

        // the empty control batch does not cause an exception when cleaned
        // Expected State: [{Producer1: EmptyBatch}], [{2}, {3}]
        cleaner.doClean(new LogToClean(log, dirtyOffset, log.activeSegment().baseOffset(), false), Long.MAX_VALUE).getKey();
        assertEquals(List.of(2L, 3L), LogTestUtils.keysInLog(log));
        assertEquals(List.of(1L, 2L), offsetsInLog(log));
        assertEquals(List.of(0L, 1L, 2L), lastOffsetsPerBatchInLog(log));
    }

    @Test
    public void testCommittedTransactionSpanningSegments() throws IOException, DigestException {
        Cleaner cleaner = makeCleaner(Integer.MAX_VALUE);
        Properties logProps = new Properties();
        logProps.put(LogConfig.INTERNAL_SEGMENT_BYTES_CONFIG, 128);
        UnifiedLog log = makeLog(LogConfig.fromProps(logConfig.originals(), logProps));
        short producerEpoch = 0;
        long producerId = 1L;

        var appendTransaction = appendTransactionalAsLeader(log, producerId, producerEpoch);
        appendTransaction.append(List.of(1));
        log.roll();

        log.appendAsLeader(commitMarker(producerId, producerEpoch), 0, AppendOrigin.COORDINATOR, RequestLocal.noCaching(),
            VerificationGuard.SENTINEL, TransactionVersion.TV_0.featureLevel());
        log.roll();

        // Both the record and the marker should remain after cleaning
        runTwoPassClean(cleaner, new LogToClean(log, 0L, log.activeSegment().baseOffset(), false), largeTimestamp);
        assertEquals(List.of(0L, 1L), offsetsInLog(log));
        assertEquals(List.of(0L, 1L), lastOffsetsPerBatchInLog(log));
    }

    @Test
    public void testAbortedTransactionSpanningSegments() throws IOException, DigestException {
        Cleaner cleaner = makeCleaner(Integer.MAX_VALUE);
        Properties logProps = new Properties();
        logProps.put(LogConfig.INTERNAL_SEGMENT_BYTES_CONFIG, 128);
        UnifiedLog log = makeLog(LogConfig.fromProps(logConfig.originals(), logProps));
        short producerEpoch = 0;
        long producerId = 1L;

        var appendTransaction = appendTransactionalAsLeader(log, producerId, producerEpoch);
        appendTransaction.append(List.of(1));
        log.roll();

        log.appendAsLeader(abortMarker(producerId, producerEpoch), 0, AppendOrigin.COORDINATOR, RequestLocal.noCaching(),
            VerificationGuard.SENTINEL, TransactionVersion.TV_0.featureLevel());
        log.roll();

        // Both the batch and the marker should remain after cleaning. The batch is retained
        // because it is the last entry for this producerId. The marker is retained because
        // there are still batches remaining from this transaction.
        cleaner.doClean(new LogToClean(log, 0L, log.activeSegment().baseOffset(), false), largeTimestamp);
        assertEquals(List.of(1L), offsetsInLog(log));
        assertEquals(List.of(0L, 1L), lastOffsetsPerBatchInLog(log));

        // The empty batch and the marker is still retained after a second cleaning.
        cleaner.doClean(new LogToClean(log, 0L, log.activeSegment().baseOffset(), false), Long.MAX_VALUE);
        assertEquals(List.of(1L), offsetsInLog(log));
        assertEquals(List.of(0L, 1L), lastOffsetsPerBatchInLog(log));
    }

    @Test
    public void testAbortMarkerRemoval() throws IOException, DigestException {
        Cleaner cleaner = makeCleaner(Integer.MAX_VALUE);
        Properties logProps = new Properties();
        logProps.put(LogConfig.INTERNAL_SEGMENT_BYTES_CONFIG, 256);
        UnifiedLog log = makeLog(LogConfig.fromProps(logConfig.originals(), logProps));

        short producerEpoch = 0;
        long producerId = 1L;
        var appendProducer = appendTransactionalAsLeader(log, producerId, producerEpoch);

        appendProducer.append(List.of(1));
        appendProducer.append(List.of(2, 3));
        log.appendAsLeader(abortMarker(producerId, producerEpoch), 0, AppendOrigin.COORDINATOR, RequestLocal.noCaching(),
            VerificationGuard.SENTINEL, TransactionVersion.TV_0.featureLevel());
        appendProducer.append(List.of(3));
        log.appendAsLeader(commitMarker(producerId, producerEpoch), 0, AppendOrigin.COORDINATOR, RequestLocal.noCaching(),
            VerificationGuard.SENTINEL, TransactionVersion.TV_0.featureLevel());
        log.roll();

        // Aborted records are removed, but the abort marker is still preserved.
        long dirtyOffset = cleaner.doClean(new LogToClean(log, 0L, log.activeSegment().baseOffset(), false), largeTimestamp).getKey();
        assertEquals(List.of(3L), LogTestUtils.keysInLog(log));
        assertEquals(List.of(3L, 4L, 5L), offsetsInLog(log));

        // In the first pass, the delete horizon for the abort marker is set. In the second pass, the abort marker is removed.
        runTwoPassClean(cleaner, new LogToClean(log, dirtyOffset, log.activeSegment().baseOffset(), false), largeTimestamp);
        assertEquals(List.of(3L), LogTestUtils.keysInLog(log));
        assertEquals(List.of(4L, 5L), offsetsInLog(log));
    }

    @Test
    public void testEmptyBatchRemovalWithSequenceReuse() throws IOException, DigestException {
        // The group coordinator always writes batches beginning with sequence number 0. This test
        // ensures that we still remove old empty batches and transaction markers under this expectation.

        short producerEpoch = 0;
        long producerId = 1L;
        Cleaner cleaner = makeCleaner(Integer.MAX_VALUE);
        Properties logProps = new Properties();
        logProps.put(LogConfig.INTERNAL_SEGMENT_BYTES_CONFIG, 2048);
        UnifiedLog log = makeLog(LogConfig.fromProps(logConfig.originals(), logProps));

        var appendFirstTransaction = appendTransactionalAsLeader(log, producerId, producerEpoch, 0, AppendOrigin.REPLICATION);
        appendFirstTransaction.append(List.of(1));
        log.appendAsLeader(commitMarker(producerId, producerEpoch), 0, AppendOrigin.COORDINATOR, RequestLocal.noCaching(),
            VerificationGuard.SENTINEL, TransactionVersion.TV_0.featureLevel());

        var appendSecondTransaction = appendTransactionalAsLeader(log, producerId, producerEpoch, 0, AppendOrigin.REPLICATION);
        appendSecondTransaction.append(List.of(2));
        log.appendAsLeader(commitMarker(producerId, producerEpoch), 0, AppendOrigin.COORDINATOR, RequestLocal.noCaching(),
            VerificationGuard.SENTINEL, TransactionVersion.TV_0.featureLevel());

        log.appendAsLeader(record(1, 1), 0);
        log.appendAsLeader(record(2, 1), 0);

        // Roll the log to ensure that the data is cleanable.
        log.roll();

        // Both transactional batches will be cleaned. The last one will remain in the log
        // as an empty batch in order to preserve the producer sequence number and epoch
        cleaner.doClean(new LogToClean(log, 0L, log.activeSegment().baseOffset(), false), largeTimestamp);
        assertEquals(List.of(1L, 3L, 4L, 5L), offsetsInLog(log));
        assertEquals(List.of(1L, 2L, 3L, 4L, 5L), lastOffsetsPerBatchInLog(log));

        // In the first pass, the delete horizon for the first marker is set. In the second pass, the first marker is removed.
        runTwoPassClean(cleaner, new LogToClean(log, 0L, log.activeSegment().baseOffset(), false), largeTimestamp);
        assertEquals(List.of(3L, 4L, 5L), offsetsInLog(log));
        assertEquals(List.of(2L, 3L, 4L, 5L), lastOffsetsPerBatchInLog(log));
    }

    @Test
    public void testAbortMarkerRetentionWithEmptyBatch() throws IOException, DigestException {
        Cleaner cleaner = makeCleaner(Integer.MAX_VALUE);
        Properties logProps = new Properties();
        logProps.put(LogConfig.INTERNAL_SEGMENT_BYTES_CONFIG, 256);
        UnifiedLog log = makeLog(LogConfig.fromProps(logConfig.originals(), logProps));

        short producerEpoch = 0;
        long producerId = 1L;
        var appendProducer = appendTransactionalAsLeader(log, producerId, producerEpoch);

        appendProducer.append(List.of(2, 3)); // batch last offset is 1
        log.appendAsLeader(abortMarker(producerId, producerEpoch), 0, AppendOrigin.COORDINATOR, RequestLocal.noCaching(),
            VerificationGuard.SENTINEL, TransactionVersion.TV_0.featureLevel());
        log.roll();

        assertAbortedTransactionIndexed(log, producerId);

        // first time through the records are removed
        long dirtyOffset = cleaner.doClean(new LogToClean(log, 0L, log.activeSegment().baseOffset(), false), largeTimestamp).getKey();
        assertAbortedTransactionIndexed(log, producerId);
        assertEquals(List.of(), LogTestUtils.keysInLog(log));
        assertEquals(List.of(2L), offsetsInLog(log)); // abort marker is retained
        assertEquals(List.of(1L, 2L), lastOffsetsPerBatchInLog(log)); // empty batch is retained

        // the empty batch remains if cleaned again because it still holds the last sequence
        dirtyOffset = runTwoPassClean(cleaner, new LogToClean(log, dirtyOffset, log.activeSegment().baseOffset(), false), largeTimestamp);
        assertAbortedTransactionIndexed(log, producerId);
        assertEquals(List.of(), LogTestUtils.keysInLog(log));
        assertEquals(List.of(2L), offsetsInLog(log)); // abort marker is still retained
        assertEquals(List.of(1L, 2L), lastOffsetsPerBatchInLog(log)); // empty batch is retained

        // now update the last sequence so that the empty batch can be removed
        appendProducer.append(List.of(1));
        log.roll();

        dirtyOffset = cleaner.doClean(new LogToClean(log, dirtyOffset, log.activeSegment().baseOffset(), false), largeTimestamp).getKey();
        assertAbortedTransactionIndexed(log, producerId);
        assertEquals(List.of(1L), LogTestUtils.keysInLog(log));
        assertEquals(List.of(2L, 3L), offsetsInLog(log)); // abort marker is not yet gone because we read the empty batch
        assertEquals(List.of(2L, 3L), lastOffsetsPerBatchInLog(log)); // but we do not preserve the empty batch

        // In the first pass, the delete horizon for the abort marker is set. In the second pass, the abort marker is removed.
        runTwoPassClean(cleaner, new LogToClean(log, dirtyOffset, log.activeSegment().baseOffset(), false), largeTimestamp);
        assertEquals(List.of(1L), LogTestUtils.keysInLog(log));
        assertEquals(List.of(3L), offsetsInLog(log)); // abort marker is gone
        assertEquals(List.of(3L), lastOffsetsPerBatchInLog(log));

        // we do not bother retaining the aborted transaction in the index
        assertEquals(0, log.collectAbortedTransactions(0L, 100L).size());
    }

    private void assertAbortedTransactionIndexed(UnifiedLog log, long producerId) {
        List<AbortedTxn> abortedTxns = log.collectAbortedTransactions(0L, 100L);
        assertEquals(1, abortedTxns.size());
        assertEquals(producerId, abortedTxns.get(0).producerId());
        assertEquals(0, abortedTxns.get(0).firstOffset());
        assertEquals(2, abortedTxns.get(0).lastOffset());
    }

    /**
     * Test log cleaning with logs containing messages larger than default message size
     */
    @Test
    public void testLargeMessage() throws IOException {
        int largeMessageSize = 1024 * 1024;
        // Create cleaner with very small default max message size
        Cleaner cleaner = makeCleaner(Integer.MAX_VALUE, 1024);
        Properties logProps = new Properties();
        logProps.put(LogConfig.INTERNAL_SEGMENT_BYTES_CONFIG, largeMessageSize * 16);
        logProps.put(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, largeMessageSize * 2);

        UnifiedLog log = makeLog(LogConfig.fromProps(logConfig.originals(), logProps));

        while (log.numberOfSegments() < 2) {
            log.appendAsLeader(record((int) log.logEndOffset(), new byte[largeMessageSize]), 0);
        }
        List<Long> keysFound = LogTestUtils.keysInLog(log);
        assertEquals(LongStream.range(0L, log.logEndOffset()).boxed().toList(), keysFound);

        // pretend we have the following keys
        List<Long> keys = List.of(1L, 3L, 5L, 7L, 9L);
        var map = new LogTestUtils.FakeOffsetMap(Integer.MAX_VALUE);
        keys.forEach(k -> map.put(key(k), Long.MAX_VALUE));

        // clean the log
        CleanerStats stats = new CleanerStats(Time.SYSTEM);
        log.updateHighWatermark(log.logSegments().get(0).readNextOffset());
        cleaner.cleanSegments(log, List.of(log.logSegments().get(0)), map, 0L, stats, new CleanedTransactionMetadata(), -1);
        List<Long> shouldRemain = LogTestUtils.keysInLog(log).stream().filter(key -> !keys.contains(key)).toList();
        assertEquals(shouldRemain, LogTestUtils.keysInLog(log));
    }

    /**
     * Test log cleaning with logs containing messages larger than topic's max message size
     */
    @Test
    public void testMessageLargerThanMaxMessageSize() throws IOException {
        LogAndOffsetMap logAndMap = createLogWithMessagesLargerThanMaxSize(1024 * 1024);
        UnifiedLog log = logAndMap.log();
        var offsetMap = logAndMap.offsetMap();

        Cleaner cleaner = makeCleaner(Integer.MAX_VALUE, 1024);
        log.updateHighWatermark(log.logSegments().get(0).readNextOffset());
        cleaner.cleanSegments(log, List.of(log.logSegments().get(0)), offsetMap, 0L,
            new CleanerStats(Time.SYSTEM), new CleanedTransactionMetadata(), -1);
        List<Long> shouldRemain = LogTestUtils.keysInLog(log).stream().filter(key -> !offsetMap.map().containsKey(key)).toList();
        assertEquals(shouldRemain, LogTestUtils.keysInLog(log));
    }

    /**
     * Test log cleaning with logs containing messages larger than topic's max message size
     * where header is corrupt
     */
    @Test
    public void testMessageLargerThanMaxMessageSizeWithCorruptHeader() throws IOException {
        LogAndOffsetMap logAndMap = createLogWithMessagesLargerThanMaxSize(1024 * 1024);
        UnifiedLog log = logAndMap.log();
        var offsetMap = logAndMap.offsetMap();
        try (RandomAccessFile file = new RandomAccessFile(log.logSegments().get(0).log().file(), "rw")) {
            file.seek(Records.MAGIC_OFFSET);
            file.write(0xff);
        }

        Cleaner cleaner = makeCleaner(Integer.MAX_VALUE, 1024);
        log.updateHighWatermark(log.logSegments().get(0).readNextOffset());
        assertThrows(CorruptRecordException.class, () ->
            cleaner.cleanSegments(log, List.of(log.logSegments().get(0)), offsetMap, 0L,
                new CleanerStats(Time.SYSTEM), new CleanedTransactionMetadata(), -1)
        );
    }

    /**
     * Test log cleaning with logs containing messages larger than topic's max message size
     * where message size is corrupt and larger than bytes available in log segment.
     */
    @Test
    public void testCorruptMessageSizeLargerThanBytesAvailable() throws IOException {
        LogAndOffsetMap logAndMap = createLogWithMessagesLargerThanMaxSize(1024 * 1024);
        UnifiedLog log = logAndMap.log();
        var offsetMap = logAndMap.offsetMap();
        try (RandomAccessFile file = new RandomAccessFile(log.logSegments().get(0).log().file(), "rw")) {
            file.setLength(1024);
        }

        Cleaner cleaner = makeCleaner(Integer.MAX_VALUE, 1024);
        assertThrows(CorruptRecordException.class, () ->
            cleaner.cleanSegments(log, List.of(log.logSegments().get(0)), offsetMap, 0L,
                new CleanerStats(Time.SYSTEM), new CleanedTransactionMetadata(), -1)
        );
    }

    private LogAndOffsetMap createLogWithMessagesLargerThanMaxSize(int largeMessageSize) throws IOException {
        Properties logProps = new Properties();
        logProps.put(LogConfig.INTERNAL_SEGMENT_BYTES_CONFIG, largeMessageSize * 16);
        logProps.put(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, largeMessageSize * 2);

        UnifiedLog log = makeLog(LogConfig.fromProps(logConfig.originals(), logProps));

        while (log.numberOfSegments() < 2) {
            log.appendAsLeader(record((int) log.logEndOffset(), new byte[largeMessageSize]), 0);
        }
        List<Long> keysFound = LogTestUtils.keysInLog(log);
        assertEquals(LongStream.range(0L, log.logEndOffset()).boxed().toList(), keysFound);

        // Decrease the log's max message size
        logProps.put(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, largeMessageSize / 2);
        log.updateConfig(LogConfig.fromProps(logConfig.originals(), logProps));

        // pretend we have the following keys
        List<Integer> keys = List.of(1, 3, 5, 7, 9);
        LogTestUtils.FakeOffsetMap map = new LogTestUtils.FakeOffsetMap(Integer.MAX_VALUE);
        keys.forEach(k -> map.put(key(k), Long.MAX_VALUE));

        return new LogAndOffsetMap(log, map);
    }

    @Test
    public void testCleaningWithDeletes() throws IOException, DigestException {
        Cleaner cleaner = makeCleaner(Integer.MAX_VALUE);
        Properties logProps = new Properties();
        logProps.put(LogConfig.INTERNAL_SEGMENT_BYTES_CONFIG, 1024);

        UnifiedLog log = makeLog(LogConfig.fromProps(logConfig.originals(), logProps));

        // append messages with the keys 0 through N
        while (log.numberOfSegments() < 2) {
            log.appendAsLeader(record((int) log.logEndOffset(), (int) log.logEndOffset()), 0);
        }

        // delete all even keys between 0 and N
        long leo = log.logEndOffset();
        for (int k = 0; k < (int) leo; k += 2) {
            log.appendAsLeader(tombstoneRecord(k), 0);
        }

        // append some new unique keys to pad out to a new active segment
        while (log.numberOfSegments() < 4) {
            log.appendAsLeader(record((int) log.logEndOffset(), (int) log.logEndOffset()), 0);
        }

        cleaner.clean(new LogToClean(log, 0, log.activeSegment().baseOffset(), false));
        Set<Long> keys = new HashSet<>(LogTestUtils.keysInLog(log));
        assertTrue(IntStream.iterate(0, k -> k < (int) leo, k -> k + 2)
            .noneMatch(k -> keys.contains((long) k)), "None of the keys we deleted should still exist.");
    }

    @Test
    public void testLogCleanerStats() throws IOException, DigestException {
        // because loadFactor is 0.75, this means we can fit 3 messages in the map
        Cleaner cleaner = makeCleaner(4);
        Properties logProps = new Properties();
        logProps.put(LogConfig.INTERNAL_SEGMENT_BYTES_CONFIG, 1024);

        UnifiedLog log = makeLog(LogConfig.fromProps(logConfig.originals(), logProps));

        log.appendAsLeader(record(0, 0), 0); // offset 0
        log.appendAsLeader(record(1, 1), 0); // offset 1
        log.appendAsLeader(record(0, 0), 0); // offset 2
        log.appendAsLeader(record(1, 1), 0); // offset 3
        log.appendAsLeader(record(0, 0), 0); // offset 4
        // roll the segment, so we can clean the messages already appended
        log.roll();

        long initialLogSize = log.size();

        var endOffsetAndStats = cleaner.clean(new LogToClean(log, 2, log.activeSegment().baseOffset(), false));
        CleanerStats stats = endOffsetAndStats.getValue();
        assertEquals(5, endOffsetAndStats.getKey());
        assertEquals(5, stats.messagesRead());
        assertEquals(initialLogSize, stats.bytesRead());
        assertEquals(2, stats.messagesWritten());
        assertEquals(log.size(), stats.bytesWritten());
        assertEquals(0, stats.invalidMessagesRead());
        assertTrue(stats.endTime() >= stats.startTime());
    }

    @Test
    public void testLogCleanerRetainsProducerLastSequence() throws IOException, DigestException {
        Cleaner cleaner = makeCleaner(10);
        Properties logProps = new Properties();
        logProps.put(LogConfig.INTERNAL_SEGMENT_BYTES_CONFIG, 1024);

        UnifiedLog log = makeLog(LogConfig.fromProps(logConfig.originals(), logProps));
        log.appendAsLeader(record(0, 0), 0); // offset 0
        log.appendAsLeader(record(0, 1, 1L, (short) 0, 0, RecordBatch.NO_PARTITION_LEADER_EPOCH), 0); // offset 1
        log.appendAsLeader(record(0, 2, 2L, (short) 0, 0, RecordBatch.NO_PARTITION_LEADER_EPOCH), 0); // offset 2
        log.appendAsLeader(record(0, 3, 3L, (short) 0, 0, RecordBatch.NO_PARTITION_LEADER_EPOCH), 0); // offset 3
        log.appendAsLeader(record(1, 1, 2L, (short) 0, 1, RecordBatch.NO_PARTITION_LEADER_EPOCH), 0); // offset 4

        // roll the segment, so we can clean the messages already appended
        log.roll();

        cleaner.clean(new LogToClean(log, 0L, log.activeSegment().baseOffset(), false));
        assertEquals(List.of(1L, 3L, 4L), lastOffsetsPerBatchInLog(log));
        assertEquals(Map.of(1L, 0, 2L, 1, 3L, 0), lastSequencesInLog(log));
        assertEquals(List.of(0L, 1L), LogTestUtils.keysInLog(log));
        assertEquals(List.of(3L, 4L), offsetsInLog(log));
    }

    @Test
    public void testLogCleanerRetainsLastSequenceEvenIfTransactionAborted() throws IOException, DigestException {
        Cleaner cleaner = makeCleaner(10);
        Properties logProps = new Properties();
        logProps.put(LogConfig.INTERNAL_SEGMENT_BYTES_CONFIG, 1024);
        UnifiedLog log = makeLog(LogConfig.fromProps(logConfig.originals(), logProps));

        short producerEpoch = 0;
        long producerId = 1L;
        var appendProducer = appendTransactionalAsLeader(log, producerId, producerEpoch);

        appendProducer.append(List.of(1));
        appendProducer.append(List.of(2, 3));
        log.appendAsLeader(abortMarker(producerId, producerEpoch), 0, AppendOrigin.COORDINATOR, RequestLocal.noCaching(),
                VerificationGuard.SENTINEL, TransactionVersion.TV_0.featureLevel());
        log.roll();

        cleaner.clean(new LogToClean(log, 0L, log.activeSegment().baseOffset(), false));
        assertEquals(List.of(2L, 3L), lastOffsetsPerBatchInLog(log));
        assertEquals(Map.of(producerId, 2), lastSequencesInLog(log));
        assertEquals(List.of(), LogTestUtils.keysInLog(log));
        assertEquals(List.of(3L), offsetsInLog(log));

        // Append a new entry from the producer and verify that the empty batch is cleaned up
        appendProducer.append(List.of(1, 5));
        log.roll();
        cleaner.clean(new LogToClean(log, 0L, log.activeSegment().baseOffset(), false));

        assertEquals(List.of(3L, 5L), lastOffsetsPerBatchInLog(log));
        assertEquals(Map.of(producerId, 4), lastSequencesInLog(log));
        assertEquals(List.of(1L, 5L), LogTestUtils.keysInLog(log));
        assertEquals(List.of(3L, 4L, 5L), offsetsInLog(log));
    }

    @Test
    public void testCleaningWithKeysConflictingWithTxnMarkerKeys() throws IOException, DigestException {
        Cleaner cleaner = makeCleaner(10);
        Properties logProps = new Properties();
        logProps.put(LogConfig.INTERNAL_SEGMENT_BYTES_CONFIG, 1024);
        UnifiedLog log = makeLog(LogConfig.fromProps(logConfig.originals(), logProps));
        int leaderEpoch = 5;
        short producerEpoch = 0;

        // First we append one committed transaction
        long producerId1 = 1L;
        var appendProducer = appendTransactionalAsLeader(log, producerId1, producerEpoch, leaderEpoch, AppendOrigin.CLIENT);
        appendProducer.append(List.of(1));
        log.appendAsLeader(commitMarker(producerId1, producerEpoch), leaderEpoch, AppendOrigin.COORDINATOR, RequestLocal.noCaching(),
                VerificationGuard.SENTINEL, TransactionVersion.TV_0.featureLevel());

        // Now we append one transaction with a key which conflicts with the COMMIT marker appended above
        ByteBuffer commitRecordKey = ControlRecordType.COMMIT.recordKey();

        long producerId2 = 2L;
        MemoryRecords records = MemoryRecords.withTransactionalRecords(
            Compression.NONE,
            producerId2,
            producerEpoch,
            0,
            new SimpleRecord(time.milliseconds(), commitRecordKey, ByteBuffer.wrap("foo".getBytes()))
        );
        log.appendAsLeader(records, leaderEpoch, AppendOrigin.CLIENT);
        log.appendAsLeader(commitMarker(producerId2, producerEpoch), leaderEpoch, AppendOrigin.COORDINATOR,
                RequestLocal.noCaching(), VerificationGuard.SENTINEL, TransactionVersion.TV_0.featureLevel());
        log.roll();
        assertEquals(List.of(0L, 1L, 2L, 3L), offsetsInLog(log));

        // After cleaning, the marker should not be removed
        cleaner.clean(new LogToClean(log, 0L, log.activeSegment().baseOffset(), false));
        assertEquals(List.of(0L, 1L, 2L, 3L), lastOffsetsPerBatchInLog(log));
        assertEquals(List.of(0L, 1L, 2L, 3L), offsetsInLog(log));
    }

    @Test
    public void testPartialSegmentClean() throws IOException, DigestException {
        // because loadFactor is 0.75, this means we can fit 1 message in the map
        Cleaner cleaner = makeCleaner(2);
        Properties logProps = new Properties();
        logProps.put(LogConfig.INTERNAL_SEGMENT_BYTES_CONFIG, 1024);

        UnifiedLog log = makeLog(LogConfig.fromProps(logConfig.originals(), logProps));

        log.appendAsLeader(record(0, 0), 0); // offset 0
        log.appendAsLeader(record(1, 1), 0); // offset 1
        log.appendAsLeader(record(0, 0), 0); // offset 2
        log.appendAsLeader(record(1, 1), 0); // offset 3
        log.appendAsLeader(record(0, 0), 0); // offset 4
        // roll the segment, so we can clean the messages already appended
        log.roll();

        // clean the log with only one message removed
        cleaner.clean(new LogToClean(log, 2, log.activeSegment().baseOffset(), false));
        assertEquals(List.of(1L, 0L, 1L, 0L), LogTestUtils.keysInLog(log));
        assertEquals(List.of(1L, 2L, 3L, 4L), offsetsInLog(log));

        // continue to make progress, even though we can only clean one message at a time
        cleaner.clean(new LogToClean(log, 3, log.activeSegment().baseOffset(), false));
        assertEquals(List.of(0L, 1L, 0L), LogTestUtils.keysInLog(log));
        assertEquals(List.of(2L, 3L, 4L), offsetsInLog(log));

        cleaner.clean(new LogToClean(log, 4, log.activeSegment().baseOffset(), false));
        assertEquals(List.of(1L, 0L), LogTestUtils.keysInLog(log));
        assertEquals(List.of(3L, 4L), offsetsInLog(log));
    }

    @Test
    public void testCleaningWithUncleanableSection() throws IOException, DigestException {
        Cleaner cleaner = makeCleaner(Integer.MAX_VALUE);
        Properties logProps = new Properties();
        logProps.put(LogConfig.INTERNAL_SEGMENT_BYTES_CONFIG, 1024);

        UnifiedLog log = makeLog(LogConfig.fromProps(logConfig.originals(), logProps));

        // Number of distinct keys. For an effective test this should be small enough such that each log segment contains some duplicates.
        int n = 10;
        int numCleanableSegments = 2;
        int numTotalSegments = 7;

        // append messages with the keys 0 through N-1, values equal offset
        while (log.numberOfSegments() <= numCleanableSegments) {
            log.appendAsLeader(record((int) log.logEndOffset() % n, (int) log.logEndOffset()), 0);
        }

        // at this point one message past the cleanable segments has been added
        // the entire segment containing the first uncleanable offset should not be cleaned.
        long firstUncleanableOffset = log.logEndOffset() + 1;  // +1  so it is past the baseOffset

        while (log.numberOfSegments() < numTotalSegments - 1) {
            log.appendAsLeader(record((int) log.logEndOffset() % n, (int) log.logEndOffset()), 0);
        }

        // the last (active) segment has just one message

        List<Integer> distinctValuesBySegmentBeforeClean = distinctValuesBySegment(log);
        for (int i = 0; i < distinctValuesBySegmentBeforeClean.size() - 1; i++) {
            assertTrue(distinctValuesBySegmentBeforeClean.get(i) > n,
                "Test is not effective unless each segment contains duplicates. Increase segment size or decrease number of keys.");
        }

        cleaner.clean(new LogToClean(log, 0, firstUncleanableOffset, false));

        List<Integer> distinctValuesBySegmentAfterClean = distinctValuesBySegment(log);

        for (int i = 0; i < numCleanableSegments; i++) {
            assertTrue(distinctValuesBySegmentAfterClean.get(i) < distinctValuesBySegmentBeforeClean.get(i),
                "The cleanable segments should have fewer number of values after cleaning");
        }
        for (int i = numCleanableSegments; i < Math.min(distinctValuesBySegmentAfterClean.size(), numTotalSegments); i++) {
            assertEquals(distinctValuesBySegmentBeforeClean.get(i), distinctValuesBySegmentAfterClean.get(i),
                "The uncleanable segments should have the same number of values after cleaning");
        }
    }

    private List<Integer> distinctValuesBySegment(UnifiedLog log) {
        List<Integer> result = new ArrayList<>();
        for (LogSegment s : log.logSegments()) {
            Set<String> values = new HashSet<>();
            for (Record record : s.log().records()) {
                values.add(Utils.utf8(record.value()));
            }
            result.add(values.size());
        }
        return result;
    }

    @Test
    public void testLogToClean() throws IOException {
        // create a log with small segment size
        Properties logProps = new Properties();
        logProps.put(LogConfig.INTERNAL_SEGMENT_BYTES_CONFIG, 100);
        UnifiedLog log = makeLog(LogConfig.fromProps(logConfig.originals(), logProps));

        // create 6 segments with only one message in each segment
        for (int i = 0; i < 6; i++) {
            log.appendAsLeader(LogTestUtils.singletonRecords(new byte[25], "1".getBytes()), 0);
        }

        LogToClean logToClean = new LogToClean(log, log.activeSegment().baseOffset(), log.activeSegment().baseOffset(), false);

        assertEquals(logToClean.totalBytes(), log.size() - log.activeSegment().size(),
            "Total bytes of LogToClean should equal size of all segments excluding the active segment");
    }

    @Test
    public void testLogToCleanWithUncleanableSection() throws IOException {
        // create a log with small segment size
        Properties logProps = new Properties();
        logProps.put(LogConfig.INTERNAL_SEGMENT_BYTES_CONFIG, 100);
        UnifiedLog log = makeLog(LogConfig.fromProps(logConfig.originals(), logProps));

        // create 6 segments with only one message in each segment
        for (int i = 0; i < 6; i++) {
            log.appendAsLeader(LogTestUtils.singletonRecords(new byte[25], "1".getBytes()), 0);
        }

        // segments [0,1] are clean; segments [2, 3] are cleanable; segments [4,5] are uncleanable
        List<LogSegment> segs = log.logSegments();
        LogToClean logToClean = new LogToClean(log, segs.get(2).baseOffset(), segs.get(4).baseOffset(), false);

        long expectedCleanSize = segs.get(0).size() + segs.get(1).size();
        long expectedCleanableSize = segs.get(2).size() + segs.get(3).size();

        assertEquals(logToClean.cleanBytes(), expectedCleanSize,
            "Uncleanable bytes of LogToClean should equal size of all segments prior the one containing first dirty");
        assertEquals(logToClean.cleanableBytes(), expectedCleanableSize,
            "Cleanable bytes of LogToClean should equal size of all segments from the one containing first dirty offset"
                + " to the segment prior to the one with the first uncleanable offset");
        assertEquals(logToClean.totalBytes(), expectedCleanSize + expectedCleanableSize,
            "Total bytes should be the sum of the clean and cleanable segments");
        assertEquals(logToClean.cleanableRatio(),
            expectedCleanableSize / (double) (expectedCleanSize + expectedCleanableSize), 1.0e-6d,
            "Total cleanable ratio should be the ratio of cleanable size to clean plus cleanable");
    }

    @Test
    public void testCleaningWithUnkeyedMessages() throws IOException, DigestException {
        Cleaner cleaner = makeCleaner(Integer.MAX_VALUE);

        // create a log with compaction turned off so we can append unkeyed messages
        Properties logProps = new Properties();
        logProps.put(LogConfig.INTERNAL_SEGMENT_BYTES_CONFIG, 1024);
        logProps.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE);

        UnifiedLog log = makeLog(LogConfig.fromProps(logConfig.originals(), logProps));

        // append unkeyed messages
        while (log.numberOfSegments() < 2) {
            log.appendAsLeader(unkeyedRecord((int) log.logEndOffset()), 0);
        }
        int numInvalidMessages = unkeyedMessageCountInLog(log);

        long sizeWithUnkeyedMessages = log.size();

        // append keyed messages
        while (log.numberOfSegments() < 3) {
            log.appendAsLeader(record((int) log.logEndOffset(), (int) log.logEndOffset()), 0);
        }

        long expectedSizeAfterCleaning = log.size() - sizeWithUnkeyedMessages;
        CleanerStats stats = cleaner.clean(new LogToClean(log, 0, log.activeSegment().baseOffset(), false)).getValue();

        assertEquals(0, unkeyedMessageCountInLog(log), "Log should only contain keyed messages after cleaning.");
        assertEquals(expectedSizeAfterCleaning, log.size(), "Log should only contain keyed messages after cleaning.");
        assertEquals(numInvalidMessages, stats.invalidMessagesRead(), "Cleaner should have seen %d invalid messages.");
    }

    private List<Long> batchBaseOffsetsInLog(UnifiedLog log) {
        List<Long> result = new ArrayList<>();
        for (LogSegment seg : log.logSegments()) {
            for (RecordBatch batch : seg.log().batches()) {
                result.add(batch.baseOffset());
            }
        }
        return result;
    }

    private List<Long> lastOffsetsPerBatchInLog(UnifiedLog log) {
        List<Long> result = new ArrayList<>();
        for (LogSegment seg : log.logSegments()) {
            for (RecordBatch batch : seg.log().batches()) {
                result.add(batch.lastOffset());
            }
        }
        return result;
    }

    private Map<Long, Integer> lastSequencesInLog(UnifiedLog log) {
        Map<Long, Integer> result = new HashMap<>();
        for (LogSegment seg : log.logSegments()) {
            for (RecordBatch batch : seg.log().batches()) {
                if (batch.hasProducerId() && !batch.isControlBatch()) {
                    result.put(batch.producerId(), batch.lastSequence());
                }
            }
        }
        return result;
    }

    private List<Long> offsetsInLog(UnifiedLog log) {
        List<Long> result = new ArrayList<>();
        for (LogSegment seg : log.logSegments()) {
            for (Record r : seg.log().records()) {
                if (r.hasValue() && r.hasKey()) {
                    result.add(r.offset());
                }
            }
        }
        return result;
    }

    private int unkeyedMessageCountInLog(UnifiedLog log) {
        int count = 0;
        for (LogSegment seg : log.logSegments()) {
            for (Record r : seg.log().records()) {
                if (r.hasValue() && !r.hasKey()) count++;
            }
        }
        return count;
    }

    private void abortCheckDone(TopicPartition topicPartition) {
        throw new LogCleaningAbortedException();
    }

    @Test
    public void testCleanSegmentsWithAbort() throws IOException {
        Cleaner cleaner = makeCleaner(Integer.MAX_VALUE, this::abortCheckDone, 64 * 1024, Integer.MAX_VALUE, Integer.MAX_VALUE);
        Properties logProps = new Properties();
        logProps.put(LogConfig.INTERNAL_SEGMENT_BYTES_CONFIG, 1024);

        UnifiedLog log = makeLog(LogConfig.fromProps(logConfig.originals(), logProps));

        // append messages to the log until we have four segments
        while (log.numberOfSegments() < 4) {
            log.appendAsLeader(record((int) log.logEndOffset(), (int) log.logEndOffset()), 0);
        }

        List<Long> keys = LogTestUtils.keysInLog(log);
        LogTestUtils.FakeOffsetMap map = new LogTestUtils.FakeOffsetMap(Integer.MAX_VALUE);
        keys.forEach(k -> map.put(key(k), Long.MAX_VALUE));
        List<LogSegment> segments = log.logSegments().subList(0, 3);
        log.updateHighWatermark(segments.get(segments.size() - 1).readNextOffset());
        assertThrows(LogCleaningAbortedException.class, () -> cleaner.cleanSegments(log, segments, map, 0L,
            new CleanerStats(Time.SYSTEM), new CleanedTransactionMetadata(), -1)
        );
    }

    /**
     * Test that if a cleaned batch's next offset equals the high watermark, the batch is retained
     * even if it is empty (i.e. all its records have been cleaned away). This ensures that the last
     * offset information before the high watermark is not lost after log cleaning.
     */
    @Test
    public void testCleanSegmentsRetainingLastEmptyBatch() throws IOException {
        Cleaner cleaner = makeCleaner(Integer.MAX_VALUE);
        Properties logProps = new Properties();
        logProps.put(LogConfig.INTERNAL_SEGMENT_BYTES_CONFIG, 1024);

        UnifiedLog log = makeLog(LogConfig.fromProps(logConfig.originals(), logProps));

        // append messages to the log until we have four segments
        while (log.numberOfSegments() < 4) {
            log.appendAsLeader(record((int) log.logEndOffset(), (int) log.logEndOffset()), 0);
        }
        List<Long> keysFound = LogTestUtils.keysInLog(log);
        assertEquals(LongStream.range(0L, log.logEndOffset()).boxed().toList(), keysFound);

        // pretend all keys are deleted
        LogTestUtils.FakeOffsetMap map = new LogTestUtils.FakeOffsetMap(Integer.MAX_VALUE);
        keysFound.forEach(k -> map.put(key(k), Long.MAX_VALUE));

        // clean the log
        List<LogSegment> segments = log.logSegments().subList(0, 3);
        CleanerStats stats = new CleanerStats(Time.SYSTEM);
        log.updateHighWatermark(segments.get(segments.size() - 1).readNextOffset());
        cleaner.cleanSegments(log, segments, map, 0L, stats, new CleanedTransactionMetadata(), -1);
        assertEquals(2, log.logSegments().size());
        LogSegment firstSegAfter = log.logSegments().get(0);
        List<? extends RecordBatch> batchList = StreamSupport.stream(firstSegAfter.log().batches().spliterator(), false).toList();
        assertEquals(1, batchList.size(), "one batch should be retained in the cleaned segment");
        RecordBatch retainedBatch = batchList.get(0);
        assertEquals(log.logSegments().get(log.logSegments().size() - 1).baseOffset() - 1, retainedBatch.lastOffset(),
            "the retained batch should be the last batch");
        assertFalse(retainedBatch.iterator().hasNext(), "the retained batch should be an empty batch");
    }

    /**
     * Test that if the high watermark is beyond the cleaned segments (i.e. no batch's next offset
     * equals the high watermark), empty batches produced by cleaning are NOT retained and can be
     * safely deleted. This is the counterpart to [[testCleanSegmentsRetainingLastEmptyBatch]].
     */
    @Test
    public void testCleanSegmentsNotRetainingLastEmptyBatch() throws IOException {
        Cleaner cleaner = makeCleaner(Integer.MAX_VALUE);
        Properties logProps = new Properties();
        logProps.put(LogConfig.INTERNAL_SEGMENT_BYTES_CONFIG, 1024);

        UnifiedLog log = makeLog(LogConfig.fromProps(logConfig.originals(), logProps));

        // append messages to the log until we have four segments
        while (log.numberOfSegments() < 4) {
            log.appendAsLeader(record((int) log.logEndOffset(), (int) log.logEndOffset()), 0);
        }
        List<Long> keysFound = LogTestUtils.keysInLog(log);
        assertEquals(LongStream.range(0L, log.logEndOffset()).boxed().toList(), keysFound);

        // pretend all keys are deleted
        LogTestUtils.FakeOffsetMap map = new LogTestUtils.FakeOffsetMap(Integer.MAX_VALUE);
        keysFound.forEach(k -> map.put(key(k), Long.MAX_VALUE));

        // clean the log
        List<LogSegment> segments = log.logSegments().subList(0, 3);
        CleanerStats stats = new CleanerStats(Time.SYSTEM);
        log.updateHighWatermark(log.logSegments().get(log.logSegments().size() - 1).readNextOffset());
        cleaner.cleanSegments(log, segments, map, 0L, stats, new CleanedTransactionMetadata(), -1);
        assertEquals(2, log.logSegments().size());
        LogSegment firstSegAfter = log.logSegments().get(0);
        List<? extends RecordBatch> batchList = StreamSupport.stream(firstSegAfter.log().batches().spliterator(), false).toList();
        assertEquals(0, batchList.size(), "no batches should remain in the cleaned segment");
    }

    /**
     * Validate the logic for grouping log segments together for cleaning
     */
    @Test
    public void testSegmentGrouping() throws IOException {
        Cleaner cleaner = makeCleaner(Integer.MAX_VALUE);
        Properties logProps = new Properties();
        logProps.put(LogConfig.INTERNAL_SEGMENT_BYTES_CONFIG, 300);
        logProps.put(TopicConfig.INDEX_INTERVAL_BYTES_CONFIG, 1);

        UnifiedLog log = makeLog(LogConfig.fromProps(logConfig.originals(), logProps));

        // append some messages to the log
        while (log.numberOfSegments() < 10) {
            log.appendAsLeader(LogTestUtils.singletonRecords("hello".getBytes(), "hello".getBytes()), 0);
        }

        // grouping by very large values should result in a single group with all the segments in it
        List<List<LogSegment>> groups = cleaner.groupSegmentsBySize(log.logSegments(), Integer.MAX_VALUE, Integer.MAX_VALUE, log.logEndOffset());
        assertEquals(1, groups.size());
        assertEquals(log.numberOfSegments(), groups.get(0).size());
        checkSegmentOrder(groups);

        // grouping by very small values should result in all groups having one entry
        groups = cleaner.groupSegmentsBySize(log.logSegments(), 1, Integer.MAX_VALUE, log.logEndOffset());
        assertEquals(log.numberOfSegments(), groups.size());
        assertTrue(groups.stream().allMatch(g -> g.size() == 1), "All groups should be singletons.");
        checkSegmentOrder(groups);
        groups = cleaner.groupSegmentsBySize(log.logSegments(), Integer.MAX_VALUE, 1, log.logEndOffset());
        assertEquals(log.numberOfSegments(), groups.size());
        assertTrue(groups.stream().allMatch(g -> g.size() == 1), "All groups should be singletons.");
        checkSegmentOrder(groups);

        int groupSize = 3;

        // check grouping by log size
        int logSize = log.logSegments().subList(0, groupSize).stream().mapToInt(LogSegment::size).sum() + 1;
        groups = cleaner.groupSegmentsBySize(log.logSegments(), logSize, Integer.MAX_VALUE, log.logEndOffset());
        checkSegmentOrder(groups);
        assertTrue(groups.subList(0, groups.size() - 1).stream().allMatch(g -> g.size() == groupSize),
            "All but the last group should be the target size.");

        // check grouping by index size
        int indexSize = 1;
        for (LogSegment s : log.logSegments().subList(0, groupSize)) {
            indexSize += s.offsetIndex().sizeInBytes();
        }
        groups = cleaner.groupSegmentsBySize(log.logSegments(), Integer.MAX_VALUE, indexSize, log.logEndOffset());
        checkSegmentOrder(groups);
        assertTrue(groups.subList(0, groups.size() - 1).stream().allMatch(g -> g.size() == groupSize),
            "All but the last group should be the target size.");
    }

    @Test
    public void testSegmentGroupingWithSparseOffsetsAndEmptySegments() throws IOException, DigestException {
        Cleaner cleaner = makeCleaner(Integer.MAX_VALUE);
        Properties logProps = new Properties();
        UnifiedLog log = makeLog(LogConfig.fromProps(logConfig.originals(), logProps));

        byte[] k = "key".getBytes();
        byte[] v = "val".getBytes();

        // create 3 segments
        for (int i = 0; i < 3; i++) {
            log.appendAsLeader(LogTestUtils.singletonRecords(v, k), 0);
            // 0 to Int.MaxValue is Int.MaxValue+1 message, -1 will be the last message of i-th segment
            MemoryRecords records = messageWithOffset(k, v, (i + 1L) * ((long) Integer.MAX_VALUE + 1L) - 1);
            log.appendAsFollower(records, Integer.MAX_VALUE);
            assertEquals(i + 1, log.numberOfSegments());
        }

        // 4th active segment, not clean
        log.appendAsLeader(LogTestUtils.singletonRecords(v, k), 0);

        int totalSegments = 4;
        // last segment not cleanable
        long firstUncleanableOffset = log.logEndOffset() - 1;
        int notCleanableSegments = 1;

        assertEquals(totalSegments, log.numberOfSegments());
        List<List<LogSegment>> groups = cleaner.groupSegmentsBySize(log.logSegments(), Integer.MAX_VALUE, Integer.MAX_VALUE, firstUncleanableOffset);
        // because index file uses 4 byte relative index offset and current segments all none empty,
        // segments will not group even their size is very small.
        assertEquals(totalSegments - notCleanableSegments, groups.size());
        // do clean to clean first 2 segments to empty
        cleaner.clean(new LogToClean(log, 0, firstUncleanableOffset, false));
        assertEquals(totalSegments, log.numberOfSegments());
        assertEquals(0, log.logSegments().get(0).size());

        // after clean we got 2 empty segment, they will group together this time
        groups = cleaner.groupSegmentsBySize(log.logSegments(), Integer.MAX_VALUE, Integer.MAX_VALUE, firstUncleanableOffset);
        int noneEmptySegment = 1;
        assertEquals(noneEmptySegment + 1, groups.size());

        // trigger a clean and 2 empty segments should cleaned to 1
        cleaner.clean(new LogToClean(log, 0, firstUncleanableOffset, false));
        assertEquals(totalSegments - 1, log.numberOfSegments());
    }

    /**
     * Validate the logic for grouping log segments together for cleaning when only a small number of
     * messages are retained, but the range of offsets is greater than Int.MaxValue. A group should not
     * contain a range of offsets greater than Int.MaxValue to ensure that relative offsets can be
     * stored in 4 bytes.
     */
    @Test
    public void testSegmentGroupingWithSparseOffsets() throws IOException {
        Cleaner cleaner = makeCleaner(Integer.MAX_VALUE);

        Properties logProps = new Properties();
        logProps.put(LogConfig.INTERNAL_SEGMENT_BYTES_CONFIG, 400);
        logProps.put(TopicConfig.INDEX_INTERVAL_BYTES_CONFIG, 1);

        UnifiedLog log = makeLog(LogConfig.fromProps(logConfig.originals(), logProps));

        // fill up first segment
        while (log.numberOfSegments() == 1) {
            log.appendAsLeader(LogTestUtils.singletonRecords("hello".getBytes(), "hello".getBytes()), 0);
        }

        // forward offset and append message to next segment at offset Int.MaxValue
        MemoryRecords records = messageWithOffset("hello".getBytes(), "hello".getBytes(), Integer.MAX_VALUE - 1);
        log.appendAsFollower(records, Integer.MAX_VALUE);
        log.appendAsLeader(LogTestUtils.singletonRecords("hello".getBytes(), "hello".getBytes()), 0);
        assertEquals(Integer.MAX_VALUE, log.activeSegment().offsetIndex().lastOffset());

        // grouping should result in a single group with maximum relative offset of Int.MaxValue
        List<List<LogSegment>> groups = cleaner.groupSegmentsBySize(log.logSegments(), Integer.MAX_VALUE, Integer.MAX_VALUE, log.logEndOffset());
        assertEquals(1, groups.size());

        // append another message, making last offset of second segment > Int.MaxValue
        log.appendAsLeader(LogTestUtils.singletonRecords("hello".getBytes(), "hello".getBytes()), 0);

        // grouping should not group the two segments to ensure that maximum relative offset in each group <= Int.MaxValue
        groups = cleaner.groupSegmentsBySize(log.logSegments(), Integer.MAX_VALUE, Integer.MAX_VALUE, log.logEndOffset());
        assertEquals(2, groups.size());
        checkSegmentOrder(groups);

        // append more messages, creating new segments, further grouping should still occur
        while (log.numberOfSegments() < 4) {
            log.appendAsLeader(LogTestUtils.singletonRecords("hello".getBytes(), "hello".getBytes()), 0);
        }

        groups = cleaner.groupSegmentsBySize(log.logSegments(), Integer.MAX_VALUE, Integer.MAX_VALUE, log.logEndOffset());
        assertEquals(log.numberOfSegments() - 1, groups.size());
        for (List<LogSegment> group : groups) {
            assertTrue(group.get(group.size() - 1).offsetIndex().lastOffset() - group.get(0).offsetIndex().baseOffset() <= Integer.MAX_VALUE,
                "Relative offset greater than Int.MaxValue");
        }
        checkSegmentOrder(groups);
    }

    /**
     * Following the loading of a log segment where the index file is zero sized,
     * the index returned would be the base offset.  Sometimes the log file would
     * contain data with offsets in excess of the baseOffset which would cause
     * the log cleaner to group together segments with a range of > Int.MaxValue
     * this test replicates that scenario to ensure that the segments are grouped
     * correctly.
     */
    @Test
    public void testSegmentGroupingFollowingLoadOfZeroIndex() throws IOException {
        Cleaner cleaner = makeCleaner(Integer.MAX_VALUE);

        Properties logProps = new Properties();
        logProps.put(LogConfig.INTERNAL_SEGMENT_BYTES_CONFIG, 400);

        // mimic the effect of loading an empty index file
        logProps.put(TopicConfig.INDEX_INTERVAL_BYTES_CONFIG, 400);

        UnifiedLog log = makeLog(LogConfig.fromProps(logConfig.originals(), logProps));

        MemoryRecords record1 = messageWithOffset("hello".getBytes(), "hello".getBytes(), 0);
        log.appendAsFollower(record1, Integer.MAX_VALUE);
        MemoryRecords record2 = messageWithOffset("hello".getBytes(), "hello".getBytes(), 1);
        log.appendAsFollower(record2, Integer.MAX_VALUE);
        log.roll(Optional.of((long) (Integer.MAX_VALUE / 2))); // starting a new log segment at offset Int.MaxValue/2
        MemoryRecords record3 = messageWithOffset("hello".getBytes(), "hello".getBytes(), Integer.MAX_VALUE / 2);
        log.appendAsFollower(record3, Integer.MAX_VALUE);
        MemoryRecords record4 = messageWithOffset("hello".getBytes(), "hello".getBytes(), (long) Integer.MAX_VALUE + 1);
        log.appendAsFollower(record4, Integer.MAX_VALUE);

        assertTrue(log.logEndOffset() - 1 - log.logStartOffset() > Integer.MAX_VALUE, "Actual offset range should be > Int.MaxValue");
        assertTrue(log.logSegments().get(log.logSegments().size() - 1).offsetIndex().lastOffset() - log.logStartOffset() <= Integer.MAX_VALUE,
            "index.lastOffset is reporting the wrong last offset");

        // grouping should result in two groups because the second segment takes the offset range > MaxInt
        List<List<LogSegment>> groups = cleaner.groupSegmentsBySize(log.logSegments(), Integer.MAX_VALUE, Integer.MAX_VALUE, log.logEndOffset());
        assertEquals(2, groups.size());

        for (List<LogSegment> group : groups) {
            assertTrue(group.get(group.size() - 1).readNextOffset() - 1 - group.get(0).baseOffset() <= Integer.MAX_VALUE,
                "Relative offset greater than Int.MaxValue");
        }
        checkSegmentOrder(groups);
    }

    private void checkSegmentOrder(List<List<LogSegment>> groups) {
        List<Long> offsets = groups.stream()
            .flatMap(Collection::stream)
            .map(LogSegment::baseOffset)
            .toList();
        List<Long> sorted = offsets.stream().sorted().toList();
        assertEquals(sorted, offsets, "Offsets should be in increasing order.");
    }

    /**
     * Test building an offset map off the log
     */
    @Test
    public void testBuildOffsetMap() throws IOException, DigestException {
        LogTestUtils.FakeOffsetMap map = new LogTestUtils.FakeOffsetMap(1000);
        UnifiedLog log = makeLog();
        Cleaner cleaner = makeCleaner(Integer.MAX_VALUE);
        int start = 0;
        int end = 500;
        List<Map.Entry<Integer, Integer>> seq = IntStream.range(start, end)
            .mapToObj(i -> Map.entry(i, i))
            .toList();
        writeToLog(log, seq);

        List<LogSegment> segments = log.logSegments();
        checkRangeForBuildOffsetMap(cleaner, log, map, 0, (int) segments.get(1).baseOffset());
        checkRangeForBuildOffsetMap(cleaner, log, map, (int) segments.get(1).baseOffset(), (int) segments.get(3).baseOffset());
        checkRangeForBuildOffsetMap(cleaner, log, map, (int) segments.get(3).baseOffset(), (int) log.logEndOffset());
    }

    private void checkRangeForBuildOffsetMap(
        Cleaner cleaner,
        UnifiedLog log,
        LogTestUtils.FakeOffsetMap map,
        int start, int end
    ) throws IOException, DigestException {
        CleanerStats stats = new CleanerStats(Time.SYSTEM);
        cleaner.buildOffsetMap(log, start, end, map, stats);
        long endOffset = map.latestOffset() + 1;
        assertEquals(end, endOffset, "Last offset should be the end offset.");
        assertEquals(end - start, map.size(), "Should have the expected number of messages in the map.");
        for (int i = start; i < end; i++) {
            assertEquals(i, map.get(key(i)), "Should find all the keys");
        }
        assertEquals(-1L, map.get(key(start - 1)), "Should not find a value too small");
        assertEquals(-1L, map.get(key(end)), "Should not find a value too large");
        assertEquals(end - start, stats.mapMessagesRead());
    }

    @Test
    public void testCleanedSegmentSizeOverflow() throws IOException {
        // Put one record per source segment so each filterTo() call reads exactly one batch.
        // After cleaning source segment 0 into currentCleaned, cleaning source segment 1 would push
        // currentCleaned over maxCleanedSegmentSize, triggering overflow and
        // rolling to a second cleaned segment.
        Properties logProps = new Properties();
        logProps.put(LogConfig.INTERNAL_SEGMENT_BYTES_CONFIG, 4096);
        UnifiedLog log = makeLog(LogConfig.fromProps(logConfig.originals(), logProps));

        log.appendAsLeader(record(0, 0), 0);
        log.roll();
        log.appendAsLeader(record(1, 1), 0);
        log.roll();

        List<LogSegment> sourceSegments = log.logSegments().subList(0, 2);
        int singleBatchSize = StreamSupport.stream(sourceSegments.get(0).log().batches().spliterator(), false)
            .mapToInt(RecordBatch::sizeInBytes)
            .max().orElse(0);
        // maxCleanedSize allows exactly 1 batch; adding a 2nd batch overflows.
        long maxCleanedSize = (long) singleBatchSize + 1L;

        // No deletions; both records are retained.
        Cleaner cleaner = makeCleaner(Integer.MAX_VALUE, tp -> { }, 64 * 1024, maxCleanedSize, Integer.MAX_VALUE);
        LogTestUtils.FakeOffsetMap offsetMap = new LogTestUtils.FakeOffsetMap(Integer.MAX_VALUE);

        // Before: sourceSegment0, sourceSegment1, activeSegment = 3 total
        int segmentCountBefore = log.logSegments().size();

        log.updateHighWatermark(sourceSegments.get(sourceSegments.size() - 1).readNextOffset());
        cleaner.cleanSegments(log, sourceSegments, offsetMap, 0L,
            new CleanerStats(Time.SYSTEM), new CleanedTransactionMetadata(), -1);

        // With overflow, 2 source segments → 2 cleaned segments; net segment count unchanged.
        // Without overflow, 2 source → 1 cleaned; net count would be segmentCountBefore - 1.
        assertEquals(segmentCountBefore, log.logSegments().size());
        assertEquals(List.of(0L, 1L), LogTestUtils.keysInLog(log));
        log.close();
    }

    @Test
    public void testCleanedSegmentOffsetOverflow() throws IOException {
        // Put one record per source segment so each filterTo() call reads exactly one batch.
        // After cleaning source segment 0 into currentCleaned (offset 0), cleaning source segment 1
        // would push the offset range of currentCleaned over maxCleanedOffsetRange, triggering
        // overflow and rolling to a second cleaned segment.
        Properties logProps = new Properties();
        logProps.put(LogConfig.INTERNAL_SEGMENT_BYTES_CONFIG, 4096);
        UnifiedLog log = makeLog(LogConfig.fromProps(logConfig.originals(), logProps));

        log.appendAsLeader(record(0, 0), 0);
        log.roll();
        log.appendAsLeader(record(1, 1), 0);
        log.roll();

        List<LogSegment> sourceSegments = log.logSegments().subList(0, 2);
        // Allow an offset range of 0: after offset 0 is written, any record at offset > 0 overflows.
        Cleaner cleaner = makeCleaner(Integer.MAX_VALUE, tp -> { }, 64 * 1024, Integer.MAX_VALUE, 0L);
        LogTestUtils.FakeOffsetMap offsetMap = new LogTestUtils.FakeOffsetMap(Integer.MAX_VALUE);

        int segmentCountBefore = log.logSegments().size();

        log.updateHighWatermark(sourceSegments.get(sourceSegments.size() - 1).readNextOffset());
        cleaner.cleanSegments(log, sourceSegments, offsetMap, 0L,
            new CleanerStats(Time.SYSTEM), new CleanedTransactionMetadata(), -1);

        assertEquals(segmentCountBefore, log.logSegments().size(),
            "offset overflow should produce 2 cleaned segments, keeping total segment count the same");
        assertEquals(List.of(0L, 1L), LogTestUtils.keysInLog(log));
        log.close();
    }

    /**
     * Tests recovery if broker crashes at the following stages during the cleaning sequence
     * <ol>
     *   <li> Cleaner has created .cleaned log containing multiple segments, swap sequence not yet started
     *   <li> .cleaned log renamed to .swap, old segment files not yet renamed to .deleted
     *   <li> .cleaned log renamed to .swap, old segment files renamed to .deleted, but not yet deleted
     *   <li> .swap suffix removed, completing the swap, but async delete of .deleted files not yet complete
     * </ol>
     */
    @Test
    public void testRecoveryAfterCrash() throws IOException {
        Cleaner cleaner = makeCleaner(Integer.MAX_VALUE);
        Properties logProps = new Properties();
        logProps.put(LogConfig.INTERNAL_SEGMENT_BYTES_CONFIG, 300);
        logProps.put(TopicConfig.INDEX_INTERVAL_BYTES_CONFIG, 1);
        logProps.put(TopicConfig.FILE_DELETE_DELAY_MS_CONFIG, 10);

        LogConfig config = LogConfig.fromProps(logConfig.originals(), logProps);

        // create a log and append some messages
        UnifiedLog log = makeLog(config);
        int messageCount = 0;
        while (log.numberOfSegments() < 10) {
            log.appendAsLeader(record((int) log.logEndOffset(), (int) log.logEndOffset()), 0);
            messageCount += 1;
        }
        List<Long> allKeys = LogTestUtils.keysInLog(log);

        // pretend we have odd-numbered keys
        LogTestUtils.FakeOffsetMap offsetMap = new LogTestUtils.FakeOffsetMap(Integer.MAX_VALUE);
        for (int k = 1; k < messageCount; k += 2) {
            offsetMap.put(key(k), Long.MAX_VALUE);
        }

        log.updateHighWatermark(log.activeSegment().baseOffset());
        // clean the log
        cleaner.cleanSegments(log, log.logSegments().subList(0, 9), offsetMap, 0L,
            new CleanerStats(Time.SYSTEM), new CleanedTransactionMetadata(), -1);
        // clear scheduler so that async deletes don't run
        time.scheduler.clear();
        log.close();

        // 1) Simulate recovery just after .cleaned file is created, before rename to .swap
        //    On recovery, clean operation is aborted. All messages should be present in the log
        log.logSegments().get(0).changeFileSuffixes("", UnifiedLog.CLEANED_FILE_SUFFIX);
        for (File file : Objects.requireNonNull(dir.listFiles())) {
            if (file.getName().endsWith(LogFileUtils.DELETED_FILE_SUFFIX)) {
                Utils.atomicMoveWithFallback(file.toPath(), Paths.get(
                    Utils.replaceSuffix(file.getPath(), LogFileUtils.DELETED_FILE_SUFFIX, "")), false);
            }
        }
        log = LogTestUtils.recoverAndCheck(dir, config, allKeys, new BrokerTopicStats(), time, time.scheduler);

        // clean again
        cleaner.cleanSegments(log, log.logSegments().subList(0, 9), offsetMap, 0L,
            new CleanerStats(Time.SYSTEM), new CleanedTransactionMetadata(), -1);
        // clear scheduler so that async deletes don't run
        time.scheduler.clear();
        log.close();

        // 2) Simulate recovery just after .cleaned file is created, and a subset of them are renamed to .swap
        //    On recovery, clean operation is aborted. All messages should be present in the log
        log.logSegments().get(0).changeFileSuffixes("", UnifiedLog.CLEANED_FILE_SUFFIX);
        log.logSegments().get(0).log().renameTo(new File(Utils.replaceSuffix(log.logSegments().get(0).log().file().getPath(),
            UnifiedLog.CLEANED_FILE_SUFFIX, UnifiedLog.SWAP_FILE_SUFFIX)));
        for (File file : Objects.requireNonNull(dir.listFiles())) {
            if (file.getName().endsWith(LogFileUtils.DELETED_FILE_SUFFIX)) {
                Utils.atomicMoveWithFallback(file.toPath(), Paths.get(
                    Utils.replaceSuffix(file.getPath(), LogFileUtils.DELETED_FILE_SUFFIX, "")), false);
            }
        }
        log = LogTestUtils.recoverAndCheck(dir, config, allKeys, new BrokerTopicStats(), time, time.scheduler);

        // clean again
        cleaner.cleanSegments(log, log.logSegments().subList(0, 9), offsetMap, 0L,
            new CleanerStats(Time.SYSTEM), new CleanedTransactionMetadata(), -1);
        // clear scheduler so that async deletes don't run
        time.scheduler.clear();
        List<Long> cleanedKeys = LogTestUtils.keysInLog(log);
        log.close();

        // 3) Simulate recovery just after swap file is created, before old segment files are
        //    renamed to .deleted. Clean operation is resumed during recovery.
        log.logSegments().get(0).changeFileSuffixes("", UnifiedLog.SWAP_FILE_SUFFIX);
        for (File file : Objects.requireNonNull(dir.listFiles())) {
            if (file.getName().endsWith(LogFileUtils.DELETED_FILE_SUFFIX)) {
                Utils.atomicMoveWithFallback(file.toPath(), Paths.get(
                    Utils.replaceSuffix(file.getPath(), LogFileUtils.DELETED_FILE_SUFFIX, "")), false);
            }
        }
        log = LogTestUtils.recoverAndCheck(dir, config, cleanedKeys, new BrokerTopicStats(), time, time.scheduler);

        // add some more messages and clean the log again
        while (log.numberOfSegments() < 10) {
            log.appendAsLeader(record((int) log.logEndOffset(), (int) log.logEndOffset()), 0);
            messageCount += 1;
        }
        for (int k = 1; k < messageCount; k += 2) {
            offsetMap.put(key(k), Long.MAX_VALUE);
        }
        cleaner.cleanSegments(log, log.logSegments().subList(0, 9), offsetMap, 0L,
            new CleanerStats(Time.SYSTEM), new CleanedTransactionMetadata(), -1);
        // clear scheduler so that async deletes don't run
        time.scheduler.clear();
        cleanedKeys = LogTestUtils.keysInLog(log);

        // 4) Simulate recovery after swap file is created and old segments files are renamed
        //    to .deleted. Clean operation is resumed during recovery.
        log.logSegments().get(0).changeFileSuffixes("", UnifiedLog.SWAP_FILE_SUFFIX);
        log = LogTestUtils.recoverAndCheck(dir, config, cleanedKeys, new BrokerTopicStats(), time, time.scheduler);

        // add some more messages and clean the log again
        while (log.numberOfSegments() < 10) {
            log.appendAsLeader(record((int) log.logEndOffset(), (int) log.logEndOffset()), 0);
            messageCount += 1;
        }
        for (int k = 1; k < messageCount; k += 2) {
            offsetMap.put(key(k), Long.MAX_VALUE);
        }
        cleaner.cleanSegments(log, log.logSegments().subList(0, 9), offsetMap, 0L,
            new CleanerStats(Time.SYSTEM), new CleanedTransactionMetadata(), -1);
        // clear scheduler so that async deletes don't run
        time.scheduler.clear();
        cleanedKeys = LogTestUtils.keysInLog(log);

        // 5) Simulate recovery after a subset of swap files are renamed to regular files and old segments files are renamed
        //    to .deleted. Clean operation is resumed during recovery.
        File timeIndexFile = log.logSegments().get(0).timeIndex().file();
        timeIndexFile.renameTo(new File(Utils.replaceSuffix(timeIndexFile.getPath(), "", UnifiedLog.SWAP_FILE_SUFFIX)));
        log = LogTestUtils.recoverAndCheck(dir, config, cleanedKeys, new BrokerTopicStats(), time, time.scheduler);

        // add some more messages and clean the log again
        while (log.numberOfSegments() < 10) {
            log.appendAsLeader(record((int) log.logEndOffset(), (int) log.logEndOffset()), 0);
            messageCount += 1;
        }
        for (int k = 1; k < messageCount; k += 2) {
            offsetMap.put(key(k), Long.MAX_VALUE);
        }
        cleaner.cleanSegments(log, log.logSegments().subList(0, 9), offsetMap, 0L,
            new CleanerStats(Time.SYSTEM), new CleanedTransactionMetadata(), -1);
        // clear scheduler so that async deletes don't run
        time.scheduler.clear();
        cleanedKeys = LogTestUtils.keysInLog(log);
        log.close();

        // 6) Simulate recovery after swap is complete, but async deletion
        //    is not yet complete. Clean operation is resumed during recovery.
        log = LogTestUtils.recoverAndCheck(dir, config, cleanedKeys, new BrokerTopicStats(), time, time.scheduler);
        log.close();
    }

    @Test
    public void testBuildOffsetMapFakeLarge() throws IOException, DigestException {
        LogTestUtils.FakeOffsetMap map = new LogTestUtils.FakeOffsetMap(1000);
        Properties logProps = new Properties();
        logProps.put(LogConfig.INTERNAL_SEGMENT_BYTES_CONFIG, 120);
        logProps.put(TopicConfig.SEGMENT_INDEX_BYTES_CONFIG, 120);
        logProps.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
        LogConfig logConfig = new LogConfig(logProps);
        UnifiedLog log = makeLog(logConfig);
        Cleaner cleaner = makeCleaner(Integer.MAX_VALUE);
        int keyStart = 0;
        int keyEnd = 2;
        long offsetStart = 0L;
        long offsetEnd = 7206178L;
        List<Long> offsetSeq = List.of(offsetStart, offsetEnd);
        List<Map.Entry<Integer, Integer>> seq = IntStream.range(keyStart, keyEnd)
            .mapToObj(i -> Map.entry(i, i))
            .toList();
        writeToLog(log, seq, offsetSeq);
        cleaner.buildOffsetMap(log, keyStart, offsetEnd + 1L, map, new CleanerStats(Time.SYSTEM));
        assertEquals(offsetEnd, map.latestOffset(), "Last offset should be the end offset.");
        assertEquals(keyEnd - keyStart, map.size(), "Should have the expected number of messages in the map.");
        assertEquals(0L, map.get(key(0)), "Map should contain first value");
        assertEquals(offsetEnd, map.get(key(1)), "Map should contain second value");
    }

    /**
     * Test building a partial offset map of part of a log segment
     */
    @Test
    public void testBuildPartialOffsetMap() throws IOException, DigestException {
        // because loadFactor is 0.75, this means we can fit 2 messages in the map
        UnifiedLog log = makeLog();
        Cleaner cleaner = makeCleaner(3);
        OffsetMap map = cleaner.offsetMap();

        log.appendAsLeader(record(0, 0), 0);
        log.appendAsLeader(record(1, 1), 0);
        log.appendAsLeader(record(2, 2), 0);
        log.appendAsLeader(record(3, 3), 0);
        log.appendAsLeader(record(4, 4), 0);
        log.roll();

        CleanerStats stats = new CleanerStats(Time.SYSTEM);
        cleaner.buildOffsetMap(log, 2, Integer.MAX_VALUE, map, stats);
        assertEquals(2, map.size());
        assertEquals(-1, map.get(key(0)));
        assertEquals(2, map.get(key(2)));
        assertEquals(3, map.get(key(3)));
        assertEquals(-1, map.get(key(4)));
        assertEquals(4, stats.mapMessagesRead());
    }

    /**
     * This test verifies that messages corrupted by KAFKA-4298 are fixed by the cleaner
     */
    @Test
    public void testCleanCorruptMessageSet() throws Exception {
        CompressionType codec = CompressionType.GZIP;

        Properties logProps = new Properties();
        logProps.put(TopicConfig.COMPRESSION_TYPE_CONFIG, codec.name);
        LogConfig logConfig = new LogConfig(logProps);

        UnifiedLog log = makeLog(logConfig);
        Cleaner cleaner = makeCleaner(10);

        // messages are constructed so that the payload matches the expecting offset to
        // make offset validation easier after cleaning

        // one compressed log entry with duplicates
        int dupSetOffset = 25;
        List<Integer> dupSetKeys = List.of(0, 1, 0, 1);
        List<Map.Entry<Integer, Integer>> dupSet = IntStream.range(0, dupSetKeys.size())
            .mapToObj(i -> Map.entry(dupSetKeys.get(i), dupSetOffset + i))
            .toList();

        // and one without (should still be fixed by the cleaner)
        int noDupSetOffset = 50;
        List<Integer> noDupSetKeys = List.of(3, 4);
        List<Map.Entry<Integer, Integer>> noDupSet = IntStream.range(0, noDupSetKeys.size())
            .mapToObj(i -> Map.entry(noDupSetKeys.get(i), noDupSetOffset + i))
            .toList();

        log.appendAsFollower(invalidCleanedMessage(dupSetOffset, dupSet, codec), Integer.MAX_VALUE);
        log.appendAsFollower(invalidCleanedMessage(noDupSetOffset, noDupSet, codec), Integer.MAX_VALUE);

        log.roll();

        cleaner.clean(new LogToClean(log, 0, log.activeSegment().baseOffset(), false));

        for (LogSegment segment : log.logSegments()) {
            for (RecordBatch batch : segment.log().batches()) {
                for (Record record : batch) {
                    assertTrue(record.hasMagic(batch.magic()));
                    long value = Long.parseLong(Utils.utf8(record.value()));
                    assertEquals(record.offset(), value);
                }
            }
        }
    }

    /**
     * Verify that the client can handle corrupted messages. Located here for now since the client
     * does not support writing messages with the old magic.
     */
    @Test
    public void testClientHandlingOfCorruptMessageSet() {
        int offset = 50;
        List<Map.Entry<Integer, Integer>> set = IntStream.range(1, 10).mapToObj(k -> Map.entry(k, offset + (k - 1))).toList();

        MemoryRecords corruptedMessage = invalidCleanedMessage(offset, set);
        MemoryRecords records = MemoryRecords.readableRecords(corruptedMessage.buffer());

        for (Record logEntry : records.records()) {
            long entryOffset = logEntry.offset();
            long value = Long.parseLong(Utils.utf8(logEntry.value()));
            assertEquals(entryOffset, value);
        }
    }

    @Test
    public void testCleanTombstone() throws Exception {
        Properties properties = new Properties();
        // This test uses future timestamps beyond the default of 1 hour.
        properties.put(TopicConfig.MESSAGE_TIMESTAMP_AFTER_MAX_MS_CONFIG, Long.toString(Long.MAX_VALUE));
        LogConfig logConfig = new LogConfig(properties);

        UnifiedLog log = makeLog(logConfig);
        Cleaner cleaner = makeCleaner(10);

        // Append a message with a large timestamp.
        log.appendAsLeader(LogTestUtils.singletonRecords("0".getBytes(), Compression.NONE, "0".getBytes(),
            time.milliseconds() + logConfig.deleteRetentionMs + 10000), 0);
        log.roll();
        cleaner.clean(new LogToClean(log, 0, log.activeSegment().baseOffset(), false));
        // Append a tombstone with a small timestamp and roll out a new log segment.
        log.appendAsLeader(LogTestUtils.singletonRecords(null, Compression.NONE, "0".getBytes(),
            time.milliseconds() - logConfig.deleteRetentionMs - 10000), 0);
        log.roll();

        cleaner.clean(new LogToClean(log, 1, log.activeSegment().baseOffset(), false));
        assertEquals(1, log.logSegments().get(0).log().batches().iterator().next().lastOffset(),
            "The tombstone should be retained.");
        // Append a message and roll out another log segment.
        log.appendAsLeader(LogTestUtils.singletonRecords("1".getBytes(), Compression.NONE, "1".getBytes(),
            time.milliseconds()), 0);
        log.roll();
        cleaner.clean(new LogToClean(log, 2, log.activeSegment().baseOffset(), false));
        assertEquals(1, log.logSegments().get(0).log().batches().iterator().next().lastOffset(),
                "The tombstone should be retained.");
    }

    /**
     * Verify that the clean is able to move beyond missing offsets records in dirty log in a single segment.
     */
    @Test
    public void testCleaningBeyondMissingOffsetsSingleSegment() throws Exception {
        Properties logProps = new Properties();
        logProps.put(LogConfig.INTERNAL_SEGMENT_BYTES_CONFIG, 1024 * 1024);
        logProps.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
        LogConfig logConfig = new LogConfig(logProps);
        Cleaner cleaner = makeCleaner(Integer.MAX_VALUE);

        UnifiedLog log = makeLog(TestUtils.randomPartitionLogDir(tmpdir), logConfig, 0L);
        List<Map.Entry<Integer, Integer>> seq = IntStream.rangeClosed(0, 9)
            .mapToObj(i -> Map.entry(i, i))
            .toList();
        List<Long> offsetSeq = LongStream.rangeClosed(0, 9).boxed().toList();
        writeToLog(log, seq, offsetSeq);
        // roll new segment with baseOffset 11, leaving previous with holes in offset range [9,10]
        log.roll(Optional.of(11L));

        // active segment record
        log.appendAsFollower(messageWithOffset(1015, 1015, 11L), Integer.MAX_VALUE);

        long nextDirtyOffset = cleaner.clean(new LogToClean(log, 0L, log.activeSegment().baseOffset(), true)).getKey();
        assertEquals(log.activeSegment().baseOffset(), nextDirtyOffset, "Cleaning point should pass offset gap");
    }

    /**
     * Verify that the clean is able to move beyond missing offsets records in dirty log across multiple segments.
     */
    @Test
    public void testCleaningBeyondMissingOffsetsMultipleSegments() throws Exception {
        Properties logProps = new Properties();
        logProps.put(LogConfig.INTERNAL_SEGMENT_BYTES_CONFIG, 1024 * 1024);
        logProps.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
        LogConfig logConfig = new LogConfig(logProps);
        Cleaner cleaner = makeCleaner(Integer.MAX_VALUE);

        UnifiedLog log = makeLog(TestUtils.randomPartitionLogDir(tmpdir), logConfig, 0L);
        List<Map.Entry<Integer, Integer>> seq = IntStream.rangeClosed(0, 9)
            .mapToObj(i -> Map.entry(i, i))
            .toList();
        List<Long> offsetSeq = LongStream.rangeClosed(0, 9).boxed().toList();
        writeToLog(log, seq, offsetSeq);
        // roll new segment with baseOffset 15, leaving previous with holes in offset range [10, 14]
        log.roll(Optional.of(15L));

        List<Map.Entry<Integer, Integer>> seq2 = IntStream.rangeClosed(15, 24)
            .mapToObj(i -> Map.entry(i, i))
            .toList();
        List<Long> offsetSeq2 = LongStream.rangeClosed(15, 24).boxed().toList();
        writeToLog(log, seq2, offsetSeq2);
        // roll new segment with baseOffset 30, leaving previous with holes in offset range [25, 29]
        log.roll(Optional.of(30L));

        // active segment record
        log.appendAsFollower(messageWithOffset(1015, 1015, 30L), Integer.MAX_VALUE);

        long nextDirtyOffset = cleaner.clean(new LogToClean(log, 0L, log.activeSegment().baseOffset(), true)).getKey();
        assertEquals(log.activeSegment().baseOffset(), nextDirtyOffset, "Cleaning point should pass offset gap in multiple segments");
    }

    @Test
    public void testMaxCleanTimeSecs() {
        LogCleaner logCleaner = new LogCleaner(new CleanerConfig(true),
            List.of(TestUtils.tempDirectory()),
            new ConcurrentHashMap<>(),
            new LogDirFailureChannel(1),
            time);

        try {
            checkGauge(logCleaner, "max-buffer-utilization-percent");
            checkGauge(logCleaner, "max-clean-time-secs");
            checkGauge(logCleaner, "max-compaction-delay-secs");
        } finally {
            logCleaner.shutdown();
        }
    }

    private void checkGauge(LogCleaner logCleaner, String name) {
        var gauge = logCleaner.metricsGroup().newGauge(name, () -> 999);
        // if there is no cleaners, 0 is default value
        assertEquals(0, gauge.value());
    }

    @Test
    public void testReconfigureLogCleanerIoMaxBytesPerSecond() {
        var oldConfig = makeReconfigureConfig(Map.of(CleanerConfig.LOG_CLEANER_IO_MAX_BYTES_PER_SECOND_PROP, 10000000D));

        LogCleaner logCleaner = new LogCleaner(oldConfig,
                List.of(TestUtils.tempDirectory()),
                new ConcurrentHashMap<>(),
                new LogDirFailureChannel(1),
                time) {
            // shutdown() and startup() are called in LogCleaner.reconfigure().
            // Empty startup() and shutdown() to ensure that no unnecessary log cleaner threads remain after this test.
            @Override
            public void startup() { }

            @Override
            public void shutdown() { }
        };

        try {
            assertEquals(10000000, logCleaner.throttler().desiredRatePerSec(),
                "Throttler.desiredRatePerSec should be initialized from initial `" +
                    CleanerConfig.LOG_CLEANER_IO_MAX_BYTES_PER_SECOND_PROP + "` config.");

            var newConfig = makeReconfigureConfig(Map.of(CleanerConfig.LOG_CLEANER_IO_MAX_BYTES_PER_SECOND_PROP, 20000000D));

            logCleaner.reconfigure(oldConfig, newConfig);

            assertEquals(20000000, logCleaner.throttler().desiredRatePerSec(),
                "Throttler.desiredRatePerSec should be updated with new `" +
                    CleanerConfig.LOG_CLEANER_IO_MAX_BYTES_PER_SECOND_PROP + "` config.");
        } finally {
            logCleaner.shutdown();
        }
    }

    @Test
    public void testMaxBufferUtilizationPercentMetric() throws Exception {
        LogCleaner logCleaner = new LogCleaner(
            new CleanerConfig(true),
            List.of(TestUtils.tempDirectory(), TestUtils.tempDirectory()),
            new ConcurrentHashMap<>(),
            new LogDirFailureChannel(1),
            time);

        try {
            // No CleanerThreads
            assertMaxBufferUtilizationPercent(logCleaner, 0);

            var cleaners = logCleaner.cleaners();

            var cleaner1 = logCleaner.new CleanerThread(1);
            cleaner1.setLastStats(new CleanerStats(time));
            cleaner1.lastStats().setBufferUtilization(0.75);
            cleaners.add(cleaner1);

            var cleaner2 = logCleaner.new CleanerThread(2);
            cleaner2.setLastStats(new CleanerStats(time));
            cleaner2.lastStats().setBufferUtilization(0.85);
            cleaners.add(cleaner2);

            var cleaner3 = logCleaner.new CleanerThread(3);
            cleaner3.setLastStats(new CleanerStats(time));
            cleaner3.lastStats().setBufferUtilization(0.65);
            cleaners.add(cleaner3);

            // expect the gauge value to reflect the maximum bufferUtilization
            assertMaxBufferUtilizationPercent(logCleaner, 85);

            // Update bufferUtilization and verify the gauge value updates
            cleaner1.lastStats().setBufferUtilization(0.9);
            assertMaxBufferUtilizationPercent(logCleaner, 90);

            // All CleanerThreads have the same bufferUtilization
            cleaners.forEach(c -> c.lastStats().setBufferUtilization(0.5));
            assertMaxBufferUtilizationPercent(logCleaner, 50);
        } finally {
            logCleaner.shutdown();
        }
    }

    private void assertMaxBufferUtilizationPercent(LogCleaner logCleaner, int expected) {
        var gauge = logCleaner.metricsGroup().newGauge(LogCleaner.MAX_BUFFER_UTILIZATION_PERCENT_METRIC_NAME,
            () -> (int) (logCleaner.maxOverCleanerThreads(t -> t.lastStats().bufferUtilization()) * 100));
        assertEquals(expected, gauge.value());
    }

    @Test
    public void testMaxCleanTimeMetric() throws Exception {
        LogCleaner logCleaner = new LogCleaner(
            new CleanerConfig(true),
            List.of(TestUtils.tempDirectory(), TestUtils.tempDirectory()),
            new ConcurrentHashMap<>(),
            new LogDirFailureChannel(1),
            time);

        try {
            // No CleanerThreads
            assertMaxCleanTime(logCleaner, 0);

            var cleaners = logCleaner.cleaners();

            var cleaner1 = logCleaner.new CleanerThread(1);
            cleaner1.setLastStats(new CleanerStats(time));
            cleaner1.lastStats().setEndTime(cleaner1.lastStats().startTime() + 1000L);
            cleaners.add(cleaner1);

            var cleaner2 = logCleaner.new CleanerThread(2);
            cleaner2.setLastStats(new CleanerStats(time));
            cleaner2.lastStats().setEndTime(cleaner2.lastStats().startTime() + 2000L);
            cleaners.add(cleaner2);

            var cleaner3 = logCleaner.new CleanerThread(3);
            cleaner3.setLastStats(new CleanerStats(time));
            cleaner3.lastStats().setEndTime(cleaner3.lastStats().startTime() + 3000L);
            cleaners.add(cleaner3);

            // expect the gauge value to reflect the maximum cleanTime
            assertMaxCleanTime(logCleaner, 3);

            // Update cleanTime and verify the gauge value updates
            cleaner1.lastStats().setEndTime(cleaner1.lastStats().startTime() + 4000L);
            assertMaxCleanTime(logCleaner, 4);

            // All CleanerThreads have the same cleanTime
            cleaners.forEach(cleaner -> cleaner.lastStats().setEndTime(cleaner.lastStats().startTime() + 1500L));
            assertMaxCleanTime(logCleaner, 1);
        } finally {
            logCleaner.shutdown();
        }
    }

    private void assertMaxCleanTime(LogCleaner logCleaner, int expected) {
        var gauge = logCleaner.metricsGroup().newGauge(LogCleaner.MAX_CLEAN_TIME_METRIC_NAME,
            () -> (int) logCleaner.maxOverCleanerThreads(t -> t.lastStats().elapsedSecs()));
        assertEquals(expected, gauge.value());
    }

    @Test
    public void testMaxCompactionDelayMetrics() throws Exception {
        LogCleaner logCleaner = new LogCleaner(
            new CleanerConfig(true),
            List.of(TestUtils.tempDirectory(), TestUtils.tempDirectory()),
            new ConcurrentHashMap<>(),
            new LogDirFailureChannel(1),
            time);

        try {
            // No CleanerThreads
            assertMaxCompactionDelay(logCleaner, 0);

            var cleaners = logCleaner.cleaners();

            var cleaner1 = logCleaner.new CleanerThread(1);
            cleaner1.setLastStats(new CleanerStats(time));
            cleaner1.lastPreCleanStats().maxCompactionDelayMs(1000L);
            cleaners.add(cleaner1);

            var cleaner2 = logCleaner.new CleanerThread(2);
            cleaner2.setLastStats(new CleanerStats(time));
            cleaner2.lastPreCleanStats().maxCompactionDelayMs(2000L);
            cleaners.add(cleaner2);

            var cleaner3 = logCleaner.new CleanerThread(3);
            cleaner3.setLastStats(new CleanerStats(time));
            cleaner3.lastPreCleanStats().maxCompactionDelayMs(3000L);
            cleaners.add(cleaner3);

            // expect the gauge value to reflect the maximum CompactionDelay
            assertMaxCompactionDelay(logCleaner, 3);

            // Update CompactionDelay and verify the gauge value updates
            cleaner1.lastPreCleanStats().maxCompactionDelayMs(4000L);
            assertMaxCompactionDelay(logCleaner, 4);

            // All CleanerThreads have the same CompactionDelay
            cleaners.forEach(c -> c.lastPreCleanStats().maxCompactionDelayMs(1500L));
            assertMaxCompactionDelay(logCleaner, 1);
        } finally {
            logCleaner.shutdown();
        }
    }

    private void assertMaxCompactionDelay(LogCleaner logCleaner, int expected) {
        var gauge = logCleaner.metricsGroup().newGauge(LogCleaner.MAX_COMPACTION_DELAY_METRICS_NAME,
            () -> (int) (logCleaner.maxOverCleanerThreads(t -> (double) t.lastPreCleanStats().maxCompactionDelayMs()) / 1000));
        assertEquals(expected, gauge.value());
    }

    private List<Long> writeToLog(UnifiedLog log, List<Map.Entry<Integer, Integer>> keysAndValues, List<Long> offsetSeq) {
        List<Long> result = new ArrayList<>();
        for (int i = 0; i < keysAndValues.size(); i++) {
            Map.Entry<Integer, Integer> kv = keysAndValues.get(i);
            long offset = offsetSeq.get(i);
            result.add(log.appendAsFollower(messageWithOffset(kv.getKey(), kv.getValue(), offset), Integer.MAX_VALUE).lastOffset());
        }
        return result;
    }

    private List<Long> writeToLog(UnifiedLog log, List<Map.Entry<Integer, Integer>> seq) throws IOException {
        List<Long> result = new ArrayList<>();
        for (Map.Entry<Integer, Integer> kv : seq) {
            result.add(log.appendAsLeader(record(kv.getKey(), kv.getValue()), 0).firstOffset());
        }
        return result;
    }

    private MemoryRecords invalidCleanedMessage(long initialOffset, List<Map.Entry<Integer, Integer>> keysAndValues) {
        return invalidCleanedMessage(initialOffset, keysAndValues, CompressionType.GZIP);
    }

    private MemoryRecords invalidCleanedMessage(
        long initialOffset,
        List<Map.Entry<Integer, Integer>> keysAndValues,
        CompressionType compressionType
    ) {
        // this function replicates the old versions of the cleaner which under some circumstances
        // would write invalid compressed message sets with the outer magic set to 1 and the inner
        // magic set to 0
        List<LegacyRecord> records = keysAndValues.stream().map(kv ->
            LegacyRecord.create(
                RecordBatch.MAGIC_VALUE_V0,
                RecordBatch.NO_TIMESTAMP,
                kv.getKey().toString().getBytes(),
                kv.getValue().toString().getBytes()))
            .toList();

        int totalSize = records.stream().mapToInt(LegacyRecord::sizeInBytes).sum();
        ByteBuffer buffer = ByteBuffer.allocate(Math.min(Math.max(totalSize / 2, 1024), 1 << 16));
        Compression codec = Compression.of(compressionType).build();
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, RecordBatch.MAGIC_VALUE_V1, codec,
            TimestampType.CREATE_TIME, initialOffset);

        long offset = initialOffset;
        for (LegacyRecord record : records) {
            builder.appendUncheckedWithOffset(offset, record);
            offset += 1;
        }

        return builder.build();
    }

    private MemoryRecords messageWithOffset(byte[] key, byte[] value, long offset) {
        return MemoryRecords.withRecords(offset, Compression.NONE, 0, new SimpleRecord(key, value));
    }

    private MemoryRecords messageWithOffset(int key, int value, long offset) {
        return messageWithOffset(Integer.toString(key).getBytes(), Integer.toString(value).getBytes(), offset);
    }

    private UnifiedLog makeLog() throws IOException {
        return makeLog(dir, logConfig, 0L);
    }

    private UnifiedLog makeLog(LogConfig config) throws IOException {
        return makeLog(dir, config, 0L);
    }

    private UnifiedLog makeLog(File dir, LogConfig config, long recoveryPoint) throws IOException {
        return UnifiedLog.create(
            dir,
            config,
            0L,
            recoveryPoint,
            time.scheduler,
            new BrokerTopicStats(),
            time,
            5 * 60 * 1000,
            producerStateManagerConfig,
            TransactionLogConfig.PRODUCER_ID_EXPIRATION_CHECK_INTERVAL_MS_DEFAULT,
            new LogDirFailureChannel(10),
            true,
            Optional.empty()
        );
    }

    private Cleaner makeCleaner(int capacity) {
        return makeCleaner(capacity, tp -> { }, 64 * 1024, Integer.MAX_VALUE, Integer.MAX_VALUE);
    }

    private Cleaner makeCleaner(int capacity, int maxMessageSize) {
        return makeCleaner(capacity, tp -> { }, maxMessageSize, Integer.MAX_VALUE, Integer.MAX_VALUE);
    }

    private Cleaner makeCleaner(int capacity, Consumer<TopicPartition> checkDone, int maxMessageSize,
                                long maxCleanedSegmentSize, long maxCleanedOffsetRange) {
        return new Cleaner(0,
            new LogTestUtils.FakeOffsetMap(capacity),
            maxMessageSize,
            maxMessageSize,
            0.75,
            throttler,
            time,
            checkDone,
            maxCleanedSegmentSize,
            maxCleanedOffsetRange);
    }

    private ByteBuffer key(long id) {
        return ByteBuffer.wrap(Long.toString(id).getBytes());
    }

    private MemoryRecords record(int key, int value) {
        return record(key, value, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH,
            RecordBatch.NO_SEQUENCE, RecordBatch.NO_PARTITION_LEADER_EPOCH);
    }

    private MemoryRecords record(
        int key,
        int value,
        long producerId,
        short producerEpoch,
        int sequence,
        int partitionLeaderEpoch
    ) {
        return MemoryRecords.withIdempotentRecords(RecordBatch.CURRENT_MAGIC_VALUE, 0L, Compression.NONE,
            producerId, producerEpoch, sequence, partitionLeaderEpoch,
            new SimpleRecord(Integer.toString(key).getBytes(), Integer.toString(value).getBytes()));
    }

    private MemoryRecords record(int key, byte[] value) {
        return LogTestUtils.singletonRecords(value, Integer.toString(key).getBytes());
    }

    private MemoryRecords unkeyedRecord(int value) {
        return LogTestUtils.singletonRecords(Integer.toString(value).getBytes(), null);
    }

    private MemoryRecords tombstoneRecord(int key) {
        return record(key, null);
    }

    @FunctionalInterface
    private interface ProducerAppender {
        LogAppendInfo append(List<Integer> keys) throws IOException;
    }

    private record LogAndOffsetMap(UnifiedLog log, LogTestUtils.FakeOffsetMap offsetMap) { }

    private LogAppendInfo appendIdempotentAsLeader(UnifiedLog log, long producerId, short producerEpoch,
                                                   List<Integer> keys) throws IOException {
        return newProducerAppender(log, producerId, producerEpoch, false, 0, AppendOrigin.CLIENT).append(keys);
    }

    private ProducerAppender appendTransactionalAsLeader(UnifiedLog log, long producerId, short producerEpoch) {
        return newProducerAppender(log, producerId, producerEpoch, true, 0, AppendOrigin.CLIENT);
    }

    private ProducerAppender appendTransactionalAsLeader(UnifiedLog log, long producerId, short producerEpoch,
                                                    int leaderEpoch, AppendOrigin origin) {
        return newProducerAppender(log, producerId, producerEpoch, true, leaderEpoch, origin);
    }

    private ProducerAppender newProducerAppender(UnifiedLog log, long producerId, short producerEpoch,
                                                 boolean isTransactional, int leaderEpoch, AppendOrigin origin) {
        AtomicInteger sequence = new AtomicInteger(0);
        return keys -> {
            int baseSequence = sequence.get();
            SimpleRecord[] simpleRecords = keys.stream()
                .map(key -> {
                    byte[] keyBytes = Integer.toString(key).getBytes();
                    return new SimpleRecord(time.milliseconds(), keyBytes, keyBytes);
                })
                .toArray(SimpleRecord[]::new);
            MemoryRecords records = isTransactional
                ? MemoryRecords.withTransactionalRecords(Compression.NONE, producerId, producerEpoch,
                        baseSequence, simpleRecords)
                : MemoryRecords.withIdempotentRecords(Compression.NONE, producerId, producerEpoch,
                        baseSequence, simpleRecords);
            LogAppendInfo appendInfo = log.appendAsLeader(records, leaderEpoch, origin);
            sequence.addAndGet(keys.size());
            return appendInfo;
        };
    }

    private MemoryRecords commitMarker(long producerId, short producerEpoch) {
        return commitMarker(producerId, producerEpoch, time.milliseconds());
    }

    private MemoryRecords commitMarker(long producerId, short producerEpoch, long timestamp) {
        return endTxnMarker(producerId, producerEpoch, ControlRecordType.COMMIT, 0L, timestamp);
    }

    private MemoryRecords abortMarker(long producerId, short producerEpoch) {
        return abortMarker(producerId, producerEpoch, time.milliseconds());
    }

    private MemoryRecords abortMarker(long producerId, short producerEpoch, long timestamp) {
        return endTxnMarker(producerId, producerEpoch, ControlRecordType.ABORT, 0L, timestamp);
    }

    private MemoryRecords endTxnMarker(
        long producerId,
        short producerEpoch,
        ControlRecordType controlRecordType,
        long offset,
        long timestamp
    ) {
        var endTxnMarker = new EndTransactionMarker(controlRecordType, 0);
        return MemoryRecords.withEndTransactionMarker(offset, timestamp, RecordBatch.NO_PARTITION_LEADER_EPOCH,
            producerId, producerEpoch, endTxnMarker);
    }

    /**
     * We need to run a two pass clean to perform the following steps to stimulate a proper clean:
     *  1. On the first run, set the delete horizon in the batches with tombstone or markers with empty txn records.
     *  2. For the second pass, we will advance the current time by tombstoneRetentionMs, which will cause the
     *     tombstones to expire, leading to their prompt removal from the log.
     * Returns the first dirty offset in the log as a result of the second cleaning.
     */
    private long runTwoPassClean(
        Cleaner cleaner,
        LogToClean logToClean,
        long currentTime
    ) throws IOException, DigestException {
        return runTwoPassClean(cleaner, logToClean, currentTime, 86400000L);
    }

    private long runTwoPassClean(
        Cleaner cleaner,
        LogToClean logToClean,
        long currentTime,
        long tombstoneRetentionMs
    ) throws IOException, DigestException {
        cleaner.doClean(logToClean, currentTime);
        return cleaner.doClean(logToClean, currentTime + tombstoneRetentionMs + 1).getKey();
    }

    private CleanerConfig makeReconfigureConfig(Map<String, Object> overrides) {
        ConfigDef configDef = new ConfigDef(CleanerConfig.CONFIG_DEF)
            .define(ServerConfigs.MESSAGE_MAX_BYTES_CONFIG, ConfigDef.Type.INT,
                ServerLogConfigs.MAX_MESSAGE_BYTES_DEFAULT, ConfigDef.Importance.HIGH,
                ServerConfigs.MESSAGE_MAX_BYTES_DOC);
        return new CleanerConfig(new AbstractConfig(configDef, new HashMap<>(overrides)));
    }
}
