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
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.common.message.AbortedTxn;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.record.internal.ControlRecordType;
import org.apache.kafka.common.record.internal.DefaultRecordBatch;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.MemoryRecordsBuilder;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.transaction.TransactionLogConfig;
import org.apache.kafka.metadata.MockConfigRepository;
import org.apache.kafka.server.common.TransactionVersion;
import org.apache.kafka.server.util.MockTime;
import org.apache.kafka.server.util.Scheduler;
import org.apache.kafka.storage.internals.checkpoint.CleanShutdownFileHandler;
import org.apache.kafka.storage.internals.epoch.LeaderEpochFileCache;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class LogLoaderTest {

    private final BrokerTopicStats brokerTopicStats = new BrokerTopicStats();
    private final ProducerStateManagerConfig producerStateManagerConfig =
            new ProducerStateManagerConfig(TransactionLogConfig.PRODUCER_ID_EXPIRATION_MS_DEFAULT, false);
    private final File tmpDir = TestUtils.tempDirectory();
    private final File logDir = TestUtils.randomPartitionLogDir(tmpDir);
    private final List<UnifiedLog> logsToClose = new ArrayList<>();
    private final MockTime mockTime = new MockTime();

    @AfterEach
    public void tearDown() throws IOException {
        brokerTopicStats.close();
        logsToClose.forEach(l -> Utils.closeQuietly(l, "UnifiedLog"));
        Utils.delete(tmpDir);
    }

    private enum ErrorType {
        IO_EXCEPTION,
        RUNTIME_EXCEPTION,
        KAFKA_STORAGE_EXCEPTION_WITH_IO_EXCEPTION_CAUSE,
        KAFKA_STORAGE_EXCEPTION_WITHOUT_IO_EXCEPTION_CAUSE
    }

    private static class SimulateError {
        boolean hasError = false;
        ErrorType errorType = ErrorType.RUNTIME_EXCEPTION;
    }

    private record LogAndSegment(UnifiedLog log, LogSegment segment) { }

    @Test
    public void testLogRecoveryIsCalledUponBrokerCrash() throws Exception {
        // LogManager must realize correctly if the last shutdown was not clean and the logs need
        // to run recovery while loading upon subsequent broker boot up.
        File logDir = TestUtils.tempDirectory();
        LogConfig logConfig = new LogConfig(Map.of());
        List<File> logDirs = List.of(logDir);
        TopicPartition topicPartition = new TopicPartition("foo", 0);
        MockTime time = new MockTime();
        LogDirFailureChannel logDirFailureChannel = new LogDirFailureChannel(logDirs.size());

        AtomicBoolean cleanShutdownInterceptedValue = new AtomicBoolean(false);
        SimulateError simulateError = new SimulateError();

        CleanShutdownFileHandler cleanShutdownFileHandler = new CleanShutdownFileHandler(logDir.getPath());
        {
            LogManager logManager = initializeLogManagerForSimulatingErrorTest(logConfig, logDirs, topicPartition, time, cleanShutdownInterceptedValue, simulateError);

            // Load logs after a clean shutdown
            cleanShutdownFileHandler.write(0L);
            cleanShutdownInterceptedValue.set(false);
            LogConfig defaultConfig = logManager.currentDefaultConfig();
            logManager.loadLogs(defaultConfig, logManager.fetchTopicConfigOverrides(defaultConfig, Set.of()), l -> false);
            assertTrue(cleanShutdownInterceptedValue.get(), "Unexpected value intercepted for clean shutdown flag");
            assertFalse(cleanShutdownFileHandler.exists(), "Clean shutdown file must not exist after loadLogs has completed");
            // Load logs without clean shutdown file
            cleanShutdownInterceptedValue.set(true);
            defaultConfig = logManager.currentDefaultConfig();
            logManager.loadLogs(defaultConfig, logManager.fetchTopicConfigOverrides(defaultConfig, Set.of()), l -> false);
            assertFalse(cleanShutdownInterceptedValue.get(), "Unexpected value intercepted for clean shutdown flag");
            assertFalse(cleanShutdownFileHandler.exists(), "Clean shutdown file must not exist after loadLogs has completed");
            // Create clean shutdown file and then simulate error while loading logs such that log loading does not complete.
            cleanShutdownFileHandler.write(0L);
            logManager.shutdown();
        }

        {
            LogManager logManager = initializeLogManagerForSimulatingErrorTest(logConfig, logDirs, topicPartition,
                    logDirFailureChannel, time, cleanShutdownInterceptedValue, simulateError);

            Callable<Void> runLoadLogs = () -> {
                LogConfig defaultConfig = logManager.currentDefaultConfig();
                logManager.loadLogs(defaultConfig, logManager.fetchTopicConfigOverrides(defaultConfig, Set.of()), l -> false);
                return null;
            };

            // Simulate Runtime error
            simulateError.hasError = true;
            simulateError.errorType = ErrorType.RUNTIME_EXCEPTION;
            assertThrows(RuntimeException.class, runLoadLogs::call);
            assertFalse(cleanShutdownFileHandler.exists(), "Clean shutdown file must not have existed");
            assertFalse(logDirFailureChannel.hasOfflineLogDir(logDir.getAbsolutePath()), "log dir should not turn offline when Runtime Exception thrown");

            // Simulate Kafka storage error with IOException cause
            // in this case, the logDir will be added into offline list before KafkaStorageThrown. So we don't verify it here
            simulateError.errorType = ErrorType.KAFKA_STORAGE_EXCEPTION_WITH_IO_EXCEPTION_CAUSE;
            assertDoesNotThrow(runLoadLogs::call, "KafkaStorageException with IOException cause should be caught and handled");

            // Simulate Kafka storage error without IOException cause
            simulateError.errorType = ErrorType.KAFKA_STORAGE_EXCEPTION_WITHOUT_IO_EXCEPTION_CAUSE;
            assertThrows(KafkaStorageException.class, runLoadLogs::call, "should throw exception when KafkaStorageException without IOException cause");
            assertFalse(logDirFailureChannel.hasOfflineLogDir(logDir.getAbsolutePath()), "log dir should not turn offline when KafkaStorageException without IOException cause thrown");

            // Simulate IO error
            simulateError.errorType = ErrorType.IO_EXCEPTION;
            assertDoesNotThrow(runLoadLogs::call, "IOException should be caught and handled");
            assertTrue(logDirFailureChannel.hasOfflineLogDir(logDir.getAbsolutePath()), "the log dir should turn offline after IOException thrown");

            // Do not simulate error on next call to LogManager#loadLogs. LogManager must understand that log had unclean shutdown the last time.
            simulateError.hasError = false;
            cleanShutdownInterceptedValue.set(true);
            LogConfig defaultConfig = logManager.currentDefaultConfig();
            logManager.loadLogs(defaultConfig, logManager.fetchTopicConfigOverrides(defaultConfig, Set.of()), l -> false);
            assertFalse(cleanShutdownInterceptedValue.get(), "Unexpected value for clean shutdown flag");
            logManager.shutdown();
        }
    }

    private LogManager initializeLogManagerForSimulatingErrorTest(
            LogConfig logConfig,
            List<File> logDirs,
            TopicPartition topicPartition,
            MockTime time,
            AtomicBoolean cleanShutdownInterceptedValue,
            SimulateError simulateError
    ) throws IOException {
        return initializeLogManagerForSimulatingErrorTest(logConfig, logDirs, topicPartition,
                new LogDirFailureChannel(logDirs.size()), time, cleanShutdownInterceptedValue, simulateError);
    }

    private LogManager initializeLogManagerForSimulatingErrorTest(
            LogConfig logConfig,
            List<File> logDirs,
            TopicPartition topicPartition,
            LogDirFailureChannel logDirFailureChannel,
            MockTime time,
            AtomicBoolean cleanShutdownInterceptedValue,
            SimulateError simulateError
    ) throws IOException {
        LogManager logManager = interceptedLogManager(logConfig, logDirs, logDirFailureChannel, time,
                cleanShutdownInterceptedValue, simulateError);
        logManager.getOrCreateLog(topicPartition, true, false, Optional.empty());
        assertFalse(logDirFailureChannel.hasOfflineLogDir(logDirs.get(0).getAbsolutePath()), "log dir should not be offline before load logs");
        return logManager;
    }

    // Create a LogManager with some overridden methods to facilitate interception of clean shutdown
    // flag and to inject an error
    private LogManager interceptedLogManager(LogConfig logConfig,
                                             List<File> logDirs,
                                             LogDirFailureChannel logDirFailureChannel,
                                             MockTime time,
                                             AtomicBoolean cleanShutdownInterceptedValue,
                                             SimulateError simulateError
    ) throws IOException {
        return new LogManager(
                logDirs.stream().map(File::getAbsoluteFile).toList(),
                List.of(),
                new MockConfigRepository(),
                logConfig,
                new CleanerConfig(false),
                4,
                1000L,
                10000L,
                10000L,
                1000L,
                5 * 60 * 1000,
                producerStateManagerConfig,
                TransactionLogConfig.PRODUCER_ID_EXPIRATION_CHECK_INTERVAL_MS_DEFAULT,
                time.scheduler,
                new BrokerTopicStats(),
                logDirFailureChannel,
                time,
                false,
                100L,
                LogCleaner::new) {

            @Override
            public UnifiedLog loadLog(File logDir, boolean hadCleanShutdown, Map<TopicPartition, Long> recoveryPoints,
                                      Map<TopicPartition, Long> logStartOffsets, LogConfig defaultConfig,
                                      Map<String, LogConfig> topicConfigs, ConcurrentMap<String, Integer> numRemainingSegments,
                                      Function<UnifiedLog, Boolean> shouldBeStrayKraftLog) throws IOException {
                if (simulateError.hasError) {
                    switch (simulateError.errorType) {
                        case KAFKA_STORAGE_EXCEPTION_WITH_IO_EXCEPTION_CAUSE:
                            throw new KafkaStorageException(new IOException("Simulated Kafka storage error with IOException cause"));
                        case KAFKA_STORAGE_EXCEPTION_WITHOUT_IO_EXCEPTION_CAUSE:
                            throw new KafkaStorageException("Simulated Kafka storage error without IOException cause");
                        case IO_EXCEPTION:
                            throw new IOException("Simulated IO error");
                        default:
                            throw new RuntimeException("Simulated Runtime error");
                    }
                }
                cleanShutdownInterceptedValue.set(hadCleanShutdown);
                TopicPartition topicPartition = UnifiedLog.parseTopicPartitionName(logDir);
                LogConfig config = topicConfigs.getOrDefault(topicPartition.topic(), defaultConfig);
                long logRecoveryPoint = recoveryPoints.getOrDefault(topicPartition, 0L);
                long logStartOffset = logStartOffsets.getOrDefault(topicPartition, 0L);
                LogDirFailureChannel innerLogDirFailureChannel = new LogDirFailureChannel(1);
                LogSegments segments = new LogSegments(topicPartition);
                LeaderEpochFileCache leaderEpochCache = UnifiedLog.createLeaderEpochCache(
                        logDir, topicPartition, innerLogDirFailureChannel, Optional.empty(), time.scheduler);
                ProducerStateManager producerStateManager = new ProducerStateManager(topicPartition, logDir,
                        this.maxTransactionTimeoutMs(), this.producerStateManagerConfig(), time);
                LogLoader logLoader = new LogLoader(logDir, topicPartition, config, time.scheduler, time,
                        innerLogDirFailureChannel, hadCleanShutdown, segments, logStartOffset, logRecoveryPoint,
                        leaderEpochCache, producerStateManager, new ConcurrentHashMap<>(), false);
                LoadedLogOffsets offsets = logLoader.load();
                LocalLog localLog = new LocalLog(logDir, logConfig, segments, offsets.recoveryPoint(),
                        offsets.nextOffsetMetadata(), mockTime.scheduler, mockTime, topicPartition,
                        innerLogDirFailureChannel);
                return new UnifiedLog(offsets.logStartOffset(), localLog, brokerTopicStats,
                        this.producerIdExpirationCheckIntervalMs(), leaderEpochCache,
                        producerStateManager, Optional.empty(), true, LogOffsetsListener.NO_OP_OFFSETS_LISTENER);
            }
        };
    }

    @Test
    public void testProducerSnapshotsRecoveryAfterUncleanShutdown() throws IOException {
        LogConfig logConfig = new LogConfig(Map.of(LogConfig.INTERNAL_SEGMENT_BYTES_CONFIG, "640"));
        UnifiedLog log = createLog(logDir, logConfig);
        assertTrue(log.oldestProducerSnapshotOffset().isEmpty());

        for (int i = 0; i <= 100; i++) {
            SimpleRecord record = new SimpleRecord(mockTime.milliseconds(), String.valueOf(i).getBytes());
            log.appendAsLeader(LogTestUtils.records(List.of(record)), 0);
        }

        assertTrue(log.logSegments().size() >= 5);
        List<Long> segmentOffsets = log.logSegments().stream().map(LogSegment::baseOffset).toList();
        long activeSegmentOffset = segmentOffsets.get(segmentOffsets.size() - 1);

        // We want the recovery point to be past the segment offset and before the last 2 segments including a gap of
        // 1 segment. We collect the data before closing the log.
        long offsetForSegmentAfterRecoveryPoint = segmentOffsets.get(segmentOffsets.size() - 3);
        long offsetForRecoveryPointSegment = segmentOffsets.get(segmentOffsets.size() - 4);
        Set<Long> segOffsetsBeforeRecovery = segmentOffsets.stream().filter(o -> o < offsetForRecoveryPointSegment).collect(Collectors.toSet());
        Set<Long> segOffsetsAfterRecovery = segmentOffsets.stream().filter(o -> o >= offsetForRecoveryPointSegment).collect(Collectors.toSet());
        long recoveryPoint = offsetForRecoveryPointSegment + 1;
        assertTrue(recoveryPoint < offsetForSegmentAfterRecoveryPoint);
        log.close();

        Set<LogSegment> segmentsWithReads = new HashSet<>();
        Set<LogSegment> recoveredSegments = new HashSet<>();
        Set<Long> expectedSegmentsWithReads = new HashSet<>(segOffsetsBeforeRecovery);
        expectedSegmentsWithReads.add(activeSegmentOffset);
        List<Long> allBaseOffsets = log.logSegments().stream().map(LogSegment::baseOffset).toList();
        Set<Long> expectedSnapshotOffsets = new HashSet<>(allBaseOffsets.subList(allBaseOffsets.size() - 4, allBaseOffsets.size()));
        expectedSnapshotOffsets.add(log.logEndOffset());

        // Retain snapshots for the last 2 segments
        log.producerStateManager().deleteSnapshotsBefore(segmentOffsets.get(segmentOffsets.size() - 2));
        log = createLogWithInterceptedReads(logConfig, offsetForRecoveryPointSegment, segmentsWithReads, recoveredSegments);
        // We will reload all segments because the recovery point is behind the producer snapshot files (pre KAFKA-5829 behaviour)
        assertEquals(expectedSegmentsWithReads, segmentsWithReads.stream().map(LogSegment::baseOffset).collect(Collectors.toSet()));
        assertEquals(segOffsetsAfterRecovery, recoveredSegments.stream().map(LogSegment::baseOffset).collect(Collectors.toSet()));
        assertEquals(expectedSnapshotOffsets, new HashSet<>(LogTestUtils.listProducerSnapshotOffsets(logDir)));
        log.close();
        segmentsWithReads.clear();
        recoveredSegments.clear();

        // Only delete snapshots before the base offset of the recovery point segment (post KAFKA-5829 behaviour) to
        // avoid reading all segments
        log.producerStateManager().deleteSnapshotsBefore(offsetForRecoveryPointSegment);
        log = createLogWithInterceptedReads(logConfig, recoveryPoint, segmentsWithReads, recoveredSegments);
        assertEquals(Set.of(activeSegmentOffset), segmentsWithReads.stream().map(LogSegment::baseOffset).collect(Collectors.toSet()));
        assertEquals(segOffsetsAfterRecovery, recoveredSegments.stream().map(LogSegment::baseOffset).collect(Collectors.toSet()));
        assertEquals(expectedSnapshotOffsets, new HashSet<>(LogTestUtils.listProducerSnapshotOffsets(logDir)));

        log.close();
    }

    private UnifiedLog createLogWithInterceptedReads(LogConfig logConfig,
                                                     long recoveryPoint,
                                                     Set<LogSegment> segmentsWithReads,
                                                     Set<LogSegment> recoveredSegments
    ) throws IOException {
        int maxTransactionTimeoutMs = 5 * 60 * 1000;
        int producerIdExpirationCheckIntervalMs = TransactionLogConfig.PRODUCER_ID_EXPIRATION_CHECK_INTERVAL_MS_DEFAULT;
        TopicPartition topicPartition = UnifiedLog.parseTopicPartitionName(logDir);
        LogDirFailureChannel logDirFailureChannel = new LogDirFailureChannel(10);
        // Intercept all segment read calls
        LogSegments interceptedLogSegments = new LogSegments(topicPartition) {
            @Override
            public LogSegment add(LogSegment segment) {
                LogSegment wrapper = spy(segment);
                try {
                    doAnswer(in -> {
                        segmentsWithReads.add(wrapper);
                        return segment.read(in.getArgument(0, Long.class), in.getArgument(1, Integer.class), in.getArgument(2, Optional.class), in.getArgument(3, Boolean.class));
                    }).when(wrapper).read(anyLong(), anyInt(), any(), anyBoolean());
                    doAnswer(in -> {
                        recoveredSegments.add(wrapper);
                        return segment.recover(in.getArgument(0, ProducerStateManager.class), in.getArgument(1, LeaderEpochFileCache.class));
                    }).when(wrapper).recover(any(), any());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                return super.add(wrapper);
            }
        };
        LeaderEpochFileCache leaderEpochCache = UnifiedLog.createLeaderEpochCache(
                logDir, topicPartition, logDirFailureChannel, Optional.empty(), mockTime.scheduler);
        ProducerStateManager producerStateManager = new ProducerStateManager(topicPartition, logDir,
                maxTransactionTimeoutMs, producerStateManagerConfig, mockTime);
        LogLoader logLoader = new LogLoader(
                logDir,
                topicPartition,
                logConfig,
                mockTime.scheduler,
                mockTime,
                logDirFailureChannel,
                false,
                interceptedLogSegments,
                0L,
                recoveryPoint,
                leaderEpochCache,
                producerStateManager,
                new ConcurrentHashMap<>(),
                false
        );
        LoadedLogOffsets offsets = logLoader.load();
        LocalLog localLog = new LocalLog(logDir, logConfig, interceptedLogSegments, offsets.recoveryPoint(),
                offsets.nextOffsetMetadata(), mockTime.scheduler, mockTime, topicPartition,
                logDirFailureChannel);
        return new UnifiedLog(offsets.logStartOffset(), localLog, brokerTopicStats,
                producerIdExpirationCheckIntervalMs, leaderEpochCache, producerStateManager,
                Optional.empty(), false, LogOffsetsListener.NO_OP_OFFSETS_LISTENER);
    }

    private UnifiedLog createLog(File dir, LogConfig config) throws IOException {
        return createLog(dir, config, true);
    }

    private UnifiedLog createLog(File dir, LogConfig config, boolean lastShutdownClean) throws IOException {
        return createLog(dir, config, 0L, lastShutdownClean);
    }

    private UnifiedLog createLog(File dir, LogConfig config, long recoveryPoint, boolean lastShutdownClean) throws IOException {
        return createLog(dir, config, brokerTopicStats, 0L, recoveryPoint, mockTime.scheduler, mockTime,
                producerStateManagerConfig.producerIdExpirationMs(), lastShutdownClean);
    }

    private UnifiedLog createLog(File dir, LogConfig config, long logStartOffset, long recoveryPoint) throws IOException {
        return createLog(dir, config, brokerTopicStats, logStartOffset, recoveryPoint, mockTime.scheduler, mockTime,
                producerStateManagerConfig.producerIdExpirationMs(), false);
    }

    private UnifiedLog createLog(File dir,
                                 LogConfig config,
                                 BrokerTopicStats brokerTopicStats,
                                 long logStartOffset,
                                 long recoveryPoint,
                                 Scheduler scheduler,
                                 Time time,
                                 int maxProducerIdExpirationMs,
                                 boolean lastShutdownClean) throws IOException {
        UnifiedLog log = LogTestUtils.createLog(
                dir,
                config,
                brokerTopicStats,
                scheduler,
                time,
                logStartOffset,
                recoveryPoint,
                new ProducerStateManagerConfig(maxProducerIdExpirationMs, false),
                600000,
                lastShutdownClean,
                Optional.empty(),
                new ConcurrentHashMap<>());
        logsToClose.add(log);
        return log;
    }

    private LogAndSegment createLogWithOffsetOverflow(LogConfig logConfig) throws IOException {
        LogTestUtils.initializeLogDirWithOverflowedSegment(logDir);

        UnifiedLog log = createLog(logDir, logConfig, Long.MAX_VALUE, true);
        LogSegment segmentWithOverflow = LogTestUtils.firstOverflowSegment(log).orElseThrow(() ->
            new AssertionError("Failed to create log with a segment which has overflowed offsets")
        );

        return new LogAndSegment(log, segmentWithOverflow);
    }

    private UnifiedLog recoverAndCheck(LogConfig config, List<Long> expectedKeys) throws IOException {
        // method is called only in case of recovery from hard reset
        UnifiedLog recoveredLog = LogTestUtils.recoverAndCheck(logDir, config, expectedKeys, brokerTopicStats, mockTime, mockTime.scheduler);
        logsToClose.add(recoveredLog);
        return recoveredLog;
    }

    /**
     * Wrap a single record log buffer with leader epoch.
     */
    private MemoryRecords singletonRecordsWithLeaderEpoch(byte[] value, int leaderEpoch, long offset) {
        List<SimpleRecord> records = List.of(new SimpleRecord(RecordBatch.NO_TIMESTAMP, null, value));

        ByteBuffer buf = ByteBuffer.allocate(DefaultRecordBatch.sizeInBytes(records));
        MemoryRecordsBuilder builder = MemoryRecords.builder(buf, RecordBatch.CURRENT_MAGIC_VALUE, Compression.NONE,
                TimestampType.CREATE_TIME, offset, mockTime.milliseconds(), leaderEpoch);
        records.forEach(builder::append);
        return builder.build();
    }

    @Test
    public void testSkipLoadingIfEmptyProducerStateBeforeTruncation() throws IOException {
        int maxTransactionTimeoutMs = 60000;
        ProducerStateManagerConfig producerStateManagerConfig = new ProducerStateManagerConfig(300000, false);

        ProducerStateManager stateManager = mock(ProducerStateManager.class);
        when(stateManager.producerStateManagerConfig()).thenReturn(producerStateManagerConfig);
        when(stateManager.maxTransactionTimeoutMs()).thenReturn(maxTransactionTimeoutMs);
        when(stateManager.latestSnapshotOffset()).thenReturn(OptionalLong.empty());
        when(stateManager.mapEndOffset()).thenReturn(0L);
        when(stateManager.isEmpty()).thenReturn(true);
        when(stateManager.firstUnstableOffset()).thenReturn(Optional.empty());

        TopicPartition topicPartition = UnifiedLog.parseTopicPartitionName(logDir);
        LogDirFailureChannel logDirFailureChannel = new LogDirFailureChannel(1);
        LogConfig config = new LogConfig(Map.of());
        LogSegments segments = new LogSegments(topicPartition);
        LeaderEpochFileCache leaderEpochCache = UnifiedLog.createLeaderEpochCache(
                logDir, topicPartition, logDirFailureChannel, Optional.empty(), mockTime.scheduler);
        LoadedLogOffsets offsets = new LogLoader(
                logDir,
                topicPartition,
                config,
                mockTime.scheduler,
                mockTime,
                logDirFailureChannel,
                false,
                segments,
                0L,
                0L,
                leaderEpochCache,
                stateManager,
                new ConcurrentHashMap<>(),
                false
        ).load();
        LocalLog localLog = new LocalLog(logDir, config, segments, offsets.recoveryPoint(),
                offsets.nextOffsetMetadata(), mockTime.scheduler, mockTime, topicPartition,
                logDirFailureChannel);
        UnifiedLog log = new UnifiedLog(offsets.logStartOffset(),
                localLog,
                brokerTopicStats,
                30000,
                leaderEpochCache,
                stateManager,
                Optional.empty(),
                false,
                LogOffsetsListener.NO_OP_OFFSETS_LISTENER);

        verify(stateManager).updateMapEndOffset(0L);
        verify(stateManager).removeStraySnapshots(any());
        verify(stateManager).takeSnapshot();
        verify(stateManager).truncateAndReload(eq(0L), eq(0L), anyLong());

        // Append some messages
        reset(stateManager);
        when(stateManager.firstUnstableOffset()).thenReturn(Optional.empty());

        log.appendAsLeader(LogTestUtils.records(List.of(new SimpleRecord("a".getBytes()))), 0);
        log.appendAsLeader(LogTestUtils.records(List.of(new SimpleRecord("b".getBytes()))), 0);

        verify(stateManager).updateMapEndOffset(1L);
        verify(stateManager).updateMapEndOffset(2L);

        // Now truncate
        reset(stateManager);
        when(stateManager.firstUnstableOffset()).thenReturn(Optional.empty());
        when(stateManager.latestSnapshotOffset()).thenReturn(OptionalLong.empty());
        when(stateManager.isEmpty()).thenReturn(true);
        when(stateManager.mapEndOffset()).thenReturn(2L);
        // Truncation causes the map end offset to reset to 0
        when(stateManager.mapEndOffset()).thenReturn(0L);

        log.truncateTo(1L);

        verify(stateManager).truncateAndReload(eq(0L), eq(1L), anyLong());
        verify(stateManager).updateMapEndOffset(1L);
        verify(stateManager, times(2)).takeSnapshot();
    }

    @Test
    public void testRecoverAfterNonMonotonicCoordinatorEpochWrite() throws IOException {
        // Due to KAFKA-9144, we may encounter a coordinator epoch which goes backwards.
        // This test case verifies that recovery logic relaxes validation in this case and
        // just takes the latest write.
        long producerId = 1L;
        int coordinatorEpoch = 5;
        LogConfig logConfig = new LogTestUtils.LogConfigBuilder()
                .segmentBytes(1024 * 1024 * 5)
                .build();
        UnifiedLog log = createLog(logDir, logConfig);
        short epoch = (short) 0;

        long firstAppendTimestamp = mockTime.milliseconds();
        LogTestUtils.appendEndTxnMarkerAsLeader(log, producerId, epoch, ControlRecordType.ABORT,
                firstAppendTimestamp, coordinatorEpoch, 0, TransactionVersion.TV_0.featureLevel());
        assertEquals(firstAppendTimestamp, log.producerStateManager().lastEntry(producerId).get().lastTimestamp());

        int maxProducerIdExpirationMs = TransactionLogConfig.PRODUCER_ID_EXPIRATION_MS_DEFAULT;
        mockTime.sleep(maxProducerIdExpirationMs);
        assertEquals(Optional.empty(), log.producerStateManager().lastEntry(producerId));

        long secondAppendTimestamp = mockTime.milliseconds();
        LogTestUtils.appendEndTxnMarkerAsLeader(log, producerId, epoch, ControlRecordType.ABORT,
                secondAppendTimestamp, coordinatorEpoch - 1, 0, TransactionVersion.TV_0.featureLevel());

        log.close();

        // Force recovery by setting the recoveryPoint to the log start
        log = createLog(logDir, logConfig, 0L, false);
        assertEquals(secondAppendTimestamp, log.producerStateManager().lastEntry(producerId).get().lastTimestamp());
        log.close();
    }

    @Test
    public void testSkipTruncateAndReloadIfNewMessageFormatAndCleanShutdown() throws IOException {
        ProducerStateManagerConfig producerStateManagerConfig = new ProducerStateManagerConfig(300000, false);

        ProducerStateManager stateManager = mock(ProducerStateManager.class);
        when(stateManager.latestSnapshotOffset()).thenReturn(OptionalLong.empty());
        when(stateManager.isEmpty()).thenReturn(true);
        when(stateManager.firstUnstableOffset()).thenReturn(Optional.empty());
        when(stateManager.producerStateManagerConfig()).thenReturn(producerStateManagerConfig);
        when(stateManager.maxTransactionTimeoutMs()).thenReturn(60000);

        TopicPartition topicPartition = UnifiedLog.parseTopicPartitionName(logDir);
        LogConfig config = new LogConfig(Map.of());
        LogSegments segments = new LogSegments(topicPartition);
        LeaderEpochFileCache leaderEpochCache = UnifiedLog.createLeaderEpochCache(
                logDir, topicPartition, null, Optional.empty(), mockTime.scheduler);
        LoadedLogOffsets offsets = new LogLoader(
                logDir,
                topicPartition,
                config,
                mockTime.scheduler,
                mockTime,
                null,
                true,
                segments,
                0L,
                0L,
                leaderEpochCache,
                stateManager,
                new ConcurrentHashMap<>(),
                false
        ).load();
        LocalLog localLog = new LocalLog(logDir, config, segments, offsets.recoveryPoint(),
                offsets.nextOffsetMetadata(), mockTime.scheduler, mockTime, topicPartition,
                null);
        new UnifiedLog(offsets.logStartOffset(),
                localLog,
                brokerTopicStats,
                30000,
                leaderEpochCache,
                stateManager,
                Optional.empty(),
                false,
                LogOffsetsListener.NO_OP_OFFSETS_LISTENER);

        verify(stateManager).removeStraySnapshots(anyList());
        verify(stateManager, times(2)).updateMapEndOffset(0L);
        verify(stateManager, times(2)).takeSnapshot();
        verify(stateManager).isEmpty();
        verify(stateManager).firstUnstableOffset();
    }

    @Test
    public void testLoadProducersAfterDeleteRecordsMidSegment() throws IOException {
        LogConfig logConfig = new LogTestUtils.LogConfigBuilder()
                .segmentBytes(2048 * 5)
                .build();
        UnifiedLog log = createLog(logDir, logConfig);
        long pid1 = 1L;
        long pid2 = 2L;
        short epoch = (short) 0;

        log.appendAsLeader(LogTestUtils.records(List.of(new SimpleRecord(mockTime.milliseconds(), "a".getBytes())), pid1,
                epoch, 0), 0);
        log.appendAsLeader(LogTestUtils.records(List.of(new SimpleRecord(mockTime.milliseconds(), "b".getBytes())), pid2,
                epoch, 0), 0);
        assertEquals(2, log.activeProducersWithLastSequence().size());

        log.updateHighWatermark(log.logEndOffset());
        log.maybeIncrementLogStartOffset(1L, LogStartOffsetIncrementReason.ClientRecordDeletion);

        // Deleting records should not remove producer state
        assertEquals(2, log.activeProducersWithLastSequence().size());
        int retainedLastSeq = log.activeProducersWithLastSequence().get(pid2);
        assertEquals(0, retainedLastSeq);

        log.close();

        // Because the log start offset did not advance, producer snapshots will still be present and the state will be rebuilt
        UnifiedLog reloadedLog = createLog(logDir, logConfig, 1L, 0L);
        assertEquals(2, reloadedLog.activeProducersWithLastSequence().size());
        int reloadedLastSeq = reloadedLog.activeProducersWithLastSequence().get(pid2);
        assertEquals(retainedLastSeq, reloadedLastSeq);
    }

    @Test
    public void testLoadingLogKeepsLargestStrayProducerStateSnapshot() throws IOException {
        LogConfig logConfig = new LogTestUtils.LogConfigBuilder()
                .segmentBytes(2048 * 5)
                .retentionBytes(0)
                .retentionMs(1000 * 60)
                .fileDeleteDelayMs(0)
                .build();
        UnifiedLog log = createLog(logDir, logConfig);
        long pid1 = 1L;
        short epoch = (short) 0;

        log.appendAsLeader(LogTestUtils.records(List.of(new SimpleRecord("a".getBytes())), pid1, epoch, 0), 0);
        log.roll();
        log.appendAsLeader(LogTestUtils.records(List.of(new SimpleRecord("b".getBytes())), pid1, epoch, 1), 0);
        log.roll();

        log.appendAsLeader(LogTestUtils.records(List.of(new SimpleRecord("c".getBytes())), pid1, epoch, 2), 0);
        log.appendAsLeader(LogTestUtils.records(List.of(new SimpleRecord("d".getBytes())), pid1, epoch, 3), 0);

        // Close the log, we should now have 3 segments
        log.close();
        assertEquals(3, log.logSegments().size());
        // We expect 3 snapshot files, two of which are for the first two segments, the last was written out during log closing.
        assertEquals(List.of(1L, 2L, 4L), ProducerStateManager.listSnapshotFiles(logDir).stream().map(s -> s.offset).sorted().toList());
        // Inject a stray snapshot file within the bounds of the log at offset 3, it should be cleaned up after loading the log
        Path straySnapshotFile = LogFileUtils.producerSnapshotFile(logDir, 3).toPath();
        Files.createFile(straySnapshotFile);
        assertEquals(List.of(1L, 2L, 3L, 4L), ProducerStateManager.listSnapshotFiles(logDir).stream().map(s -> s.offset).sorted().toList());

        createLog(logDir, logConfig, false);
        // We should clean up the stray producer state snapshot file, but keep the largest snapshot file (4)
        assertEquals(List.of(1L, 2L, 4L), ProducerStateManager.listSnapshotFiles(logDir).stream().map(s -> s.offset).sorted().toList());
    }

    @Test
    public void testLoadProducersAfterDeleteRecordsOnSegment() throws IOException {
        LogConfig logConfig = new LogTestUtils.LogConfigBuilder()
                .segmentBytes(2048 * 5)
                .build();
        UnifiedLog log = createLog(logDir, logConfig);
        long pid1 = 1L;
        long pid2 = 2L;
        short epoch = (short) 0;

        log.appendAsLeader(LogTestUtils.records(List.of(new SimpleRecord(mockTime.milliseconds(), "a".getBytes())), pid1,
                epoch, 0), 0);
        log.roll();
        log.appendAsLeader(LogTestUtils.records(List.of(new SimpleRecord(mockTime.milliseconds(), "b".getBytes())), pid2,
                epoch, 0), 0);

        assertEquals(2, log.logSegments().size());
        assertEquals(2, log.activeProducersWithLastSequence().size());

        log.updateHighWatermark(log.logEndOffset());
        log.maybeIncrementLogStartOffset(1L, LogStartOffsetIncrementReason.ClientRecordDeletion);
        log.deleteOldSegments();

        // Deleting records should not remove producer state
        assertEquals(1, log.logSegments().size());
        assertEquals(2, log.activeProducersWithLastSequence().size());
        int retainedLastSeq = log.activeProducersWithLastSequence().get(pid2);
        assertEquals(0, retainedLastSeq);

        log.close();

        // After reloading log, producer state should not be regenerated
        UnifiedLog reloadedLog = createLog(logDir, logConfig, 1L, 0L);
        assertEquals(1, reloadedLog.activeProducersWithLastSequence().size());
        int reloadedEntry = reloadedLog.activeProducersWithLastSequence().get(pid2);
        assertEquals(retainedLastSeq, reloadedEntry);
    }

    /**
     * Append a bunch of messages to a log and then re-open it both with and without recovery and check that the log re-initializes correctly.
     */
    @Test
    public void testLogRecoversToCorrectOffset() throws IOException {
        int numMessages = 100;
        int messageSize = 100;
        int segmentSize = 7 * messageSize;
        int indexInterval = 3 * messageSize;
        LogConfig logConfig = new LogTestUtils.LogConfigBuilder()
                .segmentBytes(segmentSize)
                .indexIntervalBytes(indexInterval)
                .segmentIndexBytes(4096)
                .build();
        UnifiedLog log = createLog(logDir, logConfig);
        for (int i = 0; i < numMessages; i++) {
            log.appendAsLeader(LogTestUtils.singletonRecords(TestUtils.randomBytes(messageSize), mockTime.milliseconds() + i * 10), 0);
        }
        assertEquals(numMessages, log.logEndOffset(),
                "After appending " + numMessages + " messages to an empty log, the log end offset should be " + numMessages);
        long lastIndexOffset = log.activeSegment().offsetIndex().lastOffset();
        int numIndexEntries = log.activeSegment().offsetIndex().entries();
        long lastOffset = log.logEndOffset();
        // After segment is closed, the last entry in the time index should be (largest timestamp -> last offset).
        long lastTimeIndexOffset = log.logEndOffset() - 1;
        long lastTimeIndexTimestamp = log.activeSegment().largestTimestamp();
        // Depending on when the last time index entry is inserted, an entry may or may not be inserted into the time index.
        int numTimeIndexEntries = log.activeSegment().timeIndex().entries() +
                ((log.activeSegment().timeIndex().lastEntry().offset() == log.logEndOffset() - 1) ? 0 : 1);

        log.close();

        log = createLog(logDir, logConfig, lastOffset, false);
        verifyRecoveredLog(log, lastOffset, numMessages, lastIndexOffset, numIndexEntries, lastTimeIndexOffset, lastTimeIndexTimestamp, numTimeIndexEntries);
        log.close();

        // test recovery case
        int recoveryPoint = 10;
        log = createLog(logDir, logConfig, recoveryPoint, false);
        // the recovery point should not be updated after unclean shutdown until the log is flushed
        verifyRecoveredLog(log, recoveryPoint, numMessages, lastIndexOffset, numIndexEntries, lastTimeIndexOffset, lastTimeIndexTimestamp, numTimeIndexEntries);
        log.flush(false);
        verifyRecoveredLog(log, lastOffset, numMessages, lastIndexOffset, numIndexEntries, lastTimeIndexOffset, lastTimeIndexTimestamp, numTimeIndexEntries);
        log.close();
    }

    private void verifyRecoveredLog(
            UnifiedLog log,
            long expectedRecoveryPoint,
            int numMessages,
            long lastIndexOffset,
            int numIndexEntries,
            long lastTimeIndexOffset,
            long lastTimeIndexTimestamp,
            int numTimeIndexEntries
    ) throws IOException {
        assertEquals(expectedRecoveryPoint, log.recoveryPoint(), "Unexpected recovery point");
        assertEquals(numMessages, log.logEndOffset(), "Should have " + numMessages + " messages when log is reopened w/o recovery");
        assertEquals(lastIndexOffset, log.activeSegment().offsetIndex().lastOffset(), "Should have same last index offset as before.");
        assertEquals(numIndexEntries, log.activeSegment().offsetIndex().entries(), "Should have same number of index entries as before.");
        assertEquals(lastTimeIndexTimestamp, log.activeSegment().timeIndex().lastEntry().timestamp(), "Should have same last time index timestamp");
        assertEquals(lastTimeIndexOffset, log.activeSegment().timeIndex().lastEntry().offset(), "Should have same last time index offset");
        assertEquals(numTimeIndexEntries, log.activeSegment().timeIndex().entries(), "Should have same number of time index entries as before.");
    }

    /**
     * Test that if we manually delete an index segment it is rebuilt when the log is re-opened
     */
    @Test
    public void testIndexRebuild() throws IOException {
        // publish the messages and close the log
        int numMessages = 200;
        LogConfig logConfig = new LogTestUtils.LogConfigBuilder()
                .segmentBytes(200)
                .indexIntervalBytes(1)
                .build();
        UnifiedLog log = createLog(logDir, logConfig);
        for (int i = 0; i < numMessages; i++) {
            log.appendAsLeader(LogTestUtils.singletonRecords(TestUtils.randomBytes(10), mockTime.milliseconds() + i * 10), 0);
        }
        List<File> indexFiles = log.logSegments().stream().map(LogSegment::offsetIndexFile).toList();
        List<File> timeIndexFiles = log.logSegments().stream().map(LogSegment::timeIndexFile).toList();
        log.close();

        // delete all the index files
        indexFiles.forEach(File::delete);
        timeIndexFiles.forEach(File::delete);

        // reopen the log
        log = createLog(logDir, logConfig, false);
        assertEquals(numMessages, log.logEndOffset(), "Should have " + numMessages + " messages when log is reopened");
        assertTrue(log.logSegments().get(0).offsetIndex().entries() > 0, "The index should have been rebuilt");
        assertTrue(log.logSegments().get(0).timeIndex().entries() > 0, "The time index should have been rebuilt");
        for (int i = 0; i < numMessages; i++) {
            assertEquals(i, LogTestUtils.readLog(log, i, 100).records.batches().iterator().next().lastOffset());
            if (i == 0) {
                assertEquals(log.logSegments().get(0).baseOffset(),
                        log.fetchOffsetByTimestamp(mockTime.milliseconds(), Optional.empty()).timestampAndOffsetOpt().get().offset);
            } else {
                assertEquals(i,
                        log.fetchOffsetByTimestamp(mockTime.milliseconds() + i * 10, Optional.empty()).timestampAndOffsetOpt().get().offset);
            }
        }
        log.close();
    }

    /**
     * Test that if we have corrupted an index segment it is rebuilt when the log is re-opened
     */
    @Test
    public void testCorruptIndexRebuild() throws IOException {
        // publish the messages and close the log
        int numMessages = 200;
        LogConfig logConfig = new LogTestUtils.LogConfigBuilder()
                .segmentBytes(200)
                .indexIntervalBytes(1)
                .build();
        UnifiedLog log = createLog(logDir, logConfig);
        for (int i = 0; i < numMessages; i++) {
            log.appendAsLeader(LogTestUtils.singletonRecords(TestUtils.randomBytes(10), mockTime.milliseconds() + i * 10), 0);
        }
        List<File> indexFiles = log.logSegments().stream().map(LogSegment::offsetIndexFile).toList();
        List<File> timeIndexFiles = log.logSegments().stream().map(LogSegment::timeIndexFile).toList();
        log.close();

        // corrupt all the index files
        for (File file : indexFiles) {
            BufferedWriter bw = new BufferedWriter(new FileWriter(file));
            bw.write("  ");
            bw.close();
        }

        // corrupt all the index files
        for (File file : timeIndexFiles) {
            BufferedWriter bw = new BufferedWriter(new FileWriter(file));
            bw.write("  ");
            bw.close();
        }

        // reopen the log with recovery point=0 so that the segment recovery can be triggered
        log = createLog(logDir, logConfig, false);
        assertEquals(numMessages, log.logEndOffset(), "Should have " + numMessages + " messages when log is reopened");
        for (int i = 0; i < numMessages; i++) {
            assertEquals(i, LogTestUtils.readLog(log, i, 100).records.batches().iterator().next().lastOffset());
            if (i == 0) {
                assertEquals(log.logSegments().get(0).baseOffset(),
                        log.fetchOffsetByTimestamp(mockTime.milliseconds(), Optional.empty()).timestampAndOffsetOpt().get().offset);
            } else {
                assertEquals(i,
                        log.fetchOffsetByTimestamp(mockTime.milliseconds() + i * 10, Optional.empty()).timestampAndOffsetOpt().get().offset);
            }
        }
        log.close();
    }

    /**
     * When we open a log any index segments without an associated log segment should be deleted.
     */
    @Test
    public void testBogusIndexSegmentsAreRemoved() throws IOException {
        File bogusIndex1 = LogFileUtils.offsetIndexFile(logDir, 0);
        File bogusTimeIndex1 = LogFileUtils.timeIndexFile(logDir, 0);
        File bogusIndex2 = LogFileUtils.offsetIndexFile(logDir, 5);
        File bogusTimeIndex2 = LogFileUtils.timeIndexFile(logDir, 5);

        // The files remain absent until we first access it because we are doing lazy loading for time index and offset index
        // files but in this test case we need to create these files in order to test we will remove them.
        Files.createFile(bogusIndex2.toPath());
        Files.createFile(bogusTimeIndex2.toPath());

        Supplier<MemoryRecords> createRecords = () -> LogTestUtils.singletonRecords("test".getBytes(), mockTime.milliseconds());
        LogConfig logConfig = new LogTestUtils.LogConfigBuilder()
                .segmentBytes(createRecords.get().sizeInBytes() * 5)
                .segmentIndexBytes(1000)
                .indexIntervalBytes(1)
                .build();
        UnifiedLog log = createLog(logDir, logConfig);

        // Force the segment to access the index files because we are doing index lazy loading.
        log.logSegments().get(0).offsetIndex();
        log.logSegments().get(0).timeIndex();

        assertTrue(bogusIndex1.length() > 0,
                "The first index file should have been replaced with a larger file");
        assertTrue(bogusTimeIndex1.length() > 0,
                "The first time index file should have been replaced with a larger file");
        assertFalse(bogusIndex2.exists(),
                "The second index file should have been deleted.");
        assertFalse(bogusTimeIndex2.exists(),
                "The second time index file should have been deleted.");

        // check that we can append to the log
        for (int i = 0; i < 10; i++) {
            log.appendAsLeader(createRecords.get(), 0);
        }
        log.delete();
    }

    /**
     * Verify that truncation works correctly after re-opening the log
     */
    @Test
    public void testReopenThenTruncate() throws IOException {
        Supplier<MemoryRecords> createRecords = () -> LogTestUtils.singletonRecords("test".getBytes(), mockTime.milliseconds());
        // create a log
        LogConfig logConfig = new LogTestUtils.LogConfigBuilder()
                .segmentBytes(createRecords.get().sizeInBytes() * 5)
                .segmentIndexBytes(1000)
                .indexIntervalBytes(10000)
                .build();
        UnifiedLog log = createLog(logDir, logConfig);

        // add enough messages to roll over several segments then close and re-open and attempt to truncate
        for (int i = 0; i < 100; i++) {
            log.appendAsLeader(createRecords.get(), 0);
        }
        log.close();
        log = createLog(logDir, logConfig, false);
        log.truncateTo(3);
        assertEquals(1, log.numberOfSegments(), "All but one segment should be deleted.");
        assertEquals(3, log.logEndOffset(), "Log end offset should be 3.");
    }

    /**
     * Any files ending in .deleted should be removed when the log is re-opened.
     */
    @Test
    public void testOpenDeletesObsoleteFiles() throws IOException {
        Supplier<MemoryRecords> createRecords = () -> LogTestUtils.singletonRecords("test".getBytes(), mockTime.milliseconds() - 1000);
        LogConfig logConfig = new LogTestUtils.LogConfigBuilder()
                .segmentBytes(createRecords.get().sizeInBytes() * 5)
                .segmentIndexBytes(1000)
                .retentionMs(999)
                .build();
        UnifiedLog log = createLog(logDir, logConfig);

        // append some messages to create some segments
        for (int i = 0; i < 100; i++) {
            log.appendAsLeader(createRecords.get(), 0);
        }

        // expire all segments
        log.updateHighWatermark(log.logEndOffset());
        log.deleteOldSegments();
        log.close();
        log = createLog(logDir, logConfig, false);
        assertEquals(1, log.numberOfSegments(), "The deleted segments should be gone.");
    }

    @Test
    public void testCorruptLog() throws IOException {
        // append some messages to create some segments
        LogConfig logConfig = new LogTestUtils.LogConfigBuilder()
                .segmentBytes(1000)
                .indexIntervalBytes(1)
                .maxMessageBytes(64 * 1024)
                .build();
        Supplier<MemoryRecords> createRecords = () -> LogTestUtils.singletonRecords("test".getBytes(), mockTime.milliseconds());
        long recoveryPoint = 50L;
        for (int i = 0; i < 10; i++) {
            // create a log and write some messages to it
            logDir.mkdirs();
            UnifiedLog log = createLog(logDir, logConfig);
            long numMessages = 50 + TestUtils.RANDOM.nextInt(50);
            for (int j = 0; j < numMessages; j++) {
                log.appendAsLeader(createRecords.get(), 0);
            }
            List<Record> records = LogTestUtils.allRecords(log);
            log.close();

            // corrupt index and log by appending random bytes
            LogTestUtils.appendNonsenseToFile(log.activeSegment().offsetIndexFile(), TestUtils.RANDOM.nextInt(1024) + 1);
            LogTestUtils.appendNonsenseToFile(log.activeSegment().log().file(), TestUtils.RANDOM.nextInt(1024) + 1);

            // attempt recovery
            log = createLog(logDir, logConfig, 0L, recoveryPoint);
            assertEquals(numMessages, log.logEndOffset());

            List<Record> recovered = LogTestUtils.allRecords(log);
            assertEquals(records.size(), recovered.size());

            for (int j = 0; j < records.size(); j++) {
                Record expected = records.get(j);
                Record actual = recovered.get(j);
                assertEquals(expected.key(), actual.key(), "Keys not equal");
                assertEquals(expected.value(), actual.value(), "Values not equal");
                assertEquals(expected.timestamp(), actual.timestamp(), "Timestamps not equal");
            }

            Utils.delete(logDir);
        }
    }

    @Test
    public void testOverCompactedLogRecovery() throws IOException {
        // append some messages to create some segments
        LogConfig logConfig = new LogTestUtils.LogConfigBuilder()
                .segmentBytes(1000)
                .indexIntervalBytes(1)
                .maxMessageBytes(64 * 1024)
                .build();
        UnifiedLog log = createLog(logDir, logConfig);
        MemoryRecords set1 = MemoryRecords.withRecords(0, Compression.NONE, 0, new SimpleRecord("v1".getBytes(), "k1".getBytes()));
        MemoryRecords set2 = MemoryRecords.withRecords((long) Integer.MAX_VALUE + 2, Compression.NONE, 0, new SimpleRecord("v3".getBytes(), "k3".getBytes()));
        MemoryRecords set3 = MemoryRecords.withRecords((long) Integer.MAX_VALUE + 3, Compression.NONE, 0, new SimpleRecord("v4".getBytes(), "k4".getBytes()));
        MemoryRecords set4 = MemoryRecords.withRecords((long) Integer.MAX_VALUE + 4, Compression.NONE, 0, new SimpleRecord("v5".getBytes(), "k5".getBytes()));
        //Writes into an empty log with baseOffset 0
        log.appendAsFollower(set1, Integer.MAX_VALUE);
        assertEquals(0L, log.activeSegment().baseOffset());
        //This write will roll the segment, yielding a new segment with base offset = max(1, Integer.MAX_VALUE+2) = Integer.MAX_VALUE+2
        log.appendAsFollower(set2, Integer.MAX_VALUE);
        assertEquals((long) Integer.MAX_VALUE + 2, log.activeSegment().baseOffset());
        assertTrue(LogFileUtils.producerSnapshotFile(logDir, (long) Integer.MAX_VALUE + 2).exists());
        //This will go into the existing log
        log.appendAsFollower(set3, Integer.MAX_VALUE);
        assertEquals((long) Integer.MAX_VALUE + 2, log.activeSegment().baseOffset());
        //This will go into the existing log
        log.appendAsFollower(set4, Integer.MAX_VALUE);
        assertEquals((long) Integer.MAX_VALUE + 2, log.activeSegment().baseOffset());
        log.close();
        List<File> indexFiles = Stream.of(logDir.listFiles()).filter(file -> file.getName().contains(".index")).toList();
        assertEquals(2, indexFiles.size());
        for (File file : indexFiles) {
            OffsetIndex offsetIndex = new OffsetIndex(file, Long.parseLong(file.getName().replace(".index", "")));
            assertTrue(offsetIndex.lastOffset() >= 0);
            offsetIndex.close();
        }
        Utils.delete(logDir);
    }

    @Test
    public void testOverCompactedLogRecoveryMultiRecord() throws IOException {
        // append some messages to create some segments
        LogConfig logConfig = new LogTestUtils.LogConfigBuilder()
                .segmentBytes(1000)
                .indexIntervalBytes(1)
                .maxMessageBytes(64 * 1024)
                .build();
        UnifiedLog log = createLog(logDir, logConfig);
        MemoryRecords set1 = MemoryRecords.withRecords(0, Compression.NONE, 0, new SimpleRecord("v1".getBytes(), "k1".getBytes()));
        MemoryRecords set2 = MemoryRecords.withRecords((long) Integer.MAX_VALUE + 2, Compression.gzip().build(), 0,
                new SimpleRecord("v3".getBytes(), "k3".getBytes()),
                new SimpleRecord("v4".getBytes(), "k4".getBytes()));
        MemoryRecords set3 = MemoryRecords.withRecords((long) Integer.MAX_VALUE + 4, Compression.gzip().build(), 0,
                new SimpleRecord("v5".getBytes(), "k5".getBytes()),
                new SimpleRecord("v6".getBytes(), "k6".getBytes()));
        MemoryRecords set4 = MemoryRecords.withRecords((long) Integer.MAX_VALUE + 6, Compression.gzip().build(), 0,
                new SimpleRecord("v7".getBytes(), "k7".getBytes()),
                new SimpleRecord("v8".getBytes(), "k8".getBytes()));
        //Writes into an empty log with baseOffset 0
        log.appendAsFollower(set1, Integer.MAX_VALUE);
        assertEquals(0L, log.activeSegment().baseOffset());
        //This write will roll the segment, yielding a new segment with base offset = max(1, Integer.MAX_VALUE+2) = Integer.MAX_VALUE+2
        log.appendAsFollower(set2, Integer.MAX_VALUE);
        assertEquals((long) Integer.MAX_VALUE + 2, log.activeSegment().baseOffset());
        assertTrue(LogFileUtils.producerSnapshotFile(logDir, (long) Integer.MAX_VALUE + 2).exists());
        //This will go into the existing log
        log.appendAsFollower(set3, Integer.MAX_VALUE);
        assertEquals((long) Integer.MAX_VALUE + 2, log.activeSegment().baseOffset());
        //This will go into the existing log
        log.appendAsFollower(set4, Integer.MAX_VALUE);
        assertEquals((long) Integer.MAX_VALUE + 2, log.activeSegment().baseOffset());
        log.close();
        List<File> indexFiles = Stream.of(logDir.listFiles()).filter(file -> file.getName().contains(".index")).toList();
        assertEquals(2, indexFiles.size());
        for (File file : indexFiles) {
            OffsetIndex offsetIndex = new OffsetIndex(file, Long.parseLong(file.getName().replace(".index", "")));
            assertTrue(offsetIndex.lastOffset() >= 0);
            offsetIndex.close();
        }
        Utils.delete(logDir);
    }

    @Test
    public void testOverCompactedLogRecoveryMultiRecordV1() throws IOException {
        // append some messages to create some segments
        LogConfig logConfig = new LogTestUtils.LogConfigBuilder()
                .segmentBytes(1000)
                .indexIntervalBytes(1)
                .maxMessageBytes(64 * 1024)
                .build();
        UnifiedLog log = createLog(logDir, logConfig);
        MemoryRecords set1 = MemoryRecords.withRecords(RecordBatch.MAGIC_VALUE_V1, 0, Compression.NONE,
                new SimpleRecord("v1".getBytes(), "k1".getBytes()));
        MemoryRecords set2 = MemoryRecords.withRecords(RecordBatch.MAGIC_VALUE_V1, (long) Integer.MAX_VALUE + 2, Compression.gzip().build(),
                new SimpleRecord("v3".getBytes(), "k3".getBytes()),
                new SimpleRecord("v4".getBytes(), "k4".getBytes()));
        MemoryRecords set3 = MemoryRecords.withRecords(RecordBatch.MAGIC_VALUE_V1, (long) Integer.MAX_VALUE + 4, Compression.gzip().build(),
                new SimpleRecord("v5".getBytes(), "k5".getBytes()),
                new SimpleRecord("v6".getBytes(), "k6".getBytes()));
        MemoryRecords set4 = MemoryRecords.withRecords(RecordBatch.MAGIC_VALUE_V1, (long) Integer.MAX_VALUE + 6, Compression.gzip().build(),
                new SimpleRecord("v7".getBytes(), "k7".getBytes()),
                new SimpleRecord("v8".getBytes(), "k8".getBytes()));
        //Writes into an empty log with baseOffset 0
        log.appendAsFollower(set1, Integer.MAX_VALUE);
        assertEquals(0L, log.activeSegment().baseOffset());
        //This write will roll the segment, yielding a new segment with base offset = max(1, 3) = 3
        log.appendAsFollower(set2, Integer.MAX_VALUE);
        assertEquals(3, log.activeSegment().baseOffset());
        assertTrue(LogFileUtils.producerSnapshotFile(logDir, 3).exists());
        //This will also roll the segment, yielding a new segment with base offset = max(5, Integer.MAX_VALUE+4) = Integer.MAX_VALUE+4
        log.appendAsFollower(set3, Integer.MAX_VALUE);
        assertEquals((long) Integer.MAX_VALUE + 4, log.activeSegment().baseOffset());
        assertTrue(LogFileUtils.producerSnapshotFile(logDir, (long) Integer.MAX_VALUE + 4).exists());
        //This will go into the existing log
        log.appendAsFollower(set4, Integer.MAX_VALUE);
        assertEquals((long) Integer.MAX_VALUE + 4, log.activeSegment().baseOffset());
        log.close();
        List<File> indexFiles = Stream.of(logDir.listFiles()).filter(file -> file.getName().contains(".index")).toList();
        assertEquals(3, indexFiles.size());
        for (File file : indexFiles) {
            OffsetIndex offsetIndex = new OffsetIndex(file, Long.parseLong(file.getName().replace(".index", "")));
            assertTrue(offsetIndex.lastOffset() >= 0);
            offsetIndex.close();
        }
        Utils.delete(logDir);
    }

    @Test
    public void testRecoveryOfSegmentWithOffsetOverflow() throws IOException {
        LogConfig logConfig = new LogTestUtils.LogConfigBuilder()
                .indexIntervalBytes(1)
                .fileDeleteDelayMs(1000)
                .build();
        LogAndSegment logAndSegment = createLogWithOffsetOverflow(logConfig);
        List<Long> expectedKeys = LogTestUtils.keysInLog(logAndSegment.log);

        // Run recovery on the log. This should split the segment underneath. Ignore .deleted files as we could still
        // have them lying around after the split.
        UnifiedLog recoveredLog = recoverAndCheck(logConfig, expectedKeys);
        assertEquals(expectedKeys, LogTestUtils.keysInLog(recoveredLog));

        // Running split again would throw an error

        for (LogSegment segment : recoveredLog.logSegments()) {
            assertThrows(IllegalArgumentException.class, () -> logAndSegment.log.splitOverflowedSegment(segment));
        }
    }

    @Test
    public void testRecoveryAfterCrashDuringSplitPhase1() throws IOException {
        LogConfig logConfig = new LogTestUtils.LogConfigBuilder()
                .indexIntervalBytes(1)
                .fileDeleteDelayMs(1000)
                .build();
        LogAndSegment logAndSegment = createLogWithOffsetOverflow(logConfig);
        List<Long> expectedKeys = LogTestUtils.keysInLog(logAndSegment.log);
        int numSegmentsInitial = logAndSegment.log.logSegments().size();

        // Split the segment
        List<LogSegment> newSegments = logAndSegment.log.splitOverflowedSegment(logAndSegment.segment);

        // Simulate recovery just after .cleaned file is created, before rename to .swap. On recovery, existing split
        // operation is aborted but the recovery process itself kicks off split which should complete.
        for (int i = newSegments.size() - 1; i >= 0; i--) {
            newSegments.get(i).changeFileSuffixes("", UnifiedLog.CLEANED_FILE_SUFFIX);
            newSegments.get(i).truncateTo(0);
        }
        for (File file : logDir.listFiles()) {
            if (file.getName().endsWith(LogFileUtils.DELETED_FILE_SUFFIX)) {
                Utils.atomicMoveWithFallback(file.toPath(), Paths.get(Utils.replaceSuffix(file.getPath(), LogFileUtils.DELETED_FILE_SUFFIX, "")));
            }
        }
        UnifiedLog recoveredLog = recoverAndCheck(logConfig, expectedKeys);
        assertEquals(expectedKeys, LogTestUtils.keysInLog(recoveredLog));
        assertEquals(numSegmentsInitial + 1, recoveredLog.logSegments().size());
        recoveredLog.close();
    }

    @Test
    public void testRecoveryAfterCrashDuringSplitPhase2() throws IOException {
        LogConfig logConfig = new LogTestUtils.LogConfigBuilder()
                .indexIntervalBytes(1)
                .fileDeleteDelayMs(1000)
                .build();
        LogAndSegment logAndSegment = createLogWithOffsetOverflow(logConfig);
        List<Long> expectedKeys = LogTestUtils.keysInLog(logAndSegment.log);
        int numSegmentsInitial = logAndSegment.log.logSegments().size();

        // Split the segment
        List<LogSegment> newSegments = logAndSegment.log.splitOverflowedSegment(logAndSegment.segment);

        // Simulate recovery just after one of the new segments has been renamed to .swap. On recovery, existing split
        // operation is aborted but the recovery process itself kicks off split which should complete.
        LogSegment lastSegment = newSegments.get(newSegments.size() - 1);
        for (int i = newSegments.size() - 1; i >= 0; i--) {
            LogSegment segment = newSegments.get(i);
            if (segment != lastSegment) {
                segment.changeFileSuffixes("", UnifiedLog.CLEANED_FILE_SUFFIX);
            } else {
                segment.changeFileSuffixes("", UnifiedLog.SWAP_FILE_SUFFIX);
            }
            segment.truncateTo(0);
        }
        for (File file : logDir.listFiles()) {
            if (file.getName().endsWith(LogFileUtils.DELETED_FILE_SUFFIX)) {
                Utils.atomicMoveWithFallback(file.toPath(), Paths.get(Utils.replaceSuffix(file.getPath(), LogFileUtils.DELETED_FILE_SUFFIX, "")));
            }
        }
        UnifiedLog recoveredLog = recoverAndCheck(logConfig, expectedKeys);
        assertEquals(expectedKeys, LogTestUtils.keysInLog(recoveredLog));
        assertEquals(numSegmentsInitial + 1, recoveredLog.logSegments().size());
        recoveredLog.close();
    }

    @Test
    public void testRecoveryAfterCrashDuringSplitPhase3() throws IOException {
        LogConfig logConfig = new LogTestUtils.LogConfigBuilder()
                .indexIntervalBytes(1)
                .fileDeleteDelayMs(1000)
                .build();
        LogAndSegment logAndSegment = createLogWithOffsetOverflow(logConfig);
        List<Long> expectedKeys = LogTestUtils.keysInLog(logAndSegment.log);
        int numSegmentsInitial = logAndSegment.log.logSegments().size();

        // Split the segment
        List<LogSegment> newSegments = logAndSegment.log.splitOverflowedSegment(logAndSegment.segment);

        // Simulate recovery right after all new segments have been renamed to .swap. On recovery, existing split operation
        // is completed and the old segment must be deleted.
        for (int i = newSegments.size() - 1; i >= 0; i--) {
            newSegments.get(i).changeFileSuffixes("", UnifiedLog.SWAP_FILE_SUFFIX);
        }
        for (File file : logDir.listFiles()) {
            if (file.getName().endsWith(LogFileUtils.DELETED_FILE_SUFFIX)) {
                Utils.atomicMoveWithFallback(file.toPath(), Paths.get(Utils.replaceSuffix(file.getPath(), LogFileUtils.DELETED_FILE_SUFFIX, "")));
            }
        }
        // Truncate the old segment
        logAndSegment.segment.truncateTo(0);

        UnifiedLog recoveredLog = recoverAndCheck(logConfig, expectedKeys);
        assertEquals(expectedKeys, LogTestUtils.keysInLog(recoveredLog));
        assertEquals(numSegmentsInitial + 1, recoveredLog.logSegments().size());
        logAndSegment.log.close();
    }

    @Test
    public void testRecoveryAfterCrashDuringSplitPhase4() throws IOException {
        LogConfig logConfig = new LogTestUtils.LogConfigBuilder()
                .indexIntervalBytes(1)
                .fileDeleteDelayMs(1000)
                .build();
        LogAndSegment logAndSegment = createLogWithOffsetOverflow(logConfig);
        List<Long> expectedKeys = LogTestUtils.keysInLog(logAndSegment.log);
        int numSegmentsInitial = logAndSegment.log.logSegments().size();

        // Split the segment
        List<LogSegment> newSegments = logAndSegment.log.splitOverflowedSegment(logAndSegment.segment);

        // Simulate recovery right after all new segments have been renamed to .swap and old segment has been deleted. On
        // recovery, existing split operation is completed.
        for (int i = newSegments.size() - 1; i >= 0; i--) {
            newSegments.get(i).changeFileSuffixes("", UnifiedLog.SWAP_FILE_SUFFIX);
        }

        for (File file : logDir.listFiles()) {
            if (file.getName().endsWith(LogFileUtils.DELETED_FILE_SUFFIX)) {
                Utils.delete(file);
            }
        }

        // Truncate the old segment
        logAndSegment.segment.truncateTo(0);

        UnifiedLog recoveredLog = recoverAndCheck(logConfig, expectedKeys);
        assertEquals(expectedKeys, LogTestUtils.keysInLog(recoveredLog));
        assertEquals(numSegmentsInitial + 1, recoveredLog.logSegments().size());
        recoveredLog.close();
    }

    @Test
    public void testRecoveryAfterCrashDuringSplitPhase5() throws IOException {
        LogConfig logConfig = new LogTestUtils.LogConfigBuilder()
                .indexIntervalBytes(1)
                .fileDeleteDelayMs(1000)
                .build();
        LogAndSegment logAndSegment = createLogWithOffsetOverflow(logConfig);
        List<Long> expectedKeys = LogTestUtils.keysInLog(logAndSegment.log);
        int numSegmentsInitial = logAndSegment.log.logSegments().size();

        // Split the segment
        List<LogSegment> newSegments = logAndSegment.log.splitOverflowedSegment(logAndSegment.segment);

        // Simulate recovery right after one of the new segment has been renamed to .swap and the other to .log. On
        // recovery, existing split operation is completed.
        newSegments.get(newSegments.size() - 1).changeFileSuffixes("", UnifiedLog.SWAP_FILE_SUFFIX);

        // Truncate the old segment
        logAndSegment.segment.truncateTo(0);

        UnifiedLog recoveredLog = recoverAndCheck(logConfig, expectedKeys);
        assertEquals(expectedKeys, LogTestUtils.keysInLog(recoveredLog));
        assertEquals(numSegmentsInitial + 1, recoveredLog.logSegments().size());
        recoveredLog.close();
    }

    @Test
    public void testCleanShutdownFile() throws IOException {
        // append some messages to create some segments
        LogConfig logConfig = new LogTestUtils.LogConfigBuilder()
                .segmentBytes(1000)
                .indexIntervalBytes(1)
                .maxMessageBytes(64 * 1024)
                .build();
        Supplier<MemoryRecords> createRecords = () -> LogTestUtils.singletonRecords("test".getBytes(), mockTime.milliseconds());

        // create a log and write some messages to it
        UnifiedLog log = createLog(logDir, logConfig);
        for (int i = 0; i < 100; i++) {
            log.appendAsLeader(createRecords.get(), 0);
        }
        log.close();

        // check if recovery was attempted. Even if the recovery point is 0L, recovery should not be attempted as the
        // clean shutdown file exists. Note: Earlier, Log layer relied on the presence of clean shutdown file to determine the status
        // of last shutdown. Now, LogManager checks for the presence of this file and immediately deletes the same. It passes
        // down a clean shutdown flag to the Log layer as log is loaded. Recovery is attempted based on this flag.
        long recoveryPoint = log.logEndOffset();
        log = createLog(logDir, logConfig);
        assertEquals(recoveryPoint, log.logEndOffset());
    }

    /**
     * Append a bunch of messages to a log and then re-open it with recovery and check that the leader epochs are recovered properly.
     */
    @Test
    public void testLogRecoversForLeaderEpoch() throws IOException {
        UnifiedLog log = createLog(logDir, new LogConfig(Map.of()));
        LeaderEpochFileCache leaderEpochCache = log.leaderEpochCache();
        MemoryRecords firstBatch = singletonRecordsWithLeaderEpoch("random".getBytes(), 1, 0);
        log.appendAsFollower(firstBatch, Integer.MAX_VALUE);

        MemoryRecords secondBatch = singletonRecordsWithLeaderEpoch("random".getBytes(), 2, 1);
        log.appendAsFollower(secondBatch, Integer.MAX_VALUE);

        MemoryRecords thirdBatch = singletonRecordsWithLeaderEpoch("random".getBytes(), 2, 2);
        log.appendAsFollower(thirdBatch, Integer.MAX_VALUE);

        MemoryRecords fourthBatch = singletonRecordsWithLeaderEpoch("random".getBytes(), 3, 3);
        log.appendAsFollower(fourthBatch, Integer.MAX_VALUE);

        assertEquals(List.of(new EpochEntry(1, 0), new EpochEntry(2, 1), new EpochEntry(3, 3)), leaderEpochCache.epochEntries());

        // deliberately remove some of the epoch entries
        leaderEpochCache.truncateFromEndAsyncFlush(2);
        assertNotEquals(List.of(new EpochEntry(1, 0), new EpochEntry(2, 1), new EpochEntry(3, 3)), leaderEpochCache.epochEntries());
        log.close();

        // reopen the log and recover from the beginning
        UnifiedLog recoveredLog = createLog(logDir, new LogConfig(Map.of()), false);
        LeaderEpochFileCache recoveredLeaderEpochCache = recoveredLog.leaderEpochCache();

        // epoch entries should be recovered
        assertEquals(List.of(new EpochEntry(1, 0), new EpochEntry(2, 1), new EpochEntry(3, 3)), recoveredLeaderEpochCache.epochEntries());
        recoveredLog.close();
    }

    @Test
    public void testFullTransactionIndexRecovery() throws IOException {
        LogConfig logConfig = new LogTestUtils.LogConfigBuilder()
                .segmentBytes(128 * 5)
                .build();
        UnifiedLog log = createLog(logDir, logConfig);
        short epoch = (short) 0;

        long pid1 = 1L;
        long pid2 = 2L;
        long pid3 = 3L;
        long pid4 = 4L;

        Consumer<Integer> appendPid1 = LogTestUtils.appendTransactionalAsLeader(log, pid1, epoch, mockTime);
        Consumer<Integer> appendPid2 = LogTestUtils.appendTransactionalAsLeader(log, pid2, epoch, mockTime);
        Consumer<Integer> appendPid3 = LogTestUtils.appendTransactionalAsLeader(log, pid3, epoch, mockTime);
        Consumer<Integer> appendPid4 = LogTestUtils.appendTransactionalAsLeader(log, pid4, epoch, mockTime);

        // mix transactional and non-transactional data
        appendPid1.accept(5); // nextOffset: 5
        LogTestUtils.appendNonTransactionalAsLeader(log, 3); // 8
        appendPid2.accept(2); // 10
        appendPid1.accept(4); // 14
        appendPid3.accept(3); // 17
        LogTestUtils.appendNonTransactionalAsLeader(log, 2); // 19
        appendPid1.accept(10); // 29
        LogTestUtils.appendEndTxnMarkerAsLeader(log, pid1, epoch, ControlRecordType.ABORT, mockTime.milliseconds(), 0, 0, TransactionVersion.TV_0.featureLevel()); // 30
        appendPid2.accept(6); // 36
        appendPid4.accept(3); // 39
        LogTestUtils.appendNonTransactionalAsLeader(log, 10); // 49
        appendPid3.accept(9); // 58
        LogTestUtils.appendEndTxnMarkerAsLeader(log, pid3, epoch, ControlRecordType.COMMIT, mockTime.milliseconds(), 0, 0, TransactionVersion.TV_0.featureLevel()); // 59
        appendPid4.accept(8); // 67
        appendPid2.accept(7); // 74
        LogTestUtils.appendEndTxnMarkerAsLeader(log, pid2, epoch, ControlRecordType.ABORT, mockTime.milliseconds(), 0, 0, TransactionVersion.TV_0.featureLevel()); // 75
        LogTestUtils.appendNonTransactionalAsLeader(log, 10); // 85
        appendPid4.accept(4); // 89
        LogTestUtils.appendEndTxnMarkerAsLeader(log, pid4, epoch, ControlRecordType.COMMIT, mockTime.milliseconds(), 0, 0, TransactionVersion.TV_0.featureLevel()); // 90

        // delete all the offset and transaction index files to force recovery
        for (LogSegment segment : log.logSegments()) {
            segment.offsetIndex().deleteIfExists();
            segment.txnIndex().deleteIfExists();
        }

        log.close();

        LogConfig reloadedLogConfig = new LogTestUtils.LogConfigBuilder()
                .segmentBytes(1024 * 5)
                .build();
        UnifiedLog reloadedLog = createLog(logDir, reloadedLogConfig, false);
        List<AbortedTxn> abortedTransactions = LogTestUtils.allAbortedTransactions(reloadedLog);
        assertEquals(
                List.of(
                        new AbortedTxn().setProducerId(pid1).setFirstOffset(0L).setLastOffset(29L).setLastStableOffset(8L),
                        new AbortedTxn().setProducerId(pid2).setFirstOffset(8L).setLastOffset(74L).setLastStableOffset(36L)),
                abortedTransactions);
    }

    @Test
    public void testRecoverOnlyLastSegment() throws IOException {
        LogConfig logConfig = new LogTestUtils.LogConfigBuilder()
                .segmentBytes(128 * 5)
                .build();
        UnifiedLog log = createLog(logDir, logConfig);
        short epoch = (short) 0;

        long pid1 = 1L;
        long pid2 = 2L;
        long pid3 = 3L;
        long pid4 = 4L;

        Consumer<Integer> appendPid1 = LogTestUtils.appendTransactionalAsLeader(log, pid1, epoch, mockTime);
        Consumer<Integer> appendPid2 = LogTestUtils.appendTransactionalAsLeader(log, pid2, epoch, mockTime);
        Consumer<Integer> appendPid3 = LogTestUtils.appendTransactionalAsLeader(log, pid3, epoch, mockTime);
        Consumer<Integer> appendPid4 = LogTestUtils.appendTransactionalAsLeader(log, pid4, epoch, mockTime);

        // mix transactional and non-transactional data
        appendPid1.accept(5); // nextOffset: 5
        LogTestUtils.appendNonTransactionalAsLeader(log, 3); // 8
        appendPid2.accept(2); // 10
        appendPid1.accept(4); // 14
        appendPid3.accept(3); // 17
        LogTestUtils.appendNonTransactionalAsLeader(log, 2); // 19
        appendPid1.accept(10); // 29
        LogTestUtils.appendEndTxnMarkerAsLeader(log, pid1, epoch, ControlRecordType.ABORT, mockTime.milliseconds(), 0, 0, TransactionVersion.TV_0.featureLevel()); // 30
        appendPid2.accept(6); // 36
        appendPid4.accept(3); // 39
        LogTestUtils.appendNonTransactionalAsLeader(log, 10); // 49
        appendPid3.accept(9); // 58
        LogTestUtils.appendEndTxnMarkerAsLeader(log, pid3, epoch, ControlRecordType.COMMIT, mockTime.milliseconds(), 0, 0, TransactionVersion.TV_0.featureLevel()); // 59
        appendPid4.accept(8); // 67
        appendPid2.accept(7); // 74
        LogTestUtils.appendEndTxnMarkerAsLeader(log, pid2, epoch, ControlRecordType.ABORT, mockTime.milliseconds(), 0, 0, TransactionVersion.TV_0.featureLevel()); // 75
        LogTestUtils.appendNonTransactionalAsLeader(log, 10); // 85
        appendPid4.accept(4); // 89
        LogTestUtils.appendEndTxnMarkerAsLeader(log, pid4, epoch, ControlRecordType.COMMIT, mockTime.milliseconds(), 0, 0, TransactionVersion.TV_0.featureLevel()); // 90

        // delete the last offset and transaction index files to force recovery
        LogSegment lastSegment = log.logSegments().get(log.logSegments().size() - 1);
        long recoveryPoint = lastSegment.baseOffset();
        lastSegment.offsetIndex().deleteIfExists();
        lastSegment.txnIndex().deleteIfExists();

        log.close();

        LogConfig reloadedLogConfig = new LogTestUtils.LogConfigBuilder()
                .segmentBytes(1024 * 5)
                .build();
        UnifiedLog reloadedLog = createLog(logDir, reloadedLogConfig, recoveryPoint, false);
        List<AbortedTxn> abortedTransactions = LogTestUtils.allAbortedTransactions(reloadedLog);
        assertEquals(
                List.of(
                    new AbortedTxn().setProducerId(pid1).setFirstOffset(0L).setLastOffset(29L).setLastStableOffset(8L),
                    new AbortedTxn().setProducerId(pid2).setFirstOffset(8L).setLastOffset(74L).setLastStableOffset(36L)),
                abortedTransactions);
    }

    @Test
    public void testRecoverLastSegmentWithNoSnapshots() throws IOException {
        LogConfig logConfig = new LogTestUtils.LogConfigBuilder()
                .segmentBytes(128 * 5)
                .build();
        UnifiedLog log = createLog(logDir, logConfig);
        short epoch = (short) 0;

        long pid1 = 1L;
        long pid2 = 2L;
        long pid3 = 3L;
        long pid4 = 4L;

        Consumer<Integer> appendPid1 = LogTestUtils.appendTransactionalAsLeader(log, pid1, epoch, mockTime);
        Consumer<Integer> appendPid2 = LogTestUtils.appendTransactionalAsLeader(log, pid2, epoch, mockTime);
        Consumer<Integer> appendPid3 = LogTestUtils.appendTransactionalAsLeader(log, pid3, epoch, mockTime);
        Consumer<Integer> appendPid4 = LogTestUtils.appendTransactionalAsLeader(log, pid4, epoch, mockTime);

        // mix transactional and non-transactional data
        appendPid1.accept(5); // nextOffset: 5
        LogTestUtils.appendNonTransactionalAsLeader(log, 3); // 8
        appendPid2.accept(2); // 10
        appendPid1.accept(4); // 14
        appendPid3.accept(3); // 17
        LogTestUtils.appendNonTransactionalAsLeader(log, 2); // 19
        appendPid1.accept(10); // 29
        LogTestUtils.appendEndTxnMarkerAsLeader(log, pid1, epoch, ControlRecordType.ABORT, mockTime.milliseconds(), 0, 0, TransactionVersion.TV_0.featureLevel()); // 30
        appendPid2.accept(6); // 36
        appendPid4.accept(3); // 39
        LogTestUtils.appendNonTransactionalAsLeader(log, 10); // 49
        appendPid3.accept(9); // 58
        LogTestUtils.appendEndTxnMarkerAsLeader(log, pid3, epoch, ControlRecordType.COMMIT, mockTime.milliseconds(), 0, 0, TransactionVersion.TV_0.featureLevel()); // 59
        appendPid4.accept(8); // 67
        appendPid2.accept(7); // 74
        LogTestUtils.appendEndTxnMarkerAsLeader(log, pid2, epoch, ControlRecordType.ABORT, mockTime.milliseconds(), 0, 0, TransactionVersion.TV_0.featureLevel()); // 75
        LogTestUtils.appendNonTransactionalAsLeader(log, 10); // 85
        appendPid4.accept(4); // 89
        LogTestUtils.appendEndTxnMarkerAsLeader(log, pid4, epoch, ControlRecordType.COMMIT, mockTime.milliseconds(), 0, 0, TransactionVersion.TV_0.featureLevel()); // 90

        LogTestUtils.deleteProducerSnapshotFiles(logDir);

        // delete the last offset and transaction index files to force recovery. this should force us to rebuild
        // the producer state from the start of the log
        LogSegment lastSegment = log.logSegments().get(log.logSegments().size() - 1);
        long recoveryPoint = lastSegment.baseOffset();
        lastSegment.offsetIndex().deleteIfExists();
        lastSegment.txnIndex().deleteIfExists();

        log.close();

        LogConfig reloadedLogConfig = new LogTestUtils.LogConfigBuilder()
                .segmentBytes(1024 * 5)
                .build();
        UnifiedLog reloadedLog = createLog(logDir, reloadedLogConfig, recoveryPoint, false);
        List<AbortedTxn> abortedTransactions = LogTestUtils.allAbortedTransactions(reloadedLog);
        assertEquals(
                List.of(
                        new AbortedTxn().setProducerId(pid1).setFirstOffset(0L).setLastOffset(29L).setLastStableOffset(8L),
                        new AbortedTxn().setProducerId(pid2).setFirstOffset(8L).setLastOffset(74L).setLastStableOffset(36L)),
                abortedTransactions);
    }

    @Test
    public void testLogEndLessThanStartAfterReopen() throws IOException {
        LogConfig logConfig = new LogTestUtils.LogConfigBuilder().build();
        UnifiedLog log = createLog(logDir, logConfig);
        for (int i = 0; i < 5; i++) {
            SimpleRecord record = new SimpleRecord(mockTime.milliseconds(), String.valueOf(i).getBytes());
            log.appendAsLeader(LogTestUtils.records(List.of(record)), 0);
            log.roll();
        }
        assertEquals(6, log.logSegments().size());

        // Increment the log start offset
        int startOffset = 4;
        log.updateHighWatermark(log.logEndOffset());
        log.maybeIncrementLogStartOffset(startOffset, LogStartOffsetIncrementReason.ClientRecordDeletion);
        assertTrue(log.logEndOffset() > log.logStartOffset());

        // Append garbage to a segment below the current log start offset
        LogSegment segmentToForceTruncation = log.logSegments().get(1);
        BufferedWriter bw = new BufferedWriter(new FileWriter(segmentToForceTruncation.log().file()));
        bw.write("corruptRecord");
        bw.close();
        log.close();

        // Reopen the log. This will cause truncate the segment to which we appended garbage and delete all other segments.
        // All remaining segments will be lower than the current log start offset, which will force deletion of all segments
        // and recreation of a single, active segment starting at logStartOffset.
        log = createLog(logDir, logConfig, startOffset, 0);
        // Wait for segment deletions (if any) to complete.
        mockTime.sleep(logConfig.fileDeleteDelayMs);
        assertEquals(1, log.numberOfSegments());
        assertEquals(startOffset, log.logStartOffset());
        assertEquals(startOffset, log.logEndOffset());
        // Validate that the remaining segment matches our expectations
        LogSegment onlySegment = log.logSegments().get(0);
        assertEquals(startOffset, onlySegment.baseOffset());
        assertTrue(onlySegment.log().file().exists());
        assertTrue(onlySegment.offsetIndexFile().exists());
        assertTrue(onlySegment.timeIndexFile().exists());
    }

    @Test
    public void testCorruptedLogRecoveryDoesNotDeleteProducerStateSnapshotsPostRecovery() throws IOException {
        LogConfig logConfig = new LogTestUtils.LogConfigBuilder().build();
        UnifiedLog log = createLog(logDir, logConfig);
        // Create segments: [0-0], [1-1], [2-2], [3-3], [4-4], [5-5], [6-6], [7-7], [8-8], [9-]
        //                   |---> logStartOffset                                           |---> active segment (empty)
        //                                                                                  |---> logEndOffset
        for (int i = 0; i < 9; i++) {
            SimpleRecord record = new SimpleRecord(mockTime.milliseconds(), String.valueOf(i).getBytes());
            log.appendAsLeader(LogTestUtils.records(List.of(record)), 0);
            log.roll();
        }
        assertEquals(10, log.logSegments().size());
        assertEquals(0, log.logStartOffset());
        assertEquals(9, log.activeSegment().baseOffset());
        assertEquals(9, log.logEndOffset());
        for (int offset = 1; offset < 10; offset++) {
            Optional<SnapshotFile> snapshotFileBeforeDeletion = log.producerStateManager().snapshotFileForOffset(offset);
            assertTrue(snapshotFileBeforeDeletion.isPresent());
            assertTrue(snapshotFileBeforeDeletion.get().file().exists());
        }

        // Increment the log start offset to 4.
        // After this step, the segments should be:
        //                              |---> logStartOffset
        // [0-0], [1-1], [2-2], [3-3], [4-4], [5-5], [6-6], [7-7], [8-8], [9-]
        //                                                                 |---> active segment (empty)
        //                                                                 |---> logEndOffset
        int newLogStartOffset = 4;
        log.updateHighWatermark(log.logEndOffset());
        log.maybeIncrementLogStartOffset(newLogStartOffset, LogStartOffsetIncrementReason.ClientRecordDeletion);
        assertEquals(4, log.logStartOffset());
        assertEquals(9, log.logEndOffset());

        // Append garbage to a segment at baseOffset 1, which is below the current log start offset 4.
        // After this step, the segments should be:
        //
        // [0-0], [1-1], [2-2], [3-3], [4-4], [5-5], [6-6], [7-7], [8-8], [9-]
        //           |                  |---> logStartOffset               |---> active segment  (empty)
        //           |                                                     |---> logEndOffset
        // corrupt record inserted
        //
        LogSegment segmentToForceTruncation = log.logSegments().get(1);
        assertEquals(1, segmentToForceTruncation.baseOffset());
        BufferedWriter bw = new BufferedWriter(new FileWriter(segmentToForceTruncation.log().file()));
        bw.write("corruptRecord");
        bw.close();
        log.close();

        // Reopen the log. This will do the following:
        // - Truncate the segment above to which we appended garbage and will schedule async deletion of all other
        //   segments from base offsets 2 to 9.
        // - The remaining segments at base offsets 0 and 1 will be lower than the current logStartOffset 4.
        //   This will cause async deletion of both remaining segments. Finally, a single active segment is created
        //   starting at logStartOffset 4.
        //
        // Expected segments after the log is opened again:
        // [4-]
        //  |---> active segment (empty)
        //  |---> logStartOffset
        //  |---> logEndOffset
        log = createLog(logDir, logConfig, newLogStartOffset, 0);
        assertEquals(1, log.logSegments().size());
        assertEquals(4, log.logStartOffset());
        assertEquals(4, log.activeSegment().baseOffset());
        assertEquals(4, log.logEndOffset());

        List<Long> offsetsWithSnapshotFiles = IntStream.range(1, 5)
                .mapToObj(offset -> new SnapshotFile(LogFileUtils.producerSnapshotFile(logDir, offset)))
                .filter(snapshotFile -> snapshotFile.file().exists())
                .map(snapshotFile -> snapshotFile.offset)
                .toList();
        UnifiedLog reopenedLog = log;
        List<SnapshotFile> inMemorySnapshotFiles = IntStream.range(1, 5)
                .mapToObj(offset -> reopenedLog.producerStateManager().snapshotFileForOffset(offset))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .toList();

        assertTrue(offsetsWithSnapshotFiles.isEmpty(), "Found offsets with producer state snapshot files: " + offsetsWithSnapshotFiles + " while none were expected.");
        assertTrue(inMemorySnapshotFiles.isEmpty(), "Found in-memory producer state snapshot files: " + inMemorySnapshotFiles + " while none were expected.");

        // Append records, roll the segments and check that the producer state snapshots are defined.
        // The expected segments and producer state snapshots, after the appends are complete and segments are rolled,
        // is as shown below:
        // [4-4], [5-5], [6-6], [7-7], [8-8], [9-]
        //  |      |      |      |      |      |---> active segment (empty)
        //  |      |      |      |      |      |---> logEndOffset
        //  |      |      |      |      |      |
        //  |      |------.------.------.------.-----> producer state snapshot files are DEFINED for each offset in: [5-9]
        //  |----------------------------------------> logStartOffset
        for (int i = 0; i < 5; i++) {
            SimpleRecord record = new SimpleRecord(mockTime.milliseconds(), String.valueOf(i).getBytes());
            log.appendAsLeader(LogTestUtils.records(List.of(record)), 0);
            log.roll();
        }
        assertEquals(9, log.activeSegment().baseOffset());
        assertEquals(9, log.logEndOffset());
        for (int offset = 5; offset < 10; offset++) {
            Optional<SnapshotFile> snapshotFileBeforeDeletion = log.producerStateManager().snapshotFileForOffset(offset);
            assertTrue(snapshotFileBeforeDeletion.isPresent());
            assertTrue(snapshotFileBeforeDeletion.get().file().exists());
        }

        // Wait for all async segment deletions scheduled during Log recovery to complete.
        // The expected segments and producer state snapshot after the deletions, is as shown below:
        // [4-4], [5-5], [6-6], [7-7], [8-8], [9-]
        //  |      |      |      |      |      |---> active segment (empty)
        //  |      |      |      |      |      |---> logEndOffset
        //  |      |      |      |      |      |
        //  |      |------.------.------.------.-----> producer state snapshot files should be defined for each offset in: [5-9].
        //  |----------------------------------------> logStartOffset
        mockTime.sleep(logConfig.fileDeleteDelayMs);
        assertEquals(newLogStartOffset, log.logStartOffset());
        assertEquals(9, log.logEndOffset());
        List<Long> offsetsWithMissingSnapshotFiles = new ArrayList<>();
        for (long offset = 5; offset < 10; offset++) {
            Optional<SnapshotFile> snapshotFile = log.producerStateManager().snapshotFileForOffset(offset);
            if (snapshotFile.isEmpty() || !snapshotFile.get().file().exists()) {
                offsetsWithMissingSnapshotFiles.add(offset);
            }
        }
        assertTrue(offsetsWithMissingSnapshotFiles.isEmpty(),
                "Found offsets with missing producer state snapshot files: " + offsetsWithMissingSnapshotFiles);
        assertFalse(Stream.of(logDir.list()).anyMatch(d -> d.endsWith(LogFileUtils.DELETED_FILE_SUFFIX)), "Expected no files to be present with the deleted file suffix");
    }

    @Test
    public void testRecoverWithEmptyActiveSegment() throws IOException {
        int numMessages = 100;
        int messageSize = 100;
        int segmentSize = 7 * messageSize;
        int indexInterval = 3 * messageSize;
        LogConfig logConfig = new LogTestUtils.LogConfigBuilder()
                .segmentBytes(segmentSize)
                .indexIntervalBytes(indexInterval)
                .segmentIndexBytes(4096)
                .build();
        UnifiedLog log = createLog(logDir, logConfig);
        for (int i = 0; i < numMessages; i++) {
            log.appendAsLeader(LogTestUtils.singletonRecords(TestUtils.randomBytes(messageSize), mockTime.milliseconds() + i * 10), 0);
        }
        assertEquals(numMessages, log.logEndOffset(),
                "After appending " + numMessages + " messages to an empty log, the log end offset should be " + numMessages);
        log.roll();
        log.flush(false);
        assertThrows(NoSuchFileException.class, () -> log.activeSegment().sanityCheck(true));
        long lastOffset = log.logEndOffset();
        log.closeHandlers();

        UnifiedLog log2 = createLog(logDir, logConfig, lastOffset, false);
        assertEquals(lastOffset, log2.recoveryPoint(), "Unexpected recovery point");
        assertEquals(numMessages, log2.logEndOffset(), "Should have " + numMessages + " messages when log is reopened w/o recovery");
        assertEquals(0, log2.activeSegment().timeIndex().entries(), "Should have same number of time index entries as before.");
        log2.activeSegment().sanityCheck(true); // this should not throw because the LogLoader created the empty active log index file during recovery

        for (int i = 0; i < numMessages; i++) {
            log2.appendAsLeader(LogTestUtils.singletonRecords(TestUtils.randomBytes(messageSize), mockTime.milliseconds() + i * 10), 0);
        }
        log2.roll();
        assertThrows(NoSuchFileException.class, () -> log2.activeSegment().sanityCheck(true));
        log2.flush(true);
        log2.activeSegment().sanityCheck(true); // this should not throw because we flushed the active segment which created the empty log index file
        lastOffset = log2.logEndOffset();

        UnifiedLog log3 = createLog(logDir, logConfig, lastOffset, false);
        assertEquals(lastOffset, log3.recoveryPoint(), "Unexpected recovery point");
        assertEquals(2 * numMessages, log3.logEndOffset(), "Should have " + numMessages + " messages when log is reopened w/o recovery");
        assertEquals(0, log3.activeSegment().timeIndex().entries(), "Should have same number of time index entries as before.");
        log3.activeSegment().sanityCheck(true); // this should not throw

        log3.close();
    }

    @ParameterizedTest
    @CsvSource({"false, 5", "true, 0"})
    public void testLogStartOffsetWhenRemoteStorageIsEnabled(boolean isRemoteLogEnabled, long expectedLogStartOffset) throws IOException {
        TopicPartition topicPartition = UnifiedLog.parseTopicPartitionName(logDir);
        LogConfig logConfig = new LogTestUtils.LogConfigBuilder().build();
        ProducerStateManager stateManager = mock(ProducerStateManager.class);
        when(stateManager.isEmpty()).thenReturn(true);

        UnifiedLog log = createLog(logDir, logConfig);
        // Create segments: [0-0], [1-1], [2-2], [3-3], [4-4], [5-5], [6-6], [7-7], [8-8], [9-]
        //                   |---> logStartOffset                                           |---> active segment (empty)
        //                                                                                  |---> logEndOffset
        for (int i = 0; i < 9; i++) {
            SimpleRecord record = new SimpleRecord(mockTime.milliseconds(), String.valueOf(i).getBytes());
            log.appendAsLeader(LogTestUtils.records(List.of(record)), 0);
            log.roll();
        }
        log.maybeIncrementHighWatermark(new LogOffsetMetadata(9L));
        log.maybeIncrementLogStartOffset(5L, LogStartOffsetIncrementReason.SegmentDeletion);
        log.deleteOldSegments();

        LogSegments segments = new LogSegments(topicPartition);
        log.logSegments().forEach(segments::add);
        assertEquals(5, segments.firstSegment().get().baseOffset());

        LeaderEpochFileCache leaderEpochCache = UnifiedLog.createLeaderEpochCache(
                logDir, topicPartition, null, Optional.empty(), mockTime.scheduler);
        LoadedLogOffsets offsets = new LogLoader(
                logDir,
                topicPartition,
                logConfig,
                mockTime.scheduler,
                mockTime,
                null,
                true,
                segments,
                0L,
                0L,
                leaderEpochCache,
                stateManager,
                new ConcurrentHashMap<>(),
                isRemoteLogEnabled
        ).load();
        assertEquals(expectedLogStartOffset, offsets.logStartOffset());
    }

}
