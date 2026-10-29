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
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.record.internal.ControlRecordType;
import org.apache.kafka.common.record.internal.EndTransactionMarker;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.transaction.TransactionLogConfig;
import org.apache.kafka.server.common.RequestLocal;
import org.apache.kafka.server.common.TransactionVersion;
import org.apache.kafka.server.util.MockTime;
import org.apache.kafka.storage.internals.epoch.LeaderEpochFileCache;
import org.apache.kafka.storage.internals.log.LogCleanerManager.OffsetsToClean;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.kafka.coordinator.transaction.TransactionLogConfig.PRODUCER_ID_EXPIRATION_CHECK_INTERVAL_MS_DEFAULT;
import static org.apache.kafka.storage.internals.log.LogCleaningState.LOG_CLEANING_ABORTED;
import static org.apache.kafka.storage.internals.log.LogCleaningState.LOG_CLEANING_IN_PROGRESS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for the log cleaning logic.
 */
class LogCleanerManagerTest {
    private static final TopicPartition TOPIC_PARTITION = new TopicPartition("log", 0);
    private static final TopicPartition TOPIC_PARTITION_2 = new TopicPartition("log2", 0);
    private static final LogConfig LOG_CONFIG = createLogConfig();
    private static final MockTime TIME = new MockTime(1400000000000L, 1000L);  // Tue May 13 16:53:20 UTC 2014 for `currentTimeMs`
    private static final long OFFSET = 999;
    private static final ProducerStateManagerConfig PRODUCER_STATE_MANAGER_CONFIG =
        new ProducerStateManagerConfig(TransactionLogConfig.PRODUCER_ID_EXPIRATION_MS_DEFAULT, false);

    private File tmpDir;
    private File tmpDir2;
    private File logDir;
    private File logDir2;

    static class LogCleanerManagerMock extends LogCleanerManager {
        private final Map<TopicPartition, Long> cleanerCheckpoints = new HashMap<>();

        LogCleanerManagerMock(
            List<File> logDirs,
            ConcurrentMap<TopicPartition, UnifiedLog> logs,
            LogDirFailureChannel logDirFailureChannel
        ) {
            super(logDirs, logs, logDirFailureChannel);
        }

        @Override
        public Map<TopicPartition, Long> allCleanerCheckpoints() {
            return cleanerCheckpoints;
        }

        @Override
        public void updateCheckpoints(
            File dataDir,
            Optional<Map.Entry<TopicPartition, Long>> partitionToUpdateOrAdd,
            Optional<TopicPartition> partitionToRemove
        ) {
            assert partitionToRemove.isEmpty() : "partitionToRemove argument with value not yet handled";

            Map.Entry<TopicPartition, Long> entry = partitionToUpdateOrAdd.orElseThrow(() ->
                new IllegalArgumentException("Empty 'partitionToUpdateOrAdd' argument not yet handled"));

            addCheckpoint(entry.getKey(), entry.getValue());
        }

        void addCheckpoint(TopicPartition partition, long offset) {
            cleanerCheckpoints.put(partition, offset);
        }

        long checkpointOffset(TopicPartition partition) {
            return cleanerCheckpoints.get(partition);
        }
    }

    // the exception should be caught and the partition that caused it marked as uncleanable
    static class LogMock extends UnifiedLog {

        LogMock(
            long logStartOffset,
            LocalLog localLog,
            BrokerTopicStats brokerTopicStats,
            int producerIdExpirationCheckIntervalMs,
            LeaderEpochFileCache leaderEpochCache,
            ProducerStateManager producerStateManager,
            Optional<Uuid> topicId,
            boolean remoteStorageSystemEnable,
            LogOffsetsListener logOffsetsListener
        ) throws IOException {
            super(logStartOffset, localLog, brokerTopicStats, producerIdExpirationCheckIntervalMs, leaderEpochCache,
                producerStateManager, topicId, remoteStorageSystemEnable, logOffsetsListener);
        }

        // Throw an error in getFirstBatchTimestampForSegments since it is called in grabFilthiestLog()
        @Override
        public Collection<Long> getFirstBatchTimestampForSegments(Collection<LogSegment> segments) {
            throw new IllegalStateException("Error!");
        }
    }

    @BeforeEach
    public void setup() {
        tmpDir = TestUtils.tempDirectory();
        tmpDir2 = TestUtils.tempDirectory();
        logDir = TestUtils.randomPartitionLogDir(tmpDir);
        logDir2 = TestUtils.randomPartitionLogDir(tmpDir2);
    }

    @AfterEach
    public void tearDown() throws IOException {
        Utils.delete(tmpDir);
        Utils.delete(tmpDir2);
    }

    private ConcurrentMap<TopicPartition, UnifiedLog> setupIncreasinglyFilthyLogs(List<TopicPartition> partitions) throws IOException {
        ConcurrentMap<TopicPartition, UnifiedLog> logs = new ConcurrentHashMap<>();
        int numBatches = 20;

        for (TopicPartition tp : partitions) {
            UnifiedLog log = createLog(2048, TopicConfig.CLEANUP_POLICY_COMPACT, tp);
            logs.put(tp, log);

            writeRecords(log, numBatches, 1, 5);
            numBatches += 5;
        }

        return logs;
    }

    @Test
    public void testGrabFilthiestCompactedLogThrowsException() throws IOException {
        TopicPartition tp = new TopicPartition("A", 1);
        int logSegmentSize = LogTestUtils.singletonRecords("test".getBytes(), null).sizeInBytes() * 10;
        int logSegmentsCount = 2;
        File tpDir = new File(logDir, "A-1");
        Files.createDirectories(tpDir.toPath());

        LogDirFailureChannel logDirFailureChannel = new LogDirFailureChannel(10);
        LogConfig config = createLowRetentionLogConfig(logSegmentSize, TopicConfig.CLEANUP_POLICY_COMPACT);
        LogSegments segments = new LogSegments(tp);
        LeaderEpochFileCache leaderEpochCache = UnifiedLog.createLeaderEpochCache(tpDir, TOPIC_PARTITION, logDirFailureChannel,
            Optional.empty(), TIME.scheduler);
        ProducerStateManager producerStateManager = new ProducerStateManager(TOPIC_PARTITION, tpDir, 5 * 60 * 1000,
            PRODUCER_STATE_MANAGER_CONFIG, TIME);
        LoadedLogOffsets offsets = new LogLoader(tpDir, tp, config, TIME.scheduler, TIME, logDirFailureChannel, true,
            segments, 0L, 0L, leaderEpochCache, producerStateManager, new ConcurrentHashMap<>(), false).load();
        LocalLog localLog = new LocalLog(tpDir, config, segments, offsets.recoveryPoint(), offsets.nextOffsetMetadata(),
            TIME.scheduler, TIME, tp, logDirFailureChannel);
        UnifiedLog log = new LogMock(offsets.logStartOffset(), localLog, new BrokerTopicStats(), PRODUCER_ID_EXPIRATION_CHECK_INTERVAL_MS_DEFAULT,
            leaderEpochCache, producerStateManager, Optional.empty(), false, LogOffsetsListener.NO_OP_OFFSETS_LISTENER);

        writeRecords(log, logSegmentsCount * 2, 10, 2);

        ConcurrentMap<TopicPartition, UnifiedLog> logsPool = new ConcurrentHashMap<>();
        logsPool.put(tp, log);
        LogCleanerManagerMock cleanerManager = createCleanerManagerMock(logsPool);
        cleanerManager.addCheckpoint(tp, 1L);

        LogCleaningException thrownException = assertThrows(LogCleaningException.class,
            () -> cleanerManager.grabFilthiestCompactedLog(TIME, new PreCleanStats()).get());

        assertEquals(log, thrownException.log);
        assertInstanceOf(IllegalStateException.class, thrownException.getCause());
    }

    @Test
    public void testGrabFilthiestCompactedLogReturnsLogWithDirtiestRatio() throws IOException {
        TopicPartition tp0 = new TopicPartition("wishing-well", 0);
        TopicPartition tp1 = new TopicPartition("wishing-well", 1);
        TopicPartition tp2 = new TopicPartition("wishing-well", 2);
        List<TopicPartition> partitions = List.of(tp0, tp1, tp2);

        // setup logs with cleanable range: [20, 20], [20, 25], [20, 30]
        ConcurrentMap<TopicPartition, UnifiedLog> logs = setupIncreasinglyFilthyLogs(partitions);
        LogCleanerManagerMock cleanerManager = createCleanerManagerMock(logs);
        partitions.forEach(partition -> cleanerManager.addCheckpoint(partition, 20L));

        LogToClean filthiestLog = cleanerManager.grabFilthiestCompactedLog(TIME, new PreCleanStats()).get();
        assertEquals(tp2, filthiestLog.topicPartition());
        assertEquals(tp2, filthiestLog.log().topicPartition());
    }

    @Test
    public void testGrabFilthiestCompactedLogIgnoresUncleanablePartitions() throws IOException {
        TopicPartition tp0 = new TopicPartition("wishing-well", 0);
        TopicPartition tp1 = new TopicPartition("wishing-well", 1);
        TopicPartition tp2 = new TopicPartition("wishing-well", 2);
        List<TopicPartition> partitions = List.of(tp0, tp1, tp2);

        // setup logs with cleanable range: [20, 20], [20, 25], [20, 30]
        ConcurrentMap<TopicPartition, UnifiedLog> logs = setupIncreasinglyFilthyLogs(partitions);
        LogCleanerManagerMock cleanerManager = createCleanerManagerMock(logs);
        partitions.forEach(partition -> cleanerManager.addCheckpoint(partition, 20L));

        cleanerManager.markPartitionUncleanable(logs.get(tp2).dir().getParent(), tp2);

        LogToClean filthiestLog = cleanerManager.grabFilthiestCompactedLog(TIME, new PreCleanStats()).get();
        assertEquals(tp1, filthiestLog.topicPartition());
        assertEquals(tp1, filthiestLog.log().topicPartition());
    }

    @Test
    public void testGrabFilthiestCompactedLogIgnoresInProgressPartitions() throws IOException {
        TopicPartition tp0 = new TopicPartition("wishing-well", 0);
        TopicPartition tp1 = new TopicPartition("wishing-well", 1);
        TopicPartition tp2 = new TopicPartition("wishing-well", 2);
        List<TopicPartition> partitions = List.of(tp0, tp1, tp2);

        // setup logs with cleanable range: [20, 20], [20, 25], [20, 30]
        ConcurrentMap<TopicPartition, UnifiedLog> logs = setupIncreasinglyFilthyLogs(partitions);
        LogCleanerManagerMock cleanerManager = createCleanerManagerMock(logs);
        partitions.forEach(partition -> cleanerManager.addCheckpoint(partition, 20L));

        cleanerManager.setCleaningState(tp2, LOG_CLEANING_IN_PROGRESS);

        LogToClean filthiestLog = cleanerManager.grabFilthiestCompactedLog(TIME, new PreCleanStats()).get();
        assertEquals(tp1, filthiestLog.topicPartition());
        assertEquals(tp1, filthiestLog.log().topicPartition());
    }

    @Test
    public void testGrabFilthiestCompactedLogIgnoresBothInProgressPartitionsAndUncleanablePartitions() throws IOException {
        TopicPartition tp0 = new TopicPartition("wishing-well", 0);
        TopicPartition tp1 = new TopicPartition("wishing-well", 1);
        TopicPartition tp2 = new TopicPartition("wishing-well", 2);
        List<TopicPartition> partitions = List.of(tp0, tp1, tp2);

        // setup logs with cleanable range: [20, 20], [20, 25], [20, 30]
        ConcurrentMap<TopicPartition, UnifiedLog> logs = setupIncreasinglyFilthyLogs(partitions);
        LogCleanerManagerMock cleanerManager = createCleanerManagerMock(logs);
        partitions.forEach(partition -> cleanerManager.addCheckpoint(partition, 20L));

        cleanerManager.setCleaningState(tp2, LOG_CLEANING_IN_PROGRESS);
        cleanerManager.markPartitionUncleanable(logs.get(tp1).dir().getParent(), tp1);

        Optional<LogToClean> filthiestLog = cleanerManager.grabFilthiestCompactedLog(TIME, new PreCleanStats());
        assertEquals(Optional.empty(), filthiestLog);
    }

    @Test
    public void testDirtyOffsetResetIfLargerThanEndOffset() throws IOException {
        TopicPartition tp = new TopicPartition("foo", 0);
        ConcurrentMap<TopicPartition, UnifiedLog> logs = setupIncreasinglyFilthyLogs(List.of(tp));
        LogCleanerManagerMock cleanerManager = createCleanerManagerMock(logs);
        cleanerManager.addCheckpoint(tp, 200L);

        LogToClean filthiestLog = cleanerManager.grabFilthiestCompactedLog(TIME, new PreCleanStats()).get();
        assertEquals(0L, filthiestLog.firstDirtyOffset());
    }

    @Test
    public void testDirtyOffsetResetIfSmallerThanStartOffset() throws IOException {
        TopicPartition tp = new TopicPartition("foo", 0);
        ConcurrentMap<TopicPartition, UnifiedLog> logs = setupIncreasinglyFilthyLogs(List.of(tp));

        logs.get(tp).maybeIncrementLogStartOffset(10L, LogStartOffsetIncrementReason.ClientRecordDeletion);

        LogCleanerManagerMock cleanerManager = createCleanerManagerMock(logs);
        cleanerManager.addCheckpoint(tp, 0L);

        LogToClean filthiestLog = cleanerManager.grabFilthiestCompactedLog(TIME, new PreCleanStats()).get();
        assertEquals(10L, filthiestLog.firstDirtyOffset());
    }

    @Test
    public void testLogStartOffsetLargerThanActiveSegmentBaseOffset() throws IOException {
        TopicPartition tp = new TopicPartition("foo", 0);
        UnifiedLog log = createLog(2048, TopicConfig.CLEANUP_POLICY_COMPACT, tp);

        ConcurrentMap<TopicPartition, UnifiedLog> logs = new ConcurrentHashMap<>();
        logs.put(tp, log);

        appendRecords(log, 3);
        appendRecords(log, 3);
        appendRecords(log, 3);

        assertEquals(1, log.logSegments().size());

        log.maybeIncrementLogStartOffset(2L, LogStartOffsetIncrementReason.ClientRecordDeletion);

        LogCleanerManagerMock cleanerManager = createCleanerManagerMock(logs);
        cleanerManager.addCheckpoint(tp, 0L);

        // The active segment is uncleanable and hence not filthy from the POV of the CleanerManager.
        Optional<LogToClean> filthiestLog = cleanerManager.grabFilthiestCompactedLog(TIME, new PreCleanStats());
        assertEquals(Optional.empty(), filthiestLog);
    }

    @Test
    public void testDirtyOffsetLargerThanActiveSegmentBaseOffset() throws IOException {
        // It is possible in the case of an unclean leader election for the checkpoint
        // dirty offset to get ahead of the active segment base offset, but still be
        // within the range of the log.

        TopicPartition tp = new TopicPartition("foo", 0);

        ConcurrentMap<TopicPartition, UnifiedLog> logs = new ConcurrentHashMap<>();
        UnifiedLog log = createLog(2048, TopicConfig.CLEANUP_POLICY_COMPACT, tp);
        logs.put(tp, log);

        appendRecords(log, 3);
        appendRecords(log, 3);

        assertEquals(1, log.logSegments().size());
        assertEquals(0L, log.activeSegment().baseOffset());

        LogCleanerManagerMock cleanerManager = createCleanerManagerMock(logs);
        cleanerManager.addCheckpoint(tp, 3L);

        // These segments are uncleanable and hence not filthy
        Optional<LogToClean> filthiestLog = cleanerManager.grabFilthiestCompactedLog(TIME, new PreCleanStats());
        assertEquals(Optional.empty(), filthiestLog);
    }

    /**
     * When checking for logs with segments ready for deletion
     * we shouldn't consider logs where cleanup.policy=delete
     * as they are handled by the LogManager
     */
    @Test
    public void testLogsWithSegmentsToDeleteShouldNotConsiderCleanupPolicyDeleteLogs() throws IOException {
        MemoryRecords records = LogTestUtils.singletonRecords("test".getBytes(), null);
        UnifiedLog log = createLog(records.sizeInBytes() * 5, TopicConfig.CLEANUP_POLICY_DELETE, new TopicPartition("log", 0));
        LogCleanerManager cleanerManager = createCleanerManager(log);

        int readyToDelete = cleanerManager.deletableLogs().size();
        assertEquals(0, readyToDelete, "should have 0 logs ready to be deleted");
    }

    /**
     * We should find logs with segments ready to be deleted when cleanup.policy=compact,delete
     */
    @Test
    public void testLogsWithSegmentsToDeleteShouldConsiderCleanupPolicyCompactDeleteLogs() throws IOException {
        MemoryRecords records = LogTestUtils.singletonRecords("test".getBytes(), "test".getBytes());
        UnifiedLog log = createLog(records.sizeInBytes() * 5, TopicConfig.CLEANUP_POLICY_COMPACT + "," +
            TopicConfig.CLEANUP_POLICY_DELETE, new TopicPartition("log", 0));
        LogCleanerManager cleanerManager = createCleanerManager(log);

        int readyToDelete = cleanerManager.deletableLogs().size();
        assertEquals(1, readyToDelete, "should have 1 logs ready to be deleted");
    }

    /**
     * When looking for logs with segments ready to be deleted we should consider
     * logs with cleanup.policy=compact because they may have segments from before the log start offset
     */
    @Test
    public void testLogsWithSegmentsToDeleteShouldConsiderCleanupPolicyCompactLogs() throws IOException {
        MemoryRecords records = LogTestUtils.singletonRecords("test".getBytes(), "test".getBytes());
        UnifiedLog log = createLog(records.sizeInBytes() * 5, TopicConfig.CLEANUP_POLICY_COMPACT, new TopicPartition("log", 0));
        LogCleanerManager cleanerManager = createCleanerManager(log);

        int readyToDelete = cleanerManager.deletableLogs().size();
        assertEquals(1, readyToDelete, "should have 1 logs ready to be deleted");
    }

    /**
     * log under cleanup should be ineligible for compaction
     */
    @Test
    public void testLogsUnderCleanupIneligibleForCompaction() throws IOException {
        MemoryRecords records = LogTestUtils.singletonRecords("test".getBytes(), "test".getBytes());
        UnifiedLog log = createLog(records.sizeInBytes() * 5, TopicConfig.CLEANUP_POLICY_DELETE, new TopicPartition("log", 0));
        LogCleanerManager cleanerManager = createCleanerManager(log);

        log.appendAsLeader(records, 0);
        log.roll();
        log.appendAsLeader(LogTestUtils.singletonRecords("test2".getBytes(), "test2".getBytes()), 0);
        log.updateHighWatermark(2L);

        // simulate cleanup thread working on the log partition
        List<Map.Entry<TopicPartition, UnifiedLog>> deletableLog = cleanerManager.pauseCleaningForNonCompactedPartitions();
        assertEquals(1, deletableLog.size(), "should have 1 logs ready to be deleted");

        // change cleanup policy from delete to compact
        Properties logProps = new Properties();
        logProps.put(LogConfig.INTERNAL_SEGMENT_BYTES_CONFIG, log.config().segmentSize());
        logProps.put(TopicConfig.RETENTION_MS_CONFIG, log.config().retentionMs);
        logProps.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
        logProps.put(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, 0);
        LogConfig config = new LogConfig(logProps);
        log.updateConfig(config);

        // log cleanup in progress, the log is not available for compaction
        Optional<LogToClean> cleanable = cleanerManager.grabFilthiestCompactedLog(TIME, new PreCleanStats());
        assertTrue(cleanable.isEmpty(), "should have 0 logs ready to be compacted");

        // log cleanup finished, and log can be picked up for compaction
        cleanerManager.resumeCleaning(deletableLog.stream().map(Map.Entry::getKey).collect(Collectors.toSet()));
        Optional<LogToClean> cleanable2 = cleanerManager.grabFilthiestCompactedLog(TIME, new PreCleanStats());
        assertTrue(cleanable2.isPresent(), "should have 1 logs ready to be compacted");

        // update cleanup policy to delete
        logProps.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE);
        LogConfig config2 = new LogConfig(logProps);
        log.updateConfig(config2);

        // compaction in progress, should have 0 log eligible for log cleanup
        List<Map.Entry<TopicPartition, UnifiedLog>> deletableLog2 = cleanerManager.pauseCleaningForNonCompactedPartitions();
        assertEquals(0, deletableLog2.size(), "should have 0 logs ready to be deleted");

        // compaction done, should have 1 log eligible for log cleanup
        cleanerManager.doneDeleting(List.of(cleanable2.get().topicPartition()));
        List<Map.Entry<TopicPartition, UnifiedLog>> deletableLog3 = cleanerManager.pauseCleaningForNonCompactedPartitions();
        assertEquals(1, deletableLog3.size(), "should have 1 logs ready to be deleted");
    }

    @Test
    public void testUpdateCheckpointsShouldAddOffsetToPartition() throws IOException {
        MemoryRecords records = LogTestUtils.singletonRecords("test".getBytes(), "test".getBytes());
        UnifiedLog log = createLog(records.sizeInBytes() * 5, TopicConfig.CLEANUP_POLICY_COMPACT, new TopicPartition("log", 0));
        LogCleanerManager cleanerManager = createCleanerManager(log);

        // expect the checkpoint offset is not the expectedOffset before doing updateCheckpoints
        assertNotEquals(OFFSET, cleanerManager.allCleanerCheckpoints().getOrDefault(TOPIC_PARTITION, 0L));

        cleanerManager.updateCheckpoints(logDir, Optional.of(Map.entry(TOPIC_PARTITION, OFFSET)), Optional.empty());
        // expect the checkpoint offset is now updated to the expected offset after doing updateCheckpoints
        assertEquals(OFFSET, cleanerManager.allCleanerCheckpoints().get(TOPIC_PARTITION));
    }

    @Test
    public void testUpdateCheckpointsShouldRemovePartitionData() throws IOException {
        MemoryRecords records = LogTestUtils.singletonRecords("test".getBytes(), "test".getBytes());
        UnifiedLog log = createLog(records.sizeInBytes() * 5, TopicConfig.CLEANUP_POLICY_COMPACT, new TopicPartition("log", 0));
        LogCleanerManager cleanerManager = createCleanerManager(log);

        // write some data into the cleaner-offset-checkpoint file
        cleanerManager.updateCheckpoints(logDir, Optional.of(Map.entry(TOPIC_PARTITION, OFFSET)), Optional.empty());
        assertEquals(OFFSET, cleanerManager.allCleanerCheckpoints().get(TOPIC_PARTITION));

        // updateCheckpoints should remove the topicPartition data in the logDir
        cleanerManager.updateCheckpoints(logDir, Optional.empty(), Optional.of(TOPIC_PARTITION));
        assertFalse(cleanerManager.allCleanerCheckpoints().containsKey(TOPIC_PARTITION));
    }

    @Test
    public void testHandleLogDirFailureShouldRemoveDirAndData() throws IOException {
        MemoryRecords records = LogTestUtils.singletonRecords("test".getBytes(), "test".getBytes());
        UnifiedLog log = createLog(records.sizeInBytes() * 5, TopicConfig.CLEANUP_POLICY_COMPACT, new TopicPartition("log", 0));
        LogCleanerManager cleanerManager = createCleanerManager(log);

        // write some data into the cleaner-offset-checkpoint file in logDir and logDir2
        cleanerManager.updateCheckpoints(logDir, Optional.of(Map.entry(TOPIC_PARTITION, OFFSET)), Optional.empty());
        cleanerManager.updateCheckpoints(logDir2, Optional.of(Map.entry(TOPIC_PARTITION_2, OFFSET)), Optional.empty());
        assertEquals(OFFSET, cleanerManager.allCleanerCheckpoints().get(TOPIC_PARTITION));
        assertEquals(OFFSET, cleanerManager.allCleanerCheckpoints().get(TOPIC_PARTITION_2));

        cleanerManager.handleLogDirFailure(logDir.getAbsolutePath());
        // verify the partition data in logDir is gone, and data in logDir2 is still there
        assertEquals(OFFSET, cleanerManager.allCleanerCheckpoints().get(TOPIC_PARTITION_2));
        assertFalse(cleanerManager.allCleanerCheckpoints().containsKey(TOPIC_PARTITION));
    }

    @Test
    public void testMaybeTruncateCheckpointShouldTruncateData() throws IOException {
        MemoryRecords records = LogTestUtils.singletonRecords("test".getBytes(), "test".getBytes());
        UnifiedLog log = createLog(records.sizeInBytes() * 5, TopicConfig.CLEANUP_POLICY_COMPACT, new TopicPartition("log", 0));
        LogCleanerManager cleanerManager = createCleanerManager(log);
        long lowerOffset = 1L;
        long higherOffset = 1000L;

        // write some data into the cleaner-offset-checkpoint file in logDir
        cleanerManager.updateCheckpoints(logDir, Optional.of(Map.entry(TOPIC_PARTITION, OFFSET)), Optional.empty());
        assertEquals(OFFSET, cleanerManager.allCleanerCheckpoints().get(TOPIC_PARTITION));

        // we should not truncate the checkpoint data for checkpointed offset <= the given offset (higherOffset)
        cleanerManager.maybeTruncateCheckpoint(logDir, TOPIC_PARTITION, higherOffset);
        assertEquals(OFFSET, cleanerManager.allCleanerCheckpoints().get(TOPIC_PARTITION));

        // we should truncate the checkpoint data for checkpointed offset > the given offset (lowerOffset)
        cleanerManager.maybeTruncateCheckpoint(logDir, TOPIC_PARTITION, lowerOffset);
        assertEquals(lowerOffset, cleanerManager.allCleanerCheckpoints().get(TOPIC_PARTITION));
    }

    @Test
    public void testAlterCheckpointDirShouldRemoveDataInSrcDirAndAddInNewDir() throws IOException {
        MemoryRecords records = LogTestUtils.singletonRecords("test".getBytes(), "test".getBytes());
        UnifiedLog log = createLog(records.sizeInBytes() * 5, TopicConfig.CLEANUP_POLICY_COMPACT, new TopicPartition("log", 0));
        LogCleanerManager cleanerManager = createCleanerManager(log);

        // write some data into the cleaner-offset-checkpoint file in logDir
        cleanerManager.updateCheckpoints(logDir, Optional.of(Map.entry(TOPIC_PARTITION, OFFSET)), Optional.empty());
        assertEquals(OFFSET, cleanerManager.allCleanerCheckpoints().get(TOPIC_PARTITION));

        cleanerManager.alterCheckpointDir(TOPIC_PARTITION, logDir, logDir2);
        // verify we still can get the partition offset after alterCheckpointDir
        // This data should locate in logDir2, not logDir
        assertEquals(OFFSET, cleanerManager.allCleanerCheckpoints().get(TOPIC_PARTITION));

        // force delete the logDir2 from checkpoints, so that the partition data should also be deleted
        cleanerManager.handleLogDirFailure(logDir2.getAbsolutePath());
        assertFalse(cleanerManager.allCleanerCheckpoints().containsKey(TOPIC_PARTITION));
    }

    /**
     * Log under cleanup should still be eligible for log truncation.
     */
    @Test
    public void testConcurrentLogCleanupAndLogTruncation() throws IOException {
        MemoryRecords records = LogTestUtils.singletonRecords("test".getBytes(), "test".getBytes());
        UnifiedLog log = createLog(records.sizeInBytes() * 5, TopicConfig.CLEANUP_POLICY_DELETE, new TopicPartition("log", 0));
        LogCleanerManager cleanerManager = createCleanerManager(log);

        // log cleanup starts
        List<Map.Entry<TopicPartition, UnifiedLog>> pausedPartitions = cleanerManager.pauseCleaningForNonCompactedPartitions();
        // Log truncation happens due to unclean leader election
        cleanerManager.abortAndPauseCleaning(log.topicPartition());
        cleanerManager.resumeCleaning(Set.of(log.topicPartition()));
        // log cleanup finishes and pausedPartitions are resumed
        cleanerManager.resumeCleaning(pausedPartitions.stream().map(Map.Entry::getKey).collect(Collectors.toSet()));

        assertEquals(Optional.empty(), cleanerManager.cleaningState(log.topicPartition()));
    }

    /**
     * Log under cleanup should still be eligible for topic deletion.
     */
    @Test
    public void testConcurrentLogCleanupAndTopicDeletion() throws IOException {
        MemoryRecords records = LogTestUtils.singletonRecords("test".getBytes(), "test".getBytes());
        UnifiedLog log = createLog(records.sizeInBytes() * 5, TopicConfig.CLEANUP_POLICY_DELETE, new TopicPartition("log", 0));
        LogCleanerManager cleanerManager = createCleanerManager(log);

        // log cleanup starts
        List<Map.Entry<TopicPartition, UnifiedLog>> pausedPartitions = cleanerManager.pauseCleaningForNonCompactedPartitions();
        // Broker processes StopReplicaRequest with delete=true
        cleanerManager.abortCleaning(log.topicPartition());
        // log cleanup finishes and pausedPartitions are resumed
        cleanerManager.resumeCleaning(pausedPartitions.stream().map(Map.Entry::getKey).collect(Collectors.toSet()));

        assertEquals(Optional.empty(), cleanerManager.cleaningState(log.topicPartition()));
    }

    /**
     * When looking for logs with segments ready to be deleted we shouldn't consider
     * logs that have had their partition marked as uncleanable.
     */
    @Test
    public void testLogsWithSegmentsToDeleteShouldNotConsiderUncleanablePartitions() throws IOException {
        MemoryRecords records = LogTestUtils.singletonRecords("test".getBytes(), "test".getBytes());
        UnifiedLog log = createLog(records.sizeInBytes() * 5, TopicConfig.CLEANUP_POLICY_COMPACT, new TopicPartition("log", 0));
        LogCleanerManager cleanerManager = createCleanerManager(log);
        cleanerManager.markPartitionUncleanable(log.dir().getParent(), TOPIC_PARTITION);

        int readyToDelete = cleanerManager.deletableLogs().size();
        assertEquals(0, readyToDelete, "should have 0 logs ready to be deleted");
    }

    /**
     * Test computation of cleanable range with no minimum compaction lag settings active where bounded by LSO.
     */
    @Test
    public void testCleanableOffsetsForNone() throws IOException {
        Properties logProps = new Properties();
        logProps.put(LogConfig.INTERNAL_SEGMENT_BYTES_CONFIG, 1024);
        UnifiedLog log = makeLog(LogConfig.fromProps(LOG_CONFIG.originals(), logProps));

        while (log.numberOfSegments() < 8)
            log.appendAsLeader(records((int) log.logEndOffset(), (int) log.logEndOffset(), TIME.milliseconds()), 0);

        log.updateHighWatermark(50);

        Optional<Long> lastCleanOffset = Optional.of(0L);
        OffsetsToClean cleanableOffsets = LogCleanerManager.cleanableOffsets(log, lastCleanOffset, TIME.milliseconds());
        assertEquals(0L, cleanableOffsets.firstDirtyOffset(), "The first cleanable offset starts at the beginning of the log.");
        assertEquals(log.highWatermark(), log.lastStableOffset(),
            "The high watermark equals the last stable offset as no transactions are in progress");
        assertEquals(log.lastStableOffset(), cleanableOffsets.firstUncleanableDirtyOffset(),
            "The first uncleanable offset is bounded by the last stable offset.");
    }

    /**
     * Test computation of cleanable range with no minimum compaction lag settings active where bounded by active segment.
     */
    @Test
    public void testCleanableOffsetsActiveSegment() throws IOException {
        Properties logProps = new Properties();
        logProps.put(LogConfig.INTERNAL_SEGMENT_BYTES_CONFIG, 1024);
        UnifiedLog log = makeLog(LogConfig.fromProps(LOG_CONFIG.originals(), logProps));

        while (log.numberOfSegments() < 8)
            log.appendAsLeader(records((int) log.logEndOffset(), (int) log.logEndOffset(), TIME.milliseconds()), 0);

        log.updateHighWatermark(log.logEndOffset());

        Optional<Long> lastCleanOffset = Optional.of(0L);
        OffsetsToClean cleanableOffsets = LogCleanerManager.cleanableOffsets(log, lastCleanOffset, TIME.milliseconds());

        assertEquals(0L, cleanableOffsets.firstDirtyOffset(), "The first cleanable offset starts at the beginning of the log.");
        assertEquals(log.activeSegment().baseOffset(), cleanableOffsets.firstUncleanableDirtyOffset(),
            "The first uncleanable offset begins with the active segment.");
    }

    /**
     * Test computation of cleanable range with a minimum compaction lag time
     */
    @Test
    public void testCleanableOffsetsForTime() throws IOException {
        int compactionLag = 60 * 60 * 1000;
        Properties logProps = new Properties();
        logProps.put(LogConfig.INTERNAL_SEGMENT_BYTES_CONFIG, 1024);
        logProps.put(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG, compactionLag);
        UnifiedLog log = makeLog(LogConfig.fromProps(LOG_CONFIG.originals(), logProps));

        long t0 = TIME.milliseconds();
        while (log.numberOfSegments() < 4)
            log.appendAsLeader(records((int) log.logEndOffset(), (int) log.logEndOffset(), t0), 0);

        LogSegment activeSegAtT0 = log.activeSegment();

        TIME.sleep(compactionLag + 1);
        long t1 = TIME.milliseconds();

        while (log.numberOfSegments() < 8)
            log.appendAsLeader(records((int) log.logEndOffset(), (int) log.logEndOffset(), t1), 0);

        log.updateHighWatermark(log.logEndOffset());

        Optional<Long> lastCleanOffset = Optional.of(0L);
        OffsetsToClean cleanableOffsets = LogCleanerManager.cleanableOffsets(log, lastCleanOffset, TIME.milliseconds());
        assertEquals(0L, cleanableOffsets.firstDirtyOffset(), "The first cleanable offset starts at the beginning of the log.");
        assertEquals(activeSegAtT0.baseOffset(), cleanableOffsets.firstUncleanableDirtyOffset(),
            "The first uncleanable offset begins with the second block of log entries.");
    }

    /**
     * Test computation of cleanable range with a minimum compaction lag time that is small enough that
     * the active segment contains it.
     */
    @Test
    public void testCleanableOffsetsForShortTime() throws IOException {
        int compactionLag = 60 * 60 * 1000;
        Properties logProps = new Properties();
        logProps.put(LogConfig.INTERNAL_SEGMENT_BYTES_CONFIG, 1024);
        logProps.put(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG, compactionLag);

        UnifiedLog log = makeLog(LogConfig.fromProps(LOG_CONFIG.originals(), logProps));

        long t0 = TIME.milliseconds();
        while (log.numberOfSegments() < 8)
            log.appendAsLeader(records((int) log.logEndOffset(), (int) log.logEndOffset(), t0), 0);

        log.updateHighWatermark(log.logEndOffset());

        TIME.sleep(compactionLag + 1);

        Optional<Long> lastCleanOffset = Optional.of(0L);
        OffsetsToClean cleanableOffsets = LogCleanerManager.cleanableOffsets(log, lastCleanOffset, TIME.milliseconds());
        assertEquals(0L, cleanableOffsets.firstDirtyOffset(), "The first cleanable offset starts at the beginning of the log.");
        assertEquals(log.activeSegment().baseOffset(), cleanableOffsets.firstUncleanableDirtyOffset(),
            "The first uncleanable offset begins with active segment.");
    }

    @Test
    public void testCleanableOffsetsNeedsCheckpointReset() throws IOException {
        TopicPartition tp = new TopicPartition("foo", 0);
        ConcurrentMap<TopicPartition, UnifiedLog> logs = setupIncreasinglyFilthyLogs(List.of(tp));
        logs.get(tp).maybeIncrementLogStartOffset(10L, LogStartOffsetIncrementReason.ClientRecordDeletion);

        Optional<Long> lastCleanOffset = Optional.of(15L);
        OffsetsToClean cleanableOffsets = LogCleanerManager.cleanableOffsets(logs.get(tp), lastCleanOffset, TIME.milliseconds());
        assertFalse(cleanableOffsets.forceUpdateCheckpoint(), "Checkpoint offset should not be reset if valid");

        logs.get(tp).maybeIncrementLogStartOffset(20L, LogStartOffsetIncrementReason.ClientRecordDeletion);
        cleanableOffsets = LogCleanerManager.cleanableOffsets(logs.get(tp), lastCleanOffset, TIME.milliseconds());
        assertTrue(cleanableOffsets.forceUpdateCheckpoint(), "Checkpoint offset needs to be reset if less than log start offset");

        lastCleanOffset = Optional.of(25L);
        cleanableOffsets = LogCleanerManager.cleanableOffsets(logs.get(tp), lastCleanOffset, TIME.milliseconds());
        assertTrue(cleanableOffsets.forceUpdateCheckpoint(), "Checkpoint offset needs to be reset if greater than log end offset");
    }

    @Test
    public void testUndecidedTransactionalDataNotCleanable() throws IOException {
        int compactionLag = 60 * 60 * 1000;
        Properties logProps = new Properties();
        logProps.put(LogConfig.INTERNAL_SEGMENT_BYTES_CONFIG, 1024);
        logProps.put(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG, compactionLag);
        UnifiedLog log = makeLog(LogConfig.fromProps(LOG_CONFIG.originals(), logProps));

        long producerId = 15L;
        short producerEpoch = 0;
        int sequence = 0;
        log.appendAsLeader(MemoryRecords.withTransactionalRecords(Compression.NONE, producerId, producerEpoch, sequence,
            new SimpleRecord(TIME.milliseconds(), "1".getBytes(), "a".getBytes()),
            new SimpleRecord(TIME.milliseconds(), "2".getBytes(), "b".getBytes())), 0);
        log.appendAsLeader(MemoryRecords.withTransactionalRecords(Compression.NONE, producerId, producerEpoch, sequence + 2,
            new SimpleRecord(TIME.milliseconds(), "3".getBytes(), "c".getBytes())), 0);
        log.roll();
        log.updateHighWatermark(3L);

        TIME.sleep(compactionLag + 1);
        // although the compaction lag has been exceeded, the undecided data should not be cleaned
        OffsetsToClean cleanableOffsets = LogCleanerManager.cleanableOffsets(log, Optional.of(0L), TIME.milliseconds());
        assertEquals(0L, cleanableOffsets.firstDirtyOffset());
        assertEquals(0L, cleanableOffsets.firstUncleanableDirtyOffset());

        log.appendAsLeader(MemoryRecords.withEndTransactionMarker(TIME.milliseconds(), producerId, producerEpoch,
            new EndTransactionMarker(ControlRecordType.ABORT, 15)), 0, AppendOrigin.COORDINATOR, RequestLocal.noCaching(),
            VerificationGuard.SENTINEL, TransactionVersion.TV_1.featureLevel());
        log.roll();
        log.updateHighWatermark(4L);

        // the first segment should now become cleanable immediately
        cleanableOffsets = LogCleanerManager.cleanableOffsets(log, Optional.of(0L), TIME.milliseconds());
        assertEquals(0L, cleanableOffsets.firstDirtyOffset());
        assertEquals(3L, cleanableOffsets.firstUncleanableDirtyOffset());

        TIME.sleep(compactionLag + 1);

        // the second segment becomes cleanable after the compaction lag
        cleanableOffsets = LogCleanerManager.cleanableOffsets(log, Optional.of(0L), TIME.milliseconds());
        assertEquals(0L, cleanableOffsets.firstDirtyOffset());
        assertEquals(4L, cleanableOffsets.firstUncleanableDirtyOffset());
    }

    @Test
    public void testDoneCleaning() throws IOException {
        Properties logProps = new Properties();
        logProps.put(LogConfig.INTERNAL_SEGMENT_BYTES_CONFIG, 1024);
        UnifiedLog log = makeLog(LogConfig.fromProps(LOG_CONFIG.originals(), logProps));

        while (log.numberOfSegments() < 8)
            log.appendAsLeader(records((int) log.logEndOffset(), (int) log.logEndOffset(), TIME.milliseconds()), 0);

        LogCleanerManager cleanerManager = createCleanerManager(log);
        assertThrows(IllegalStateException.class, () -> cleanerManager.doneCleaning(TOPIC_PARTITION, log.dir(), 1));

        cleanerManager.setCleaningState(TOPIC_PARTITION, LogCleaningState.logCleaningPaused(1));
        assertThrows(IllegalStateException.class, () -> cleanerManager.doneCleaning(TOPIC_PARTITION, log.dir(), 1));

        cleanerManager.setCleaningState(TOPIC_PARTITION, LOG_CLEANING_IN_PROGRESS);
        long endOffset = 1L;
        cleanerManager.doneCleaning(TOPIC_PARTITION, log.dir(), endOffset);

        assertTrue(cleanerManager.cleaningState(TOPIC_PARTITION).isEmpty());
        assertTrue(cleanerManager.allCleanerCheckpoints().containsKey(TOPIC_PARTITION));
        assertEquals(endOffset, cleanerManager.allCleanerCheckpoints().get(TOPIC_PARTITION));

        cleanerManager.setCleaningState(TOPIC_PARTITION, LOG_CLEANING_ABORTED);
        cleanerManager.doneCleaning(TOPIC_PARTITION, log.dir(), endOffset);

        assertEquals(LogCleaningState.logCleaningPaused(1), cleanerManager.cleaningState(TOPIC_PARTITION).get());
        assertTrue(cleanerManager.allCleanerCheckpoints().containsKey(TOPIC_PARTITION));
    }

    @Test
    public void testDoneDeleting() throws IOException {
        MemoryRecords records = LogTestUtils.singletonRecords("test".getBytes(), "test".getBytes());
        UnifiedLog log = createLog(records.sizeInBytes() * 5, TopicConfig.CLEANUP_POLICY_COMPACT +
            "," + TopicConfig.CLEANUP_POLICY_DELETE, new TopicPartition("log", 0));
        LogCleanerManager cleanerManager = createCleanerManager(log);
        TopicPartition tp = new TopicPartition("log", 0);

        assertThrows(IllegalStateException.class, () -> cleanerManager.doneDeleting(List.of(tp)));

        cleanerManager.setCleaningState(tp, LogCleaningState.logCleaningPaused(1));
        assertThrows(IllegalStateException.class, () -> cleanerManager.doneDeleting(List.of(tp)));

        cleanerManager.setCleaningState(tp, LOG_CLEANING_IN_PROGRESS);
        cleanerManager.doneDeleting(List.of(tp));
        assertTrue(cleanerManager.cleaningState(tp).isEmpty());

        cleanerManager.setCleaningState(tp, LOG_CLEANING_ABORTED);
        cleanerManager.doneDeleting(List.of(tp));
        assertEquals(LogCleaningState.logCleaningPaused(1), cleanerManager.cleaningState(tp).get());
    }

    /**
     * Logs with invalid checkpoint offsets should update their checkpoint offset even if the log doesn't need cleaning.
     */
    @Test
    public void testCheckpointUpdatedForInvalidOffsetNoCleaning() throws IOException {
        TopicPartition tp = new TopicPartition("foo", 0);
        ConcurrentMap<TopicPartition, UnifiedLog> logs = setupIncreasinglyFilthyLogs(List.of(tp));

        logs.get(tp).maybeIncrementLogStartOffset(20L, LogStartOffsetIncrementReason.ClientRecordDeletion);
        LogCleanerManagerMock cleanerManager = createCleanerManagerMock(logs);
        cleanerManager.addCheckpoint(tp, 15L);

        Optional<LogToClean> filthiestLog = cleanerManager.grabFilthiestCompactedLog(TIME, new PreCleanStats());
        assertEquals(Optional.empty(), filthiestLog, "Log should not be selected for cleaning");
        assertEquals(20L, cleanerManager.checkpointOffset(tp), "Unselected log should have checkpoint offset updated");
    }

    /**
     * Logs with invalid checkpoint offsets should update their checkpoint offset even if they aren't selected
     * for immediate cleaning.
     */
    @Test
    public void testCheckpointUpdatedForInvalidOffsetNotSelected() throws IOException {
        TopicPartition tp0 = new TopicPartition("foo", 0);
        TopicPartition tp1 = new TopicPartition("foo", 1);
        List<TopicPartition> partitions = List.of(tp0, tp1);

        // create two logs, one with an invalid offset, and one that is dirtier than the log with an invalid offset
        ConcurrentMap<TopicPartition, UnifiedLog> logs = setupIncreasinglyFilthyLogs(partitions);
        logs.get(tp0).maybeIncrementLogStartOffset(15L, LogStartOffsetIncrementReason.ClientRecordDeletion);
        LogCleanerManagerMock cleanerManager = createCleanerManagerMock(logs);
        cleanerManager.addCheckpoint(tp0, 10L);
        cleanerManager.addCheckpoint(tp1, 5L);

        LogToClean filthiestLog = cleanerManager.grabFilthiestCompactedLog(TIME, new PreCleanStats()).get();
        assertEquals(tp1, filthiestLog.topicPartition(), "Dirtier log should be selected");
        assertEquals(15L, cleanerManager.checkpointOffset(tp0), "Unselected log should have checkpoint offset updated");
    }

    private LogCleanerManager createCleanerManager(UnifiedLog log) {
        ConcurrentMap<TopicPartition, UnifiedLog> logs = new ConcurrentHashMap<>();
        logs.put(TOPIC_PARTITION, log);

        return new LogCleanerManager(List.of(logDir, logDir2), logs, null);
    }

    private LogCleanerManagerMock createCleanerManagerMock(ConcurrentMap<TopicPartition, UnifiedLog> pool) {
        return new LogCleanerManagerMock(List.of(logDir), pool, null);
    }

    private UnifiedLog createLog(int segmentSize, String cleanupPolicy, TopicPartition topicPartition) throws IOException {
        LogConfig config = createLowRetentionLogConfig(segmentSize, cleanupPolicy);
        File partitionDir = new File(logDir, UnifiedLog.logDirName(topicPartition));

        return UnifiedLog.create(partitionDir, config, 0L, 0L, TIME.scheduler, new BrokerTopicStats(), TIME, 5 * 60 * 1000,
            PRODUCER_STATE_MANAGER_CONFIG, PRODUCER_ID_EXPIRATION_CHECK_INTERVAL_MS_DEFAULT, new LogDirFailureChannel(10),
            true, Optional.empty());
    }

    private LogConfig createLowRetentionLogConfig(int segmentSize, String cleanupPolicy) {
        Properties logProps = new Properties();
        logProps.put(LogConfig.INTERNAL_SEGMENT_BYTES_CONFIG, segmentSize);
        logProps.put(TopicConfig.RETENTION_MS_CONFIG, 1);
        logProps.put(TopicConfig.CLEANUP_POLICY_CONFIG, cleanupPolicy);
        logProps.put(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, 0.05); // small for easier and clearer tests

        return new LogConfig(logProps);
    }

    private void writeRecords(UnifiedLog log, int numBatches, int recordsPerBatch, int batchesPerSegment) throws IOException {
        for (int i = 0; i < numBatches; i++) {
            appendRecords(log, recordsPerBatch);
            if (i % batchesPerSegment == 0)
                log.roll();
        }
        log.roll();
    }

    private void appendRecords(UnifiedLog log, int numRecords) throws IOException {
        long startOffset = log.logEndOffset();
        long endOffset = startOffset + numRecords;

        SimpleRecord[] records = IntStream.range((int) startOffset, (int) endOffset)
            .mapToObj(offset -> new SimpleRecord(TIME.milliseconds(), String.format("key-%d", offset).getBytes(),
                String.format("value-%d", offset).getBytes()))
            .toArray(SimpleRecord[]::new);

        log.appendAsLeader(MemoryRecords.withRecords(Compression.NONE, records), 1);
        log.maybeIncrementHighWatermark(log.logEndOffsetMetadata());
    }

    private UnifiedLog makeLog(LogConfig config) throws IOException {
        return UnifiedLog.create(logDir, config, 0L, 0L, TIME.scheduler, new BrokerTopicStats(), TIME, 5 * 60 * 1000,
            PRODUCER_STATE_MANAGER_CONFIG, PRODUCER_ID_EXPIRATION_CHECK_INTERVAL_MS_DEFAULT, new LogDirFailureChannel(10),
            true, Optional.empty());
    }

    private MemoryRecords records(int key, int value, long timestamp) {
        return MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(timestamp, Integer.toString(key).getBytes(),
            Integer.toString(value).getBytes()));
    }

    private static LogConfig createLogConfig() {
        Properties logProps = new Properties();
        logProps.put(LogConfig.INTERNAL_SEGMENT_BYTES_CONFIG, 1024);
        logProps.put(TopicConfig.SEGMENT_INDEX_BYTES_CONFIG, 1024);
        logProps.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);

        return new LogConfig(logProps);
    }
}
