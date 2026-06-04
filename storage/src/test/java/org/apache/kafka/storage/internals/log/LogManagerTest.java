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

import org.apache.kafka.common.DirectoryId;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.common.errors.OffsetOutOfRangeException;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.transaction.TransactionLogConfig;
import org.apache.kafka.metadata.ConfigRepository;
import org.apache.kafka.metadata.MockConfigRepository;
import org.apache.kafka.metadata.properties.MetaProperties;
import org.apache.kafka.metadata.properties.MetaPropertiesEnsemble;
import org.apache.kafka.metadata.properties.MetaPropertiesVersion;
import org.apache.kafka.metadata.properties.PropertiesUtils;
import org.apache.kafka.server.metrics.KafkaYammerMetrics;
import org.apache.kafka.server.storage.log.FetchIsolation;
import org.apache.kafka.server.util.FileLock;
import org.apache.kafka.server.util.KafkaScheduler;
import org.apache.kafka.server.util.MockTime;
import org.apache.kafka.server.util.Scheduler;
import org.apache.kafka.storage.internals.checkpoint.CleanShutdownFileHandler;
import org.apache.kafka.storage.internals.checkpoint.OffsetCheckpointFile;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;
import org.apache.kafka.test.TestUtils;

import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.MetricName;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermission;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class LogManagerTest {

    private final MockTime time = new MockTime();
    private static final int MAX_LOG_AGE_MS = 10 * 60 * 1000;
    private static final Map<?, ?> LOG_PROPS = Map.of(
        LogConfig.INTERNAL_SEGMENT_BYTES_CONFIG, 1024,
        TopicConfig.SEGMENT_INDEX_BYTES_CONFIG, 4096,
        TopicConfig.RETENTION_MS_CONFIG, MAX_LOG_AGE_MS
    );
    private static final LogConfig LOG_CONFIG = new LogConfig(LOG_PROPS);
    private static final String NAME = "kafka";
    private static final long INITIAL_TASK_DELAY_MS = 10 * 1000;

    private File logDir;
    private File logDir2;
    private LogManager logManager;

    @BeforeEach
    public void setUp() throws Exception {
        logDir = TestUtils.tempDirectory();
        logDir2 = TestUtils.tempDirectory();
        logManager = createLogManager();
        logManager.startup(Set.of());
        assertEquals(INITIAL_TASK_DELAY_MS, logManager.initialTaskDelayMs());
    }

    @AfterEach
    public void tearDown() throws IOException {
        if (logManager != null) {
            logManager.shutdown();
        }
        Utils.delete(logDir);
        Utils.delete(logDir2);
        // Some tests assign a new LogManager
        if (logManager != null) {
            for (File dir : logManager.liveLogDirs()) {
                Utils.delete(dir);
            }
        }
    }

    /**
     * Test that getOrCreateLog on a non-existent log creates a new log and that we can append to the new log.
     */
    @Test
    public void testCreateLog() throws IOException {
        UnifiedLog log = logManager.getOrCreateLog(new TopicPartition(NAME, 0), false, false, Optional.empty(), Optional.empty());
        assertEquals(1, logManager.liveLogDirs().size());

        File logFile = new File(logDir, NAME + "-0");
        assertTrue(logFile.exists());
        log.appendAsLeader(LogTestUtils.singletonRecords("test".getBytes()), 0);
    }

    /**
     * Test that getOrCreateLog on a non-existent log creates a new log in given logDirectory using directory id and that we can append to the new log.
     */
    @Test
    public void testCreateLogOnTargetedLogDirectory() throws IOException {
        Uuid targetedLogDirectoryId = DirectoryId.random();

        List<File> dirs = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            dirs.add(TestUtils.tempDirectory());
        }
        writeMetaProperties(dirs.get(0));
        writeMetaProperties(dirs.get(1), Optional.of(targetedLogDirectoryId));
        writeMetaProperties(dirs.get(3), Optional.of(DirectoryId.random()));
        writeMetaProperties(dirs.get(4));

        logManager = createLogManager(dirs);

        UnifiedLog log = logManager.getOrCreateLog(new TopicPartition(NAME, 0), false, false, Optional.empty(), Optional.of(targetedLogDirectoryId));
        assertEquals(5, logManager.liveLogDirs().size());

        File logFile = new File(dirs.get(1), NAME + "-0");
        assertTrue(logFile.exists());
        assertEquals(dirs.get(1).getAbsolutePath(), logFile.getParent());
        log.appendAsLeader(LogTestUtils.singletonRecords("test".getBytes()), 0);
    }

    /**
     * Test that getOrCreateLog on a non-existent log creates a new log in the next selected logDirectory if the given directory id is DirectoryId.UNASSIGNED.
     */
    @Test
    public void testCreateLogWithTargetedLogDirectorySetAsUnassigned() throws IOException {
        UnifiedLog log = logManager.getOrCreateLog(new TopicPartition(NAME, 0), false, false, Optional.empty(), Optional.of(DirectoryId.UNASSIGNED));
        assertEquals(1, logManager.liveLogDirs().size());
        File logFile = new File(logDir, NAME + "-0");
        assertTrue(logFile.exists());
        assertTrue(logManager.directoryId(logFile.getParent()).isEmpty());
        log.appendAsLeader(LogTestUtils.singletonRecords("test".getBytes()), 0);
    }

    @Test
    public void testCreateLogWithTargetedLogDirectorySetAsUnknownWithoutAnyOfflineDirectories() throws IOException {
        UnifiedLog log = logManager.getOrCreateLog(new TopicPartition(NAME, 0), false, false, Optional.empty(), Optional.of(DirectoryId.LOST));
        assertEquals(1, logManager.liveLogDirs().size());
        File logFile = new File(logDir, NAME + "-0");
        assertTrue(logFile.exists());
        assertTrue(logManager.directoryId(logFile.getParent()).isEmpty());
        log.appendAsLeader(LogTestUtils.singletonRecords("test".getBytes()), 0);
    }

    /**
     * Tests that all internal futures are completed before LogManager.shutdown() returns to the
     * caller during error situations.
     */
    @Test
    public void testHandlingExceptionsDuringShutdown() throws Exception {
        // We create two directories logDir1 and logDir2 to help effectively test error handling
        // during LogManager.shutdown().
        File logDir1 = TestUtils.tempDirectory();
        File logDir2 = TestUtils.tempDirectory();
        LogManager manager = createLogManager(List.of(logDir1, logDir2));
        try {
            assertEquals(2, manager.liveLogDirs().size());
            manager.startup(Set.of());

            UnifiedLog log1 = manager.getOrCreateLog(new TopicPartition(NAME, 0), Optional.empty());
            UnifiedLog log2 = manager.getOrCreateLog(new TopicPartition(NAME, 1), Optional.empty());

            File logFile1 = new File(logDir1, NAME + "-0");
            assertTrue(logFile1.exists());
            File logFile2 = new File(logDir2, NAME + "-1");
            assertTrue(logFile2.exists());

            log1.appendAsLeader(LogTestUtils.singletonRecords("test1".getBytes()), 0);
            log1.takeProducerSnapshot();
            log1.appendAsLeader(LogTestUtils.singletonRecords("test1".getBytes()), 0);

            log2.appendAsLeader(LogTestUtils.singletonRecords("test2".getBytes()), 0);
            log2.takeProducerSnapshot();
            log2.appendAsLeader(LogTestUtils.singletonRecords("test2".getBytes()), 0);

            // This should cause log1.close() to fail during LogManger shutdown sequence.
            Utils.delete(logFile1);

            manager.shutdown(3);

            assertFalse(Files.exists(new File(logDir1, CleanShutdownFileHandler.CLEAN_SHUTDOWN_FILE_NAME).toPath()));
            assertTrue(Files.exists(new File(logDir2, CleanShutdownFileHandler.CLEAN_SHUTDOWN_FILE_NAME).toPath()));
            assertEquals(OptionalLong.empty(), manager.readBrokerEpochFromCleanShutdownFiles());
        } finally {
            if (manager != null) {
                for (File log : manager.liveLogDirs()) {
                    Utils.delete(log);
                }
            }
        }
    }

    @Test
    public void testCleanShutdownFileWithBrokerEpoch() throws Exception {
        File logDir1 = TestUtils.tempDirectory();
        File logDir2 = TestUtils.tempDirectory();
        LogManager manager = createLogManager(List.of(logDir1, logDir2));
        try {
            assertEquals(2, manager.liveLogDirs().size());
            manager.startup(Set.of());
            manager.getOrCreateLog(new TopicPartition(NAME, 0), Optional.empty());
            manager.getOrCreateLog(new TopicPartition(NAME, 1), Optional.empty());

            File logFile1 = new File(logDir1, NAME + "-0");
            assertTrue(logFile1.exists());
            File logFile2 = new File(logDir2, NAME + "-1");
            assertTrue(logFile2.exists());

            manager.shutdown(3);

            assertTrue(Files.exists(new File(logDir1, CleanShutdownFileHandler.CLEAN_SHUTDOWN_FILE_NAME).toPath()));
            assertTrue(Files.exists(new File(logDir2, CleanShutdownFileHandler.CLEAN_SHUTDOWN_FILE_NAME).toPath()));
            assertEquals(OptionalLong.of(3L), manager.readBrokerEpochFromCleanShutdownFiles());
        } finally {
            if (manager != null) {
                for (File log : manager.liveLogDirs()) {
                    Utils.delete(log);
                }
            }
        }
    }

    /*
     * Test that LogManager.shutdown() doesn't create clean shutdown file for a log directory that has not completed
     * recovery.
     */
    @Test
    public void testCleanShutdownFileWhenShutdownCalledBeforeStartupComplete() throws IOException, InterruptedException {
        // 1. create two logs under logDir
        TopicPartition topicPartition0 = new TopicPartition(NAME, 0);
        TopicPartition topicPartition1 = new TopicPartition(NAME, 1);
        UnifiedLog log0 = logManager.getOrCreateLog(topicPartition0, Optional.empty());
        UnifiedLog log1 = logManager.getOrCreateLog(topicPartition1, Optional.empty());
        File logFile0 = new File(logDir, NAME + "-0");
        File logFile1 = new File(logDir, NAME + "-1");
        assertTrue(logFile0.exists());
        assertTrue(logFile1.exists());

        log0.appendAsLeader(LogTestUtils.singletonRecords("test1".getBytes()), 0);
        log0.takeProducerSnapshot();

        log1.appendAsLeader(LogTestUtils.singletonRecords("test1".getBytes()), 0);
        log1.takeProducerSnapshot();

        // 2. simulate unclean shutdown by deleting clean shutdown marker file
        logManager.shutdown();
        assertTrue(Files.deleteIfExists(new File(logDir, CleanShutdownFileHandler.CLEAN_SHUTDOWN_FILE_NAME).toPath()));

        // 3. create a new LogManager and start it in a different thread
        AtomicInteger loadLogCalled = new AtomicInteger(0);
        logManager = spy(createLogManager());
        doAnswer(invocation -> {
            // intercept LogManager.loadLog to sleep 5 seconds so that there is enough time to call LogManager.shutdown
            // before LogManager.startup completes.
            Thread.sleep(5000);
            loadLogCalled.incrementAndGet();
            return invocation.callRealMethod();
        }).when(logManager).loadLog(any(File.class), any(Boolean.class), anyMap(), anyMap(),
            any(LogConfig.class), anyMap(), any(ConcurrentMap.class), any(Function.class));

        Thread t = new Thread(() -> {
            try {
                logManager.startup(Set.of());
            } catch (Exception exc) {
                throw new RuntimeException(exc);
            }
        }, "log-manager-test-startup");
        t.start();

        // 4. shutdown LogManager after the first log is loaded but before the second log is loaded
        TestUtils.waitForCondition(() -> loadLogCalled.get() == 1,
                "Timed out waiting for only the first log to be loaded");
        logManager.shutdown();
        logManager = null;
        t.join();

        // 5. verify that CleanShutdownFile is not created under logDir
        assertFalse(Files.exists(new File(logDir, CleanShutdownFileHandler.CLEAN_SHUTDOWN_FILE_NAME).toPath()));
    }

    /**
     * Test that getOrCreateLog on a non-existent log creates a new log and that we can append to the new log.
     * The LogManager is configured with one invalid log directory which should be marked as offline.
     */
    @Test
    public void testCreateLogWithInvalidLogDir() throws Exception {
        // Configure the log dir with the Nul character as the path, which causes dir.getCanonicalPath() to throw an
        // IOException. This simulates the scenario where the disk is not properly mounted (which is hard to achieve in
        // a unit test)
        List<File> dirs = List.of(logDir, new File("\u0000"));

        logManager.shutdown();
        Utils.delete(logDir);
        logManager = createLogManager(dirs);
        logManager.startup(Set.of());

        UnifiedLog log = logManager.getOrCreateLog(new TopicPartition(NAME, 0), true, false, Optional.empty());
        File logFile = new File(logDir, NAME + "-0");
        assertTrue(logFile.exists());
        log.appendAsLeader(LogTestUtils.singletonRecords("test".getBytes()), 0);
    }

    @Test
    public void testCreateLogWithLogDirFallback() throws Exception {
        // Configure a number of directories one level deeper in logDir,
        // so they all get cleaned up in tearDown().
        List<File> dirs = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            dirs.add(logDir.toPath().resolve(String.valueOf(i)).toFile());
        }

        // Create a new LogManager with the configured directories and an overridden createLogDirectory.
        logManager.shutdown();
        logManager = spy(createLogManager(dirs));
        Set<File> brokenDirs = new HashSet<>();
        doAnswer(invocation -> {
            // The first half of directories tried will fail, the rest goes through.
            File logDir = invocation.getArgument(0);
            if (brokenDirs.contains(logDir) || brokenDirs.size() < dirs.size() / 2) {
                brokenDirs.add(logDir);
                throw new KafkaStorageException("broken dir");
            } else {
                return invocation.callRealMethod();
            }
        }).when(logManager).createLogDirectory(any(), any());
        logManager.startup(Set.of());

        // Request creating a new log.
        // LogManager should try using all configured log directories until one succeeds.
        logManager.getOrCreateLog(new TopicPartition(NAME, 0), true, false, Optional.empty());

        // Verify that half the directories were considered broken,
        assertEquals(dirs.size() / 2, brokenDirs.size());

        // and that exactly one log file was created,
        Function<File, Boolean> containsLogFile = dir -> new File(dir, NAME + "-0").exists();
        int found = 0;
        for (File dir : dirs) {
            if (containsLogFile.apply(dir)) {
                found++;
            }
        }
        assertEquals(1, found, "More than one log file created");

        // and that it wasn't created in one of the broken directories.
        for (File dir : brokenDirs) {
            assertFalse(containsLogFile.apply(dir));
        }
    }

    /**
     * Test that get on a non-existent returns None and no log is created.
     */
    @Test
    public void testGetNonExistentLog() {
        Optional<UnifiedLog> log = logManager.getLog(new TopicPartition(NAME, 0));
        assertEquals(Optional.empty(), log, "No log should be found.");
        File logFile = new File(logDir, NAME + "-0");
        assertFalse(logFile.exists());
    }

    /**
     * Test time-based log cleanup. First append messages, then set the time into the future and run cleanup.
     */
    @Test
    public void testCleanupExpiredSegments() throws IOException {
        UnifiedLog log = logManager.getOrCreateLog(new TopicPartition(NAME, 0), Optional.empty());
        long offset = 0L;
        for (int i = 0; i < 200; i++) {
            MemoryRecords set = LogTestUtils.singletonRecords("test".getBytes());
            LogAppendInfo info = log.appendAsLeader(set, 0);
            offset = info.lastOffset();
        }
        assertTrue(log.numberOfSegments() > 1, "There should be more than one segment now.");
        log.updateHighWatermark(log.logEndOffset());

        log.logSegments().forEach(s -> s.log().file().setLastModified(time.milliseconds()));

        time.sleep(MAX_LOG_AGE_MS + 1);
        assertEquals(1, log.numberOfSegments(), "Now there should only be only one segment in the index.");
        time.sleep(log.config().fileDeleteDelayMs + 1);

        for (LogSegment s : log.logSegments()) {
            s.offsetIndex();
            s.timeIndex();
        }

        // there should be a log file, two indexes, one producer snapshot, and the leader epoch checkpoint
        assertEquals(log.numberOfSegments() * 4 + 1, log.dir().list().length, "Files should have been deleted");
        assertEquals(0, readLog(log, offset + 1).records.sizeInBytes(), "Should get empty fetch off new log.");

        assertThrows(OffsetOutOfRangeException.class, () -> readLog(log, 0), "Should get exception from fetching earlier.");
        // log should still be appendable
        log.appendAsLeader(LogTestUtils.singletonRecords("test".getBytes()), 0);
    }

    /**
     * Test size-based cleanup. Append messages, then run cleanup and check that segments are deleted.
     */
    @Test
    public void testCleanupSegmentsToMaintainSize() throws Exception {
        int setSize = LogTestUtils.singletonRecords("test".getBytes()).sizeInBytes();
        logManager.shutdown();
        int segmentBytes = 10 * setSize;
        Properties properties = new Properties();
        properties.put(LogConfig.INTERNAL_SEGMENT_BYTES_CONFIG, String.valueOf(segmentBytes));
        properties.put(TopicConfig.RETENTION_BYTES_CONFIG, String.valueOf(5L * 10L * setSize + 10L));
        ConfigRepository configRepository = MockConfigRepository.forTopic(NAME, properties);

        logManager = createLogManager(configRepository);
        logManager.startup(Set.of());

        // create a log
        UnifiedLog log = logManager.getOrCreateLog(new TopicPartition(NAME, 0), Optional.empty());
        long offset = 0L;

        // add a bunch of messages that should be larger than the retentionSize
        int numMessages = 200;
        for (int i = 0; i < numMessages; i++) {
            MemoryRecords set = LogTestUtils.singletonRecords("test".getBytes());
            LogAppendInfo info = log.appendAsLeader(set, 0);
            offset = info.firstOffset();
        }

        log.updateHighWatermark(log.logEndOffset());
        assertEquals(numMessages * setSize / segmentBytes, log.numberOfSegments(), "Check we have the expected number of segments.");

        // this cleanup shouldn't find any expired segments but should delete some to reduce size
        time.sleep(logManager.initialTaskDelayMs());
        assertEquals(6, log.numberOfSegments(), "Now there should be exactly 6 segments");
        time.sleep(log.config().fileDeleteDelayMs + 1);

        // there should be a log file, two indexes (the txn index is created lazily),
        // and a producer snapshot file per segment, and the leader epoch checkpoint.
        assertEquals(log.numberOfSegments() * 4 + 1, log.dir().list().length, "Files should have been deleted");
        assertEquals(0, readLog(log, offset + 1).records.sizeInBytes(), "Should get empty fetch off new log.");
        assertThrows(OffsetOutOfRangeException.class, () -> readLog(log, 0));
        // log should still be appendable
        log.appendAsLeader(LogTestUtils.singletonRecords("test".getBytes()), 0);
    }

    /**
     * Ensures that LogManager doesn't run on logs with cleanup.policy=compact,delete
     * LogCleaner.CleanerThread handles all logs where compaction is enabled.
     */
    @Test
    public void testDoesntCleanLogsWithCompactDeletePolicy() throws IOException {
        testDoesntCleanLogs(TopicConfig.CLEANUP_POLICY_COMPACT + "," + TopicConfig.CLEANUP_POLICY_DELETE);
    }

    /**
     * Ensures that LogManager doesn't run on logs with cleanup.policy=compact
     * LogCleaner.CleanerThread handles all logs where compaction is enabled.
     */
    @Test
    public void testDoesntCleanLogsWithCompactPolicy() throws IOException {
        testDoesntCleanLogs(TopicConfig.CLEANUP_POLICY_COMPACT);
    }

    private void testDoesntCleanLogs(String policy) throws IOException {
        logManager.shutdown();
        ConfigRepository configRepository = MockConfigRepository.forTopic(NAME, TopicConfig.CLEANUP_POLICY_CONFIG, policy);

        logManager = createLogManager(configRepository);
        UnifiedLog log = logManager.getOrCreateLog(new TopicPartition(NAME, 0), Optional.empty());
        for (int i = 0; i < 200; i++) {
            MemoryRecords set = LogTestUtils.singletonRecords("test".getBytes(), "test".getBytes());
            log.appendAsLeader(set, 0);
        }

        int numSegments = log.numberOfSegments();
        assertTrue(log.numberOfSegments() > 1, "There should be more than one segment now.");

        log.logSegments().forEach(s -> s.log().file().setLastModified(time.milliseconds()));

        time.sleep(MAX_LOG_AGE_MS + 1);
        assertEquals(numSegments, log.numberOfSegments(), "number of segments shouldn't have changed");
    }

    /**
     * Test that flush is invoked by the background scheduler thread.
     */
    @Test
    public void testTimeBasedFlush() throws Exception {
        logManager.shutdown();
        ConfigRepository configRepository = MockConfigRepository.forTopic(NAME, TopicConfig.FLUSH_MS_CONFIG, "1000");

        logManager = createLogManager(configRepository);
        logManager.startup(Set.of());
        UnifiedLog log = logManager.getOrCreateLog(new TopicPartition(NAME, 0), Optional.empty());
        long lastFlush = log.lastFlushTime();
        for (int i = 0; i < 200; i++) {
            MemoryRecords set = LogTestUtils.singletonRecords("test".getBytes());
            log.appendAsLeader(set, 0);
        }
        time.sleep(logManager.initialTaskDelayMs());
        assertTrue(lastFlush != log.lastFlushTime(), "Time based flush should have been triggered");
    }

    /**
     * Test that new logs that are created are assigned to the least loaded log directory
     */
    @Test
    public void testLeastLoadedAssignment() throws IOException {
        // create a log manager with multiple data directories
        List<File> dirs = List.of(TestUtils.tempDirectory(),
                TestUtils.tempDirectory(),
                TestUtils.tempDirectory());
        logManager.shutdown();
        logManager = createLogManager(dirs);

        // verify that logs are always assigned to the least loaded partition
        for (int partition = 0; partition < 20; partition++) {
            logManager.getOrCreateLog(new TopicPartition("test", partition), Optional.empty());
            assertEquals(partition + 1, logManager.allLogs().size(), "We should have created the right number of logs");
            List<Integer> counts = logManager.allLogs().stream()
                    .collect(Collectors.groupingBy(log -> log.dir().getParent()))
                    .values().stream()
                    .map(List::size)
                    .toList();
            int max = counts.stream().mapToInt(Integer::intValue).max().orElse(0);
            int min = counts.stream().mapToInt(Integer::intValue).min().orElse(0);
            assertTrue(max <= min + 1, "Load should balance evenly");
        }
    }

    /**
     * Tests that the log manager skips the remote-log-index-cache directory when loading the logs from disk
     */
    @Test
    public void testLoadLogsSkipRemoteIndexCache() throws Exception {
        File logDir = TestUtils.tempDirectory();
        File remoteIndexCache = new File(logDir, RemoteIndexCache.DIR_NAME);
        remoteIndexCache.mkdir();
        logManager = createLogManager(List.of(logDir));
        logManager.loadLogs(LOG_CONFIG, Map.of(), unifiedLog ->  false);
    }

    @Test
    public void testLoadLogRenameLogThatShouldBeStray() throws IOException {
        AtomicInteger invokedCount = new AtomicInteger(0);
        File logDir = TestUtils.tempDirectory();
        logManager = createLogManager(List.of(logDir));

        String testTopic = "test-stray-topic";
        TopicPartition testTopicPartition = new TopicPartition(testTopic, 0);
        UnifiedLog log = logManager.getOrCreateLog(testTopicPartition, Optional.of(Uuid.randomUuid()));
        Function<UnifiedLog, Boolean> providedIsStray = unifiedLog -> {
            invokedCount.incrementAndGet();
            return true;
        };

        logManager.loadLog(log.dir(), true, Map.of(), Map.of(), LOG_CONFIG, Map.of(), new ConcurrentHashMap<>(), providedIsStray);
        assertEquals(1, invokedCount.get());
        assertTrue(
                Arrays.stream(logDir.listFiles())
                        .anyMatch(f -> f.getName().startsWith(testTopic) && f.getName().endsWith(UnifiedLog.STRAY_DIR_SUFFIX))
        );
    }

    /**
     * Test that it is not possible to open two log managers using the same data directory
     */
    @Test
    public void testTwoLogManagersUsingSameDirFails() {
        assertThrows(KafkaException.class, this::createLogManager);
    }

    /**
     * Test that recovery points are correctly written out to disk
     */
    @Test
    public void testCheckpointRecoveryPoints() throws IOException {
        verifyCheckpointRecovery(List.of(new TopicPartition("test-a", 1), new TopicPartition("test-b", 1)), logManager, logDir);
    }

    /**
     * Test that recovery points directory checking works with trailing slash
     */
    @Test
    public void testRecoveryDirectoryMappingWithTrailingSlash() throws Exception {
        logManager.shutdown();
        logManager = createLogManager(List.of(new File(TestUtils.tempDirectory().getAbsolutePath() + File.separator)));
        logManager.startup(Set.of());
        verifyCheckpointRecovery(List.of(new TopicPartition("test-a", 1)), logManager, logManager.liveLogDirs().iterator().next());
    }

    /**
     * Test that recovery points directory checking works with relative directory
     */
    @Test
    public void testRecoveryDirectoryMappingWithRelativeDirectory() throws Exception {
        logManager.shutdown();
        logManager = createLogManager(List.of(new File("data", logDir.getName()).getAbsoluteFile()));
        logManager.startup(Set.of());
        verifyCheckpointRecovery(List.of(new TopicPartition("test-a", 1)), logManager, logManager.liveLogDirs().iterator().next());
    }

    private void verifyCheckpointRecovery(List<TopicPartition> topicPartitions, LogManager logManager, File logDir) throws IOException {
        List<UnifiedLog> logs = new ArrayList<>();
        for (TopicPartition tp : topicPartitions) {
            logs.add(logManager.getOrCreateLog(tp, Optional.empty()));
        }
        for (UnifiedLog log : logs) {
            for (int i = 0; i < 50; i++) {
                log.appendAsLeader(LogTestUtils.singletonRecords("test".getBytes()), 0);
            }
            log.flush(false);
        }

        logManager.checkpointLogRecoveryOffsets();
        Map<TopicPartition, Long> checkpoints = new OffsetCheckpointFile(new File(logDir, LogManager.RECOVERY_POINT_CHECKPOINT_FILE), null).read();

        for (int i = 0; i < topicPartitions.size(); i++) {
            TopicPartition tp = topicPartitions.get(i);
            UnifiedLog log = logs.get(i);
            assertEquals(checkpoints.get(tp), log.recoveryPoint(), "Recovery point should equal checkpoint");
        }
    }

    private LogManager createLogManager() throws IOException {
        return createLogManager(List.of(this.logDir));
    }

    private LogManager createLogManager(List<File> logDirs) throws IOException {
        return createLogManager(logDirs, new MockConfigRepository(), 1);
    }

    private LogManager createLogManager(ConfigRepository configRepository) throws IOException {
        return createLogManager(List.of(this.logDir), configRepository, 1);
    }

    private LogManager createLogManager(List<File> logDirs, int recoveryThreadsPerDataDir) throws IOException {
        return createLogManager(logDirs, new MockConfigRepository(), recoveryThreadsPerDataDir);
    }

    private LogManager createLogManager(List<File> logDirs, ConfigRepository configRepository, int recoveryThreadsPerDataDir) throws IOException {
        return LogTestUtils.createLogManager(
                logDirs,
                LOG_CONFIG,
                configRepository,
                this.time,
                recoveryThreadsPerDataDir,
                INITIAL_TASK_DELAY_MS);
    }

    @Test
    public void testFileReferencesAfterAsyncDelete() throws IOException {
        UnifiedLog log = logManager.getOrCreateLog(new TopicPartition(NAME, 0), Optional.empty());
        LogSegment activeSegment = log.activeSegment();
        String logName = activeSegment.log().file().getName();
        String indexName = activeSegment.offsetIndex().file().getName();
        String timeIndexName = activeSegment.timeIndex().file().getName();
        String txnIndexName = activeSegment.txnIndex().file().getName();
        List<File> indexFilesOnDiskBeforeDelete = Arrays.stream(activeSegment.log().file().getParentFile().listFiles())
                .filter(f -> f.getName().endsWith("index"))
                .toList();

        UnifiedLog removedLog = logManager.asyncDelete(new TopicPartition(NAME, 0)).get();
        LogSegment removedSegment = removedLog.activeSegment();
        List<File> indexFilesAfterDelete = List.of(removedSegment.offsetIndexFile(), removedSegment.timeIndexFile(),
                removedSegment.txnIndex().file());

        assertEquals(new File(removedLog.dir(), logName), removedSegment.log().file());
        assertEquals(new File(removedLog.dir(), indexName), removedSegment.offsetIndexFile());
        assertEquals(new File(removedLog.dir(), timeIndexName), removedSegment.timeIndexFile());
        assertEquals(new File(removedLog.dir(), txnIndexName), removedSegment.txnIndex().file());

        // Try to detect the case where a new index type was added and we forgot to update the pointer
        // This will only catch cases where the index file is created eagerly instead of lazily
        indexFilesOnDiskBeforeDelete.forEach(fileBeforeDelete -> {
            Optional<File> fileInIndex = indexFilesAfterDelete.stream()
                    .filter(f -> f.getName().equals(fileBeforeDelete.getName()))
                    .findFirst();
            assertEquals(Optional.of(fileBeforeDelete.getName()), fileInIndex.map(File::getName),
                    "Could not find index file " + fileBeforeDelete.getName() + " in indexFilesAfterDelete");
            assertNotEquals("File reference was not updated in index", fileBeforeDelete.getAbsolutePath(),
                    fileInIndex.get().getAbsolutePath());
        });

        time.sleep(logManager.initialTaskDelayMs());
        assertTrue(logManager.hasLogsToBeDeleted(), "Logs deleted too early");
        time.sleep(logManager.currentDefaultConfig().fileDeleteDelayMs - logManager.initialTaskDelayMs());
        assertFalse(logManager.hasLogsToBeDeleted(), "Logs not deleted");
    }

    @Test
    public void testCreateAndDeleteOverlyLongTopic() throws IOException {
        String invalidTopicName = String.join("", Collections.nCopies(253, "x"));
        logManager.getOrCreateLog(new TopicPartition(invalidTopicName, 0), Optional.empty());
        logManager.asyncDelete(new TopicPartition(invalidTopicName, 0));
    }

    @Test
    public void testCheckpointForOnlyAffectedLogs() throws IOException {
        List<TopicPartition> tps = List.of(
                new TopicPartition("test-a", 0),
                new TopicPartition("test-a", 1),
                new TopicPartition("test-a", 2),
                new TopicPartition("test-b", 0),
                new TopicPartition("test-b", 1));

        List<UnifiedLog> allLogs = new ArrayList<>();
        for (TopicPartition tp : tps) {
            allLogs.add(logManager.getOrCreateLog(tp, Optional.empty()));
        }
        for (UnifiedLog log : allLogs) {
            for (int i = 0; i < 50; i++) {
                log.appendAsLeader(LogTestUtils.singletonRecords("test".getBytes()), 0);
            }
            log.flush(false);
        }

        logManager.checkpointRecoveryOffsetsInDir(logDir);

        Map<TopicPartition, Long> checkpoints = new OffsetCheckpointFile(new File(logDir, LogManager.RECOVERY_POINT_CHECKPOINT_FILE), null).read();

        for (int i = 0; i < tps.size(); i++) {
            TopicPartition tp = tps.get(i);
            UnifiedLog log = allLogs.get(i);
            assertEquals(checkpoints.get(tp), log.recoveryPoint(),
                    "Recovery point should equal checkpoint");
        }
    }

    private FetchDataInfo readLog(UnifiedLog log, long offset) throws IOException {
        return readLog(log, offset, 1024);
    }

    private FetchDataInfo readLog(UnifiedLog log, long offset, int maxLength) throws IOException {
        return log.read(offset, maxLength, FetchIsolation.LOG_END, true);
    }

    /**
     * Test when a configuration of a topic is updated while its log is getting initialized,
     * the config is refreshed when log initialization is finished.
     */
    @Test
    public void testTopicConfigChangeUpdatesLogConfig() throws IOException {
        logManager.shutdown();
        ConfigRepository spyConfigRepository = spy(new MockConfigRepository());
        logManager = createLogManager(spyConfigRepository);
        LogManager spyLogManager = spy(logManager);
        UnifiedLog mockLog = mock(UnifiedLog.class);

        String testTopicOne = "test-topic-one";
        String testTopicTwo = "test-topic-two";
        TopicPartition testTopicOnePartition = new TopicPartition(testTopicOne, 1);
        TopicPartition testTopicTwoPartition = new TopicPartition(testTopicTwo, 1);

        spyLogManager.initializingLog(testTopicOnePartition);
        spyLogManager.initializingLog(testTopicTwoPartition);

        spyLogManager.topicConfigUpdated(testTopicOne);

        spyLogManager.finishedInitializingLog(testTopicOnePartition, Optional.of(mockLog));
        spyLogManager.finishedInitializingLog(testTopicTwoPartition, Optional.of(mockLog));

        // testTopicOne configs loaded again due to the update
        verify(spyLogManager).initializingLog(eq(testTopicOnePartition));
        verify(spyLogManager).finishedInitializingLog(eq(testTopicOnePartition), any());
        verify(spyConfigRepository, times(1)).topicConfig(testTopicOne);

        // testTopicTwo configs not loaded again since there was no update
        verify(spyLogManager).initializingLog(eq(testTopicTwoPartition));
        verify(spyLogManager).finishedInitializingLog(eq(testTopicTwoPartition), any());
        verify(spyConfigRepository, never()).topicConfig(testTopicTwo);
    }

    /**
     * Test if an error occurs when creating log, log manager removes corresponding
     * topic partition from the list of initializing partitions and no configs are retrieved.
     */
    @Test
    public void testConfigChangeGetsCleanedUp() throws IOException {
        logManager.shutdown();
        ConfigRepository spyConfigRepository = spy(new MockConfigRepository());
        logManager = createLogManager(spyConfigRepository);
        LogManager spyLogManager = spy(logManager);

        TopicPartition testTopicPartition = new TopicPartition("test-topic", 1);
        spyLogManager.initializingLog(testTopicPartition);
        spyLogManager.finishedInitializingLog(testTopicPartition, Optional.empty());

        assertTrue(logManager.partitionsInitializing().isEmpty());
        verify(spyConfigRepository, never()).topicConfig(testTopicPartition.topic());
    }

    /**
     * Test when a broker configuration change happens all logs in process of initialization
     * pick up latest config when finished with initialization.
     */
    @Test
    public void testBrokerConfigChangeDeliveredToAllLogs() throws IOException {
        logManager.shutdown();
        ConfigRepository spyConfigRepository = spy(new MockConfigRepository());
        logManager = createLogManager(spyConfigRepository);
        LogManager spyLogManager = spy(logManager);
        UnifiedLog mockLog = mock(UnifiedLog.class);

        String testTopicOne = "test-topic-one";
        String testTopicTwo = "test-topic-two";
        TopicPartition testTopicOnePartition = new TopicPartition(testTopicOne, 1);
        TopicPartition testTopicTwoPartition = new TopicPartition(testTopicTwo, 1);

        spyLogManager.initializingLog(testTopicOnePartition);
        spyLogManager.initializingLog(testTopicTwoPartition);

        spyLogManager.brokerConfigUpdated();

        spyLogManager.finishedInitializingLog(testTopicOnePartition, Optional.of(mockLog));
        spyLogManager.finishedInitializingLog(testTopicTwoPartition, Optional.of(mockLog));

        verify(spyConfigRepository, times(1)).topicConfig(testTopicOne);
        verify(spyConfigRepository, times(1)).topicConfig(testTopicTwo);
    }

    /**
     * Test when compact is removed that cleaning of the partitions is aborted.
     */
    @Test
    public void testTopicConfigChangeStopCleaningIfCompactIsRemoved() throws IOException {
        logManager.shutdown();
        logManager = createLogManager(new MockConfigRepository());
        LogManager spyLogManager = spy(logManager);

        String topic = "topic";
        TopicPartition tp0 = new TopicPartition(topic, 0);
        TopicPartition tp1 = new TopicPartition(topic, 1);

        Properties oldProperties = new Properties();
        oldProperties.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
        LogConfig oldLogConfig = LogConfig.fromProps(LOG_CONFIG.originals(), oldProperties);

        UnifiedLog log0 = spyLogManager.getOrCreateLog(tp0, Optional.empty());
        log0.updateConfig(oldLogConfig);
        UnifiedLog log1 = spyLogManager.getOrCreateLog(tp1, Optional.empty());
        log1.updateConfig(oldLogConfig);

        assertEquals(List.of(log0, log1), spyLogManager.logsByTopic(topic));

        Properties newProperties = new Properties();
        newProperties.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE);

        spyLogManager.updateTopicConfig(topic, newProperties, false, false);

        assertTrue(log0.config().delete);
        assertTrue(log1.config().delete);
        assertFalse(log0.config().compact);
        assertFalse(log1.config().compact);

        verify(spyLogManager, times(1)).topicConfigUpdated(topic);
        verify(spyLogManager, times(1)).abortCleaning(tp0);
        verify(spyLogManager, times(1)).abortCleaning(tp1);
    }

    /**
     * Test even if no log is getting initialized, if config change events are delivered
     * things continue to work correctly. This test should not throw.
     *
     * This makes sure that events can be delivered even when no log is getting initialized.
     */
    @Test
    public void testConfigChangesWithNoLogGettingInitialized() {
        logManager.brokerConfigUpdated();
        logManager.topicConfigUpdated("test-topic");
        assertTrue(logManager.partitionsInitializing().isEmpty());
    }

    private void appendRecordsToLog(MockTime time, File parentLogDir, int partitionId, BrokerTopicStats brokerTopicStats, int expectedSegmentsPerLog) throws IOException {
        File tpFile = new File(parentLogDir, NAME + "-" + partitionId);
        int segmentBytes = 1024;

        // calculate numMessages to append to logs. It'll create "expectedSegmentsPerLog" log segments with segment.bytes=1024

        try (UnifiedLog log = UnifiedLog.create(tpFile, LOG_CONFIG, 0L, 0L, time.scheduler, brokerTopicStats, time,
                5 * 60 * 1000, new ProducerStateManagerConfig(TransactionLogConfig.PRODUCER_ID_EXPIRATION_MS_DEFAULT, false), TransactionLogConfig.PRODUCER_ID_EXPIRATION_CHECK_INTERVAL_MS_DEFAULT, new LogDirFailureChannel(10), false,
                Optional.empty())) {
            assertTrue(expectedSegmentsPerLog > 0);
            // Create a sample record to calculate size
            MemoryRecords sampleRecord = LogTestUtils.singletonRecords("test".getBytes(), time.milliseconds());
            int numMessages = (int) Math.floor(segmentBytes * expectedSegmentsPerLog / sampleRecord.sizeInBytes());
            for (int i = 0; i < numMessages; i++) {
                MemoryRecords createRecord = LogTestUtils.singletonRecords("test".getBytes(), time.milliseconds());
                log.appendAsLeader(createRecord, 0);
            }

            assertEquals(expectedSegmentsPerLog, log.numberOfSegments());
        }
    }

    private void verifyRemainingLogsToRecoverMetric(LogManager spyLogManager, Map<String, Integer> expectedParams) {
        String logManagerClassName = LogManager.class.getSimpleName();
        // get all `remainingLogsToRecover` metrics
        List<Gauge<Integer>> logMetrics = KafkaYammerMetrics.defaultRegistry().allMetrics().entrySet().stream()
                .filter(entry -> entry.getKey().getType().equals(logManagerClassName) && entry.getKey().getName().equals("remainingLogsToRecover"))
                .map(entry -> (Gauge<Integer>) entry.getValue())
                .toList();

        assertEquals(expectedParams.size(), logMetrics.size());

        ArgumentCaptor<String> capturedPath = ArgumentCaptor.forClass(String.class);

        int expectedCallTimes = expectedParams.values().stream()
                .mapToInt(Integer::intValue)
                .sum();
        verify(spyLogManager, times(expectedCallTimes)).decNumRemainingLogs(any(ConcurrentMap.class), capturedPath.capture());

        List<String> paths = capturedPath.getAllValues();
        expectedParams.forEach((path, totalLogs) -> {
            // make sure each path is called "totalLogs" times, which means it is decremented to 0 in the end
            assertEquals(totalLogs, Collections.frequency(paths, path));
        });

        // expected the end value is 0
        logMetrics.forEach(gauge -> assertEquals(0, gauge.value()));
    }

    private void verifyRemainingSegmentsToRecoverMetric(List<File> logDirs,
                                                        int recoveryThreadsPerDataDir,
                                                        ConcurrentHashMap<String, Integer> mockMap,
                                                        Map<String, Integer> expectedParams) {
        String logManagerClassName = LogManager.class.getSimpleName();
        // get all `remainingSegmentsToRecover` metrics
        List<Gauge<Integer>> logSegmentMetrics = KafkaYammerMetrics.defaultRegistry().allMetrics().entrySet().stream()
            .filter(entry -> entry.getKey().getType().equals(logManagerClassName) && entry.getKey().getName().equals("remainingSegmentsToRecover"))
            .map(entry -> (Gauge<Integer>) entry.getValue())
            .toList();

        // expected each log dir has 1 metrics for each thread
        assertEquals(recoveryThreadsPerDataDir * logDirs.size(), logSegmentMetrics.size());

        // expected the end value is 0
        logSegmentMetrics.forEach(gauge -> assertEquals(0, gauge.value()));

        // Only verify mockMap calls if it's actually a mock/spy
        if (mockMap != null && mockingDetails(mockMap).isMock()) {
            ArgumentCaptor<String> capturedThreadName = ArgumentCaptor.forClass(String.class);
            ArgumentCaptor<Integer> capturedNumRemainingSegments = ArgumentCaptor.forClass(Integer.class);

            // Since we'll update numRemainingSegments from totalSegments to 0 for each thread, so we need to add 1 here
            int expectedCallTimes = expectedParams.values().stream()
                    .mapToInt(num -> num + 1)
                    .sum();
            verify(mockMap, times(expectedCallTimes)).put(capturedThreadName.capture(), capturedNumRemainingSegments.capture());

            List<String> threadNames = capturedThreadName.getAllValues();
            List<Integer> numRemainingSegments = capturedNumRemainingSegments.getAllValues();

            expectedParams.forEach((threadName, totalSegments) -> {
                // make sure we update the numRemainingSegments from totalSegments to 0 in order for each thread
                int expectedCurRemainingSegments = totalSegments + 1;
                for (int i = 0; i < threadNames.size(); i++) {
                    if (threadNames.get(i).contains(threadName)) {
                        expectedCurRemainingSegments -= 1;
                        assertEquals(expectedCurRemainingSegments, numRemainingSegments.get(i));
                    }
                }
                assertEquals(0, expectedCurRemainingSegments);
            });
        }
    }

    private void verifyLogRecoverMetricsRemoved() {
        String logManagerClassName = LogManager.class.getSimpleName();
        // get all `remainingLogsToRecover` metrics
        Set<MetricName> logMetrics = KafkaYammerMetrics.defaultRegistry().allMetrics().keySet().stream()
                .filter(metric -> metric.getType().equals(logManagerClassName) && metric.getName().equals("remainingLogsToRecover"))
                .collect(Collectors.toSet());

        assertTrue(logMetrics.isEmpty());

        // get all `remainingSegmentsToRecover` metrics
        Set<MetricName> logSegmentMetrics = KafkaYammerMetrics.defaultRegistry().allMetrics().keySet().stream()
                .filter(metric -> metric.getType().equals(logManagerClassName) && metric.getName().equals("remainingSegmentsToRecover"))
                .collect(Collectors.toSet());

        assertTrue(logSegmentMetrics.isEmpty());
    }

    @Test
    public void testLogRecoveryMetrics() throws Exception {
        logManager.shutdown();
        File logDir1 = TestUtils.tempDirectory();
        File logDir2 = TestUtils.tempDirectory();
        List<File> logDirs = List.of(logDir1, logDir2);
        int recoveryThreadsPerDataDir = 2;
        // create logManager with expected recovery thread number
        logManager = createLogManager(logDirs, recoveryThreadsPerDataDir);
        LogManager spyLogManager = spy(logManager);

        assertEquals(2, spyLogManager.liveLogDirs().size());

        MockTime mockTime = new MockTime();
        BrokerTopicStats mockBrokerTopicStats = mock(BrokerTopicStats.class);
        int expectedSegmentsPerLog = 2;

        // create log segments for log recovery - need enough partitions to use all recovery threads
        // With 2 log dirs and 2 recovery threads per dir, we need 4 partitions minimum
        appendRecordsToLog(mockTime, logDir1, 0, mockBrokerTopicStats, expectedSegmentsPerLog);
        appendRecordsToLog(mockTime, logDir1, 1, mockBrokerTopicStats, expectedSegmentsPerLog);
        appendRecordsToLog(mockTime, logDir2, 2, mockBrokerTopicStats, expectedSegmentsPerLog);
        appendRecordsToLog(mockTime, logDir2, 3, mockBrokerTopicStats, expectedSegmentsPerLog);

        // intercept loadLog method to pass expected parameter to do log recovery
        doAnswer(invocation -> {
            File dir = invocation.getArgument(0);
            Map<String, LogConfig> topicConfigOverrides = invocation.getArgument(5);
            // Get the real numRemainingSegments map - it should be the same one we captured from addLogRecoveryMetrics
            ConcurrentMap<String, Integer> realMap = invocation.getArgument(6);

            TopicPartition topicPartition = UnifiedLog.parseTopicPartitionName(dir);
            LogConfig config = topicConfigOverrides.getOrDefault(topicPartition.topic(), LOG_CONFIG);

            return UnifiedLog.create(
                    dir,
                    config,
                    0,
                    0,
                    mock(Scheduler.class),
                    mockBrokerTopicStats,
                    mockTime,
                    5 * 60 * 1000,
                    new ProducerStateManagerConfig(5 * 60 * 1000, false),
                    TransactionLogConfig.PRODUCER_ID_EXPIRATION_CHECK_INTERVAL_MS_DEFAULT,
                    mock(LogDirFailureChannel.class),
                    false, // not clean shutdown
                    Optional.empty(),
                    realMap, // Pass the real map so UnifiedLog updates it and the gauges can read from it
                    false,
                    LogOffsetsListener.NO_OP_OFFSETS_LISTENER);

        }).when(spyLogManager).loadLog(any(File.class), any(Boolean.class), anyMap(), any(),
            any(LogConfig.class), anyMap(), any(ConcurrentMap.class), any(Function.class));

        // do nothing for removeLogRecoveryMetrics for metrics verification
        doNothing().when(spyLogManager).removeLogRecoveryMetrics();

        // start the logManager to do log recovery
        spyLogManager.startup(Set.of());

        // make sure log recovery metrics are added and removed
        verify(spyLogManager, times(1)).addLogRecoveryMetrics(any(ConcurrentMap.class), any(ConcurrentMap.class));
        verify(spyLogManager, times(1)).removeLogRecoveryMetrics();
        // Verify loadLog was called for all 4 partitions
        verify(spyLogManager, times(4)).loadLog(any(File.class), any(Boolean.class), anyMap(), any(),
                any(LogConfig.class), anyMap(), any(ConcurrentMap.class), any(Function.class));

        // expected 2 logs in each log dir since we created 4 partitions (2 per dir)
        Map<String, Integer> expectedRemainingLogsParams = Map.of(
                logDir1.getAbsolutePath(), 2,
                logDir2.getAbsolutePath(), 2);
        verifyRemainingLogsToRecoverMetric(spyLogManager, expectedRemainingLogsParams);

        Map<String, Integer> expectedRemainingSegmentsParams = Map.of(
                logDir1.getAbsolutePath(), expectedSegmentsPerLog * 2,  // 2 logs per dir
                logDir2.getAbsolutePath(), expectedSegmentsPerLog * 2);

        // Pass null for the mock map parameter since we're using the real map
        verifyRemainingSegmentsToRecoverMetric(logDirs, recoveryThreadsPerDataDir, null, expectedRemainingSegmentsParams);
    }

    @Test
    public void testLogRecoveryMetricsShouldBeRemovedAfterLogRecovered() throws Exception {
        logManager.shutdown();
        File logDir1 = TestUtils.tempDirectory();
        File logDir2 = TestUtils.tempDirectory();
        List<File> logDirs = List.of(logDir1, logDir2);
        int recoveryThreadsPerDataDir = 2;
        // create logManager with expected recovery thread number
        logManager = createLogManager(logDirs, recoveryThreadsPerDataDir);
        LogManager spyLogManager = spy(logManager);

        assertEquals(2, spyLogManager.liveLogDirs().size());

        // start the logManager to do log recovery
        spyLogManager.startup(Set.of());

        // make sure log recovery metrics are added and removed once
        verify(spyLogManager, times(1)).addLogRecoveryMetrics(any(ConcurrentMap.class), any(ConcurrentMap.class));
        verify(spyLogManager, times(1)).removeLogRecoveryMetrics();

        verifyLogRecoverMetricsRemoved();
    }

    @Test
    public void testMetricsExistWhenLogIsRecreatedBeforeDeletion() throws IOException {
        String topicName = "metric-test";
        Supplier<Set<MetricName>> logMetrics = () -> KafkaYammerMetrics.defaultRegistry().allMetrics().keySet().stream()
                .filter(metric -> metric.getType().equals("Log") && metric.getScope().contains(topicName))
                .collect(Collectors.toSet());

        TopicPartition tp = new TopicPartition(topicName, 0);
        String metricTag = "topic=" + tp.topic() + ",partition=" + tp.partition();

        // Create the Log and assert that the metrics are present
        logManager.getOrCreateLog(tp, Optional.empty());
        verifyMetrics(logMetrics.get(), metricTag);

        // Trigger the deletion and assert that the metrics have been removed
        UnifiedLog removedLog = logManager.asyncDelete(tp).get();
        assertTrue(logMetrics.get().isEmpty());

        // Recreate the Log and assert that the metrics are present
        logManager.getOrCreateLog(tp, Optional.empty());
        verifyMetrics(logMetrics.get(), metricTag);

        // Advance time past the file deletion delay and assert that the removed log has been deleted but the metrics
        // are still present
        time.sleep(LOG_CONFIG.fileDeleteDelayMs + 1);
        assertTrue(removedLog.logSegments().isEmpty());
        verifyMetrics(logMetrics.get(), metricTag);
    }

    private static void verifyMetrics(Set<MetricName> logMetrics, String metricTag) {
        assertEquals(LogMetricNames.ALL_METRIC_NAMES.size(), logMetrics.size());
        logMetrics.forEach(metric -> assertTrue(metric.getMBeanName().contains(metricTag)));
    }

    @Test
    public void testLogManagerMetrics() throws IOException {
        KafkaYammerMetrics.defaultRegistry().allMetrics().keySet().forEach(metricName ->
            KafkaYammerMetrics.defaultRegistry().removeMetric(metricName));
        logManager.shutdown();
        logManager = createLogManager(List.of(logDir, logDir2));

        Function<String, Gauge<Integer>> metric = filter -> (Gauge<Integer>) KafkaYammerMetrics.defaultRegistry().allMetrics().entrySet().stream()
                .filter(entry -> entry.getKey().getName().contains(filter))
                .map(Map.Entry::getValue)
                .findFirst()
                .get();
        BiFunction<String, File, Gauge<Integer>> logDirMetric = (filter, logDir) -> (Gauge<Integer>) KafkaYammerMetrics.defaultRegistry().allMetrics().entrySet().stream()
                .filter(entry -> entry.getKey().getName().contains(filter))
                .filter(entry -> entry.getKey().getScope().contains(logDir.getAbsolutePath()))
                .map(Map.Entry::getValue)
                .findFirst()
                .get();
        // expecting 6 metrics:
        // - OfflineLogDirectoryCount
        // - CordonedLogDirectoryCount
        // - LogDirectoryOffline per log dir
        // - LogDirectoryCordoned per log dir
        assertEquals(6, allMetrics().size());
        assertEquals(0, metric.apply("OfflineLogDirectoryCount").value());
        assertEquals(0, metric.apply("CordonedLogDirectoryCount").value());
        assertEquals(0, logDirMetric.apply("LogDirectoryOffline", logDir).value());
        assertEquals(0, logDirMetric.apply("LogDirectoryOffline", logDir2).value());
        assertEquals(0, logDirMetric.apply("LogDirectoryCordoned", logDir).value());
        assertEquals(0, logDirMetric.apply("LogDirectoryCordoned", logDir2).value());

        logManager.updateCordonedLogDirs(Set.of(logDir.getAbsolutePath()));
        assertEquals(1, metric.apply("CordonedLogDirectoryCount").value());
        assertEquals(1, logDirMetric.apply("LogDirectoryCordoned", logDir).value());
        assertEquals(0, logDirMetric.apply("LogDirectoryCordoned", logDir2).value());
        logManager.updateCordonedLogDirs(Set.of(logDir.getAbsolutePath(), logDir2.getAbsolutePath()));
        assertEquals(2, metric.apply("CordonedLogDirectoryCount").value());
        assertEquals(1, logDirMetric.apply("LogDirectoryCordoned", logDir).value());
        assertEquals(1, logDirMetric.apply("LogDirectoryCordoned", logDir2).value());

        logManager.handleLogDirFailure(logDir.getAbsolutePath());
        assertEquals(1, metric.apply("OfflineLogDirectoryCount").value());
        assertEquals(1, logDirMetric.apply("LogDirectoryOffline", logDir).value());
        assertEquals(0, logDirMetric.apply("LogDirectoryOffline", logDir2).value());

        try {
            logManager.shutdown();
        } catch (IOException t) {
            // ignore
        } finally {
            logManager = null;
        }
    }

    private static Set<MetricName> allMetrics() {
        return KafkaYammerMetrics.defaultRegistry().allMetrics().keySet().stream()
                .filter(metric -> metric.getType().contains("LogManager"))
                .collect(Collectors.toSet());
    }

    @Test
    public void testMetricsAreRemovedWhenMovingCurrentToFutureLog() throws Exception {
        File dir1 = TestUtils.tempDirectory();
        File dir2 = TestUtils.tempDirectory();
        logManager = createLogManager(List.of(dir1, dir2));
        logManager.startup(Set.of());

        String topicName = "future-log";
        Supplier<Set<MetricName>> logMetrics = () ->  KafkaYammerMetrics.defaultRegistry().allMetrics().keySet().stream()
                .filter(metric -> metric.getType().equals("Log") && metric.getScope().contains(topicName))
                .collect(Collectors.toSet());

        TopicPartition tp = new TopicPartition(topicName, 0);
        String metricTag = "topic=" + tp.topic() + ",partition=" + tp.partition();

        // Create the current and future logs and verify that metrics are present for both current and future logs
        logManager.maybeUpdatePreferredLogDir(tp, dir1.getAbsolutePath());
        logManager.getOrCreateLog(tp, Optional.empty());
        logManager.maybeUpdatePreferredLogDir(tp, dir2.getAbsolutePath());
        logManager.getOrCreateLog(tp, false, true, Optional.empty());
        verifyMetrics(2, logMetrics.get(), metricTag);

        // Replace the current log with the future one and verify that only one set of metrics are present
        logManager.replaceCurrentWithFutureLog(tp);
        verifyMetrics(1, logMetrics.get(), metricTag);
        // the future log is gone, so we have to make sure the metrics gets gone also.
        assertEquals(0, logMetrics.get().stream().filter(m -> m.getMBeanName().contains("is-future")).count());

        // Trigger the deletion of the former current directory and verify that one set of metrics is still present
        time.sleep(LOG_CONFIG.fileDeleteDelayMs + 1);
        verifyMetrics(1,  logMetrics.get(), metricTag);
    }

    private void verifyMetrics(int logCount, Set<MetricName> logMetrics, String metricTag) {
        assertEquals(LogMetricNames.ALL_METRIC_NAMES.size() * logCount, logMetrics.size());
        logMetrics.forEach(metric -> assertTrue(metric.getMBeanName().contains(metricTag)));
    }

    @Test
    public void testLoadDirectoryIds() throws IOException {
        List<File> dirs = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            dirs.add(TestUtils.tempDirectory());
        }
        writeMetaProperties(dirs.get(0));
        writeMetaProperties(dirs.get(1), Optional.of(Uuid.fromString("ZwkGXjB0TvSF6mjVh6gO7Q")));
        // no meta.properties on dirs(2)
        writeMetaProperties(dirs.get(3), Optional.of(Uuid.fromString("kQfNPJ2FTHq_6Qlyyv6Jqg")));
        writeMetaProperties(dirs.get(4));

        logManager = createLogManager(dirs);

        assertFalse(logManager.directoryId(dirs.get(0).getAbsolutePath()).isPresent());
        assertTrue(logManager.directoryId(dirs.get(1).getAbsolutePath()).isPresent());
        assertEquals(Optional.of(Uuid.fromString("ZwkGXjB0TvSF6mjVh6gO7Q")), logManager.directoryId(dirs.get(1).getAbsolutePath()));
        assertEquals(Optional.empty(), logManager.directoryId(dirs.get(2).getAbsolutePath()));
        assertEquals(Optional.of(Uuid.fromString("kQfNPJ2FTHq_6Qlyyv6Jqg")), logManager.directoryId(dirs.get(3).getAbsolutePath()));
        assertTrue(logManager.directoryId(dirs.get(3).getAbsolutePath()).isPresent());
        assertEquals(2, logManager.directoryIds().size());
    }

    /**
     * Test that replaceCurrentWithFutureLog does not close the source log, preventing race conditions
     * where a concurrent read/flush could fail with ClosedChannelException.
     */
    @Test
    public void testReplaceCurrentWithFutureLogDoesNotCloseSourceLog() throws Exception {
        File logDir1 = TestUtils.tempDirectory();
        File logDir2 = TestUtils.tempDirectory();
        logManager = createLogManager(List.of(logDir1, logDir2));
        logManager.startup(Set.of());

        String topicName = "replace-log";
        TopicPartition tp = new TopicPartition(topicName, 0);
        UnifiedLog currentLog = logManager.getOrCreateLog(tp, Optional.empty());
        // Create a future log in a different directory
        logManager.maybeUpdatePreferredLogDir(tp, logDir2.getAbsolutePath());
        logManager.getOrCreateLog(tp, false, true, Optional.empty());

        // Spy on the source log to verify close() is not called
        UnifiedLog spyCurrentLog = spy(currentLog);
        // Inject the spy into the map
        Field field = LogManager.class.getDeclaredField("currentLogs");
        field.setAccessible(true);
        ConcurrentHashMap<TopicPartition, UnifiedLog> currentLogs = (ConcurrentHashMap<TopicPartition, UnifiedLog>) field.get(logManager);
        currentLogs.put(tp, spyCurrentLog);

        logManager.replaceCurrentWithFutureLog(tp);

        // Verify close() was NOT called on the source log
        verify(spyCurrentLog, never()).close();

        // Verify the source log was renamed to .delete
        assertTrue(spyCurrentLog.dir().getName().endsWith(LogFileUtils.DELETE_DIR_SUFFIX));

        // Verify that flush() can be called without error (no ClosedChannelException)
        // Mock logEndOffset > 0 to trigger actual flush (flush only happens when flushOffset > recoveryPoint)
        when(spyCurrentLog.logEndOffset()).thenReturn(100L);
        assertDoesNotThrow(() -> spyCurrentLog.flush(false));
    }

    @Test
    public void testCheckpointLogStartOffsetForRemoteTopic() throws IOException {
        logManager.shutdown();

        Properties props = new Properties();
        props.putAll(LOG_PROPS);
        props.put(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true");
        LogConfig logConfig = new LogConfig(props);
        logManager = LogTestUtils.createLogManager(
                List.of(this.logDir),
                logConfig,
                new MockConfigRepository(),
                time,
                1,
                true
        );

        File checkpointFile = new File(logDir, LogManager.LOG_START_OFFSET_CHECKPOINT_FILE);
        OffsetCheckpointFile checkpoint = new OffsetCheckpointFile(checkpointFile, null);
        TopicPartition topicPartition = new TopicPartition("test", 0);
        UnifiedLog log = logManager.getOrCreateLog(topicPartition, Optional.empty());
        long offset;
        for (int i = 0; i < 50; i++) {
            MemoryRecords set = LogTestUtils.singletonRecords("test".getBytes());
            LogAppendInfo info = log.appendAsLeader(set, 0);
            offset = info.lastOffset();
            if (offset != 0 && offset % 10 == 0) {
                log.roll();
            }
        }
        assertEquals(5, log.logSegments().size());
        log.updateHighWatermark(49);
        // simulate calls to upload 3 segments to remote storage and remove them from local-log.
        log.updateHighestOffsetInRemoteStorage(30);
        log.maybeIncrementLocalLogStartOffset(31L, LogStartOffsetIncrementReason.SegmentDeletion);
        log.deleteOldSegments();
        assertEquals(2, log.logSegments().size());

        // simulate two remote-log segment deletion
        long logStartOffset = 21L;
        log.maybeIncrementLogStartOffset(logStartOffset, LogStartOffsetIncrementReason.SegmentDeletion);
        logManager.checkpointLogStartOffsets();

        assertEquals(logStartOffset, log.logStartOffset());
        assertEquals(logStartOffset, checkpoint.read().getOrDefault(topicPartition, -1L));
    }

    @Test
    public void testCheckpointLogStartOffsetForNormalTopic() throws IOException {
        File checkpointFile = new File(logDir, LogManager.LOG_START_OFFSET_CHECKPOINT_FILE);
        OffsetCheckpointFile checkpoint = new OffsetCheckpointFile(checkpointFile, null);
        TopicPartition topicPartition = new TopicPartition("test", 0);
        UnifiedLog log = logManager.getOrCreateLog(topicPartition, Optional.empty());
        long offset;
        for (int i = 0; i < 50; i++) {
            MemoryRecords set = LogTestUtils.singletonRecords("test".getBytes());
            LogAppendInfo info = log.appendAsLeader(set, 0);
            offset = info.lastOffset();
            if (offset != 0 && offset % 10 == 0) {
                log.roll();
            }
        }
        assertEquals(5, log.logSegments().size());
        log.updateHighWatermark(49);

        long logStartOffset = 31L;
        log.maybeIncrementLogStartOffset(logStartOffset, LogStartOffsetIncrementReason.SegmentDeletion);
        logManager.checkpointLogStartOffsets();
        assertEquals(5, log.logSegments().size());
        assertEquals(logStartOffset, checkpoint.read().getOrDefault(topicPartition, -1L));

        log.deleteOldSegments();
        assertEquals(2, log.logSegments().size());
        assertEquals(logStartOffset, log.logStartOffset());

        // When you checkpoint log-start-offset after removing the segments, then there should not be any checkpoint
        logManager.checkpointLogStartOffsets();
        assertEquals(-1L, checkpoint.read().getOrDefault(topicPartition, -1L));
    }

    void writeMetaProperties(File dir) throws IOException {
        writeMetaProperties(dir, Optional.empty());
    }

    void writeMetaProperties(File dir, Optional<Uuid> directoryId) throws IOException {
        MetaProperties metaProps = new MetaProperties.Builder().
                setVersion(MetaPropertiesVersion.V0).
                setClusterId("IVT1Seu3QjacxS7oBTKhDQ").
                setNodeId(1).
                setDirectoryId(directoryId).
                build();
        PropertiesUtils.writePropertiesFile(metaProps.toProperties(),
                new File(dir, MetaPropertiesEnsemble.META_PROPERTIES_NAME).getAbsolutePath(), false);
    }

    /**
     * Test LogManager takes file lock by default and the lock is released after shutdown.
     */
    @Test
    public void testLock() throws IOException {
        File tmpLogDir = TestUtils.tempDirectory();
        LogManager tmpLogManager = createLogManager(List.of(tmpLogDir));

        try {
            // tmpLogDir.lock is acquired by tmpLogManager
            FileLock fileLock = new FileLock(new File(tmpLogDir, LogManager.LOCK_FILE_NAME));
            assertFalse(fileLock.tryLock());
        } finally {
            // tmpLogDir.lock is removed after shutdown
            tmpLogManager.shutdown();
            File f = new File(tmpLogDir, LogManager.LOCK_FILE_NAME);
            assertFalse(f.exists());
        }
    }

    /**
     * Test KafkaScheduler can be shutdown when file delete delay is set to 0.
     */
    @Test
    public void testShutdownWithZeroFileDeleteDelayMs() throws Exception {
        File tmpLogDir = TestUtils.tempDirectory();
        Properties tmpProperties = new Properties();
        tmpProperties.put(TopicConfig.FILE_DELETE_DELAY_MS_CONFIG, "0");
        KafkaScheduler scheduler = new KafkaScheduler(1, true, "log-manager-test");
        LogManager tmpLogManager = new LogManager(List.of(tmpLogDir.getAbsoluteFile()),
                List.of(),
                new MockConfigRepository(),
                new LogConfig(tmpProperties),
                new CleanerConfig(false),
                1,
                1000L,
                10000L,
                10000L,
                1000L,
                5 * 60 * 1000,
                new ProducerStateManagerConfig(TransactionLogConfig.PRODUCER_ID_EXPIRATION_MS_DEFAULT, false),
                TransactionLogConfig.PRODUCER_ID_EXPIRATION_CHECK_INTERVAL_MS_DEFAULT,
                scheduler,
                new BrokerTopicStats(),
                new LogDirFailureChannel(1),
                Time.SYSTEM,
                false,
                0,
                LogCleaner::new);

        scheduler.startup();
        tmpLogManager.startup(Set.of());
        assertTimeoutPreemptively(Duration.ofMillis(5000), () -> tmpLogManager.shutdown());
        assertTimeoutPreemptively(Duration.ofMillis(5000), () -> scheduler.shutdown());
    }

    /**
     * This test simulates an offline log directory by removing write permissions from the directory.
     * It verifies that the LogManager continues to operate without failure in this scenario.
     * For more details, refer to KAFKA-17356.
     */
    @Test
    public void testInvalidLogDirNotFailLogManager() throws IOException {
        logManager.shutdown();
        Set<PosixFilePermission> permissions = Files.getPosixFilePermissions(logDir.toPath());
        // Remove write permissions for user, group, and others
        permissions.remove(PosixFilePermission.OWNER_WRITE);
        permissions.remove(PosixFilePermission.GROUP_WRITE);
        permissions.remove(PosixFilePermission.OTHERS_WRITE);
        Files.setPosixFilePermissions(logDir.toPath(), permissions);

        try {
            logManager = assertDoesNotThrow(() -> createLogManager());
            assertEquals(0, logManager.dirLocks().size());
        } finally {
            // Add write permissions back to make file cleanup passed
            permissions.add(PosixFilePermission.OWNER_WRITE);
            permissions.add(PosixFilePermission.GROUP_WRITE);
            permissions.add(PosixFilePermission.OTHERS_WRITE);
            Files.setPosixFilePermissions(logDir.toPath(), permissions);
        }
    }

    @Test
    public void testUpdateCordonedLogDirs() throws IOException {
        logManager.shutdown();
        logManager = createLogManager(List.of(this.logDir, this.logDir2));
        assertTrue(logManager.cordonedLogDirs().isEmpty());

        Set<String> cordonedDirs = Set.of(logDir.getAbsolutePath());
        logManager.updateCordonedLogDirs(Set.of(logDir.getAbsolutePath()));
        assertEquals(cordonedDirs, logManager.cordonedLogDirs());
    }

    @Test
    public void testNextLogDirs() throws IOException {
        logManager.shutdown();
        logManager = createLogManager(List.of(this.logDir, this.logDir2));
        List<File> nextLogDirs = logManager.nextLogDirs();
        assertTrue(nextLogDirs.contains(logDir));
        assertTrue(nextLogDirs.contains(logDir2));

        logManager.updateCordonedLogDirs(Set.of(logDir.getAbsolutePath()));
        List<File> nextLogDirs2 = logManager.nextLogDirs();
        assertFalse(nextLogDirs2.contains(logDir));
        assertTrue(nextLogDirs2.contains(logDir2));

        logManager.updateCordonedLogDirs(Set.of(logDir.getAbsolutePath(), logDir2.getAbsolutePath()));
        List<File> nextLogDirs3 = logManager.nextLogDirs();
        assertFalse(nextLogDirs3.isEmpty());
    }


    @SuppressWarnings("unchecked")
    @Test
    public void testWaitForAllToComplete() throws ExecutionException, InterruptedException {
        AtomicInteger invokedCount = new AtomicInteger(0);
        Future<Boolean> success = mock(Future.class);
        when(success.get()).thenAnswer(a -> {
            invokedCount.incrementAndGet();
            return true;
        });
        Future<Boolean> failure = mock(Future.class);
        when(failure.get()).thenAnswer(a -> {
            invokedCount.incrementAndGet();
            throw new RuntimeException();
        });

        AtomicInteger failureCount = new AtomicInteger(0);
        // all futures should be evaluated
        assertFalse(LogManager.waitForAllToComplete(List.of(success, failure), t -> failureCount.incrementAndGet()));
        assertEquals(2, invokedCount.get());
        assertEquals(1, failureCount.get());
        assertFalse(LogManager.waitForAllToComplete(List.of(failure, success), t -> failureCount.incrementAndGet()));
        assertEquals(4, invokedCount.get());
        assertEquals(2, failureCount.get());
        assertTrue(LogManager.waitForAllToComplete(List.of(success, success), t -> failureCount.incrementAndGet()));
        assertEquals(6, invokedCount.get());
        assertEquals(2, failureCount.get());
        assertFalse(LogManager.waitForAllToComplete(List.of(failure, failure), t -> failureCount.incrementAndGet()));
        assertEquals(8, invokedCount.get());
        assertEquals(4, failureCount.get());
    }

    @Test
    public void testIsStrayReplica() {
        UnifiedLog log = mock(UnifiedLog.class);
        when(log.topicId()).thenReturn(Optional.of(Uuid.ONE_UUID));
        assertTrue(LogManager.isStrayReplica(List.of(), 0, log));
        assertTrue(LogManager.isStrayReplica(List.of(1, 2, 3), 0, log));
        assertFalse(LogManager.isStrayReplica(List.of(0, 1, 2), 0, log));
    }
}
