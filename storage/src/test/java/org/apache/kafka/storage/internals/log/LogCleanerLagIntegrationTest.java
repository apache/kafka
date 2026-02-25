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
import org.apache.kafka.common.record.internal.CompressionType;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.record.internal.RecordVersion;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.transaction.TransactionLogConfig;
import org.apache.kafka.server.util.MockTime;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * This is an integration test that tests the fully integrated log cleaner
 */
public class LogCleanerLagIntegrationTest {
    private static final Logger log = LoggerFactory.getLogger(LogCleanerLagIntegrationTest.class);

    protected LogCleaner cleaner;
    protected final File logDir = TestUtils.tempDirectory();

    private final List<UnifiedLog> logs = new ArrayList<>();
    private static final int DEFAULT_MAX_MESSAGE_SIZE = 128;
    private static final int DEFAULT_DELETE_DELAY = 1000;
    private static final int DEFAULT_SEGMENT_SIZE = 2048;
    private static final long DEFAULT_MIN_COMPACTION_LAG_MS = 0L;
    private static final long DEFAULT_MAX_COMPACTION_LAG_MS = Long.MAX_VALUE;
    private static final long MIN_COMPACTION_LAG = Duration.ofHours(1).toMillis();
    private static final long CLEANER_BACKOFF_MS = 200L;
    private static final float DEFAULT_MIN_CLEANABLE_DIRTY_RATIO = 0.0F;
    private static final int SEGMENT_SIZE = 512;

    private int counter = 0;

    private final MockTime time = new MockTime(1400000000000L, 1000L);  // Tue May 13 16:53:20 UTC 2014
    private static final List<TopicPartition> TOPIC_PARTITIONS = List.of(
        new TopicPartition("log", 0),
        new TopicPartition("log", 1),
        new TopicPartition("log", 2)
    );

    public record KeyValueOffset(int key, String value, long firstOffset) { }
    public record ValueAndRecords(String value, MemoryRecords records) { }

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

    private long calculateSizeUpToOffset(UnifiedLog log, long offset) {
        long size = 0;
        for (LogSegment segment : log.logSegments(0L, offset)) {
            size += segment.size();
        }
        return size;
    }

    private Map<Integer, Integer> readFromLog(UnifiedLog log) {
        Map<Integer, Integer> result = new HashMap<>();
        for (LogSegment segment : log.logSegments()) {
            for (Record record : segment.log().records()) {
                int key = Integer.parseInt(LogTestUtils.readString(record.key()));
                int value = Integer.parseInt(LogTestUtils.readString(record.value()));
                result.put(key, value);
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

    private void closeLog(UnifiedLog log) throws IOException {
        log.close();
        logs.remove(log);
    }

    @AfterEach
    public void teardown() throws IOException, InterruptedException {
        if (cleaner != null) {
            cleaner.shutdown();
        }
        time.scheduler.shutdown();
        for (UnifiedLog log : logs) {
            log.close();
        }
        Utils.delete(logDir);
    }
}
