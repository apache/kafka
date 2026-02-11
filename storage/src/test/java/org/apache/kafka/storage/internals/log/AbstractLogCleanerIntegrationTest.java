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
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.record.internal.RecordVersion;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.transaction.TransactionLogConfig;
import org.apache.kafka.server.util.MockTime;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Tag("integration")
public abstract class AbstractLogCleanerIntegrationTest {

    protected LogCleaner cleaner;
    protected final File logDir = TestUtils.tempDirectory();

    private final List<UnifiedLog> logs = new ArrayList<>();
    private static final int DEFAULT_MAX_MESSAGE_SIZE = 128;
    private static final float DEFAULT_MIN_CLEANABLE_DIRTY_RATIO = 0.0F;
    private static final long DEFAULT_MIN_COMPACTION_LAG_MS = 0L;
    private static final int DEFAULT_DELETE_DELAY = 1000;
    private static final int DEFAULT_SEGMENT_SIZE = 2048;
    private static final long DEFAULT_MAX_COMPACTION_LAG_MS = Long.MAX_VALUE;

    private int counter = 0;

    protected abstract MockTime time();

    @AfterEach
    public void teardown() throws IOException, InterruptedException {
        if (cleaner != null) {
            cleaner.shutdown();
        }
        time().scheduler.shutdown();
        for (UnifiedLog log : logs) {
            log.close();
        }
        Utils.delete(logDir);
    }

    protected Properties logConfigProperties(Properties propertyOverrides,
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

    // TODO: This would be used when `LogCleanerParameterizedIntegrationTest` is ported to Java and moved to storage module
    protected Properties logConfigProperties(int maxMessageSize) {
        return logConfigProperties(new Properties(), maxMessageSize,
            DEFAULT_MIN_CLEANABLE_DIRTY_RATIO, DEFAULT_MIN_COMPACTION_LAG_MS,
            DEFAULT_DELETE_DELAY, DEFAULT_SEGMENT_SIZE, DEFAULT_MAX_COMPACTION_LAG_MS);
    }

    protected LogCleaner makeCleaner(Iterable<TopicPartition> partitions,
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
                time().scheduler,
                new BrokerTopicStats(),
                time(),
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
            time());
    }

    protected LogCleaner makeCleaner(Iterable<TopicPartition> partitions,
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

    protected int counter() {
        return counter;
    }

    protected void incCounter() {
        counter++;
    }

    protected List<KeyValueOffset> writeDups(int numKeys, int numDups, UnifiedLog log, Compression codec,
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

    // TODO: This would be used when `LogCleanerParameterizedIntegrationTest` is ported to Java and moved to storage module
    protected List<KeyValueOffset> writeDups(int numKeys, int numDups, UnifiedLog log, Compression codec) throws IOException {
        return writeDups(numKeys, numDups, log, codec, 0, RecordBatch.CURRENT_MAGIC_VALUE);
    }

    // TODO: This would be used when `LogCleanerParameterizedIntegrationTest` is ported to Java and moved to storage module
    protected ValueAndRecords createLargeSingleMessageSet(int key, byte messageFormatVersion, Compression codec) {
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

    // TODO: This would be used when `LogCleanerParameterizedIntegrationTest` is ported to Java and moved to storage module
    protected void closeLog(UnifiedLog log) throws IOException {
        log.close();
        logs.remove(log);
    }

    public static class KeyValueOffset {
        public final int key;
        public final String value;
        public final long firstOffset;

        public KeyValueOffset(int key, String value, long firstOffset) {
            this.key = key;
            this.value = value;
            this.firstOffset = firstOffset;
        }
    }

    public static class ValueAndRecords {
        public final String value;
        public final MemoryRecords records;

        public ValueAndRecords(String value, MemoryRecords records) {
            this.value = value;
            this.records = records;
        }
    }
}
