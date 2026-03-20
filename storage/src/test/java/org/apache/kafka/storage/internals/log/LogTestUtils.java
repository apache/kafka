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

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.record.internal.ControlRecordType;
import org.apache.kafka.common.record.internal.DefaultRecordBatch;
import org.apache.kafka.common.record.internal.EndTransactionMarker;
import org.apache.kafka.common.record.internal.FileRecords;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.MemoryRecordsBuilder;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.common.RequestLocal;
import org.apache.kafka.server.storage.log.FetchIsolation;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.LongFunction;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public class LogTestUtils {
    public static LogSegment createSegment(long offset, File logDir, int indexIntervalBytes, Time time) throws IOException {
        // Create instances of the required components
        FileRecords ms = FileRecords.open(LogFileUtils.logFile(logDir, offset));
        LazyIndex<OffsetIndex> idx = LazyIndex.forOffset(LogFileUtils.offsetIndexFile(logDir, offset), offset, 1000);
        LazyIndex<TimeIndex> timeIdx = LazyIndex.forTime(LogFileUtils.timeIndexFile(logDir, offset), offset, 1500);
        TransactionIndex txnIndex = new TransactionIndex(offset, LogFileUtils.transactionIndexFile(logDir, offset, ""));

        // Create and return the LogSegment instance
        return new LogSegment(ms, idx, timeIdx, txnIndex, offset, indexIntervalBytes, 0, time);
    }


    /**
     * Append an end transaction marker (commit or abort) to the log as a leader.
     *
     * @param transactionVersion the transaction version (1 = TV1, 2 = TV2) etc. Must be explicitly specified.
     *                          TV2 markers require strict epoch validation (markerEpoch > currentEpoch),
     *                          while legacy markers use relaxed validation (markerEpoch >= currentEpoch).
     */
    public static LogAppendInfo appendEndTxnMarkerAsLeader(UnifiedLog log,
                                                           long producerId,
                                                           short producerEpoch,
                                                           ControlRecordType controlType,
                                                           long timestamp,
                                                           int coordinatorEpoch,
                                                           int leaderEpoch,
                                                           short transactionVersion) {
        MemoryRecords records = endTxnRecords(controlType, producerId, producerEpoch, 0L, coordinatorEpoch, leaderEpoch, timestamp);

        return log.appendAsLeader(records, leaderEpoch, AppendOrigin.COORDINATOR, RequestLocal.noCaching(), VerificationGuard.SENTINEL, transactionVersion);
    }

    public static MemoryRecords endTxnRecords(ControlRecordType controlRecordType,
                                              long producerId,
                                              short epoch,
                                              long offset,
                                              int coordinatorEpoch,
                                              int partitionLeaderEpoch,
                                              long timestamp) {
        EndTransactionMarker marker = new EndTransactionMarker(controlRecordType, coordinatorEpoch);
        return MemoryRecords.withEndTransactionMarker(offset, timestamp, partitionLeaderEpoch, producerId, epoch, marker);
    }

    /**
     * Wrap a single record log buffer.
     */
    public static MemoryRecords singletonRecords(byte[] value, byte[] key) {
        return records(
            List.of(new SimpleRecord(RecordBatch.NO_TIMESTAMP, key, value)),
            RecordBatch.CURRENT_MAGIC_VALUE,
            Compression.NONE,
            RecordBatch.NO_PRODUCER_ID,
            RecordBatch.NO_PRODUCER_EPOCH,
            RecordBatch.NO_SEQUENCE,
            0L,
            RecordBatch.NO_PARTITION_LEADER_EPOCH
        );
    }

    /**
     * Create a single record batch with the specified compression and timestamp.
     */
    public static MemoryRecords singletonRecords(byte[] value, Compression codec, byte[] key, long timestamp) {
        return records(
            List.of(new SimpleRecord(timestamp, key, value)),
            RecordBatch.CURRENT_MAGIC_VALUE,
            codec,
            RecordBatch.NO_PRODUCER_ID,
            RecordBatch.NO_PRODUCER_EPOCH,
            RecordBatch.NO_SEQUENCE,
            0L,
            RecordBatch.NO_PARTITION_LEADER_EPOCH
        );
    }

    /**
     * Create a single record batch with the specified compression, timestamp, and magic value.
     */
    public static MemoryRecords singletonRecords(byte[] value, Compression codec, byte[] key,
                                                  long timestamp, byte magicValue) {
        return records(
            List.of(new SimpleRecord(timestamp, key, value)),
            magicValue,
            codec,
            RecordBatch.NO_PRODUCER_ID,
            RecordBatch.NO_PRODUCER_EPOCH,
            RecordBatch.NO_SEQUENCE,
            0L,
            RecordBatch.NO_PARTITION_LEADER_EPOCH
        );
    }

    /**
     * Read a string from a ByteBuffer using the default charset.
     */
    public static String readString(ByteBuffer buffer) {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return new String(bytes);
    }

    public static MemoryRecords records(List<SimpleRecord> records,
                                        byte magicValue,
                                        Compression codec,
                                        long producerId,
                                        short producerEpoch,
                                        int sequence,
                                        long baseOffset,
                                        int partitionLeaderEpoch,
                                        long timestamp) {
        ByteBuffer buf = ByteBuffer.allocate(DefaultRecordBatch.sizeInBytes(records));
        MemoryRecordsBuilder builder = MemoryRecords.builder(buf, magicValue, codec, TimestampType.CREATE_TIME, baseOffset,
            timestamp, producerId, producerEpoch, sequence, false, partitionLeaderEpoch);

        records.forEach(builder::append);

        return builder.build();
    }

    public static MemoryRecords records(List<SimpleRecord> records,
                                        byte magicValue,
                                        Compression codec,
                                        long producerId,
                                        short producerEpoch,
                                        int sequence,
                                        long baseOffset,
                                        int partitionLeaderEpoch) {
        return records(records, magicValue, codec, producerId, producerEpoch, sequence, baseOffset, partitionLeaderEpoch, System.currentTimeMillis());
    }

    public static MemoryRecords records(List<SimpleRecord> records,
                                        long producerId,
                                        short producerEpoch,
                                        int sequence,
                                        long baseOffset) {
        return records(records, RecordBatch.CURRENT_MAGIC_VALUE, Compression.NONE, producerId, producerEpoch, sequence, baseOffset, RecordBatch.NO_PARTITION_LEADER_EPOCH);
    }

    public static MemoryRecords records(List<SimpleRecord> records) {
        return records(records, RecordBatch.CURRENT_MAGIC_VALUE, Compression.NONE, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE, 0L, RecordBatch.NO_PARTITION_LEADER_EPOCH);
    }

    public static MemoryRecords records(List<SimpleRecord> records, long timestamp) {
        return records(records, RecordBatch.CURRENT_MAGIC_VALUE, Compression.NONE, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE, 0L, RecordBatch.NO_PARTITION_LEADER_EPOCH, timestamp);
    }

    public static MemoryRecords records(List<SimpleRecord> records, long baseOffset, int partitionLeaderEpoch) {
        return records(records, RecordBatch.CURRENT_MAGIC_VALUE, Compression.NONE, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE, baseOffset, partitionLeaderEpoch);
    }

    public static MemoryRecords records(List<SimpleRecord> records, byte magicValue, long baseOffset) {
        return records(records, magicValue, Compression.NONE, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE, baseOffset, RecordBatch.NO_PARTITION_LEADER_EPOCH);
    }

    public static void deleteProducerSnapshotFiles(File logDir) {
        Stream.of(logDir.listFiles())
                .filter(f -> f.isFile() && f.getName().endsWith(LogFileUtils.PRODUCER_SNAPSHOT_FILE_SUFFIX))
                .forEach(f -> assertDoesNotThrow(() -> Utils.delete(f)));
    }

    public static List<Long> listProducerSnapshotOffsets(File logDir) throws IOException {
        return ProducerStateManager.listSnapshotFiles(logDir).stream().map(f -> f.offset).sorted().toList();
    }

    public static FetchDataInfo readLog(UnifiedLog log,
                                        long startOffset,
                                        int maxLength,
                                        FetchIsolation isolation,
                                        boolean minOneMessage) throws IOException {
        return log.read(startOffset, maxLength, isolation, minOneMessage);
    }

    public static FetchDataInfo readLog(UnifiedLog log, long startOffset, int maxLength) throws IOException {
        return readLog(log, startOffset, maxLength, FetchIsolation.LOG_END, true);
    }

    public static boolean hasOffsetOverflow(UnifiedLog log) {
        return firstOverflowSegment(log).isPresent();
    }

    public static Optional<LogSegment> firstOverflowSegment(UnifiedLog log) {
        for (LogSegment segment : log.logSegments()) {
            for (RecordBatch batch : segment.log().batches()) {
                if (batch.lastOffset() > segment.baseOffset() + Integer.MAX_VALUE || batch.baseOffset() < segment.baseOffset()) {
                    return Optional.of(segment);
                }
            }
        }
        return Optional.empty();
    }

    public static FileRecords rawSegment(File logDir, long baseOffset) throws IOException {
        return FileRecords.open(LogFileUtils.logFile(logDir, baseOffset));
    }

    /**
     * Initialize the given log directory with a set of segments, one of which will have an
     * offset which overflows the segment
     */
    public static void initializeLogDirWithOverflowedSegment(File logDir) throws IOException {
        long nextOffset = 0L;
        nextOffset = writeNormalSegment(logDir, nextOffset);
        nextOffset = writeOverflowSegment(logDir, nextOffset);
        writeNormalSegment(logDir, nextOffset);
    }

    private static long writeSampleBatches(File logDir, long baseOffset, FileRecords segment) throws IOException {
        LongFunction<SimpleRecord> record = offset -> {
            byte[] data = Long.toString(offset).getBytes();
            return new SimpleRecord(data, data);
        };
        segment.append(MemoryRecords.withRecords(baseOffset, Compression.NONE, 0,
            record.apply(baseOffset)));
        segment.append(MemoryRecords.withRecords(baseOffset + 1, Compression.NONE, 0,
            record.apply(baseOffset + 1),
            record.apply(baseOffset + 2)));
        segment.append(MemoryRecords.withRecords(baseOffset + Integer.MAX_VALUE - 1, Compression.NONE, 0,
            record.apply(baseOffset + Integer.MAX_VALUE - 1)));
        // Need to create the offset files explicitly to avoid triggering segment recovery to truncate segment.
        Files.createFile(LogFileUtils.offsetIndexFile(logDir, baseOffset).toPath());
        Files.createFile(LogFileUtils.timeIndexFile(logDir, baseOffset).toPath());
        return baseOffset + Integer.MAX_VALUE;
    }

    private static long writeNormalSegment(File logDir, long baseOffset) throws IOException {
        try (FileRecords segment = rawSegment(logDir, baseOffset)) {
            return writeSampleBatches(logDir, baseOffset, segment);
        }
    }

    private static long writeOverflowSegment(File logDir, long baseOffset) throws IOException {
        try (FileRecords segment = rawSegment(logDir, baseOffset)) {
            long nextOffset = writeSampleBatches(logDir, baseOffset, segment);
            return writeSampleBatches(logDir, nextOffset, segment);
        }
    }

    public static void appendNonTransactionalAsLeader(UnifiedLog log, int numRecords) throws IOException {
        List<SimpleRecord> simpleRecords = new ArrayList<>();
        for (int i = 0; i < numRecords; i++) {
            simpleRecords.add(new SimpleRecord(String.valueOf(i).getBytes()));
        }
        MemoryRecords records = MemoryRecords.withRecords(Compression.NONE, simpleRecords.toArray(new SimpleRecord[0]));
        log.appendAsLeader(records, 0);
    }

    public static Consumer<Integer> appendTransactionalAsLeader(UnifiedLog log,
                                                                long producerId,
                                                                short producerEpoch,
                                                                Time time) {
        return appendIdempotentAsLeader(log, producerId, producerEpoch, time, true);
    }

    public static Consumer<Integer> appendIdempotentAsLeader(UnifiedLog log,
                                                             long producerId,
                                                             short producerEpoch,
                                                             Time time,
                                                             boolean isTransactional) {
        final AtomicInteger sequence = new AtomicInteger(0);
        return numRecords -> {
            int baseSequence = sequence.get();
            List<SimpleRecord> simpleRecords = new ArrayList<>();
            for (int i = baseSequence; i < baseSequence + numRecords; i++) {
                simpleRecords.add(new SimpleRecord(time.milliseconds(), String.valueOf(i).getBytes()));
            }

            MemoryRecords records = isTransactional
                ? MemoryRecords.withTransactionalRecords(Compression.NONE, producerId,
                        producerEpoch, baseSequence, simpleRecords.toArray(new SimpleRecord[0]))
                : MemoryRecords.withIdempotentRecords(Compression.NONE, producerId,
                        producerEpoch, baseSequence, simpleRecords.toArray(new SimpleRecord[0]));

            assertDoesNotThrow(() -> log.appendAsLeader(records, 0));
            sequence.addAndGet(numRecords);
        };
    }

    public static class LogConfigBuilder {
        private final Map<String, Object> configs = new HashMap<>();

        public LogConfigBuilder segmentMs(long segmentMs) {
            configs.put(TopicConfig.SEGMENT_MS_CONFIG, segmentMs);
            return this;
        }

        public LogConfigBuilder segmentBytes(int segmentBytes) {
            configs.put(LogConfig.INTERNAL_SEGMENT_BYTES_CONFIG, segmentBytes);
            return this;
        }

        public LogConfigBuilder retentionMs(long retentionMs) {
            configs.put(TopicConfig.RETENTION_MS_CONFIG, retentionMs);
            return this;
        }

        public LogConfigBuilder localRetentionMs(long localRetentionMs) {
            configs.put(TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG, localRetentionMs);
            return this;
        }

        public LogConfigBuilder retentionBytes(long retentionBytes) {
            configs.put(TopicConfig.RETENTION_BYTES_CONFIG, retentionBytes);
            return this;
        }

        public LogConfigBuilder localRetentionBytes(long localRetentionBytes) {
            configs.put(TopicConfig.LOCAL_LOG_RETENTION_BYTES_CONFIG, localRetentionBytes);
            return this;
        }

        public LogConfigBuilder segmentJitterMs(long segmentJitterMs) {
            configs.put(TopicConfig.SEGMENT_JITTER_MS_CONFIG, segmentJitterMs);
            return this;
        }

        public LogConfigBuilder cleanupPolicy(String cleanupPolicy) {
            configs.put(TopicConfig.CLEANUP_POLICY_CONFIG, cleanupPolicy);
            return this;
        }

        public LogConfigBuilder maxMessageBytes(int maxMessageBytes) {
            configs.put(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, maxMessageBytes);
            return this;
        }

        public LogConfigBuilder indexIntervalBytes(int indexIntervalBytes) {
            configs.put(TopicConfig.INDEX_INTERVAL_BYTES_CONFIG, indexIntervalBytes);
            return this;
        }

        public LogConfigBuilder segmentIndexBytes(int segmentIndexBytes) {
            configs.put(TopicConfig.SEGMENT_INDEX_BYTES_CONFIG, segmentIndexBytes);
            return this;
        }

        public LogConfigBuilder fileDeleteDelayMs(long fileDeleteDelayMs) {
            configs.put(TopicConfig.FILE_DELETE_DELAY_MS_CONFIG, fileDeleteDelayMs);
            return this;
        }

        public LogConfigBuilder remoteLogStorageEnable(boolean remoteLogStorageEnable) {
            configs.put(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, remoteLogStorageEnable);
            return this;
        }

        public LogConfigBuilder remoteLogCopyDisable(boolean remoteLogCopyDisable) {
            configs.put(TopicConfig.REMOTE_LOG_COPY_DISABLE_CONFIG, remoteLogCopyDisable);
            return this;
        }

        public LogConfigBuilder remoteLogDeleteOnDisable(boolean remoteLogDeleteOnDisable) {
            configs.put(TopicConfig.REMOTE_LOG_DELETE_ON_DISABLE_CONFIG, remoteLogDeleteOnDisable);
            return this;
        }

        public LogConfig build() {
            return new LogConfig(configs);
        }
    }
    
    public static class FakeOffsetMap implements OffsetMap {

        private final Map<String, Long> map = new HashMap<>();
        private long latestOff = -1L;

        @Override
        public int slots() {
            return Integer.MAX_VALUE;
        }
        
        @Override
        public void put(ByteBuffer key, long offset) {
            latestOff = offset;
            map.put(new String(Utils.readBytes(key.duplicate()), StandardCharsets.UTF_8), offset);
        }
        
        @Override
        public long get(ByteBuffer key) {
            return map.getOrDefault(new String(Utils.readBytes(key.duplicate()), StandardCharsets.UTF_8), -1L);
        }
        
        @Override
        public void updateLatestOffset(long offset) {
            latestOff = offset;
        }
        
        @Override
        public void clear() {
            map.clear();
        }
        
        @Override
        public int size() {
            return map.size();
        }
        
        @Override
        public long latestOffset() {
            return latestOff;
        }
    } 
}
