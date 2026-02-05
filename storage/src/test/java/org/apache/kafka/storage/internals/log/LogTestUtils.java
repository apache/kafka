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
import org.apache.kafka.common.record.ControlRecordType;
import org.apache.kafka.common.record.DefaultRecordBatch;
import org.apache.kafka.common.record.EndTransactionMarker;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.common.RequestLocal;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    public static MemoryRecords records(List<SimpleRecord> records,
                                        byte magicValue,
                                        Compression codec,
                                        long producerId,
                                        short producerEpoch,
                                        int sequence,
                                        long baseOffset,
                                        int partitionLeaderEpoch) {
        ByteBuffer buf = ByteBuffer.allocate(DefaultRecordBatch.sizeInBytes(records));
        MemoryRecordsBuilder builder = MemoryRecords.builder(buf, magicValue, codec, TimestampType.CREATE_TIME, baseOffset,
            System.currentTimeMillis(), producerId, producerEpoch, sequence, false, partitionLeaderEpoch);

        records.forEach(builder::append);

        return builder.build();
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
}
