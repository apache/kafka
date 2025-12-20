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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.record.ControlRecordType;
import org.apache.kafka.common.record.EndTransactionMarker;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.common.RequestLocal;
import org.apache.kafka.server.common.TransactionVersion;
import org.apache.kafka.server.config.ServerLogConfigs;
import org.apache.kafka.server.util.Scheduler;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import java.io.File;
import java.io.IOException;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentMap;

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

    public static LogAppendInfo appendEndTxnMarkerAsLeader(UnifiedLog log,
                                                           long producerId,
                                                           short producerEpoch,
                                                           ControlRecordType controlType,
                                                           long timestamp,
                                                           int coordinatorEpoch,
                                                           int leaderEpoch) {
        MemoryRecords records = endTxnRecords(controlType, producerId, producerEpoch, 0L, coordinatorEpoch, leaderEpoch, timestamp);

        return log.appendAsLeader(records, leaderEpoch, AppendOrigin.COORDINATOR, RequestLocal.noCaching(), VerificationGuard.SENTINEL, TransactionVersion.TV_UNKNOWN);
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

    @SuppressWarnings("ParameterNumber")
    public static UnifiedLog createLog(File dir,
                                       LogConfig config,
                                       BrokerTopicStats brokerTopicStats,
                                       Scheduler scheduler,
                                       Time time,
                                       long logStartOffset,
                                       long recoveryPoint,
                                       int maxTransactionTimeoutMs,
                                       ProducerStateManagerConfig producerStateManagerConfig,
                                       int producerIdExpirationCheckIntervalMs,
                                       boolean lastShutdownClean,
                                       Optional<Uuid> topicId,
                                       ConcurrentMap<String, Integer> numRemainingSegments,
                                       boolean remoteStorageSystemEnable,
                                       LogOffsetsListener logOffsetsListener) throws IOException {
        return UnifiedLog.create(
                dir,
                config,
                logStartOffset,
                recoveryPoint,
                scheduler,
                brokerTopicStats,
                time,
                maxTransactionTimeoutMs,
                producerStateManagerConfig,
                producerIdExpirationCheckIntervalMs,
                new LogDirFailureChannel(10),
                lastShutdownClean,
                topicId,
                numRemainingSegments,
                remoteStorageSystemEnable,
                logOffsetsListener
        );
    }

    public static class LogConfigBuilder {
        private long segmentMs = LogConfig.DEFAULT_SEGMENT_MS;
        private int segmentBytes = LogConfig.DEFAULT_SEGMENT_BYTES;
        private long retentionMs = LogConfig.DEFAULT_RETENTION_MS;
        private long localRetentionMs = LogConfig.DEFAULT_LOCAL_RETENTION_MS;
        private long retentionBytes = ServerLogConfigs.LOG_RETENTION_BYTES_DEFAULT;
        private long localRetentionBytes = LogConfig.DEFAULT_LOCAL_RETENTION_BYTES;
        private long segmentJitterMs = LogConfig.DEFAULT_SEGMENT_JITTER_MS;
        private String cleanupPolicy = ServerLogConfigs.LOG_CLEANUP_POLICY_DEFAULT;
        private int maxMessageBytes = ServerLogConfigs.MAX_MESSAGE_BYTES_DEFAULT;
        private int indexIntervalBytes = ServerLogConfigs.LOG_INDEX_INTERVAL_BYTES_DEFAULT;
        private int segmentIndexBytes = ServerLogConfigs.LOG_INDEX_SIZE_MAX_BYTES_DEFAULT;
        private long fileDeleteDelayMs = ServerLogConfigs.LOG_DELETE_DELAY_MS_DEFAULT;
        private boolean remoteLogStorageEnable = LogConfig.DEFAULT_REMOTE_STORAGE_ENABLE;
        private boolean remoteLogCopyDisable = LogConfig.DEFAULT_REMOTE_LOG_COPY_DISABLE_CONFIG;
        private boolean remoteLogDeleteOnDisable = LogConfig.DEFAULT_REMOTE_LOG_DELETE_ON_DISABLE_CONFIG;

        public LogConfigBuilder withSegmentMs(long segmentMs) {
            this.segmentMs = segmentMs;
            return this;
        }

        public LogConfigBuilder withSegmentBytes(int segmentBytes) {
            this.segmentBytes = segmentBytes;
            return this;
        }

        public LogConfigBuilder withRetentionMs(long retentionMs) {
            this.retentionMs = retentionMs;
            return this;
        }

        public LogConfigBuilder withLocalRetentionMs(long localRetentionMs) {
            this.localRetentionMs = localRetentionMs;
            return this;
        }

        public LogConfigBuilder withRetentionBytes(long retentionBytes) {
            this.retentionBytes = retentionBytes;
            return this;
        }

        public LogConfigBuilder withLocalRetentionBytes(long localRetentionBytes) {
            this.localRetentionBytes = localRetentionBytes;
            return this;
        }

        public LogConfigBuilder withSegmentJitterMs(long segmentJitterMs) {
            this.segmentJitterMs = segmentJitterMs;
            return this;
        }

        public LogConfigBuilder withCleanupPolicy(String cleanupPolicy) {
            this.cleanupPolicy = cleanupPolicy;
            return this;
        }

        public LogConfigBuilder withMaxMessageBytes(int maxMessageBytes) {
            this.maxMessageBytes = maxMessageBytes;
            return this;
        }

        public LogConfigBuilder withIndexIntervalBytes(int indexIntervalBytes) {
            this.indexIntervalBytes = indexIntervalBytes;
            return this;
        }

        public LogConfigBuilder withSegmentIndexBytes(int segmentIndexBytes) {
            this.segmentIndexBytes = segmentIndexBytes;
            return this;
        }

        public LogConfigBuilder withFileDeleteDelayMs(long fileDeleteDelayMs) {
            this.fileDeleteDelayMs = fileDeleteDelayMs;
            return this;
        }

        public LogConfigBuilder withRemoteLogStorageEnable(boolean remoteLogStorageEnable) {
            this.remoteLogStorageEnable = remoteLogStorageEnable;
            return this;
        }

        public LogConfigBuilder withRemoteLogCopyDisable(boolean remoteLogCopyDisable) {
            this.remoteLogCopyDisable = remoteLogCopyDisable;
            return this;
        }

        public LogConfigBuilder withRemoteLogDeleteOnDisable(boolean remoteLogDeleteOnDisable) {
            this.remoteLogDeleteOnDisable = remoteLogDeleteOnDisable;
            return this;
        }

        public LogConfig build() {
            Properties logProps = new Properties();
            logProps.put(TopicConfig.SEGMENT_MS_CONFIG, String.valueOf(segmentMs));
            logProps.put(LogConfig.INTERNAL_SEGMENT_BYTES_CONFIG, String.valueOf(segmentBytes));
            logProps.put(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(retentionMs));
            logProps.put(TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG, String.valueOf(localRetentionMs));
            logProps.put(TopicConfig.RETENTION_BYTES_CONFIG, String.valueOf(retentionBytes));
            logProps.put(TopicConfig.LOCAL_LOG_RETENTION_BYTES_CONFIG, String.valueOf(localRetentionBytes));
            logProps.put(TopicConfig.SEGMENT_JITTER_MS_CONFIG, String.valueOf(segmentJitterMs));
            logProps.put(TopicConfig.CLEANUP_POLICY_CONFIG, cleanupPolicy);
            logProps.put(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, String.valueOf(maxMessageBytes));
            logProps.put(TopicConfig.INDEX_INTERVAL_BYTES_CONFIG, String.valueOf(indexIntervalBytes));
            logProps.put(TopicConfig.SEGMENT_INDEX_BYTES_CONFIG, String.valueOf(segmentIndexBytes));
            logProps.put(TopicConfig.FILE_DELETE_DELAY_MS_CONFIG, String.valueOf(fileDeleteDelayMs));
            logProps.put(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, String.valueOf(remoteLogStorageEnable));
            logProps.put(TopicConfig.REMOTE_LOG_COPY_DISABLE_CONFIG, String.valueOf(remoteLogCopyDisable));
            logProps.put(TopicConfig.REMOTE_LOG_DELETE_ON_DISABLE_CONFIG, String.valueOf(remoteLogDeleteOnDisable));
            return new LogConfig(logProps);
        }
    }
}
