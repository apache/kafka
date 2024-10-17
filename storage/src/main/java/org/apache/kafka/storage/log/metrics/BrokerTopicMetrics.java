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
package org.apache.kafka.storage.log.metrics;

import org.apache.kafka.server.log.remote.storage.RemoteStorageMetrics;
import org.apache.kafka.server.metrics.KafkaMetricsGroup;

import com.yammer.metrics.core.Meter;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public final class BrokerTopicMetrics {
    public static final String MESSAGE_IN_PER_SEC = "MessagesInPerSec";
    public static final String BYTES_IN_PER_SEC = "BytesInPerSec";
    public static final String BYTES_OUT_PER_SEC = "BytesOutPerSec";
    public static final String BYTES_REJECTED_PER_SEC = "BytesRejectedPerSec";
    public static final String REPLICATION_BYTES_IN_PER_SEC = "ReplicationBytesInPerSec";
    public static final String REPLICATION_BYTES_OUT_PER_SEC = "ReplicationBytesOutPerSec";
    public static final String FAILED_PRODUCE_REQUESTS_PER_SEC = "FailedProduceRequestsPerSec";
    public static final String FAILED_FETCH_REQUESTS_PER_SEC = "FailedFetchRequestsPerSec";
    public static final String TOTAL_PRODUCE_REQUESTS_PER_SEC = "TotalProduceRequestsPerSec";
    public static final String TOTAL_FETCH_REQUESTS_PER_SEC = "TotalFetchRequestsPerSec";
    public static final String FETCH_MESSAGE_CONVERSIONS_PER_SEC = "FetchMessageConversionsPerSec";
    public static final String PRODUCE_MESSAGE_CONVERSIONS_PER_SEC = "ProduceMessageConversionsPerSec";
    public static final String REASSIGNMENT_BYTES_IN_PER_SEC = "ReassignmentBytesInPerSec";
    public static final String REASSIGNMENT_BYTES_OUT_PER_SEC = "ReassignmentBytesOutPerSec";
    // These following topics are for LogValidator for better debugging on failed records
    public static final String NO_KEY_COMPACTED_TOPIC_RECORDS_PER_SEC = "NoKeyCompactedTopicRecordsPerSec";
    public static final String INVALID_MAGIC_NUMBER_RECORDS_PER_SEC = "InvalidMagicNumberRecordsPerSec";
    public static final String INVALID_MESSAGE_CRC_RECORDS_PER_SEC = "InvalidMessageCrcRecordsPerSec";
    public static final String INVALID_OFFSET_OR_SEQUENCE_RECORDS_PER_SEC = "InvalidOffsetOrSequenceRecordsPerSec";

    // KAFKA-16972: BrokerTopicMetrics is migrated from "kafka.server" package.
    // For backward compatibility, we keep the old package name as metric group name.
    private final KafkaMetricsGroup metricsGroup = new KafkaMetricsGroup("kafka.server", "BrokerTopicMetrics");
    private final Map<String, String> tags;
    private final Map<String, MeterWrapper> metricTypeMap = new java.util.HashMap<>();
    private final Map<String, GaugeWrapper> metricGaugeTypeMap = new java.util.HashMap<>();

    public BrokerTopicMetrics(boolean remoteStorageEnabled) {
        this(Optional.empty(), remoteStorageEnabled);
    }

    public BrokerTopicMetrics(String name, boolean remoteStorageEnabled) {
        this(Optional.of(name), remoteStorageEnabled);
    }

    private BrokerTopicMetrics(Optional<String> name, boolean remoteStorageEnabled) {
        this.tags = name.map(s -> Collections.singletonMap("topic", s)).orElse(Collections.emptyMap());

        metricTypeMap.put(MESSAGE_IN_PER_SEC, new MeterWrapper(MESSAGE_IN_PER_SEC, "messages"));
        metricTypeMap.put(BYTES_IN_PER_SEC, new MeterWrapper(BYTES_IN_PER_SEC, "bytes"));
        metricTypeMap.put(BYTES_OUT_PER_SEC, new MeterWrapper(BYTES_OUT_PER_SEC, "bytes"));
        metricTypeMap.put(BYTES_REJECTED_PER_SEC, new MeterWrapper(BYTES_REJECTED_PER_SEC, "bytes"));
        metricTypeMap.put(FAILED_PRODUCE_REQUESTS_PER_SEC, new MeterWrapper(FAILED_PRODUCE_REQUESTS_PER_SEC, "requests"));
        metricTypeMap.put(FAILED_FETCH_REQUESTS_PER_SEC, new MeterWrapper(FAILED_FETCH_REQUESTS_PER_SEC, "requests"));
        metricTypeMap.put(TOTAL_PRODUCE_REQUESTS_PER_SEC, new MeterWrapper(TOTAL_PRODUCE_REQUESTS_PER_SEC, "requests"));
        metricTypeMap.put(TOTAL_FETCH_REQUESTS_PER_SEC, new MeterWrapper(TOTAL_FETCH_REQUESTS_PER_SEC, "requests"));
        metricTypeMap.put(FETCH_MESSAGE_CONVERSIONS_PER_SEC, new MeterWrapper(FETCH_MESSAGE_CONVERSIONS_PER_SEC, "requests"));
        metricTypeMap.put(PRODUCE_MESSAGE_CONVERSIONS_PER_SEC, new MeterWrapper(PRODUCE_MESSAGE_CONVERSIONS_PER_SEC, "requests"));
        metricTypeMap.put(NO_KEY_COMPACTED_TOPIC_RECORDS_PER_SEC, new MeterWrapper(NO_KEY_COMPACTED_TOPIC_RECORDS_PER_SEC, "requests"));
        metricTypeMap.put(INVALID_MAGIC_NUMBER_RECORDS_PER_SEC, new MeterWrapper(INVALID_MAGIC_NUMBER_RECORDS_PER_SEC, "requests"));
        metricTypeMap.put(INVALID_MESSAGE_CRC_RECORDS_PER_SEC, new MeterWrapper(INVALID_MESSAGE_CRC_RECORDS_PER_SEC, "requests"));
        metricTypeMap.put(INVALID_OFFSET_OR_SEQUENCE_RECORDS_PER_SEC, new MeterWrapper(INVALID_OFFSET_OR_SEQUENCE_RECORDS_PER_SEC, "requests"));

        if (!name.isPresent()) {
            metricTypeMap.put(REPLICATION_BYTES_IN_PER_SEC, new MeterWrapper(REPLICATION_BYTES_IN_PER_SEC, "bytes"));
            metricTypeMap.put(REPLICATION_BYTES_OUT_PER_SEC, new MeterWrapper(REPLICATION_BYTES_OUT_PER_SEC, "bytes"));
            metricTypeMap.put(REASSIGNMENT_BYTES_IN_PER_SEC, new MeterWrapper(REASSIGNMENT_BYTES_IN_PER_SEC, "bytes"));
            metricTypeMap.put(REASSIGNMENT_BYTES_OUT_PER_SEC, new MeterWrapper(REASSIGNMENT_BYTES_OUT_PER_SEC, "bytes"));
        }

        if (remoteStorageEnabled) {
            metricTypeMap.put(RemoteStorageMetrics.REMOTE_COPY_BYTES_PER_SEC_METRIC.getName(), new MeterWrapper(RemoteStorageMetrics.REMOTE_COPY_BYTES_PER_SEC_METRIC.getName(), "bytes"));
            metricTypeMap.put(RemoteStorageMetrics.REMOTE_FETCH_BYTES_PER_SEC_METRIC.getName(), new MeterWrapper(RemoteStorageMetrics.REMOTE_FETCH_BYTES_PER_SEC_METRIC.getName(), "bytes"));
            metricTypeMap.put(RemoteStorageMetrics.REMOTE_FETCH_REQUESTS_PER_SEC_METRIC.getName(), new MeterWrapper(RemoteStorageMetrics.REMOTE_FETCH_REQUESTS_PER_SEC_METRIC.getName(), "requests"));
            metricTypeMap.put(RemoteStorageMetrics.REMOTE_COPY_REQUESTS_PER_SEC_METRIC.getName(), new MeterWrapper(RemoteStorageMetrics.REMOTE_COPY_REQUESTS_PER_SEC_METRIC.getName(), "requests"));
            metricTypeMap.put(RemoteStorageMetrics.REMOTE_DELETE_REQUESTS_PER_SEC_METRIC.getName(), new MeterWrapper(RemoteStorageMetrics.REMOTE_DELETE_REQUESTS_PER_SEC_METRIC.getName(), "requests"));
            metricTypeMap.put(RemoteStorageMetrics.BUILD_REMOTE_LOG_AUX_STATE_REQUESTS_PER_SEC_METRIC.getName(), new MeterWrapper(RemoteStorageMetrics.BUILD_REMOTE_LOG_AUX_STATE_REQUESTS_PER_SEC_METRIC.getName(), "requests"));
            metricTypeMap.put(RemoteStorageMetrics.FAILED_REMOTE_FETCH_PER_SEC_METRIC.getName(), new MeterWrapper(RemoteStorageMetrics.FAILED_REMOTE_FETCH_PER_SEC_METRIC.getName(), "requests"));
            metricTypeMap.put(RemoteStorageMetrics.FAILED_REMOTE_COPY_PER_SEC_METRIC.getName(), new MeterWrapper(RemoteStorageMetrics.FAILED_REMOTE_COPY_PER_SEC_METRIC.getName(), "requests"));
            metricTypeMap.put(RemoteStorageMetrics.FAILED_REMOTE_DELETE_PER_SEC_METRIC.getName(), new MeterWrapper(RemoteStorageMetrics.FAILED_REMOTE_DELETE_PER_SEC_METRIC.getName(), "requests"));
            metricTypeMap.put(RemoteStorageMetrics.FAILED_BUILD_REMOTE_LOG_AUX_STATE_PER_SEC_METRIC.getName(), new MeterWrapper(RemoteStorageMetrics.FAILED_BUILD_REMOTE_LOG_AUX_STATE_PER_SEC_METRIC.getName(), "requests"));

            metricGaugeTypeMap.put(RemoteStorageMetrics.REMOTE_COPY_LAG_BYTES_METRIC.getName(), new GaugeWrapper(RemoteStorageMetrics.REMOTE_COPY_LAG_BYTES_METRIC.getName()));
            metricGaugeTypeMap.put(RemoteStorageMetrics.REMOTE_COPY_LAG_SEGMENTS_METRIC.getName(), new GaugeWrapper(RemoteStorageMetrics.REMOTE_COPY_LAG_SEGMENTS_METRIC.getName()));
            metricGaugeTypeMap.put(RemoteStorageMetrics.REMOTE_DELETE_LAG_BYTES_METRIC.getName(), new GaugeWrapper(RemoteStorageMetrics.REMOTE_DELETE_LAG_BYTES_METRIC.getName()));
            metricGaugeTypeMap.put(RemoteStorageMetrics.REMOTE_DELETE_LAG_SEGMENTS_METRIC.getName(), new GaugeWrapper(RemoteStorageMetrics.REMOTE_DELETE_LAG_SEGMENTS_METRIC.getName()));
            metricGaugeTypeMap.put(RemoteStorageMetrics.REMOTE_LOG_METADATA_COUNT_METRIC.getName(), new GaugeWrapper(RemoteStorageMetrics.REMOTE_LOG_METADATA_COUNT_METRIC.getName()));
            metricGaugeTypeMap.put(RemoteStorageMetrics.REMOTE_LOG_SIZE_COMPUTATION_TIME_METRIC.getName(), new GaugeWrapper(RemoteStorageMetrics.REMOTE_LOG_SIZE_COMPUTATION_TIME_METRIC.getName()));
            metricGaugeTypeMap.put(RemoteStorageMetrics.REMOTE_LOG_SIZE_BYTES_METRIC.getName(), new GaugeWrapper(RemoteStorageMetrics.REMOTE_LOG_SIZE_BYTES_METRIC.getName()));
        }
    }

    public void closeMetric(String metricName) {
        MeterWrapper mw = metricTypeMap.get(metricName);
        if (mw != null) mw.close();
        GaugeWrapper mg = metricGaugeTypeMap.get(metricName);
        if (mg != null) mg.close();
    }

    public void close() {
        metricTypeMap.values().forEach(MeterWrapper::close);
        metricGaugeTypeMap.values().forEach(GaugeWrapper::close);
    }

    // used for testing only
    public Set<String> metricMapKeySet() {
        return metricTypeMap.keySet();
    }

    public Map<String, GaugeWrapper> metricGaugeMap() {
        return metricGaugeTypeMap;
    }

    public Meter messagesInRate() {
        return metricTypeMap.get(MESSAGE_IN_PER_SEC).meter();
    }

    public Meter bytesInRate() {
        return metricTypeMap.get(BYTES_IN_PER_SEC).meter();
    }

    public Meter bytesOutRate() {
        return metricTypeMap.get(BYTES_OUT_PER_SEC).meter();
    }

    public Meter bytesRejectedRate() {
        return metricTypeMap.get(BYTES_REJECTED_PER_SEC).meter();
    }

    public Optional<Meter> replicationBytesInRate() {
        if (tags.isEmpty()) {
            return Optional.of(metricTypeMap.get(REPLICATION_BYTES_IN_PER_SEC).meter());
        } else {
            return Optional.empty();
        }
    }

    public Optional<Meter> replicationBytesOutRate() {
        if (tags.isEmpty()) {
            return Optional.of(metricTypeMap.get(REPLICATION_BYTES_OUT_PER_SEC).meter());
        } else {
            return Optional.empty();
        }
    }

    public Optional<Meter> reassignmentBytesInPerSec() {
        if (tags.isEmpty()) {
            return Optional.of(metricTypeMap.get(REASSIGNMENT_BYTES_IN_PER_SEC).meter());
        } else {
            return Optional.empty();
        }
    }

    public Optional<Meter> reassignmentBytesOutPerSec() {
        if (tags.isEmpty()) {
            return Optional.of(metricTypeMap.get(REASSIGNMENT_BYTES_OUT_PER_SEC).meter());
        } else {
            return Optional.empty();
        }
    }

    public Meter failedProduceRequestRate() {
        return metricTypeMap.get(FAILED_PRODUCE_REQUESTS_PER_SEC).meter();
    }

    public Meter failedFetchRequestRate() {
        return metricTypeMap.get(FAILED_FETCH_REQUESTS_PER_SEC).meter();
    }

    public Meter totalProduceRequestRate() {
        return metricTypeMap.get(TOTAL_PRODUCE_REQUESTS_PER_SEC).meter();
    }

    public Meter totalFetchRequestRate() {
        return metricTypeMap.get(TOTAL_FETCH_REQUESTS_PER_SEC).meter();
    }

    public Meter fetchMessageConversionsRate() {
        return metricTypeMap.get(FETCH_MESSAGE_CONVERSIONS_PER_SEC).meter();
    }

    public Meter produceMessageConversionsRate() {
        return metricTypeMap.get(PRODUCE_MESSAGE_CONVERSIONS_PER_SEC).meter();
    }

    public Meter noKeyCompactedTopicRecordsPerSec() {
        return metricTypeMap.get(NO_KEY_COMPACTED_TOPIC_RECORDS_PER_SEC).meter();
    }

    public Meter invalidMagicNumberRecordsPerSec() {
        return metricTypeMap.get(INVALID_MAGIC_NUMBER_RECORDS_PER_SEC).meter();
    }

    public Meter invalidMessageCrcRecordsPerSec() {
        return metricTypeMap.get(INVALID_MESSAGE_CRC_RECORDS_PER_SEC).meter();
    }

    public Meter invalidOffsetOrSequenceRecordsPerSec() {
        return metricTypeMap.get(INVALID_OFFSET_OR_SEQUENCE_RECORDS_PER_SEC).meter();
    }

    public GaugeWrapper remoteCopyLagBytesAggrMetric() {
        return metricGaugeTypeMap.get(RemoteStorageMetrics.REMOTE_COPY_LAG_BYTES_METRIC.getName());
    }

    // Visible for testing
    public long remoteCopyLagBytes() {
        return remoteCopyLagBytesAggrMetric().value();
    }

    public GaugeWrapper remoteCopyLagSegmentsAggrMetric() {
        return metricGaugeTypeMap.get(RemoteStorageMetrics.REMOTE_COPY_LAG_SEGMENTS_METRIC.getName());
    }

    // Visible for testing
    public long remoteCopyLagSegments() {
        return remoteCopyLagSegmentsAggrMetric().value();
    }

    public GaugeWrapper remoteLogMetadataCountAggrMetric() {
        return metricGaugeTypeMap.get(RemoteStorageMetrics.REMOTE_LOG_METADATA_COUNT_METRIC.getName());
    }

    public long remoteLogMetadataCount() {
        return remoteLogMetadataCountAggrMetric().value();
    }

    public GaugeWrapper remoteLogSizeBytesAggrMetric() {
        return metricGaugeTypeMap.get(RemoteStorageMetrics.REMOTE_LOG_SIZE_BYTES_METRIC.getName());
    }

    public long remoteLogSizeBytes() {
        return remoteLogSizeBytesAggrMetric().value();
    }

    public GaugeWrapper remoteLogSizeComputationTimeAggrMetric() {
        return metricGaugeTypeMap.get(RemoteStorageMetrics.REMOTE_LOG_SIZE_COMPUTATION_TIME_METRIC.getName());
    }

    public long remoteLogSizeComputationTime() {
        return remoteLogSizeComputationTimeAggrMetric().value();
    }

    public GaugeWrapper remoteDeleteLagBytesAggrMetric() {
        return metricGaugeTypeMap.get(RemoteStorageMetrics.REMOTE_DELETE_LAG_BYTES_METRIC.getName());
    }

    // Visible for testing
    public long remoteDeleteLagBytes() {
        return remoteDeleteLagBytesAggrMetric().value();
    }

    public GaugeWrapper remoteDeleteLagSegmentsAggrMetric() {
        return metricGaugeTypeMap.get(RemoteStorageMetrics.REMOTE_DELETE_LAG_SEGMENTS_METRIC.getName());
    }

    // Visible for testing
    public long remoteDeleteLagSegments() {
        return remoteDeleteLagSegmentsAggrMetric().value();
    }

    public Meter remoteCopyBytesRate() {
        return metricTypeMap.get(RemoteStorageMetrics.REMOTE_COPY_BYTES_PER_SEC_METRIC.getName()).meter();
    }

    public Meter remoteFetchBytesRate() {
        return metricTypeMap.get(RemoteStorageMetrics.REMOTE_FETCH_BYTES_PER_SEC_METRIC.getName()).meter();
    }

    public Meter remoteFetchRequestRate() {
        return metricTypeMap.get(RemoteStorageMetrics.REMOTE_FETCH_REQUESTS_PER_SEC_METRIC.getName()).meter();
    }

    public Meter remoteCopyRequestRate() {
        return metricTypeMap.get(RemoteStorageMetrics.REMOTE_COPY_REQUESTS_PER_SEC_METRIC.getName()).meter();
    }

    public Meter remoteDeleteRequestRate() {
        return metricTypeMap.get(RemoteStorageMetrics.REMOTE_DELETE_REQUESTS_PER_SEC_METRIC.getName()).meter();
    }

    public Meter buildRemoteLogAuxStateRequestRate() {
        return metricTypeMap.get(RemoteStorageMetrics.BUILD_REMOTE_LOG_AUX_STATE_REQUESTS_PER_SEC_METRIC.getName()).meter();
    }

    public Meter failedRemoteFetchRequestRate() {
        return metricTypeMap.get(RemoteStorageMetrics.FAILED_REMOTE_FETCH_PER_SEC_METRIC.getName()).meter();
    }

    public Meter failedRemoteCopyRequestRate() {
        return metricTypeMap.get(RemoteStorageMetrics.FAILED_REMOTE_COPY_PER_SEC_METRIC.getName()).meter();
    }

    public Meter failedRemoteDeleteRequestRate() {
        return metricTypeMap.get(RemoteStorageMetrics.FAILED_REMOTE_DELETE_PER_SEC_METRIC.getName()).meter();
    }

    public Meter failedBuildRemoteLogAuxStateRate() {
        return metricTypeMap.get(RemoteStorageMetrics.FAILED_BUILD_REMOTE_LOG_AUX_STATE_PER_SEC_METRIC.getName()).meter();
    }

    private class MeterWrapper {
        private final String metricType;
        private final String eventType;
        private volatile Meter lazyMeter;
        private final Lock meterLock = new ReentrantLock();

        public MeterWrapper(String metricType, String eventType) {
            this.metricType = metricType;
            this.eventType = eventType;
            if (tags.isEmpty()) {
                meter(); // greedily initialize the general topic metrics
            }
        }

        public Meter meter() {
            Meter meter = lazyMeter;
            if (meter == null) {
                meterLock.lock();
                try {
                    meter = lazyMeter;
                    if (meter == null) {
                        meter = metricsGroup.newMeter(metricType, eventType, TimeUnit.SECONDS, tags);
                        lazyMeter = meter;
                    }
                } finally {
                    meterLock.unlock();
                }
            }
            return meter;
        }

        public void close() {
            meterLock.lock();
            try {
                if (lazyMeter != null) {
                    metricsGroup.removeMetric(metricType, tags);
                    lazyMeter = null;
                }
            } finally {
                meterLock.unlock();
            }
        }
    }

    public class GaugeWrapper {
        // The map to store:
        //   - per-partition value for topic-level metrics. The key will be the partition number
        //   - per-topic value for broker-level metrics. The key will be the topic name
        private final ConcurrentHashMap<String, Long> metricValues = new ConcurrentHashMap<>();
        private final String metricType;

        public GaugeWrapper(String metricType) {
            this.metricType = metricType;
            newGaugeIfNeed();
        }

        public void setValue(String key, long value) {
            newGaugeIfNeed();
            metricValues.put(key, value);
        }

        public void removeKey(String key) {
            newGaugeIfNeed();
            metricValues.remove(key);
        }

        public void close() {
            metricsGroup.removeMetric(metricType, tags);
            metricValues.clear();
        }

        public long value() {
            return metricValues.values().stream().mapToLong(v -> v).sum();
        }

        // metricsGroup uses ConcurrentMap to store gauges, so we don't need to use synchronized block here
        private void newGaugeIfNeed() {
            metricsGroup.newGauge(metricType, () -> metricValues.values().stream().mapToLong(v -> v).sum(), tags);
        }
    }
}
