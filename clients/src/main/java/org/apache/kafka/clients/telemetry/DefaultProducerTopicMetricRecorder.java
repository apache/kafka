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
package org.apache.kafka.clients.telemetry;

import static org.apache.kafka.clients.telemetry.ClientTelemetryUtils.convertToReason;
import static org.apache.kafka.clients.telemetry.ClientTelemetryUtils.formatAcks;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;

public class DefaultProducerTopicMetricRecorder extends MetricRecorder implements ProducerTopicMetricRecorder {

    private static final String GROUP_NAME = "producer-topic-telemetry";

    private static final int LATENCY_HISTOGRAM_NUM_BIN = 10;
    private static final int LATENCY_HISTOGRAM_MAX_BIN = 2000; // ms

    private final MetricNameTemplate recordQueueBytes;

    private final MetricNameTemplate recordQueueCount;

    private final MetricNameTemplate recordLatency;

    private final MetricNameTemplate queueLatency;

    private final MetricNameTemplate recordRetries;

    private final MetricNameTemplate recordFailures;

    private final MetricNameTemplate recordSuccess;

    public DefaultProducerTopicMetricRecorder(Metrics metrics) {
        super(metrics);

        Set<String> topicPartitionAcksTags = appendTags(tags, TOPIC_LABEL, PARTITION_LABEL, ACKS_LABEL);
        Set<String> topicPartitionAcksReasonTags = appendTags(topicPartitionAcksTags, REASON_LABEL);

        this.recordQueueBytes = createMetricNameTemplate(RECORD_QUEUE_BYTES_NAME, GROUP_NAME, RECORD_QUEUE_BYTES_DESCRIPTION, topicPartitionAcksTags);
        this.recordQueueCount = createMetricNameTemplate(RECORD_QUEUE_COUNT_NAME, GROUP_NAME, RECORD_QUEUE_COUNT_DESCRIPTION, topicPartitionAcksTags);
        this.recordLatency = createMetricNameTemplate(RECORD_LATENCY_NAME, GROUP_NAME, RECORD_LATENCY_DESCRIPTION, topicPartitionAcksTags);
        this.queueLatency = createMetricNameTemplate(RECORD_QUEUE_LATENCY_NAME, GROUP_NAME, RECORD_QUEUE_LATENCY_DESCRIPTION, topicPartitionAcksTags);
        this.recordRetries = createMetricNameTemplate(RECORD_RETRIES_NAME, GROUP_NAME, RECORD_RETRIES_DESCRIPTION, topicPartitionAcksTags);
        this.recordFailures = createMetricNameTemplate(RECORD_FAILURES_NAME, GROUP_NAME, RECORD_FAILURES_DESCRIPTION, topicPartitionAcksReasonTags);
        this.recordSuccess = createMetricNameTemplate(RECORD_SUCCESS_NAME, GROUP_NAME, RECORD_SUCCESS_DESCRIPTION, topicPartitionAcksTags);
    }

    @Override
    public void incrementRecordQueueBytes(TopicPartition topicPartition, short acks, long amount) {
        Map<String, String> metricsTags = getMetricsTags(topicPartition, acks);
        gaugeUpdateSensor(recordQueueBytes, metricsTags).record(amount);
    }

    @Override
    public void incrementRecordQueueCount(TopicPartition topicPartition, short acks, long amount) {
        Map<String, String> metricsTags = getMetricsTags(topicPartition, acks);
        gaugeUpdateSensor(recordQueueCount, metricsTags).record(amount);
    }

    @Override
    public void recordRecordLatency(TopicPartition topicPartition, short acks, long amount) {
        Map<String, String> metricsTags = getMetricsTags(topicPartition, acks);
        histogramSensor(recordLatency, metricsTags, LATENCY_HISTOGRAM_NUM_BIN, LATENCY_HISTOGRAM_MAX_BIN).record(amount);
    }

    @Override
    public void recordRecordQueueLatency(TopicPartition topicPartition, short acks, long amount) {
        Map<String, String> metricsTags = getMetricsTags(topicPartition, acks);
        histogramSensor(queueLatency, metricsTags, LATENCY_HISTOGRAM_NUM_BIN, LATENCY_HISTOGRAM_MAX_BIN).record(amount);
    }

    @Override
    public void addRecordRetries(TopicPartition topicPartition, short acks, long amount) {
        Map<String, String> metricsTags = getMetricsTags(topicPartition, acks);
        sumSensor(recordRetries, metricsTags).record(amount);
    }

    @Override
    public void addRecordFailures(TopicPartition topicPartition, short acks, Throwable error, long amount) {
        String reason = convertToReason(error);
        Map<String, String> metricsTags = getMetricsTags(topicPartition, acks);
        metricsTags.put(REASON_LABEL, reason);
        sumSensor(recordFailures, metricsTags).record(amount);
    }

    @Override
    public void addRecordSuccess(TopicPartition topicPartition, short acks, long amount) {
        Map<String, String> metricsTags = getMetricsTags(topicPartition, acks);
        sumSensor(recordSuccess, metricsTags).record(amount);
    }

    private Map<String, String> getMetricsTags(TopicPartition topicPartition, short acks) {
        Map<String, String> metricsTags = new HashMap<>();
        metricsTags.put(ACKS_LABEL, formatAcks(acks));
        metricsTags.put(PARTITION_LABEL, String.valueOf(topicPartition.partition()));
        metricsTags.put(TOPIC_LABEL, topicPartition.topic());
        return metricsTags;
    }
}
