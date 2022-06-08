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
package org.apache.kafka.clients.producer.internals;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.ClientTelemetryMetricsRegistry;
import org.apache.kafka.clients.ClientTelemetryUtils;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;

/**
 * Metrics corresponding to the producer topic-level per
 * <a href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-714:+Client+metrics+and+observability#KIP714:Clientmetricsandobservability-Clienttopic-levelProducermetrics">KIP-714</a>.
 */
public class ProducerTopicMetricsRegistry extends ClientTelemetryMetricsRegistry {

    public final static String PARTITION_LABEL = "partition";

    public final static String REASON_LABEL = "reason";

    public final static String TOPIC_LABEL = "topic";

    private final static String PREFIX = "org.apache.kafka.client.producer.partition.";

    private final static String RECORD_QUEUE_BYTES_NAME = PREFIX + "record.queue.bytes";

    private final static String RECORD_QUEUE_BYTES_DESCRIPTION = "Number of bytes queued on partition queue.";

    private final static String RECORD_QUEUE_COUNT_NAME = PREFIX + "record.queue.count";

    private final static String RECORD_QUEUE_COUNT_DESCRIPTION = "Number of records queued on partition queue.";

    private final static String RECORD_RETRIES_NAME = PREFIX + "record.retries";

    private final static String RECORD_RETRIES_DESCRIPTION = "Number of ProduceRequest retries.";

    private final static String RECORD_FAILURES_NAME = PREFIX + "record.failures";

    private final static String RECORD_FAILURES_DESCRIPTION = "Number of records that permanently failed delivery. Reason is a short string representation of the reason, which is typically the name of a Kafka protocol error code, e.g., “RequestTimedOut”.";

    private final static String RECORD_SUCCESS_NAME = PREFIX + "record.success";

    private final static String RECORD_SUCCESS_DESCRIPTION = "Number of records that have been successfully produced.";

    private final static String GROUP_NAME = "producer-topic-telemetry";

    private final MetricNameTemplate recordQueueBytes;

    private final MetricNameTemplate recordQueueCount;

    private final MetricNameTemplate recordRetries;

    private final MetricNameTemplate recordFailures;

    private final MetricNameTemplate recordSuccess;

    public ProducerTopicMetricsRegistry(Metrics metrics) {
        super(metrics);

        Set<String> topicPartitionTags = appendTags(tags, TOPIC_LABEL, PARTITION_LABEL);
        Set<String> topicPartitionReasonTags = appendTags(topicPartitionTags, REASON_LABEL);

        this.recordQueueBytes = createMetricNameTemplate(RECORD_QUEUE_BYTES_NAME, RECORD_QUEUE_BYTES_DESCRIPTION, topicPartitionTags);
        this.recordQueueCount = createMetricNameTemplate(RECORD_QUEUE_COUNT_NAME, RECORD_QUEUE_COUNT_DESCRIPTION, topicPartitionTags);
        this.recordRetries = createMetricNameTemplate(RECORD_RETRIES_NAME, RECORD_RETRIES_DESCRIPTION, topicPartitionTags);
        this.recordFailures = createMetricNameTemplate(RECORD_FAILURES_NAME, RECORD_FAILURES_DESCRIPTION, topicPartitionReasonTags);
        this.recordSuccess = createMetricNameTemplate(RECORD_SUCCESS_NAME, RECORD_SUCCESS_DESCRIPTION, topicPartitionTags);
    }

    private MetricNameTemplate createMetricNameTemplate(String name, String description, Set<String> tags) {
        return new MetricNameTemplate(name, GROUP_NAME, description, tags);
    }

    public MetricName recordQueueBytes(TopicPartition topicPartition) {
        Map<String, String> metricsTags = getMetricsTags(topicPartition);
        return metrics.metricInstance(recordQueueBytes, metricsTags);
    }

    public MetricName recordQueueCount(TopicPartition topicPartition) {
        Map<String, String> metricsTags = getMetricsTags(topicPartition);
        return metrics.metricInstance(recordQueueCount, metricsTags);
    }

    public MetricName recordRetries(TopicPartition topicPartition) {
        Map<String, String> metricsTags = getMetricsTags(topicPartition);
        return metrics.metricInstance(recordRetries, metricsTags);
    }

    public MetricName recordFailures(TopicPartition topicPartition, Throwable error) {
        String reason = ClientTelemetryUtils.convertToReason(error);
        Map<String, String> metricsTags = getMetricsTags(topicPartition);
        metricsTags.put(REASON_LABEL, reason);
        return metrics.metricInstance(recordFailures, metricsTags);
    }

    public MetricName recordSuccess(TopicPartition topicPartition) {
        Map<String, String> metricsTags = getMetricsTags(topicPartition);
        return metrics.metricInstance(recordSuccess, metricsTags);
    }

    private Map<String, String> getMetricsTags(TopicPartition topicPartition) {
        Map<String, String> metricsTags = new HashMap<>();
        metricsTags.put(PARTITION_LABEL, String.valueOf(topicPartition.partition()));
        metricsTags.put(TOPIC_LABEL, topicPartition.topic());
        return metricsTags;
    }

}
