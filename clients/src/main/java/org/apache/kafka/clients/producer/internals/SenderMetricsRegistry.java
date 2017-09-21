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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;

public class SenderMetricsRegistry {

    final static String METRIC_GROUP_NAME = "producer-metrics";
    final static String TOPIC_METRIC_GROUP_NAME = "producer-topic-metrics";

    private final List<MetricNameTemplate> allTemplates;

    public final MetricName batchSizeAvg;
    public final MetricName batchSizeMax;
    public final MetricName compressionRateAvg;
    public final MetricName recordQueueTimeAvg;
    public final MetricName recordQueueTimeMax;
    public final MetricName requestLatencyAvg;
    public final MetricName requestLatencyMax;   
    public final MetricName produceThrottleTimeAvg;
    public final MetricName produceThrottleTimeMax;
    public final MetricName recordSendRate;
    public final MetricName recordSendTotal;
    public final MetricName recordsPerRequestAvg;
    public final MetricName recordRetryRate;
    public final MetricName recordRetryTotal;
    public final MetricName recordErrorRate;
    public final MetricName recordErrorTotal;
    public final MetricName recordSizeMax;
    public final MetricName recordSizeAvg;
    public final MetricName requestsInFlight;
    public final MetricName metadataAge;
    public final MetricName batchSplitRate;
    public final MetricName batchSplitTotal;

    private final MetricNameTemplate topicRecordSendRate;
    private final MetricNameTemplate topicRecordSendTotal;
    private final MetricNameTemplate topicByteRate;
    private final MetricNameTemplate topicByteTotal;
    private final MetricNameTemplate topicCompressionRate;
    private final MetricNameTemplate topicRecordRetryRate;
    private final MetricNameTemplate topicRecordRetryTotal;
    private final MetricNameTemplate topicRecordErrorRate;
    private final MetricNameTemplate topicRecordErrorTotal;
    
    private final Metrics metrics;
    private final Set<String> tags;
    private final HashSet<String> topicTags;

    public SenderMetricsRegistry(Metrics metrics) {
        this.metrics = metrics;
        this.tags = this.metrics.config().tags().keySet();
        this.allTemplates = new ArrayList<MetricNameTemplate>();
        
        /* ***** Client level *****/
        
        this.batchSizeAvg = createMetricName("batch-size-avg", METRIC_GROUP_NAME, "The average number of bytes sent per partition per-request.", tags);
        this.batchSizeMax = createMetricName("batch-size-max", METRIC_GROUP_NAME, "The max number of bytes sent per partition per-request.", tags);
        this.compressionRateAvg = createMetricName("compression-rate-avg", METRIC_GROUP_NAME, "The average compression rate of record batches.", tags);
        this.recordQueueTimeAvg = createMetricName("record-queue-time-avg", METRIC_GROUP_NAME, "The average time in ms record batches spent in the send buffer.", tags);
        this.recordQueueTimeMax = createMetricName("record-queue-time-max", METRIC_GROUP_NAME, "The maximum time in ms record batches spent in the send buffer.", tags);
        this.requestLatencyAvg = createMetricName("request-latency-avg", METRIC_GROUP_NAME, "The average request latency in ms", tags);
        this.requestLatencyMax = createMetricName("request-latency-max", METRIC_GROUP_NAME, "The maximum request latency in ms", tags);
        this.recordSendRate = createMetricName("record-send-rate", METRIC_GROUP_NAME, "The average number of records sent per second.", tags);
        this.recordSendTotal = createMetricName("record-send-total", METRIC_GROUP_NAME, "The total number of records sent.", tags);
        this.recordsPerRequestAvg = createMetricName("records-per-request-avg", METRIC_GROUP_NAME, "The average number of records per request.", tags);
        this.recordRetryRate = createMetricName("record-retry-rate", METRIC_GROUP_NAME, "The average per-second number of retried record sends", tags);
        this.recordRetryTotal = createMetricName("record-retry-total", METRIC_GROUP_NAME, "The total number of retried record sends", tags);
        this.recordErrorRate = createMetricName("record-error-rate", METRIC_GROUP_NAME, "The average per-second number of record sends that resulted in errors", tags);
        this.recordErrorTotal = createMetricName("record-error-total", METRIC_GROUP_NAME, "The total number of record sends that resulted in errors", tags);
        this.recordSizeMax = createMetricName("record-size-max", METRIC_GROUP_NAME, "The maximum record size", tags);
        this.recordSizeAvg = createMetricName("record-size-avg", METRIC_GROUP_NAME, "The average record size", tags);
        this.requestsInFlight = createMetricName("requests-in-flight", METRIC_GROUP_NAME, "The current number of in-flight requests awaiting a response.", tags);
        this.metadataAge = createMetricName("metadata-age", METRIC_GROUP_NAME, "The age in seconds of the current producer metadata being used.", tags);
        this.batchSplitRate = createMetricName("batch-split-rate", METRIC_GROUP_NAME, "The average number of batch splits per second", tags);
        this.batchSplitTotal = createMetricName("batch-split-total", METRIC_GROUP_NAME, "The total number of batch splits", tags);

        this.produceThrottleTimeAvg = createMetricName("produce-throttle-time-avg", METRIC_GROUP_NAME, "The average time in ms a request was throttled by a broker", tags);
        this.produceThrottleTimeMax = createMetricName("produce-throttle-time-max", METRIC_GROUP_NAME, "The maximum time in ms a request was throttled by a broker", tags);

        /* ***** Topic level *****/
        this.topicTags = new HashSet<String>(tags);
        this.topicTags.add("topic");

        this.topicRecordSendRate = createTemplate("record-send-rate", TOPIC_METRIC_GROUP_NAME, "The average number of records sent per second for a topic.", topicTags);
        this.topicRecordSendTotal = createTemplate("record-send-total", TOPIC_METRIC_GROUP_NAME, "The total number of records sent for a topic.", topicTags);
        this.topicByteRate = createTemplate("byte-rate", TOPIC_METRIC_GROUP_NAME, "The average number of bytes sent per second for a topic.", topicTags);
        this.topicByteTotal = createTemplate("byte-total", TOPIC_METRIC_GROUP_NAME, "The total number of bytes sent for a topic.", topicTags);
        this.topicCompressionRate = createTemplate("compression-rate", TOPIC_METRIC_GROUP_NAME, "The average compression rate of record batches for a topic.", topicTags);
        this.topicRecordRetryRate = createTemplate("record-retry-rate", TOPIC_METRIC_GROUP_NAME, "The average per-second number of retried record sends for a topic", topicTags);
        this.topicRecordRetryTotal = createTemplate("record-retry-total", TOPIC_METRIC_GROUP_NAME, "The total number of retried record sends for a topic", topicTags);
        this.topicRecordErrorRate = createTemplate("record-error-rate", TOPIC_METRIC_GROUP_NAME, "The average per-second number of record sends that resulted in errors for a topic", topicTags);
        this.topicRecordErrorTotal = createTemplate("record-error-total", TOPIC_METRIC_GROUP_NAME, "The total number of record sends that resulted in errors for a topic", topicTags);

    }

    private MetricName createMetricName(String name, String group, String description, Set<String> tags) {
        return this.metrics.metricInstance(createTemplate(name, group, description, tags));
    }

    /** topic level metrics **/
    public MetricName topicRecordSendRate(Map<String, String> metricTags) {
        return this.metrics.metricInstance(this.topicRecordSendRate, metricTags);
    }

    public MetricName topicRecordSendTotal(Map<String, String> metricTags) {
        return this.metrics.metricInstance(this.topicRecordSendTotal, metricTags);
    }

    public MetricName topicByteRate(Map<String, String> metricTags) {
        return this.metrics.metricInstance(this.topicByteRate, metricTags);
    }

    public MetricName topicByteTotal(Map<String, String> metricTags) {
        return this.metrics.metricInstance(this.topicByteTotal, metricTags);
    }

    public MetricName topicCompressionRate(Map<String, String> metricTags) {
        return this.metrics.metricInstance(this.topicCompressionRate, metricTags);
    }

    public MetricName topicRecordRetryRate(Map<String, String> metricTags) {
        return this.metrics.metricInstance(this.topicRecordRetryRate, metricTags);
    }

    public MetricName topicRecordRetryTotal(Map<String, String> metricTags) {
        return this.metrics.metricInstance(this.topicRecordRetryTotal, metricTags);
    }

    public MetricName topicRecordErrorRate(Map<String, String> metricTags) {
        return this.metrics.metricInstance(this.topicRecordErrorRate, metricTags);
    }

    public MetricName topicRecordErrorTotal(Map<String, String> metricTags) {
        return this.metrics.metricInstance(this.topicRecordErrorTotal, metricTags);
    }

    public List<MetricNameTemplate> getAllTemplates() {
        return allTemplates;
    }

    public Sensor sensor(String name) {
        return this.metrics.sensor(name);
    }

    public void addMetric(MetricName m, Measurable measurable) {
        this.metrics.addMetric(m, measurable);
    }

    public Sensor getSensor(String name) {
        return this.metrics.getSensor(name);
    }

    private MetricNameTemplate createTemplate(String name, String group, String description, Set<String> tags) {
        MetricNameTemplate template = new MetricNameTemplate(name, group, description, tags);
        this.allTemplates.add(template);
        return template;
    }

}
