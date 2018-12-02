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
import java.util.LinkedHashSet;
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
    private final LinkedHashSet<String> topicTags;

    public SenderMetricsRegistry(Metrics metrics) {
        this.metrics = metrics;
        this.tags = this.metrics.config().tags().keySet();
        this.allTemplates = new ArrayList<>();
        
        /***** Client level *****/
        
        this.batchSizeAvg = createMetricName("batch-size-avg",
                "The average number of bytes sent per partition per-request.");
        this.batchSizeMax = createMetricName("batch-size-max",
                "The max number of bytes sent per partition per-request.");
        this.compressionRateAvg = createMetricName("compression-rate-avg",
                "The average compression rate of record batches.");
        this.recordQueueTimeAvg = createMetricName("record-queue-time-avg",
                "The average time in ms record batches spent in the send buffer.");
        this.recordQueueTimeMax = createMetricName("record-queue-time-max",
                "The maximum time in ms record batches spent in the send buffer.");
        this.requestLatencyAvg = createMetricName("request-latency-avg", 
                "The average request latency in ms");
        this.requestLatencyMax = createMetricName("request-latency-max", 
                "The maximum request latency in ms");
        this.recordSendRate = createMetricName("record-send-rate", 
                "The average number of records sent per second.");
        this.recordSendTotal = createMetricName("record-send-total", 
                "The total number of records sent.");
        this.recordsPerRequestAvg = createMetricName("records-per-request-avg",
                "The average number of records per request.");
        this.recordRetryRate = createMetricName("record-retry-rate",
                "The average per-second number of retried record sends");
        this.recordRetryTotal = createMetricName("record-retry-total", 
                "The total number of retried record sends");
        this.recordErrorRate = createMetricName("record-error-rate",
                "The average per-second number of record sends that resulted in errors");
        this.recordErrorTotal = createMetricName("record-error-total",
                "The total number of record sends that resulted in errors");
        this.recordSizeMax = createMetricName("record-size-max", 
                "The maximum record size");
        this.recordSizeAvg = createMetricName("record-size-avg", 
                "The average record size");
        this.requestsInFlight = createMetricName("requests-in-flight",
                "The current number of in-flight requests awaiting a response.");
        this.metadataAge = createMetricName("metadata-age",
                "The age in seconds of the current producer metadata being used.");
        this.batchSplitRate = createMetricName("batch-split-rate", 
                "The average number of batch splits per second");
        this.batchSplitTotal = createMetricName("batch-split-total", 
                "The total number of batch splits");

        this.produceThrottleTimeAvg = createMetricName("produce-throttle-time-avg",
                "The average time in ms a request was throttled by a broker");
        this.produceThrottleTimeMax = createMetricName("produce-throttle-time-max",
                "The maximum time in ms a request was throttled by a broker");

        /***** Topic level *****/
        this.topicTags = new LinkedHashSet<>(tags);
        this.topicTags.add("topic");

        // We can't create the MetricName up front for these, because we don't know the topic name yet.
        this.topicRecordSendRate = createTopicTemplate("record-send-rate",
                "The average number of records sent per second for a topic.");
        this.topicRecordSendTotal = createTopicTemplate("record-send-total",
                "The total number of records sent for a topic.");
        this.topicByteRate = createTopicTemplate("byte-rate",
                "The average number of bytes sent per second for a topic.");
        this.topicByteTotal = createTopicTemplate("byte-total", 
                "The total number of bytes sent for a topic.");
        this.topicCompressionRate = createTopicTemplate("compression-rate",
                "The average compression rate of record batches for a topic.");
        this.topicRecordRetryRate = createTopicTemplate("record-retry-rate",
                "The average per-second number of retried record sends for a topic");
        this.topicRecordRetryTotal = createTopicTemplate("record-retry-total",
                "The total number of retried record sends for a topic");
        this.topicRecordErrorRate = createTopicTemplate("record-error-rate",
                "The average per-second number of record sends that resulted in errors for a topic");
        this.topicRecordErrorTotal = createTopicTemplate("record-error-total",
                "The total number of record sends that resulted in errors for a topic");

    }

    private MetricName createMetricName(String name, String description) {
        return this.metrics.metricInstance(createTemplate(name, METRIC_GROUP_NAME, description, this.tags));
    }

    private MetricNameTemplate createTopicTemplate(String name, String description) {
        return createTemplate(name, TOPIC_METRIC_GROUP_NAME, description, this.topicTags);
    }

    /** topic level metrics **/
    public MetricName topicRecordSendRate(Map<String, String> tags) {
        return this.metrics.metricInstance(this.topicRecordSendRate, tags);
    }

    public MetricName topicRecordSendTotal(Map<String, String> tags) {
        return this.metrics.metricInstance(this.topicRecordSendTotal, tags);
    }

    public MetricName topicByteRate(Map<String, String> tags) {
        return this.metrics.metricInstance(this.topicByteRate, tags);
    }

    public MetricName topicByteTotal(Map<String, String> tags) {
        return this.metrics.metricInstance(this.topicByteTotal, tags);
    }

    public MetricName topicCompressionRate(Map<String, String> tags) {
        return this.metrics.metricInstance(this.topicCompressionRate, tags);
    }

    public MetricName topicRecordRetryRate(Map<String, String> tags) {
        return this.metrics.metricInstance(this.topicRecordRetryRate, tags);
    }

    public MetricName topicRecordRetryTotal(Map<String, String> tags) {
        return this.metrics.metricInstance(this.topicRecordRetryTotal, tags);
    }

    public MetricName topicRecordErrorRate(Map<String, String> tags) {
        return this.metrics.metricInstance(this.topicRecordErrorRate, tags);
    }

    public MetricName topicRecordErrorTotal(Map<String, String> tags) {
        return this.metrics.metricInstance(this.topicRecordErrorTotal, tags);
    }

    public List<MetricNameTemplate> allTemplates() {
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
