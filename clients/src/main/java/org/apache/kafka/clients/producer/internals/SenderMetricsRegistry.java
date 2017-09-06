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

import java.util.Arrays;
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

    public MetricNameTemplate batchSizeAvg;
    public MetricNameTemplate batchSizeMax;
    public MetricNameTemplate compressionRateAvg;
    public MetricNameTemplate recordQueueTimeAvg;
    public MetricNameTemplate recordQueueTimeMax;
    public MetricNameTemplate requestLatencyAvg;
    public MetricNameTemplate requestLatencyMax;
    public MetricNameTemplate produceThrottleTimeAvg;
    public MetricNameTemplate produceThrottleTimeMax;
    public MetricNameTemplate recordSendRate;
    public MetricNameTemplate recordsPerRequestAvg;
    public MetricNameTemplate recordRetryRate;
    public MetricNameTemplate recordErrorRate;
    public MetricNameTemplate recordSizeMax;
    public MetricNameTemplate recordSizeAvg;
    public MetricNameTemplate requestsInFlight;
    public MetricNameTemplate metadataAge;
    public MetricNameTemplate topicRecordSendRate;
    public MetricNameTemplate topicByteRate;
    public MetricNameTemplate topicCompressionRate;
    public MetricNameTemplate topicRecordRetryRate;
    public MetricNameTemplate topicRecordErrorRate;
    public MetricNameTemplate batchSplitRate;
    private Metrics metrics;
    private Set<String> tags;
    private HashSet<String> topicTags;

    public SenderMetricsRegistry(Metrics metrics) {
        this.metrics = metrics;
        this.tags = this.metrics.config().tags().keySet();

        /* ***** Client level *****/
        
        this.batchSizeAvg = new MetricNameTemplate("batch-size-avg", METRIC_GROUP_NAME, "The average number of bytes sent per partition per-request.", tags);
        this.batchSizeMax = new MetricNameTemplate("batch-size-max", METRIC_GROUP_NAME, "The max number of bytes sent per partition per-request.", tags);
        this.compressionRateAvg = new MetricNameTemplate("compression-rate-avg", METRIC_GROUP_NAME, "The average compression rate of record batches.", tags);
        this.recordQueueTimeAvg = new MetricNameTemplate("record-queue-time-avg", METRIC_GROUP_NAME, "The average time in ms record batches spent in the send buffer.", tags);
        this.recordQueueTimeMax = new MetricNameTemplate("record-queue-time-max", METRIC_GROUP_NAME, "The maximum time in ms record batches spent in the send buffer.", tags);
        this.requestLatencyAvg = new MetricNameTemplate("request-latency-avg", METRIC_GROUP_NAME, "The average request latency in ms", tags);
        this.requestLatencyMax = new MetricNameTemplate("request-latency-max", METRIC_GROUP_NAME, "The maximum request latency in ms", tags);
        this.recordSendRate = new MetricNameTemplate("record-send-rate", METRIC_GROUP_NAME, "The average number of records sent per second.", tags);
        this.recordsPerRequestAvg = new MetricNameTemplate("records-per-request-avg", METRIC_GROUP_NAME, "The average number of records per request.", tags);
        this.recordRetryRate = new MetricNameTemplate("record-retry-rate", METRIC_GROUP_NAME, "The average per-second number of retried record sends", tags);
        this.recordErrorRate = new MetricNameTemplate("record-error-rate", METRIC_GROUP_NAME, "The average per-second number of record sends that resulted in errors", tags);
        this.recordSizeMax = new MetricNameTemplate("record-size-max", METRIC_GROUP_NAME, "The maximum record size", tags);
        this.recordSizeAvg = new MetricNameTemplate("record-size-avg", METRIC_GROUP_NAME, "The average record size", tags);
        this.requestsInFlight = new MetricNameTemplate("requests-in-flight", METRIC_GROUP_NAME, "The current number of in-flight requests awaiting a response.", tags);
        this.metadataAge = new MetricNameTemplate("metadata-age", METRIC_GROUP_NAME, "The age in seconds of the current producer metadata being used.", tags);
        this.batchSplitRate = new MetricNameTemplate("batch-split-rate", METRIC_GROUP_NAME, "The average number of batch splits per second", tags);

        this.produceThrottleTimeAvg = new MetricNameTemplate("produce-throttle-time-avg", METRIC_GROUP_NAME, "The average time in ms a request was throttled by a broker", tags);
        this.produceThrottleTimeMax = new MetricNameTemplate("produce-throttle-time-max", METRIC_GROUP_NAME, "The maximum time in ms a request was throttled by a broker", tags);

        /* ***** Topic level *****/
        this.topicTags = new HashSet<String>(tags);
        this.topicTags.add("topic");

        this.topicRecordSendRate = new MetricNameTemplate("record-send-rate", TOPIC_METRIC_GROUP_NAME, "The average number of records sent per second for a topic.", topicTags);
        this.topicByteRate = new MetricNameTemplate("byte-rate", TOPIC_METRIC_GROUP_NAME, "The average number of bytes sent per second for a topic.", topicTags);
        this.topicCompressionRate = new MetricNameTemplate("compression-rate", TOPIC_METRIC_GROUP_NAME, "The average compression rate of record batches for a topic.", topicTags);
        this.topicRecordRetryRate = new MetricNameTemplate("record-retry-rate", TOPIC_METRIC_GROUP_NAME, "The average per-second number of retried record sends for a topic", topicTags);
        this.topicRecordErrorRate = new MetricNameTemplate("record-error-rate", TOPIC_METRIC_GROUP_NAME, "The average per-second number of record sends that resulted in errors for a topic", topicTags);

    }

    public MetricName getBatchSizeAvg() {
        return this.metrics.metricInstance(this.batchSizeAvg);
    }

    public MetricName getBatchSizeMax() {
        return this.metrics.metricInstance(this.batchSizeMax);
    }

    public MetricName getCompressionRateAvg() {
        return this.metrics.metricInstance(this.compressionRateAvg);
    }

    public MetricName getRecordQueueTimeAvg() {
        return this.metrics.metricInstance(this.recordQueueTimeAvg);
    }
    
    public MetricName getRecordQueueTimeMax() {
        return this.metrics.metricInstance(this.recordQueueTimeMax);
    }

    public MetricName getRequestLatencyAvg() {
        return this.metrics.metricInstance(this.requestLatencyAvg);
    }

    public MetricName getRequestLatencyMax() {
        return this.metrics.metricInstance(this.requestLatencyMax);
    }

    public MetricName getRecordSendRate() {
        return this.metrics.metricInstance(this.recordSendRate);
    }

    public MetricName getRecordsPerRequestAvg() {
        return this.metrics.metricInstance(this.recordsPerRequestAvg);
    }

    public MetricName getRecordRetryRate() {
        return this.metrics.metricInstance(this.recordRetryRate);
    }

    public MetricName getRecordErrorRate() {
        return this.metrics.metricInstance(this.recordErrorRate);
    }

    public MetricName getRecordSizeMax() {
        return this.metrics.metricInstance(this.recordSizeMax);
    }

    public MetricName getRecordSizeAvg() {
        return this.metrics.metricInstance(this.recordSizeAvg);
    }

    public MetricName getRequestsInFlight() {
        return this.metrics.metricInstance(this.requestsInFlight);
    }

    public MetricName getBatchSplitRate() {
        return this.metrics.metricInstance(this.batchSplitRate);
    }

    public MetricName getMetadataAge() {
        return this.metrics.metricInstance(this.metadataAge);
    }

    /** topic level metrics **/
    public MetricName getTopicRecordSendRate(Map<String, String> metricTags) {
        return this.metrics.metricInstance(this.topicRecordSendRate, metricTags);
    }

    public MetricName getTopicByteRate(Map<String, String> metricTags) {
        return this.metrics.metricInstance(this.topicByteRate, metricTags);
    }

    public MetricName getTopicCompressionRate(Map<String, String> metricTags) {
        return this.metrics.metricInstance(this.topicCompressionRate, metricTags);
    }

    public MetricName getTopicRecordRetryRate(Map<String, String> metricTags) {
        return this.metrics.metricInstance(this.topicRecordRetryRate, metricTags);
    }

    public MetricName getTopicRecordErrorRate(Map<String, String> metricTags) {
        return this.metrics.metricInstance(this.topicRecordErrorRate, metricTags);
    }

    
    
    public List<MetricNameTemplate> getAllTemplates() {
        return Arrays.asList(this.batchSizeAvg,
                this.batchSizeMax,
                this.compressionRateAvg,
                this.recordQueueTimeAvg,
                this.recordQueueTimeMax,
                this.requestLatencyAvg,
                this.requestLatencyMax,
                this.recordSendRate,
                this.recordsPerRequestAvg,
                this.recordRetryRate,
                this.recordErrorRate,
                this.recordSizeMax,
                this.recordSizeAvg,
                this.requestsInFlight,
                this.metadataAge,
                this.batchSplitRate,
                
                this.produceThrottleTimeAvg,
                this.produceThrottleTimeMax,

                // per-topic metrics
                this.topicRecordSendRate,
                this.topicByteRate,
                this.topicCompressionRate,
                this.topicRecordRetryRate,
                this.topicRecordErrorRate
                );
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

}
