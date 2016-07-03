/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.clients.producer.internals;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.kafka.common.MetricNameTemplate;

public class SenderMetricsRegistry {

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

    public SenderMetricsRegistry() {
        this(new HashSet<String>());
    }

    public SenderMetricsRegistry(Set<String> tags) {

        /* ***** Client level *****/
        String metricGrpName = "producer-metrics";
        
        this.batchSizeAvg = new MetricNameTemplate("batch-size-avg", metricGrpName, "The average number of bytes sent per partition per-request.", tags);
        this.batchSizeMax = new MetricNameTemplate("batch-size-max", metricGrpName, "The max number of bytes sent per partition per-request.", tags);
        this.compressionRateAvg = new MetricNameTemplate("compression-rate-avg", metricGrpName, "The average compression rate of record batches.", tags);
        this.recordQueueTimeAvg = new MetricNameTemplate("record-queue-time-avg", metricGrpName, "The average time in ms record batches spent in the record accumulator.", tags);
        this.recordQueueTimeMax = new MetricNameTemplate("record-queue-time-max", metricGrpName, "The maximum time in ms record batches spent in the record accumulator.", tags);
        this.requestLatencyAvg = new MetricNameTemplate("request-latency-avg", metricGrpName, "The average request latency in ms", tags);
        this.requestLatencyMax = new MetricNameTemplate("request-latency-max", metricGrpName, "The maximum request latency in ms", tags);
        this.produceThrottleTimeAvg = new MetricNameTemplate("produce-throttle-time-avg", metricGrpName, "The average throttle time in ms", tags);
        this.produceThrottleTimeMax = new MetricNameTemplate("produce-throttle-time-max", metricGrpName, "The maximum throttle time in ms", tags);
        this.recordSendRate = new MetricNameTemplate("record-send-rate", metricGrpName, "The average number of records sent per second.", tags);
        this.recordsPerRequestAvg = new MetricNameTemplate("records-per-request-avg", metricGrpName, "The average number of records per request.", tags);
        this.recordRetryRate = new MetricNameTemplate("record-retry-rate", metricGrpName, "The average per-second number of retried record sends", tags);
        this.recordErrorRate = new MetricNameTemplate("record-error-rate", metricGrpName, "The average per-second number of record sends that resulted in errors", tags);
        this.recordSizeMax = new MetricNameTemplate("record-size-max", metricGrpName, "The maximum record size", tags);
        this.recordSizeAvg = new MetricNameTemplate("record-size-avg", metricGrpName, "The average record size", tags);
        this.requestsInFlight = new MetricNameTemplate("requests-in-flight", metricGrpName, "The current number of in-flight requests awaiting a response.", tags);
        this.metadataAge = new MetricNameTemplate("metadata-age", metricGrpName, "The age in seconds of the current producer metadata being used.", tags);

        /* ***** Topic level *****/
        String topicMetricGrpName = "producer-topic-metrics";
        Set<String> topicTags = new HashSet<String>(tags);
        topicTags.add("topic");

        this.topicRecordSendRate = new MetricNameTemplate("record-send-rate", topicMetricGrpName, "The average number of records sent per second.", topicTags);
        this.topicByteRate = new MetricNameTemplate("byte-rate", topicMetricGrpName, "The average number of bytes sent per second.", topicTags);
        this.topicCompressionRate = new MetricNameTemplate("compression-rate", topicMetricGrpName, "The average compression rate of record batches.", topicTags);
        this.topicRecordRetryRate = new MetricNameTemplate("record-retry-rate", topicMetricGrpName, "The average per-second number of retried record sends", topicTags);
        this.topicRecordErrorRate = new MetricNameTemplate("record-error-rate", topicMetricGrpName, "The average per-second number of record sends that resulted in errors", topicTags);

    }

    public List<MetricNameTemplate> getAllTemplates() {
        return Arrays.asList(this.batchSizeAvg,
                this.batchSizeMax,
                this.compressionRateAvg,
                this.recordQueueTimeAvg,
                this.recordQueueTimeMax,
                this.requestLatencyAvg,
                this.requestLatencyMax,
                this.produceThrottleTimeAvg,
                this.produceThrottleTimeMax,
                this.recordSendRate,
                this.recordsPerRequestAvg,
                this.recordRetryRate,
                this.recordErrorRate,
                this.recordSizeMax,
                this.recordSizeAvg,
                this.requestsInFlight,
                this.metadataAge,
                
                // per-topic metrics
                this.topicRecordSendRate,
                this.topicByteRate,
                this.topicCompressionRate,
                this.topicRecordRetryRate,
                this.topicRecordErrorRate
                );
    }

}
