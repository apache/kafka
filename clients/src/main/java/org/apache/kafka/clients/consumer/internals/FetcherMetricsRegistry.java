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
package org.apache.kafka.clients.consumer.internals;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;

public class FetcherMetricsRegistry {

    final static String METRIC_GROUP_NAME = "consumer-fetch-manager-metrics";
    
    private final List<MetricNameTemplate> allTemplates;

    public final MetricName fetchSizeAvg;
    public final MetricName fetchSizeMax;
    public final MetricName bytesConsumedRate;
    public final MetricName bytesConsumedTotal;
    public final MetricName recordsPerRequestAvg;
    public final MetricName recordsConsumedRate;
    public final MetricName recordsConsumedTotal;
    public final MetricName fetchLatencyAvg;
    public final MetricName fetchLatencyMax;
    public final MetricName fetchRequestRate;
    public final MetricName fetchRequestTotal;
    public final MetricName recordsLagMax;
    public final MetricName fetchThrottleTimeAvg;
    public final MetricName fetchThrottleTimeMax;

    private final MetricNameTemplate topicFetchSizeAvg;
    private final MetricNameTemplate topicFetchSizeMax;
    private final MetricNameTemplate topicBytesConsumedRate;
    private final MetricNameTemplate topicBytesConsumedTotal;
    private final MetricNameTemplate topicRecordsPerRequestAvg;
    private final MetricNameTemplate topicRecordsConsumedRate;
    private final MetricNameTemplate topicRecordsConsumedTotal;
    private final MetricNameTemplate partitionRecordsLag;
    private final MetricNameTemplate partitionRecordsLagMax;
    private final MetricNameTemplate partitionRecordsLagAvg;

    private final Metrics metrics;
    private final Set<String> tags;
    private final HashSet<String> topicTags;

    public FetcherMetricsRegistry(Metrics metrics) {
        this.metrics = metrics;
        this.tags = this.metrics.config().tags().keySet();
        this.allTemplates = new ArrayList<MetricNameTemplate>();

        /***** Client level *****/
        this.fetchSizeAvg = createMetricName("fetch-size-avg",
                "The average number of bytes fetched per request");
        this.fetchSizeMax = createMetricName("fetch-size-max",
                "The maximum number of bytes fetched per request");
        this.bytesConsumedRate = createMetricName("bytes-consumed-rate",
                "The average number of bytes consumed per second");
        this.bytesConsumedTotal = createMetricName("bytes-consumed-total",
                "The total number of bytes consumed");
        this.recordsPerRequestAvg = createMetricName("records-per-request-avg",
                "The average number of records in each request");
        this.recordsConsumedRate = createMetricName("records-consumed-rate",
                "The average number of records consumed per second");
        this.recordsConsumedTotal = createMetricName("records-consumed-total",
                "The total number of records consumed");
        this.fetchLatencyAvg = createMetricName("fetch-latency-avg",
                "The average time taken for a fetch request.");
        this.fetchLatencyMax = createMetricName("fetch-latency-max",
                "The max time taken for any fetch request.");
        this.fetchRequestRate = createMetricName("fetch-rate",
                "The number of fetch requests per second.");
        this.fetchRequestTotal = createMetricName("fetch-total",
                "The total number of fetch requests.");
        this.recordsLagMax = createMetricName("records-lag-max",
                "The maximum lag in terms of number of records for any partition in this window");
        this.fetchThrottleTimeAvg = createMetricName("fetch-throttle-time-avg", 
                "The average throttle time in ms");
        this.fetchThrottleTimeMax = createMetricName("fetch-throttle-time-max", 
                "The maximum throttle time in ms");

        /***** Topic level *****/
        this.topicTags = new HashSet<>(tags);
        this.topicTags.add("topic");

        // We can't create the MetricName up front for these, because we don't know the topic name yet.
        this.topicFetchSizeAvg = createTopicTemplate("fetch-size-avg",
                "The average number of bytes fetched per request for a topic");
        this.topicFetchSizeMax = createTopicTemplate("fetch-size-max",
                "The maximum number of bytes fetched per request for a topic");
        this.topicBytesConsumedRate = createTopicTemplate("bytes-consumed-rate",
                "The average number of bytes consumed per second for a topic");
        this.topicBytesConsumedTotal = createTopicTemplate("bytes-consumed-total",
                "The total number of bytes consumed for a topic");
        this.topicRecordsPerRequestAvg = createTopicTemplate("records-per-request-avg",
                "The average number of records in each request for a topic");
        this.topicRecordsConsumedRate = createTopicTemplate("records-consumed-rate",
                "The average number of records consumed per second for a topic");
        this.topicRecordsConsumedTotal = createTopicTemplate("records-consumed-total",
                "The total number of records consumed for a topic");
        
        /***** Partition level *****/
        // We can't create the MetricName up front for these, because we don't know the topic and partition name.
        this.partitionRecordsLag = createTemplate("{topic}-{partition}.records-lag", "The latest lag of the partition", 
                tags);
        this.partitionRecordsLagMax = createTemplate("{topic}-{partition}.records-lag-max", "The max lag of the partition", 
                tags);
        this.partitionRecordsLagAvg = createTemplate("{topic}-{partition}.records-lag-avg", "The average lag of the partition", 
                tags);
        
    }
    
    public MetricName topicFetchSizeAvg(Map<String, String> tags) {
        return metrics.metricInstance(this.topicFetchSizeAvg, tags);
    }

    public MetricName topicFetchSizeMax(Map<String, String> tags) {
        return metrics.metricInstance(this.topicFetchSizeMax, tags);
    }

    public MetricName topicBytesConsumedRate(Map<String, String> tags) {
        return metrics.metricInstance(this.topicBytesConsumedRate, tags);
    }

    public MetricName topicBytesConsumedTotal(Map<String, String> tags) {
        return metrics.metricInstance(this.topicBytesConsumedTotal, tags);
    }

    public MetricName topicRecordsPerRequestAvg(Map<String, String> tags) {
        return metrics.metricInstance(this.topicRecordsPerRequestAvg, tags);
    }

    public MetricName topicRecordsConsumedRate(Map<String, String> tags) {
        return metrics.metricInstance(this.topicRecordsConsumedRate, tags);
    }

    public MetricName topicRecordsConsumedTotal(Map<String, String> tags) {
        return metrics.metricInstance(this.topicRecordsConsumedTotal, tags);
    }

    public MetricName partitionRecordsLag(String partitionLagMetricName) {
        return metrics.metricName(partitionLagMetricName,
                partitionRecordsLag.group(),
                partitionRecordsLag.description());
    }

    public MetricName partitionRecordsLagMax(String partitionLagMetricName) {
        return metrics.metricName(partitionLagMetricName + "-max",
                partitionRecordsLagMax.group(),
                partitionRecordsLagMax.description());
    }

    public MetricName partitionRecordsLagAvg(String partitionLagMetricName) {
        return metrics.metricName(partitionLagMetricName + "-avg",
                partitionRecordsLagAvg.group(),
                partitionRecordsLagAvg.description());
    }

    public List<MetricNameTemplate> allTemplates() {
        return allTemplates;
    }
    
    public Sensor sensor(String name) {
        return this.metrics.sensor(name);
    }

    public Sensor getSensor(String name) {
        return this.metrics.getSensor(name);
    }

    public void removeSensor(String name) {
        this.metrics.removeSensor(name);
    }

    private MetricName createMetricName(String name, String description) {
        return this.metrics.metricInstance(createTemplate(name, description, this.tags));
    }

    private MetricNameTemplate createTopicTemplate(String name, String description) {
        return createTemplate(name, description, this.topicTags);
    }

    
    private MetricNameTemplate createTemplate(String name, String description, Set<String> tags) {
        MetricNameTemplate template = new MetricNameTemplate(name, METRIC_GROUP_NAME, description, tags);
        this.allTemplates.add(template);
        return template;
    }

}
