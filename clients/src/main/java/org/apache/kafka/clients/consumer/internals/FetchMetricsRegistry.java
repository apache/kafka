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

import org.apache.kafka.common.MetricNameTemplate;

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class FetchMetricsRegistry {

    public MetricNameTemplate fetchSizeAvg;
    public MetricNameTemplate fetchSizeMax;
    public MetricNameTemplate bytesConsumedRate;
    public MetricNameTemplate bytesConsumedTotal;
    public MetricNameTemplate recordsPerRequestAvg;
    public MetricNameTemplate recordsConsumedRate;
    public MetricNameTemplate recordsConsumedTotal;
    public MetricNameTemplate fetchLatencyAvg;
    public MetricNameTemplate fetchLatencyMax;
    public MetricNameTemplate fetchRequestRate;
    public MetricNameTemplate fetchRequestTotal;
    public MetricNameTemplate recordsLagMax;
    public MetricNameTemplate recordsLeadMin;
    public MetricNameTemplate fetchThrottleTimeAvg;
    public MetricNameTemplate fetchThrottleTimeMax;
    public MetricNameTemplate topicFetchSizeAvg;
    public MetricNameTemplate topicFetchSizeMax;
    public MetricNameTemplate topicBytesConsumedRate;
    public MetricNameTemplate topicBytesConsumedTotal;
    public MetricNameTemplate topicRecordsPerRequestAvg;
    public MetricNameTemplate topicRecordsConsumedRate;
    public MetricNameTemplate topicRecordsConsumedTotal;
    public MetricNameTemplate partitionRecordsLag;
    public MetricNameTemplate partitionRecordsLagMax;
    public MetricNameTemplate partitionRecordsLagAvg;
    public MetricNameTemplate partitionRecordsLead;
    public MetricNameTemplate partitionRecordsLeadMin;
    public MetricNameTemplate partitionRecordsLeadAvg;
    public MetricNameTemplate partitionPreferredReadReplica;

    public FetchMetricsRegistry() {
        this(new HashSet<>(), "");
    }

    public FetchMetricsRegistry(String metricGrpPrefix) {
        this(new HashSet<>(), metricGrpPrefix);
    }

    public FetchMetricsRegistry(Set<String> tags, String metricGrpPrefix) {

        /* Client level */
        String groupName = metricGrpPrefix + "-fetch-manager-metrics";

        this.fetchSizeAvg = new MetricNameTemplate("fetch-size-avg", groupName,
                "The average number of bytes fetched per request", tags);

        this.fetchSizeMax = new MetricNameTemplate("fetch-size-max", groupName,
                "The maximum number of bytes fetched per request", tags);
        this.bytesConsumedRate = new MetricNameTemplate("bytes-consumed-rate", groupName,
                "The average number of bytes consumed per second", tags);
        this.bytesConsumedTotal = new MetricNameTemplate("bytes-consumed-total", groupName,
                "The total number of bytes consumed", tags);

        this.recordsPerRequestAvg = new MetricNameTemplate("records-per-request-avg", groupName,
                "The average number of records in each request", tags);
        this.recordsConsumedRate = new MetricNameTemplate("records-consumed-rate", groupName,
                "The average number of records consumed per second", tags);
        this.recordsConsumedTotal = new MetricNameTemplate("records-consumed-total", groupName,
                "The total number of records consumed", tags);

        this.fetchLatencyAvg = new MetricNameTemplate("fetch-latency-avg", groupName,
                "The average time taken for a fetch request.", tags);
        this.fetchLatencyMax = new MetricNameTemplate("fetch-latency-max", groupName,
                "The max time taken for any fetch request.", tags);
        this.fetchRequestRate = new MetricNameTemplate("fetch-rate", groupName,
                "The number of fetch requests per second.", tags);
        this.fetchRequestTotal = new MetricNameTemplate("fetch-total", groupName,
                "The total number of fetch requests.", tags);

        this.recordsLagMax = new MetricNameTemplate("records-lag-max", groupName,
                "The maximum lag in terms of number of records for any partition in this window. NOTE: This is based on current offset and not committed offset", tags);
        this.recordsLeadMin = new MetricNameTemplate("records-lead-min", groupName,
                "The minimum lead in terms of number of records for any partition in this window", tags);

        this.fetchThrottleTimeAvg = new MetricNameTemplate("fetch-throttle-time-avg", groupName,
                "The average throttle time in ms", tags);
        this.fetchThrottleTimeMax = new MetricNameTemplate("fetch-throttle-time-max", groupName,
                "The maximum throttle time in ms", tags);

        /*  Topic level */
        Set<String> topicTags = new LinkedHashSet<>(tags);
        topicTags.add("topic");

        this.topicFetchSizeAvg = new MetricNameTemplate("fetch-size-avg", groupName,
                "The average number of bytes fetched per request for a topic", topicTags);
        this.topicFetchSizeMax = new MetricNameTemplate("fetch-size-max", groupName,
                "The maximum number of bytes fetched per request for a topic", topicTags);
        this.topicBytesConsumedRate = new MetricNameTemplate("bytes-consumed-rate", groupName,
                "The average number of bytes consumed per second for a topic", topicTags);
        this.topicBytesConsumedTotal = new MetricNameTemplate("bytes-consumed-total", groupName,
                "The total number of bytes consumed for a topic", topicTags);

        this.topicRecordsPerRequestAvg = new MetricNameTemplate("records-per-request-avg", groupName,
                "The average number of records in each request for a topic", topicTags);
        this.topicRecordsConsumedRate = new MetricNameTemplate("records-consumed-rate", groupName,
                "The average number of records consumed per second for a topic", topicTags);
        this.topicRecordsConsumedTotal = new MetricNameTemplate("records-consumed-total", groupName,
                "The total number of records consumed for a topic", topicTags);

        /* Partition level */
        Set<String> partitionTags = new HashSet<>(topicTags);
        partitionTags.add("partition");
        this.partitionRecordsLag = new MetricNameTemplate("records-lag", groupName,
                "The latest lag of the partition", partitionTags);
        this.partitionRecordsLagMax = new MetricNameTemplate("records-lag-max", groupName,
                "The max lag of the partition", partitionTags);
        this.partitionRecordsLagAvg = new MetricNameTemplate("records-lag-avg", groupName,
                "The average lag of the partition", partitionTags);
        this.partitionRecordsLead = new MetricNameTemplate("records-lead", groupName,
                "The latest lead of the partition", partitionTags);
        this.partitionRecordsLeadMin = new MetricNameTemplate("records-lead-min", groupName,
                "The min lead of the partition", partitionTags);
        this.partitionRecordsLeadAvg = new MetricNameTemplate("records-lead-avg", groupName,
                "The average lead of the partition", partitionTags);
        this.partitionPreferredReadReplica = new MetricNameTemplate(
                "preferred-read-replica", groupName,
                "The current read replica for the partition, or -1 if reading from leader", partitionTags);
    }

    public List<MetricNameTemplate> getAllTemplates() {
        return Arrays.asList(
            fetchSizeAvg,
            fetchSizeMax,
            bytesConsumedRate,
            bytesConsumedTotal,
            recordsPerRequestAvg,
            recordsConsumedRate,
            recordsConsumedTotal,
            fetchLatencyAvg,
            fetchLatencyMax,
            fetchRequestRate,
            fetchRequestTotal,
            recordsLagMax,
            recordsLeadMin,
            fetchThrottleTimeAvg,
            fetchThrottleTimeMax,
            topicFetchSizeAvg,
            topicFetchSizeMax,
            topicBytesConsumedRate,
            topicBytesConsumedTotal,
            topicRecordsPerRequestAvg,
            topicRecordsConsumedRate,
            topicRecordsConsumedTotal,
            partitionRecordsLag,
            partitionRecordsLagAvg,
            partitionRecordsLagMax,
            partitionRecordsLead,
            partitionRecordsLeadMin,
            partitionRecordsLeadAvg,
            partitionPreferredReadReplica
        );
    }

}
