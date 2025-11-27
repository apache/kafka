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
package org.apache.kafka.server.share.metrics;

import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.metrics.Metrics;

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class ShareGroupMetricsRegistry {

    public final MetricNameTemplate shareGroupCount;
    public final MetricNameTemplate writeRate;
    public final MetricNameTemplate writeTotal;
    public final MetricNameTemplate writeLatencyAvg;
    public final MetricNameTemplate writeLatencyMax;
    public final MetricNameTemplate lastPrunedOffset;
    public final MetricNameTemplate totalShareFetchRequestsPerSec;
    public final MetricNameTemplate failedShareFetchRequestsPerSec;
    public final MetricNameTemplate totalShareAcknowledgementRequestsPerSec;
    public final MetricNameTemplate failedShareAcknowledgementRequestsPerSec;
    public final MetricNameTemplate recordAcknowledgementsPerSec;
    public final MetricNameTemplate partitionLoadTimeMs;
    public final MetricNameTemplate requestTopicPartitionsFetchRatio;
    public final MetricNameTemplate topicPartitionsAcquireTimeMs;
    public final MetricNameTemplate acquisitionLockTimeoutPerSec;
    public final MetricNameTemplate inFlightMessageCount;
    public final MetricNameTemplate inFlightBatchCount;
    public final MetricNameTemplate inFlightBatchMessageCount;
    public final MetricNameTemplate fetchLockTimeMs;
    public final MetricNameTemplate fetchLockRatio;
    public final MetricNameTemplate shareSessionEvictionsPerSec;
    public final MetricNameTemplate sharePartitionsCount;
    public final MetricNameTemplate shareSessionsCount;
    public final MetricNameTemplate delayedShareFetchExpiresPerSec;
    public final MetricNameTemplate shareFetchPurgatorySize;
    public final MetricNameTemplate shareFetchNumDelayedOperations;

    public ShareGroupMetricsRegistry() {
        this(new HashSet<>(), "");
    }

    public ShareGroupMetricsRegistry(Set<String> tags, String metricGrpPrefix) {

        /* BrokerTopicMetrics */
        String brokerTopicGroupName = "BrokerTopicMetrics";
        Set<String> topicTags = Set.of("topic");

        this.totalShareFetchRequestsPerSec = new MetricNameTemplate("TotalShareFetchRequestsPerSec", brokerTopicGroupName,
            "The fetch request rate per second. ", topicTags);

        this.failedShareFetchRequestsPerSec = new MetricNameTemplate("FailedShareFetchRequestsPerSec", brokerTopicGroupName,
            "The share fetch request rate for requests that failed. ", topicTags);

        this.totalShareAcknowledgementRequestsPerSec = new MetricNameTemplate("TotalShareAcknowledgementRequestsPerSec", brokerTopicGroupName,
            "The acknowledgement request rate per second. ", topicTags);

        this.failedShareAcknowledgementRequestsPerSec = new MetricNameTemplate("FailedShareAcknowledgementRequestsPerSec", brokerTopicGroupName,
            "The share acknowledgement request rate for requests that failed. ", topicTags);

        /* Group Coordinator metrics */
        Set<String> state = Set.of("state");
        this.shareGroupCount = new MetricNameTemplate("share-group-count", "GroupCoordinator",
            "The number of share groups in respective state.", state);

        /* Share Coordinator metrics */
        String shareCoordinatorMetrics = "share-coordinator-metrics";
        this.writeRate = new MetricNameTemplate("write-rate", shareCoordinatorMetrics,
            "The number of share-group state write calls per second.", tags);

        this.writeTotal = new MetricNameTemplate("write-total", shareCoordinatorMetrics,
            "The total number of share-group state write calls.", tags);

        this.writeLatencyAvg = new MetricNameTemplate("write-latency-avg", shareCoordinatorMetrics,
            "The average time taken for a share-group state write call, including the time to write to the share-group state topic.", tags);

        this.writeLatencyMax = new MetricNameTemplate("write-latency-max", shareCoordinatorMetrics,
            "The maximum time taken for a share-group state write call, including the time to write to the share-group state topic.", tags);

        Set<String> topicPartitionTags = Set.of("topic", "partition");
        this.lastPrunedOffset = new MetricNameTemplate("last-pruned-offset", shareCoordinatorMetrics,
            "The last pruned offset in the share-group state topic.", topicPartitionTags);

        /* ShareGroupMetrics */
        String shareGroupMetricsName = metricGrpPrefix + "ShareGroupMetrics";
        // Acknowledgement type level metrics
        Set<String> ackTypeTags = Set.of("ackType");

        this.recordAcknowledgementsPerSec = new MetricNameTemplate("RecordAcknowledgementsPerSec", shareGroupMetricsName,
            "The rate per second of records acknowledged per acknowledgement type.", ackTypeTags);

        // General metrics
        this.partitionLoadTimeMs = new MetricNameTemplate("PartitionLoadTimeMs", shareGroupMetricsName,
            "The time taken to load the share partitions.", tags);
        // Group level metrics
        Set<String> groupTags = Set.of("group");

        this.requestTopicPartitionsFetchRatio = new MetricNameTemplate("RequestTopicPartitionsFetchRatio", shareGroupMetricsName,
            "The ratio of topic-partitions acquired to the total number of topic-partitions in share fetch request.", groupTags);

        this.topicPartitionsAcquireTimeMs = new MetricNameTemplate("TopicPartitionsAcquireTimeMs", shareGroupMetricsName,
            "The time elapsed (in millisecond) to acquire any topic partition for fetch.", groupTags);

        /* SharePartitionMetrics - Partition level */
        String sharePartitionMetricsName = "SharePartitionMetrics";
        Set<String> partitionTags = Set.of("group", "topic", "partition");

        this.acquisitionLockTimeoutPerSec = new MetricNameTemplate("AcquisitionLockTimeoutPerSec", sharePartitionMetricsName,
            "The rate of acquisition locks for records which are not acknowledged within the timeout.", partitionTags);

        this.inFlightMessageCount = new MetricNameTemplate("InFlightMessageCount", sharePartitionMetricsName,
            "The number of in-flight messages for the share partition.", partitionTags);

        this.inFlightBatchCount = new MetricNameTemplate("InFlightBatchCount", sharePartitionMetricsName,
            "The number of in-flight batches for the share partition.", partitionTags);

        this.inFlightBatchMessageCount = new MetricNameTemplate("InFlightBatchMessageCount", sharePartitionMetricsName,
            "The number of messages in the in-flight batch.", partitionTags);

        this.fetchLockTimeMs = new MetricNameTemplate("FetchLockTimeMs", sharePartitionMetricsName,
            "The time elapsed (in milliseconds) while a share partition is held under lock for fetching messages.", partitionTags);

        this.fetchLockRatio = new MetricNameTemplate("FetchLockRatio", sharePartitionMetricsName,
            "The fraction of time share partition is held under lock.", partitionTags);

        /* ShareSessionCache metrics */
        String shareSessionCacheName = "ShareSessionCache";
        this.shareSessionEvictionsPerSec = new MetricNameTemplate("ShareSessionEvictionsPerSec", shareSessionCacheName,
            "The share session eviction rate per second.", tags);

        this.sharePartitionsCount = new MetricNameTemplate("SharePartitionsCount", shareSessionCacheName,
            "The number of cached share partitions.", tags);

        this.shareSessionsCount = new MetricNameTemplate("ShareSessionsCount", shareSessionCacheName,
            "The number of cached share sessions.", tags);

        /* DelayedShareFetchMetrics */
        String delayedShareFetchMetricsName = "DelayedShareFetchMetrics";
        this.delayedShareFetchExpiresPerSec = new MetricNameTemplate("ExpiresPerSec", delayedShareFetchMetricsName,
            "The expired delayed share fetch operation rate per second.", tags);

        /* DelayedOperationPurgatory metrics */
        String delayedOperationPurgatoryName = "DelayedOperationPurgatory";
        Set<String> shareFetchTags = new LinkedHashSet<>(tags);
        shareFetchTags.add("delayedOperation");

        this.shareFetchPurgatorySize = new MetricNameTemplate("PurgatorySize", delayedOperationPurgatoryName,
            "The number of requests waiting in the share fetch purgatory. This is high if share consumers use a large value for fetch.wait.max.ms", shareFetchTags);

        this.shareFetchNumDelayedOperations = new MetricNameTemplate("NumDelayedOperations", delayedOperationPurgatoryName,
            "The number of delayed operations for share fetch purgatory.", shareFetchTags);
    }

    public List<MetricNameTemplate> getAllTemplates() {
        return Arrays.asList(
            shareGroupCount,
            writeRate,
            writeTotal,
            writeLatencyAvg,
            writeLatencyMax,
            totalShareFetchRequestsPerSec,
            failedShareFetchRequestsPerSec,
            totalShareAcknowledgementRequestsPerSec,
            failedShareAcknowledgementRequestsPerSec,
            recordAcknowledgementsPerSec,
            partitionLoadTimeMs,
            requestTopicPartitionsFetchRatio,
            topicPartitionsAcquireTimeMs,
            acquisitionLockTimeoutPerSec,
            inFlightMessageCount,
            inFlightBatchCount,
            inFlightBatchMessageCount,
            fetchLockTimeMs,
            fetchLockRatio,
            shareSessionEvictionsPerSec,
            sharePartitionsCount,
            shareSessionsCount,
            delayedShareFetchExpiresPerSec,
            shareFetchPurgatorySize,
            shareFetchNumDelayedOperations
        );
    }

    public static void main(String[] args) {
        ShareGroupMetricsRegistry metrics = new ShareGroupMetricsRegistry();
        System.out.println(Metrics.toHtmlTable("kafka.server", metrics.getAllTemplates()));
    }
}
