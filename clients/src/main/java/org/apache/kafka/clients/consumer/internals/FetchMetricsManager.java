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

import org.apache.kafka.clients.consumer.internals.metrics.AbstractMetricsManager;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.WindowedCount;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;

/**
 * The {@link FetchMetricsManager} class provides wrapper methods to record lag, lead, latency, and fetch metrics.
 * It keeps an internal ID of the assigned set of partitions which is updated to ensure the set of metrics it
 * records matches up with the topic-partitions in use.
 */
public class FetchMetricsManager extends AbstractMetricsManager {

    private final FetchMetricsRegistry metricsRegistry;
    private final Sensor throttleTime;
    private final Sensor bytesFetched;
    private final Sensor recordsFetched;
    private final Sensor fetchLatency;
    private final Sensor recordsLag;
    private final Sensor recordsLead;

    private int assignmentId = 0;
    private Set<TopicPartition> assignedPartitions = Collections.emptySet();

    public FetchMetricsManager(Metrics metrics, FetchMetricsRegistry metricsRegistry) {
        super(metrics, metricsRegistry.groupName);
        this.metricsRegistry = metricsRegistry;

        this.throttleTime = newSensorBuilder("fetch-throttle-time")
                .withAvg(metricsRegistry.fetchThrottleTimeAvg)
                .withMax(metricsRegistry.fetchThrottleTimeMax)
                .sensor();
        this.bytesFetched = newSensorBuilder("bytes-fetched")
                .withAvg(metricsRegistry.fetchSizeAvg)
                .withMax(metricsRegistry.fetchSizeMax)
                .withMeter(metricsRegistry.bytesConsumedRate, metricsRegistry.bytesConsumedTotal)
                .sensor();
        this.recordsFetched = newSensorBuilder("records-fetched")
                .withAvg(metricsRegistry.recordsPerRequestAvg)
                .withMeter(metricsRegistry.recordsConsumedRate, metricsRegistry.recordsConsumedTotal)
                .sensor();
        this.fetchLatency = newSensorBuilder("fetch-latency")
                .withAvg(metricsRegistry.fetchLatencyAvg)
                .withMax(metricsRegistry.fetchLatencyMax)
                .withMeter(new WindowedCount(), metricsRegistry.fetchRequestRate, metricsRegistry.fetchRequestTotal)
                .sensor();
        this.recordsLag = newSensorBuilder("records-lag")
                .withMax(metricsRegistry.recordsLagMax)
                .sensor();
        this.recordsLead = newSensorBuilder("records-lead")
                .withMin(metricsRegistry.recordsLeadMin)
                .sensor();
    }

    public Sensor throttleTimeSensor() {
        return throttleTime;
    }

    void recordLatency(String node, long requestLatencyMs) {
        fetchLatency.record(requestLatencyMs);
        if (!node.isEmpty()) {
            String nodeTimeName = "node-" + node + ".latency";
            Sensor nodeRequestTime = this.metrics.getSensor(nodeTimeName);
            if (nodeRequestTime != null)
                nodeRequestTime.record(requestLatencyMs);
        }
    }

    void recordBytesFetched(int bytes) {
        bytesFetched.record(bytes);
    }

    void recordRecordsFetched(int records) {
        recordsFetched.record(records);
    }

    void recordBytesFetched(String topic, int bytes) {
        String name = topicBytesFetchedMetricName(topic);
        maybeRecordDeprecatedBytesFetched(name, topic, bytes);

        Sensor bytesFetched = newSensorBuilder(name, () -> Map.of("topic", topic))
            .withAvg(metricsRegistry.topicFetchSizeAvg)
            .withMax(metricsRegistry.topicFetchSizeMax)
            .withMeter(metricsRegistry.topicBytesConsumedRate, metricsRegistry.topicBytesConsumedTotal)
            .sensor();
        bytesFetched.record(bytes);
    }

    void recordRecordsFetched(String topic, int records) {
        String name = topicRecordsFetchedMetricName(topic);
        maybeRecordDeprecatedRecordsFetched(name, topic, records);

        Sensor recordsFetched = newSensorBuilder(name, () -> Map.of("topic", topic))
            .withAvg(metricsRegistry.topicRecordsPerRequestAvg)
            .withMeter(metricsRegistry.topicRecordsConsumedRate, metricsRegistry.topicRecordsConsumedTotal)
            .sensor();
        recordsFetched.record(records);
    }

    void recordPartitionLag(TopicPartition tp, long lag) {
        this.recordsLag.record(lag);

        String name = partitionRecordsLagMetricName(tp);
        maybeRecordDeprecatedPartitionLag(name, tp, lag);

        Sensor recordsLag = newSensorBuilder(name, () -> mkMap(mkEntry("topic", tp.topic()), mkEntry("partition", String.valueOf(tp.partition()))))
            .withValue(metricsRegistry.partitionRecordsLag)
            .withMax(metricsRegistry.partitionRecordsLagMax)
            .withAvg(metricsRegistry.partitionRecordsLagAvg)
            .sensor();

        recordsLag.record(lag);
    }

    void recordPartitionLead(TopicPartition tp, long lead) {
        this.recordsLead.record(lead);

        String name = partitionRecordsLeadMetricName(tp);
        maybeRecordDeprecatedPartitionLead(name, tp, lead);

        Sensor recordsLead = newSensorBuilder(name, () -> mkMap(mkEntry("topic", tp.topic()), mkEntry("partition", String.valueOf(tp.partition()))))
            .withValue(metricsRegistry.partitionRecordsLead)
            .withMin(metricsRegistry.partitionRecordsLeadMin)
            .withAvg(metricsRegistry.partitionRecordsLeadAvg)
            .sensor();

        recordsLead.record(lead);
    }

    /**
     * This method is called by the {@link Fetch fetch} logic before it requests fetches in order to update the
     * internal set of metrics that are tracked.
     *
     * @param subscription {@link SubscriptionState} that contains the set of assigned partitions
     * @see SubscriptionState#assignmentId()
     */
    void maybeUpdateAssignment(SubscriptionState subscription) {
        int newAssignmentId = subscription.assignmentId();

        if (this.assignmentId != newAssignmentId) {
            Set<TopicPartition> newAssignedPartitions = subscription.assignedPartitions();

            for (TopicPartition tp : this.assignedPartitions) {
                if (!newAssignedPartitions.contains(tp)) {
                    metrics.removeSensor(partitionRecordsLagMetricName(tp));
                    metrics.removeSensor(partitionRecordsLeadMetricName(tp));
                    metrics.removeMetric(partitionPreferredReadReplicaMetricName(tp));
                    // Remove deprecated metrics.
                    metrics.removeSensor(deprecatedMetricName(partitionRecordsLagMetricName(tp)));
                    metrics.removeSensor(deprecatedMetricName(partitionRecordsLeadMetricName(tp)));
                    metrics.removeMetric(deprecatedPartitionPreferredReadReplicaMetricName(tp));
                }
            }

            for (TopicPartition tp : newAssignedPartitions) {
                if (!this.assignedPartitions.contains(tp)) {
                    maybeRecordDeprecatedPreferredReadReplica(tp, subscription);

                    MetricName metricName = partitionPreferredReadReplicaMetricName(tp);
                    metrics.addMetricIfAbsent(
                        metricName,
                        null,
                        (Gauge<Integer>) (config, now) -> subscription.preferredReadReplica(tp, 0L).orElse(-1)
                    );
                }
            }

            this.assignedPartitions = newAssignedPartitions;
            this.assignmentId = newAssignmentId;
        }
    }

    @Deprecated // To be removed in Kafka 5.0 release.
    private void maybeRecordDeprecatedBytesFetched(String name, String topic, int bytes) {
        if (shouldReportDeprecatedMetric(topic)) {
            Sensor deprecatedBytesFetched = newSensorBuilder(deprecatedMetricName(name), () -> topicTags(topic))
                .withAvg(metricsRegistry.topicFetchSizeAvg)
                .withMax(metricsRegistry.topicFetchSizeMax)
                .withMeter(metricsRegistry.topicBytesConsumedRate, metricsRegistry.topicBytesConsumedTotal)
                .sensor();
            deprecatedBytesFetched.record(bytes);
        }
    }

    @Deprecated // To be removed in Kafka 5.0 release.
    private void maybeRecordDeprecatedRecordsFetched(String name, String topic, int records) {
        if (shouldReportDeprecatedMetric(topic)) {
            Sensor deprecatedRecordsFetched = newSensorBuilder(deprecatedMetricName(name), () -> topicTags(topic))
                .withAvg(metricsRegistry.topicRecordsPerRequestAvg)
                .withMeter(metricsRegistry.topicRecordsConsumedRate, metricsRegistry.topicRecordsConsumedTotal)
                .sensor();
            deprecatedRecordsFetched.record(records);
        }
    }

    @Deprecated // To be removed in Kafka 5.0 release.
    private void maybeRecordDeprecatedPartitionLag(String name, TopicPartition tp, long lag) {
        if (shouldReportDeprecatedMetric(tp.topic())) {
            Sensor deprecatedRecordsLag = newSensorBuilder(deprecatedMetricName(name), () -> topicPartitionTags(tp))
                .withValue(metricsRegistry.partitionRecordsLag)
                .withMax(metricsRegistry.partitionRecordsLagMax)
                .withAvg(metricsRegistry.partitionRecordsLagAvg)
                .sensor();

            deprecatedRecordsLag.record(lag);
        }
    }

    @Deprecated // To be removed in Kafka 5.0 release.
    private void maybeRecordDeprecatedPartitionLead(String name, TopicPartition tp, double lead) {
        if (shouldReportDeprecatedMetric(tp.topic())) {
            Sensor deprecatedRecordsLead = newSensorBuilder(deprecatedMetricName(name), () -> topicPartitionTags(tp))
                .withValue(metricsRegistry.partitionRecordsLead)
                .withMin(metricsRegistry.partitionRecordsLeadMin)
                .withAvg(metricsRegistry.partitionRecordsLeadAvg)
                .sensor();

            deprecatedRecordsLead.record(lead);
        }
    }

    @Deprecated // To be removed in Kafka 5.0 release.
    private void maybeRecordDeprecatedPreferredReadReplica(TopicPartition tp, SubscriptionState subscription) {
        if (shouldReportDeprecatedMetric(tp.topic())) {
            MetricName metricName = deprecatedPartitionPreferredReadReplicaMetricName(tp);
            metrics.addMetricIfAbsent(
                metricName,
                null,
                (Gauge<Integer>) (config, now) -> subscription.preferredReadReplica(tp, 0L).orElse(-1)
            );
        }
    }

    private static String topicBytesFetchedMetricName(String topic) {
        return "topic." + topic + ".bytes-fetched";
    }

    private static String topicRecordsFetchedMetricName(String topic) {
        return "topic." + topic + ".records-fetched";
    }

    private static String partitionRecordsLeadMetricName(TopicPartition tp) {
        return tp + ".records-lead";
    }

    private static String partitionRecordsLagMetricName(TopicPartition tp) {
        return tp + ".records-lag";
    }

    private static String deprecatedMetricName(String name) {
        return name + ".deprecated";
    }

    private static boolean shouldReportDeprecatedMetric(String topic) {
        return topic.contains(".");
    }

    private MetricName partitionPreferredReadReplicaMetricName(TopicPartition tp) {
        Map<String, String> metricTags = mkMap(mkEntry("topic", tp.topic()), mkEntry("partition", String.valueOf(tp.partition())));
        return this.metrics.metricInstance(metricsRegistry.partitionPreferredReadReplica, metricTags);
    }

    @Deprecated
    private MetricName deprecatedPartitionPreferredReadReplicaMetricName(TopicPartition tp) {
        Map<String, String> metricTags = topicPartitionTags(tp);
        return this.metrics.metricInstance(metricsRegistry.partitionPreferredReadReplica, metricTags);
    }

    @Deprecated
    static Map<String, String> topicTags(String topic) {
        return Map.of("topic", topic.replace('.', '_'));
    }

    @Deprecated
    static Map<String, String> topicPartitionTags(TopicPartition tp) {
        return mkMap(mkEntry("topic", tp.topic().replace('.', '_')),
            mkEntry("partition", String.valueOf(tp.partition())));
    }
}