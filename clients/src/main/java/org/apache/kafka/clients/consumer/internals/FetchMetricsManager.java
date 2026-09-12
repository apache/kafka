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

import org.apache.kafka.clients.consumer.internals.metrics.AbstractConsumerMetricsManager;
import org.apache.kafka.clients.consumer.internals.metrics.MetricsLedger;
import org.apache.kafka.clients.consumer.internals.metrics.SensorBuilder;
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
 *
 * <p>Note: metric tag maps use {@code Utils.mkMap} to preserve insertion order; do not replace
 * with {@code Map.of} as tag order affects JMX MBean names.
 */
public class FetchMetricsManager extends AbstractConsumerMetricsManager {

    private final FetchMetricsRegistry metricsRegistry;
    private final Sensor throttleTime;
    private final Sensor bytesFetched;
    private final Sensor recordsFetched;
    private final Sensor fetchLatency;
    private final Sensor recordsLag;
    private final Sensor recordsLead;

    private int assignmentId = 0;
    private Set<TopicPartition> assignedPartitions = Collections.emptySet();

    @SuppressWarnings("this-escape")
    public FetchMetricsManager(Metrics metrics, FetchMetricsRegistry metricsRegistry) {
        this(new MetricsLedger(metrics), metricsRegistry);
    }

    @SuppressWarnings("this-escape")
    private FetchMetricsManager(MetricsLedger metrics, FetchMetricsRegistry metricsRegistry) {
        super(metrics);
        this.metricsRegistry = metricsRegistry;

        this.throttleTime = new SensorBuilder(metrics, "fetch-throttle-time")
                .withAvg(metricsRegistry.fetchThrottleTimeAvg)
                .withMax(metricsRegistry.fetchThrottleTimeMax)
                .build();
        this.bytesFetched = new SensorBuilder(metrics, "bytes-fetched")
                .withAvg(metricsRegistry.fetchSizeAvg)
                .withMax(metricsRegistry.fetchSizeMax)
                .withMeter(metricsRegistry.bytesConsumedRate, metricsRegistry.bytesConsumedTotal)
                .build();
        this.recordsFetched = new SensorBuilder(metrics, "records-fetched")
                .withAvg(metricsRegistry.recordsPerRequestAvg)
                .withMeter(metricsRegistry.recordsConsumedRate, metricsRegistry.recordsConsumedTotal)
                .build();
        this.fetchLatency = new SensorBuilder(metrics, "fetch-latency")
                .withAvg(metricsRegistry.fetchLatencyAvg)
                .withMax(metricsRegistry.fetchLatencyMax)
                .withMeter(new WindowedCount(), metricsRegistry.fetchRequestRate, metricsRegistry.fetchRequestTotal)
                .build();
        this.recordsLag = new SensorBuilder(metrics, "records-lag")
                .withMax(metricsRegistry.recordsLagMax)
                .build();
        this.recordsLead = new SensorBuilder(metrics, "records-lead")
                .withMin(metricsRegistry.recordsLeadMin)
                .build();
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
        maybeRecordDeprecatedBytesFetched(topic, bytes);
        maybeRemoveDeprecatedBytesFetched(topic);

        Sensor bytesFetched = new SensorBuilder(metrics, name, () -> Map.of("topic", topic))
            .withAvg(metricsRegistry.topicFetchSizeAvg)
            .withMax(metricsRegistry.topicFetchSizeMax)
            .withMeter(metricsRegistry.topicBytesConsumedRate, metricsRegistry.topicBytesConsumedTotal)
            .build();
        bytesFetched.record(bytes);
    }

    void recordRecordsFetched(String topic, int records) {
        String name = topicRecordsFetchedMetricName(topic);
        maybeRecordDeprecatedRecordsFetched(topic, records);
        maybeRemoveDeprecatedRecordsFetched(topic);

        Sensor recordsFetched = new SensorBuilder(metrics, name, () -> Map.of("topic", topic))
            .withAvg(metricsRegistry.topicRecordsPerRequestAvg)
            .withMeter(metricsRegistry.topicRecordsConsumedRate, metricsRegistry.topicRecordsConsumedTotal)
            .build();
        recordsFetched.record(records);
    }

    void recordPartitionLag(TopicPartition tp, long lag) {
        this.recordsLag.record(lag);

        String name = partitionRecordsLagMetricName(tp);
        maybeRecordDeprecatedPartitionLag(tp, lag);
        maybeRemoveDeprecatedPartitionLag(tp);

        Sensor recordsLag = new SensorBuilder(metrics, name, () -> mkMap(mkEntry("topic", tp.topic()), mkEntry("partition", String.valueOf(tp.partition()))))
            .withValue(metricsRegistry.partitionRecordsLag)
            .withMax(metricsRegistry.partitionRecordsLagMax)
            .withAvg(metricsRegistry.partitionRecordsLagAvg)
            .build();

        recordsLag.record(lag);
    }

    void recordPartitionLead(TopicPartition tp, long lead) {
        this.recordsLead.record(lead);

        String name = partitionRecordsLeadMetricName(tp);
        maybeRecordDeprecatedPartitionLead(tp, lead);
        maybeRemoveDeprecatedPartitionLead(tp);

        Sensor recordsLead = new SensorBuilder(metrics, name, () -> mkMap(mkEntry("topic", tp.topic()), mkEntry("partition", String.valueOf(tp.partition()))))
            .withValue(metricsRegistry.partitionRecordsLead)
            .withMin(metricsRegistry.partitionRecordsLeadMin)
            .withAvg(metricsRegistry.partitionRecordsLeadAvg)
            .build();

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
                    metrics.removeSensor(deprecatedPartitionRecordsLagMetricName(tp));
                    metrics.removeSensor(deprecatedPartitionRecordsLeadMetricName(tp));
                    if (!newAssignedPartitions.contains(deprecatedTopicPartition(tp))) {
                        metrics.removeMetric(deprecatedPartitionPreferredReadReplicaMetricName(tp));
                    }
                }
            }

            for (TopicPartition tp : newAssignedPartitions) {
                if (!this.assignedPartitions.contains(tp)) {
                    maybeRemoveDeprecatedPreferredReadReplica(tp);

                    MetricName metricName = partitionPreferredReadReplicaMetricName(tp);
                    metrics.addMetricIfAbsent(
                        metricName,
                        null,
                        (Gauge<Integer>) (config, now) -> subscription.preferredReadReplica(tp, 0L).orElse(-1)
                    );
                }
            }

            for (TopicPartition tp : newAssignedPartitions) {
                maybeRecordDeprecatedPreferredReadReplica(tp, subscription, newAssignedPartitions);
            }

            this.assignedPartitions = newAssignedPartitions;
            this.assignmentId = newAssignmentId;
        }
    }

    @Deprecated // To be removed in Kafka 5.0 release.
    private void maybeRecordDeprecatedBytesFetched(String topic, int bytes) {
        if (shouldReportDeprecatedMetric(topic) &&
                metrics.getSensor(topicBytesFetchedMetricName(topic.replace('.', '_'))) == null) {
            Sensor deprecatedBytesFetched = new SensorBuilder(
                    metrics,
                    deprecatedTopicBytesFetchedMetricName(topic),
                    () -> topicTags(topic))
                .withAvg(metricsRegistry.topicFetchSizeAvg)
                .withMax(metricsRegistry.topicFetchSizeMax)
                .withMeter(metricsRegistry.topicBytesConsumedRate, metricsRegistry.topicBytesConsumedTotal)
                .build();
            deprecatedBytesFetched.record(bytes);
        }
    }

    @Deprecated // To be removed in Kafka 5.0 release.
    private void maybeRecordDeprecatedRecordsFetched(String topic, int records) {
        if (shouldReportDeprecatedMetric(topic) &&
                metrics.getSensor(topicRecordsFetchedMetricName(topic.replace('.', '_'))) == null) {
            Sensor deprecatedRecordsFetched = new SensorBuilder(
                    metrics,
                    deprecatedTopicRecordsFetchedMetricName(topic),
                    () -> topicTags(topic))
                .withAvg(metricsRegistry.topicRecordsPerRequestAvg)
                .withMeter(metricsRegistry.topicRecordsConsumedRate, metricsRegistry.topicRecordsConsumedTotal)
                .build();
            deprecatedRecordsFetched.record(records);
        }
    }

    @Deprecated // To be removed in Kafka 5.0 release.
    private void maybeRecordDeprecatedPartitionLag(TopicPartition tp, long lag) {
        if (shouldReportDeprecatedMetric(tp.topic()) &&
                metrics.getSensor(partitionRecordsLagMetricName(deprecatedTopicPartition(tp))) == null) {
            Sensor deprecatedRecordsLag = new SensorBuilder(
                    metrics,
                    deprecatedPartitionRecordsLagMetricName(tp),
                    () -> topicPartitionTags(tp))
                .withValue(metricsRegistry.partitionRecordsLag)
                .withMax(metricsRegistry.partitionRecordsLagMax)
                .withAvg(metricsRegistry.partitionRecordsLagAvg)
                .build();

            deprecatedRecordsLag.record(lag);
        }
    }

    @Deprecated // To be removed in Kafka 5.0 release.
    private void maybeRecordDeprecatedPartitionLead(TopicPartition tp, double lead) {
        if (shouldReportDeprecatedMetric(tp.topic()) &&
                metrics.getSensor(partitionRecordsLeadMetricName(deprecatedTopicPartition(tp))) == null) {
            Sensor deprecatedRecordsLead = new SensorBuilder(
                    metrics,
                    deprecatedPartitionRecordsLeadMetricName(tp),
                    () -> topicPartitionTags(tp))
                .withValue(metricsRegistry.partitionRecordsLead)
                .withMin(metricsRegistry.partitionRecordsLeadMin)
                .withAvg(metricsRegistry.partitionRecordsLeadAvg)
                .build();

            deprecatedRecordsLead.record(lead);
        }
    }

    @Deprecated // To be removed in Kafka 5.0 release.
    private void maybeRecordDeprecatedPreferredReadReplica(
            TopicPartition tp,
            SubscriptionState subscription,
            Set<TopicPartition> assignedPartitions) {
        if (shouldReportDeprecatedMetric(tp.topic()) && !assignedPartitions.contains(deprecatedTopicPartition(tp))) {
            MetricName metricName = deprecatedPartitionPreferredReadReplicaMetricName(tp);
            metrics.addMetricIfAbsent(
                metricName,
                null,
                (Gauge<Integer>) (config, now) -> subscription.preferredReadReplica(tp, 0L).orElse(-1)
            );
        }
    }

    @Deprecated // To be removed in Kafka 5.0 release.
    private void maybeRemoveDeprecatedBytesFetched(String topic) {
        if (!shouldReportDeprecatedMetric(topic)) {
            metrics.removeSensor(deprecatedTopicBytesFetchedMetricName(topic));
        }
    }

    @Deprecated // To be removed in Kafka 5.0 release.
    private void maybeRemoveDeprecatedRecordsFetched(String topic) {
        if (!shouldReportDeprecatedMetric(topic)) {
            metrics.removeSensor(deprecatedTopicRecordsFetchedMetricName(topic));
        }
    }

    @Deprecated // To be removed in Kafka 5.0 release.
    private void maybeRemoveDeprecatedPartitionLag(TopicPartition tp) {
        if (!shouldReportDeprecatedMetric(tp.topic())) {
            metrics.removeSensor(deprecatedPartitionRecordsLagMetricName(tp));
        }
    }

    @Deprecated // To be removed in Kafka 5.0 release.
    private void maybeRemoveDeprecatedPartitionLead(TopicPartition tp) {
        if (!shouldReportDeprecatedMetric(tp.topic())) {
            metrics.removeSensor(deprecatedPartitionRecordsLeadMetricName(tp));
        }
    }

    @Deprecated // To be removed in Kafka 5.0 release.
    private void maybeRemoveDeprecatedPreferredReadReplica(TopicPartition tp) {
        if (!shouldReportDeprecatedMetric(tp.topic())) {
            metrics.removeMetric(deprecatedPartitionPreferredReadReplicaMetricName(tp));
        }
    }

    private static String topicBytesFetchedMetricName(String topic) {
        return "topic." + topic + ".bytes-fetched";
    }

    private static String topicRecordsFetchedMetricName(String topic) {
        return "topic." + topic + ".records-fetched";
    }

    @Deprecated
    private static String deprecatedTopicBytesFetchedMetricName(String topic) {
        return deprecatedMetricName(topicBytesFetchedMetricName(topic.replace('.', '_')));
    }

    @Deprecated
    private static String deprecatedTopicRecordsFetchedMetricName(String topic) {
        return deprecatedMetricName(topicRecordsFetchedMetricName(topic.replace('.', '_')));
    }

    private static String partitionRecordsLeadMetricName(TopicPartition tp) {
        return tp + ".records-lead";
    }

    private static String partitionRecordsLagMetricName(TopicPartition tp) {
        return tp + ".records-lag";
    }

    @Deprecated
    private static String deprecatedPartitionRecordsLeadMetricName(TopicPartition tp) {
        return deprecatedMetricName(partitionRecordsLeadMetricName(deprecatedTopicPartition(tp)));
    }

    @Deprecated
    private static String deprecatedPartitionRecordsLagMetricName(TopicPartition tp) {
        return deprecatedMetricName(partitionRecordsLagMetricName(deprecatedTopicPartition(tp)));
    }

    @Deprecated
    private static TopicPartition deprecatedTopicPartition(TopicPartition tp) {
        return new TopicPartition(tp.topic().replace('.', '_'), tp.partition());
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
