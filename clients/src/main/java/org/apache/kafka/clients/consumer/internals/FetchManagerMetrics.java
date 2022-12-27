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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Meter;
import org.apache.kafka.common.metrics.stats.Min;
import org.apache.kafka.common.metrics.stats.Value;
import org.apache.kafka.common.metrics.stats.WindowedCount;

public class FetchManagerMetrics {
    private final Metrics metrics;
    private final FetcherMetricsRegistry metricsRegistry;
    final Sensor bytesFetched;
    final Sensor recordsFetched;
    final Sensor fetchLatency;
    private final Sensor recordsFetchLag;
    private final Sensor recordsFetchLead;

    private int assignmentId = 0;
    private Set<TopicPartition> assignedPartitions = Collections.emptySet();

    FetchManagerMetrics(Metrics metrics, FetcherMetricsRegistry metricsRegistry) {
        this.metrics = metrics;
        this.metricsRegistry = metricsRegistry;

        this.bytesFetched = metrics.sensor("bytes-fetched");
        this.bytesFetched.add(metrics.metricInstance(metricsRegistry.fetchSizeAvg), new Avg());
        this.bytesFetched.add(metrics.metricInstance(metricsRegistry.fetchSizeMax), new Max());
        this.bytesFetched.add(new Meter(metrics.metricInstance(metricsRegistry.bytesConsumedRate),
            metrics.metricInstance(metricsRegistry.bytesConsumedTotal)));

        this.recordsFetched = metrics.sensor("records-fetched");
        this.recordsFetched.add(metrics.metricInstance(metricsRegistry.recordsPerRequestAvg), new Avg());
        this.recordsFetched.add(new Meter(metrics.metricInstance(metricsRegistry.recordsConsumedRate),
            metrics.metricInstance(metricsRegistry.recordsConsumedTotal)));

        this.fetchLatency = metrics.sensor("fetch-latency");
        this.fetchLatency.add(metrics.metricInstance(metricsRegistry.fetchLatencyAvg), new Avg());
        this.fetchLatency.add(metrics.metricInstance(metricsRegistry.fetchLatencyMax), new Max());
        this.fetchLatency.add(new Meter(new WindowedCount(), metrics.metricInstance(metricsRegistry.fetchRequestRate),
            metrics.metricInstance(metricsRegistry.fetchRequestTotal)));

        this.recordsFetchLag = metrics.sensor("records-lag");
        this.recordsFetchLag.add(metrics.metricInstance(metricsRegistry.recordsLagMax), new Max());

        this.recordsFetchLead = metrics.sensor("records-lead");
        this.recordsFetchLead.add(metrics.metricInstance(metricsRegistry.recordsLeadMin), new Min());
    }

    void recordTopicFetchMetrics(String topic, int bytes, int records) {
        // record bytes fetched
        String name = "topic." + topic + ".bytes-fetched";
        Sensor bytesFetched = this.metrics.getSensor(name);
        if (bytesFetched == null) {
            Map<String, String> metricTags = Collections.singletonMap("topic", topic.replace('.', '_'));

            bytesFetched = this.metrics.sensor(name);
            bytesFetched.add(this.metrics.metricInstance(metricsRegistry.topicFetchSizeAvg,
                metricTags), new Avg());
            bytesFetched.add(this.metrics.metricInstance(metricsRegistry.topicFetchSizeMax,
                metricTags), new Max());
            bytesFetched.add(new Meter(this.metrics.metricInstance(metricsRegistry.topicBytesConsumedRate, metricTags),
                this.metrics.metricInstance(metricsRegistry.topicBytesConsumedTotal, metricTags)));
        }
        bytesFetched.record(bytes);

        // record records fetched
        name = "topic." + topic + ".records-fetched";
        Sensor recordsFetched = this.metrics.getSensor(name);
        if (recordsFetched == null) {
            Map<String, String> metricTags = new HashMap<>(1);
            metricTags.put("topic", topic.replace('.', '_'));

            recordsFetched = this.metrics.sensor(name);
            recordsFetched.add(this.metrics.metricInstance(metricsRegistry.topicRecordsPerRequestAvg,
                metricTags), new Avg());
            recordsFetched.add(new Meter(this.metrics.metricInstance(metricsRegistry.topicRecordsConsumedRate, metricTags),
                this.metrics.metricInstance(metricsRegistry.topicRecordsConsumedTotal, metricTags)));
        }
        recordsFetched.record(records);
    }

    void maybeUpdateAssignment(SubscriptionState subscription) {
        int newAssignmentId = subscription.assignmentId();
        if (this.assignmentId != newAssignmentId) {
            Set<TopicPartition> newAssignedPartitions = subscription.assignedPartitions();
            for (TopicPartition tp : this.assignedPartitions) {
                if (!newAssignedPartitions.contains(tp)) {
                    metrics.removeSensor(partitionLagMetricName(tp));
                    metrics.removeSensor(partitionLeadMetricName(tp));
                    metrics.removeMetric(partitionPreferredReadReplicaMetricName(tp));
                }
            }

            for (TopicPartition tp : newAssignedPartitions) {
                if (!this.assignedPartitions.contains(tp)) {
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

    void recordPartitionLead(TopicPartition tp, long lead) {
        this.recordsFetchLead.record(lead);

        String name = partitionLeadMetricName(tp);
        Sensor recordsLead = this.metrics.getSensor(name);
        if (recordsLead == null) {
            Map<String, String> metricTags = topicPartitionTags(tp);

            recordsLead = this.metrics.sensor(name);

            recordsLead.add(this.metrics.metricInstance(metricsRegistry.partitionRecordsLead, metricTags), new Value());
            recordsLead.add(this.metrics.metricInstance(metricsRegistry.partitionRecordsLeadMin, metricTags), new Min());
            recordsLead.add(this.metrics.metricInstance(metricsRegistry.partitionRecordsLeadAvg, metricTags), new Avg());
        }
        recordsLead.record(lead);
    }

    void recordPartitionLag(TopicPartition tp, long lag) {
        this.recordsFetchLag.record(lag);

        String name = partitionLagMetricName(tp);
        Sensor recordsLag = this.metrics.getSensor(name);
        if (recordsLag == null) {
            Map<String, String> metricTags = topicPartitionTags(tp);
            recordsLag = this.metrics.sensor(name);

            recordsLag.add(this.metrics.metricInstance(metricsRegistry.partitionRecordsLag, metricTags), new Value());
            recordsLag.add(this.metrics.metricInstance(metricsRegistry.partitionRecordsLagMax, metricTags), new Max());
            recordsLag.add(this.metrics.metricInstance(metricsRegistry.partitionRecordsLagAvg, metricTags), new Avg());
        }
        recordsLag.record(lag);
    }

    private static String partitionLagMetricName(TopicPartition tp) {
        return tp + ".records-lag";
    }

    private static String partitionLeadMetricName(TopicPartition tp) {
        return tp + ".records-lead";
    }

    private MetricName partitionPreferredReadReplicaMetricName(TopicPartition tp) {
        Map<String, String> metricTags = topicPartitionTags(tp);
        return this.metrics.metricInstance(metricsRegistry.partitionPreferredReadReplica, metricTags);
    }

    private Map<String, String> topicPartitionTags(TopicPartition tp) {
        Map<String, String> metricTags = new HashMap<>(2);
        metricTags.put("topic", tp.topic().replace('.', '_'));
        metricTags.put("partition", String.valueOf(tp.partition()));
        return metricTags;
    }
}
