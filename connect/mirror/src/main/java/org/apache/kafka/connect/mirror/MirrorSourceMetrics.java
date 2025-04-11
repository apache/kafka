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
package org.apache.kafka.connect.mirror;

import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Meter;
import org.apache.kafka.common.metrics.stats.Min;
import org.apache.kafka.common.metrics.stats.Value;

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Metrics for replicated topic-partitions */
class MirrorSourceMetrics implements AutoCloseable {

    private static final String SOURCE_CONNECTOR_GROUP = MirrorSourceConnector.class.getSimpleName();

    private final MetricNameTemplate recordCount;
    private final MetricNameTemplate recordRate;
    private final MetricNameTemplate recordAge;
    private final MetricNameTemplate recordAgeMax;
    private final MetricNameTemplate recordAgeMin;
    private final MetricNameTemplate recordAgeAvg;
    private final MetricNameTemplate byteCount;
    private final MetricNameTemplate byteRate;
    private final MetricNameTemplate replicationLatency;
    private final MetricNameTemplate replicationLatencyMax;
    private final MetricNameTemplate replicationLatencyMin;
    private final MetricNameTemplate replicationLatencyAvg;

    private final Metrics metrics;
    private final Map<TopicPartition, PartitionMetrics> partitionMetrics;
    private final String source;
    private final String target;

    MirrorSourceMetrics(MirrorSourceTaskConfig taskConfig) {
        this.target = taskConfig.targetClusterAlias();
        this.source = taskConfig.sourceClusterAlias();
        this.metrics = new Metrics();

        Set<String> partitionTags = new HashSet<>(Arrays.asList("source", "target", "topic", "partition"));

        recordCount = new MetricNameTemplate(
                "record-count", SOURCE_CONNECTOR_GROUP,
                "Number of source records replicated to the target cluster.", partitionTags);
        recordRate = new MetricNameTemplate(
                "record-rate", SOURCE_CONNECTOR_GROUP,
                "Average number of source records replicated to the target cluster per second.", partitionTags);
        recordAge = new MetricNameTemplate(
                "record-age-ms", SOURCE_CONNECTOR_GROUP,
                "The age of incoming source records when replicated to the target cluster.", partitionTags);
        recordAgeMax = new MetricNameTemplate(
                "record-age-ms-max", SOURCE_CONNECTOR_GROUP,
                "The max age of incoming source records when replicated to the target cluster.", partitionTags);
        recordAgeMin = new MetricNameTemplate(
                "record-age-ms-min", SOURCE_CONNECTOR_GROUP,
                "The min age of incoming source records when replicated to the target cluster.", partitionTags);
        recordAgeAvg = new MetricNameTemplate(
                "record-age-ms-avg", SOURCE_CONNECTOR_GROUP,
                "The average age of incoming source records when replicated to the target cluster.", partitionTags);
        byteCount = new MetricNameTemplate(
                "byte-count", SOURCE_CONNECTOR_GROUP,
                "Number of bytes replicated to the target cluster.", partitionTags);
        byteRate = new MetricNameTemplate(
                "byte-rate", SOURCE_CONNECTOR_GROUP,
                "Average number of bytes replicated per second.", partitionTags);
        replicationLatency = new MetricNameTemplate(
                "replication-latency-ms", SOURCE_CONNECTOR_GROUP,
                "Time it takes records to replicate from source to target cluster.", partitionTags);
        replicationLatencyMax = new MetricNameTemplate(
                "replication-latency-ms-max", SOURCE_CONNECTOR_GROUP,
                "Max time it takes records to replicate from source to target cluster.", partitionTags);
        replicationLatencyMin = new MetricNameTemplate(
                "replication-latency-ms-min", SOURCE_CONNECTOR_GROUP,
                "Min time it takes records to replicate from source to target cluster.", partitionTags);
        replicationLatencyAvg = new MetricNameTemplate(
                "replication-latency-ms-avg", SOURCE_CONNECTOR_GROUP,
                "Average time it takes records to replicate from source to target cluster.", partitionTags);

        // for side-effect
        metrics.sensor("record-count");
        metrics.sensor("byte-rate");
        metrics.sensor("record-age");
        metrics.sensor("replication-latency");

        ReplicationPolicy replicationPolicy = taskConfig.replicationPolicy();
        partitionMetrics = taskConfig.taskTopicPartitions().stream()
            .map(x -> new TopicPartition(replicationPolicy.formatRemoteTopic(source, x.topic()), x.partition()))
            .collect(Collectors.toMap(x -> x, PartitionMetrics::new));

    }

    @Override
    public void close() {
        metrics.close();
    }

    void countRecord(TopicPartition topicPartition) {
        partitionMetrics.get(topicPartition).recordSensor.record();
    }

    void recordAge(TopicPartition topicPartition, long ageMillis) {
        partitionMetrics.get(topicPartition).recordAgeSensor.record((double) ageMillis);
    }

    void replicationLatency(TopicPartition topicPartition, long millis) {
        partitionMetrics.get(topicPartition).replicationLatencySensor.record((double) millis);
    }

    void recordBytes(TopicPartition topicPartition, long bytes) {
        partitionMetrics.get(topicPartition).byteSensor.record((double) bytes);
    }

    void addReporter(MetricsReporter reporter) {
        metrics.addReporter(reporter);
    }

    private class PartitionMetrics {
        private final Sensor recordSensor;
        private final Sensor byteSensor;
        private final Sensor recordAgeSensor;
        private final Sensor replicationLatencySensor;

        PartitionMetrics(TopicPartition topicPartition) {
            String prefix = topicPartition.topic() + "-" + topicPartition.partition() + "-";

            Map<String, String> tags = new LinkedHashMap<>();
            tags.put("source", source);
            tags.put("target", target); 
            tags.put("topic", topicPartition.topic());
            tags.put("partition", Integer.toString(topicPartition.partition()));

            recordSensor = metrics.sensor(prefix + "records-sent");
            recordSensor.add(new Meter(metrics.metricInstance(recordRate, tags), metrics.metricInstance(recordCount, tags)));

            byteSensor = metrics.sensor(prefix + "bytes-sent");
            byteSensor.add(new Meter(metrics.metricInstance(byteRate, tags), metrics.metricInstance(byteCount, tags)));

            recordAgeSensor = metrics.sensor(prefix + "record-age");
            recordAgeSensor.add(metrics.metricInstance(recordAge, tags), new Value());
            recordAgeSensor.add(metrics.metricInstance(recordAgeMax, tags), new Max());
            recordAgeSensor.add(metrics.metricInstance(recordAgeMin, tags), new Min());
            recordAgeSensor.add(metrics.metricInstance(recordAgeAvg, tags), new Avg());

            replicationLatencySensor = metrics.sensor(prefix + "replication-latency");
            replicationLatencySensor.add(metrics.metricInstance(replicationLatency, tags), new Value());
            replicationLatencySensor.add(metrics.metricInstance(replicationLatencyMax, tags), new Max());
            replicationLatencySensor.add(metrics.metricInstance(replicationLatencyMin, tags), new Min());
            replicationLatencySensor.add(metrics.metricInstance(replicationLatencyAvg, tags), new Avg());
        }
    }
}
