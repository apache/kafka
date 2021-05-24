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
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Value;
import org.apache.kafka.common.metrics.stats.Min;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Meter;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.stream.Collectors;

/** Metrics for replicated topic-partitions */
class MirrorMetrics implements AutoCloseable {

    private static final String SOURCE_CONNECTOR_GROUP = MirrorSourceConnector.class.getSimpleName();
    private static final String CHECKPOINT_CONNECTOR_GROUP = MirrorCheckpointConnector.class.getSimpleName();

    private static final Set<String> PARTITION_TAGS = new HashSet<>(Arrays.asList("target", "topic", "partition"));
    private static final Set<String> GROUP_TAGS = new HashSet<>(Arrays.asList("source", "target", "group", "topic", "partition"));
    
    private static final MetricNameTemplate RECORD_COUNT = new MetricNameTemplate(
            "record-count", SOURCE_CONNECTOR_GROUP,
            "Number of source records replicated to the target cluster.", PARTITION_TAGS);
    private static final MetricNameTemplate RECORD_RATE = new MetricNameTemplate(
            "record-rate", SOURCE_CONNECTOR_GROUP,
            "Average number of source records replicated to the target cluster per second.", PARTITION_TAGS);
    private static final MetricNameTemplate RECORD_AGE = new MetricNameTemplate(
            "record-age-ms", SOURCE_CONNECTOR_GROUP,
            "The age of incoming source records when replicated to the target cluster.", PARTITION_TAGS);
    private static final MetricNameTemplate RECORD_AGE_MAX = new MetricNameTemplate(
            "record-age-ms-max", SOURCE_CONNECTOR_GROUP,
            "The max age of incoming source records when replicated to the target cluster.", PARTITION_TAGS);
    private static final MetricNameTemplate RECORD_AGE_MIN = new MetricNameTemplate(
            "record-age-ms-min", SOURCE_CONNECTOR_GROUP,
            "The min age of incoming source records when replicated to the target cluster.", PARTITION_TAGS);
    private static final MetricNameTemplate RECORD_AGE_AVG = new MetricNameTemplate(
            "record-age-ms-avg", SOURCE_CONNECTOR_GROUP,
            "The average age of incoming source records when replicated to the target cluster.", PARTITION_TAGS);
    private static final MetricNameTemplate BYTE_COUNT = new MetricNameTemplate(
            "byte-count", SOURCE_CONNECTOR_GROUP,
            "Number of bytes replicated to the target cluster.", PARTITION_TAGS);
    private static final MetricNameTemplate BYTE_RATE = new MetricNameTemplate(
            "byte-rate", SOURCE_CONNECTOR_GROUP,
            "Average number of bytes replicated per second.", PARTITION_TAGS);
    private static final MetricNameTemplate REPLICATION_LATENCY = new MetricNameTemplate(
            "replication-latency-ms", SOURCE_CONNECTOR_GROUP,
            "Time it takes records to replicate from source to target cluster.", PARTITION_TAGS);
    private static final MetricNameTemplate REPLICATION_LATENCY_MAX = new MetricNameTemplate(
            "replication-latency-ms-max", SOURCE_CONNECTOR_GROUP,
            "Max time it takes records to replicate from source to target cluster.", PARTITION_TAGS);
    private static final MetricNameTemplate REPLICATION_LATENCY_MIN = new MetricNameTemplate(
            "replication-latency-ms-min", SOURCE_CONNECTOR_GROUP,
            "Min time it takes records to replicate from source to target cluster.", PARTITION_TAGS);
    private static final MetricNameTemplate REPLICATION_LATENCY_AVG = new MetricNameTemplate(
            "replication-latency-ms-avg", SOURCE_CONNECTOR_GROUP,
            "Average time it takes records to replicate from source to target cluster.", PARTITION_TAGS);

    private static final MetricNameTemplate CHECKPOINT_LATENCY = new MetricNameTemplate(
            "checkpoint-latency-ms", CHECKPOINT_CONNECTOR_GROUP,
            "Time it takes consumer group offsets to replicate from source to target cluster.", GROUP_TAGS);
    private static final MetricNameTemplate CHECKPOINT_LATENCY_MAX = new MetricNameTemplate(
            "checkpoint-latency-ms-max", CHECKPOINT_CONNECTOR_GROUP,
            "Max time it takes consumer group offsets to replicate from source to target cluster.", GROUP_TAGS);
    private static final MetricNameTemplate CHECKPOINT_LATENCY_MIN = new MetricNameTemplate(
            "checkpoint-latency-ms-min", CHECKPOINT_CONNECTOR_GROUP,
            "Min time it takes consumer group offsets to replicate from source to target cluster.", GROUP_TAGS);
    private static final MetricNameTemplate CHECKPOINT_LATENCY_AVG = new MetricNameTemplate(
            "checkpoint-latency-ms-avg", CHECKPOINT_CONNECTOR_GROUP,
            "Average time it takes consumer group offsets to replicate from source to target cluster.", GROUP_TAGS);


    private final Metrics metrics; 
    private final Map<TopicPartition, PartitionMetrics> partitionMetrics; 
    private final Map<String, GroupMetrics> groupMetrics = new HashMap<>();
    private final String source;
    private final String target;

    MirrorMetrics(MirrorTaskConfig taskConfig) {
        this.target = taskConfig.targetClusterAlias();
        this.source = taskConfig.sourceClusterAlias();
        this.metrics = new Metrics();

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

    void checkpointLatency(TopicPartition topicPartition, String group, long millis) {
        group(topicPartition, group).checkpointLatencySensor.record((double) millis);
    }

    GroupMetrics group(TopicPartition topicPartition, String group) {
        return groupMetrics.computeIfAbsent(String.join("-", topicPartition.toString(), group),
            x -> new GroupMetrics(topicPartition, group));
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
            tags.put("target", target); 
            tags.put("topic", topicPartition.topic());
            tags.put("partition", Integer.toString(topicPartition.partition()));

            recordSensor = metrics.sensor(prefix + "records-sent");
            recordSensor.add(new Meter(metrics.metricInstance(RECORD_RATE, tags), metrics.metricInstance(RECORD_COUNT, tags)));

            byteSensor = metrics.sensor(prefix + "bytes-sent");
            byteSensor.add(new Meter(metrics.metricInstance(BYTE_RATE, tags), metrics.metricInstance(BYTE_COUNT, tags)));

            recordAgeSensor = metrics.sensor(prefix + "record-age");
            recordAgeSensor.add(metrics.metricInstance(RECORD_AGE, tags), new Value());
            recordAgeSensor.add(metrics.metricInstance(RECORD_AGE_MAX, tags), new Max());
            recordAgeSensor.add(metrics.metricInstance(RECORD_AGE_MIN, tags), new Min());
            recordAgeSensor.add(metrics.metricInstance(RECORD_AGE_AVG, tags), new Avg());

            replicationLatencySensor = metrics.sensor(prefix + "replication-latency");
            replicationLatencySensor.add(metrics.metricInstance(REPLICATION_LATENCY, tags), new Value());
            replicationLatencySensor.add(metrics.metricInstance(REPLICATION_LATENCY_MAX, tags), new Max());
            replicationLatencySensor.add(metrics.metricInstance(REPLICATION_LATENCY_MIN, tags), new Min());
            replicationLatencySensor.add(metrics.metricInstance(REPLICATION_LATENCY_AVG, tags), new Avg());
        }
    }

    private class GroupMetrics {
        private final Sensor checkpointLatencySensor;

        GroupMetrics(TopicPartition topicPartition, String group) {
            Map<String, String> tags = new LinkedHashMap<>();
            tags.put("source", source); 
            tags.put("target", target); 
            tags.put("group", group);
            tags.put("topic", topicPartition.topic());
            tags.put("partition", Integer.toString(topicPartition.partition()));
 
            checkpointLatencySensor = metrics.sensor("checkpoint-latency");
            checkpointLatencySensor.add(metrics.metricInstance(CHECKPOINT_LATENCY, tags), new Value());
            checkpointLatencySensor.add(metrics.metricInstance(CHECKPOINT_LATENCY_MAX, tags), new Max());
            checkpointLatencySensor.add(metrics.metricInstance(CHECKPOINT_LATENCY_MIN, tags), new Min());
            checkpointLatencySensor.add(metrics.metricInstance(CHECKPOINT_LATENCY_AVG, tags), new Avg());
        }
    }
}
