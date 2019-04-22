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
import org.apache.kafka.common.metrics.stats.Count;
import org.apache.kafka.common.metrics.stats.Value;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.metrics.stats.Min;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;

/** Metrics for replicated topic-partitions */
class MirrorMetrics {

    private static final String SOURCE_CONNECTOR_GROUP = MirrorSourceConnector.class.getSimpleName();

    private static final Set<String> TAGS = new HashSet<>(Arrays.asList("target", "topic", "partition"));
    
    private static final MetricNameTemplate RECORD_COUNT = new MetricNameTemplate(
            "record-count", SOURCE_CONNECTOR_GROUP,
            "Number of source records replicated to the target cluster.", TAGS);
    private static final MetricNameTemplate RECORD_AGE = new MetricNameTemplate(
            "record-age-ms", SOURCE_CONNECTOR_GROUP,
            "The age of incoming source records when replicated to the target cluster.", TAGS);
    private static final MetricNameTemplate RECORD_AGE_MAX = new MetricNameTemplate(
            "record-age-ms-max", SOURCE_CONNECTOR_GROUP,
            "The age of incoming source records when replicated to the target cluster.", TAGS);
    private static final MetricNameTemplate RECORD_AGE_MIN = new MetricNameTemplate(
            "record-age-ms-min", SOURCE_CONNECTOR_GROUP,
            "The age of incoming source records when replicated to the target cluster.", TAGS);
    private static final MetricNameTemplate RECORD_AGE_AVG = new MetricNameTemplate(
            "record-age-ms-avg", SOURCE_CONNECTOR_GROUP,
            "The age of incoming source records when replicated to the target cluster.", TAGS);
    private static final MetricNameTemplate BYTE_RATE = new MetricNameTemplate(
            "byte-rate", SOURCE_CONNECTOR_GROUP,
            "Average number of bytes replicated per second.", TAGS);
    private static final MetricNameTemplate REPLICATION_LATENCY = new MetricNameTemplate(
            "replication-latency-ms", SOURCE_CONNECTOR_GROUP,
            "Time it takes records to get replicated from source to target cluster.", TAGS);
    private static final MetricNameTemplate REPLICATION_LATENCY_MAX = new MetricNameTemplate(
            "replication-latency-ms-max", SOURCE_CONNECTOR_GROUP,
            "Time it takes records to get replicated from source to target cluster.", TAGS);
    private static final MetricNameTemplate REPLICATION_LATENCY_MIN = new MetricNameTemplate(
            "replication-latency-ms-min", SOURCE_CONNECTOR_GROUP,
            "Time it takes records to get replicated from source to target cluster.", TAGS);
    private static final MetricNameTemplate REPLICATION_LATENCY_AVG = new MetricNameTemplate(
            "replication-latency-ms-avg", SOURCE_CONNECTOR_GROUP,
            "Time it takes records to get replicated from source to target cluster.", TAGS);

    private final Metrics metrics; 
    private final ConcurrentMap<TopicPartition, PartitionMetrics> partitionMetrics = new ConcurrentHashMap<>();
    private final String target;

    MirrorMetrics(String target) {
        this.target = target;
        this.metrics = new Metrics();

        // for side-effect
        metrics.sensor("record-count");
        metrics.sensor("byte-rate");
        metrics.sensor("record-age");
        metrics.sensor("replication-latency");
    }

    void countRecord(TopicPartition topicPartition) {
        partitionMetrics(topicPartition).recordSensor.record();
    }

    void recordAge(TopicPartition topicPartition, long ageMillis) {
        partitionMetrics(topicPartition).recordAgeSensor.record((double) ageMillis);
    }

    void replicationLatency(TopicPartition topicPartition, long millis) {
        partitionMetrics(topicPartition).replicationLatencySensor.record((double) millis);
    }

    void recordBytes(TopicPartition topicPartition, long bytes) {
        partitionMetrics(topicPartition).byteRateSensor.record((double) bytes);
    }

    void addReporter(MetricsReporter reporter) {
        metrics.addReporter(reporter);
    }

    private PartitionMetrics partitionMetrics(TopicPartition topicPartition) {
        return partitionMetrics.computeIfAbsent(topicPartition, x -> new PartitionMetrics(x));
    }

    private class PartitionMetrics {
        private final Sensor recordSensor;
        private final Sensor byteRateSensor;
        private final Sensor recordAgeSensor;
        private final Sensor replicationLatencySensor;
     
        PartitionMetrics(TopicPartition topicPartition) {
            Map<String, String> tags = new LinkedHashMap<>();
            tags.put("target", target); 
            tags.put("topic", topicPartition.topic());
            tags.put("partition", Integer.toString(topicPartition.partition()));

            recordSensor = metrics.sensor("record-count");
            recordSensor.add(metrics.metricInstance(RECORD_COUNT, tags), new Count());

            byteRateSensor = metrics.sensor("byte-rate");
            byteRateSensor.add(metrics.metricInstance(BYTE_RATE, tags), new Rate());

            recordAgeSensor = metrics.sensor("record-age");
            recordAgeSensor.add(metrics.metricInstance(RECORD_AGE, tags), new Value());
            recordAgeSensor.add(metrics.metricInstance(RECORD_AGE_MAX, tags), new Max());
            recordAgeSensor.add(metrics.metricInstance(RECORD_AGE_MIN, tags), new Min());
            recordAgeSensor.add(metrics.metricInstance(RECORD_AGE_AVG, tags), new Avg());

            replicationLatencySensor = metrics.sensor("replication-latency");
            replicationLatencySensor.add(metrics.metricInstance(REPLICATION_LATENCY, tags), new Value());
            replicationLatencySensor.add(metrics.metricInstance(REPLICATION_LATENCY_MAX, tags), new Max());
            replicationLatencySensor.add(metrics.metricInstance(REPLICATION_LATENCY_MIN, tags), new Min());
            replicationLatencySensor.add(metrics.metricInstance(REPLICATION_LATENCY_AVG, tags), new Avg());
        }
    }
}
