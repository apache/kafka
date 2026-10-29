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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.kafka.connect.mirror.MirrorSourceMetrics.BYTE_COUNT_DESCRIPTION;
import static org.apache.kafka.connect.mirror.MirrorSourceMetrics.BYTE_RATE_DESCRIPTION;
import static org.apache.kafka.connect.mirror.MirrorSourceMetrics.RECORD_AGE_MS;
import static org.apache.kafka.connect.mirror.MirrorSourceMetrics.RECORD_AGE_MS_AVG;
import static org.apache.kafka.connect.mirror.MirrorSourceMetrics.RECORD_AGE_MS_AVG_DESCRIPTION;
import static org.apache.kafka.connect.mirror.MirrorSourceMetrics.RECORD_AGE_MS_DESCRIPTION;
import static org.apache.kafka.connect.mirror.MirrorSourceMetrics.RECORD_AGE_MS_MAX;
import static org.apache.kafka.connect.mirror.MirrorSourceMetrics.RECORD_AGE_MS_MAX_DESCRIPTION;
import static org.apache.kafka.connect.mirror.MirrorSourceMetrics.RECORD_AGE_MS_MIN;
import static org.apache.kafka.connect.mirror.MirrorSourceMetrics.RECORD_AGE_MS_MIN_DESCRIPTION;
import static org.apache.kafka.connect.mirror.MirrorSourceMetrics.RECORD_COUNT_DESCRIPTION;
import static org.apache.kafka.connect.mirror.MirrorSourceMetrics.RECORD_RATE_DESCRIPTION;
import static org.apache.kafka.connect.mirror.MirrorSourceMetrics.REPLICATION_LATENCY_MS;
import static org.apache.kafka.connect.mirror.MirrorSourceMetrics.REPLICATION_LATENCY_MS_AVG;
import static org.apache.kafka.connect.mirror.MirrorSourceMetrics.REPLICATION_LATENCY_MS_AVG_DESCRIPTION;
import static org.apache.kafka.connect.mirror.MirrorSourceMetrics.REPLICATION_LATENCY_MS_DESCRIPTION;
import static org.apache.kafka.connect.mirror.MirrorSourceMetrics.REPLICATION_LATENCY_MS_MAX;
import static org.apache.kafka.connect.mirror.MirrorSourceMetrics.REPLICATION_LATENCY_MS_MAX_DESCRIPTION;
import static org.apache.kafka.connect.mirror.MirrorSourceMetrics.REPLICATION_LATENCY_MS_MIN;
import static org.apache.kafka.connect.mirror.MirrorSourceMetrics.REPLICATION_LATENCY_MS_MIN_DESCRIPTION;

/** Metrics for replicated topic-partitions */
class MirrorSourceLegacyMetrics implements AutoCloseable {

    private static final String SOURCE_CONNECTOR_GROUP = MirrorSourceConnector.class.getSimpleName();

    private static final Set<String> PARTITION_TAGS = Set.of("source", "target", "topic", "partition");
    private static final MetricNameTemplate RECORD_COUNT = new MetricNameTemplate(
            MirrorSourceMetrics.RECORD_COUNT, SOURCE_CONNECTOR_GROUP,
            RECORD_COUNT_DESCRIPTION, PARTITION_TAGS);
    private static final MetricNameTemplate RECORD_RATE = new MetricNameTemplate(
            MirrorSourceMetrics.RECORD_RATE, SOURCE_CONNECTOR_GROUP,
            RECORD_RATE_DESCRIPTION, PARTITION_TAGS);
    private static final MetricNameTemplate RECORD_AGE = new MetricNameTemplate(
            RECORD_AGE_MS, SOURCE_CONNECTOR_GROUP,
            RECORD_AGE_MS_DESCRIPTION, PARTITION_TAGS);
    private static final MetricNameTemplate RECORD_AGE_MAX = new MetricNameTemplate(
            RECORD_AGE_MS_MAX, SOURCE_CONNECTOR_GROUP,
            RECORD_AGE_MS_MAX_DESCRIPTION, PARTITION_TAGS);
    private static final MetricNameTemplate RECORD_AGE_MIN = new MetricNameTemplate(
            RECORD_AGE_MS_MIN, SOURCE_CONNECTOR_GROUP,
            RECORD_AGE_MS_MIN_DESCRIPTION, PARTITION_TAGS);
    private static final MetricNameTemplate RECORD_AGE_AVG = new MetricNameTemplate(
            RECORD_AGE_MS_AVG, SOURCE_CONNECTOR_GROUP,
            RECORD_AGE_MS_AVG_DESCRIPTION, PARTITION_TAGS);
    private static final MetricNameTemplate BYTE_COUNT = new MetricNameTemplate(
            MirrorSourceMetrics.BYTE_COUNT, SOURCE_CONNECTOR_GROUP,
            BYTE_COUNT_DESCRIPTION, PARTITION_TAGS);
    private static final MetricNameTemplate BYTE_RATE = new MetricNameTemplate(
            MirrorSourceMetrics.BYTE_RATE, SOURCE_CONNECTOR_GROUP,
            BYTE_RATE_DESCRIPTION, PARTITION_TAGS);
    private static final MetricNameTemplate REPLICATION_LATENCY = new MetricNameTemplate(
            REPLICATION_LATENCY_MS, SOURCE_CONNECTOR_GROUP,
            REPLICATION_LATENCY_MS_DESCRIPTION, PARTITION_TAGS);
    private static final MetricNameTemplate REPLICATION_LATENCY_MAX = new MetricNameTemplate(
            REPLICATION_LATENCY_MS_MAX, SOURCE_CONNECTOR_GROUP,
            REPLICATION_LATENCY_MS_MAX_DESCRIPTION, PARTITION_TAGS);
    private static final MetricNameTemplate REPLICATION_LATENCY_MIN = new MetricNameTemplate(
            REPLICATION_LATENCY_MS_MIN, SOURCE_CONNECTOR_GROUP,
            REPLICATION_LATENCY_MS_MIN_DESCRIPTION, PARTITION_TAGS);
    private static final MetricNameTemplate REPLICATION_LATENCY_AVG = new MetricNameTemplate(
            REPLICATION_LATENCY_MS_AVG, SOURCE_CONNECTOR_GROUP,
            REPLICATION_LATENCY_MS_AVG_DESCRIPTION, PARTITION_TAGS);

    private final Metrics metrics;
    private final Map<TopicPartition, PartitionMetrics> partitionMetrics;
    private final String source;
    private final String target;

    MirrorSourceLegacyMetrics(MirrorSourceTaskConfig taskConfig) {
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
}
