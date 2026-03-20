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

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.PluginMetrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Meter;
import org.apache.kafka.common.metrics.stats.Min;
import org.apache.kafka.common.metrics.stats.Value;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class MirrorSourceMetrics {

    static final String RECORD_COUNT = "record-count";
    static final String RECORD_COUNT_DESCRIPTION = "Number of source records replicated to the target cluster.";
    static final String RECORD_RATE = "record-rate";
    static final String RECORD_RATE_DESCRIPTION = "Average number of source records replicated to the target cluster per second.";
    static final String RECORD_AGE_MS = "record-age-ms";
    static final String RECORD_AGE_MS_DESCRIPTION = "The age of incoming source records when replicated to the target cluster.";
    static final String RECORD_AGE_MS_MAX = "record-age-ms-max";
    static final String RECORD_AGE_MS_MAX_DESCRIPTION = "The max age of incoming source records when replicated to the target cluster.";
    static final String RECORD_AGE_MS_MIN = "record-age-ms-min";
    static final String RECORD_AGE_MS_MIN_DESCRIPTION = "The min age of incoming source records when replicated to the target cluster.";
    static final String RECORD_AGE_MS_AVG = "record-age-ms-avg";
    static final String RECORD_AGE_MS_AVG_DESCRIPTION = "The average age of incoming source records when replicated to the target cluster.";
    static final String BYTE_COUNT = "byte-count";
    static final String BYTE_COUNT_DESCRIPTION = "Number of bytes replicated to the target cluster.";
    static final String BYTE_RATE = "byte-rate";
    static final String BYTE_RATE_DESCRIPTION = "Average number of bytes replicated per second.";
    static final String REPLICATION_LATENCY_MS = "replication-latency-ms";
    static final String REPLICATION_LATENCY_MS_DESCRIPTION = "Time it takes records to replicate from source to target cluster.";
    static final String REPLICATION_LATENCY_MS_MAX = "replication-latency-ms-max";
    static final String REPLICATION_LATENCY_MS_MAX_DESCRIPTION = "Max time it takes records to replicate from source to target cluster.";
    static final String REPLICATION_LATENCY_MS_MIN = "replication-latency-ms-min";
    static final String REPLICATION_LATENCY_MS_MIN_DESCRIPTION = "Min time it takes records to replicate from source to target cluster.";
    static final String REPLICATION_LATENCY_MS_AVG = "replication-latency-ms-avg";
    static final String REPLICATION_LATENCY_MS_AVG_DESCRIPTION = "Average time it takes records to replicate from source to target cluster.";

    private final PluginMetrics metrics;
    private final Map<TopicPartition, PartitionMetrics> partitionMetrics;
    private final String source;
    private final String target;

    MirrorSourceMetrics(PluginMetrics pluginMetrics, MirrorSourceTaskConfig taskConfig) {
        this.metrics = pluginMetrics;
        this.target = taskConfig.targetClusterAlias();
        this.source = taskConfig.sourceClusterAlias();

        ReplicationPolicy replicationPolicy = taskConfig.replicationPolicy();
        partitionMetrics = taskConfig.taskTopicPartitions().stream()
                .map(x -> new TopicPartition(replicationPolicy.formatRemoteTopic(source, x.topic()), x.partition()))
                .collect(Collectors.toMap(x -> x, MirrorSourceMetrics.PartitionMetrics::new));

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

    private class PartitionMetrics {
        private final Sensor recordSensor;
        private final Sensor byteSensor;
        private final Sensor recordAgeSensor;
        private final Sensor replicationLatencySensor;

        PartitionMetrics(TopicPartition topicPartition) {
            String prefix = topicPartition.topic() + "-" + topicPartition.partition() + "-";

            LinkedHashMap<String, String> tags = new LinkedHashMap<>();
            tags.put("source", source);
            tags.put("target", target);
            tags.put("topic", topicPartition.topic());
            tags.put("partition", Integer.toString(topicPartition.partition()));

            MetricName recordCount = metrics.metricName(RECORD_COUNT, RECORD_COUNT_DESCRIPTION, tags);
            MetricName recordRate = metrics.metricName(RECORD_RATE, RECORD_RATE_DESCRIPTION, tags);
            MetricName recordAge = metrics.metricName(RECORD_AGE_MS, RECORD_AGE_MS_DESCRIPTION, tags);
            MetricName recordAgeMax = metrics.metricName(RECORD_AGE_MS_MAX, RECORD_AGE_MS_MAX_DESCRIPTION, tags);
            MetricName recordAgeMin = metrics.metricName(RECORD_AGE_MS_MIN, RECORD_AGE_MS_MIN_DESCRIPTION, tags);
            MetricName recordAgeAvg = metrics.metricName(RECORD_AGE_MS_AVG, RECORD_AGE_MS_AVG_DESCRIPTION, tags);
            MetricName byteCount = metrics.metricName(BYTE_COUNT, BYTE_COUNT_DESCRIPTION, tags);
            MetricName byteRate = metrics.metricName(BYTE_RATE, BYTE_RATE_DESCRIPTION, tags);
            MetricName replicationLatency = metrics.metricName(REPLICATION_LATENCY_MS, REPLICATION_LATENCY_MS_DESCRIPTION, tags);
            MetricName replicationLatencyMax = metrics.metricName(REPLICATION_LATENCY_MS_MAX, REPLICATION_LATENCY_MS_MAX_DESCRIPTION, tags);
            MetricName replicationLatencyMin = metrics.metricName(REPLICATION_LATENCY_MS_MIN, REPLICATION_LATENCY_MS_MIN_DESCRIPTION, tags);
            MetricName replicationLatencyAvg = metrics.metricName(REPLICATION_LATENCY_MS_AVG, REPLICATION_LATENCY_MS_AVG_DESCRIPTION, tags);

            recordSensor = metrics.addSensor(prefix + "records-sent");
            recordSensor.add(new Meter(recordRate, recordCount));

            byteSensor = metrics.addSensor(prefix + "bytes-sent");
            byteSensor.add(new Meter(byteRate, byteCount));

            recordAgeSensor = metrics.addSensor(prefix + "record-age");
            recordAgeSensor.add(recordAge, new Value());
            recordAgeSensor.add(recordAgeMax, new Max());
            recordAgeSensor.add(recordAgeMin, new Min());
            recordAgeSensor.add(recordAgeAvg, new Avg());

            replicationLatencySensor = metrics.addSensor(prefix + "replication-latency");
            replicationLatencySensor.add(replicationLatency, new Value());
            replicationLatencySensor.add(replicationLatencyMax, new Max());
            replicationLatencySensor.add(replicationLatencyMin, new Min());
            replicationLatencySensor.add(replicationLatencyAvg, new Avg());
        }
    }
}
