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
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.HashMap;
import java.util.LinkedHashMap;

/** Metrics for replicated topic-partitions */
class MirrorCheckpointMetrics implements AutoCloseable {

    private static final String CHECKPOINT_CONNECTOR_GROUP = MirrorCheckpointConnector.class.getSimpleName();

    private static final Set<String> GROUP_TAGS = new HashSet<>(Arrays.asList("source", "target", "group", "topic", "partition"));

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
    private final Map<String, GroupMetrics> groupMetrics = new HashMap<>();
    private final String source;
    private final String target;

    MirrorCheckpointMetrics(MirrorCheckpointTaskConfig taskConfig) {
        this.target = taskConfig.targetClusterAlias();
        this.source = taskConfig.sourceClusterAlias();
        this.metrics = new Metrics();

        // for side-effect
        metrics.sensor("record-count");
        metrics.sensor("byte-rate");
        metrics.sensor("record-age");
        metrics.sensor("replication-latency");
    }

    @Override
    public void close() {
        metrics.close();
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
