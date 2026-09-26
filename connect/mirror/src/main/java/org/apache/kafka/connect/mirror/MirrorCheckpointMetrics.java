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
import org.apache.kafka.common.metrics.stats.Min;
import org.apache.kafka.common.metrics.stats.Value;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class MirrorCheckpointMetrics {

    static final String LATENCY_MS = "checkpoint-latency-ms";
    static final String LATENCY_MS_DESCRIPTION = "Time it takes consumer group offsets to replicate from source to target cluster.";
    static final String LATENCY_MS_MAX = "checkpoint-latency-ms-max";
    static final String LATENCY_MS_MAX_DESCRIPTION = "Max time it takes consumer group offsets to replicate from source to target cluster.";
    static final String LATENCY_MS_MIN = "checkpoint-latency-ms-min";
    static final String LATENCY_MS_MIN_DESCRIPTION = "Min time it takes consumer group offsets to replicate from source to target cluster.";
    static final String LATENCY_MS_AVG = "checkpoint-latency-ms-avg";
    static final String LATENCY_MS_AVG_DESCRIPTION = "Average time it takes consumer group offsets to replicate from source to target cluster.";

    private final Map<String, GroupMetrics> groupMetrics = new HashMap<>();
    private final PluginMetrics metrics;
    private final String source;
    private final String target;

    MirrorCheckpointMetrics(PluginMetrics metrics, MirrorCheckpointTaskConfig taskConfig) {
        this.metrics = metrics;
        this.source = taskConfig.sourceClusterAlias();
        this.target = taskConfig.targetClusterAlias();
    }

    void checkpointLatency(TopicPartition topicPartition, String group, long millis) {
        group(topicPartition, group).checkpointLatencySensor.record((double) millis);
    }

    GroupMetrics group(TopicPartition topicPartition, String group) {
        return groupMetrics.computeIfAbsent(String.join("-", topicPartition.toString(), group),
                x -> new GroupMetrics(topicPartition, group));
    }

    private class GroupMetrics {
        private final Sensor checkpointLatencySensor;

        GroupMetrics(TopicPartition topicPartition, String group) {
            LinkedHashMap<String, String> tags = groupTags(source, target, group, topicPartition);

            checkpointLatencySensor = metrics.addSensor("checkpoint-latency");
            MetricName checkpointLatency = metrics.metricName(LATENCY_MS, LATENCY_MS_DESCRIPTION, tags);
            MetricName checkpointLatencyMax = metrics.metricName(LATENCY_MS_MAX, LATENCY_MS_MAX_DESCRIPTION, tags);
            MetricName checkpointLatencyMin = metrics.metricName(LATENCY_MS_MIN, LATENCY_MS_MIN_DESCRIPTION, tags);
            MetricName checkpointLatencyAvg = metrics.metricName(LATENCY_MS_AVG, LATENCY_MS_AVG_DESCRIPTION, tags);
            checkpointLatencySensor.add(checkpointLatency, new Value());
            checkpointLatencySensor.add(checkpointLatencyMax, new Max());
            checkpointLatencySensor.add(checkpointLatencyMin, new Min());
            checkpointLatencySensor.add(checkpointLatencyAvg, new Avg());
        }
    }

    static LinkedHashMap<String, String> groupTags(String source, String target, String group, TopicPartition topicPartition) {
        LinkedHashMap<String, String> tags = new LinkedHashMap<>();
        tags.put("source", source);
        tags.put("target", target);
        tags.put("group", group);
        tags.put("topic", topicPartition.topic());
        tags.put("partition", Integer.toString(topicPartition.partition()));
        return tags;
    }
}
