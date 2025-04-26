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
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class MirrorCheckpointMetricsTest {
    private static final String SOURCE = "source";
    private static final String TARGET = "target";
    private static final String GROUP = "group";
    private static final TopicPartition TP = new TopicPartition("topic", 0);
    private static final TopicPartition SOURCE_TP = new TopicPartition(SOURCE + "." + TP.topic(), TP.partition());
    private static final String CONNECTOR_NAME = "name";
    private static final int TASK_INDEX = 0;
    private final Map<String, String> configs = new HashMap<>();
    private TestReporter reporter;

    @BeforeEach
    public void setUp() {
        configs.put(ConnectorConfig.NAME_CONFIG, CONNECTOR_NAME);
        configs.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, MirrorSourceConnector.class.getName());
        configs.put(MirrorConnectorConfig.SOURCE_CLUSTER_ALIAS, SOURCE);
        configs.put(MirrorConnectorConfig.TARGET_CLUSTER_ALIAS, TARGET);
        configs.put(MirrorConnectorConfig.TASK_INDEX, String.valueOf(TASK_INDEX));
        configs.put(MirrorSourceTaskConfig.TASK_TOPIC_PARTITIONS, TP.toString());
        reporter = new TestReporter();
    }

    @Test
    public void testTags() {
        MirrorCheckpointTaskConfig taskConfig = new MirrorCheckpointTaskConfig(configs);
        MirrorCheckpointMetrics metrics = new MirrorCheckpointMetrics(taskConfig);
        metrics.addReporter(reporter);

        metrics.checkpointLatency(SOURCE_TP, GROUP, 0);

        assertEquals(5, reporter.metrics.size());
        MetricName metric = reporter.metrics.get(0).metricName();
        if (Objects.equals(metric.group(), "kafka-metrics-count")) {
            metric = reporter.metrics.get(1).metricName();
        }
        Map<String, String> tags = metric.tags();
        assertEquals(SOURCE, tags.get("source"));
        assertEquals(TARGET, tags.get("target"));
        assertEquals(GROUP, tags.get("group"));
        assertEquals(SOURCE_TP.topic(), tags.get("topic"));
        assertEquals(String.valueOf(SOURCE_TP.partition()), tags.get("partition"));
        assertEquals(CONNECTOR_NAME, tags.get("connector"));
        assertEquals(String.valueOf(TASK_INDEX), tags.get("task"));
    }

    @Test
    public void testTagsForKafkaMetricsCount() {
        MirrorCheckpointTaskConfig taskConfig = new MirrorCheckpointTaskConfig(configs);
        MirrorCheckpointMetrics metrics = new MirrorCheckpointMetrics(taskConfig);
        metrics.addReporter(reporter);

        MetricName targetMetric = null;
        for (KafkaMetric m : reporter.metrics) {
            MetricName metricName = m.metricName();
            if (metricName.group().equals("kafka-metrics-count")) {
                targetMetric = metricName;
                break;
            }
        }
        assertNotNull(targetMetric);
        Map<String, String> tags = targetMetric.tags();
        assertEquals(CONNECTOR_NAME, tags.get("connector"));
        assertEquals(String.valueOf(TASK_INDEX), tags.get("task"));
    }

    static class TestReporter implements MetricsReporter {

        List<KafkaMetric> metrics = new ArrayList<>();

        @Override
        public void init(List<KafkaMetric> metrics) {
            for (KafkaMetric metric : metrics) {
                metricChange(metric);
            }
        }

        @Override
        public void metricChange(KafkaMetric metric) {
            metrics.add(metric);
        }

        @Override
        public void metricRemoval(KafkaMetric metric) {}

        @Override
        public void close() {}

        @Override
        public void configure(Map<String, ?> configs) {}
    }
}
