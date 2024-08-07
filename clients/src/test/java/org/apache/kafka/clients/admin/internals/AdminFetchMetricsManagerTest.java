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
package org.apache.kafka.clients.admin.internals;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AdminFetchMetricsManagerTest {
    private static final double EPSILON = 0.0001;

    private final Time time = new MockTime(1, 0, 0);
    private Metrics metrics;
    private AdminFetchMetricsManager adminFetchMetricsManager;


    @BeforeEach
    public void setup() {
        metrics = new Metrics(time);
        adminFetchMetricsManager = new AdminFetchMetricsManager(metrics);
    }

    @AfterEach
    public void tearDown() {
        if (metrics != null) {
            metrics.close();
            metrics = null;
        }

        adminFetchMetricsManager = null;
    }

    @Test
    public void testSingleNodeLatency() {
        String connectionId = "0";
        MetricName nodeLatencyAvg = metrics.metricName("request-latency-avg", "group");
        MetricName nodeLatencyMax = metrics.metricName("request-latency-max", "group");
        registerNodeLatencyMetric(connectionId, nodeLatencyAvg, nodeLatencyMax);

        adminFetchMetricsManager.recordLatency(connectionId, 333);
        mockSleepTimeWindow();
        adminFetchMetricsManager.recordLatency(connectionId, 444);
        assertEquals(388.5, metricValue(nodeLatencyAvg), EPSILON);
        assertEquals(444, metricValue(nodeLatencyMax), EPSILON);
        adminFetchMetricsManager.recordLatency(connectionId, 666);
        assertEquals(481, metricValue(nodeLatencyAvg), EPSILON);
        assertEquals(666, metricValue(nodeLatencyMax), EPSILON);

        // first record(333) expired
        mockSleepTimeWindow();
        assertEquals(555, metricValue(nodeLatencyAvg), EPSILON);
        assertEquals(666, metricValue(nodeLatencyMax), EPSILON);

        // all records expired
        mockSleepTimeWindow();
        assertTrue(Double.isNaN(metricValue(nodeLatencyAvg)));
        assertTrue(Double.isNaN(metricValue(nodeLatencyMax)));
    }


    @Test
    public void testMultiNodeLatency() {
        String connectionId = "0";
        MetricName nodeLatencyAvg0 = metrics.metricName("request-latency-avg", "group", genericTag(connectionId));
        MetricName nodeLatencyMax0 = metrics.metricName("request-latency-max", "group", genericTag(connectionId));
        registerNodeLatencyMetric(connectionId, nodeLatencyAvg0, nodeLatencyMax0);
        adminFetchMetricsManager.recordLatency(connectionId, 1);
        adminFetchMetricsManager.recordLatency(connectionId, 1);


        // Record metric against another node.
        connectionId = "1";
        MetricName nodeLatencyAvg1 = metrics.metricName("request-latency-avg", "group", genericTag(connectionId));
        MetricName nodeLatencyMax1 = metrics.metricName("request-latency-max", "group", genericTag(connectionId));
        registerNodeLatencyMetric(connectionId, nodeLatencyAvg1, nodeLatencyMax1);
        adminFetchMetricsManager.recordLatency(connectionId, 10);
        adminFetchMetricsManager.recordLatency(connectionId, 10);


        // Node specific metric should not be affected.
//        assertEquals(289.5, metricValue(nodeLatencyAvg), EPSILON);
//        assertEquals(456, metricValue(nodeLatencyMax), EPSILON);
    }

    private Map<String, String> genericTag(String connectionId) {
        Map<String, String> tags = new LinkedHashMap<>();
        tags.put("node-id", "node-" + connectionId);
        return tags;
    }


    private void mockSleepTimeWindow() {
        time.sleep(metrics.config().timeWindowMs() + 1);
    }

    private void registerNodeLatencyMetric(String connectionId, MetricName nodeLatencyAvg, MetricName nodeLatencyMax) {
        String nodeTimeName = "node-" + connectionId + ".latency";
        Sensor nodeRequestTime = metrics.sensor(nodeTimeName);
        nodeRequestTime.add(nodeLatencyAvg, new Avg());
        nodeRequestTime.add(nodeLatencyMax, new Max());
    }

    private double metricValue(MetricNameTemplate name) {
        MetricName metricName = metrics.metricInstance(name);
        return metricValue(metricName);
    }

    private double metricValue(MetricNameTemplate name, Map<String, String> tags) {
        MetricName metricName = metrics.metricInstance(name, tags);
        return metricValue(metricName);
    }

    private double metricValue(MetricName metricName) {
        KafkaMetric metric = metrics.metric(metricName);
        return (Double) metric.metricValue();
    }
}
