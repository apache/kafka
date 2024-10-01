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
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AdminFetchMetricsManagerTest {
    private static final double EPSILON = 0.0001;

    private final Time time = new MockTime(1, 0, 0);
    private Metrics metrics;
    private AdminFetchMetricsManager adminFetchMetricsManager;
    private final String group = "group";


    @BeforeEach
    public void setup() {
        metrics = new Metrics(time);
        adminFetchMetricsManager = new AdminFetchMetricsManager(metrics);
    }

    @AfterEach
    public void tearDown() {
        Utils.closeQuietly(metrics, "metrics");
        metrics = null;
        adminFetchMetricsManager = null;
    }

    @Test
    public void testSingleNodeLatency() {
        String connectionId = "0";
        MetricName nodeLatencyAvg = metrics.metricName("request-latency-avg", group);
        MetricName nodeLatencyMax = metrics.metricName("request-latency-max", group);
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
        String connectionId0 = "0";
        MetricName nodeLatencyAvg0 = metrics.metricName("request-latency-avg", group, genericTag(connectionId0));
        MetricName nodeLatencyMax0 = metrics.metricName("request-latency-max", group, genericTag(connectionId0));
        registerNodeLatencyMetric(connectionId0, nodeLatencyAvg0, nodeLatencyMax0);
        adminFetchMetricsManager.recordLatency(connectionId0, 5);
        adminFetchMetricsManager.recordLatency(connectionId0, 8);

        // Record metric against another node.
        String connectionId1 = "1";
        MetricName nodeLatencyAvg1 = metrics.metricName("request-latency-avg", group, genericTag(connectionId1));
        MetricName nodeLatencyMax1 = metrics.metricName("request-latency-max", group, genericTag(connectionId1));
        registerNodeLatencyMetric(connectionId1, nodeLatencyAvg1, nodeLatencyMax1);
        adminFetchMetricsManager.recordLatency(connectionId1, 105);
        adminFetchMetricsManager.recordLatency(connectionId1, 108);

        assertEquals(6.5, metricValue(nodeLatencyAvg0), EPSILON);
        assertEquals(8, metricValue(nodeLatencyMax0), EPSILON);
        assertEquals(106.5, metricValue(nodeLatencyAvg1), EPSILON);
        assertEquals(108, metricValue(nodeLatencyMax1), EPSILON);

        mockSleepTimeWindow();
        adminFetchMetricsManager.recordLatency(connectionId0, 11);
        adminFetchMetricsManager.recordLatency(connectionId1, 111);
        assertEquals(8, metricValue(nodeLatencyAvg0), EPSILON);
        assertEquals(11, metricValue(nodeLatencyMax0), EPSILON);
        assertEquals(108, metricValue(nodeLatencyAvg1), EPSILON);
        assertEquals(111, metricValue(nodeLatencyMax1), EPSILON);

        mockSleepTimeWindow();
        assertEquals(11, metricValue(nodeLatencyAvg0), EPSILON);
        assertEquals(11, metricValue(nodeLatencyMax0), EPSILON);
        assertEquals(111, metricValue(nodeLatencyAvg1), EPSILON);
        assertEquals(111, metricValue(nodeLatencyMax1), EPSILON);

        mockSleepTimeWindow();
        assertTrue(Double.isNaN(metricValue(nodeLatencyAvg0)));
        assertTrue(Double.isNaN(metricValue(nodeLatencyMax0)));
        assertTrue(Double.isNaN(metricValue(nodeLatencyAvg1)));
        assertTrue(Double.isNaN(metricValue(nodeLatencyMax1)));

        adminFetchMetricsManager.recordLatency(connectionId0, 500);
        adminFetchMetricsManager.recordLatency(connectionId0, 600);
        mockSleepTimeWindow();
        adminFetchMetricsManager.recordLatency(connectionId1, 800);
        adminFetchMetricsManager.recordLatency(connectionId1, 900);
        assertEquals(550, metricValue(nodeLatencyAvg0), EPSILON);
        assertEquals(600, metricValue(nodeLatencyMax0), EPSILON);
        assertEquals(850, metricValue(nodeLatencyAvg1), EPSILON);
        assertEquals(900, metricValue(nodeLatencyMax1), EPSILON);

        mockSleepTimeWindow();
        assertTrue(Double.isNaN(metricValue(nodeLatencyAvg0)));
        assertTrue(Double.isNaN(metricValue(nodeLatencyMax0)));
        assertEquals(850, metricValue(nodeLatencyAvg1), EPSILON);
        assertEquals(900, metricValue(nodeLatencyMax1), EPSILON);

        mockSleepTimeWindow();
        assertTrue(Double.isNaN(metricValue(nodeLatencyAvg1)));
        assertTrue(Double.isNaN(metricValue(nodeLatencyMax1)));
    }

    private Map<String, String> genericTag(String connectionId) {
        return Collections.singletonMap("node-id", "node-" + connectionId);
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

    private double metricValue(MetricName metricName) {
        KafkaMetric metric = metrics.metric(metricName);
        return (double) metric.metricValue();
    }
}
