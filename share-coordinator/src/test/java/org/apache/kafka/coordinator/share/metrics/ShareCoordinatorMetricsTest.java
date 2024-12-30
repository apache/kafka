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
package org.apache.kafka.coordinator.share.metrics;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.timeline.SnapshotRegistry;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;

import static org.apache.kafka.coordinator.share.metrics.ShareCoordinatorMetrics.SHARE_COORDINATOR_WRITE_LATENCY_SENSOR_NAME;
import static org.apache.kafka.coordinator.share.metrics.ShareCoordinatorMetrics.SHARE_COORDINATOR_WRITE_SENSOR_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ShareCoordinatorMetricsTest {

    @Test
    public void testMetricNames() {
        Metrics metrics = new Metrics();

        HashSet<MetricName> expectedMetrics = new HashSet<>(Arrays.asList(
            metrics.metricName("write-rate", ShareCoordinatorMetrics.METRICS_GROUP),
            metrics.metricName("write-total", ShareCoordinatorMetrics.METRICS_GROUP),
            metrics.metricName("write-latency-avg", ShareCoordinatorMetrics.METRICS_GROUP),
            metrics.metricName("write-latency-max", ShareCoordinatorMetrics.METRICS_GROUP)
        ));

        ShareCoordinatorMetrics ignored = new ShareCoordinatorMetrics(metrics);
        for (MetricName metricName : expectedMetrics) {
            assertTrue(metrics.metrics().containsKey(metricName));
        }
    }

    @Test
    public void testGlobalSensors() {
        MockTime time = new MockTime();
        Metrics metrics = new Metrics(time);
        ShareCoordinatorMetrics coordinatorMetrics = new ShareCoordinatorMetrics(metrics);
        ShareCoordinatorMetricsShard shard = coordinatorMetrics.newMetricsShard(
            new SnapshotRegistry(new LogContext()), new TopicPartition("__share_group_state", 0)
        );

        shard.record(SHARE_COORDINATOR_WRITE_SENSOR_NAME);
        assertMetricValue(metrics, metrics.metricName("write-rate", ShareCoordinatorMetrics.METRICS_GROUP), 1.0 / 30);  //sampled stats
        assertMetricValue(metrics, metrics.metricName("write-total", ShareCoordinatorMetrics.METRICS_GROUP), 1.0);

        shard.record(SHARE_COORDINATOR_WRITE_LATENCY_SENSOR_NAME, 20);
        shard.record(SHARE_COORDINATOR_WRITE_LATENCY_SENSOR_NAME, 30);
        assertMetricValue(metrics, metrics.metricName("write-latency-avg", ShareCoordinatorMetrics.METRICS_GROUP), 50.0 / 2);
        assertMetricValue(metrics, metrics.metricName("write-latency-max", ShareCoordinatorMetrics.METRICS_GROUP), 30.0);
    }

    private void assertMetricValue(Metrics metrics, MetricName metricName, double val) {
        assertEquals(val, metrics.metric(metricName).metricValue());
    }
}
