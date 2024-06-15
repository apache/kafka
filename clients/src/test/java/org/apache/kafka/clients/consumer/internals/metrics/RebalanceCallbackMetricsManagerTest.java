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
package org.apache.kafka.clients.consumer.internals.metrics;

import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class RebalanceCallbackMetricsManagerTest {
    private final Time time = new MockTime();
    private final Metrics metrics = new Metrics(time);

    @Test
    public void testRebalanceCallbackMetrics() {
        RebalanceCallbackMetricsManager metricsManager = new RebalanceCallbackMetricsManager(metrics);
        assertNotNull(metrics.metric(metricsManager.partitionRevokeLatencyAvg));
        assertNotNull(metrics.metric(metricsManager.partitionRevokeLatencyMax));
        assertNotNull(metrics.metric(metricsManager.partitionAssignLatencyAvg));
        assertNotNull(metrics.metric(metricsManager.partitionAssignLatencyMax));
        assertNotNull(metrics.metric(metricsManager.partitionLostLatencyAvg));
        assertNotNull(metrics.metric(metricsManager.partitionLostLatencyMax));

        metricsManager.recordPartitionsAssignedLatency(100);
        metricsManager.recordPartitionsRevokedLatency(101);
        metricsManager.recordPartitionsLostLatency(102);

        assertEquals(101d, metrics.metric(metricsManager.partitionRevokeLatencyAvg).metricValue());
        assertEquals(101d, metrics.metric(metricsManager.partitionRevokeLatencyMax).metricValue());
        assertEquals(100d, metrics.metric(metricsManager.partitionAssignLatencyAvg).metricValue());
        assertEquals(100d, metrics.metric(metricsManager.partitionAssignLatencyMax).metricValue());
        assertEquals(102d, metrics.metric(metricsManager.partitionLostLatencyAvg).metricValue());
        assertEquals(102d, metrics.metric(metricsManager.partitionLostLatencyMax).metricValue());
    }
}
