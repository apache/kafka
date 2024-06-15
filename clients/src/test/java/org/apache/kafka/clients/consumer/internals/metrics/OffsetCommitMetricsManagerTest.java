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

public class OffsetCommitMetricsManagerTest {
    private final Time time = new MockTime();
    private final Metrics metrics = new Metrics(time);

    @Test
    public void testOffsetCommitMetrics() {
        // Assuming 'metrics' is an instance of your Metrics class

        // Create an instance of OffsetCommitMetricsManager
        OffsetCommitMetricsManager metricsManager = new OffsetCommitMetricsManager(metrics);

        // Assert the existence of metrics
        assertNotNull(metrics.metric(metricsManager.commitLatencyAvg));
        assertNotNull(metrics.metric(metricsManager.commitLatencyMax));
        assertNotNull(metrics.metric(metricsManager.commitRate));
        assertNotNull(metrics.metric(metricsManager.commitTotal));

        // Record request latency
        metricsManager.recordRequestLatency(100);
        metricsManager.recordRequestLatency(102);
        metricsManager.recordRequestLatency(98);

        // Assert the recorded latency
        assertEquals(100d, metrics.metric(metricsManager.commitLatencyAvg).metricValue());
        assertEquals(102d, metrics.metric(metricsManager.commitLatencyMax).metricValue());
        assertEquals(0.1d, metrics.metric(metricsManager.commitRate).metricValue());
        assertEquals(3d, metrics.metric(metricsManager.commitTotal).metricValue());
    }
}
