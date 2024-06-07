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

import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class HeartbeatMetricsManagerTest {
    private final Time time = new MockTime();
    private final Metrics metrics = new Metrics(time);

    @Test
    public void testHeartbeatMetrics() {
        // Assuming 'metrics' is an instance of your Metrics class
        HeartbeatMetricsManager heartbeatMetricsManager = new HeartbeatMetricsManager(metrics);

        // Assert the existence of metrics
        assertNotNull(metrics.metric(heartbeatMetricsManager.heartbeatResponseTimeMax));
        assertNotNull(metrics.metric(heartbeatMetricsManager.heartbeatRate));
        assertNotNull(metrics.metric(heartbeatMetricsManager.heartbeatTotal));

        // Record heartbeat sent time and request latencies
        long currentTimeMs = time.milliseconds();
        heartbeatMetricsManager.recordHeartbeatSentMs(currentTimeMs);
        heartbeatMetricsManager.recordRequestLatency(100);
        heartbeatMetricsManager.recordRequestLatency(103);
        heartbeatMetricsManager.recordRequestLatency(102);

        // Assert recorded metrics values
        assertEquals(103d, metrics.metric(heartbeatMetricsManager.heartbeatResponseTimeMax).metricValue());
        assertEquals(0.1d, (double) metrics.metric(heartbeatMetricsManager.heartbeatRate).metricValue(), 0.01d);
        assertEquals(3d, metrics.metric(heartbeatMetricsManager.heartbeatTotal).metricValue());

        // Randomly sleep 1-10 seconds
        Random rand = new Random();
        int randomSleepS = rand.nextInt(10) + 1;
        time.sleep(TimeUnit.SECONDS.toMillis(randomSleepS));
        assertEquals((double) randomSleepS, metrics.metric(heartbeatMetricsManager.lastHeartbeatSecondsAgo).metricValue());
    }
}
