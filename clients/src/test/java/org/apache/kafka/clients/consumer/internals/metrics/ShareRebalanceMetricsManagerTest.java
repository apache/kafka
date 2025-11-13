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


class ShareRebalanceMetricsManagerTest {

    private final Time time = new MockTime();
    private final Metrics metrics = new Metrics(time);

    @Test
    public void testRebalanceMetrics() {
        ShareRebalanceMetricsManager shareRebalanceMetricsManager = new ShareRebalanceMetricsManager(metrics);

        assertNotNull(metrics.metric(shareRebalanceMetricsManager.rebalanceTotal));
        assertNotNull(metrics.metric(shareRebalanceMetricsManager.rebalanceRatePerHour));

        shareRebalanceMetricsManager.recordRebalanceStarted(10);
        shareRebalanceMetricsManager.recordRebalanceEnded(25);
        shareRebalanceMetricsManager.recordRebalanceStarted(30);
        shareRebalanceMetricsManager.recordRebalanceEnded(40);
        shareRebalanceMetricsManager.recordRebalanceStarted(50);
        shareRebalanceMetricsManager.recordRebalanceEnded(100);

        assertEquals(3.0d, metrics.metric(shareRebalanceMetricsManager.rebalanceTotal).metricValue());
        assertEquals(3.d, metrics.metric(shareRebalanceMetricsManager.rebalanceRatePerHour).metricValue());
    }
}
