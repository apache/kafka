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

package org.apache.kafka.controller.metrics;

import org.apache.kafka.common.utils.MockTime;

import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class QuorumControllerMetricsTest {
    @Test
    public void testMetricNames() {
        MetricsRegistry registry = new MetricsRegistry();
        MockTime time = new MockTime();
        try {
            try (QuorumControllerMetrics metrics = new QuorumControllerMetrics(
                    Optional.of(registry),
                    time)) {
                HashSet<String> expected = new HashSet<>(Arrays.asList(
                    "kafka.controller:type=ControllerEventManager,name=EventQueueProcessingTimeMs",
                    "kafka.controller:type=ControllerEventManager,name=EventQueueTimeMs",
                    "kafka.controller:type=KafkaController,name=ActiveControllerCount",
                    "kafka.controller:type=KafkaController,name=EventQueueOperationsStartedCount",
                    "kafka.controller:type=KafkaController,name=EventQueueOperationsTimedOutCount",
                    "kafka.controller:type=KafkaController,name=LastAppliedRecordLagMs",
                    "kafka.controller:type=KafkaController,name=LastAppliedRecordOffset",
                    "kafka.controller:type=KafkaController,name=LastAppliedRecordTimestamp",
                    "kafka.controller:type=KafkaController,name=LastCommittedRecordOffset",
                    "kafka.controller:type=KafkaController,name=NewActiveControllersCount",
                    "kafka.controller:type=KafkaController,name=TimedOutBrokerHeartbeatCount"
                ));
                ControllerMetricsTestUtils.assertMetricsForTypeEqual(registry, "kafka.controller", expected);
            }
            ControllerMetricsTestUtils.assertMetricsForTypeEqual(registry, "kafka.controller",
                    Collections.emptySet());
        } finally {
            registry.shutdown();
        }
    }

    @Test
    public void testUpdateEventQueueTime() {
        MetricsRegistry registry = new MetricsRegistry();
        MockTime time = new MockTime();
        try (QuorumControllerMetrics metrics = new QuorumControllerMetrics(Optional.of(registry), time)) {
            metrics.updateEventQueueTime(1000);
            assertMetricHistogram(registry, metricName("ControllerEventManager", "EventQueueTimeMs"), 1, 1000);
        } finally {
            registry.shutdown();
        }
    }

    @Test
    public void testUpdateEventQueueProcessingTime() {
        MetricsRegistry registry = new MetricsRegistry();
        MockTime time = new MockTime();
        try (QuorumControllerMetrics metrics = new QuorumControllerMetrics(Optional.of(registry), time)) {
            metrics.updateEventQueueProcessingTime(1000);
            assertMetricHistogram(registry, metricName("ControllerEventManager", "EventQueueProcessingTimeMs"), 1, 1000);
        } finally {
            registry.shutdown();
        }
    }

    @Test
    public void testLastAppliedRecordMetrics() {
        MetricsRegistry registry = new MetricsRegistry();
        MockTime time = new MockTime();
        time.sleep(1000);
        try (QuorumControllerMetrics metrics = new QuorumControllerMetrics(Optional.of(registry), time)) {
            metrics.setLastAppliedRecordOffset(100);
            metrics.setLastAppliedRecordTimestamp(500);
            metrics.setLastCommittedRecordOffset(50);
            metrics.setActive(true);
            for (int i = 0; i < 2; i++) {
                metrics.incrementTimedOutHeartbeats();
            }
            for (int i = 0; i < 3; i++) {
                metrics.incrementOperationsStarted();
            }
            for (int i = 0; i < 4; i++) {
                metrics.incrementOperationsTimedOut();
            }
            for (int i = 0; i < 5; i++) {
                metrics.incrementNewActiveControllers();
            }

            @SuppressWarnings("unchecked")
            Gauge<Long> lastAppliedRecordOffset = (Gauge<Long>) registry
                .allMetrics()
                .get(metricName("KafkaController", "LastAppliedRecordOffset"));
            assertEquals(100, lastAppliedRecordOffset.value());

            @SuppressWarnings("unchecked")
            Gauge<Long> lastAppliedRecordTimestamp = (Gauge<Long>) registry
                .allMetrics()
                .get(metricName("KafkaController", "LastAppliedRecordTimestamp"));
            assertEquals(500, lastAppliedRecordTimestamp.value());

            @SuppressWarnings("unchecked")
            Gauge<Long> lastAppliedRecordLagMs = (Gauge<Long>) registry
                .allMetrics()
                .get(metricName("KafkaController", "LastAppliedRecordLagMs"));
            assertEquals(time.milliseconds() - 500, lastAppliedRecordLagMs.value());

            @SuppressWarnings("unchecked")
            Gauge<Long> lastCommittedRecordOffset = (Gauge<Long>) registry
                .allMetrics()
                .get(metricName("KafkaController", "LastCommittedRecordOffset"));
            assertEquals(50, lastCommittedRecordOffset.value());

            @SuppressWarnings("unchecked")
            Gauge<Long> timedOutBrokerHeartbeats = (Gauge<Long>) registry
                    .allMetrics()
                    .get(metricName("KafkaController", "TimedOutBrokerHeartbeatCount"));
            assertEquals(2L, timedOutBrokerHeartbeats.value());

            @SuppressWarnings("unchecked")
            Gauge<Long> operationsStarted = (Gauge<Long>) registry
                    .allMetrics()
                    .get(metricName("KafkaController", "EventQueueOperationsStartedCount"));
            assertEquals(3L, operationsStarted.value());

            @SuppressWarnings("unchecked")
            Gauge<Long> operationsTimedOut = (Gauge<Long>) registry
                    .allMetrics()
                    .get(metricName("KafkaController", "EventQueueOperationsTimedOutCount"));
            assertEquals(4L, operationsTimedOut.value());

            @SuppressWarnings("unchecked")
            Gauge<Long> newActiveControllers = (Gauge<Long>) registry
                    .allMetrics()
                    .get(metricName("KafkaController", "NewActiveControllersCount"));
            assertEquals(5L, newActiveControllers.value());
        } finally {
            registry.shutdown();
        }
    }

    private static void assertMetricHistogram(MetricsRegistry registry, MetricName metricName, long count, double sum) {
        Histogram histogram = (Histogram) registry.allMetrics().get(metricName);

        assertEquals(count, histogram.count());
        assertEquals(sum, histogram.sum(), .1);
    }

    private static MetricName metricName(String type, String name) {
        String mBeanName = String.format("kafka.controller:type=%s,name=%s", type, name);
        return new MetricName("kafka.controller", type, name, null, mBeanName);
    }
}
