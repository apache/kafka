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

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class QuorumControllerMetricsTest {
    @Test
    public void testMetricNames() {
        MetricsRegistry registry = new MetricsRegistry();
        MockTime time = new MockTime();
        try {
            try (QuorumControllerMetrics metrics = new QuorumControllerMetrics(
                    Optional.of(registry),
                    time,
                    9000)) {
                metrics.addTimeSinceLastHeartbeatMetric(1);
                metrics.updateBrokerOutOfSyncCounts(Map.of(2, 5));
                Set<String> expected = Set.of(
                    "kafka.controller:type=ControllerEventManager,name=EventQueueProcessingTimeMs",
                    "kafka.controller:type=ControllerEventManager,name=EventQueueTimeMs",
                    "kafka.controller:type=ControllerEventManager,name=AvgIdleRatio",
                    "kafka.controller:type=KafkaController,name=ActiveControllerCount",
                    "kafka.controller:type=KafkaController,name=EventQueueOperationsStartedCount",
                    "kafka.controller:type=KafkaController,name=EventQueueOperationsTimedOutCount",
                    "kafka.controller:type=KafkaController,name=LastAppliedRecordLagMs",
                    "kafka.controller:type=KafkaController,name=LastAppliedRecordOffset",
                    "kafka.controller:type=KafkaController,name=LastAppliedRecordTimestamp",
                    "kafka.controller:type=KafkaController,name=LastCommittedRecordOffset",
                    "kafka.controller:type=KafkaController,name=NewActiveControllersCount",
                    "kafka.controller:type=KafkaController,name=TimedOutBrokerHeartbeatCount",
                    "kafka.controller:type=KafkaController,name=TimeSinceLastHeartbeatReceivedMs,broker=1",
                    "kafka.controller:type=KafkaController,name=OutOfSyncPreferredPartitionCount,broker=2",
                    "kafka.controller:type=KafkaController,name=PreferredLeaderElectionsPerRun",
                    "kafka.controller:type=KafkaController,name=GatedPreferredLeaderBrokerCount",
                    "kafka.controller:type=KafkaController,name=PreferredLeaderElectionThrottledRunCount",
                    "kafka.controller:type=KafkaController,name=PreferredLeaderElectionEscapeHatchCount"
                );
                ControllerMetricsTestUtils.assertMetricsForTypeEqual(registry, "kafka.controller", expected);
            }
            ControllerMetricsTestUtils.assertMetricsForTypeEqual(registry, "kafka.controller",
                    Set.of());
        } finally {
            registry.shutdown();
        }
    }

    @Test
    public void testUpdateEventQueueTime() {
        MetricsRegistry registry = new MetricsRegistry();
        MockTime time = new MockTime();
        try (QuorumControllerMetrics metrics = new QuorumControllerMetrics(Optional.of(registry), time, 9000)) {
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
        try (QuorumControllerMetrics metrics = new QuorumControllerMetrics(Optional.of(registry), time, 9000)) {
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
        try (QuorumControllerMetrics metrics = new QuorumControllerMetrics(Optional.of(registry), time, 9000)) {
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

    @SuppressWarnings("unchecked")
    @Test
    public void testTimeSinceLastHeartbeatReceivedMs() {
        MetricsRegistry registry = new MetricsRegistry();
        MockTime time = new MockTime();
        int brokerId = 1;
        int sessionTimeoutMs = 9000;
        try (QuorumControllerMetrics metrics = new QuorumControllerMetrics(Optional.of(registry), time, sessionTimeoutMs)) {
            metrics.addTimeSinceLastHeartbeatMetric(1);
            int numMetrics = registry.allMetrics().size();
            Gauge<Integer> timeSinceLastHeartbeatReceivedMs = (Gauge<Integer>) registry.allMetrics().get(metricName("KafkaController", "TimeSinceLastHeartbeatReceivedMs", "broker=1"));
            metrics.updateBrokerContactTime(brokerId);
            time.sleep(1000);
            assertEquals(1000, timeSinceLastHeartbeatReceivedMs.value());
            metrics.updateBrokerContactTime(brokerId);
            assertEquals(0, timeSinceLastHeartbeatReceivedMs.value());
            time.sleep(100000);
            assertEquals(sessionTimeoutMs, timeSinceLastHeartbeatReceivedMs.value());
            metrics.removeTimeSinceLastHeartbeatMetrics();
            assertEquals(numMetrics - 1, registry.allMetrics().size());
        } finally {
            registry.shutdown();
        }
    }

    @SuppressWarnings("unchecked") // do not warn about Gauge typecast.
    @Test
    public void testAvgIdleRatio() {
        final double delta = 0.001;
        MetricsRegistry registry = new MetricsRegistry();
        MockTime time = new MockTime();
        try (QuorumControllerMetrics metrics = new QuorumControllerMetrics(Optional.of(registry), time, 9000)) {
            Gauge<Double> avgIdleRatio = (Gauge<Double>) registry.allMetrics().get(metricName("ControllerEventManager", "AvgIdleRatio"));

            // No idle time recorded yet; returns default ratio of 1.0
            assertEquals(1.0, avgIdleRatio.value(), delta);

            // First recording is dropped to establish the interval start time
            // This is because TimeRatio needs an initial timestamp to measure intervals from
            metrics.updateIdleTime(10, time.milliseconds());
            time.sleep(40);
            metrics.updateIdleTime(20, time.milliseconds());
            // avgIdleRatio = (20ms idle) / (40ms interval) = 0.5
            assertEquals(0.5, avgIdleRatio.value(), delta);

            time.sleep(20);
            metrics.updateIdleTime(1, time.milliseconds());
            // avgIdleRatio = (1ms idle) / (20ms interval) = 0.05
            assertEquals(0.05, avgIdleRatio.value(), delta);

        } finally {
            registry.shutdown();
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testPreferredLeaderElectionMetrics() {
        MetricsRegistry registry = new MetricsRegistry();
        MockTime time = new MockTime();
        try (QuorumControllerMetrics metrics = new QuorumControllerMetrics(Optional.of(registry), time, 9000)) {
            Gauge<Long> electionsPerRun = (Gauge<Long>) registry.allMetrics()
                .get(metricName("KafkaController", "PreferredLeaderElectionsPerRun"));
            Gauge<Long> gatedBrokerCount = (Gauge<Long>) registry.allMetrics()
                .get(metricName("KafkaController", "GatedPreferredLeaderBrokerCount"));
            Gauge<Long> throttledRunCount = (Gauge<Long>) registry.allMetrics()
                .get(metricName("KafkaController", "PreferredLeaderElectionThrottledRunCount"));
            Gauge<Long> escapeHatchCount = (Gauge<Long>) registry.allMetrics()
                .get(metricName("KafkaController", "PreferredLeaderElectionEscapeHatchCount"));

            assertEquals(0L, electionsPerRun.value());
            assertEquals(0L, gatedBrokerCount.value());
            assertEquals(0L, throttledRunCount.value());
            assertEquals(0L, escapeHatchCount.value());

            metrics.setPreferredLeaderElectionsPerRun(7);
            assertEquals(7L, electionsPerRun.value());

            metrics.setGatedPreferredLeaderBrokerCount(3);
            assertEquals(3L, gatedBrokerCount.value());

            metrics.incrementPreferredLeaderElectionThrottledRunCount();
            metrics.incrementPreferredLeaderElectionThrottledRunCount();
            assertEquals(2L, throttledRunCount.value());

            metrics.setPreferredLeaderElectionEscapeHatchCount(1);
            assertEquals(1L, escapeHatchCount.value());
        } finally {
            registry.shutdown();
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testOutOfSyncPreferredPartitionCountMetric() {
        MetricsRegistry registry = new MetricsRegistry();
        MockTime time = new MockTime();
        try (QuorumControllerMetrics metrics = new QuorumControllerMetrics(Optional.of(registry), time, 9000)) {
            // Initially no per-broker metrics registered.
            int baseCount = registry.allMetrics().size();

            // Register broker 1 with 3 out-of-sync partitions, broker 2 with 1.
            metrics.updateBrokerOutOfSyncCounts(Map.of(1, 3, 2, 1));
            assertEquals(baseCount + 2, registry.allMetrics().size());

            Gauge<Integer> broker1 = (Gauge<Integer>) registry.allMetrics()
                .get(metricName("KafkaController", "OutOfSyncPreferredPartitionCount", "broker=1"));
            Gauge<Integer> broker2 = (Gauge<Integer>) registry.allMetrics()
                .get(metricName("KafkaController", "OutOfSyncPreferredPartitionCount", "broker=2"));

            assertEquals(3, broker1.value());
            assertEquals(1, broker2.value());

            // Update broker 1's count; broker 2 disappears (now fully in sync).
            metrics.updateBrokerOutOfSyncCounts(Map.of(1, 7));
            assertEquals(baseCount + 1, registry.allMetrics().size());
            assertEquals(7, broker1.value());

            // All brokers in sync — no per-broker metrics remain.
            metrics.updateBrokerOutOfSyncCounts(Map.of());
            assertEquals(baseCount, registry.allMetrics().size());
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

    private static MetricName metricName(String type, String name, String scope) {
        String mBeanName = String.format("kafka.controller:type=%s,name=%s,%s", type, name, scope);
        return new MetricName("kafka.controller", type, name, scope, mBeanName);
    }
}
