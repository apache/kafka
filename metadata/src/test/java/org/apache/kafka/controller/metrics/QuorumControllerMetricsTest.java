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

import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;

import org.apache.kafka.common.utils.MockTime;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class QuorumControllerMetricsTest {
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testMetricNames(boolean inMigration) {
        MetricsRegistry registry = new MetricsRegistry();
        MockTime time = new MockTime();
        try {
            try (QuorumControllerMetrics metrics = new QuorumControllerMetrics(
                    Optional.of(registry),
                    time,
                    inMigration)) {
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
                if (inMigration) {
                    expected.add("kafka.controller:type=KafkaController,name=ZkWriteBehindLag");
                    expected.add("kafka.controller:type=KafkaController,name=ZkWriteSnapshotTimeMs");
                    expected.add("kafka.controller:type=KafkaController,name=ZkWriteDeltaTimeMs");
                }
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
        try (QuorumControllerMetrics metrics = new QuorumControllerMetrics(Optional.of(registry), time, false)) {
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
        try (QuorumControllerMetrics metrics = new QuorumControllerMetrics(Optional.of(registry), time, false)) {
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
        try (QuorumControllerMetrics metrics = new QuorumControllerMetrics(Optional.of(registry), time, true)) {
            metrics.setLastAppliedRecordOffset(100);
            metrics.setLastAppliedRecordTimestamp(500);
            metrics.setLastCommittedRecordOffset(50);
            metrics.updateDualWriteOffset(40L);
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
            Gauge<Long> zkWriteBehindLag = (Gauge<Long>) registry
                    .allMetrics()
                    .get(metricName("KafkaController", "ZkWriteBehindLag"));
            assertEquals(10L, zkWriteBehindLag.value());

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

    @Test
    public void testUpdateZKWriteBehindLag() {
        MetricsRegistry registry = new MetricsRegistry();
        MockTime time = new MockTime();
        // test zkWriteBehindLag metric when NOT in dual-write mode
        try (QuorumControllerMetrics metrics = new QuorumControllerMetrics(Optional.of(registry), time, true)) {
            metrics.updateDualWriteOffset(0);
            @SuppressWarnings("unchecked")
            Gauge<Long> zkWriteBehindLag = (Gauge<Long>) registry
                .allMetrics()
                .get(metricName("KafkaController", "ZkWriteBehindLag"));
            assertEquals(0, zkWriteBehindLag.value());
        } finally {
            registry.shutdown();
        }

        // test zkWriteBehindLag metric when in dual-write mode
        try (QuorumControllerMetrics metrics = new QuorumControllerMetrics(Optional.of(registry), time, true)) {
            metrics.updateDualWriteOffset(90);
            metrics.setLastCommittedRecordOffset(100);
            metrics.setActive(true);
            @SuppressWarnings("unchecked")
            Gauge<Long> zkWriteBehindLag = (Gauge<Long>) registry
                .allMetrics()
                .get(metricName("KafkaController", "ZkWriteBehindLag"));
            assertEquals(10, zkWriteBehindLag.value());
        } finally {
            registry.shutdown();
        }

        // test zkWriteBehindLag metric when in dual-write mode and not active
        try (QuorumControllerMetrics metrics = new QuorumControllerMetrics(Optional.of(registry), time, true)) {
            metrics.updateDualWriteOffset(90);
            metrics.setLastCommittedRecordOffset(100);
            metrics.setActive(false);
            @SuppressWarnings("unchecked")
            Gauge<Long> zkWriteBehindLag = (Gauge<Long>) registry
                    .allMetrics()
                    .get(metricName("KafkaController", "ZkWriteBehindLag"));
            assertEquals(0, zkWriteBehindLag.value());
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
