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

package org.apache.kafka.image.publisher.metrics;

import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;

import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.controller.metrics.ControllerMetricsTestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;


@Timeout(value = 40)
public class SnapshotEmitterMetricsTest {
    private static final Logger log = LoggerFactory.getLogger(SnapshotEmitterMetricsTest.class);

    static class SnapshotEmitterMetricsTestContext implements AutoCloseable {
        final MetricsRegistry registry;
        final MockTime time;
        final SnapshotEmitterMetrics metrics;

        SnapshotEmitterMetricsTestContext() {
            this.registry = new MetricsRegistry();
            this.time = new MockTime(0, 10000L, 0L);
            this.metrics = new SnapshotEmitterMetrics(Optional.of(registry), time);
        }

        @SuppressWarnings("unchecked") // suppress warning about Gauge typecast
        long readLongGauge(String name) {
            MetricName metricName = new MetricName(
                "kafka.server",
                "SnapshotEmitter",
                name,
                null,
                "kafka.server:type=SnapshotEmitter,name=" + name
            );
            return ((Gauge<Long>) registry.allMetrics().get(metricName)).value();
        }

        @Override
        public void close() {
            try {
                registry.shutdown();
            } catch (Exception e) {
                log.error("Error closing registry", e);
            }
        }
    }

    @Test
    public void testMetricNames() {
        try (SnapshotEmitterMetricsTestContext ctx = new SnapshotEmitterMetricsTestContext()) {
            ControllerMetricsTestUtils.assertMetricsForTypeEqual(ctx.registry, "kafka.server:",
                new HashSet<>(Arrays.asList(
                    "kafka.server:type=SnapshotEmitter,name=LatestSnapshotGeneratedBytes",
                    "kafka.server:type=SnapshotEmitter,name=LatestSnapshotGeneratedAgeMs"
                )));
            ctx.metrics.close();
            ControllerMetricsTestUtils.assertMetricsForTypeEqual(ctx.registry, "KafkaController",
                    Collections.emptySet());
        }
    }

    @Test
    public void testLatestSnapshotGeneratedBytesMetric() {
        try (SnapshotEmitterMetricsTestContext ctx = new SnapshotEmitterMetricsTestContext()) {
            assertEquals(0L, ctx.metrics.latestSnapshotGeneratedBytes());
            ctx.metrics.setLatestSnapshotGeneratedBytes(12345L);
            assertEquals(12345L, ctx.metrics.latestSnapshotGeneratedBytes());
            assertEquals(12345L, ctx.readLongGauge("LatestSnapshotGeneratedBytes"));
        }
    }

    @Test
    public void testLatestSnapshotGeneratedAgeMsMetric() {
        try (SnapshotEmitterMetricsTestContext ctx = new SnapshotEmitterMetricsTestContext()) {
            assertEquals(10000L, ctx.metrics.latestSnapshotGeneratedTimeMs());
            assertEquals(0L, ctx.metrics.latestSnapshotGeneratedAgeMs());
            ctx.time.sleep(20000L);
            assertEquals(10000L, ctx.metrics.latestSnapshotGeneratedTimeMs());
            assertEquals(20000L, ctx.metrics.latestSnapshotGeneratedAgeMs());
            assertEquals(20000L, ctx.readLongGauge("LatestSnapshotGeneratedAgeMs"));
        }
    }
}
