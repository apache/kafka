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
package org.apache.kafka.common.telemetry.internals;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ClientTelemetryEmitterTest {

    private MetricKey metricKey;
    private Instant now;

    @BeforeEach
    public void setUp() {
        metricKey = new MetricKey("name", Collections.emptyMap());
        now = Instant.now();
    }

    @Test
    public void testShouldEmitMetric() {
        Predicate<? super MetricKeyable> selector = ClientTelemetryUtils.getSelectorFromRequestedMetrics(
            Collections.singletonList("io.test.metric"));
        ClientTelemetryEmitter emitter = new ClientTelemetryEmitter(selector, true);

        assertTrue(emitter.shouldEmitMetric(new MetricKey("io.test.metric")));
        assertTrue(emitter.shouldEmitMetric(new MetricKey("io.test.metric1")));
        assertTrue(emitter.shouldEmitMetric(new MetricKey("io.test.metric.producer.bytes")));
        assertFalse(emitter.shouldEmitMetric(new MetricKey("io.test")));
        assertFalse(emitter.shouldEmitMetric(new MetricKey("org.io.test.metric")));
        assertTrue(emitter.shouldEmitDeltaMetrics());
    }

    @Test
    public void testShouldEmitMetricSelectorAll() {
        ClientTelemetryEmitter emitter = new ClientTelemetryEmitter(ClientTelemetryUtils.SELECTOR_ALL_METRICS, true);

        assertTrue(emitter.shouldEmitMetric(new MetricKey("io.test.metric")));
        assertTrue(emitter.shouldEmitMetric(new MetricKey("io.test.metric1")));
        assertTrue(emitter.shouldEmitMetric(new MetricKey("io.test.metric.producer.bytes")));
        assertTrue(emitter.shouldEmitMetric(new MetricKey("io.test")));
        assertTrue(emitter.shouldEmitMetric(new MetricKey("org.io.test.metric")));
        assertTrue(emitter.shouldEmitDeltaMetrics());
    }

    @Test
    public void testShouldEmitMetricSelectorNone() {
        ClientTelemetryEmitter emitter = new ClientTelemetryEmitter(ClientTelemetryUtils.SELECTOR_NO_METRICS, true);

        assertFalse(emitter.shouldEmitMetric(new MetricKey("io.test.metric")));
        assertFalse(emitter.shouldEmitMetric(new MetricKey("io.test.metric1")));
        assertFalse(emitter.shouldEmitMetric(new MetricKey("io.test.metric.producer.bytes")));
        assertFalse(emitter.shouldEmitMetric(new MetricKey("io.test")));
        assertFalse(emitter.shouldEmitMetric(new MetricKey("org.io.test.metric")));
        assertTrue(emitter.shouldEmitDeltaMetrics());
    }

    @Test
    public void testShouldEmitDeltaMetricsFalse() {
        ClientTelemetryEmitter emitter = new ClientTelemetryEmitter(ClientTelemetryUtils.SELECTOR_ALL_METRICS, false);
        assertFalse(emitter.shouldEmitDeltaMetrics());
    }

    @Test
    public void testEmitMetric() {
        Predicate<? super MetricKeyable> selector = ClientTelemetryUtils.getSelectorFromRequestedMetrics(
            Collections.singletonList("name"));
        ClientTelemetryEmitter emitter = new ClientTelemetryEmitter(selector, true);

        SinglePointMetric gauge = SinglePointMetric.gauge(metricKey, Long.valueOf(1), now, Collections.emptySet());
        SinglePointMetric sum = SinglePointMetric.sum(metricKey, 1.0, true, now, Collections.emptySet());
        assertTrue(emitter.emitMetric(gauge));
        assertTrue(emitter.emitMetric(sum));

        MetricKey anotherKey = new MetricKey("io.name", Collections.emptyMap());
        assertFalse(emitter.emitMetric(SinglePointMetric.gauge(anotherKey, Long.valueOf(1), now, Collections.emptySet())));

        assertEquals(2, emitter.emittedMetrics().size());
        assertEquals(Arrays.asList(gauge, sum), emitter.emittedMetrics());
    }
}
