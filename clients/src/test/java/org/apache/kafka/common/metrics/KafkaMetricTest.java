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
package org.apache.kafka.common.metrics;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.utils.MockTime;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KafkaMetricTest {

    private static final MetricName METRIC_NAME_1 = new MetricName("name", "group", "description", Map.of());
    private static final MetricName METRIC_NAME_2 = new MetricName(
            "request-latency-avg",
            "consumer-fetch-manager-metrics",
            "The average request latency in ms",
            Map.of("client-id", "consumer-1")
    );

    @Test
    public void testIsMeasurable() {
        Measurable metricValueProvider = (config, now) -> 0;
        KafkaMetric metric = new KafkaMetric(new Object(), METRIC_NAME_1, metricValueProvider, new MetricConfig(), new MockTime());
        assertTrue(metric.isMeasurable());
        assertEquals(metricValueProvider, metric.measurable());
    }

    @Test
    public void testIsMeasurableWithGaugeProvider() {
        Gauge<Double> metricValueProvider = (config, now) -> 0.0;
        KafkaMetric metric = new KafkaMetric(new Object(), METRIC_NAME_1, metricValueProvider, new MetricConfig(), new MockTime());
        assertFalse(metric.isMeasurable());
        assertThrows(IllegalStateException.class, metric::measurable);
    }

    @Test
    public void testMeasurableValueReturnsZeroWhenNotMeasurable() {
        MockTime time = new MockTime();
        MetricConfig config = new MetricConfig();
        Gauge<Integer> gauge = (c, now) -> 7;

        KafkaMetric metric = new KafkaMetric(new Object(), METRIC_NAME_1, gauge, config, time);
        assertEquals(0.0d, metric.measurableValue(time.milliseconds()), 0.0d);
    }

    @Test
    public void testKafkaMetricAcceptsNonMeasurableNonGaugeProvider() {
        MetricValueProvider<String> provider = (config, now) -> "metric value provider";
        KafkaMetric metric = new KafkaMetric(new Object(), METRIC_NAME_1, provider, new MetricConfig(), new MockTime());

        Object value = metric.metricValue();
        assertEquals("metric value provider", value);
    }

    @Test
    public void testConstructorWithNullProvider() {
        assertThrows(NullPointerException.class, () ->
                new KafkaMetric(new Object(), METRIC_NAME_1, null, new MetricConfig(), new MockTime())
        );
    }

    /**
     * Verifies that toString produces a human-readable representation suitable for logging.
     * Note that we skip the metric provider in this case, but this is still a
     * significant improvement over the default Object.toString() output (e.g. "KafkaMetric@62a7d6c6").
     */
    @Test
    public void testToStringWithLambdaProvider() {
        Measurable metricValueProvider = (config, now) -> 0;
        testToStringOnLambdaOrAnonymousClass(metricValueProvider);
    }

    /**
     * Verifies that toString produces a human-readable representation suitable for logging.
     * Note that we skip the metric provider in this case, but this is still a
     * significant improvement over the default Object.toString() output (e.g. "KafkaMetric@62a7d6c6").
     */
    @Test
    public void testToStringWithAnonymousClassProvider() {
        //noinspection Convert2Lambda
        Measurable metricValueProvider = new Measurable() {
            @Override
            public double measure(MetricConfig config, long now) {
                return 0;
            }
        };
        testToStringOnLambdaOrAnonymousClass(metricValueProvider);
    }

    @Test
    public void testToStringWithStatProvider() {
        Avg avg = new Avg();
        KafkaMetric metric = new KafkaMetric(new Object(), METRIC_NAME_2, avg, new MetricConfig(), new MockTime());

        assertEquals("KafkaMetric [metricName=MetricName [name=request-latency-avg, "
                + "group=consumer-fetch-manager-metrics, "
                + "description=The average request latency in ms, "
                + "tags={client-id=consumer-1}], "
                + "metricValueProvider=org.apache.kafka.common.metrics.stats.Avg]",
                metric.toString());
    }

    private void testToStringOnLambdaOrAnonymousClass(Measurable metricValueProvider) {
        KafkaMetric metric = new KafkaMetric(
                new Object(), METRIC_NAME_2, metricValueProvider, new MetricConfig(), new MockTime());

        String result = metric.toString();
        assertEquals("KafkaMetric [metricName=MetricName [name=request-latency-avg, "
                        + "group=consumer-fetch-manager-metrics, "
                        + "description=The average request latency in ms, "
                        + "tags={client-id=consumer-1}]]",
                result);
    }
}
