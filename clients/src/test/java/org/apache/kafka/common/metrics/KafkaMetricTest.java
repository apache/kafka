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
import org.apache.kafka.common.utils.MockTime;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KafkaMetricTest {

    private static final MetricName METRIC_NAME = new MetricName("name", "group", "description", Collections.emptyMap());

    @Test
    public void testIsMeasurable() {
        Measurable metricValueProvider = (config, now) -> 0;
        KafkaMetric metric = new KafkaMetric(new Object(), METRIC_NAME, metricValueProvider, new MetricConfig(), new MockTime());
        assertTrue(metric.isMeasurable());
        assertEquals(metricValueProvider, metric.measurable());
    }

    @Test
    public void testIsMeasurableWithGaugeProvider() {
        Gauge<Double> metricValueProvider = (config, now) -> 0.0;
        KafkaMetric metric = new KafkaMetric(new Object(), METRIC_NAME, metricValueProvider, new MetricConfig(), new MockTime());
        assertFalse(metric.isMeasurable());
        assertThrows(IllegalStateException.class, metric::measurable);
    }

}
