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
package org.apache.kafka.coordinator.common.runtime;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.CompoundStat;
import org.apache.kafka.common.metrics.Metrics;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class KafkaMetricHistogramTest {

    @Test
    public void testStats() {
        try (Metrics metrics = new Metrics()) {
            KafkaMetricHistogram histogram = KafkaMetricHistogram.newLatencyHistogram(
                suffix -> metrics.metricName(
                    "test-metric-" + suffix,
                    "test-group",
                    "test description"
                )
            );

            Set<MetricName> expected = Set.of(
                new MetricName("test-metric-max", "test-group", "test description", Collections.emptyMap()),
                new MetricName("test-metric-p999", "test-group", "test description", Collections.emptyMap()),
                new MetricName("test-metric-p99", "test-group", "test description", Collections.emptyMap()),
                new MetricName("test-metric-p95", "test-group", "test description", Collections.emptyMap()),
                new MetricName("test-metric-p50", "test-group", "test description", Collections.emptyMap())
            );
            Set<MetricName> actual = histogram.stats().stream().map(CompoundStat.NamedMeasurable::name).collect(Collectors.toSet());
            assertEquals(expected, actual);
        }
    }
}
