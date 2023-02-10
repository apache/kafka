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
package org.apache.kafka.common.metrics.stats;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.CompoundStat.NamedMeasurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.junit.jupiter.api.Test;

public class MeterTest {

    private static final double EPS = 0.0000001d;

    @Test
    public void testMeter() {
        Map<String, String> emptyTags = Collections.emptyMap();
        MetricName rateMetricName = new MetricName("rate", "test", "", emptyTags);
        MetricName totalMetricName = new MetricName("total", "test", "", emptyTags);
        Meter meter = new Meter(rateMetricName, totalMetricName);
        List<NamedMeasurable> stats = meter.stats();
        assertEquals(2, stats.size());
        NamedMeasurable total = stats.get(0);
        NamedMeasurable rate = stats.get(1);
        assertEquals(rateMetricName, rate.name());
        assertEquals(totalMetricName, total.name());
        Rate rateStat = (Rate) rate.stat();
        CumulativeSum totalStat = (CumulativeSum) total.stat();

        MetricConfig config = new MetricConfig();
        double nextValue = 0.0;
        double expectedTotal = 0.0;
        long now = 0;
        double intervalMs = 100;
        double delta = 5.0;

        // Record values in multiple windows and verify that rates are reported
        // for time windows and that the total is cumulative.
        for (int i = 1; i <= 100; i++) {
            for (; now < i * 1000; now += intervalMs, nextValue += delta) {
                expectedTotal += nextValue;
                meter.record(config, nextValue, now);
            }
            assertEquals(expectedTotal, totalStat.measure(config, now), EPS);
            long windowSizeMs = rateStat.windowSize(config, now);
            long windowStartMs = Math.max(now - windowSizeMs, 0);
            double sampledTotal = 0.0;
            double prevValue = nextValue - delta;
            for (long timeMs = now - 100; timeMs >= windowStartMs; timeMs -= intervalMs, prevValue -= delta)
                sampledTotal += prevValue;
            assertEquals(sampledTotal * 1000 / windowSizeMs, rateStat.measure(config, now), EPS);
        }
    }
}
