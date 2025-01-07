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
package org.apache.kafka.server.quota;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Quota;
import org.apache.kafka.common.metrics.QuotaViolationException;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.metrics.stats.Value;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class QuotaUtilsTest {

    private final Time time = new MockTime();
    private final int numSamples = 10;
    private final int maxThrottleTimeMs = 500;
    private final MetricName metricName = new MetricName("test-metric", "groupA", "testA", Collections.emptyMap());

    @Test
    public void testThrottleTimeObservedRateEqualsQuota() {
        int numSamples = 10;
        double observedValue = 16.5;

        assertEquals(0, throttleTime(observedValue, observedValue, numSamples));

        // should be independent of window size
        assertEquals(0, throttleTime(observedValue, observedValue, numSamples + 1));
    }

    @Test
    public void testThrottleTimeObservedRateBelowQuota() {
        double observedValue = 16.5;
        double quota = 20.4;
        assertTrue(throttleTime(observedValue, quota, numSamples) < 0);

        // should be independent of window size
        assertTrue(throttleTime(observedValue, quota, numSamples + 1) < 0);
    }

    @Test
    public void testThrottleTimeObservedRateAboveQuota() {
        double quota = 50.0;
        double observedValue = 100.0;
        assertEquals(2000, throttleTime(observedValue, quota, 3));
    }

    @Test
    public void testBoundedThrottleTimeObservedRateEqualsQuota() {
        double observedValue = 18.2;
        assertEquals(0, boundedThrottleTime(observedValue, observedValue, numSamples, maxThrottleTimeMs));

        // should be independent of window size
        assertEquals(0, boundedThrottleTime(observedValue, observedValue, numSamples + 1, maxThrottleTimeMs));
    }

    @Test
    public void testBoundedThrottleTimeObservedRateBelowQuota() {
        double observedValue = 16.5;
        double quota = 22.4;

        assertTrue(boundedThrottleTime(observedValue, quota, numSamples, maxThrottleTimeMs) < 0);

        // should be independent of window size
        assertTrue(boundedThrottleTime(observedValue, quota, numSamples + 1, maxThrottleTimeMs) < 0);
    }

    @Test
    public void testBoundedThrottleTimeObservedRateAboveQuotaBelowLimit() {
        double quota = 50.0;
        double observedValue = 55.0;
        assertEquals(100, boundedThrottleTime(observedValue, quota, 2, maxThrottleTimeMs));
    }

    @Test
    public void testBoundedThrottleTimeObservedRateAboveQuotaAboveLimit() {
        double quota = 50.0;
        double observedValue = 100.0;
        assertEquals(maxThrottleTimeMs, boundedThrottleTime(observedValue, quota, numSamples, maxThrottleTimeMs));
    }

    @Test
    public void testThrottleTimeThrowsExceptionIfProvidedNonRateMetric() {
        KafkaMetric testMetric = new KafkaMetric(new Object(), metricName, new Value(), new MetricConfig(), time);

        assertThrows(IllegalArgumentException.class,
                () -> QuotaUtils.throttleTime(new QuotaViolationException(testMetric, 10.0, 20.0), time.milliseconds()));
    }

    @Test
    public void testBoundedThrottleTimeThrowsExceptionIfProvidedNonRateMetric() {
        KafkaMetric testMetric = new KafkaMetric(new Object(), metricName, new Value(), new MetricConfig(), time);

        assertThrows(IllegalArgumentException.class,
                () -> QuotaUtils.boundedThrottleTime(new QuotaViolationException(testMetric, 10.0, 20.0), maxThrottleTimeMs, time.milliseconds()));
    }

    // the `metric` passed into the returned QuotaViolationException will return windowSize = 'numSamples' - 1
    private QuotaViolationException quotaViolationException(double observedValue, double quota, int numSamples) {
        int sampleWindowSec = 1;
        MetricConfig metricConfig = new MetricConfig()
                .timeWindow(sampleWindowSec, TimeUnit.SECONDS)
                .samples(numSamples)
                .quota(new Quota(quota, true));
        KafkaMetric metric = new KafkaMetric(new Object(), metricName, new Rate(), metricConfig, time);
        return new QuotaViolationException(metric, observedValue, quota);
    }

    private long throttleTime(double observedValue, double quota, int numSamples) {
        QuotaViolationException e = quotaViolationException(observedValue, quota, numSamples);
        return QuotaUtils.throttleTime(e, time.milliseconds());
    }

    private long boundedThrottleTime(double observedValue, double quota, int numSamples, long maxThrottleTime) {
        QuotaViolationException e = quotaViolationException(observedValue, quota, numSamples);
        return QuotaUtils.boundedThrottleTime(e, maxThrottleTime, time.milliseconds());
    }
}
