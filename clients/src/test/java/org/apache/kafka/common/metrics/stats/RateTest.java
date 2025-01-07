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

import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.internals.MetricsUtils;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RateTest {
    private static final double EPS = 0.000001;
    private Rate rate;
    private Time time;

    @BeforeEach
    public void setup() {
        rate = new Rate();
        time = new MockTime();
    }

    // Tests the scenario where the recording and measurement is done before the window for first sample finishes
    // with no prior samples retained.
    @ParameterizedTest
    @CsvSource({"1,1", "1,11", "11,1", "11,11"})
    public void testRateWithNoPriorAvailableSamples(int numSample, int sampleWindowSizeSec) {
        final MetricConfig config = new MetricConfig().samples(numSample).timeWindow(sampleWindowSizeSec, TimeUnit.SECONDS);
        final double sampleValue = 50.0;
        // record at beginning of the window
        rate.record(config, sampleValue, time.milliseconds());
        // forward time till almost the end of window
        final long measurementTime = TimeUnit.SECONDS.toMillis(sampleWindowSizeSec) - 1;
        time.sleep(measurementTime);
        // calculate rate at almost the end of window
        final double observedRate = rate.measure(config, time.milliseconds());
        assertFalse(Double.isNaN(observedRate));

        // In a scenario where sufficient number of samples is not available yet, the rate calculation algorithm assumes
        // presence of N-1 (where N = numSample) prior samples with sample values of 0. Hence, the window size for rate
        // calculation accounts for N-1 prior samples
        final int dummyPriorSamplesAssumedByAlgorithm = numSample - 1;
        final double windowSize = MetricsUtils.convert(measurementTime, TimeUnit.SECONDS) + (dummyPriorSamplesAssumedByAlgorithm * sampleWindowSizeSec);
        double expectedRatePerSec = sampleValue / windowSize;
        assertEquals(expectedRatePerSec, observedRate, EPS);
    }

    // Record an event every 100 ms on average, moving some 1 ms back or forth for fine-grained 
    // window control. The expected rate, hence, is 10-11 events/sec depending on the moment of 
    // measurement. Start assertions from the second window. This test covers the case where a
    // sample window partially overlaps with the monitored window.
    @Test
    public void testRateIsConsistentAfterTheFirstWindow() {
        MetricConfig config = new MetricConfig().timeWindow(1, SECONDS).samples(2);
        List<Integer> steps = Arrays.asList(0, 99, 100, 100, 100, 100, 100, 100, 100, 100, 100);

        // start the first window and record events at 0,99,199,...,999 ms 
        for (int stepMs : steps) {
            time.sleep(stepMs);
            rate.record(config, 1, time.milliseconds());
        }

        // making a gap of 100 ms between windows
        time.sleep(101);

        // start the second window and record events at 0,99,199,...,999 ms
        for (int stepMs : steps) {
            time.sleep(stepMs);
            rate.record(config, 1, time.milliseconds());
            double observedRate = rate.measure(config, time.milliseconds());
            assertTrue(10 <= observedRate && observedRate <= 11);
            // make sure measurements are repeatable with the same timestamp
            double measuredAgain = rate.measure(config, time.milliseconds());
            assertEquals(observedRate, measuredAgain);
        }
    }
}
