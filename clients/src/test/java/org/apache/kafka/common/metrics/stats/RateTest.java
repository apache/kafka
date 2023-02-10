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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class RateTest {
    private static final double EPS = 0.000001;
    private Rate r;
    private Time timeClock;

    @BeforeEach
    public void setup() {
        r = new Rate();
        timeClock = new MockTime();
    }

    // Tests the scenario where the recording and measurement is done before the window for first sample finishes
    // with no prior samples retained.
    @ParameterizedTest
    @CsvSource({"1,1", "1,11", "11,1", "11,11"})
    public void testRateWithNoPriorAvailableSamples(int numSample, int sampleWindowSizeSec) {
        final MetricConfig config = new MetricConfig().samples(numSample).timeWindow(sampleWindowSizeSec, TimeUnit.SECONDS);
        final double sampleValue = 50.0;
        // record at beginning of the window
        r.record(config, sampleValue, timeClock.milliseconds());
        // forward time till almost the end of window
        final long measurementTime = TimeUnit.SECONDS.toMillis(sampleWindowSizeSec) - 1;
        timeClock.sleep(measurementTime);
        // calculate rate at almost the end of window
        final double observedRate = r.measure(config, timeClock.milliseconds());
        assertFalse(Double.isNaN(observedRate));

        // In a scenario where sufficient number of samples is not available yet, the rate calculation algorithm assumes
        // presence of N-1 (where N = numSample) prior samples with sample values of 0. Hence, the window size for rate
        // calculation accounts for N-1 prior samples
        final int dummyPriorSamplesAssumedByAlgorithm = numSample - 1;
        final double windowSize = MetricsUtils.convert(measurementTime, TimeUnit.SECONDS) + (dummyPriorSamplesAssumedByAlgorithm * sampleWindowSizeSec);
        double expectedRatePerSec = sampleValue / windowSize;
        assertEquals(expectedRatePerSec, observedRate, EPS);
    }
}
