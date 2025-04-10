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
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;

class SampledStatTest {

    private SampledStat stat;
    private Time time;

    @BeforeEach
    public void setup() {
        stat = new SampleCount();
        time = new MockTime();
    }

    @Test
    public void testSampleIsPurgedIfDoesntOverlap() {
        MetricConfig config = new MetricConfig().timeWindow(1, SECONDS).samples(2);

        // Monitored window: 2s. Complete a sample and wait 2.5s after.
        completeSample(config);
        time.sleep(2500);

        double numSamples = stat.measure(config, time.milliseconds());
        assertEquals(0, numSamples, "Sample should be purged if doesn't overlap the window");
    }

    @Test
    public void testSampleIsKeptIfOverlaps() {
        MetricConfig config = new MetricConfig().timeWindow(1, SECONDS).samples(2);

        // Monitored window: 2s. Complete a sample and wait 1.5s after.
        completeSample(config);
        time.sleep(1500);

        double numSamples = stat.measure(config, time.milliseconds());
        assertEquals(1, numSamples, "Sample should be kept if overlaps the window");
    }

    @Test
    public void testSampleIsKeptIfOverlapsAndExtra() {
        MetricConfig config = new MetricConfig().timeWindow(1, SECONDS).samples(2);

        // Monitored window: 2s. Create 2 samples with gaps in between and
        // take a measurement at 2.2s from the start.
        completeSample(config);
        time.sleep(100);
        completeSample(config);
        time.sleep(100);
        stat.record(config, 1, time.milliseconds());

        double numSamples = stat.measure(config, time.milliseconds());
        assertEquals(3, numSamples, "Sample should be kept if overlaps the window and is n+1");
    }

    // Creates a sample with events at the start and at the end. Positions clock at the end.
    private void completeSample(MetricConfig config) {
        stat.record(config, 1, time.milliseconds());
        time.sleep(config.timeWindowMs() - 1);
        stat.record(config, 1, time.milliseconds());
        time.sleep(1);
    }

    // measure() of this impl returns the number of samples
    static class SampleCount extends SampledStat {

        SampleCount() {
            super(0);
        }

        @Override
        protected void update(Sample sample, MetricConfig config, double value, long timeMs) {
            sample.value = 1;
        }

        @Override
        public double combine(List<Sample> samples, MetricConfig config, long now) {
            return samples.stream().mapToDouble(s -> s.value).sum();
        }
    }
}
