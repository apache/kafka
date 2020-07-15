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

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.kafka.common.metrics.MetricConfig;

/**
 * TODO
 */
public class BackfillTokenBucket extends SampledStat {

    private final TimeUnit unit;

    public BackfillTokenBucket() {
        this(TimeUnit.SECONDS);
    }

    public BackfillTokenBucket(TimeUnit unit) {
        super(0);
        this.unit = unit;
    }

    @Override
    public void record(MetricConfig config, double value, long timeMs) {
        if (value < 0) {
            unrecord(config, -value, timeMs);
            return;
        }

        final double quota = config.quota().bound();
        final long oldestTimeWindowMs = timeMs - config.timeWindowMs() * (config.samples() - 1);

        // Get current sample or create one if empty
        Sample sample = current(oldestTimeWindowMs);

        // Verify that the current sample was not reinitialized. If it was the case,
        // restart from the past
        if (sample.eventCount == 0) {
            sample.reset(oldestTimeWindowMs);
        }

        // Add the value to the current sample
        sample.value += value;
        sample.eventCount += 1;

        // If current sample is completed AND a new one can be created,
        // create one and spill over the amount above the quota to the
        // new sample. Repeat until either the sample is not complete
        // or no new sample can be created.
        while (sample.isComplete(timeMs, config)) {
            double extra = sample.value - quota;
            sample.value = quota;

            sample = advance(config, sample.lastWindowMs + config.timeWindowMs());
            sample.value = extra;
            sample.eventCount += 1;
        }
    }

    private void unrecord(MetricConfig config, double value, long timeMs) {
        final double quota = config.quota().bound();
        final long oldestTimeWindowMs = timeMs - config.timeWindowMs() * (config.samples() - 1);

        // Rewind
        while (value > 0) {
            // Get current sample or create one if empty
            Sample sample = current(oldestTimeWindowMs);

            // If the current sample has been purged, we can't unrecord anything
            if (sample.eventCount == 0) {
                return;
            }

            if (value <= sample.value) {
                sample.value -= value;
                return;
            }

            sample.reset(timeMs);
            value -= quota;

            this.current = this.current - 1;
            if (this.current <= 0)
                this.current = this.samples.size() - 1;
        }
    }

    @Override
    protected void update(final Sample sample, final MetricConfig config, final double value, final long timeMs) {
        // Nothing
    }

    @Override
    public double combine(final List<Sample> samples, final MetricConfig config, final long now) {
        printSamples(now);

        double total = 0.0;
        for (Sample sample : samples) {
            total += sample.value;
        }
        return total;
    }

    private void printSamples(long now) {
        System.out.print("" + now + ": " + this.getClass().getSimpleName() + " [");
        System.out.print(samples.stream()
            .filter(s -> s.eventCount > 0)
            .sorted((o1, o2) -> (int) (o1.lastWindowMs - o2.lastWindowMs))
            .map(s -> s.value)
            .map(v -> "" + v)
            .collect(Collectors.joining(",")));
        System.out.println("]");
    }

    protected Sample newSample(long timeMs) {
        return new TokenBucketSample(this.initialValue, timeMs, this.unit);
    }

    private static class TokenBucketSample extends SampledStat.Sample {
        private final TimeUnit unit;

        public TokenBucketSample(double initialValue, long now, TimeUnit unit) {
            super(initialValue, now);
            this.unit = unit;
        }

        public boolean isComplete(long timeMs, MetricConfig config) {
            // TODO Handle time unit
            final double quota = config.quota().bound();
            return value > quota * convert(config.timeWindowMs())
                && lastWindowMs + config.timeWindowMs() <= timeMs;
        }

        private double convert(long timeMs) {
            return unit.convert(timeMs, MILLISECONDS);
        }
    }
}
