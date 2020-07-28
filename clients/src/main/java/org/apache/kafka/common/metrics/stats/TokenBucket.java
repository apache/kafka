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
import org.apache.kafka.common.metrics.MetricConfig;

/**
 * The {@link TokenBucket} is a {@link SampledStat} implementing a token bucket that can be used
 * in conjunction with a {@link Rate} to enforce a quota.
 *
 * A token bucket accumulates tokens with a constant rate R, one token per 1/R second, up to a
 * maximum burst size B. The burst size essentially means that we keep permission to do a unit of
 * work for up to B / R seconds.
 *
 * The {@link TokenBucket} adapts this to fit within a {@link SampledStat}. It accumulates tokens
 * in chunks of Q units by default (sample length * quota) instead of one token every 1/R second
 * and expires in chunks of Q units too (when the oldest sample is expired).
 *
 * Internally, we achieve this behavior by not completing the current sample until we fill that
 * sample up to Q (used all credits). Samples are filled up one after the others until the maximum
 * number of samples is reached. If it is not possible to created a new sample, we accumulate in
 * the last one until a new one can be created. The over used credits are spilled over to the new
 * sample at when it is created. Every time a sample is purged, Q credits are made available.
 *
 * It is important to note that the maximum burst is not enforced in the class and depends on
 * how the quota is enforced in the {@link Rate}.
 */
public class TokenBucket extends SampledStat {

    private final TimeUnit unit;

    /**
     * Instantiates a new TokenBucket that works by default with a Quota {@link MetricConfig#quota()}
     * in {@link TimeUnit#SECONDS}.
     */
    public TokenBucket() {
        this(TimeUnit.SECONDS);
    }

    /**
     * Instantiates a new TokenBucket that works with the provided time unit.
     *
     * @param unit The time unit of the Quota {@link MetricConfig#quota()}
     */
    public TokenBucket(TimeUnit unit) {
        super(0);
        this.unit = unit;
    }

    @Override
    public void record(MetricConfig config, double value, long timeMs) {
        if (value < 0) {
            unrecord(config, -value, timeMs);
            return;
        }

        final double sampleWindowQuota = sampleWindowQuota(config);
        final long firstTimeWindowMs = firstTimeWindowMs(config, timeMs);

        // Get current sample or create one if empty.
        Sample sample = current(firstTimeWindowMs);

        // Verify that the current sample was not reinitialized. If it was the case,
        // restart from the first time window.
        if (sample.eventCount == 0) {
            sample.reset(firstTimeWindowMs);
        }

        // Add the value to the current sample.
        sample.value += value;
        sample.eventCount += 1;

        // If current sample is completed AND a new one can be created,
        // create one and spill over the amount above the quota to the
        // new sample. Repeat until either the sample is not complete
        // or no new sample can be created.
        while (sample.isComplete(timeMs, config)) {
            double extra = sample.value - sampleWindowQuota;
            sample.value = sampleWindowQuota;

            sample = advance(config, sample.lastWindowMs + config.timeWindowMs());
            sample.value = extra;
            sample.eventCount += 1;
        }
    }

    private void unrecord(MetricConfig config, double value, long timeMs) {
        final long firstTimeWindowMs = firstTimeWindowMs(config, timeMs);

        // Rewind
        while (value > 0) {
            // Get current sample or create one if empty.
            Sample sample = current(firstTimeWindowMs);

            // If the current sample has been purged, we can't unrecord anything.
            if (sample.eventCount == 0) {
                return;
            }

            // If the value to unrecord is smaller than the current sample, we
            // deduct it and return.
            if (value <= sample.value) {
                sample.value -= value;
                return;
            }

            // The value of the current sample is removed, and the current sample
            // is reset.
            value -= sample.value;
            sample.reset(timeMs);

            this.current = this.current - 1;
            if (this.current < 0)
                this.current = this.samples.size() - 1;
        }
    }

    private double sampleWindowQuota(MetricConfig config) {
        if (config.quota() == null)
            throw new IllegalStateException("TokenBucket can't be used without a quota.");

        return config.quota().bound() * unit.convert(config.timeWindowMs(), MILLISECONDS);
    }

    private long firstTimeWindowMs(MetricConfig config, long timeMs) {
        return timeMs - config.timeWindowMs() * (config.samples() - 1);
    }

    @Override
    protected void update(final Sample sample, final MetricConfig config, final double value, final long timeMs) {
        // Unused.
    }

    @Override
    public double combine(final List<Sample> samples, final MetricConfig config, final long now) {
        double total = 0.0;
        for (Sample sample : samples) {
            total += sample.value;
        }
        return total;
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
            final double quota = config.quota().bound();
            return value >= quota * unit.convert(config.timeWindowMs(), MILLISECONDS)
                && lastWindowMs + config.timeWindowMs() <= timeMs;
        }
    }
}
