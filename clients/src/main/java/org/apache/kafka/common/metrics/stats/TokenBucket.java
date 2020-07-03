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

import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.metrics.MetricConfig;

import java.util.List;

/**
 * A {@link SampledStat} that mimics the behavior of a Token Bucket and is meant to
 * be used in conjunction with a {@link Rate} and a {@link org.apache.kafka.common.metrics.Quota}.
 *
 * The {@link TokenBucket} considers each sample as the amount of credits spent during the sample's
 * window while giving back credits based on the defined quota.
 *
 * At time T, it computes the total O as follow:
 * - O(T) = max(0, O(T-1) - Q * (W(T) - W(T-1)) + S(T)
 * Where:
 * - Q is the defined Quota or 0 if undefined
 * - W is the time of the sample or now if undefined
 * - S is the value of the sample or 0 if undefined
 *
 * Example with 3 samples with a Quota = 2:
 * - S1 at T+0s => 4
 * - S2 at T+2s => 2
 * - S3 at T+4s => 6
 *
 * The total at T+6s is computed as follow:
 * - T0 => Total at T+0s => S1 = 4
 * - T1 => Total at T+2s => max(0, T0 - Q * dT) + S2 = (4 - 2 * 2) + 2 = 2
 * - T2 => Total at T+4s => max(0, T1 - Q * dT) + S3 = (2 - 2 * 2) + 6 = 6
 * - T3 => Total at T+6s => max(0, T2 - Q * dT) = (6 - 2 * 2) = 2
 */
public class TokenBucket extends SampledStat {

    private final TimeUnit unit;

    public TokenBucket() {
        this(TimeUnit.SECONDS);
    }

    public TokenBucket(TimeUnit unit) {
        super(0);
        this.unit = unit;
    }

    @Override
    protected void update(Sample sample, MetricConfig config, double value, long now) {
        sample.value += value;
    }

    @Override
    public double combine(List<Sample> samples, MetricConfig config, long now) {
        if (this.samples.isEmpty())
            return 0;

        final double quota = config.quota() != null ? config.quota().bound() : 0;
        final int startIndex = (this.current + 1) % this.samples.size();

        long lastWindowMs = Long.MAX_VALUE;
        double total = 0.0;

        for (int i = 0; i < this.samples.size(); i++) {
            int current = (startIndex + i) % this.samples.size();
            Sample sample = this.samples.get(current);

            // The total is decreased by (last window - current window) * quota. We basically
            // pay back the amount of credits spent until we reach zero (dept fully paid).
            total = adjustedTotal(total, quota, lastWindowMs, sample.lastWindowMs);

            // The current sample is added to the total. If the sample is a negative number,
            // we ensure that total remains >= 0. A negative number can be used to give back
            // credits if there were not used. If the number of credits given back is higher
            // than the current dept, we basically assume that the dept is paid off.
            total = Math.max(0, total + sample.value);

            // Memorize the current window so we can use for the next sample.
            lastWindowMs = sample.lastWindowMs;
        }

        return adjustedTotal(total, quota, lastWindowMs, now);
    }

    private double adjustedTotal(double total, double quota, long lastWindowMs, long nowMs) {
        return Math.max(0, total - delta(quota, lastWindowMs, nowMs));
    }

    private double delta(double quota, long lastWindowMs, long nowMs) {
        return quota * convert(Math.max(0, nowMs - lastWindowMs));
    }

    private double convert(long timeMs) {
        switch (unit) {
            case NANOSECONDS:
                return timeMs * 1000.0 * 1000.0;
            case MICROSECONDS:
                return timeMs * 1000.0;
            case MILLISECONDS:
                return timeMs;
            case SECONDS:
                return timeMs / 1000.0;
            case MINUTES:
                return timeMs / (60.0 * 1000.0);
            case HOURS:
                return timeMs / (60.0 * 60.0 * 1000.0);
            case DAYS:
                return timeMs / (24.0 * 60.0 * 60.0 * 1000.0);
            default:
                throw new IllegalStateException("Unknown unit: " + unit);
        }
    }
}
