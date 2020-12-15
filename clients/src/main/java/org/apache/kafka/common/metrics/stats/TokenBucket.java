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
import org.apache.kafka.common.metrics.MeasurableStat;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Quota;

import static org.apache.kafka.common.metrics.internals.MetricsUtils.convert;

/**
 * The {@link TokenBucket} is a {@link MeasurableStat} implementing a token bucket algorithm
 * that is usable within a {@link org.apache.kafka.common.metrics.Sensor}.
 *
 * The {@link Quota#bound()} defined the refill rate of the bucket while the maximum burst or
 * the maximum number of credits of the bucket is defined by
 * {@link MetricConfig#samples() * MetricConfig#timeWindowMs() * Quota#bound()}.
 *
 * The quota is considered as exhausted when the amount of remaining credits in the bucket
 * is below zero. The enforcement is done by the {@link org.apache.kafka.common.metrics.Sensor}.
 *
 * Token Bucket vs Rate based Quota:
 * The current sampled rate based quota does not cope well with bursty workloads. The issue is
 * that a unique and large sample can hold the average above the quota until it is discarded.
 * Practically, when this happens, one must wait until the sample is expired to bring the rate
 * below the quota even though less time would be theoretically required. As an examples, let's
 * imagine that we have:
 * - Quota (Q)   = 5
 * - Samples (S) = 100
 * - Window (W)  = 1s
 * A burst of 560 brings the average rate (R) to 5.6 (560 / 100). The expected throttle time is
 * computed as follow: ((R - Q / Q * S * W)) = ((5.6 - 5) / 5 * 100 * 1) = 12 secs. In practice,
 * the average rate won't go below the quota before the burst is dropped from the samples so one
 * must wait 100s (S * W).
 *
 * The token bucket relies on continuously updated amount of credits. Therefore, it does not
 * suffers from the above issue. The same example would work as follow:
 * - Quota (Q) = 5
 * - Burst (B) = 5 * 1 * 100 = 500 (Q * S * W)
 * A burst of 560 brings the amount of credits to -60. One must wait 12s (-(-60)/5) to refill the
 * bucket to zero.
 */
public class TokenBucket implements MeasurableStat {
    private final TimeUnit unit;
    private double tokens;
    private long lastUpdateMs;

    public TokenBucket() {
        this(TimeUnit.SECONDS);
    }

    public TokenBucket(TimeUnit unit) {
        this.unit = unit;
        this.tokens = 0;
        this.lastUpdateMs = 0;
    }

    @Override
    public double measure(final MetricConfig config, final long timeMs) {
        if (config.quota() == null)
            return Long.MAX_VALUE;
        final double quota = config.quota().bound();
        final double burst = burst(config);
        refill(quota, burst, timeMs);
        return this.tokens;
    }

    @Override
    public void record(final MetricConfig config, final double value, final long timeMs) {
        if (config.quota() == null)
            return;
        final double quota = config.quota().bound();
        final double burst = burst(config);
        refill(quota, burst, timeMs);
        this.tokens = Math.min(burst, this.tokens - value);
    }

    private void refill(final double quota, final double burst, final long timeMs) {
        this.tokens = Math.min(burst, this.tokens + quota * convert(timeMs - lastUpdateMs, unit));
        this.lastUpdateMs = timeMs;
    }

    private double burst(final MetricConfig config) {
        return config.samples() * convert(config.timeWindowMs(), unit) * config.quota().bound();
    }
}
