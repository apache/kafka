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

public class TokenBucket implements MeasurableStat {
    private final TimeUnit unit;
    private double credits;
    private long lastUpdateMs;

    public TokenBucket() {
        this(TimeUnit.SECONDS);
    }

    public TokenBucket(TimeUnit unit) {
        this.unit = unit;
        this.credits = 0;
        this.lastUpdateMs = 0;
    }

    @Override
    public double measure(final MetricConfig config, final long timeMs) {
        if (config.quota() == null)
            return Long.MAX_VALUE;
        final double quota = config.quota().bound();
        final double burst = (config.samples() - 1) * convert(config.timeWindowMs()) * quota;
        refill(quota, burst, timeMs);
        return this.credits;
    }

    @Override
    public void record(final MetricConfig config, final double value, final long timeMs) {
        if (config.quota() == null)
            return;
        final double quota = config.quota().bound();
        final double burst = (config.samples() - 1) * convert(config.timeWindowMs()) * quota;
        refill(quota, burst, timeMs);
        this.credits = Math.min(burst, this.credits - value);
    }

    private void refill(final double quota, final double burst, final long timeMs) {
        this.credits = Math.min(burst, this.credits + quota * convert(timeMs - lastUpdateMs));
        this.lastUpdateMs = timeMs;
    }

    private double convert(final long timeMs) {
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
