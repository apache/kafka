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

public class RealTokenBucket extends SampledStat {

    private TimeUnit unit;
    private double credits;
    private long lastUpdateMs;

    public RealTokenBucket() {
        this(TimeUnit.SECONDS);
    }

    public RealTokenBucket(TimeUnit unit) {
        super(0);
        this.unit = unit;
        this.credits = 0;
        this.lastUpdateMs = 0;
    }

    private void refill(double quota, long now) {
        this.credits = Math.max(0, this.credits - quota * convert(now - lastUpdateMs));
        this.lastUpdateMs = now;
    }

    private double convert(long timeMs) {
        return unit.convert(timeMs, MILLISECONDS);
    }

    @Override
    public void record(MetricConfig config, double value, long timeMs) {
        refill(config.quota().bound(), timeMs);
        this.credits += value;
    }

    @Override
    protected void update(final Sample sample, final MetricConfig config, final double value,
        final long timeMs) {
        // Nothing
    }

    @Override
    public double combine(final List<Sample> samples, final MetricConfig config, final long now) {
        refill(config.quota().bound(), now);
        return this.credits;
    }
}
