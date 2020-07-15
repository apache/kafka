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

import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.kafka.common.metrics.MetricConfig;

import java.util.List;

/**
 * A {@link SampledStat} that mimics the behavior of a Token Bucket and is meant to
 * be used in conjunction with a {@link Rate} and a {@link org.apache.kafka.common.metrics.Quota}.
 *
 * The {@link SampledStat} records samples, where each sample has a value and belongs to a
 * time window, and combine them by applying algorithm sample by sample.
 *
 * Our Token Bucket inspired algorithm differs from a regular Token Bucket in three ways:
 * 1) The bucket is reversed and starts at 0 instead of starting at the maximum burst. It
 *    means that using tokens is reflected by increasing the number of tokens in the bucket
 *    rather than decreasing it.
 * 2) The bucket has a maximum burst but that burst can be exceeded. This allows to consume
 *    more tokens than the maximum burst. When this happens, one has to wait until the
 *    number of tokens goes back bellow the maximum burst to be able to consume more tokens.
 * 3) The bucket relies on a set of samples. When a sample is expired, it is gone and therefore
 *    not counted anymore. It is like that is has been fully reimbursed eventhough it may not be
 *    true (e.g. a single burst >> samples * sample length * rate). When all the samples are
 *    expired, the bucket restarts from zero.
 *
 * Let's define the following Token Bucket algorithm:
 * - TK: The current number of tokens in the bucket
 * - LT: The last time TK got updated
 * - B: The maximum burst (number of samples * sample length)
 * - R: The rate at which the tokens are decreased (quota * sample length)
 *
 * At time T, the number of tokens in the bucket is computed with:
 * - TK = max(TK - (T - LT) * R), 0)
 *
 * At time T, a request with a burst worth of K is admitted iif TK < B, and
 * update TK = TK + K.
 *
 * When combining the samples, TK is initialized to 0 and all the samples are applied
 * in chronological order. For each sample, TK is updated with the above formula to
 * reflect the credits that have been accumulated, the lastWindowMs of the sample is
 * used to update LT, and finally the value of the sample is added to TK. In the end,
 * that gives the amount of credits that have been used until the last sample.
 *
 * As the last sample may be already quite old at the time of combining, because a new
 * sample is only created at the recording time, TK is further decreased with the above
 * formula.
 */
public class SampledTokenBucket extends SampledStat {

    private final TimeUnit unit;

    public SampledTokenBucket() {
        this(TimeUnit.SECONDS);
    }

    public SampledTokenBucket(TimeUnit unit) {
        super(0);
        this.unit = unit;
    }

    @Override
    protected void update(Sample sample, MetricConfig config, double value, long now) {
        sample.value += value;
    }

    @Override
    public double combine(List<Sample> samples, MetricConfig config, long now) {
        printSamples(now);

        if (this.samples.isEmpty())
            return 0;

        final double quota = config.quota() != null ? config.quota().bound() : 0;
        final int startIndex = (this.current + 1) % this.samples.size();

        long lastWindowMs = Long.MAX_VALUE;
        double credits = 0.0;

        for (int i = 0; i < this.samples.size(); i++) {
            int current = (startIndex + i) % this.samples.size();
            Sample sample = this.samples.get(current);

            // The credits is decreased by (last window - current window) * quota. We basically
            // pay back the amount of credits spent until we reach zero (dept fully paid).
            credits = Math.max(0, credits - quota * convert(Math.max(0, sample.lastWindowMs - lastWindowMs)));
            // Memorize the current window so we can use for the next sample.
            lastWindowMs = sample.lastWindowMs;

            // The current sample is added to the total. If the sample is a negative number,
            // we ensure that total remains >= 0. A negative number can be used to give back
            // credits if there were not used. If the number of credits given back is higher
            // than the current dept, we basically assume that the dept is paid off.
            credits = Math.max(0, credits + sample.value);
        }

        return Math.max(0, credits - quota * convert(Math.max(0, now - lastWindowMs)));
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

    private double convert(long timeMs) {
        return unit.convert(timeMs, MILLISECONDS);
    }
}
