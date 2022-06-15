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

import java.util.Locale;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.metrics.MeasurableStat;
import org.apache.kafka.common.metrics.MetricConfig;

import static org.apache.kafka.common.metrics.internals.MetricsUtils.convert;

/**
 * The rate of the given quantity. By default this is the total observed over a set of samples from a sampled statistic
 * divided by the elapsed time over the sample windows. Alternative {@link SampledStat} implementations can be provided,
 * however, to record the rate of occurrences (e.g. the count of values measured over the time interval) or other such
 * values.
 */
public class Rate implements MeasurableStat {

    protected final TimeUnit unit;
    protected final SampledStat stat;

    public Rate() {
        this(TimeUnit.SECONDS);
    }

    public Rate(TimeUnit unit) {
        this(unit, new WindowedSum());
    }

    public Rate(SampledStat stat) {
        this(TimeUnit.SECONDS, stat);
    }

    public Rate(TimeUnit unit, SampledStat stat) {
        this.stat = stat;
        this.unit = unit;
    }

    public String unitName() {
        return unit.name().substring(0, unit.name().length() - 2).toLowerCase(Locale.ROOT);
    }

    @Override
    public void record(MetricConfig config, double value, long timeMs) {
        this.stat.record(config, value, timeMs);
    }

    @Override
    public double measure(MetricConfig config, long now) {
        double value = stat.measure(config, now);
        return value / convert(windowSize(config, now), unit);
    }

    public long windowSize(MetricConfig config, long now) {
        // Purge obsolete samples. Obsolete samples are the ones which are not relevant to the current calculation
        // because their creation time is outside (before) the duration of time window used to calculate rate.
        stat.purgeObsoleteSamples(config, now);

        /*
         * Here we check the total amount of time elapsed since the oldest non-obsolete window.
         * This gives the duration of computation time window which is used to calculate Rate.
         *
         * For scenarios when rate computation is performed after at least `config.samples` have completed,
         * the duration of computation time window is determined by:
         *      window duration = (now - start time of oldest non-obsolete window)
         *
         * ## Special case: First ever window
         * A special scenario occurs when rate calculation is performed before at least `config.samples` have completed
         * (e.g. if only 1 second has elapsed in a 30 second). In such a scenario, window duration would be equal to the
         * time elapsed in the current window (since oldest non-obsolete window is current window). This leads to the
         * following values for rate based on the above window duration formula above. Consider the following example:
         *      config.timeWindowMs() = 1s
         *      config.samples() = 2
         *      Record events (E) at timestamps:
         *          E1 = CurrentTimeStamp (T1)
         *          E2 = T1 + 30ms
         *          E3 = T1 + 60ms
         *      Rate calculated at T1 + 20ms = 1/0.02s = 50 events per second
         *      Rate calculated at T1 + 50ms = 2/0.05s = 40 events per second
         *      Rate calculated at T1 + 60ms = 3/0.06s = 50 events per second
         * When the Rate function in this class is used to calculate throttling, this approach doesn't handle the
         * scenarios where an occasional burst of traffic will be acceptable (because the overall rate might exceed at
         * the first request itself) and instead opts for ensuring a conservative approach of assuming higher values of
         * rate. Note that {@link SimpleRate} uses this approach. But current implementation uses a different formula
         * for this special case as described later.
         *
         * ## Special case: First window after prolonged period of no record events
         * Another special scenario occurs when a record event is received after multiples of `config.timeWindowMs`
         * have elapsed since the last record event. In such a scenario, all the older samples would have been
         * considered obsolete and would have been purged at the beginning of this function. Note that this scenario is
         * different from Special Case of First ever window because unlike first ever window (where prior window is
         * missing), this scenario genuinely has a prior window with zero events.
         *
         * In both the above special cases, the algorithm in this function takes the same approach i.e. it assumes
         * presence of prior windows with zero records. Hence, the duration of computation time window is determined by:
         *          window duration = (now - start time of oldest non-obsolete window) + time duration of missing windows
         *
         */
        long totalElapsedTimeMs = now - stat.oldest(now).lastWindowMs;

        // Check how many full windows of data we have currently retained
        int numFullWindows = (int) (totalElapsedTimeMs / config.timeWindowMs());
        int minFullWindows = config.samples() - 1; // i.e. prior samples = required samples - current sample

        // If the available windows are less than the minimum required, add the difference to the totalElapsedTime
        if (numFullWindows < minFullWindows)
            totalElapsedTimeMs += (minFullWindows - numFullWindows) * config.timeWindowMs();

        return totalElapsedTimeMs;
    }

    @Override
    public String toString() {
        return "Rate(" +
            "unit=" + unit +
            ", stat=" + stat +
            ')';
    }
}
