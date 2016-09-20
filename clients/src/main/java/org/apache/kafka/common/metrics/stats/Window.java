/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.common.metrics.stats;

import org.apache.kafka.common.metrics.MetricConfig;

/**
 * Class that defines how windows, used for Rate calculation, should be derived.
 */

public class Window {
    public static final Policy FIXED = new FixedWindowPolicy();
    public static final Policy ELAPSED = new ElapsedWindowPolicy();

    interface Policy {
        long windowSize(long first, long last, MetricConfig config);
    }

    /**
     * This policy pads the window length out to fill the full N-1 sub
     * windows. Thus it is conservative. It will default to under-
     * estimating the rate.
     *
     * Imagine we have 11 sub-windows. We might get measurements in
     * windows 3, 5, 7. This policy will take will fix the window to
     * be the duration of 10 full sub-windows.
     *
     * This is in contrast to the Elapsed policy, which would use the
     * difference between measurement time in sub-windows 3 & 7.
     *
     * If the frequency of measurement is smaller than the sub-window
     * size, as is often the case, this policy will simulate a slow-
     * start over N-1 windows. It will stabilise after N-1 windows
     * have passed
     *
     * This policy is well suited to use cases where measurements
     * which are erratic in their frequency should be underestimated,
     * and a slow start can be tolerated.
     */
    private static class FixedWindowPolicy implements Policy {
        @Override
        public long windowSize(long first, long last, MetricConfig config) {
            long elapsed = last - first;
            // Check how many full windows of data we have currently retained
            int numFullWindows = (int) (elapsed / config.timeWindowMs());
            int minFullWindows = config.samples() - 1;

            // If the available windows are less than the minimum required, add the difference to the totalElapsedTime
            if (numFullWindows < minFullWindows)
                elapsed += (minFullWindows - numFullWindows) * config.timeWindowMs();

            return elapsed;
        }
    }

    /**
     * This policy is essentially just the difference between first and last
     * measurements. So when starting, say we have only two measurements, A & B,
     * the elapsed window would be B.time - A.time. This creates a higher rate
     * initially, which then drops gradually as more samples are collected.
     *
     * If the window is 0, meaning only one rate has been collected, the full-
     * window duration is returned. This is equivalent to a "slow start" for
     * the first measurement.
     *
     * This policy is suited to use cases where immediate accuracy is preferred,
     * regardless of whether you are starting or have infrequent measurements.
     */
    private static class ElapsedWindowPolicy implements Policy {
        @Override
        public long windowSize(long first, long last, MetricConfig config) {
            long elapsed = last - first;
            return elapsed == 0 ? config.samples() * config.timeWindowMs() : elapsed;
        }
    }
}