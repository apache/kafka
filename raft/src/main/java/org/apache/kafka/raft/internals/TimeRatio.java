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
package org.apache.kafka.raft.internals;

import org.apache.kafka.common.metrics.MeasurableStat;
import org.apache.kafka.common.metrics.MetricConfig;

/**
 * Maintains an approximate ratio of the duration of a specific event
 * over all time. For example, this can be used to compute the ratio of
 * time that a thread is busy or idle. The value is approximate since the
 * measurement and recording intervals may not be aligned.
 *
 * Note that the duration of the event is assumed to be small relative to
 * the interval of measurement.
 *
 */
public class TimeRatio implements MeasurableStat {
    private long intervalStartTimestampMs = -1;
    private long lastRecordedTimestampMs = -1;
    private double totalRecordedDurationMs = 0;

    private final double defaultRatio;

    public TimeRatio(double defaultRatio) {
        if (defaultRatio < 0.0 || defaultRatio > 1.0) {
            throw new IllegalArgumentException("Invalid ratio: value " + defaultRatio + " is not between 0 and 1.");
        }

        this.defaultRatio = defaultRatio;
    }

    @Override
    public double measure(MetricConfig config, long currentTimestampMs) {
        if (lastRecordedTimestampMs < 0) {
            // Return the default value if no recordings have been captured.
            return defaultRatio;
        } else {
            // We measure the ratio over the
            double intervalDurationMs = Math.max(lastRecordedTimestampMs - intervalStartTimestampMs, 0);
            final double ratio;
            if (intervalDurationMs == 0) {
                ratio = defaultRatio;
            } else if (totalRecordedDurationMs > intervalDurationMs) {
                ratio = 1.0;
            } else {
                ratio = totalRecordedDurationMs / intervalDurationMs;
            }

            // The next interval begins at the
            intervalStartTimestampMs = lastRecordedTimestampMs;
            totalRecordedDurationMs = 0;
            return ratio;
        }
    }

    @Override
    public void record(MetricConfig config, double value, long currentTimestampMs) {
        if (intervalStartTimestampMs < 0) {
            // Discard the initial value since the value occurred prior to the interval start
            intervalStartTimestampMs = currentTimestampMs;
        } else {
            totalRecordedDurationMs += value;
            lastRecordedTimestampMs = currentTimestampMs;
        }
    }

}
