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

/**
 * This metrics agnostic implementation maintain an approximate ratio of
 * the duration of a specific event over all time. For example, this can
 * be used to compute the ratio of time that a thread is busy or idle. The value
 * is approximate since the measurement and recording intervals may not be aligned.
 *
 * Note that the duration of the event is assumed to be small relative to
 * the interval of measurement.
 */

public class TimeRatio {
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

    /**
     * Measure the ratio of the total recorded duration over the interval duration.
     * If no recordings have been captured, it returns the default ratio.
     * After measuring, it resets the recorded duration and starts a new interval.
     *
     * @return The ratio of total recorded duration to the interval duration
     */
    public double measure() {
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

    /**
     * Record the duration of an event at the current timestamp.
     * If this is the first record, it initializes the interval start timestamp.
     * Otherwise, it updates the total recorded duration and last recorded timestamp.
     *
     * @param value              The duration of the event in milliseconds
     * @param currentTimestampMs The current time in milliseconds
     */
    public void record(double value, long currentTimestampMs) {
        if (intervalStartTimestampMs < 0) {
            // Discard the initial value since the value occurred prior to the interval start
            intervalStartTimestampMs = currentTimestampMs;
        } else {
            totalRecordedDurationMs += value;
            lastRecordedTimestampMs = currentTimestampMs;
        }
    }
}