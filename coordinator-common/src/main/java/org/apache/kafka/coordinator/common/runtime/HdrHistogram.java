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
package org.apache.kafka.coordinator.common.runtime;

import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;
import org.HdrHistogram.ValueRecorder;

/**
 * <p>A wrapper on top of the HdrHistogram API. It handles writing to the histogram by delegating
 * to an internal {@link ValueRecorder} implementation, and reading from the histogram by
 * efficiently implementing the retrieval of up-to-date histogram data.
 *
 * <p>Note that all APIs expect a timestamp which is used by the histogram to discard decaying data
 * and determine when the snapshot from which the histogram metrics are calculated should be
 * refreshed.
 */
public final class HdrHistogram {

    private static final long DEFAULT_MAX_SNAPSHOT_AGE_MS = 1000L;

    private final Object lock = new Object();
    /**
     * The duration (in millis) after which the latest histogram snapshot is considered outdated and
     * subsequent calls to {@link #latestHistogram(long)} will result in the snapshot being recreated.
     */
    private final long maxSnapshotAgeMs;

    /**
     * The internal HdrHistogram data structure used to:
     * <ul>
     *   <li>ingest data;</li>
     *   <li>get a {@link Histogram} of the latest ingested data;</li>
     * </ul>
     */
    private final Recorder recorder;

    /**
     * The latest snapshot of the internal HdrHistogram. Automatically updated by
     * {@link #latestHistogram(long)} if older than {@link #maxSnapshotAgeMs}.
     */
    private volatile Timestamped<Histogram> timestampedHistogramSnapshot;

    public HdrHistogram(
        long highestTrackableValue,
        int numberOfSignificantValueDigits
    ) {
        this(DEFAULT_MAX_SNAPSHOT_AGE_MS, highestTrackableValue, numberOfSignificantValueDigits);
    }

    HdrHistogram(
        long maxSnapshotAgeMs,
        long highestTrackableValue,
        int numberOfSignificantValueDigits
    ) {
        this.maxSnapshotAgeMs = maxSnapshotAgeMs;
        recorder = new Recorder(highestTrackableValue, numberOfSignificantValueDigits);
        this.timestampedHistogramSnapshot = new Timestamped<>(0, null);
    }

    private Histogram latestHistogram(long now) {
        Timestamped<Histogram> latest = timestampedHistogramSnapshot;
        if (now - latest.timestamp > maxSnapshotAgeMs) {
            // Double-checked locking ensures that the thread that extracts the histogram data is
            // the one that updates the internal snapshot.
            synchronized (lock) {
                latest = timestampedHistogramSnapshot;
                if (now - latest.timestamp > maxSnapshotAgeMs) {
                    latest = new Timestamped<>(now, recorder.getIntervalHistogram());
                    timestampedHistogramSnapshot = latest;
                }
            }
        }
        return latest.value;
    }

    /**
     * Writes to the histogram. Will throw {@link ArrayIndexOutOfBoundsException} if the histogram's
     * highestTrackableValue is lower than the value being recorded.
     *
     * @param value The value to be recorded. Cannot be negative.
     */
    public void record(long value) {
        recorder.recordValue(value);
    }

    /**
     * @param now An externally provided timestamp expected to be in milliseconds.
     * @return The total number of updates recorded by the histogram, i.e. the number of times
     * {@link #record(long)} has been called.
     */
    public long count(long now) {
        return latestHistogram(now).getTotalCount();
    }

    /**
     * @param now An externally provided timestamp expected to be in milliseconds.
     * @return The maximum value recorded by the histogram.
     */
    public long max(long now) {
        return latestHistogram(now).getMaxValue();
    }

    /**
     * Reads percentile data from the histogram.
     *
     * @param now        An externally provided timestamp expected to be in milliseconds.
     * @param percentile The percentile for which a value is going to be retrieved. Expected to be
     *                   between 0.0 and 100.0.
     * @return The histogram value for the given percentile.
     */
    public double measurePercentile(long now, double percentile) {
        return latestHistogram(now).getValueAtPercentile(percentile);
    }

    /**
     * A simple tuple of a timestamp and a value. Can be used updating a value and recording the
     * timestamp of the update in a single atomic operation.
     */
    private static final class Timestamped<T> {

        private final long timestamp;
        private final T value;

        private Timestamped(long timestamp, T value) {
            this.timestamp = timestamp;
            this.value = value;
        }
    }

}
