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
package org.apache.kafka.streams.kstream;

import org.apache.kafka.streams.internals.ApiUtils;
import org.apache.kafka.streams.kstream.internals.UnlimitedWindow;
import org.apache.kafka.streams.processor.TimestampExtractor;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.apache.kafka.streams.internals.ApiUtils.prepareMillisCheckFailMsgPrefix;

/**
 * The unlimited window specifications used for aggregations.
 * <p>
 * An unlimited time window is also called landmark window.
 * It has a fixed starting point while its window end is defined as infinite.
 * With this regard, it is a fixed-size window with infinite window size.
 * <p>
 * For time semantics, see {@link TimestampExtractor}.
 *
 * @see TimeWindows
 * @see SessionWindows
 * @see JoinWindows
 * @see KGroupedStream#windowedBy(Windows)
 * @see TimestampExtractor
 */
public final class UnlimitedWindows extends Windows<UnlimitedWindow> {

    private static final long DEFAULT_START_TIMESTAMP_MS = 0L;

    /** The start timestamp of the window. */
    @SuppressWarnings("WeakerAccess")
    public final long startMs;

    private UnlimitedWindows(final long startMs) {
        this.startMs = startMs;
    }

    /**
     * Return an unlimited window starting at timestamp zero.
     */
    public static UnlimitedWindows of() {
        return new UnlimitedWindows(DEFAULT_START_TIMESTAMP_MS);
    }

    /**
     * Return a new unlimited window for the specified start timestamp.
     *
     * @param start the window start time
     * @return a new unlimited window that starts at {@code start}
     * @throws IllegalArgumentException if the start time is negative or can't be represented as {@code long milliseconds}
     */
    public UnlimitedWindows startOn(final Instant start) throws IllegalArgumentException {
        final String msgPrefix = prepareMillisCheckFailMsgPrefix(start, "start");
        final long startMs = ApiUtils.validateMillisecondInstant(start, msgPrefix);
        if (startMs < 0) {
            throw new IllegalArgumentException("Window start time (startMs) cannot be negative.");
        }
        return new UnlimitedWindows(startMs);
    }

    @Override
    public Map<Long, UnlimitedWindow> windowsFor(final long timestamp) {
        // always return the single unlimited window

        // we cannot use Collections.singleMap since it does not support remove()
        final Map<Long, UnlimitedWindow> windows = new HashMap<>();
        if (timestamp >= startMs) {
            windows.put(startMs, new UnlimitedWindow(startMs));
        }
        return windows;
    }

    /**
     * {@inheritDoc}
     * As unlimited windows have conceptually infinite size, this method just returns {@link Long#MAX_VALUE}.
     *
     * @return the size of the specified windows which is {@link Long#MAX_VALUE}
     */
    @Override
    public long size() {
        return Long.MAX_VALUE;
    }

    @Override
    public long gracePeriodMs() {
        return 0L;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final UnlimitedWindows that = (UnlimitedWindows) o;
        return startMs == that.startMs;
    }

    @Override
    public int hashCode() {
        return Objects.hash(startMs);
    }

    @Override
    public String toString() {
        return "UnlimitedWindows{" +
            "startMs=" + startMs +
            '}';
    }
}
