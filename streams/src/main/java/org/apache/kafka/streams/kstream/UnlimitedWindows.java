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

import org.apache.kafka.streams.kstream.internals.UnlimitedWindow;
import org.apache.kafka.streams.processor.TimestampExtractor;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

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
     * @param startMs the window start time
     * @return a new unlimited window that starts at {@code startMs}
     * @throws IllegalArgumentException if the start time is negative
     */
    public UnlimitedWindows startOn(final long startMs) throws IllegalArgumentException {
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
     * As unlimited windows have conceptually infinite size, this methods just returns {@link Long#MAX_VALUE}.
     *
     * @return the size of the specified windows which is {@link Long#MAX_VALUE}
     */
    @Override
    public long size() {
        return Long.MAX_VALUE;
    }

    /**
     * Throws an {@link IllegalArgumentException} because the retention time for unlimited windows is always infinite
     * and cannot be changed.
     *
     * @throws IllegalArgumentException on every invocation.
     * @deprecated since 2.1.
     */
    @Override
    @Deprecated
    public UnlimitedWindows until(final long durationMs) {
        throw new IllegalArgumentException("Window retention time (durationMs) cannot be set for UnlimitedWindows.");
    }

    /**
     * {@inheritDoc}
     * The retention time for unlimited windows in infinite and thus represented as {@link Long#MAX_VALUE}.
     *
     * @return the window retention time that is {@link Long#MAX_VALUE}
     * @deprecated since 2.1. Use {@link Materialized#retention} instead.
     */
    @Override
    @Deprecated
    public long maintainMs() {
        return Long.MAX_VALUE;
    }

    /**
     * Throws an {@link IllegalArgumentException} because the window never ends and the
     * grace period is therefore meaningless.
     *
     * @throws IllegalArgumentException on every invocation
     */
    @Override
    public UnlimitedWindows grace(final Duration afterWindowEnd) {
        throw new IllegalArgumentException("Grace period cannot be set for UnlimitedWindows.");
    }

    @Override
    public boolean equals(final Object o) {
        if (o == this) {
            return true;
        }

        if (!(o instanceof UnlimitedWindows)) {
            return false;
        }

        final UnlimitedWindows other = (UnlimitedWindows) o;
        return startMs == other.startMs;
    }

    @Override
    public int hashCode() {
        return (int) (startMs ^ (startMs >>> 32));
    }

}
