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

import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;

import java.time.Duration;

import static org.apache.kafka.streams.kstream.internals.WindowingDefaults.DEFAULT_RETENTION_MS;

/**
 * The window specification for fixed size windows that is used to define window boundaries and grace period.
 * <p>
 * Grace period defines how long to wait on out-of-order events. That is, windows will continue to accept new records until {@code stream_time >= window_end + grace_period}.
 * Records that arrive after the grace period passed are considered <em>late</em> and will not be processed but are dropped.
 * <p>
 * Warning: It may be unsafe to use objects of this class in set- or map-like collections,
 * since the equals and hashCode methods depend on mutable fields.
 *
 * @param <W> type of the window instance
 * @see TimeWindows
 * @see UnlimitedWindows
 * @see JoinWindows
 * @see SessionWindows
 * @see TimestampExtractor
 * @deprecated since 2.7 Implement EnumerableWindowDefinition instead.
 */
@Deprecated
public abstract class Windows<W extends Window> implements EnumerableWindowDefinition {

    private long maintainDurationMs = DEFAULT_RETENTION_MS;
    @Deprecated public int segments = 3;

    protected Windows() {}

    @Deprecated // remove this constructor when we remove segments.
    Windows(final int segments) {
        this.segments = segments;
    }

    /**
     * Set the window maintain duration (retention time) in milliseconds.
     * This retention time is a guaranteed <i>lower bound</i> for how long a window will be maintained.
     *
     * @param durationMs the window retention time in milliseconds
     * @return itself
     * @throws IllegalArgumentException if {@code durationMs} is negative
     * @deprecated since 2.1. Use {@link Materialized#withRetention(Duration)}
     *             or directly configure the retention in a store supplier and use {@link Materialized#as(WindowBytesStoreSupplier)}.
     */
    @Deprecated
    public Windows<W> until(final long durationMs) throws IllegalArgumentException {
        if (durationMs < 0) {
            throw new IllegalArgumentException("Window retention time (durationMs) cannot be negative.");
        }
        maintainDurationMs = durationMs;

        return this;
    }

    /**
     * Return the window maintain duration (retention time) in milliseconds.
     *
     * @return the window maintain duration
     * @deprecated since 2.1. Use {@link Materialized#retention} instead.
     */
    @Deprecated
    public long maintainMs() {
        return maintainDurationMs;
    }

    /**
     * Set the number of segments to be used for rolling the window store.
     * This function is not exposed to users but can be called by developers that extend this class.
     *
     * @param segments the number of segments to be used
     * @return itself
     * @throws IllegalArgumentException if specified segments is small than 2
     * @deprecated since 2.1 Override segmentInterval() instead.
     */
    @Deprecated
    protected Windows<W> segments(final int segments) throws IllegalArgumentException {
        if (segments < 2) {
            throw new IllegalArgumentException("Number of segments must be at least 2.");
        }
        this.segments = segments;

        return this;
    }

    /**
     * Return an upper bound on the size of the specified windows in milliseconds.
     * Used to determine the lower bound on store retention time.
     *
     * @return the size of the specified window
     */
    public long maxSize() {
        // Adding an implementation to deleagate to the existing
        // equivalent API so that existing subclasses will still compile.
        return size();
    }

    /**
     * Return the size of the specified windows in milliseconds.
     *
     * @return the size of the specified windows
     * @deprecated since 2.7 Implement EnumerableWindowDefinition instead.
     */
    @Deprecated
    public abstract long size();
}
