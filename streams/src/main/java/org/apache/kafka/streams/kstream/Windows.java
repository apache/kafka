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

import java.util.Map;

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
 */
public abstract class Windows<W extends Window> {

    /**
     * By default grace period is 24 hours for all windows in other words we allow out-of-order data for up to a day
     * This behavior is now deprecated and additional details are available in the motivation for the KIP
     * Check out <a href="https://cwiki.apache.org/confluence/x/Ho2NCg">KIP-633</a> for more details
     */
    protected static final long DEPRECATED_DEFAULT_24_HR_GRACE_PERIOD = 24 * 60 * 60 * 1000L;

    /**
     * This constant is used as the specified grace period where we do not have any grace periods instead of magic constants
     */
    protected static final long NO_GRACE_PERIOD = 0L;

    protected Windows() {}

    /**
     * Create all windows that contain the provided timestamp, indexed by non-negative window start timestamps.
     *
     * @param timestamp the timestamp window should get created for
     * @return a map of {@code windowStartTimestamp -> Window} entries
     */
    public abstract Map<Long, W> windowsFor(final long timestamp);

    /**
     * Return the size of the specified windows in milliseconds.
     *
     * @return the size of the specified windows
     */
    public abstract long size();

    /**
     * Return the window grace period (the time to admit
     * out-of-order events after the end of the window.)
     *
     * Delay is defined as (stream_time - record_timestamp).
     */
    public abstract long gracePeriodMs();
}
