/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import java.util.HashMap;
import java.util.Map;

/**
 * The unlimited window specifications.
 */
public class UnlimitedWindows extends Windows<UnlimitedWindow> {

    private static final long DEFAULT_START_TIMESTAMP_MS = 0L;

    /** The start timestamp of the window. */
    public final long startMs;

    private UnlimitedWindows(final long startMs) throws IllegalArgumentException {
        if (startMs < 0) {
            throw new IllegalArgumentException("startMs must be > 0 (you provided " + startMs + ")");
        }
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
     * @param start  the window start time
     * @return       a new unlimited window that starts at {@code start}
     */
    public UnlimitedWindows startOn(final long start) throws IllegalArgumentException {
        return new UnlimitedWindows(start);
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

    @Override
    public long size() {
        return Long.MAX_VALUE;
    }

    @Override
    public final boolean equals(final Object o) {
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

    @Override
    public UnlimitedWindows until(final long durationMs) {
        throw new IllegalArgumentException("Window retention time (durationMs) cannot be set for UnlimitedWindows.");
    }

    @Override
    public long maintainMs() {
        return Long.MAX_VALUE;
    }
}