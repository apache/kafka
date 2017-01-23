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

import java.util.Map;

/**
 * The window specification interface that can be extended for windowing operation in joins and aggregations.
 *
 * @param <W>   type of the window instance
 */
public abstract class Windows<W extends Window> {

    private static final int DEFAULT_NUM_SEGMENTS = 3;

    static final long DEFAULT_MAINTAIN_DURATION_MS = 24 * 60 * 60 * 1000L; // one day

    private long maintainDurationMs;

    public int segments;

    protected Windows() {
        segments = DEFAULT_NUM_SEGMENTS;
        maintainDurationMs = DEFAULT_MAINTAIN_DURATION_MS;
    }

    /**
     * Set the window maintain duration in milliseconds of streams time.
     * This retention time is a guaranteed <i>lower bound</i> for how long a window will be maintained.
     *
     * @return  itself
     */
    // This should always get overridden to provide the correct return type and thus to avoid a cast
    public Windows<W> until(final long durationMs) throws IllegalArgumentException {
        if (durationMs < 0) {
            throw new IllegalArgumentException("Window retention time (durationMs) cannot be negative.");
        }
        maintainDurationMs = durationMs;

        return this;
    }

    /**
     * Specify the number of segments to be used for rolling the window store,
     * this function is not exposed to users but can be called by developers that extend this JoinWindows specs.
     *
     * @return  itself
     */
    protected Windows<W> segments(final int segments) throws IllegalArgumentException {
        if (segments < 2) {
            throw new IllegalArgumentException("Number of segments must be at least 2.");
        }
        this.segments = segments;

        return this;
    }

    /**
     * Return the window maintain duration in milliseconds of streams time.
     *
     * @return the window maintain duration in milliseconds of streams time
     */
    public long maintainMs() {
        return maintainDurationMs;
    }

    /**
     * Creates all windows that contain the provided timestamp, indexed by non-negative window start timestamps.
     *
     * @param timestamp  the timestamp window should get created for
     * @return  a map of {@code windowStartTimestamp -> Window} entries
     */
    public abstract Map<Long, W> windowsFor(final long timestamp);

    public abstract long size();
}
