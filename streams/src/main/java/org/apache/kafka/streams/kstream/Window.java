/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.kstream;

import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.streams.processor.TimestampExtractor;

/**
 * A single window instance, defined by its start and end timestamp.
 * {@code Window} is agnostic if start/end boundaries are inclusive or exclusive; this is defined by concrete
 * window implementations.
 * <p>
 * To specify how {@code Window} boundaries are defined use {@link Windows}.
 * For time semantics, see {@link TimestampExtractor}.
 *
 * @see Windows
 * @see org.apache.kafka.streams.kstream.internals.TimeWindow
 * @see org.apache.kafka.streams.kstream.internals.SessionWindow
 * @see org.apache.kafka.streams.kstream.internals.UnlimitedWindow
 * @see TimestampExtractor
 */
@InterfaceStability.Unstable
public abstract class Window {

    protected final long startMs;
    protected final long endMs;

    /**
     * Create a new window for the given start and end time.
     *
     * @param startMs the start timestamp of the window
     * @param endMs   the end timestamp of the window
     * @throws IllegalArgumentException if {@code startMs} is negative or if {@code endMs} is smaller than {@code startMs}
     */
    public Window(final long startMs, final long endMs) throws IllegalArgumentException {
        if (startMs < 0) {
            throw new IllegalArgumentException("Window startMs time cannot be negative.");
        }
        if (endMs < startMs) {
            throw new IllegalArgumentException("Window endMs time cannot be smaller than window startMs time.");
        }
        this.startMs = startMs;
        this.endMs = endMs;
    }

    /**
     * Return the start timestamp of this window.
     */
    public long start() {
        return startMs;
    }

    /**
     * Return the end timestamp of this window.
     */
    public long end() {
        return endMs;
    }

    /**
     * Check if the given window overlaps with this window.
     * Should throw an {@link IllegalArgumentException} if the {@code other} window has a different type than {@code
     * this} window.
     *
     * @param other another window of the same type
     * @return {@code true} if {@code other} overlaps with this window&mdash;{@code false} otherwise
     */
    public abstract boolean overlap(final Window other);

    @Override
    public boolean equals(final Object obj) {
        if (obj == this) {
            return true;
        }

        if (getClass() != obj.getClass()) {
            return false;
        }

        final Window other = (Window) obj;
        return startMs == other.startMs && endMs == other.endMs;
    }

    @Override
    public int hashCode() {
        return (int) (((startMs << 32) | endMs) % 0xFFFFFFFFL);
    }

    @Override
    public String toString() {
        return "Window{" +
            "start=" + startMs +
            ", end=" + endMs +
            '}';
    }
}
