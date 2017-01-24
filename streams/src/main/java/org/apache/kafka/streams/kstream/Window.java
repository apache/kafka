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

/**
 * A single window instance, defined by its start and end timestamp.
 */
public abstract class Window {

    protected final long startMs;
    protected final long endMs;

    /**
     * Create a new window for the given start time (inclusive) and end time (exclusive).
     *
     * @param start  the start timestamp of the window (inclusive)
     * @param end    the end timestamp of the window (exclusive)
     * @throws IllegalArgumentException if {@code start} or {@code end} is negative or if {@code end} is smaller than
     * {@code start}
     */
    public Window(long startMs, long endMs) throws IllegalArgumentException {
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
     * Return the start timestamp of this window, inclusive
     */
    public long start() {
        return startMs;
    }

    /**
     * Return the end timestamp of this window, exclusive
     */
    public long end() {
        return endMs;
    }

    /**
     * Check if the given window overlaps with this window.
     *
     * @param other  another window
     * @return       {@code true} if {@code other} overlaps with this window&mdash;{@code false} otherwise
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
