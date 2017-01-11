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

    protected final long start;
    protected final long end;

    /**
     * Create a new window for the given start time (inclusive) and end time (exclusive).
     *
     * @param start  the start timestamp of the window (inclusive)
     * @param end    the end timestamp of the window (exclusive)
     * @throws IllegalArgumentException if {@code start} or {@code end} is negative or if {@code end} is smaller than
     * {@code start}
     */
    public Window(long start, long end) throws IllegalArgumentException {
        if (start < 0) {
            throw new IllegalArgumentException("Window start time cannot be negative.");
        }
        if (end < 0) {
            throw new IllegalArgumentException("Window end time cannot be negative.");
        }
        if (end < start) {
            throw new IllegalArgumentException("Window end time cannot be smaller than window start time.");
        }
        this.start = start;
        this.end = end;
    }

    /**
     * Return the start timestamp of this window, inclusive
     */
    public long start() {
        return start;
    }

    /**
     * Return the end timestamp of this window, exclusive
     */
    public long end() {
        return end;
    }

    /**
     * Check if the given window overlaps with this window.
     *
     * @param other  another window
     * @return       {@code true} if {@code other} overlaps with this window&mdash;{@code false} otherwise
     */
    public abstract boolean overlap(Window other);

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }

        if (getClass() != obj.getClass()) {
            return false;
        }

        Window other = (Window) obj;
        return this.start == other.start && this.end == other.end;
    }

    @Override
    public int hashCode() {
        long n = (this.start << 32) | this.end;
        return (int) (n % 0xFFFFFFFFL);
    }

    @Override
    public String toString() {
        return "Window{" +
                "start=" + start +
                ", end=" + end +
                '}';
    }
}
