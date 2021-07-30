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
package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.streams.kstream.Window;

/**
 * A {@link TimeWindow} is a time interval window container that holds the start and end time for use in window-agnostic cases,
 * ex: in {@link org.apache.kafka.streams.state.WindowStore}, we'll store the aggregated values of any fixed-size types of time windows.
 * We use {@link TimeWindow} to represent these time windows
 *
 * @see SessionWindow
 * @see UnlimitedWindow
 */
public class TimeWindow extends Window {

    /**
     * Create a new window for the given start time and end time.
     *
     * @param startMs the start timestamp of the window
     * @param endMs   the end timestamp of the window
     * @throws IllegalArgumentException if {@code startMs} is negative or if {@code endMs} is smaller than {@code startMs}
     */
    public TimeWindow(final long startMs, final long endMs) throws IllegalArgumentException {
        super(startMs, endMs);
    }

    /**
     * Check if the given window overlaps with this window.
     *
     * @param other another window
     * @return {@code true} if {@code other} overlaps with this window&mdash;{@code false} otherwise
     * @throws IllegalArgumentException if the {@code other} window has a different type than {@code this} window
     */
    @Override
    public boolean overlap(final Window other) throws IllegalArgumentException {
        if (getClass() != other.getClass()) {
            throw new IllegalArgumentException("Cannot compare windows of different type. Other window has type "
                + other.getClass() + ".");
        }
        final TimeWindow otherWindow = (TimeWindow) other;
        return startMs < otherWindow.endMs && otherWindow.startMs < endMs;
    }

}
