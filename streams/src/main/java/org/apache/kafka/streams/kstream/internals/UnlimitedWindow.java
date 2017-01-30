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
package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.streams.kstream.Window;

/**
 * {@link UnlimitedWindow} is an "infinite" large window with a fixed (inclusive) start time.
 * All windows of the same {@link org.apache.kafka.streams.kstream.UnlimitedWindows window specification} will have the
 * same start time.
 * To make the window size "infinite" end time is set to {@link Long#MAX_VALUE}.
 * <p>
 * For time semantics, see {@link org.apache.kafka.streams.processor.TimestampExtractor TimestampExtractor}.
 *
 * @see TimeWindow
 * @see SessionWindow
 * @see org.apache.kafka.streams.kstream.UnlimitedWindows
 * @see org.apache.kafka.streams.processor.TimestampExtractor
 */
@InterfaceStability.Unstable
public class UnlimitedWindow extends Window {

    /**
     * Create a new window for the given start time (inclusive).
     *
     * @param startMs the start timestamp of the window (inclusive)
     * @throws IllegalArgumentException if {@code start} is negative
     */
    public UnlimitedWindow(final long startMs) {
        super(startMs, Long.MAX_VALUE);
    }

    /**
     * Returns {@code true} if the given window is of the same type, because all unlimited windows overlap with each
     * other due to their infinite size.
     *
     * @param other another window
     * @return {@code true}
     * @throws IllegalArgumentException if the {@code other} window has a different type than {@link this} window
     */
    @Override
    public boolean overlap(final Window other) {
        if (getClass() != other.getClass()) {
            throw new IllegalArgumentException("Cannot compare windows of different type. Other window has type "
                + other.getClass() + ".");
        }
        return true;
    }

}
