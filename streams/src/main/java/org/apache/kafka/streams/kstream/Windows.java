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
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The window specification interface that can be extended for windowing operation in joins and aggregations.
 *
 * @param <W>   type of the window instance
 */
public abstract class Windows<W extends Window> {

    private static final int DEFAULT_NUM_SEGMENTS = 3;

    private static final long DEFAULT_MAINTAIN_DURATION = 24 * 60 * 60 * 1000L;   // one day

    private static final AtomicInteger NAME_INDEX = new AtomicInteger(0);

    protected String name;

    private long maintainDurationMs;

    public int segments;

    protected Windows(String name) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name must not be null or empty");
        }
        this.name = name;
        this.segments = DEFAULT_NUM_SEGMENTS;
        this.maintainDurationMs = DEFAULT_MAINTAIN_DURATION;
    }

    public String name() {
        return name;
    }

    /**
     * Set the window maintain duration in milliseconds of system time.
     *
     * @return  itself
     */
    public Windows until(long durationMs) {
        this.maintainDurationMs = durationMs;

        return this;
    }

    /**
     * Specify the number of segments to be used for rolling the window store,
     * this function is not exposed to users but can be called by developers that extend this JoinWindows specs.
     *
     * @return  itself
     */
    protected Windows segments(int segments) {
        this.segments = segments;

        return this;
    }

    /**
     * Return the window maintain duration in milliseconds of system time.
     *
     * @return the window maintain duration in milliseconds of system time
     */
    public long maintainMs() {
        return this.maintainDurationMs;
    }

    /**
     * Creates all windows that contain the provided timestamp.
     *
     * @param timestamp  the timestamp window should get created for
     * @return  a map of {@code windowStartTimestamp -> Window} entries
     */
    public abstract Map<Long, W> windowsFor(long timestamp);

}
