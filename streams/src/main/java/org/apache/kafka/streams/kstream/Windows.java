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

    private static final long DEFAULT_EMIT_DURATION = 1000L;

    private static final long DEFAULT_MAINTAIN_DURATION = 24 * 60 * 60 * 1000L;   // one day

    private static final AtomicInteger NAME_INDEX = new AtomicInteger(0);

    protected String name;

    private long emitDurationMs;

    private long maintainDurationMs;

    public int segments;

    protected Windows(String name) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name must not be null or empty");
        }
        this.name = name;
        this.segments = DEFAULT_NUM_SEGMENTS;
        this.emitDurationMs = DEFAULT_EMIT_DURATION;
        this.maintainDurationMs = DEFAULT_MAINTAIN_DURATION;
    }

    public String name() {
        return name;
    }

    /**
     * Set the window emit duration in milliseconds of system time.
     */
    public Windows emit(long durationMs) {
        this.emitDurationMs = durationMs;

        return this;
    }

    /**
     * Set the window maintain duration in milliseconds of system time.
     */
    public Windows until(long durationMs) {
        this.maintainDurationMs = durationMs;

        return this;
    }

    /**
     * Specify the number of segments to be used for rolling the window store,
     * this function is not exposed to users but can be called by developers that extend this JoinWindows specs.
     */
    protected Windows segments(int segments) {
        this.segments = segments;

        return this;
    }

    public long emitEveryMs() {
        return this.emitDurationMs;
    }

    public long maintainMs() {
        return this.maintainDurationMs;
    }

    protected String newName(String prefix) {
        return prefix + String.format("%010d", NAME_INDEX.getAndIncrement());
    }

    public abstract Map<Long, W> windowsFor(long timestamp);

}
