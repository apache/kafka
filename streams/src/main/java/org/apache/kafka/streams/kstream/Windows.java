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

import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class Windows<W extends Window> {

    private static final long DEFAULT_EMIT_DURATION = 1000L;

    private static final long DEFAULT_MAINTAIN_DURATION = 24 * 60 * 60 * 1000L;   // one day

    private static final AtomicInteger NAME_INDEX = new AtomicInteger(0);

    private long emitDuration;

    private long maintainDuration;

    private String name;

    protected Windows(String name) {
        this.emitDuration = DEFAULT_EMIT_DURATION;
        this.maintainDuration = DEFAULT_MAINTAIN_DURATION;
    }

    public String name() {
        return name;
    }

    /**
     * Set the window emit duration in milliseconds of system time
     */
    public Windows emit(long duration) {
        this.emitDuration = duration;

        return this;
    }

    /**
     * Set the window maintain duration in milliseconds of system time
     */
    public Windows until(long duration) {
        this.maintainDuration = duration;

        return this;
    }

    public long emitEveryMs() {
        return this.emitDuration;
    }

    public long maintainMs() {
        return this.maintainDuration;
    }

    protected String newName(String prefix) {
        return prefix + String.format("%010d", NAME_INDEX.getAndIncrement());
    }

    abstract boolean equalTo(Windows other);

    abstract Collection<Window> windowsFor(long timestamp);
}
