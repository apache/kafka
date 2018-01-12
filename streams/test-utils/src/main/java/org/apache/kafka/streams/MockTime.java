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
package org.apache.kafka.streams;

import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.utils.Time;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This is an internal class and should not be used.
 */
@InterfaceStability.Unstable
public class MockTime implements Time {
    private final AtomicLong timeMs;
    private final AtomicLong highResTimeNs;

    public MockTime(final long startTimestampMs) {
        this.timeMs = new AtomicLong(startTimestampMs);
        this.highResTimeNs = new AtomicLong(startTimestampMs * 1000L * 1000L);
    }

    @Override
    public long milliseconds() {
        return timeMs.get();
    }

    @Override
    public long nanoseconds() {
        return highResTimeNs.get();
    }

    @Override
    public long hiResClockMs() {
        return TimeUnit.NANOSECONDS.toMillis(nanoseconds());
    }

    @Override
    public void sleep(final long ms) {
        if (ms < 0) {
            throw new IllegalArgumentException("Sleep ms cannot be negative.");
        }
        timeMs.addAndGet(ms);
        highResTimeNs.addAndGet(TimeUnit.MILLISECONDS.toNanos(ms));
    }
}
