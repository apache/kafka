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
package org.apache.kafka.common.utils;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A clock that you can manually advance by calling sleep
 */
public class MockTime implements Time {

    private final long autoTickMs;

    // Values from `nanoTime` and `currentTimeMillis` are not comparable, so we store them separately to allow tests
    // using this class to detect bugs where this is incorrectly assumed to be true
    private final AtomicLong timeMs;
    private final AtomicLong highResTimeNs;

    public MockTime() {
        this(0);
    }

    public MockTime(long autoTickMs) {
        this(autoTickMs, System.currentTimeMillis(), System.nanoTime());
    }

    public MockTime(long autoTickMs, long currentTimeMs, long currentHighResTimeNs) {
        this.timeMs = new AtomicLong(currentTimeMs);
        this.highResTimeNs = new AtomicLong(currentHighResTimeNs);
        this.autoTickMs = autoTickMs;
    }

    @Override
    public long milliseconds() {
        maybeSleep(autoTickMs);
        return timeMs.get();
    }

    @Override
    public long nanoseconds() {
        maybeSleep(autoTickMs);
        return highResTimeNs.get();
    }

    @Override
    public long hiResClockMs() {
        return TimeUnit.NANOSECONDS.toMillis(nanoseconds());
    }

    private void maybeSleep(long ms) {
        if (ms != 0)
            sleep(ms);
    }

    @Override
    public void sleep(long ms) {
        timeMs.addAndGet(ms);
        highResTimeNs.addAndGet(TimeUnit.MILLISECONDS.toNanos(ms));
    }

}
