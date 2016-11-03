/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.common.utils;

import java.util.concurrent.TimeUnit;

/**
 * A clock that you can manually advance by calling sleep
 */
public class MockTime implements Time {

    private final long autoTickMs;

    // Values from `nanoTime` and `currentTimeMillis` are not comparable, so we store them separately to catch bugs
    // where this is incorrectly assumed to be true
    private long timeMs;
    private long highResTimeNs;

    public MockTime() {
        this(0);
    }

    public MockTime(long autoTickMs) {
        this(autoTickMs, System.currentTimeMillis(), System.nanoTime());
    }

    public MockTime(long autoTickMs, long currentTimeMs, long currentHighResTimeNs) {
        this.timeMs = currentTimeMs;
        this.highResTimeNs = currentHighResTimeNs;
        this.autoTickMs = autoTickMs;
    }

    @Override
    public long milliseconds() {
        maybeSleep(autoTickMs);
        return timeMs;
    }

    @Override
    public long nanoseconds() {
        maybeSleep(autoTickMs);
        return highResTimeNs;
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
        timeMs += ms;
        highResTimeNs += TimeUnit.MILLISECONDS.toNanos(ms);
    }

}
