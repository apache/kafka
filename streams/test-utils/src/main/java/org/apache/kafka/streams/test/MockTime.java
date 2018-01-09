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
package org.apache.kafka.streams.test;

import org.apache.kafka.common.utils.Time;

import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A clock that you can manually advance by calling {@code sleep()}.
 */
public class MockTime implements Time {

    /**
     * Listener that is invoked each time <em>after</em> time was advanced or modified.
     */
    public interface MockTimeListener {
        void tick(final long currentTimeMs);
    }

    private final CopyOnWriteArrayList<MockTimeListener> listeners = new CopyOnWriteArrayList<>();

    private long autoTickMs;

    // Values from `nanoTime` and `currentTimeMillis` are not comparable, so we store them separately to allow tests
    // using this class to detect bugs where this is incorrectly assumed to be true
    private final AtomicLong timeMs;
    private final AtomicLong highResTimeNs;

    public MockTime() {
        this(0);
    }

    public MockTime(final long autoTickMs) {
        this(autoTickMs, System.currentTimeMillis(), System.nanoTime());
    }

    public MockTime(final long autoTickMs,
                    final long currentTimeMs,
                    final long currentHighResTimeNs) {
        if (autoTickMs < 0) {
            throw new IllegalArgumentException("autoTickMs cannot be negative.");
        }
        this.timeMs = new AtomicLong(currentTimeMs);
        this.highResTimeNs = new AtomicLong(currentHighResTimeNs);
        this.autoTickMs = autoTickMs;
    }

    public void addListener(final MockTimeListener listener) {
        Objects.requireNonNull(listener);
        listeners.add(listener);
    }

    @Override
    public long milliseconds() {
        final long currentTime = timeMs.get();
        maybeSleep(autoTickMs);
        return currentTime;
    }

    @Override
    public long nanoseconds() {
        final long currentTime = highResTimeNs.get();
        maybeSleep(autoTickMs);
        return currentTime;
    }

    @Override
    public long hiResClockMs() {
        return TimeUnit.NANOSECONDS.toMillis(nanoseconds());
    }

    private void maybeSleep(final long ms) {
        if (ms != 0)
            sleep(ms);
    }

    @Override
    public void sleep(final long ms) {
        if (ms < 0) {
            throw new IllegalArgumentException("Sleep ms cannot be negative.");
        }
        timeMs.addAndGet(ms);
        highResTimeNs.addAndGet(TimeUnit.MILLISECONDS.toNanos(ms));
        tick();
    }

    public void setAutoTickMs(final long autoTickMs) {
        if (autoTickMs < 0) {
            throw new IllegalArgumentException("autoTickMs cannot be negative.");
        }
        this.autoTickMs = autoTickMs;
    }

    public void setCurrentTimeMs(final long newMs) {
        final long oldMs = timeMs.getAndSet(newMs);

        // does not allow to set to an older timestamp
        if (oldMs > newMs) {
            timeMs.getAndSet(oldMs); // reset to old
            throw new IllegalArgumentException("Setting the time to " + newMs + " while current time " + oldMs + " is newer; this is not allowed");
        }

        highResTimeNs.set(TimeUnit.MILLISECONDS.toNanos(newMs));
        tick();
    }

    private void tick() {
        for (final MockTimeListener listener : listeners) {
            listener.tick(timeMs.get());
        }
    }
}
