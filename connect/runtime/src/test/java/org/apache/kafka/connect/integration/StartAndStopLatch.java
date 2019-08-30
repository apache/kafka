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

package org.apache.kafka.connect.integration;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.kafka.common.utils.Time;

/**
 * A latch that can be used to count down the number of times a connector and/or tasks have
 * been started and stopped.
 */
public class StartAndStopLatch {
    private final CountDownLatch startLatch;
    private final CountDownLatch stopLatch;
    private final List<StartAndStopLatch> dependents;
    private final Consumer<StartAndStopLatch> uponCompletion;
    private final Time clock;

    StartAndStopLatch(int expectedStarts, int expectedStops, Consumer<StartAndStopLatch> uponCompletion,
                 List<StartAndStopLatch> dependents, Time clock) {
        this.startLatch = new CountDownLatch(expectedStarts < 0 ? 0 : expectedStarts);
        this.stopLatch = new CountDownLatch(expectedStops < 0 ? 0 : expectedStops);
        this.dependents = dependents;
        this.uponCompletion = uponCompletion;
        this.clock = clock;
    }

    protected void recordStart() {
        startLatch.countDown();
    }

    protected void recordStop() {
        stopLatch.countDown();
    }

    /**
     * Causes the current thread to wait until the latch has counted down the starts and
     * stops to zero, unless the thread is {@linkplain Thread#interrupt interrupted},
     * or the specified waiting time elapses.
     *
     * <p>If the current counts are zero then this method returns immediately
     * with the value {@code true}.
     *
     * <p>If the current count is greater than zero then the current
     * thread becomes disabled for thread scheduling purposes and lies
     * dormant until one of three things happen:
     * <ul>
     * <li>The counts reach zero due to invocations of the {@link #recordStart()} and
     * {@link #recordStop()} methods; or
     * <li>Some other thread {@linkplain Thread#interrupt interrupts}
     * the current thread; or
     * <li>The specified waiting time elapses.
     * </ul>
     *
     * <p>If the count reaches zero then the method returns with the
     * value {@code true}.
     *
     * <p>If the current thread:
     * <ul>
     * <li>has its interrupted status set on entry to this method; or
     * <li>is {@linkplain Thread#interrupt interrupted} while waiting,
     * </ul>
     * then {@link InterruptedException} is thrown and the current thread's
     * interrupted status is cleared.
     *
     * <p>If the specified waiting time elapses then the value {@code false}
     * is returned.  If the time is less than or equal to zero, the method
     * will not wait at all.
     *
     * @param timeout the maximum time to wait
     * @param unit    the time unit of the {@code timeout} argument
     * @return {@code true} if the counts reached zero and {@code false}
     *         if the waiting time elapsed before the counts reached zero
     * @throws InterruptedException if the current thread is interrupted
     *         while waiting
     */
    public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
        final long start = clock.milliseconds();
        final long end = start + unit.toMillis(timeout);
        if (!startLatch.await(end - start, TimeUnit.MILLISECONDS)) {
            return false;
        }
        if (!stopLatch.await(end - clock.milliseconds(), TimeUnit.MILLISECONDS)) {
            return false;
        }

        if (dependents != null) {
            for (StartAndStopLatch dependent : dependents) {
                if (!dependent.await(end - clock.milliseconds(), TimeUnit.MILLISECONDS)) {
                    return false;
                }
            }
        }
        if (uponCompletion != null) {
            uponCompletion.accept(this);
        }
        return true;
    }
}
