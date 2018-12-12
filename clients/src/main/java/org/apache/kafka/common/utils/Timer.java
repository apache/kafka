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

/**
 * This is a helper class which makes blocking methods with a timeout easier to implement.
 * In particular it enables use cases where a high-level blocking call with a timeout is
 * composed of several lower level calls, each of which has their own respective timeouts. The idea
 * is to create a single timer object for the high level timeout and carry it along to
 * all of the lower level methods. This class also handles common problems such as integer overflow.
 * This class also ensures monotonic updates to the timer even if the underlying clock is subject
 * to non-monotonic behavior. For example, the remaining time returned by {@link #remainingMs()} is
 * guaranteed to decrease monotonically until it hits zero.
 *
 * Note that it is up to the caller to ensure progress of the timer using one of the
 * {@link #update()} methods or {@link #sleep(long)}. The timer will cache the current time and
 * return it indefinitely until the timer has been updated. This allows the caller to limit
 * unnecessary system calls and update the timer only when needed. For example, a timer which is
 * waiting a request sent through the {@link org.apache.kafka.clients.NetworkClient} should call
 * {@link #update()} following each blocking call to
 * {@link org.apache.kafka.clients.NetworkClient#poll(long, long)}.
 *
 * A typical usage might look something like this:
 *
 * <pre>
 *     Time time = Time.SYSTEM;
 *     Timer timer = time.timer(500);
 *
 *     while (!conditionSatisfied() && timer.notExpired) {
 *         client.poll(timer.remainingMs(), timer.currentTimeMs());
 *         timer.update();
 *     }
 * </pre>
 */
public class Timer {
    private final Time time;
    private long startMs;
    private long currentTimeMs;
    private long deadlineMs;

    Timer(Time time, long timeoutMs) {
        this.time = time;
        update();
        reset(timeoutMs);
    }

    /**
     * Check timer expiration. Like {@link #remainingMs()}, this depends on the current cached
     * time in milliseconds, which is only updated through one of the {@link #update()} methods
     * or with {@link #sleep(long)};
     *
     * @return true if the timer has expired, false otherwise
     */
    public boolean isExpired() {
        return currentTimeMs >= deadlineMs;
    }

    /**
     * Check whether the timer has not yet expired.
     * @return true if there is still time remaining before expiration
     */
    public boolean notExpired() {
        return !isExpired();
    }

    /**
     * Reset the timer to the specific timeout. This will use the underlying {@link #Timer(Time, long)}
     * implementation to update the current cached time in milliseconds and it will set a new timer
     * deadline.
     *
     * @param timeoutMs The new timeout in milliseconds
     */
    public void updateAndReset(long timeoutMs) {
        update();
        reset(timeoutMs);
    }

    /**
     * Reset the timer using a new timeout. Note that this does not update the cached current time
     * in milliseconds, so it typically must be accompanied with a separate call to {@link #update()}.
     * Typically, you can just use {@link #updateAndReset(long)}.
     *
     * @param timeoutMs The new timeout in milliseconds
     */
    public void reset(long timeoutMs) {
        if (timeoutMs < 0)
            throw new IllegalArgumentException("Invalid negative timeout " + timeoutMs);

        this.startMs = this.currentTimeMs;

        if (currentTimeMs > Long.MAX_VALUE - timeoutMs)
            this.deadlineMs = Long.MAX_VALUE;
        else
            this.deadlineMs = currentTimeMs + timeoutMs;
    }

    /**
     * Use the underlying {@link Time} implementation to update the current cached time. If
     * the underlying time returns a value which is smaller than the current cached time,
     * the update will be ignored.
     */
    public void update() {
        update(time.milliseconds());
    }

    /**
     * Update the cached current time to a specific value. In some contexts, the caller may already
     * have an accurate time, so this avoids unnecessary calls to system time.
     *
     * Note that if the updated current time is smaller than the cached time, then the update
     * is ignored.
     *
     * @param currentTimeMs The current time in milliseconds to cache
     */
    public void update(long currentTimeMs) {
        this.currentTimeMs = Math.max(currentTimeMs, this.currentTimeMs);
    }

    /**
     * Get the remaining time in milliseconds until the timer expires. Like {@link #currentTimeMs},
     * this depends on the cached current time, so the returned value will not change until the timer
     * has been updated using one of the {@link #update()} methods or {@link #sleep(long)}.
     *
     * @return The cached remaining time in milliseconds until timer expiration
     */
    public long remainingMs() {
        return Math.max(0, deadlineMs - currentTimeMs);
    }

    /**
     * Get the current time in milliseconds. This will return the same cached value until the timer
     * has been updated using one of the {@link #update()} methods or {@link #sleep(long)} is used.
     *
     * Note that the value returned is guaranteed to increase monotonically even if the underlying
     * {@link Time} implementation goes backwards. Effectively, the timer will just wait for the
     * time to catch up.
     *
     * @return The current cached time in milliseconds
     */
    public long currentTimeMs() {
        return currentTimeMs;
    }

    /**
     * Get the amount of time that has elapsed since the timer began. If the timer was reset, this
     * will be the amount of time since the last reset.
     *
     * @return The elapsed time since construction or the last reset
     */
    public long elapsedMs() {
        return currentTimeMs - startMs;
    }

    /**
     * Sleep for the requested duration and update the timer. Return when either the duration has
     * elapsed or the timer has expired.
     *
     * @param durationMs The duration in milliseconds to sleep
     */
    public void sleep(long durationMs) {
        long sleepDurationMs = Math.min(durationMs, remainingMs());
        time.sleep(sleepDurationMs);
        update();
    }
}
