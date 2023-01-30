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

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 * An interface abstracting the clock to use in unit testing classes that make use of clock time.
 *
 * Implementations of this class should be thread-safe.
 */
public interface Time {

    Time SYSTEM = new SystemTime();

    /**
     * Returns the current time in milliseconds.
     */
    long milliseconds();

    /**
     * Returns the value returned by `nanoseconds` converted into milliseconds.
     */
    default long hiResClockMs() {
        return TimeUnit.NANOSECONDS.toMillis(nanoseconds());
    }

    /**
     * Returns the current value of the running JVM's high-resolution time source, in nanoseconds.
     *
     * <p>This method can only be used to measure elapsed time and is
     * not related to any other notion of system or wall-clock time.
     * The value returned represents nanoseconds since some fixed but
     * arbitrary <i>origin</i> time (perhaps in the future, so values
     * may be negative).  The same origin is used by all invocations of
     * this method in an instance of a Java virtual machine; other
     * virtual machine instances are likely to use a different origin.
     */
    long nanoseconds();

    /**
     * Sleep for the given number of milliseconds
     */
    void sleep(long ms);

    /**
     * Wait for a condition using the monitor of a given object. This avoids the implicit
     * dependence on system time when calling {@link Object#wait()}.
     *
     * @param obj The object that will be waited with {@link Object#wait()}. Note that it is the responsibility
     *      of the caller to call notify on this object when the condition is satisfied.
     * @param condition The condition we are awaiting
     * @param deadlineMs The deadline timestamp at which to raise a timeout error
     *
     * @throws org.apache.kafka.common.errors.TimeoutException if the timeout expires before the condition is satisfied
     */
    void waitObject(Object obj, Supplier<Boolean> condition, long deadlineMs) throws InterruptedException;

    /**
     * Get a timer which is bound to this time instance and expires after the given timeout
     */
    default Timer timer(long timeoutMs) {
        return new Timer(this, timeoutMs);
    }

    /**
     * Get a timer which is bound to this time instance and expires after the given timeout
     */
    default Timer timer(Duration timeout) {
        return timer(timeout.toMillis());
    }

    /**
     * Wait for a future to complete, or time out.
     *
     * @param future        The future to wait for.
     * @param deadlineNs    The time in the future, in monotonic nanoseconds, to time out.
     * @return              The result of the future.
     * @param <T>           The type of the future.
     */
    default <T> T waitForFuture(
        CompletableFuture<T> future,
        long deadlineNs
    ) throws TimeoutException, InterruptedException, ExecutionException  {
        TimeoutException timeoutException = null;
        while (true) {
            long nowNs = nanoseconds();
            if (deadlineNs <= nowNs) {
                throw (timeoutException == null) ? new TimeoutException() : timeoutException;
            }
            long deltaNs = deadlineNs - nowNs;
            try {
                return future.get(deltaNs, TimeUnit.NANOSECONDS);
            } catch (TimeoutException t) {
                timeoutException = t;
            }
        }
    }
}
