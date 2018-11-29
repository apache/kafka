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
import java.util.concurrent.TimeUnit;

/**
 * An interface abstracting the clock to use in unit testing classes that make use of clock time.
 *
 * Implementations of this class should be thread-safe.
 */
public interface Time {

    Time SYSTEM = new SystemTime();

    /**
     * Returns the number of milliseconds since midnight UTC on 1/1/1970.
     *
     * This value is likely less precise than relative time.
     */
    long absoluteMilliseconds();

    /**
     * Returns the current value of the running JVM's high-resolution time source, rounded down to milliseconds.
     *
     * <p>This method can only be used to measure elapsed time and is
     * not related to any other notion of system or wall-clock time.
     * The value returned represents nanoseconds since some fixed but
     * arbitrary <i>origin</i> time (perhaps in the future, so values
     * may be negative).  The same origin is used by all invocations of
     * this method in an instance of a Java virtual machine; other
     * virtual machine instances are likely to use a different origin.
     */
    default long relativeMilliseconds() {
        return TimeUnit.NANOSECONDS.toMillis(relativeNanoseconds());
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
    long relativeNanoseconds();

    /**
     * Sleep for the given number of milliseconds
     */
    void sleep(long ms);

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
}
