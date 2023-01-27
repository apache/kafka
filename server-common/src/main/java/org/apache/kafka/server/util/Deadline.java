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
package org.apache.kafka.server.util;

import org.apache.kafka.common.utils.Time;

import java.math.BigInteger;
import java.util.Objects;
import java.util.concurrent.TimeUnit;


public class Deadline {
    private final long nanoseconds;

    public static Deadline fromMonotonicNanoseconds(
        long nanoseconds
    ) {
        return new Deadline(nanoseconds);
    }

    public static Deadline fromDelay(
        Time time,
        long delay,
        TimeUnit timeUnit
    ) {
        if (delay < 0) {
            throw new RuntimeException("Negative delays are not allowed.");
        }
        long nowNs = time.nanoseconds();
        BigInteger deadlineNs = BigInteger.valueOf(nowNs).
                add(BigInteger.valueOf(timeUnit.toNanos(delay)));
        if (deadlineNs.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) >= 0) {
            return new Deadline(Long.MAX_VALUE);
        } else {
            return new Deadline(deadlineNs.longValue());
        }
    }

    private Deadline(long nanoseconds) {
        this.nanoseconds = nanoseconds;
    }

    public long nanoseconds() {
        return nanoseconds;
    }

    @Override
    public int hashCode() {
        return Objects.hash(nanoseconds);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !(o.getClass().equals(this.getClass()))) return false;
        Deadline other = (Deadline) o;
        return nanoseconds == other.nanoseconds;
    }
}
