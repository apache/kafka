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
package org.apache.kafka.clients;

import org.apache.kafka.common.config.AbstractConfig;

/**
 * A policy which exponentially grows the delay between each reconnection attempt.
 * A reconnection attempt (@code i] is tried after {@code Math.min(2^(i-1) * getBaseDelayMs(), getMaxDelayMs())} milliseconds.
 */
public class ExponentialReconnectAttemptPolicy implements ReconnectAttemptPolicy {

    /**
     * The amount of time to wait before attempting a first reconnection to a given host.
     */
    private long baseDelayMs;
    /**
     * The maximum amount of time to wait before attempting to reconnect to a given host.
     */
    private long maxDelayMs;

    /**
     * Creates a new {@link ExponentialReconnectAttemptPolicy} instance.
     */
    public ExponentialReconnectAttemptPolicy() {

    }

    /**
     * Creates a new {@link ExponentialReconnectAttemptPolicy} instance.
     *
     * @param baseDelayMs {@link #baseDelayMs}.
     * @param maxDelayMs {@link #maxDelayMs}.
     */
    public ExponentialReconnectAttemptPolicy(long baseDelayMs, long maxDelayMs) {
        setDelays(baseDelayMs, maxDelayMs);
    }

    public long getBaseDelayMs() {
        return baseDelayMs;
    }

    public long getMaxDelayMs() {
        return maxDelayMs;
    }

    @Override
    public void configure(AbstractConfig configs) {
        setDelays(configs.getLong(CommonClientConfigs.RECONNECT_EXPONENTIAL_BASE_DELAY_MS_CONFIG),
                configs.getLong(CommonClientConfigs.RECONNECT_EXPONENTIAL_MAX_DELAY_MS_CONFIG));
    }

    private void setDelays(long baseDelayMs, long maxDelayMs) {
        if (baseDelayMs < 0 || maxDelayMs < 0)
            throw new IllegalArgumentException(String.format("Delay must be positive - baseDelayMs %d, maxDelayMs %d ", baseDelayMs, maxDelayMs));
        if (maxDelayMs < baseDelayMs)
            throw new IllegalArgumentException(String.format("maxDelayMs (%d) must be superior to baseDelayMs (%d)", maxDelayMs, baseDelayMs));
        this.baseDelayMs = baseDelayMs;
        this.maxDelayMs  = maxDelayMs;
    }

    @Override
    public ReconnectAttemptScheduler newScheduler() {
        return new ExponentialScheduler();
    }

    public class ExponentialScheduler implements ReconnectAttemptScheduler {

        private long attempts = 0L;

        @Override
        public long nextReconnectBackoffMs() {
            try {
                if (attempts + 1 > 64) return maxDelayMs;
                return Math.min(multiplyExact(baseDelayMs, 1L << attempts++), maxDelayMs);
            } catch (ArithmeticException e) {  // this should never happen
                return maxDelayMs;
            }
        }
    }

    /**
     * This method is part of Math class since java 8.
     */
    private static long multiplyExact(long x, long y) {
        long r = x * y;
        long ax = Math.abs(x);
        long ay = Math.abs(y);
        if ((ax | ay) >>> 31 != 0) {
            // Some bits greater than 2^31 that might cause overflow
            // Check the result using the divide operator
            // and check for the special case of Long.MIN_VALUE * -1
            if (((y != 0) && (r / y != x)) ||
                    (x == Long.MIN_VALUE && y == -1)) {
                throw new ArithmeticException("long overflow");
            }
        }
        return r;
    }
}


