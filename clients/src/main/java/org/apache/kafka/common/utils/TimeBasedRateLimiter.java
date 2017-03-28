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

import org.slf4j.Logger;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A time-based rate limiter.
 */
public final class TimeBasedRateLimiter {
    /**
     * The minimum period in milliseconds at which we will expire the timer.
     */
    private final long periodMs;

    /**
     * The last time the timer expired.
     */
    private final AtomicLong lastExpiredTimeMs = new AtomicLong(0);

    /**
     * The base class for objects which perform an action.
     */
    public static class Performer {
        public void timerExpired() {
        }
        public void timerNotExpired() {
        }
    }

    /**
     * A Performer which logs a message at either INFO level or TRACE level.
     */
    public static class InfoOrTraceLog extends Performer {
        private final Logger log;
        private final String format;
        private final Object[] params;

        public InfoOrTraceLog(Logger log, String format, Object... params) {
            this.log = log;
            this.format = format;
            this.params = params;
        }

        @Override
        public void timerExpired() {
            if (log.isInfoEnabled()) {
                log.info(getMessage());
            }
        }

        @Override
        public void timerNotExpired() {
            if (log.isTraceEnabled()) {
                log.trace(getMessage());
            }
        }

        private String getMessage() {
            return String.format(format, params);
        }
    }

    public TimeBasedRateLimiter(long periodValue, TimeUnit timeUnit) {
        this.periodMs = timeUnit.toMillis(periodValue);
    }

    /**
     * Perform an action.
     *
     * @param currentMs         The current time in milliseconds.
     * @param performer         The Performer to user.
     */
    public void perform(long currentMs, Performer performer) {
        long last;
        do {
            last = lastExpiredTimeMs.get();
            long deltaMs = currentMs - last;
            if (deltaMs < periodMs) {
                performer.timerNotExpired();
                return;
            }
        } while (!lastExpiredTimeMs.compareAndSet(last, currentMs));
        performer.timerExpired();
    }

    /**
     * Perform an action.
     *
     * @param performer         The Performer to user.
     */
    public void perform(Performer performer) {
        perform(Time.SYSTEM.milliseconds(), performer);
    }
}
