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

package org.apache.kafka.controller;

import org.apache.kafka.common.utils.LogContext;

import org.slf4j.Logger;

import java.text.DecimalFormat;
import java.util.AbstractMap;
import java.util.Map;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Track the performance of controller events. Periodically log the slowest events.
 * Log any event slower than a certain threshold.
 */
class EventPerformanceMonitor {
    /**
     * The format to use when displaying milliseconds.
     */
    private static final DecimalFormat MILLISECOND_DECIMAL_FORMAT = new DecimalFormat("#0.00");

    static class Builder {
        LogContext logContext = null;
        long periodNs = SECONDS.toNanos(60);
        long alwaysLogThresholdNs = SECONDS.toNanos(2);

        Builder setLogContext(LogContext logContext) {
            this.logContext = logContext;
            return this;
        }

        Builder setPeriodNs(long periodNs) {
            this.periodNs = periodNs;
            return this;
        }

        Builder setAlwaysLogThresholdNs(long alwaysLogThresholdNs) {
            this.alwaysLogThresholdNs = alwaysLogThresholdNs;
            return this;
        }

        EventPerformanceMonitor build() {
            if (logContext == null) logContext = new LogContext();
            return new EventPerformanceMonitor(logContext,
                    periodNs,
                    alwaysLogThresholdNs);
        }
    }

    /**
     * The log4j object to use.
     */
    private final Logger log;

    /**
     * The period in nanoseconds.
     */
    private final long periodNs;

    /**
     * The always-log threshold in nanoseconds.
     */
    private final long alwaysLogThresholdNs;

    /**
     * The name of the slowest event we've seen so far, or null if none has been seen.
     */
    private String slowestEventName;

    /**
     * The duration of the slowest event we've seen so far, or 0 if none has been seen.
     */
    private long slowestEventDurationNs;

    /**
     * The total duration of all the events we've seen.
     */
    private long totalEventDurationNs;

    /**
     * The number of events we've seen.
     */
    private int numEvents;

    private EventPerformanceMonitor(
        LogContext logContext,
        long periodNs,
        long alwaysLogThresholdNs
    ) {
        this.log = logContext.logger(EventPerformanceMonitor.class);
        this.periodNs = periodNs;
        this.alwaysLogThresholdNs = alwaysLogThresholdNs;
        reset();
    }

    long periodNs() {
        return periodNs;
    }

    Map.Entry<String, Long> slowestEvent() {
        return new AbstractMap.SimpleImmutableEntry<>(slowestEventName, slowestEventDurationNs);
    }

    /**
     * Reset all internal state.
     */
    void reset() {
        this.slowestEventName = null;
        this.slowestEventDurationNs = 0;
        this.totalEventDurationNs = 0;
        this.numEvents = 0;
    }

    /**
     * Handle a controller event being finished.
     *
     * @param name          The name of the controller event.
     * @param durationNs    The duration of the controller event in nanoseconds.
     */
    void observeEvent(String name, long durationNs) {
        String message = doObserveEvent(name, durationNs);
        if (message != null) {
            log.error("{}", message);
        }
    }

    /**
     * Handle a controller event being finished.
     *
     * @param name          The name of the controller event.
     * @param durationNs    The duration of the controller event in nanoseconds.
     *
     * @return              The message to log, or null otherwise.
     */
    String doObserveEvent(String name, long durationNs) {
        if (slowestEventName == null || slowestEventDurationNs < durationNs) {
            slowestEventName = name;
            slowestEventDurationNs = durationNs;
        }
        totalEventDurationNs += durationNs;
        numEvents++;
        if (durationNs < alwaysLogThresholdNs) {
            return null;
        }
        return "Exceptionally slow controller event " + name + " took " +
            NANOSECONDS.toMillis(durationNs) + " ms.";
    }

    /**
     * Generate a log message summarizing the events of the last period,
     * and then reset our internal state.
     */
    void generatePeriodicPerformanceMessage() {
        String message = periodicPerformanceMessage();
        log.info("{}", message);
        reset();
    }

    /**
     * Generate a log message summarizing the events of the last period.
     *
     * @return                          The summary string.
     */
    String periodicPerformanceMessage() {
        StringBuilder bld = new StringBuilder();
        bld.append("In the last ");
        bld.append(NANOSECONDS.toMillis(periodNs));
        bld.append(" ms period, ");
        if (numEvents == 0) {
            bld.append("there were no controller events completed.");
        } else {
            bld.append(numEvents).append(" controller events were completed, which took an average of ");
            bld.append(formatNsAsDecimalMs(totalEventDurationNs / numEvents));
            bld.append(" ms each. The slowest event was ").append(slowestEventName);
            bld.append(", which took ");
            bld.append(formatNsAsDecimalMs(slowestEventDurationNs));
            bld.append(" ms.");
        }
        return bld.toString();
    }

    /**
     * Translate a duration in nanoseconds to a decimal duration in milliseconds.
     *
     * @param durationNs    The duration in nanoseconds.
     * @return              The decimal duration in milliseconds.
     */
    static String formatNsAsDecimalMs(long durationNs) {
        double number = NANOSECONDS.toMicros(durationNs);
        number /= 1000;
        return MILLISECOND_DECIMAL_FORMAT.format(number);
    }
}
