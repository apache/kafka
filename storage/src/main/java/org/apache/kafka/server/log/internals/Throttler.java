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
package org.apache.kafka.server.log.internals;

import com.yammer.metrics.core.Meter;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.log.internals.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * A class to measure and throttle the rate of some process. The throttler takes a desired rate-per-second
 * (the units of the process don't matter, it could be bytes or a count of some other thing), and will sleep for
 * an appropriate amount of time when maybeThrottle() is called to attain the desired rate.
 */
@ThreadSafe
public class Throttler {
    private static final Logger log = LoggerFactory.getLogger(Throttler.class);

    private volatile double desiredRatePerSec;
    private final long checkIntervalNs;
    private final boolean throttleDown;
    private final Time time;
    private final Meter meter;
    private final Object lock = new Object();
    private long periodStartNs;
    private double observedSoFar;

    /**
     * Constructor.
     * @param desiredRatePerSec The rate we want to hit in units/sec
     * @param checkIntervalMs The interval at which to check our rate
     * @param throttleDown Does throttling increase or decrease our rate?
     * @param time The time implementation to use
     */
    public Throttler(final double desiredRatePerSec,
                     final long checkIntervalMs,
                     final boolean throttleDown,
                     final String metricName,
                     final String units,
                     final Time time) {
        this.desiredRatePerSec = desiredRatePerSec;
        this.checkIntervalNs = TimeUnit.MILLISECONDS.toNanos(checkIntervalMs);
        this.throttleDown = throttleDown;
        this.time = time;
        final KafkaMetricsGroup metricsGroup = new KafkaMetricsGroup(this.getClass());
        meter = metricsGroup.newMeter(metricName, units, TimeUnit.SECONDS);
        periodStartNs = time.nanoseconds();
        observedSoFar = 0.0;
    }

    public Throttler(final double desiredRatePerSec,
                     final long checkIntervalMs,
                     final Time time) {
        this(desiredRatePerSec, checkIntervalMs,
                true, "throttler", "entries",
                time);
    }

    public Throttler(final double desiredRatePerSec,
                     final long checkIntervalMs,
                     final boolean throttleDown,
                     final Time time) {
        this(desiredRatePerSec, checkIntervalMs, throttleDown,
                "throttler", "entries",
                time);
    }

    public final void maybeThrottle(final double observed) {
        final long msPerSec = TimeUnit.SECONDS.toMillis(1);
        final long nsPerSec = TimeUnit.SECONDS.toNanos(1);
        final double currentDesiredRatePerSec = desiredRatePerSec;

        meter.mark((long) observed);
        synchronized (lock) {
            observedSoFar += observed;
            final long now = time.nanoseconds();
            final long elapsedNs = now - periodStartNs;
            // if we have completed an interval AND we have observed something, maybe
            // we should take a little nap
            if (elapsedNs > checkIntervalNs && observedSoFar > 0) {
                final double rateInSecs = (observedSoFar * nsPerSec) / elapsedNs;
                final boolean needAdjustment = !(throttleDown ^ (rateInSecs > currentDesiredRatePerSec));
                if (needAdjustment) {
                    // solve for the amount of time to sleep to make us hit the desired rate
                    final double desiredRateMs = currentDesiredRatePerSec / (double) msPerSec;
                    final long elapsedMs = TimeUnit.NANOSECONDS.toMillis(elapsedNs);
                    final long sleepTime = Math.round(observedSoFar / desiredRateMs - elapsedMs);
                    if (sleepTime > 0) {
                        log.trace(
                                String.format("Natural rate is %f per second but desired rate is %f, sleeping for %d ms to compensate.",
                                rateInSecs, currentDesiredRatePerSec, sleepTime));
                        time.sleep(sleepTime);
                    }
                }
                periodStartNs = time.nanoseconds();
                observedSoFar = 0;
            }
        }
    }

    public final void updateDesiredRatePerSec(final double updatedDesiredRatePerSec) {
        this.desiredRatePerSec = updatedDesiredRatePerSec;
    }

    public static void main(String[] args) throws InterruptedException {
        final Random rand = new Random();
        final Throttler throttler = new Throttler(100000, 100, true, Time.SYSTEM);
        final long interval = 30000;
        long start = System.currentTimeMillis();
        long total = 0;
        while (true) {
            final int value = rand.nextInt(1000);
            Thread.sleep(1);
            throttler.maybeThrottle(value);
            total += value;
            final long now = System.currentTimeMillis();
            if (now - start >= interval) {
                System.out.println(total / (interval/1000.0));
                start = now;
                total = 0;
            }
        }
    }
}
