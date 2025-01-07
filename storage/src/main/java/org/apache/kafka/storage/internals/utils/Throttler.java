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
package org.apache.kafka.storage.internals.utils;

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.metrics.KafkaMetricsGroup;

import com.yammer.metrics.core.Meter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * A class to measure and throttle the rate of some process.
 */
public class Throttler {

    private static final Logger LOG = LoggerFactory.getLogger(Throttler.class);
    private static final long MS_PER_SEC = TimeUnit.SECONDS.toMillis(1);
    private static final long NS_PER_SEC = TimeUnit.SECONDS.toNanos(1);

    private final Object lock = new Object();
    private final long checkIntervalNs;
    private final Meter meter;
    private final Time time;

    private volatile double desiredRatePerSec;
    private long periodStartNs;
    private double observedSoFar;

    /**
     * The throttler takes a desired rate-per-second (the units of the process don't matter, it could be bytes
     * or a count of some other thing), and will sleep for an appropriate amount of time when maybeThrottle()
     * is called to attain the desired rate.
     *
     * @param desiredRatePerSec  The rate we want to hit in units/sec
     * @param checkIntervalMs    The interval at which to check our rate
     * @param metricName         The name of the metric
     * @param units              The name of the unit
     * @param time               The time implementation to use
     */
    public Throttler(double desiredRatePerSec,
                     long checkIntervalMs,
                     String metricName,
                     String units,
                     Time time) {
        this.desiredRatePerSec = desiredRatePerSec;
        this.checkIntervalNs = TimeUnit.MILLISECONDS.toNanos(checkIntervalMs);
        // For compatibility - this metrics group was previously defined within a Scala class named `kafka.utils.Throttler`
        this.meter = new KafkaMetricsGroup("kafka.utils", "Throttler").newMeter(metricName, units, TimeUnit.SECONDS);
        this.time = time;
        this.periodStartNs = time.nanoseconds();
    }

    public void updateDesiredRatePerSec(double updatedDesiredRatePerSec) {
        desiredRatePerSec = updatedDesiredRatePerSec;
    }

    public double desiredRatePerSec() {
        return desiredRatePerSec;
    }

    public void maybeThrottle(double observed) {
        double currentDesiredRatePerSec = desiredRatePerSec;
        meter.mark((long) observed);
        synchronized (lock) {
            observedSoFar += observed;
            long now = time.nanoseconds();
            long elapsedNs = now - periodStartNs;
            // if we have completed an interval AND we have observed something, maybe
            // we should take a little nap
            if (elapsedNs > checkIntervalNs && observedSoFar > 0) {
                double rateInSecs = (observedSoFar * NS_PER_SEC) / elapsedNs;
                if (rateInSecs > currentDesiredRatePerSec) {
                    // solve for the amount of time to sleep to make us hit the desired rate
                    double desiredRateMs = currentDesiredRatePerSec / MS_PER_SEC;
                    long elapsedMs = TimeUnit.NANOSECONDS.toMillis(elapsedNs);
                    long sleepTime = Math.round(observedSoFar / desiredRateMs - elapsedMs);
                    if (sleepTime > 0) {
                        LOG.trace("Natural rate is {} per second but desired rate is {}, sleeping for {} ms to compensate.", rateInSecs, currentDesiredRatePerSec, sleepTime);
                        time.sleep(sleepTime);
                    }
                }
                periodStartNs = time.nanoseconds();
                observedSoFar = 0.0;
            }
        }
    }
}
