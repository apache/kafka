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


/**
 * This class helps producers throttle throughput.
 * <br>
 * If targetThroughput >= 0, the resulting average throughput will be approximately
 * min(targetThroughput, maximumPossibleThroughput). If targetThroughput < 0,
 * no throttling will occur.
 * <br>
 * To use, do this between successive send attempts:
 * <pre>
 *     {@code
 *      if (throttler.shouldThrottle(...)) {
 *          throttler.throttle();
 *      }
 *     }
 * </pre>
 *
 * Note that this can be used to throttle message throughput or data throughput.
 */
public class ThroughputThrottler {

    private static final long NS_PER_MS = 1000000L;
    private static final long NS_PER_SEC = 1000 * NS_PER_MS;
    private static final long MIN_SLEEP_NS = 2 * NS_PER_MS;

    private final long startMs;
    private final long sleepTimeNs;
    private final long targetThroughput;

    private long sleepDeficitNs = 0;
    private boolean wakeup = false;

    /**
     * @param targetThroughput Can be messages/sec or bytes/sec
     * @param startMs          When the very first message is sent
     */
    public ThroughputThrottler(long targetThroughput, long startMs) {
        this.startMs = startMs;
        this.targetThroughput = targetThroughput;
        this.sleepTimeNs = targetThroughput > 0 ?
                           NS_PER_SEC / targetThroughput :
                           Long.MAX_VALUE;
    }

    /**
     * @param amountSoFar bytes produced so far if you want to throttle data throughput, or
     *                    messages produced so far if you want to throttle message throughput.
     * @param sendStartMs timestamp of the most recently sent message
     * @return <code>true</code> if throttling should happen
     */
    public boolean shouldThrottle(long amountSoFar, long sendStartMs) {
        if (this.targetThroughput < 0) {
            // No throttling in this case
            return false;
        }

        float elapsedSec = (sendStartMs - startMs) / 1000.f;
        return elapsedSec > 0 && (amountSoFar / elapsedSec) > this.targetThroughput;
    }

    /**
     * Occasionally blocks for small amounts of time to achieve targetThroughput.
     * <br>
     * Note that if targetThroughput is 0, this will block extremely aggressively.
     */
    public void throttle() {
        if (targetThroughput == 0) {
            try {
                synchronized (this) {
                    while (!wakeup) {
                        this.wait();
                    }
                }
            } catch (InterruptedException e) {
                // do nothing
            }
            return;
        }

        // throttle throughput by sleeping, on average,
        // (1 / this.throughput) seconds between "things sent"
        sleepDeficitNs += sleepTimeNs;

        // If enough sleep deficit has accumulated, sleep a little
        if (sleepDeficitNs >= MIN_SLEEP_NS) {
            long sleepStartNs = System.nanoTime();
            try {
                synchronized (this) {
                    long remaining = sleepDeficitNs;
                    while (!wakeup && remaining > 0) {
                        long sleepMs = remaining / 1000000;
                        long sleepNs = remaining - sleepMs * 1000000;
                        this.wait(sleepMs, (int) sleepNs);
                        long elapsed = System.nanoTime() - sleepStartNs;
                        remaining = sleepDeficitNs - elapsed;
                    }
                    wakeup = false;
                }
                sleepDeficitNs = 0;
            } catch (InterruptedException e) {
                // If sleep is cut short, reduce deficit by the amount of
                // time we actually spent sleeping
                long sleepElapsedNs = System.nanoTime() - sleepStartNs;
                if (sleepElapsedNs <= sleepDeficitNs) {
                    sleepDeficitNs -= sleepElapsedNs;
                }
            }
        }
    }

    /**
     * Wakeup the throttler if its sleeping.
     */
    public void wakeup() {
        synchronized (this) {
            wakeup = true;
            this.notifyAll();
        }
    }
}

