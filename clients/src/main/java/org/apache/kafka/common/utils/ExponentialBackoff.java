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

import java.util.concurrent.ThreadLocalRandom;

/**
 * A utility class for keeping the parameters and providing the value of exponential
 * retry backoff, exponential reconnect backoff, exponential timeout, etc.
 * <p>
 * The formula is:
 * <pre>Backoff(attempts) = random(1 - jitter, 1 + jitter) * initialInterval * multiplier ^ attempts</pre>
 * If {$code maxInterval} is less that {@code initialInterval}, a constant backoff of
 * {@code maxInterval} will be provided. The jitter will never cause the backoff to exceed
 * {@code maxInterval}.
 * <p>
 * This class is thread-safe.
 */
public class ExponentialBackoff {
    private final long initialInterval;
    private final int multiplier;
    private final long maxInterval;
    private final double jitter;
    private final double expMax;

    public ExponentialBackoff(long initialInterval, int multiplier, long maxInterval, double jitter) {
        this.initialInterval = maxInterval < initialInterval ? maxInterval : initialInterval;
        this.multiplier = multiplier;
        this.maxInterval = maxInterval;
        this.jitter = jitter;
        this.expMax = maxInterval > initialInterval ?
                Math.log(maxInterval / (double) Math.max(initialInterval, 1)) / Math.log(multiplier) : 0;
    }

    public long backoff(long attempts) {
        if (expMax == 0) {
            return initialInterval;
        }
        double exp = Math.min(attempts, this.expMax);
        double term = initialInterval * Math.pow(multiplier, exp);
        double randomFactor = jitter < Double.MIN_NORMAL ? 1.0 :
            ThreadLocalRandom.current().nextDouble(1 - jitter, 1 + jitter);
        long backoffValue = (long) (randomFactor * term);
        return backoffValue > maxInterval ? maxInterval : backoffValue;
    }

    @Override
    public String toString() {
        return "ExponentialBackoff{" +
                "multiplier=" + multiplier +
                ", expMax=" + expMax +
                ", initialInterval=" + initialInterval +
                ", jitter=" + jitter +
                '}';
    }
}
