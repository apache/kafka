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
 * An util class for exponential backoff, backoff, etc...
 * The formula is Term(n) = random(1 - jitter, 1 + jitter) * scaleFactor * (ratio) ^ n
 * If scaleFactor is greater or equal than termMax, a constant backoff of will be provided
 * This class is thread-safe
 */
public class ExponentialBackoff {
    private final int ratio;
    private final double expMax;
    private final long scaleFactor;
    private final double jitter;

    public ExponentialBackoff(long scaleFactor, int ratio, long termMax, double jitter) {
        this.scaleFactor = scaleFactor;
        this.ratio = ratio;
        this.jitter = jitter;
        this.expMax = termMax > scaleFactor ?
                Math.log(termMax / (double) Math.max(scaleFactor, 1)) / Math.log(ratio) : 0;
    }

    public long backoff(long n) {
        if (expMax == 0) {
            return scaleFactor;
        }
        double exp = Math.min(n, this.expMax);
        double term = scaleFactor * Math.pow(ratio, exp);
        double randomFactor = ThreadLocalRandom.current().nextDouble(1 - jitter, 1 + jitter);
        return (long) (randomFactor * term);
    }
}
