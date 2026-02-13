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

/**
 * Manages retry attempts and exponential backoff for requests.
 */
public class ExponentialBackoffManager {
    private final int maxAttempts;
    private int attempts;
    private final ExponentialBackoff backoff;

    public ExponentialBackoffManager(int maxAttempts, long initialInterval, int multiplier, long maxInterval, double jitter) {
        this.maxAttempts = maxAttempts;
        this.backoff = new ExponentialBackoff(
            initialInterval,
            multiplier,
            maxInterval,
            jitter);
    }

    public void incrementAttempt() {
        attempts++;
    }

    public void resetAttempts() {
        attempts = 0;
    }

    public boolean canAttempt() {
        return attempts < maxAttempts;
    }

    public long backOff() {
        return this.backoff.backoff(attempts);
    }

    public int attempts() {
        return attempts;
    }
}