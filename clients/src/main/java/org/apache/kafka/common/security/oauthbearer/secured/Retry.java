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

package org.apache.kafka.common.security.oauthbearer.secured;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Retry encapsulates the mechanism to perform a retry and then exponential
 * backoff using provided wait times between attempts.
 *
 * We have some state exposed to the caller to determine how many attempts were
 * made. This is mostly to expose for testing purposes. As a result, the
 * Retry class is not meant to be used more than once.
 *
 * @param <R> Result type
 */

public class Retry<R> {

    private static final Logger log = LoggerFactory.getLogger(Retry.class);

    private static final int INITIAL_CURR_ATTEMPT_VALUE = 0;

    private final Time time;

    private final long retryWaitMs;

    private final long maxWaitMs;

    private final int attempts;

    private final AtomicInteger currAttempt = new AtomicInteger(INITIAL_CURR_ATTEMPT_VALUE);

    public Retry(Time time, int attempts, long retryWaitMs, long maxWaitMs) {
        this.time = time;
        this.attempts = attempts;
        this.retryWaitMs = retryWaitMs;
        this.maxWaitMs = maxWaitMs;

        if (this.attempts < 1)
            throw new IllegalArgumentException("attempts value must be positive");

        if (this.retryWaitMs < 0)
            throw new IllegalArgumentException("retryWaitMs value must be non-negative");

        if (this.maxWaitMs < 0)
            throw new IllegalArgumentException("maxWaitWs value must be non-negative");

        if (this.maxWaitMs < this.retryWaitMs)
            log.warn("maxWaitMs {} is less than retryWaitMs {}", this.maxWaitMs, this.retryWaitMs);
    }

    public R execute(Retryable<R> retryable) throws IOException {
        if (currAttempt.get() != INITIAL_CURR_ATTEMPT_VALUE)
            throw new IllegalStateException(String.format("Current attempt count %s is not set to initial starting state %s", currAttempt.get(), INITIAL_CURR_ATTEMPT_VALUE));

        while (currAttempt.incrementAndGet() <= attempts) {
            try {
                return retryable.call();
            } catch (IOException e) {
                if (currAttempt.get() >= attempts) {
                    throw e;
                } else {
                    long waitMs = retryWaitMs * (long) Math.pow(2, currAttempt.get() - 1);
                    waitMs = Math.min(waitMs, maxWaitMs);

                    String message = String.format("Attempt %s of %s to retryable call resulted in an error; sleeping %s ms before retrying",
                        currAttempt.get(), attempts, waitMs);
                    log.warn(message, e);

                    time.sleep(waitMs);
                }
            }
        }

        // Really shouldn't ever get to here, but...
        throw new IllegalStateException("Exhausted all retry attempts but neither returned value or encountered exception");
    }

    public int getAttemptsMade() {
        return currAttempt.get();
    }

}
