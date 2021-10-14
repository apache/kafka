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
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Retry encapsulates the mechanism to perform a retry and then exponential
 * backoff using provided wait times between attempts.
 *
 * @param <R> Result type
 */

public class Retry<R> {

    private static final Logger log = LoggerFactory.getLogger(Retry.class);

    private final Time time;

    private final long retryBackoffMs;

    private final long retryBackoffMaxMs;

    public Retry(Time time, long retryBackoffMs, long retryBackoffMaxMs) {
        this.time = time;
        this.retryBackoffMs = retryBackoffMs;
        this.retryBackoffMaxMs = retryBackoffMaxMs;

        if (this.retryBackoffMs < 0)
            throw new IllegalArgumentException(String.format("retryBackoffMs value %s must be non-negative", retryBackoffMs));

        if (this.retryBackoffMaxMs < 0)
            throw new IllegalArgumentException(String.format("retryBackoffMaxMs %s value must be non-negative", retryBackoffMaxMs));

        if (this.retryBackoffMaxMs < this.retryBackoffMs)
            throw new IllegalArgumentException(String.format("retryBackoffMaxMs %s is less than retryBackoffMs %s", retryBackoffMaxMs, retryBackoffMs));
    }

    public R execute(Retryable<R> retryable) throws IOException {
        int currAttempt = 0;
        long end = time.milliseconds() + retryBackoffMaxMs;
        IOException error = null;

        while (time.milliseconds() <= end) {
            currAttempt++;

            try {
                return retryable.call();
            } catch (IOException e) {
                if (error == null)
                    error = e;

                long waitMs = retryBackoffMs * (long) Math.pow(2, currAttempt - 1);
                long diff = end - time.milliseconds();
                waitMs = Math.min(waitMs, diff);

                if (waitMs <= 0)
                    break;

                String message = String.format("Attempt %s to make call resulted in an error; sleeping %s ms before retrying",
                    currAttempt, waitMs);
                log.warn(message, e);

                time.sleep(waitMs);
            } catch (UnretryableException e) {
                // We've deemed this error to not be worth retrying, so collect the error and
                // fail immediately.
                if (error == null) {
                    if (e.getCause() instanceof IOException)
                        error = (IOException) e.getCause();
                    else
                        error = new IOException(e.getCause());
                }

                break;
            }
        }

        if (error == null)
            // Really shouldn't ever get to here, but...
            error = new IOException("Exhausted all retry attempts but no attempt returned value or encountered exception");

        throw error;
    }

}
