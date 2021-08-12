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

package org.apache.kafka.common.security.oauthbearer.internals.secured.httpclient;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Retry<T> {

    private static final Logger log = LoggerFactory.getLogger(Retry.class);

    private final String uri;

    private final long retryWaitMs;

    private final long maxWaitMs;

    private final int attempts;

    public Retry(final String uri, final int attempts, final long retryWaitMs, final long maxWaitMs) {
        this.uri = uri;
        this.attempts = attempts;
        this.retryWaitMs = retryWaitMs;
        this.maxWaitMs = maxWaitMs;

        if (this.attempts <= 1)
            throw new IllegalArgumentException("attempts value must be positive");

        if (this.retryWaitMs < 0)
            throw new IllegalArgumentException("retryWaitMs value must be non-negative");

        if (this.maxWaitMs < 0)
            throw new IllegalArgumentException("maxWaitWs value must be non-negative");

        if (this.maxWaitMs < this.retryWaitMs)
            log.warn("maxWaitMs {} is less than retryWaitMs {}", this.maxWaitMs, this.retryWaitMs);
    }

    public T execute(HttpCall<T> call) throws IOException {
        for (int attempt = 1; attempt <= attempts; attempt++) {
            try {
                return call.call();
            } catch (IOException e) {
                if (attempt >= attempts) {
                    throw e;
                } else {
                    double jitterFactor = ThreadLocalRandom.current().nextDouble(0.0, 0.1);
                    long waitMs = retryWaitMs * (long)Math.pow(2, attempt - 1);
                    waitMs += (double)waitMs * jitterFactor;
                    waitMs = Math.min(waitMs, maxWaitMs);

                    String message = String.format("Attempt %s of %s to call HTTP URL %s resulted in an error; sleeping %s ms before retrying",
                        attempt, attempts, uri, waitMs);
                    log.warn(message, e);

                    Utils.sleep(waitMs);
                }
            }
        }

        // Really shouldn't ever get to here, but...
        throw new IllegalStateException("Exhausted all retry attempts but neither returned value or encountered exception");
    }

}
