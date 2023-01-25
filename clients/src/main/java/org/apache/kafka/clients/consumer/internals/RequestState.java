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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.common.utils.ExponentialBackoff;

class RequestState {
    final static int RETRY_BACKOFF_EXP_BASE = 2;
    final static double RETRY_BACKOFF_JITTER = 0.2;
    private final ExponentialBackoff exponentialBackoff;
    private long lastSentMs = -1;
    private long lastReceivedMs = -1;
    private int numAttempts = 0;
    private long backoffMs = 0;

    public RequestState(long retryBackoffMs) {
        this.exponentialBackoff = new ExponentialBackoff(
            retryBackoffMs,
            RETRY_BACKOFF_EXP_BASE,
            retryBackoffMs,
            RETRY_BACKOFF_JITTER
        );
    }

    // Visible for testing
    RequestState(final long retryBackoffMs,
                 final int retryBackoffExpBase,
                 final long retryBackoffMaxMs,
                 final double jitter) {
        this.exponentialBackoff = new ExponentialBackoff(
            retryBackoffMs,
            retryBackoffExpBase,
            retryBackoffMaxMs,
            jitter
        );
    }

    /**
     * Reset request state so that new requests can be sent immediately
     * and the backoff is restored to its minimal configuration.
     */
    public void reset() {
        this.lastSentMs = -1;
        this.lastReceivedMs = -1;
        this.numAttempts = 0;
        this.backoffMs = exponentialBackoff.backoff(0);
    }

    public boolean canSendRequest(final long currentTimeMs) {
        if (this.lastSentMs == -1) {
            // no request has been sent
            return true;
        }

        if (this.lastReceivedMs == -1 ||
                this.lastReceivedMs < this.lastSentMs) {
            // there is an inflight request
            return false;
        }

        return requestBackoffExpired(currentTimeMs);
    }

    public void onSendAttempt(final long currentTimeMs) {
        // Here we update the timer everytime we try to send a request. Also increment number of attempts.
        this.lastSentMs = currentTimeMs;
    }

    /**
     * Callback invoked after a successful send. This resets the number of attempts
     * to 0, but the minimal backoff will still be enforced prior to allowing a new
     * send. To send immediately, use {@link #reset()}.
     *
     * @param currentTimeMs Current time in milliseconds
     */
    public void onSuccessfulAttempt(final long currentTimeMs) {
        this.lastReceivedMs = currentTimeMs;
        this.backoffMs = exponentialBackoff.backoff(0);
        this.numAttempts = 0;
    }

    /**
     * Callback invoked after a failed send. The number of attempts
     * will be incremented, which may increase the backoff before allowing
     * the next send attempt.
     *
     * @param currentTimeMs Current time in milliseconds
     */
    public void onFailedAttempt(final long currentTimeMs) {
        this.lastReceivedMs = currentTimeMs;
        this.backoffMs = exponentialBackoff.backoff(numAttempts);
        this.numAttempts++;
    }

    private boolean requestBackoffExpired(final long currentTimeMs) {
        return remainingBackoffMs(currentTimeMs) <= 0;
    }

    long remainingBackoffMs(final long currentTimeMs) {
        long timeSinceLastReceiveMs = currentTimeMs - this.lastReceivedMs;
        return Math.max(0, backoffMs - timeSinceLastReceiveMs);
    }
}
