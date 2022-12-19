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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.utils.ExponentialBackoff;

class RequestState {
    final static int RECONNECT_BACKOFF_EXP_BASE = 2;
    final static double RECONNECT_BACKOFF_JITTER = 0.2;
    private final ExponentialBackoff exponentialBackoff;
    private long lastSentMs = -1;
    private long lastReceivedMs = -1;
    private int numAttempts = 0;
    private long backoffMs = 0;

    public RequestState(ConsumerConfig config) {
        this.exponentialBackoff = new ExponentialBackoff(
                config.getLong(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG),
                RECONNECT_BACKOFF_EXP_BASE,
                config.getLong(ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG),
                RECONNECT_BACKOFF_JITTER);
    }

    // Visible for testing
    RequestState(final int reconnectBackoffMs,
                 final int reconnectBackoffExpBase,
                 final int reconnectBackoffMaxMs,
                 final int jitter) {
        this.exponentialBackoff = new ExponentialBackoff(
                reconnectBackoffMs,
                reconnectBackoffExpBase,
                reconnectBackoffMaxMs,
                jitter);
    }

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

    public void updateLastSend(final long currentTimeMs) {
        // Here we update the timer everytime we try to send a request. Also increment number of attempts.
        this.lastSentMs = currentTimeMs;
    }

    public void updateLastFailedAttempt(final long currentTimeMs) {
        this.lastReceivedMs = currentTimeMs;
        this.backoffMs = exponentialBackoff.backoff(numAttempts);
        this.numAttempts++;
    }

    private boolean requestBackoffExpired(final long currentTimeMs) {
        return remainingBackoffMs(currentTimeMs) <= 0;
    }

    long remainingBackoffMs(final long currentTimeMs) {
        return Math.max(0, this.backoffMs - (currentTimeMs - this.lastReceivedMs));
    }
}
