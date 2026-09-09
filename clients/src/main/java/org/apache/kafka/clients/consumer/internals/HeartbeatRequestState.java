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

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.common.utils.internals.LogContext;

/**
 * Represents the state of a heartbeat request, including logic for timing, retries, and exponential backoff.
 *
 * The class extends {@link org.apache.kafka.clients.consumer.internals.RequestState} to enable exponential backoff
 * and duplicated request handling.
 */
public class HeartbeatRequestState extends RequestState {

    /**
     * Lower bound for the wait returned while a heartbeat is in flight. retry.backoff.ms and
     * retry.backoff.max.ms both accept 0, and callers use this value as a poll timeout, so it must
     * never be 0.
     */
    private static final long MIN_IN_FLIGHT_WAIT_MS = 1L;

    /**
     * The heartbeat timer tracks the time since the last heartbeat was sent
     */
    private final Timer heartbeatTimer;

    /**
     * The heartbeat interval which is acquired/updated through the heartbeat request
     */
    private long heartbeatIntervalMs;

    public HeartbeatRequestState(final LogContext logContext,
                                 final Time time,
                                 final long heartbeatIntervalMs,
                                 final long retryBackoffMs,
                                 final long retryBackoffMaxMs,
                                 final double jitter) {
        super(
            logContext,
            HeartbeatRequestState.class.getName(),
            retryBackoffMs,
            2,
            retryBackoffMaxMs,
            jitter
        );
        this.heartbeatIntervalMs = heartbeatIntervalMs;
        this.heartbeatTimer = time.timer(heartbeatIntervalMs);
    }

    public long heartbeatIntervalMs() {
        return heartbeatIntervalMs;
    }

    public void resetTimer() {
        this.heartbeatTimer.reset(heartbeatIntervalMs);
    }

    public long timeToNextHeartbeatMs(final long currentTimeMs) {
        if (heartbeatTimer.isExpired()) {
            if (requestInFlight()) {
                // The timer can be expired while a request is in flight both for the first heartbeat (the
                // interval is initialised to 0 and only learned from the first response) and for any later
                // heartbeat whose response takes longer than the interval. No heartbeat can be sent until
                // the in-flight one completes. The remaining backoff is measured from the last response and,
                // with default settings, is already 0 here, which would busy-spin both the application and
                // network threads. Wait the initial retry backoff (floored at 1 ms) instead of waiting forever: the network thread still has to notice a request timeout promptly
                // (NetworkClient only checks timed-out requests after selector.poll returns, and
                // ConsumerNetworkThread caps the poll at 5s), so it must come back and re-check.
                return Math.max(MIN_IN_FLIGHT_WAIT_MS, exponentialBackoff.initialInterval());
            }
            return remainingBackoffMs(currentTimeMs);
        }
        return heartbeatTimer.remainingMs();
    }

    /**
     * @inheritDoc
     *
     * Adds to the overridden method the reset of the heartbeat timer to a zero interval which allows sending
     * heartbeats after a failure without waiting for the interval.
     * After a failure, a next heartbeat may be needed with backoff (ex. errors that lead to retries, like coordinator
     * load error), or immediately (ex. errors that lead to rejoining, like fencing errors).
     */
    @Override
    public void onFailedAttempt(final long currentTimeMs) {
        heartbeatTimer.reset(0);
        super.onFailedAttempt(currentTimeMs);
    }

    @Override
    public boolean canSendRequest(final long currentTimeMs) {
        update(currentTimeMs);
        return heartbeatTimer.isExpired() && super.canSendRequest(currentTimeMs);
    }

    private void update(final long currentTimeMs) {
        this.heartbeatTimer.update(currentTimeMs);
    }

    public void updateHeartbeatIntervalMs(final long heartbeatIntervalMs) {
        if (this.heartbeatIntervalMs == heartbeatIntervalMs) {
            // no need to update the timer if the interval hasn't changed
            return;
        }
        this.heartbeatIntervalMs = heartbeatIntervalMs;
        this.heartbeatTimer.updateAndReset(heartbeatIntervalMs);
    }

    @Override
    public String toStringBase() {
        return super.toStringBase() +
            ", remainingMs=" + heartbeatTimer.remainingMs() +
            ", heartbeatIntervalMs=" + heartbeatIntervalMs;
    }
}
