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

import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;

/**
 * Represents the state of a heartbeat request, including logic for timing, retries, and exponential backoff. The object extends
 * {@link org.apache.kafka.clients.consumer.internals.RequestState} to enable exponential backoff and duplicated request handling. The two fields that it holds are:
 */
public class HeartbeatRequestState extends RequestState {

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
            return remainingBackoffMs(currentTimeMs);
        }
        return heartbeatTimer.remainingMs();
    }

    @Override
    public void onFailedAttempt(final long currentTimeMs) {
        // Reset timer to allow sending HB after a failure without waiting for the interval.
        // After a failure, a next HB may be needed with backoff (ex. errors that lead to
        // retries, like coordinator load error), or immediately (ex. errors that lead to
        // rejoining, like fencing errors).
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
