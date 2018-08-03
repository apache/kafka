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

/**
 * A helper class for managing the heartbeat to the coordinator
 */
public final class Heartbeat {
    private final int sessionTimeoutMs;
    private final int heartbeatIntervalMs;
    private final int maxPollIntervalMs;
    private final long retryBackoffMs;
    private final Time time;
    private final Timer heartbeatTimer;
    private final Timer sessionTimer;
    private final Timer pollTimer;

    private volatile long lastHeartbeatSend;

    public Heartbeat(Time time,
                     int sessionTimeoutMs,
                     int heartbeatIntervalMs,
                     int maxPollIntervalMs,
                     long retryBackoffMs) {
        if (heartbeatIntervalMs >= sessionTimeoutMs)
            throw new IllegalArgumentException("Heartbeat must be set lower than the session timeout");

        this.time = time;
        this.sessionTimeoutMs = sessionTimeoutMs;
        this.heartbeatIntervalMs = heartbeatIntervalMs;
        this.maxPollIntervalMs = maxPollIntervalMs;
        this.retryBackoffMs = retryBackoffMs;
        this.heartbeatTimer = time.timer(heartbeatIntervalMs);
        this.sessionTimer = time.timer(sessionTimeoutMs);
        this.pollTimer = time.timer(maxPollIntervalMs);
    }

    private void update(long now) {
        heartbeatTimer.update(now);
        sessionTimer.update(now);
        pollTimer.update(now);
    }

    public void poll(long now) {
        update(now);
        pollTimer.reset(maxPollIntervalMs);
    }

    public void sentHeartbeat(long now) {
        this.lastHeartbeatSend = now;
        update(now);
        heartbeatTimer.reset(heartbeatIntervalMs);
    }

    public void failHeartbeat() {
        update(time.milliseconds());
        heartbeatTimer.reset(retryBackoffMs);
    }

    public void receiveHeartbeat() {
        update(time.milliseconds());
        sessionTimer.reset(sessionTimeoutMs);
    }

    public boolean shouldHeartbeat(long now) {
        update(now);
        return heartbeatTimer.isExpired();
    }
    
    public long lastHeartbeatSend() {
        return this.lastHeartbeatSend;
    }

    public long timeToNextHeartbeat(long now) {
        update(now);
        return heartbeatTimer.remainingMs();
    }

    public boolean sessionTimeoutExpired(long now) {
        update(now);
        return sessionTimer.isExpired();
    }

    public void resetTimeouts() {
        update(time.milliseconds());
        sessionTimer.reset(sessionTimeoutMs);
        pollTimer.reset(maxPollIntervalMs);
        heartbeatTimer.reset(heartbeatIntervalMs);
    }

    public void resetSessionTimeout() {
        update(time.milliseconds());
        sessionTimer.reset(sessionTimeoutMs);
    }

    public boolean pollTimeoutExpired(long now) {
        update(now);
        return pollTimer.isExpired();
    }

    public long lastPollTime() {
        return pollTimer.currentTimeMs();
    }

}