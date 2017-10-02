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

/**
 * A helper class for managing the heartbeat to the coordinator
 */
public final class Heartbeat {
    private final long sessionTimeout;
    private final long heartbeatInterval;
    private final long maxPollInterval;
    private final long retryBackoffMs;

    private volatile long lastHeartbeatSend; // volatile since it is read by metrics
    private long lastHeartbeatReceive;
    private long lastSessionReset;
    private long lastPoll;
    private boolean heartbeatFailed;

    public Heartbeat(long sessionTimeout,
                     long heartbeatInterval,
                     long maxPollInterval,
                     long retryBackoffMs) {
        if (heartbeatInterval >= sessionTimeout)
            throw new IllegalArgumentException("Heartbeat must be set lower than the session timeout");

        this.sessionTimeout = sessionTimeout;
        this.heartbeatInterval = heartbeatInterval;
        this.maxPollInterval = maxPollInterval;
        this.retryBackoffMs = retryBackoffMs;
    }

    public void poll(long now) {
        this.lastPoll = now;
    }

    public void sentHeartbeat(long now) {
        this.lastHeartbeatSend = now;
        this.heartbeatFailed = false;
    }

    public void failHeartbeat() {
        this.heartbeatFailed = true;
    }

    public void receiveHeartbeat(long now) {
        this.lastHeartbeatReceive = now;
    }

    public boolean shouldHeartbeat(long now) {
        return timeToNextHeartbeat(now) == 0;
    }
    
    public long lastHeartbeatSend() {
        return this.lastHeartbeatSend;
    }

    public long timeToNextHeartbeat(long now) {
        long timeSinceLastHeartbeat = now - Math.max(lastHeartbeatSend, lastSessionReset);
        final long delayToNextHeartbeat;
        if (heartbeatFailed)
            delayToNextHeartbeat = retryBackoffMs;
        else
            delayToNextHeartbeat = heartbeatInterval;

        if (timeSinceLastHeartbeat > delayToNextHeartbeat)
            return 0;
        else
            return delayToNextHeartbeat - timeSinceLastHeartbeat;
    }

    public boolean sessionTimeoutExpired(long now) {
        return now - Math.max(lastSessionReset, lastHeartbeatReceive) > sessionTimeout;
    }

    public long interval() {
        return heartbeatInterval;
    }

    public void resetTimeouts(long now) {
        this.lastSessionReset = now;
        this.lastPoll = now;
        this.heartbeatFailed = false;
    }

    public boolean pollTimeoutExpired(long now) {
        return now - lastPoll > maxPollInterval;
    }

}