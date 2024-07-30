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
package org.apache.kafka.server.metrics;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.server.util.timer.TimerTask;

import java.util.Objects;
import java.util.Set;

/**
 * Contains the metrics instance metadata and the state of the client instance.
 */
public class ClientMetricsInstance {

    private final Uuid clientInstanceId;
    private final ClientMetricsInstanceMetadata instanceMetadata;
    private final int subscriptionId;
    private final int subscriptionVersion;
    private final Set<String> metrics;
    private final int pushIntervalMs;

    private long lastGetRequestTimestamp;
    private long lastPushRequestTimestamp;
    private volatile boolean terminating;
    private volatile Errors lastKnownError;
    private TimerTask expirationTimerTask;

    public ClientMetricsInstance(Uuid clientInstanceId, ClientMetricsInstanceMetadata instanceMetadata,
        int subscriptionId, int subscriptionVersion, Set<String> metrics, int pushIntervalMs) {
        this.clientInstanceId = Objects.requireNonNull(clientInstanceId);
        this.instanceMetadata = Objects.requireNonNull(instanceMetadata);
        this.subscriptionId = subscriptionId;
        this.subscriptionVersion = subscriptionVersion;
        this.metrics = metrics;
        this.terminating = false;
        this.pushIntervalMs = pushIntervalMs;
        this.lastKnownError = Errors.NONE;
    }

    public Uuid clientInstanceId() {
        return clientInstanceId;
    }

    public ClientMetricsInstanceMetadata instanceMetadata() {
        return instanceMetadata;
    }

    public int pushIntervalMs() {
        return pushIntervalMs;
    }

    public int subscriptionId() {
        return subscriptionId;
    }

    public int subscriptionVersion() {
        return subscriptionVersion;
    }

    public Set<String> metrics() {
        return metrics;
    }

    public boolean terminating() {
        return terminating;
    }

    public synchronized void terminating(boolean terminating) {
        this.terminating = terminating;
    }

    public Errors lastKnownError() {
        return lastKnownError;
    }

    public synchronized void lastKnownError(Errors lastKnownError) {
        this.lastKnownError = lastKnownError;
    }

    // Visible for testing
    public synchronized TimerTask expirationTimerTask() {
        return expirationTimerTask;
    }

    public synchronized boolean maybeUpdateGetRequestTimestamp(long currentTime) {
        long lastRequestTimestamp = Math.max(lastGetRequestTimestamp, lastPushRequestTimestamp);
        long timeElapsedSinceLastMsg = currentTime - lastRequestTimestamp;
        if (timeElapsedSinceLastMsg >= pushIntervalMs) {
            lastGetRequestTimestamp = currentTime;
            return true;
        }
        return false;
    }

    public synchronized boolean maybeUpdatePushRequestTimestamp(long currentTime) {
        /*
         Immediate push request after get subscriptions fetch can be accepted outside push interval
         time as client applies a jitter to the push interval, which might result in a request being
         sent between 0.5 * pushIntervalMs and 1.5 * pushIntervalMs.
        */
        boolean canAccept = lastGetRequestTimestamp > lastPushRequestTimestamp;
        if (!canAccept) {
            long timeElapsedSinceLastMsg = currentTime - lastPushRequestTimestamp;
            canAccept = timeElapsedSinceLastMsg >= pushIntervalMs;
        }

        // Update the timestamp only if the request can be accepted.
        if (canAccept) {
            lastPushRequestTimestamp = currentTime;
        }
        return canAccept;
    }

    public synchronized void cancelExpirationTimerTask() {
        if (expirationTimerTask != null) {
            expirationTimerTask.cancel();
            expirationTimerTask = null;
        }
    }

    public synchronized void updateExpirationTimerTask(TimerTask timerTask) {
        cancelExpirationTimerTask();
        expirationTimerTask = timerTask;
    }
}
