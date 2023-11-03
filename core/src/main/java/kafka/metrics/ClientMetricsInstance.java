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
package kafka.metrics;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.Errors;

import java.util.Objects;
import java.util.Set;

/**
 * Contains the metrics instance metadata and the state of the client instance.
 */
public class ClientMetricsInstance {

    private final Uuid clientInstanceId;
    private final ClientMetricsInstanceMetadata instanceMetadata;
    private final int subscriptionId;
    private final long subscriptionUpdateEpoch;
    private final Set<String> metrics;
    private final int pushIntervalMs;

    private boolean terminating;
    private long lastRequestEpoch;
    private Errors lastKnownError;

    public ClientMetricsInstance(Uuid clientInstanceId, ClientMetricsInstanceMetadata instanceMetadata,
        int subscriptionId, long subscriptionUpdateEpoch, Set<String> metrics, int pushIntervalMs) {
        this.clientInstanceId = Objects.requireNonNull(clientInstanceId);
        this.instanceMetadata = Objects.requireNonNull(instanceMetadata);
        this.subscriptionId = subscriptionId;
        this.subscriptionUpdateEpoch = subscriptionUpdateEpoch;
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

    public long subscriptionUpdateEpoch() {
        return subscriptionUpdateEpoch;
    }

    public int subscriptionId() {
        return subscriptionId;
    }

    public Set<String> metrics() {
        return metrics;
    }

    public boolean terminating() {
        return terminating;
    }

    public void terminating(boolean terminating) {
        this.terminating = terminating;
    }

    public void lastRequestEpoch(long lastRequestEpoch) {
        this.lastRequestEpoch = lastRequestEpoch;
    }

    public Errors lastKnownError() {
        return lastKnownError;
    }

    public void lastKnownError(Errors lastKnownError) {
        this.lastKnownError = lastKnownError;
    }

    public boolean canAcceptRequest() {
        /*
         lastRequestEpoch initial value is 0 which means that first request can be accepted outside
         push interval time as client applies a jitter to the push interval, which might result in a
         request being sent between 0.5 * pushIntervalMs and 1.5 * pushIntervalMs.
        */
        long timeElapsedSinceLastMsg = System.currentTimeMillis() - lastRequestEpoch;
        return timeElapsedSinceLastMsg >= pushIntervalMs;
    }
}
