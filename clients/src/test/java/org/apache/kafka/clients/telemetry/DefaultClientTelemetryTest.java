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
package org.apache.kafka.clients.telemetry;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.PushTelemetryResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class DefaultClientTelemetryTest extends BaseClientTelemetryTest {
    // state transition
    private static Map<TelemetryState, TelemetryState> stateMap;
    static {
        stateMap = new HashMap<>();
        stateMap.put(TelemetryState.subscription_needed, TelemetryState.subscription_in_progress);
        stateMap.put(TelemetryState.subscription_in_progress, TelemetryState.push_needed);
        stateMap.put(TelemetryState.push_needed, TelemetryState.push_in_progress);
        stateMap.put(TelemetryState.push_in_progress, TelemetryState.terminating_push_needed);
        stateMap.put(TelemetryState.terminating_push_needed, TelemetryState.terminating_push_in_progress);
        stateMap.put(TelemetryState.terminating_push_in_progress, TelemetryState.terminated);
    }

    @Test
    public void testSingleClose() {
        DefaultClientTelemetry clientTelemetry = newClientTelemetry();
        clientTelemetry.close();
    }

    @Test
    public void testDoubleClose() {
        DefaultClientTelemetry clientTelemetry = newClientTelemetry();
        clientTelemetry.close();
        clientTelemetry.close();
    }

    @Test
    public void testInitiateCloseWithoutSubscription() {
        DefaultClientTelemetry clientTelemetry = newClientTelemetry();
        clientTelemetry.initiateClose(Duration.ofMillis(50));
        clientTelemetry.close();
    }

    @Test
    public void testInitiateCloseWithSubscription() {
        DefaultClientTelemetry clientTelemetry = newClientTelemetry();
        clientTelemetry.setSubscription(newTelemetrySubscription());
        clientTelemetry.initiateClose(Duration.ofMillis(50));
        clientTelemetry.close();
    }

    @Test
    public void testClientInstanceIdSetWithinBlock() {
        testClientInstanceIdTiming(500, 250, true);
    }

    @Test
    public void testClientInstanceIdNotSet() {
        testClientInstanceIdTiming(500, -1, false);
    }

    @Test
    public void testClientInstanceIdSetBefore() {
        testClientInstanceIdTiming(50, 0, true);
    }

    @Test
    public void testClientInstanceIdSetAfter() {
        testClientInstanceIdTiming(250, 500, false);
    }

    private void testClientInstanceIdTiming(long readerThreadBlockMs, long writerThreadSleepMs, boolean shouldBePresent) {
        Time time = Time.SYSTEM;

        try (DefaultClientTelemetry clientTelemetry = new DefaultClientTelemetry(time, "test")) {
            if (writerThreadSleepMs < 0) {
                // If the amount of time for the writer to sleep is null, interpret that as not
                // writing at all.
                time.milliseconds();
            } else if (writerThreadSleepMs > 0) {
                // If the amount of time for the writer to sleep is a positive number, interpret
                // sleep for that amount of time.
                new Thread(() -> {
                    Utils.sleep(writerThreadSleepMs);
                    clientTelemetry.setSubscription(newTelemetrySubscription(time));
                }).start();
            } else {
                // If the amount of time for the writer to sleep is 0, interpret that as a request
                // to run immediately.
                clientTelemetry.setSubscription(newTelemetrySubscription(time));
            }

            Optional<String> clientInstanceId = clientTelemetry.clientInstanceId(Duration.ofMillis(readerThreadBlockMs));
            assertNotNull(clientInstanceId);
            assertEquals(shouldBePresent, clientInstanceId.isPresent());
        }
    }

    @Test
    public void testTelemetrySubscriptionReceivedStateTransition() {
        DefaultClientTelemetry clientTelemetry = newClientTelemetry();
        clientTelemetry.setSubscription(new TelemetrySubscription(
                0,
                0,
                Uuid.randomUuid(),
                42,
                Collections.singletonList(CompressionType.NONE),
                10000,
                true,
                MetricSelector.ALL));
        transitionState(TelemetryState.push_in_progress, clientTelemetry);
        assertEquals(TelemetryState.push_in_progress, clientTelemetry.state().get());
        PushTelemetryResponseData data = new PushTelemetryResponseData();
        data.setErrorCode(Errors.CLUSTER_AUTHORIZATION_FAILED.code());
        clientTelemetry.pushTelemetryReceived(data);
        assertEquals(TelemetryState.subscription_needed, clientTelemetry.state().orElseThrow(() -> new RuntimeException("unable to make state transition")));

        clientTelemetry.close();
    }

    // an utility function that transition the current state to a target state
    private void transitionState(final TelemetryState toState, ClientTelemetry clientTelemetry) {
        TelemetryState currentState = clientTelemetry.state().orElseThrow(
                () -> new RuntimeException("null state should not happen"));

        TelemetryState nextState;
        while (currentState != toState) {
            nextState = stateMap.get(currentState);
            clientTelemetry.setState(nextState);
            currentState = nextState;
        }
    }
}
