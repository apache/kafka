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
package org.apache.kafka.clients;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

public class ClientTelemetryTest extends BaseClientTelemetryTest {
    // state transition
    private static final Map<ClientTelemetryState, ClientTelemetryState> STATE_MAP;
    static {
        STATE_MAP = new HashMap<>();
        STATE_MAP.put(ClientTelemetryState.subscription_needed, ClientTelemetryState.subscription_in_progress);
        STATE_MAP.put(ClientTelemetryState.subscription_in_progress, ClientTelemetryState.push_needed);
        STATE_MAP.put(ClientTelemetryState.push_needed, ClientTelemetryState.push_in_progress);
        STATE_MAP.put(ClientTelemetryState.push_in_progress, ClientTelemetryState.terminating_push_needed);
        STATE_MAP.put(ClientTelemetryState.terminating_push_needed, ClientTelemetryState.terminating_push_in_progress);
        STATE_MAP.put(ClientTelemetryState.terminating_push_in_progress, ClientTelemetryState.terminated);
    }

    @Test
    public void testSingleClose() {
        ClientTelemetry clientTelemetry = newClientTelemetry();
        clientTelemetry.close();
    }

    @Test
    public void testDoubleClose() {
        ClientTelemetry clientTelemetry = newClientTelemetry();
        clientTelemetry.close();
        clientTelemetry.close();
    }

    @Test
    public void testInitiateCloseWithoutSubscription() {
        ClientTelemetry clientTelemetry = newClientTelemetry();
        clientTelemetry.initiateClose(Duration.ofMillis(50));
        clientTelemetry.close();
    }

    @Test
    public void testInitiateCloseWithSubscription() {
        ClientTelemetry clientTelemetry = newClientTelemetry();
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

        try (ClientTelemetry clientTelemetry = newClientTelemetry()) {
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
        ClientTelemetry clientTelemetry = newClientTelemetry();
        clientTelemetry.setSubscription(new ClientTelemetrySubscription(
                0,
                Uuid.randomUuid(),
                42,
                Collections.singletonList(CompressionType.NONE),
                10000,
                true,
                ClientTelemetryUtils.SELECTOR_ALL_METRICS));
        transitionState(ClientTelemetryState.push_in_progress, clientTelemetry);
        assertEquals(ClientTelemetryState.push_in_progress, clientTelemetry.state());
        PushTelemetryResponseData data = new PushTelemetryResponseData();
        data.setErrorCode(Errors.CLUSTER_AUTHORIZATION_FAILED.code());
        clientTelemetry.handleResponse(data);
        assertEquals(ClientTelemetryState.subscription_needed, clientTelemetry.state());

        clientTelemetry.close();
    }

    // an utility function that transition the current state to a target state
    private void transitionState(ClientTelemetryState toState, ClientTelemetry clientTelemetry) {
        ClientTelemetryState currentState = clientTelemetry.state();
        ClientTelemetryState nextState;
        while (currentState != toState) {
            nextState = STATE_MAP.get(currentState);
            assertTrue(clientTelemetry.maybeSetState(nextState));
            currentState = nextState;
        }
    }
}
