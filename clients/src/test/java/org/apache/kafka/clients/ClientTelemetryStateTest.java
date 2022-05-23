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

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

public class ClientTelemetryStateTest extends BaseClientTelemetryTest {

    @Test
    public void testValidateTransitionForSubscriptionNeeded() {
        ClientTelemetryState currState = ClientTelemetryState.subscription_needed;

        List<ClientTelemetryState> validStates = new ArrayList<>();
        // Happy path...
        validStates.add(ClientTelemetryState.subscription_in_progress);

        // 'Start shutdown w/o having done anything' case
        validStates.add(ClientTelemetryState.terminating_push_needed);

        // 'Shutdown w/o a terminal push' case
        validStates.add(ClientTelemetryState.terminated);

        testValidateTransition(currState, validStates);
    }

    @Test
    public void testValidateTransitionForSubscriptionInProgress() {
        ClientTelemetryState currState = ClientTelemetryState.subscription_in_progress;

        List<ClientTelemetryState> validStates = new ArrayList<>();
        // Happy path...
        validStates.add(ClientTelemetryState.push_needed);

        // 'Subscription had errors or requested/matches no metrics' case
        validStates.add(ClientTelemetryState.subscription_needed);

        // 'Start shutdown while waiting for the subscription' case
        validStates.add(ClientTelemetryState.terminating_push_needed);

        // 'Shutdown w/o a terminal push' case
        validStates.add(ClientTelemetryState.terminated);

        testValidateTransition(currState, validStates);
    }

    @Test
    public void testValidateTransitionForPushNeeded() {
        ClientTelemetryState currState = ClientTelemetryState.push_needed;

        List<ClientTelemetryState> validStates = new ArrayList<>();
        // Happy path...
        validStates.add(ClientTelemetryState.push_in_progress);

        // 'Attempt to send push request failed (maybe a network issue?), so loop back to getting
        // the subscription' case
        validStates.add(ClientTelemetryState.subscription_needed);

        // 'Start shutdown while waiting for a telemetry push' case
        validStates.add(ClientTelemetryState.terminating_push_needed);

        // 'Shutdown w/o a terminal push' case
        validStates.add(ClientTelemetryState.terminated);

        testValidateTransition(currState, validStates);
    }

    @Test
    public void testValidateTransitionForPushInProgress() {
        ClientTelemetryState currState = ClientTelemetryState.push_in_progress;

        List<ClientTelemetryState> validStates = new ArrayList<>();
        // Happy path...
        validStates.add(ClientTelemetryState.subscription_needed);

        // 'Start shutdown while we happen to be pushing telemetry' case
        validStates.add(ClientTelemetryState.terminating_push_needed);

        // 'Shutdown w/o a terminal push' case
        validStates.add(ClientTelemetryState.terminated);

        testValidateTransition(currState, validStates);
    }

    @Test
    public void testValidateTransitionForTerminating() {
        ClientTelemetryState currState = ClientTelemetryState.terminating_push_needed;

        List<ClientTelemetryState> validStates = new ArrayList<>();
        // Happy path...
        validStates.add(ClientTelemetryState.terminating_push_in_progress);

        // 'Forced shutdown w/o terminal push' case
        validStates.add(ClientTelemetryState.terminated);

        testValidateTransition(currState, validStates);
    }

    @Test
    public void testValidateTransitionForTerminatingPushInProgress() {
        ClientTelemetryState currState = ClientTelemetryState.terminating_push_in_progress;

        List<ClientTelemetryState> validStates = new ArrayList<>();
        // Happy path...
        validStates.add(ClientTelemetryState.terminated);

        testValidateTransition(currState, validStates);
    }

    @Test
    public void testValidateTransitionForTerminated() {
        ClientTelemetryState currState = ClientTelemetryState.terminated;

        // There's no transitioning out of the terminated state
        testValidateTransition(currState, Collections.emptyList());
    }

    private void testValidateTransition(ClientTelemetryState oldState, List<ClientTelemetryState> validStates) {
        for (ClientTelemetryState newState : validStates)
            oldState.validateTransition(newState);

        // Have to copy to a new list because asList returns an unmodifiable list
        List<ClientTelemetryState> invalidStates = new ArrayList<>(Arrays.asList(
            ClientTelemetryState.values()));

        // Remove the valid states from the list of all states, leaving only the invalid
        invalidStates.removeAll(validStates);

        for (ClientTelemetryState newState : invalidStates) {
            Executable e = () -> oldState.validateTransition(newState);
            String unexpectedSuccessMessage = "Should have thrown an IllegalTelemetryStateException for transitioning from " + oldState + " to " + newState;
            assertThrows(IllegalClientTelemetryStateException.class, e, unexpectedSuccessMessage);
        }
    }

}
