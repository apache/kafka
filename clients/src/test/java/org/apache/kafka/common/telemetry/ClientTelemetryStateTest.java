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
package org.apache.kafka.common.telemetry;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class ClientTelemetryStateTest {

    @Test
    public void testValidateTransitionForSubscriptionNeeded() {
        List<ClientTelemetryState> validStates = new ArrayList<>();
        validStates.add(ClientTelemetryState.SUBSCRIPTION_IN_PROGRESS);
        validStates.add(ClientTelemetryState.TERMINATED);

        testValidateTransition(ClientTelemetryState.SUBSCRIPTION_NEEDED, validStates);
    }

    @Test
    public void testValidateTransitionForSubscriptionInProgress() {
        List<ClientTelemetryState> validStates = new ArrayList<>();
        validStates.add(ClientTelemetryState.PUSH_NEEDED);
        validStates.add(ClientTelemetryState.SUBSCRIPTION_NEEDED);
        validStates.add(ClientTelemetryState.TERMINATING_PUSH_NEEDED);
        validStates.add(ClientTelemetryState.TERMINATED);

        testValidateTransition(ClientTelemetryState.SUBSCRIPTION_IN_PROGRESS, validStates);
    }

    @Test
    public void testValidateTransitionForPushNeeded() {
        List<ClientTelemetryState> validStates = new ArrayList<>();
        validStates.add(ClientTelemetryState.PUSH_IN_PROGRESS);
        validStates.add(ClientTelemetryState.SUBSCRIPTION_NEEDED);
        validStates.add(ClientTelemetryState.TERMINATING_PUSH_NEEDED);
        validStates.add(ClientTelemetryState.TERMINATED);

        testValidateTransition(ClientTelemetryState.PUSH_NEEDED, validStates);
    }

    @Test
    public void testValidateTransitionForPushInProgress() {
        List<ClientTelemetryState> validStates = new ArrayList<>();
        validStates.add(ClientTelemetryState.PUSH_NEEDED);
        validStates.add(ClientTelemetryState.SUBSCRIPTION_NEEDED);
        validStates.add(ClientTelemetryState.TERMINATING_PUSH_NEEDED);
        validStates.add(ClientTelemetryState.TERMINATED);

        testValidateTransition(ClientTelemetryState.PUSH_IN_PROGRESS, validStates);
    }

    @Test
    public void testValidateTransitionForTerminating() {
        List<ClientTelemetryState> validStates = new ArrayList<>();
        validStates.add(ClientTelemetryState.TERMINATING_PUSH_IN_PROGRESS);
        validStates.add(ClientTelemetryState.TERMINATED);

        testValidateTransition(ClientTelemetryState.TERMINATING_PUSH_NEEDED, validStates);
    }

    @Test
    public void testValidateTransitionForTerminatingPushInProgress() {
        testValidateTransition(ClientTelemetryState.TERMINATING_PUSH_IN_PROGRESS,
            Collections.singletonList(ClientTelemetryState.TERMINATED));
    }

    @Test
    public void testValidateTransitionForTerminated() {
        // There's no transitioning out of the terminated state
        testValidateTransition(ClientTelemetryState.TERMINATED, Collections.emptyList());
    }

    private void testValidateTransition(ClientTelemetryState oldState, List<ClientTelemetryState> validStates) {
        for (ClientTelemetryState newState : validStates) {
            oldState.validateTransition(newState);
        }

        // Copy value to a new list for modification.
        List<ClientTelemetryState> invalidStates = new ArrayList<>(Arrays.asList(ClientTelemetryState.values()));
        // Remove the valid states from the list of all states, leaving only the invalid.
        invalidStates.removeAll(validStates);

        for (ClientTelemetryState newState : invalidStates) {
            Executable e = () -> oldState.validateTransition(newState);
            String unexpectedSuccessMessage = "Should have thrown an IllegalTelemetryStateException for transitioning from " + oldState + " to " + newState;
            assertThrows(IllegalStateException.class, e, unexpectedSuccessMessage);
        }
    }

}
