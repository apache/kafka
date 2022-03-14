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

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

public class TelemetryStateTest extends BaseClientTelemetryTest {

    @Test
    public void testValidateTransitionForSubscriptionNeeded() {
        TelemetryState currState = TelemetryState.subscription_needed;

        List<TelemetryState> validStates = new ArrayList<>();
        // Happy path...
        validStates.add(TelemetryState.subscription_in_progress);

        // 'Start shutdown w/o having done anything' case
        validStates.add(TelemetryState.terminating_push_needed);

        // 'Shutdown w/o a terminal push' case
        validStates.add(TelemetryState.terminated);

        testValidateTransition(currState, validStates);
    }

    @Test
    public void testValidateTransitionForSubscriptionInProgress() {
        TelemetryState currState = TelemetryState.subscription_in_progress;

        List<TelemetryState> validStates = new ArrayList<>();
        // Happy path...
        validStates.add(TelemetryState.push_needed);

        // 'Subscription had errors or requested/matches no metrics' case
        validStates.add(TelemetryState.subscription_needed);

        // 'Start shutdown while waiting for the subscription' case
        validStates.add(TelemetryState.terminating_push_needed);

        // 'Shutdown w/o a terminal push' case
        validStates.add(TelemetryState.terminated);

        testValidateTransition(currState, validStates);
    }

    @Test
    public void testValidateTransitionForPushNeeded() {
        TelemetryState currState = TelemetryState.push_needed;

        List<TelemetryState> validStates = new ArrayList<>();
        // Happy path...
        validStates.add(TelemetryState.push_in_progress);

        // 'Attempt to send push request failed (maybe a network issue?), so loop back to getting
        // the subscription' case
        validStates.add(TelemetryState.subscription_needed);

        // 'Start shutdown while waiting for a telemetry push' case
        validStates.add(TelemetryState.terminating_push_needed);

        // 'Shutdown w/o a terminal push' case
        validStates.add(TelemetryState.terminated);

        testValidateTransition(currState, validStates);
    }

    @Test
    public void testValidateTransitionForPushInProgress() {
        TelemetryState currState = TelemetryState.push_in_progress;

        List<TelemetryState> validStates = new ArrayList<>();
        // Happy path...
        validStates.add(TelemetryState.subscription_needed);

        // 'Start shutdown while we happen to be pushing telemetry' case
        validStates.add(TelemetryState.terminating_push_needed);

        // 'Shutdown w/o a terminal push' case
        validStates.add(TelemetryState.terminated);

        testValidateTransition(currState, validStates);
    }

    @Test
    public void testValidateTransitionForTerminating() {
        TelemetryState currState = TelemetryState.terminating_push_needed;

        List<TelemetryState> validStates = new ArrayList<>();
        // Happy path...
        validStates.add(TelemetryState.terminating_push_in_progress);

        // 'Forced shutdown w/o terminal push' case
        validStates.add(TelemetryState.terminated);

        testValidateTransition(currState, validStates);
    }

    @Test
    public void testValidateTransitionForTerminatingPushInProgress() {
        TelemetryState currState = TelemetryState.terminating_push_in_progress;

        List<TelemetryState> validStates = new ArrayList<>();
        // Happy path...
        validStates.add(TelemetryState.terminated);

        testValidateTransition(currState, validStates);
    }

    @Test
    public void testValidateTransitionForTerminated() {
        TelemetryState currState = TelemetryState.terminated;

        // There's no transitioning out of the terminated state
        testValidateTransition(currState, Collections.emptyList());
    }

    private void testValidateTransition(TelemetryState oldState, List<TelemetryState> validStates) {
        for (TelemetryState newState : validStates)
            oldState.validateTransition(newState);

        // Have to copy to a new list because asList returns an unmodifiable list
        List<TelemetryState> invalidStates = new ArrayList<>(Arrays.asList(TelemetryState.values()));

        // Remove the valid states from the list of all states, leaving only the invalid
        invalidStates.removeAll(validStates);

        for (TelemetryState newState : invalidStates) {
            Executable e = () -> oldState.validateTransition(newState);
            String unexpectedSuccessMessage = "Should have thrown an IllegalTelemetryStateException for transitioning from " + oldState + " to " + newState;
            assertThrows(IllegalTelemetryStateException.class, e, unexpectedSuccessMessage);
        }
    }

}
