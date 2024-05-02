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

import org.apache.kafka.common.utils.Utils;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

/**
 * State that helps determine where client exists in the telemetry state i.e. subscribe->wait->push loop.
 */
public enum ClientTelemetryState {

    /**
     * Client needs subscription from the broker.
     */
    SUBSCRIPTION_NEEDED,

    /**
     * Network I/O is in progress to retrieve subscription.
     */
    SUBSCRIPTION_IN_PROGRESS,

    /**
     * Awaiting telemetry interval for pushing metrics to broker.
     */
    PUSH_NEEDED,

    /**
     * Network I/O in progress for pushing metrics payload.
     */
    PUSH_IN_PROGRESS,

    /**
     * Need to push the terminal metrics payload.
     */
    TERMINATING_PUSH_NEEDED,

    /**
     * Network I/O in progress for pushing terminal metrics payload.
     */
    TERMINATING_PUSH_IN_PROGRESS,

    /**
     * No more work should be performed, telemetry client terminated.
     */
    TERMINATED;

    private final static Map<ClientTelemetryState, List<ClientTelemetryState>> VALID_NEXT_STATES = new EnumMap<>(ClientTelemetryState.class);

    static {
        /*
         If clients needs a subscription, then issue telemetry API to fetch subscription from broker.

         However, it's still possible that client doesn't get very far before terminating.
        */
        VALID_NEXT_STATES.put(
            SUBSCRIPTION_NEEDED, Arrays.asList(SUBSCRIPTION_IN_PROGRESS, TERMINATED));

        /*
         If client is finished waiting for subscription, then client is ready to push the telemetry.
         But, it's possible that no telemetry metrics are requested, hence client should go back to
         subscription needed state i.e. requesting the next updated subscription.

         However, it's still possible that client doesn't get very far before terminating.
        */
        VALID_NEXT_STATES.put(SUBSCRIPTION_IN_PROGRESS, Arrays.asList(PUSH_NEEDED,
            SUBSCRIPTION_NEEDED, TERMINATING_PUSH_NEEDED, TERMINATED));

        /*
         If client transitions out of this state, then client should proceed to push the metrics.
         But, if the push fails (network issues, the subscription changed, etc.) then client should
         go back to subscription needed state and request the next subscription.

         However, it's still possible that client doesn't get very far before terminating.
        */
        VALID_NEXT_STATES.put(PUSH_NEEDED, Arrays.asList(PUSH_IN_PROGRESS, SUBSCRIPTION_NEEDED,
            TERMINATING_PUSH_NEEDED, TERMINATED));

        /*
         A successful push should transition client to push needed which sends the next telemetry
         metrics after the elapsed wait interval. But, if the push fails (network issues, the
         subscription changed, etc.) then client should go back to subscription needed state and
         request the next subscription.

         However, it's still possible that client doesn't get very far before terminating.
        */
        VALID_NEXT_STATES.put(
            PUSH_IN_PROGRESS, Arrays.asList(PUSH_NEEDED, SUBSCRIPTION_NEEDED, TERMINATING_PUSH_NEEDED,
                TERMINATED));

        /*
         If client is moving out of this state, then try to send last metrics push.

         However, it's still possible that client doesn't get very far before terminating.
        */
        VALID_NEXT_STATES.put(
            TERMINATING_PUSH_NEEDED, Arrays.asList(TERMINATING_PUSH_IN_PROGRESS, TERMINATED));

        /*
         Client should only be transited to terminated state.
        */
        VALID_NEXT_STATES.put(TERMINATING_PUSH_IN_PROGRESS, Collections.singletonList(TERMINATED));

        /*
         Client should never be able to transition out of terminated state.
        */
        VALID_NEXT_STATES.put(TERMINATED, Collections.emptyList());
    }

    /**
     * Validates that the <code>newState</code> is one of the valid transition from the current
     * {@code TelemetryState}.
     *
     * @param newState State into which the telemetry client requesting to transition; must be
     *                 non-<code>null</code>
     * @return {@code TelemetryState} <code>newState</code> if validation succeeds. Returning
     * <code>newState</code> helps state assignment chaining.
     * @throws IllegalStateException if the state transition validation fails.
     */

    public ClientTelemetryState validateTransition(ClientTelemetryState newState) {
        List<ClientTelemetryState> allowableStates = VALID_NEXT_STATES.get(this);

        if (allowableStates != null && allowableStates.contains(newState)) {
            return newState;
        }

        // State transition validation failed, construct error message and throw exception.
        String validStatesClause;
        if (allowableStates != null && !allowableStates.isEmpty()) {
            validStatesClause = String.format("the valid telemetry state transitions from %s are: %s",
                this,
                Utils.join(allowableStates, ", "));
        } else {
            validStatesClause = String.format("there are no valid telemetry state transitions from %s", this);
        }

        String message = String.format("Invalid telemetry state transition from %s to %s; %s",
            this,
            newState,
            validStatesClause);

        throw new IllegalStateException(message);
    }

}
