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

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.utils.Utils;

/**
 * State that helps determine where we are in the telemetry subscribe->wait->push loop.
 */
public enum ClientTelemetryState {

    subscription_needed,            // Subscription is needed from the broker
    subscription_in_progress,       // Network I/O in progress to retrieve subscription
    push_needed,                    // Awaiting timeout for pushing telemetry to broker
    push_in_progress,               // Network I/O in progress for pushing telemetry payload
    terminating_push_needed,        // Need to push the terminal payload
    terminating_push_in_progress,   // Network I/O in progress for pushing terminal telemetry payload
    terminated;                     // No more work should be performed

    private final static Map<ClientTelemetryState, List<ClientTelemetryState>> VALID_NEXT_STATES = new EnumMap<>(ClientTelemetryState.class);

    static {
        // If we need a subscription, the main thing we can do is request one.
        //
        // However, it's still possible that we don't get very far before terminating.
        VALID_NEXT_STATES.put(subscription_needed, Arrays.asList(subscription_in_progress, terminating_push_needed, terminated));

        // If we are finished awaiting our subscription, the most likely step is to next
        // push the telemetry. But, it's possible for there to be no telemetry requested,
        // at which point we would go back to waiting a bit before requesting the next
        // subscription.
        //
        // As before, it's possible that we don't get our response before we have to
        // terminate.
        VALID_NEXT_STATES.put(subscription_in_progress, Arrays.asList(push_needed, subscription_needed, terminating_push_needed, terminated));

        // If we are transitioning out of this state, chances are that we are doing so
        // because we want to push the telemetry. Alternatively, it's possible for the
        // push to fail (network issues, the subscription might have changed, etc.),
        // at which point we would again go back to waiting and requesting the next
        // subscription.
        //
        // But guess what? Yep - it's possible that we don't get to push before we have
        // to terminate.
        VALID_NEXT_STATES.put(push_needed, Arrays.asList(push_in_progress, subscription_needed, terminating_push_needed, terminated));

        // If we are transitioning out of this state, I'm guessing it's because we
        // did a successful push. We're going to want to sit tight before requesting
        // our subscription.
        //
        // But it's also possible that the push failed (again: network issues, the
        // subscription might have changed, etc.). We're not going to attempt to
        // re-push, but rather, take a breather and wait to request the
        // next subscription.
        //
        // So in either case, noting that we're now waiting for a subscription is OK.
        //
        // Again, it's possible that we don't get our response before we have to terminate.
        VALID_NEXT_STATES.put(push_in_progress, Arrays.asList(subscription_needed, terminating_push_needed, terminated));

        // If we are moving out of this state, we are hopefully doing so because we're
        // going to try to send our last push. Either that or we want to be fully
        // terminated.
        VALID_NEXT_STATES.put(terminating_push_needed, Arrays.asList(terminated, terminating_push_in_progress, terminated));

        // If we are done in this state, we should only be transitioning to fully
        // terminated.
        VALID_NEXT_STATES.put(terminating_push_in_progress, Collections.singletonList(terminated));

        // We should never be able to transition out of this state...
        VALID_NEXT_STATES.put(terminated, Collections.emptyList());
    }

    /**
     * Validates that the <code>newState</code> parameter value is one of the options in
     * the current {@code TelemetryState}.
     *
     * @param newState State into which the telemetry is trying to transition; must be
     *                 non-<code>null</code>
     * @return {@code TelemetryState} that is <code>newState</code>; this is done for assignment
     * chaining
     * @throws IllegalClientTelemetryStateException if the state transition isn't valid
     */

    public ClientTelemetryState validateTransition(ClientTelemetryState newState) {
        List<ClientTelemetryState> allowableStates = VALID_NEXT_STATES.get(this);

        if (allowableStates != null && allowableStates.contains(newState))
            return newState;

        // We didn't find a match above, so now we're just formatting a nice error message...
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

        throw new IllegalClientTelemetryStateException(message);
    }

}
