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

import org.apache.kafka.common.utils.LogContext;
import org.junit.jupiter.api.Test;

import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RequestStateTest {
    @Test
    public void testRequestStateSimple() {
        RequestState state = new RequestState(
                new LogContext(),
                this.getClass().getSimpleName(),
                100,
                2,
                1000,
                0);

        // ensure not permitting consecutive requests
        assertTrue(state.canSendRequest(0));
        state.onSendAttempt(0);
        assertFalse(state.canSendRequest(0));
        state.onFailedAttempt(35);
        assertTrue(state.canSendRequest(135));
        state.onFailedAttempt(140);
        assertFalse(state.canSendRequest(200));
        // exponential backoff
        assertTrue(state.canSendRequest(340));

        // test reset
        state.reset();
        assertTrue(state.canSendRequest(200));
    }

    @Test
    public void testTrackInflightOnSuccessfulAttempt() {
        testTrackInflight(RequestState::onSuccessfulAttempt);
    }

    @Test
    public void testTrackInflightOnFailedAttempt() {
        testTrackInflight(RequestState::onFailedAttempt);
    }

    /**
     * In some cases, the network layer is <em>very</em> fast and can send out multiple requests within the same
     * millisecond timestamp.
     *
     * <p/>
     *
     * The previous logic for tracking inflight status used timestamps: if the timestamp from the last received
     * response was <em>less</em> than the timestamp from the last sent request, we'd interpret that as having an
     * inflight request. However, this approach would incorrectly return <code>false</code> from
     * {@link RequestState#requestInFlight()} if the two timestamps were <em>equal</em>.
     */
    private void testTrackInflight(BiConsumer<RequestState, Integer> onCompletedAttempt) {
        RequestState state = new RequestState(
                new LogContext(),
                this.getClass().getSimpleName(),
                100,
                2,
                1000,
                0);

        // This is just being paranoid...
        assertFalse(state.requestInFlight());

        // When we've sent a request, the flag should update from false to true.
        state.onSendAttempt(202);
        assertTrue(state.requestInFlight());

        // Now we've received the response.
        onCompletedAttempt.accept(state, 236);

        // When we've sent a second request with THE SAME TIMESTAMP as the previous request,
        // the flag should update from false to true.
        assertFalse(state.requestInFlight());
        state.onSendAttempt(236);
        assertTrue(state.requestInFlight());
    }
}
