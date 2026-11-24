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
package org.apache.kafka.raft.internals;

import org.apache.kafka.common.message.AddRaftVoterResponseData;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.raft.Endpoints;
import org.apache.kafka.raft.ReplicaKey;

import org.junit.jupiter.api.Test;

import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AddVoterHandlerStateTest {

    @Test
    public void testSetLastOffsetOnce() {
        var time = new MockTime();
        var state = new AddVoterHandlerState(
            ReplicaKey.of(1, ReplicaKey.NO_DIRECTORY_ID),
            Endpoints.empty(),
            true,
            time.timer(1000)
        );

        assertTrue(state.lastOffset().isEmpty());
        state.setLastOffset(100L);
        assertEquals(OptionalLong.of(100L), state.lastOffset());
    }

    @Test
    public void testCannotOverrideLastOffset() {
        var time = new MockTime();
        var state = new AddVoterHandlerState(
            ReplicaKey.of(1, ReplicaKey.NO_DIRECTORY_ID),
            Endpoints.empty(),
            true,
            time.timer(1000)
        );

        state.setLastOffset(100L);
        assertThrows(IllegalStateException.class, () -> state.setLastOffset(200L));
    }

    @Test
    public void testExpectingApiResponseBeforeLastOffset() {
        var time = new MockTime();
        var state = new AddVoterHandlerState(
            ReplicaKey.of(1, ReplicaKey.NO_DIRECTORY_ID),
            Endpoints.empty(),
            true,
            time.timer(1000)
        );

        assertTrue(state.expectingApiResponse(1));
        assertFalse(state.expectingApiResponse(2));

        state.setLastOffset(100L);
        assertFalse(state.expectingApiResponse(1));
    }

    @Test
    public void testTimeUntilExpiration() {
        var time = new MockTime();
        var state = new AddVoterHandlerState(
            ReplicaKey.of(1, ReplicaKey.NO_DIRECTORY_ID),
            Endpoints.empty(),
            true,
            time.timer(1000)
        );

        assertEquals(1000, state.timeUntilOperationExpiration(time.milliseconds()));
        time.sleep(500);
        assertEquals(500, state.timeUntilOperationExpiration(time.milliseconds()));
        time.sleep(500);
        assertEquals(0, state.timeUntilOperationExpiration(time.milliseconds()));
    }

    @Test
    public void testCompleteFuture() {
        var time = new MockTime();
        var state = new AddVoterHandlerState(
            ReplicaKey.of(1, ReplicaKey.NO_DIRECTORY_ID),
            Endpoints.empty(),
            true,
            time.timer(1000)
        );

        var future = state.future().toCompletableFuture();
        assertFalse(future.isDone());

        var response = new AddRaftVoterResponseData().setErrorCode((short) 0);
        state.completeFuture(response);

        assertTrue(future.isDone());
        assertEquals(response, future.join());
    }

    @Test
    public void testGetters() {
        var time = new MockTime();
        var voterKey = ReplicaKey.of(1, ReplicaKey.NO_DIRECTORY_ID);
        var endpoints = Endpoints.empty();

        var state = new AddVoterHandlerState(voterKey, endpoints, true, time.timer(1000));

        assertEquals(voterKey, state.voterKey());
        assertEquals(endpoints, state.voterEndpoints());
        assertTrue(state.ackWhenCommitted());

        var stateNoAck = new AddVoterHandlerState(voterKey, endpoints, false, time.timer(1000));
        assertFalse(stateNoAck.ackWhenCommitted());
    }
}
