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

import org.apache.kafka.common.message.RemoveRaftVoterResponseData;
import org.apache.kafka.common.utils.MockTime;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RemoveVoterHandlerStateTest {

    @Test
    public void testLastOffset() {
        var time = new MockTime();
        var state = new RemoveVoterHandlerState(100L, time.timer(1000));

        assertEquals(100L, state.lastOffset());
    }

    @Test
    public void testTimeUntilExpiration() {
        var time = new MockTime();
        var state = new RemoveVoterHandlerState(100L, time.timer(1000));

        assertEquals(1000, state.timeUntilOperationExpiration(time.milliseconds()));
        time.sleep(500);
        assertEquals(500, state.timeUntilOperationExpiration(time.milliseconds()));
        time.sleep(500);
        assertEquals(0, state.timeUntilOperationExpiration(time.milliseconds()));
    }

    @Test
    public void testCompleteFuture() {
        var time = new MockTime();
        var state = new RemoveVoterHandlerState(100L, time.timer(1000));

        var future = state.future().toCompletableFuture();
        assertFalse(future.isDone());

        var response = new RemoveRaftVoterResponseData().setErrorCode((short) 0);
        state.completeFuture(response);

        assertTrue(future.isDone());
        assertEquals(response, future.join());
    }
}
