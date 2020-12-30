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
package org.apache.kafka.raft;

import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Utils;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class FollowerStateTest {
    private final MockTime time = new MockTime();

    @Test
    public void testFetchTimeoutExpiration() {
        int epoch = 5;
        int leaderId = 3;
        int fetchTimeoutMs = 15000;

        FollowerState state = new FollowerState(
            time,
            epoch,
            leaderId,
            Utils.mkSet(1, 2, 3),
            Optional.empty(),
            fetchTimeoutMs
        );

        assertFalse(state.hasFetchTimeoutExpired(time.milliseconds()));
        assertEquals(fetchTimeoutMs, state.remainingFetchTimeMs(time.milliseconds()));

        time.sleep(5000);
        assertFalse(state.hasFetchTimeoutExpired(time.milliseconds()));
        assertEquals(fetchTimeoutMs - 5000, state.remainingFetchTimeMs(time.milliseconds()));

        time.sleep(10000);
        assertTrue(state.hasFetchTimeoutExpired(time.milliseconds()));
        assertEquals(0, state.remainingFetchTimeMs(time.milliseconds()));
    }

    @Test
    public void testMonotonicHighWatermark() {
        int epoch = 5;
        int leaderId = 3;
        int fetchTimeoutMs = 15000;

        FollowerState state = new FollowerState(
            time,
            epoch,
            leaderId,
            Utils.mkSet(1, 2, 3),
            Optional.empty(),
            fetchTimeoutMs
        );

        OptionalLong highWatermark = OptionalLong.of(15L);
        state.updateHighWatermark(highWatermark);
        assertThrows(IllegalArgumentException.class, () -> state.updateHighWatermark(OptionalLong.empty()));
        assertThrows(IllegalArgumentException.class, () -> state.updateHighWatermark(OptionalLong.of(14L)));
        state.updateHighWatermark(highWatermark);
        assertEquals(Optional.of(new LogOffsetMetadata(15L)), state.highWatermark());
    }

}
