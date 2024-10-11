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

import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class FollowerStateTest {
    private final MockTime time = new MockTime();
    private final LogContext logContext = new LogContext();
    private final int epoch = 5;
    private final int fetchTimeoutMs = 15000;
    private final int leaderId = 3;
    private final Endpoints leaderEndpoints = Endpoints.fromInetSocketAddresses(
        Collections.singletonMap(
            ListenerName.normalised("CONTROLLER"),
            InetSocketAddress.createUnresolved("mock-host-3", 1234)
        )
    );

    private FollowerState newFollowerState(
        Set<Integer> voters,
        Optional<LogOffsetMetadata> highWatermark
    ) {
        return new FollowerState(
            time,
            epoch,
            leaderId,
            leaderEndpoints,
            voters,
            highWatermark,
            fetchTimeoutMs,
            logContext
        );
    }

    @Test
    public void testFetchTimeoutExpiration() {
        FollowerState state = newFollowerState(Set.of(1, 2, 3), Optional.empty());

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
        FollowerState state = newFollowerState(Set.of(1, 2, 3), Optional.empty());

        OptionalLong highWatermark = OptionalLong.of(15L);
        state.updateHighWatermark(highWatermark);
        assertThrows(IllegalArgumentException.class, () -> state.updateHighWatermark(OptionalLong.empty()));
        assertThrows(IllegalArgumentException.class, () -> state.updateHighWatermark(OptionalLong.of(14L)));
        state.updateHighWatermark(highWatermark);
        assertEquals(Optional.of(new LogOffsetMetadata(15L)), state.highWatermark());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testGrantVote(boolean isLogUpToDate) {
        FollowerState state = newFollowerState(
            Set.of(1, 2, 3),
            Optional.empty()
        );

        assertFalse(state.canGrantVote(ReplicaKey.of(1, ReplicaKey.NO_DIRECTORY_ID), isLogUpToDate));
        assertFalse(state.canGrantVote(ReplicaKey.of(2, ReplicaKey.NO_DIRECTORY_ID), isLogUpToDate));
        assertFalse(state.canGrantVote(ReplicaKey.of(3, ReplicaKey.NO_DIRECTORY_ID), isLogUpToDate));
    }

    @Test
    public void testLeaderIdAndEndpoint() {
        FollowerState state = newFollowerState(Set.of(0, 1, 2), Optional.empty());

        assertEquals(leaderId, state.leaderId());
        assertEquals(leaderEndpoints, state.leaderEndpoints());
    }
}
