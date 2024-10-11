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

import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class UnattachedStateTest {

    private final MockTime time = new MockTime();
    private final LogContext logContext = new LogContext();
    private final int epoch = 5;
    private final int electionTimeoutMs = 10000;

    private UnattachedState newUnattachedState(
        Set<Integer> voters,
        OptionalInt leaderId
    ) {
        return new UnattachedState(
            time,
            epoch,
            leaderId,
            Optional.empty(),
            voters,
            Optional.empty(),
            electionTimeoutMs,
            logContext
        );
    }

    @Test
    public void testElectionTimeout() {
        Set<Integer> voters = Set.of(1, 2, 3);

        UnattachedState state = newUnattachedState(voters, OptionalInt.empty());

        assertEquals(epoch, state.epoch());

        assertEquals(ElectionState.withUnknownLeader(epoch, voters), state.election());
        assertEquals(electionTimeoutMs, state.remainingElectionTimeMs(time.milliseconds()));
        assertFalse(state.hasElectionTimeoutExpired(time.milliseconds()));

        time.sleep(electionTimeoutMs / 2);
        assertEquals(electionTimeoutMs / 2, state.remainingElectionTimeMs(time.milliseconds()));
        assertFalse(state.hasElectionTimeoutExpired(time.milliseconds()));

        time.sleep(electionTimeoutMs / 2);
        assertEquals(0, state.remainingElectionTimeMs(time.milliseconds()));
        assertTrue(state.hasElectionTimeoutExpired(time.milliseconds()));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testGrantVote(boolean isLogUpToDate) {
        UnattachedState state = newUnattachedState(Set.of(1, 2, 3), OptionalInt.empty());

        assertEquals(
            isLogUpToDate,
            state.canGrantVote(ReplicaKey.of(1, ReplicaKey.NO_DIRECTORY_ID), isLogUpToDate)
        );
        assertEquals(
            isLogUpToDate,
            state.canGrantVote(ReplicaKey.of(2, ReplicaKey.NO_DIRECTORY_ID), isLogUpToDate)
        );
        assertEquals(
            isLogUpToDate,
            state.canGrantVote(ReplicaKey.of(3, ReplicaKey.NO_DIRECTORY_ID), isLogUpToDate)
        );
    }

    @Test
    void testLeaderEndpoints() {
        UnattachedState state = newUnattachedState(Set.of(1, 2, 3), OptionalInt.empty());

        assertEquals(Endpoints.empty(), state.leaderEndpoints());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testUnattachedWithLeader(boolean isLogUpToDate) {
        int leaderId = 3;
        Set<Integer> voters = Set.of(1, 2, leaderId);

        UnattachedState state = newUnattachedState(voters, OptionalInt.of(leaderId));

        // Check that the leader is persisted if the leader is known
        assertEquals(ElectionState.withElectedLeader(epoch, leaderId, voters), state.election());

        // Check that the replica rejects all votes request if the leader is known
        assertFalse(state.canGrantVote(ReplicaKey.of(1, ReplicaKey.NO_DIRECTORY_ID), isLogUpToDate));
        assertFalse(state.canGrantVote(ReplicaKey.of(2, ReplicaKey.NO_DIRECTORY_ID), isLogUpToDate));
        assertFalse(state.canGrantVote(ReplicaKey.of(3, ReplicaKey.NO_DIRECTORY_ID), isLogUpToDate));
    }
}
