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

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ResignedStateTest {

    private final MockTime time = new MockTime();
    private final LogContext logContext = new LogContext();
    int electionTimeoutMs = 5000;
    int localId = 0;
    int epoch = 5;
    Endpoints localEndpoints = Endpoints.fromInetSocketAddresses(
        Collections.singletonMap(
            VoterSetTest.DEFAULT_LISTENER_NAME,
            InetSocketAddress.createUnresolved("localhost", 1234)
        )
    );

    private ResignedState newResignedState(Set<Integer> voters) {
        return new ResignedState(
            time,
            localId,
            epoch,
            voters,
            electionTimeoutMs,
            Collections.emptyList(),
            localEndpoints,
            logContext
        );
    }

    @Test
    public void testResignedState() {
        int remoteId = 1;
        Set<Integer> voters = Set.of(localId, remoteId);

        ResignedState state = newResignedState(voters);

        assertEquals(ElectionState.withElectedLeader(epoch, localId, voters), state.election());
        assertEquals(epoch, state.epoch());

        assertEquals(Collections.singleton(remoteId), state.unackedVoters());
        state.acknowledgeResignation(remoteId);
        assertEquals(Collections.emptySet(), state.unackedVoters());

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
        ResignedState state = newResignedState(Set.of(1, 2, 3));

        assertEquals(
            isLogUpToDate,
            state.canGrantVote(ReplicaKey.of(1, ReplicaKey.NO_DIRECTORY_ID), isLogUpToDate, true)
        );
        assertEquals(
            isLogUpToDate,
            state.canGrantVote(ReplicaKey.of(2, ReplicaKey.NO_DIRECTORY_ID), isLogUpToDate, true)
        );
        assertEquals(
            isLogUpToDate,
            state.canGrantVote(ReplicaKey.of(3, ReplicaKey.NO_DIRECTORY_ID), isLogUpToDate, true)
        );

        assertFalse(state.canGrantVote(ReplicaKey.of(1, ReplicaKey.NO_DIRECTORY_ID), isLogUpToDate, false));
        assertFalse(state.canGrantVote(ReplicaKey.of(2, ReplicaKey.NO_DIRECTORY_ID), isLogUpToDate, false));
        assertFalse(state.canGrantVote(ReplicaKey.of(3, ReplicaKey.NO_DIRECTORY_ID), isLogUpToDate, false));
    }

    @Test
    void testNegativeScenarioAcknowledgeResignation() {
        Set<Integer> voters = Set.of(0, 1, 2, 3, 4, 5);

        ResignedState state = newResignedState(voters);

        assertEquals(ElectionState.withElectedLeader(epoch, 0, voters), state.election());
        assertEquals(epoch, state.epoch());

        // try non-existed voter must throw an exception
        assertThrows(IllegalArgumentException.class, () -> state.acknowledgeResignation(10));
    }

    @Test
    void testLeaderEndpoints() {
        ResignedState state = newResignedState(Set.of(1, 2, 3));

        assertEquals(localEndpoints, state.leaderEndpoints());
        assertNotEquals(Endpoints.empty(), state.leaderEndpoints());
    }
}
