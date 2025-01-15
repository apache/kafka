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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.Optional;
import java.util.OptionalInt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UnattachedStateWithVoteTest {

    private final MockTime time = new MockTime();
    private final LogContext logContext = new LogContext();
    private final int epoch = 5;
    private final int votedId = 1;
    private final int electionTimeoutMs = 10000;

    private UnattachedState newUnattachedVotedState(
        Uuid votedDirectoryId
    ) {
        return new UnattachedState(
            time,
            epoch,
            OptionalInt.empty(),
            Optional.of(ReplicaKey.of(votedId, votedDirectoryId)),
            Collections.emptySet(),
            Optional.empty(),
            electionTimeoutMs,
            logContext
        );
    }

    @Test
    public void testElectionTimeout() {
        UnattachedState state = newUnattachedVotedState(ReplicaKey.NO_DIRECTORY_ID);
        ReplicaKey votedKey  = ReplicaKey.of(votedId, ReplicaKey.NO_DIRECTORY_ID);

        assertEquals(epoch, state.epoch());
        assertEquals(votedKey, state.votedKey().get());
        assertEquals(
            ElectionState.withVotedCandidate(epoch, votedKey, Collections.emptySet()),
            state.election()
        );
        assertEquals(electionTimeoutMs, state.remainingElectionTimeMs(time.milliseconds()));
        assertFalse(state.hasElectionTimeoutExpired(time.milliseconds()));

        time.sleep(5000);
        assertEquals(electionTimeoutMs - 5000, state.remainingElectionTimeMs(time.milliseconds()));
        assertFalse(state.hasElectionTimeoutExpired(time.milliseconds()));

        time.sleep(5000);
        assertEquals(0, state.remainingElectionTimeMs(time.milliseconds()));
        assertTrue(state.hasElectionTimeoutExpired(time.milliseconds()));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testCanGrantVoteWithoutDirectoryId(boolean isLogUpToDate) {
        UnattachedState state = newUnattachedVotedState(ReplicaKey.NO_DIRECTORY_ID);

        assertEquals(
            isLogUpToDate,
            state.canGrantVote(ReplicaKey.of(votedId, ReplicaKey.NO_DIRECTORY_ID), isLogUpToDate, true)
        );
        assertTrue(state.canGrantVote(ReplicaKey.of(votedId, ReplicaKey.NO_DIRECTORY_ID), isLogUpToDate, false));

        assertEquals(
            isLogUpToDate,
            state.canGrantVote(ReplicaKey.of(votedId, Uuid.randomUuid()), isLogUpToDate, true)
        );
        assertTrue(state.canGrantVote(ReplicaKey.of(votedId, Uuid.randomUuid()), isLogUpToDate, false));

        // Can grant PreVote to other replicas even if we have granted a standard vote to another replica
        assertEquals(
            isLogUpToDate,
            state.canGrantVote(ReplicaKey.of(votedId + 1, ReplicaKey.NO_DIRECTORY_ID), isLogUpToDate, true)
        );
        assertFalse(state.canGrantVote(ReplicaKey.of(votedId + 1, ReplicaKey.NO_DIRECTORY_ID), isLogUpToDate, false));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testCanGrantVoteWithDirectoryId(boolean isLogUpToDate) {
        Uuid votedDirectoryId = Uuid.randomUuid();
        UnattachedState state = newUnattachedVotedState(votedDirectoryId);

        // Same voterKey
        // We can reject PreVote for a replica we have already granted a standard vote to if their log is behind
        assertEquals(
            isLogUpToDate,
            state.canGrantVote(ReplicaKey.of(votedId, votedDirectoryId), isLogUpToDate, true)
        );
        assertTrue(state.canGrantVote(ReplicaKey.of(votedId, votedDirectoryId), isLogUpToDate, false));

        // Different directoryId
        // We can grant PreVote for a replica we have already granted a standard vote to if their log is up-to-date,
        // even if the directoryId is different
        assertEquals(
            isLogUpToDate,
            state.canGrantVote(ReplicaKey.of(votedId, Uuid.randomUuid()), isLogUpToDate, true)
        );
        assertFalse(state.canGrantVote(ReplicaKey.of(votedId, Uuid.randomUuid()), isLogUpToDate, false));

        // Missing directoryId
        assertEquals(
            isLogUpToDate,
            state.canGrantVote(ReplicaKey.of(votedId, ReplicaKey.NO_DIRECTORY_ID), isLogUpToDate, true)
        );
        assertFalse(state.canGrantVote(ReplicaKey.of(votedId, ReplicaKey.NO_DIRECTORY_ID), isLogUpToDate, false));

        // Different voterId
        assertEquals(
            isLogUpToDate,
            state.canGrantVote(ReplicaKey.of(votedId + 1, votedDirectoryId), isLogUpToDate, true)
        );
        assertEquals(
            isLogUpToDate,
            state.canGrantVote(ReplicaKey.of(votedId + 1, ReplicaKey.NO_DIRECTORY_ID), isLogUpToDate, true)
        );
        assertFalse(state.canGrantVote(ReplicaKey.of(votedId + 1, votedDirectoryId), true, false));
        assertFalse(state.canGrantVote(ReplicaKey.of(votedId + 1, ReplicaKey.NO_DIRECTORY_ID), true, false));
    }

    @Test
    void testLeaderEndpoints() {
        Uuid votedDirectoryId = Uuid.randomUuid();
        UnattachedState state = newUnattachedVotedState(votedDirectoryId);

        assertEquals(Endpoints.empty(), state.leaderEndpoints());
    }
}
