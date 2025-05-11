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
import org.junit.jupiter.params.provider.CsvSource;
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
    private final Set<Integer> voters = Set.of(1, 2, 3);
    private final ReplicaKey voter1Key = ReplicaKey.of(1, Uuid.randomUuid());
    private final ReplicaKey votedKey = voter1Key;

    private UnattachedState newUnattachedState(
        OptionalInt leaderId,
        Optional<ReplicaKey> votedKey
    ) {
        return new UnattachedState(
            time,
            epoch,
            leaderId,
            votedKey,
            voters,
            Optional.empty(),
            electionTimeoutMs,
            logContext
        );
    }

    @ParameterizedTest
    @CsvSource({ "true,false", "false,true", "false,false" })
    public void testElectionStateAndElectionTimeout(boolean hasVotedKey, boolean hasLeaderId) {
        OptionalInt leader = hasLeaderId ? OptionalInt.of(3) : OptionalInt.empty();
        Optional<ReplicaKey> votedKey = hasVotedKey ? Optional.of(this.votedKey) : Optional.empty();
        UnattachedState state = newUnattachedState(leader, votedKey);

        assertEquals(
            new ElectionState(epoch, leader, votedKey, voters),
            state.election()
        );
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
    public void testGrantVoteWithoutVotedKey(boolean isLogUpToDate) {
        UnattachedState state = newUnattachedState(OptionalInt.empty(), Optional.empty());

        assertEquals(
            isLogUpToDate,
            state.canGrantVote(voter1Key, isLogUpToDate, true)
        );
        assertEquals(
            isLogUpToDate,
            state.canGrantVote(voter1Key, isLogUpToDate, false)
        );

        assertEquals(
            isLogUpToDate,
            state.canGrantVote(ReplicaKey.of(2, ReplicaKey.NO_DIRECTORY_ID), isLogUpToDate, true)
        );
        assertEquals(
            isLogUpToDate,
            state.canGrantVote(ReplicaKey.of(2, ReplicaKey.NO_DIRECTORY_ID), isLogUpToDate, false)
        );

        assertEquals(
            isLogUpToDate,
            state.canGrantVote(ReplicaKey.of(3, ReplicaKey.NO_DIRECTORY_ID), isLogUpToDate, true)
        );
        assertEquals(
            isLogUpToDate,
            state.canGrantVote(ReplicaKey.of(3, ReplicaKey.NO_DIRECTORY_ID), isLogUpToDate, false)
        );

        assertEquals(
            isLogUpToDate,
            state.canGrantVote(ReplicaKey.of(10, ReplicaKey.NO_DIRECTORY_ID), isLogUpToDate, true)
        );
        assertEquals(
            isLogUpToDate,
            state.canGrantVote(ReplicaKey.of(10, ReplicaKey.NO_DIRECTORY_ID), isLogUpToDate, false)
        );
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testCanGrantVoteWithVotedKey(boolean isLogUpToDate) {
        UnattachedState state = newUnattachedState(OptionalInt.empty(), Optional.of(votedKey));

        // Same voterKey
        // Local can reject PreVote for a replica that local has already granted a standard vote to if their log is behind
        assertEquals(
            isLogUpToDate,
            state.canGrantVote(votedKey, isLogUpToDate, true)
        );
        assertTrue(state.canGrantVote(votedKey, isLogUpToDate, false));

        // Different directoryId
        // Local can grant PreVote for a replica that local has already granted a standard vote to if their log is up-to-date,
        // even if the directoryId is different
        assertEquals(
            isLogUpToDate,
            state.canGrantVote(ReplicaKey.of(votedKey.id(), Uuid.randomUuid()), isLogUpToDate, true)
        );
        assertFalse(state.canGrantVote(ReplicaKey.of(votedKey.id(), Uuid.randomUuid()), isLogUpToDate, false));

        // Missing directoryId
        assertEquals(
            isLogUpToDate,
            state.canGrantVote(ReplicaKey.of(votedKey.id(), ReplicaKey.NO_DIRECTORY_ID), isLogUpToDate, true)
        );
        assertFalse(state.canGrantVote(ReplicaKey.of(votedKey.id(), ReplicaKey.NO_DIRECTORY_ID), isLogUpToDate, false));

        // Different voterId
        assertEquals(
            isLogUpToDate,
            state.canGrantVote(ReplicaKey.of(2, votedKey.directoryId().get()), isLogUpToDate, true)
        );
        assertEquals(
            isLogUpToDate,
            state.canGrantVote(ReplicaKey.of(2, ReplicaKey.NO_DIRECTORY_ID), isLogUpToDate, true)
        );
        assertFalse(state.canGrantVote(ReplicaKey.of(2, votedKey.directoryId().get()), isLogUpToDate, false));
        assertFalse(state.canGrantVote(ReplicaKey.of(2, ReplicaKey.NO_DIRECTORY_ID), isLogUpToDate, false));

        // Observer
        assertEquals(
            isLogUpToDate,
            state.canGrantVote(ReplicaKey.of(10, ReplicaKey.NO_DIRECTORY_ID), isLogUpToDate, true)
        );
        assertFalse(state.canGrantVote(ReplicaKey.of(10, ReplicaKey.NO_DIRECTORY_ID), isLogUpToDate, false));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testGrantVoteWithLeader(boolean isLogUpToDate) {
        int leaderId = 3;
        UnattachedState state = newUnattachedState(OptionalInt.of(leaderId), Optional.empty());

        // Check that the leader is persisted if the leader is known
        assertEquals(ElectionState.withElectedLeader(epoch, leaderId, Optional.empty(), voters), state.election());

        // Check that the replica can grant PreVotes if the log is up-to-date, even if the last leader is known
        // This is because nodes in Unattached have not successfully fetched from the leader yet
        assertEquals(
            isLogUpToDate,
            state.canGrantVote(voter1Key, isLogUpToDate, true)
        );
        assertEquals(
            isLogUpToDate,
            state.canGrantVote(ReplicaKey.of(2, ReplicaKey.NO_DIRECTORY_ID), isLogUpToDate, true)
        );
        assertEquals(
            isLogUpToDate,
            state.canGrantVote(ReplicaKey.of(leaderId, ReplicaKey.NO_DIRECTORY_ID), isLogUpToDate, true)
        );
        assertEquals(
            isLogUpToDate,
            state.canGrantVote(ReplicaKey.of(10, ReplicaKey.NO_DIRECTORY_ID), isLogUpToDate, true)
        );

        // Check that the replica rejects all standard votes request if the leader is known
        assertFalse(state.canGrantVote(ReplicaKey.of(1, ReplicaKey.NO_DIRECTORY_ID), isLogUpToDate, false));
        assertFalse(state.canGrantVote(ReplicaKey.of(2, ReplicaKey.NO_DIRECTORY_ID), isLogUpToDate, false));
        assertFalse(state.canGrantVote(ReplicaKey.of(leaderId, ReplicaKey.NO_DIRECTORY_ID), isLogUpToDate, false));
        assertFalse(state.canGrantVote(ReplicaKey.of(10, ReplicaKey.NO_DIRECTORY_ID), isLogUpToDate, false));
    }

    @Test
    public void testLeaderEndpoints() {
        UnattachedState state = newUnattachedState(OptionalInt.of(3), Optional.of(this.votedKey));

        assertEquals(Endpoints.empty(), state.leaderEndpoints());
    }
}
