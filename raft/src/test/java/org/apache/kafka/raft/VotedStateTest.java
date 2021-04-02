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
import org.apache.kafka.common.utils.Utils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class VotedStateTest {

    private final MockTime time = new MockTime();
    private final LogContext logContext = new LogContext();
    private final int epoch = 5;
    private final int votedId = 1;
    private final int electionTimeoutMs = 10000;

    private VotedState newVotedState(
        Set<Integer> voters,
        Optional<LogOffsetMetadata> highWatermark
    ) {
        return new VotedState(
            time,
            epoch,
            votedId,
            voters,
            highWatermark,
            electionTimeoutMs,
            logContext
        );
    }

    @Test
    public void testElectionTimeout() {
        Set<Integer> voters = Utils.mkSet(1, 2, 3);

        VotedState state = newVotedState(voters, Optional.empty());

        assertEquals(epoch, state.epoch());
        assertEquals(votedId, state.votedId());
        assertEquals(ElectionState.withVotedCandidate(epoch, votedId, voters), state.election());
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
    public void testGrantVote(boolean isLogUpToDate) {
        VotedState state = newVotedState(
            Utils.mkSet(1, 2, 3),
            Optional.empty()
        );

        assertTrue(state.canGrantVote(1, isLogUpToDate));
        assertFalse(state.canGrantVote(2, isLogUpToDate));
        assertFalse(state.canGrantVote(3, isLogUpToDate));
    }
}
