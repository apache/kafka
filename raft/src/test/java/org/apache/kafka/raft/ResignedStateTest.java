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

import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ResignedStateTest {

    private final MockTime time = new MockTime();
    private final LogContext logContext = new LogContext();
    int electionTimeoutMs = 5000;
    int localId = 0;
    int epoch = 5;

    private ResignedState newResignedState(
        Set<Integer> voters,
        List<Integer> preferredSuccessors
    ) {
        return new ResignedState(
            time,
            localId,
            epoch,
            voters,
            electionTimeoutMs,
            preferredSuccessors,
            logContext
        );
    }

    @Test
    public void testResignedState() {
        int remoteId = 1;
        Set<Integer> voters = Utils.mkSet(localId, remoteId);

        ResignedState state = newResignedState(voters, Collections.emptyList());

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
        ResignedState state = newResignedState(
            Utils.mkSet(1, 2, 3),
            Collections.emptyList()
        );

        assertFalse(state.canGrantVote(1, isLogUpToDate));
        assertFalse(state.canGrantVote(2, isLogUpToDate));
        assertFalse(state.canGrantVote(3, isLogUpToDate));
    }
}
