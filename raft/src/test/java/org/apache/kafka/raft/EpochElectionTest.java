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
import org.apache.kafka.raft.internals.EpochElection;

import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class EpochElectionTest {
    private final int voter1 = randomReplicaId();
    private final Set<ReplicaKey> voters = Set.of(
        ReplicaKey.of(voter1, Uuid.randomUuid()),
        ReplicaKey.of(voter1 + 1, Uuid.randomUuid()),
        ReplicaKey.of(voter1 + 2, Uuid.randomUuid())
    );
    @Test
    public void testStateOnInitialization() {
        EpochElection epochElection = new EpochElection(voters);

        assertEquals(voters, epochElection.unrecordedVoters());
        assertTrue(epochElection.grantingVoters().isEmpty());
        assertTrue(epochElection.rejectingVoters().isEmpty());
        assertFalse(epochElection.isVoteGranted());
        assertFalse(epochElection.isVoteRejected());
        assertFalse(epochElection.isGrantedVoter(voter1));
        assertFalse(epochElection.isRejectedVoter(voter1));
    }

    @Test
    public void testRecordGrantedVote() {
        EpochElection epochElection = new EpochElection(voters);

        assertTrue(epochElection.recordVote(voter1, true));
        assertEquals(1, epochElection.grantingVoters().size());
        assertTrue(epochElection.grantingVoters().contains(voter1));
        assertEquals(0, epochElection.rejectingVoters().size());
        assertEquals(2, epochElection.unrecordedVoters().size());
        assertTrue(epochElection.isGrantedVoter(voter1));
        assertFalse(epochElection.isRejectedVoter(voter1));
        assertFalse(epochElection.isVoteGranted());
        assertFalse(epochElection.isVoteRejected());

        // recording same id as granted
        assertFalse(epochElection.recordVote(voter1, true));
        assertTrue(epochElection.isGrantedVoter(voter1));
        assertFalse(epochElection.isVoteGranted());

        // recording majority as granted
        assertTrue(epochElection.recordVote(voter1 + 1, true));
        assertEquals(2, epochElection.grantingVoters().size());
        assertEquals(0, epochElection.rejectingVoters().size());
        assertEquals(1, epochElection.unrecordedVoters().size());
        assertTrue(epochElection.isGrantedVoter(voter1 + 1));
        assertFalse(epochElection.isRejectedVoter(voter1 + 1));
        assertTrue(epochElection.isVoteGranted());
        assertFalse(epochElection.isVoteRejected());
    }

    @Test
    public void testRecordRejectedVote() {
        EpochElection epochElection = new EpochElection(voters);

        assertTrue(epochElection.recordVote(voter1, false));
        assertEquals(0, epochElection.grantingVoters().size());
        assertEquals(1, epochElection.rejectingVoters().size());
        assertTrue(epochElection.rejectingVoters().contains(voter1));
        assertEquals(2, epochElection.unrecordedVoters().size());
        assertFalse(epochElection.isGrantedVoter(voter1));
        assertTrue(epochElection.isRejectedVoter(voter1));
        assertFalse(epochElection.isVoteGranted());
        assertFalse(epochElection.isVoteRejected());

        // recording same id as rejected
        assertFalse(epochElection.recordVote(voter1, false));
        assertFalse(epochElection.isGrantedVoter(voter1));
        assertFalse(epochElection.isVoteRejected());

        // recording majority as rejected
        assertTrue(epochElection.recordVote(voter1 + 1, false));
        assertEquals(0, epochElection.grantingVoters().size());
        assertEquals(2, epochElection.rejectingVoters().size());
        assertEquals(1, epochElection.unrecordedVoters().size());
        assertFalse(epochElection.isGrantedVoter(voter1 + 1));
        assertTrue(epochElection.isRejectedVoter(voter1 + 1));
        assertFalse(epochElection.isVoteGranted());
        assertTrue(epochElection.isVoteRejected());
    }

    @Test
    public void testOverWritingVote() {
        EpochElection epochElection = new EpochElection(voters);

        assertTrue(epochElection.recordVote(voter1, true));
        assertFalse(epochElection.recordVote(voter1, false));
        assertEquals(0, epochElection.grantingVoters().size());
        assertEquals(1, epochElection.rejectingVoters().size());
        assertTrue(epochElection.rejectingVoters().contains(voter1));
        assertFalse(epochElection.isGrantedVoter(voter1));
        assertTrue(epochElection.isRejectedVoter(voter1));
        assertFalse(epochElection.isVoteGranted());
        assertFalse(epochElection.isVoteRejected());

        assertTrue(epochElection.recordVote(voter1 + 2, false));
        assertFalse(epochElection.recordVote(voter1 + 2, true));
        assertEquals(1, epochElection.grantingVoters().size());
        assertEquals(1, epochElection.rejectingVoters().size());
        assertTrue(epochElection.grantingVoters().contains(voter1 + 2));
        assertTrue(epochElection.isGrantedVoter(voter1 + 2));
        assertFalse(epochElection.isRejectedVoter(voter1 + 2));
        assertFalse(epochElection.isVoteGranted());
        assertFalse(epochElection.isVoteRejected());
    }

    private static int randomReplicaId() {
        return ThreadLocalRandom.current().nextInt(1025);
    }
}
