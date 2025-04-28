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
import org.apache.kafka.common.message.AddRaftVoterRequestData;
import org.apache.kafka.common.message.RemoveRaftVoterRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;

import static org.apache.kafka.raft.KafkaRaftClientTest.replicaKey;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class KafkaRaftClientAutoJoinTest {

    @Test
    public void testRemoveOldVoter() throws Exception {
        ReplicaKey leader = replicaKey(randomReplicaId(), true);
        ReplicaKey oldFollower = replicaKey(leader.id() + 1, true);
        ReplicaKey newFollowerKey = ReplicaKey.of(oldFollower.id(), Uuid.ONE_UUID);
        int epoch = 1;

        VoterSet voters = VoterSetTest.voterSet(Stream.of(leader, oldFollower));

        RaftClientTestContext context = new RaftClientTestContext.Builder(newFollowerKey.id(), newFollowerKey.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .withElectedLeader(epoch, leader.id())
            .withAutoJoinEnabled(true)
            .withAlwaysFlush(true)
            .build();
        context.time.sleep(2 * context.fetchTimeoutMs);
        FollowerState state = context.client.quorum().followerStateOrThrow();
        state.resetFetchTimeoutForSuccessfulFetch(context.time.milliseconds());
        context.pollUntilRequest();
        RemoveRaftVoterRequestData removeVoterRequest = (RemoveRaftVoterRequestData) context.channel.drainSentRequests(Optional.of(ApiKeys.REMOVE_RAFT_VOTER)).get(0).data();
        assertEquals(oldFollower.id(), removeVoterRequest.voterId());
        assertEquals(oldFollower.directoryId().get(), removeVoterRequest.voterDirectoryId());
    }

    @Test
    public void testAddNewVoter() throws Exception {
        ReplicaKey leader = replicaKey(randomReplicaId(), true);
        ReplicaKey follower = replicaKey(leader.id() + 1, true);
        ReplicaKey newVoter = replicaKey(follower.id() + 1, true);
        int epoch = 1;

        VoterSet voters = VoterSetTest.voterSet(Stream.of(leader, follower));

        RaftClientTestContext context = new RaftClientTestContext.Builder(newVoter.id(), newVoter.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .withElectedLeader(epoch, leader.id())
            .withAutoJoinEnabled(true)
            .withAlwaysFlush(true)
            .build();
        context.time.sleep(2 * context.fetchTimeoutMs);
        FollowerState state = context.client.quorum().followerStateOrThrow();
        state.resetFetchTimeoutForSuccessfulFetch(context.time.milliseconds());
        context.pollUntilRequest();
        AddRaftVoterRequestData addVoterRequest = (AddRaftVoterRequestData) context.channel.drainSentRequests(Optional.of(ApiKeys.ADD_RAFT_VOTER)).get(0).data();
        assertEquals(newVoter.id(), addVoterRequest.voterId());
        assertEquals(newVoter.directoryId().get(), addVoterRequest.voterDirectoryId());
        // TODO: check listeners if possible
    }

    private int randomReplicaId() {
        return ThreadLocalRandom.current().nextInt(1025);
    }
}
