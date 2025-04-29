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
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;

import static org.apache.kafka.raft.KafkaRaftClientTest.replicaKey;

public class KafkaRaftClientAutoJoinTest {
    private static final int NUMBER_FETCH_TIMEOUTS_IN_ADD_REMOVE_PERIOD = 1;

    @Test
    public void testAutoRemoveOldVoter() throws Exception {
        var leader = replicaKey(randomReplicaId(), true);
        var oldFollower = replicaKey(leader.id() + 1, true);
        var newFollowerKey = ReplicaKey.of(oldFollower.id(), Uuid.ONE_UUID);
        int epoch = 1;

        var voters = VoterSetTest.voterSet(Stream.of(leader, oldFollower));

        var context = new RaftClientTestContext.Builder(newFollowerKey.id(), newFollowerKey.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .withElectedLeader(epoch, leader.id())
            .withAutoJoinEnabled(true)
            .withAlwaysFlush(true)
            .build();

        completeFetch(context, epoch, newFollowerKey.id());

        context.time.sleep(context.fetchTimeoutMs - 1);
        context.pollUntilRequest();
        var removeRequest = context.assertSentRemoveVoterRequest(oldFollower);
        context.deliverResponse(
            removeRequest.correlationId(),
            removeRequest.destination(),
            RaftUtil.removeVoterResponse(Errors.NONE, Errors.NONE.message())
        );

        // after sending a remove voter the next request should be a fetch
        context.pollUntilRequest();
        var fetchRequest = context.assertSentFetchRequest();
        context.assertFetchRequestData(fetchRequest, epoch, 0L, 0);
    }

    @Test
    public void testAutoAddNewVoter() throws Exception {
        var leader = replicaKey(randomReplicaId(), true);
        var follower = replicaKey(leader.id() + 1, true);
        var newVoter = replicaKey(follower.id() + 1, true);
        int epoch = 1;

        var voters = VoterSetTest.voterSet(Stream.of(leader, follower));

        var context = new RaftClientTestContext.Builder(newVoter.id(), newVoter.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .withElectedLeader(epoch, leader.id())
            .withAutoJoinEnabled(true)
            .withAlwaysFlush(true)
            .build();

        completeFetch(context, epoch, newVoter.id());

        context.time.sleep(context.fetchTimeoutMs - 1);
        context.pollUntilRequest();
        var addRequest = context.assertSentAddVoterRequest(newVoter, context.client.quorum().localVoterNodeOrThrow().listeners());
        context.deliverResponse(
            addRequest.correlationId(),
            addRequest.destination(),
            RaftUtil.removeVoterResponse(Errors.NONE, Errors.NONE.message())
        );

        // after sending an add voter the next request should be a fetch
        context.pollUntilRequest();
        var fetchRequest = context.assertSentFetchRequest();
        context.assertFetchRequestData(fetchRequest, epoch, 0L, 0);
    }



    // Used to prevent fetch timer expiration when advancing time
    private void completeFetch(RaftClientTestContext context, int epoch, int fetchingId) throws Exception {
        // waiting for FETCH requests until the AddVoter request is sent
        for (int i = 0; i < NUMBER_FETCH_TIMEOUTS_IN_ADD_REMOVE_PERIOD; i++) {
            context.time.sleep(context.fetchTimeoutMs - 1);
            context.pollUntilRequest();
            var fetchRequest = context.assertSentFetchRequest();
            context.assertFetchRequestData(fetchRequest, epoch, 0L, 0);

            context.deliverResponse(
                fetchRequest.correlationId(),
                fetchRequest.destination(),
                context.fetchResponse(
                    epoch,
                    fetchingId,
                    MemoryRecords.EMPTY,
                    0L,
                    Errors.NONE
                )
            );
            // poll kraft to handle the fetch response
            context.client.poll();
        }
    }

    private int randomReplicaId() {
        return ThreadLocalRandom.current().nextInt(1025);
    }
}
