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

import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.server.common.KRaftVersion;

import org.junit.jupiter.api.Test;

import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;

import static org.apache.kafka.raft.KafkaRaftClientTest.replicaKey;
import static org.apache.kafka.raft.RaftClientTestContext.RaftProtocol.KIP_595_PROTOCOL;
import static org.apache.kafka.raft.RaftClientTestContext.RaftProtocol.KIP_853_PROTOCOL;

public class KafkaRaftClientAutoJoinTest {
    @Test
    public void testAutoRemoveOldVoter() throws Exception {
        final var leader = replicaKey(randomReplicaId(), true);
        final var oldFollower = replicaKey(leader.id() + 1, true);
        final var newFollowerKey = replicaKey(oldFollower.id(), true);
        final int epoch = 1;
        final var context = new RaftClientTestContext.Builder(
            newFollowerKey.id(),
            newFollowerKey.directoryId().get()
        )
            .withRaftProtocol(KIP_853_PROTOCOL)
            .withStartingVoters(
                VoterSetTest.voterSet(Stream.of(leader, oldFollower)), KRaftVersion.KRAFT_VERSION_1
            )
            .withElectedLeader(epoch, leader.id())
            .withAutoJoin(true)
            .withCanBecomeVoter(true)
            .build();

        context.advanceTimeAndCompleteFetch(epoch, leader.id(), true);

        // the next request should be a remove voter request
        pollAndDeliverRemoveVoter(context, oldFollower);

        // after sending a remove voter the next request should be a fetch
        context.advanceTimeAndCompleteFetch(epoch, leader.id(), true);

        // the replica should send remove voter again because the fetch did not update the voter set
        pollAndDeliverRemoveVoter(context, oldFollower);
    }

    @Test
    public void testAutoAddNewVoter() throws Exception {
        final var leader = replicaKey(randomReplicaId(), true);
        final var follower = replicaKey(leader.id() + 1, true);
        final var newVoter = replicaKey(follower.id() + 1, true);
        final int epoch = 1;
        final var context = new RaftClientTestContext.Builder(
            newVoter.id(),
            newVoter.directoryId().get()
        )
            .withRaftProtocol(KIP_853_PROTOCOL)
            .withStartingVoters(
                VoterSetTest.voterSet(Stream.of(leader, follower)), KRaftVersion.KRAFT_VERSION_1
            )
            .withElectedLeader(epoch, leader.id())
            .withAutoJoin(true)
            .withCanBecomeVoter(true)
            .build();

        context.advanceTimeAndCompleteFetch(epoch, leader.id(), true);

        // the next request should be an add voter request
        pollAndSendAddVoter(context, newVoter);

        // expire the add voter request, the next request should be a fetch
        context.advanceTimeAndCompleteFetch(epoch, leader.id(), true);

        // the replica should send add voter again because the completed fetch
        // did not update the voter set, and its timer has expired
        final var addVoterRequest = pollAndSendAddVoter(context, newVoter);

        // deliver the add voter response, this is possible before a completed fetch because of KIP-1186
        context.deliverResponse(
            addVoterRequest.correlationId(),
            addVoterRequest.destination(),
            RaftUtil.addVoterResponse(Errors.NONE, Errors.NONE.message())
        );

        // verify the replica can perform a fetch to commit the new voter set
        pollAndDeliverFetchToUpdateVoterSet(
            context,
            epoch,
            VoterSetTest.voterSet(Stream.of(leader, newVoter))
        );
    }

    @Test
    public void testObserverRemovesOldVoterAndAutoJoins() throws Exception {
        final var leader = replicaKey(randomReplicaId(), true);
        final var oldFollower = replicaKey(leader.id() + 1, true);
        final var newFollowerKey = replicaKey(oldFollower.id(), true);
        final int epoch = 1;
        final var context = new RaftClientTestContext.Builder(
            newFollowerKey.id(),
            newFollowerKey.directoryId().get()
        )
            .withRaftProtocol(KIP_853_PROTOCOL)
            .withStartingVoters(
                VoterSetTest.voterSet(Stream.of(leader, oldFollower)), KRaftVersion.KRAFT_VERSION_1
            )
            .withElectedLeader(epoch, leader.id())
            .withAutoJoin(true)
            .withCanBecomeVoter(true)
            .build();

        // advance time and complete a fetch to trigger the remove voter request
        context.advanceTimeAndCompleteFetch(epoch, leader.id(), true);

        // the next request should be a remove voter request
        pollAndDeliverRemoveVoter(context, oldFollower);

        // after sending a remove voter the next request should be a fetch
        // this fetch will remove the old follower from the voter set
        pollAndDeliverFetchToUpdateVoterSet(
            context,
            epoch,
            VoterSetTest.voterSet(Stream.of(leader))
        );

        // advance time and complete a fetch to trigger the add voter request
        context.advanceTimeAndCompleteFetch(epoch, leader.id(), true);

        // the next request should be an add voter request
        final var addVoterRequest = pollAndSendAddVoter(context, newFollowerKey);

        // deliver the add voter response, this is possible before a completed fetch because of KIP-1186
        context.deliverResponse(
            addVoterRequest.correlationId(),
            addVoterRequest.destination(),
            RaftUtil.addVoterResponse(Errors.NONE, Errors.NONE.message())
        );

        // verify the replica can perform a fetch to commit the new voter set
        pollAndDeliverFetchToUpdateVoterSet(
            context,
            epoch,
            VoterSetTest.voterSet(Stream.of(leader, newFollowerKey))
        );

        // advance time and complete a fetch and expire the update voter set timer
        // the next request should be a fetch because the log voter configuration is up-to-date
        context.advanceTimeAndCompleteFetch(epoch, leader.id(), true);
        context.pollUntilRequest();
        context.assertSentFetchRequest();
    }


    @Test
    public void testObserversDoNotAutoJoin() throws Exception {
        final var leader = replicaKey(randomReplicaId(), true);
        final var follower = replicaKey(leader.id() + 1, true);
        final var newObserver = replicaKey(follower.id() + 1, true);
        final int epoch = 1;
        final var context = new RaftClientTestContext.Builder(
            newObserver.id(),
            newObserver.directoryId().get()
        )
            .withRaftProtocol(KIP_853_PROTOCOL)
            .withStartingVoters(
                VoterSetTest.voterSet(Stream.of(leader, follower)), KRaftVersion.KRAFT_VERSION_1
            )
            .withElectedLeader(epoch, leader.id())
            .withAutoJoin(true)
            .withCanBecomeVoter(false)
            .build();

        context.advanceTimeAndCompleteFetch(epoch, leader.id(), true);

        context.time.sleep(context.fetchTimeoutMs - 1);
        context.pollUntilRequest();

        // When canBecomeVoter == false, the replica should not send an add voter request
        final var fetchRequest = context.assertSentFetchRequest();
        context.assertFetchRequestData(fetchRequest, epoch, 0L, 0, context.client.highWatermark());
    }

    @Test
    public void testObserverDoesNotAddItselfWhenAutoJoinDisabled() throws Exception {
        final var leader = replicaKey(randomReplicaId(), true);
        final var follower = replicaKey(leader.id() + 1, true);
        final var observer = replicaKey(follower.id() + 1, true);
        final int epoch = 1;
        final var context = new RaftClientTestContext.Builder(
            observer.id(),
            observer.directoryId().get()
        )
            .withRaftProtocol(KIP_853_PROTOCOL)
            .withStartingVoters(
                VoterSetTest.voterSet(Stream.of(leader, follower)), KRaftVersion.KRAFT_VERSION_1
            )
            .withElectedLeader(epoch, leader.id())
            .withAutoJoin(false)
            .withCanBecomeVoter(true)
            .build();

        context.advanceTimeAndCompleteFetch(epoch, leader.id(), true);

        context.time.sleep(context.fetchTimeoutMs - 1);
        context.pollUntilRequest();

        // When autoJoin == false, the replica should not send an add voter request
        final var fetchRequest = context.assertSentFetchRequest();
        context.assertFetchRequestData(fetchRequest, epoch, 0L, 0, context.client.highWatermark());
    }

    @Test
    public void testObserverDoesNotAutoJoinWithKRaftVersion0() throws Exception {
        final var leader = replicaKey(randomReplicaId(), true);
        final var follower = replicaKey(leader.id() + 1, true);
        final var observer = replicaKey(follower.id() + 1, true);
        final int epoch = 1;
        final var context = new RaftClientTestContext.Builder(
            observer.id(),
            observer.directoryId().get()
        )
            .withRaftProtocol(KIP_595_PROTOCOL)
            .withStartingVoters(
                VoterSetTest.voterSet(Stream.of(leader, follower)), KRaftVersion.KRAFT_VERSION_0
            )
            .withElectedLeader(epoch, leader.id())
            .withAutoJoin(true)
            .withCanBecomeVoter(true)
            .build();

        context.advanceTimeAndCompleteFetch(epoch, leader.id(), true);

        context.time.sleep(context.fetchTimeoutMs - 1);
        context.pollUntilRequest();

        // When kraft.version == 0, the replica should not send an add voter request
        final var fetchRequest = context.assertSentFetchRequest();

        context.assertFetchRequestData(fetchRequest, epoch, 0L, 0, context.client.highWatermark());
    }

    private void pollAndDeliverRemoveVoter(
        RaftClientTestContext context,
        ReplicaKey oldFollower
    ) throws Exception {
        context.pollUntilRequest();
        final var removeRequest = context.assertSentRemoveVoterRequest(oldFollower);
        context.deliverResponse(
            removeRequest.correlationId(),
            removeRequest.destination(),
            RaftUtil.removeVoterResponse(Errors.NONE, Errors.NONE.message())
        );
    }

    private RaftRequest.Outbound pollAndSendAddVoter(
        RaftClientTestContext context,
        ReplicaKey newVoter
    ) throws Exception {
        context.pollUntilRequest();
        return context.assertSentAddVoterRequest(
            newVoter,
            context.client.quorum().localVoterNodeOrThrow().listeners()
        );
    }

    private void pollAndDeliverFetchToUpdateVoterSet(
        RaftClientTestContext context,
        int epoch,
        VoterSet newVoterSet
    ) throws Exception {
        context.pollUntilRequest();
        final var fetchRequest = context.assertSentFetchRequest();
        context.assertFetchRequestData(
            fetchRequest,
            epoch,
            context.log.endOffset().offset(),
            context.log.lastFetchedEpoch(),
            context.client.highWatermark()
        );
        // deliver the fetch response with the updated voter set
        context.deliverResponse(
            fetchRequest.correlationId(),
            fetchRequest.destination(),
            context.fetchResponse(
                epoch,
                fetchRequest.destination().id(),
                MemoryRecords.withVotersRecord(
                    context.log.endOffset().offset(),
                    context.time.milliseconds(),
                    epoch,
                    BufferSupplier.NO_CACHING.get(300),
                    newVoterSet.toVotersRecord((short) 0)
                ),
                context.log.endOffset().offset() + 1,
                Errors.NONE
            )
        );
        // poll kraft to update the replica's voter set
        context.client.poll();
    }

    private int randomReplicaId() {
        return ThreadLocalRandom.current().nextInt(1025);
    }
}
