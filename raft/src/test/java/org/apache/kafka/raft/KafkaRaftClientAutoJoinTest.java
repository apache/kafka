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
import org.apache.kafka.common.record.MemoryRecords;

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

        context.advanceTimeAndFetchToUpdateVoterSetTimer(epoch, leader.id());

        context.time.sleep(context.fetchTimeoutMs - 1);
        context.pollUntilRequest();
        final var removeRequest = context.assertSentRemoveVoterRequest(oldFollower);
        context.deliverResponse(
            removeRequest.correlationId(),
            removeRequest.destination(),
            RaftUtil.removeVoterResponse(Errors.NONE, Errors.NONE.message())
        );

        // after sending a remove voter the next request should be a fetch
        context.pollUntilRequest();
        final var fetchRequest = context.assertSentFetchRequest();
        context.assertFetchRequestData(fetchRequest, epoch, 0L, 0);
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

        context.advanceTimeAndFetchToUpdateVoterSetTimer(epoch, leader.id());

        context.time.sleep(context.fetchTimeoutMs - 1);
        context.pollUntilRequest();
        final var addRequest = context.assertSentAddVoterRequest(
            newVoter,
            context.client.quorum().localVoterNodeOrThrow().listeners()
        );
        context.deliverResponse(
            addRequest.correlationId(),
            addRequest.destination(),
            RaftUtil.addVoterResponse(Errors.NONE, Errors.NONE.message())
        );

        // after sending an add voter the next request should be a fetch
        context.pollUntilRequest();
        final var fetchRequest = context.assertSentFetchRequest();
        context.assertFetchRequestData(fetchRequest, epoch, 0L, 0);
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
        context.advanceTimeAndFetchToUpdateVoterSetTimer(epoch, leader.id());
        context.time.sleep(context.fetchTimeoutMs - 1);
        context.pollUntilRequest();
        final var removeRequest = context.assertSentRemoveVoterRequest(oldFollower);
        context.deliverResponse(
            removeRequest.correlationId(),
            removeRequest.destination(),
            RaftUtil.removeVoterResponse(Errors.NONE, Errors.NONE.message())
        );

        // after sending a remove voter the next request should be a fetch
        context.pollUntilRequest();
        final var removeVoterFetch = context.assertSentFetchRequest();
        context.assertFetchRequestData(
            removeVoterFetch,
            epoch,
            context.log.endOffset().offset(),
            context.log.lastFetchedEpoch()
        );

        // deliver the fetch response with the updated voter set after removing the old voter
        var localEndOffset = context.log.endOffset().offset();
        context.deliverResponse(
            removeVoterFetch.correlationId(),
            removeVoterFetch.destination(),
            context.fetchResponse(
                epoch,
                leader.id(),
                MemoryRecords.withVotersRecord(
                    localEndOffset,
                    0,
                    epoch,
                    BufferSupplier.NO_CACHING.get(300),
                    VoterSetTest.voterSet(Stream.of(leader)).toVotersRecord((short) 0)),
                localEndOffset + 1,
                Errors.NONE
            )
        );
        // poll kraft to update the replica's voter set
        context.client.poll();

        // advance time and complete a fetch to trigger the add voter request
        context.advanceTimeAndFetchToUpdateVoterSetTimer(epoch, leader.id());
        context.time.sleep(context.fetchTimeoutMs - 1);
        context.pollUntilRequest();
        final var addVoterRequest = context.assertSentAddVoterRequest(
            newFollowerKey,
            context.client.quorum().localVoterNodeOrThrow().listeners()
        );
        context.deliverResponse(
            addVoterRequest.correlationId(),
            addVoterRequest.destination(),
            RaftUtil.addVoterResponse(Errors.NONE, Errors.NONE.message())
        );

        // after sending an add voter the next request should be a fetch
        context.pollUntilRequest();
        final var addVoterFetch = context.assertSentFetchRequest();
        context.assertFetchRequestData(
            addVoterFetch,
            epoch,
            context.log.endOffset().offset(),
            context.log.lastFetchedEpoch()
        );

        // deliver the fetch response with the updated voter set after adding the observer
        localEndOffset = context.log.endOffset().offset();
        context.deliverResponse(
            addVoterFetch.correlationId(),
            addVoterFetch.destination(),
            context.fetchResponse(
                epoch,
                leader.id(),
                MemoryRecords.withVotersRecord(
                    localEndOffset,
                    0,
                    epoch,
                    BufferSupplier.NO_CACHING.get(300),
                    VoterSetTest.voterSet(Stream.of(leader, newFollowerKey)).toVotersRecord((short) 0)),
                localEndOffset + 1,
                Errors.NONE
            )
        );
        // poll kraft to update the replica's voter set
        context.client.poll();
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

        context.advanceTimeAndFetchToUpdateVoterSetTimer(epoch, leader.id());

        context.time.sleep(context.fetchTimeoutMs - 1);
        context.pollUntilRequest();

        // When canBecomeVoter == false, the replica should not send an add voter request
        final var fetchRequest = context.assertSentFetchRequest();
        context.assertFetchRequestData(fetchRequest, epoch, 0L, 0);
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

        context.advanceTimeAndFetchToUpdateVoterSetTimer(epoch, leader.id());

        context.time.sleep(context.fetchTimeoutMs - 1);
        context.pollUntilRequest();

        // When autoJoin == false, the replica should not send an add voter request
        final var fetchRequest = context.assertSentFetchRequest();
        context.assertFetchRequestData(fetchRequest, epoch, 0L, 0);
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

        context.advanceTimeAndFetchToUpdateVoterSetTimer(epoch, leader.id());

        context.time.sleep(context.fetchTimeoutMs - 1);
        context.pollUntilRequest();

        // When kraft.version == 0, the replica should not send an add voter request
        final var fetchRequest = context.assertSentFetchRequest();

        context.assertFetchRequestData(fetchRequest, epoch, 0L, 0);
    }

    private int randomReplicaId() {
        return ThreadLocalRandom.current().nextInt(1025);
    }
}
