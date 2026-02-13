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

import org.apache.kafka.common.Node;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.errors.InvalidUpdateVersionException;
import org.apache.kafka.common.feature.SupportedVersionRange;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.EndQuorumEpochResponseData;
import org.apache.kafka.common.message.KRaftVersionRecord;
import org.apache.kafka.common.message.LeaderChangeMessage;
import org.apache.kafka.common.message.SnapshotFooterRecord;
import org.apache.kafka.common.message.SnapshotHeaderRecord;
import org.apache.kafka.common.message.VotersRecord;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.record.internal.ControlRecordType;
import org.apache.kafka.common.record.internal.ControlRecordUtils;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.MemoryRecordsBuilder;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.record.internal.Records;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.server.common.Feature;
import org.apache.kafka.server.common.KRaftVersion;
import org.apache.kafka.snapshot.RecordsSnapshotReader;
import org.apache.kafka.snapshot.SnapshotReader;
import org.apache.kafka.snapshot.SnapshotWriterReaderTest;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mockito;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.kafka.raft.KafkaRaftClientTest.replicaKey;
import static org.apache.kafka.raft.RaftClientTestContext.RaftProtocol;
import static org.apache.kafka.snapshot.Snapshots.BOOTSTRAP_SNAPSHOT_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KafkaRaftClientReconfigTest {
    @Test
    public void testLeaderWritesBootstrapRecords() throws Exception {
        ReplicaKey local = replicaKey(randomReplicaId(), true);
        ReplicaKey follower = replicaKey(local.id() + 1, true);

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, follower));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .withUnknownLeader(0)
            .build();

        List<List<ControlRecord>> expectedBootstrapRecords = List.of(
            List.of(
                ControlRecord.of(
                    new SnapshotHeaderRecord()
                        .setVersion((short) 0)
                        .setLastContainedLogTimestamp(0)
                ),
                ControlRecord.of(
                    new KRaftVersionRecord()
                        .setVersion(ControlRecordUtils.KRAFT_VERSION_CURRENT_VERSION)
                        .setKRaftVersion((short) 1)
                ),
                ControlRecord.of(
                    voters.toVotersRecord(ControlRecordUtils.KRAFT_VOTERS_CURRENT_VERSION)
                )
            ),
            List.of(
                ControlRecord.of(
                    new SnapshotFooterRecord()
                        .setVersion((short) 0)
                )
            )
        );

        // check the bootstrap snapshot exists and contains the expected records
        assertEquals(BOOTSTRAP_SNAPSHOT_ID, context.log.latestSnapshotId().get());
        try (SnapshotReader<?> reader = RecordsSnapshotReader.of(
                context.log.latestSnapshot().get(),
                context.serde,
                BufferSupplier.NO_CACHING,
                KafkaRaftClient.MAX_BATCH_SIZE_BYTES,
                false,
                new LogContext()
            )
        ) {
            SnapshotWriterReaderTest.assertControlSnapshot(expectedBootstrapRecords, reader);
        }

        context.unattachedToLeader();

        // check if leader writes 3 bootstrap records to the log
        Records records = context.log.read(0, Isolation.UNCOMMITTED).records;
        RecordBatch batch = records.batches().iterator().next();
        assertTrue(batch.isControlBatch());
        Iterator<Record> recordIterator = batch.iterator();
        Record record = recordIterator.next();
        RaftClientTestContext.verifyLeaderChangeMessage(
            local.id(),
            List.of(local.id(), follower.id()),
            List.of(local.id(), follower.id()),
            record.key(),
            record.value()
        );
        record = recordIterator.next();
        verifyKRaftVersionRecord((short) 1, record.key(), record.value());
        record = recordIterator.next();
        verifyVotersRecord(voters, record.key(), record.value());
    }

    @Test
    public void testBootstrapCheckpointIsNotReturnedOnFetch() throws Exception {
        ReplicaKey local = replicaKey(randomReplicaId(), true);
        ReplicaKey follower = replicaKey(local.id() + 1, true);

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, follower));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .withUnknownLeader(0)
            .build();

        context.unattachedToLeader();
        int epoch = context.currentEpoch();

        // check that leader does not respond with bootstrap snapshot id when follower fetches offset 0
        context.deliverRequest(
            context.fetchRequest(
                epoch,
                follower,
                0,
                0,
                0
            )
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(local.id()));
    }

    @Test
    public void testLeaderDoesNotBootstrapRecordsWithKraftVersion0() throws Exception {
        ReplicaKey local = replicaKey(randomReplicaId(), true);
        ReplicaKey follower = replicaKey(local.id() + 1, true);

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, follower));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withStaticVoters(voters)
            .withUnknownLeader(0)
            .build();

        // check leader does not write bootstrap records to log
        context.unattachedToLeader();

        Records records = context.log.read(0, Isolation.UNCOMMITTED).records;
        RecordBatch batch = records.batches().iterator().next();
        assertTrue(batch.isControlBatch());
        Iterator<Record> recordIterator = batch.iterator();
        Record record = recordIterator.next();
        RaftClientTestContext.verifyLeaderChangeMessage(
            local.id(),
            List.of(local.id(), follower.id()),
            List.of(local.id(), follower.id()),
            record.key(),
            record.value()
        );
        assertFalse(recordIterator.hasNext());
    }

    @Test
    public void testFollowerDoesNotRequestLeaderBootstrapSnapshot() throws Exception {
        ReplicaKey local = replicaKey(randomReplicaId(), true);
        ReplicaKey leader = replicaKey(local.id() + 1, true);
        int epoch = 1;

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, leader));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .withElectedLeader(epoch, leader.id())
            .build();

        // check that follower will send fetch request to leader
        context.pollUntilRequest();
        RaftRequest.Outbound fetchRequest = context.assertSentFetchRequest();
        context.assertFetchRequestData(fetchRequest, epoch, 0L, 0, context.client.highWatermark());

        // check if leader response were to contain bootstrap snapshot id, follower would not send fetch snapshot request
        context.deliverResponse(
            fetchRequest.correlationId(),
            fetchRequest.destination(),
            context.snapshotFetchResponse(epoch, leader.id(), BOOTSTRAP_SNAPSHOT_ID, 0)
        );
        context.pollUntilRequest();
        fetchRequest = context.assertSentFetchRequest();
        context.assertFetchRequestData(fetchRequest, epoch, 0L, 0, context.client.highWatermark());
    }

    @Test
    public void testFollowerReadsKRaftBootstrapRecords() throws Exception {
        ReplicaKey local = replicaKey(randomReplicaId(), true);
        ReplicaKey leader = replicaKey(local.id() + 1, true);
        ReplicaKey follower = replicaKey(local.id() + 2, true);
        VoterSet voterSet = VoterSetTest.voterSet(Stream.of(local, leader));
        int epoch = 5;

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voterSet))
            .withElectedLeader(epoch, leader.id())
            .build();

        // check that follower will send fetch request to leader
        context.pollUntilRequest();
        RaftRequest.Outbound fetchRequest = context.assertSentFetchRequest();
        context.assertFetchRequestData(fetchRequest, epoch, 0L, 0, context.client.highWatermark());

        // check that before receiving bootstrap records from leader, follower is not in the voter set
        assertFalse(context.client.quorum().isVoter(follower));

        // leader sends batch with bootstrap records
        VoterSet leadersVoterSet = VoterSetTest.voterSet(
            Stream.concat(voterSet.voterKeys().stream(), Stream.of(follower))
        );
        ByteBuffer buffer = ByteBuffer.allocate(128);
        try (MemoryRecordsBuilder builder = new MemoryRecordsBuilder(
                buffer,
                RecordBatch.CURRENT_MAGIC_VALUE,
                Compression.NONE,
                TimestampType.CREATE_TIME,
                0, // baseOffset
                0, // logAppendTime
                RecordBatch.NO_PRODUCER_ID,
                RecordBatch.NO_PRODUCER_EPOCH,
                RecordBatch.NO_SEQUENCE,
                false, // isTransactional
                true, // isControlBatch
                epoch,
                buffer.capacity()
            )
        ) {
            builder.appendLeaderChangeMessage(
                0,
                new LeaderChangeMessage()
            );
            builder.appendKRaftVersionMessage(
                0, // timestamp
                new KRaftVersionRecord()
                    .setVersion(ControlRecordUtils.KRAFT_VERSION_CURRENT_VERSION)
                    .setKRaftVersion((short) 1)
            );
            builder.appendVotersMessage(
                0, // timestamp
                leadersVoterSet.toVotersRecord(ControlRecordUtils.KRAFT_VOTERS_CURRENT_VERSION)
            );
            MemoryRecords leaderRecords = builder.build();
            context.deliverResponse(
                fetchRequest.correlationId(),
                fetchRequest.destination(),
                context.fetchResponse(epoch, leader.id(), leaderRecords, 0, Errors.NONE)
            );
        }

        // follower applies the bootstrap records, registering follower2 as a new voter
        context.client.poll();
        assertTrue(context.client.quorum().isVoter(follower));
    }

    @Test
    public void testAddVoter() throws Exception {
        ReplicaKey local = replicaKey(randomReplicaId(), true);
        ReplicaKey follower = replicaKey(local.id() + 1, true);

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, follower));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .withUnknownLeader(3)
            .build();

        context.unattachedToLeader();
        int epoch = context.currentEpoch();

        checkLeaderMetricValues(2, 0, 0, context);

        ReplicaKey newVoter = replicaKey(local.id() + 2, true);
        InetSocketAddress newAddress = InetSocketAddress.createUnresolved(
            "localhost",
            9990 + newVoter.id()
        );
        Endpoints newListeners = Endpoints.fromInetSocketAddresses(
            Map.of(context.channel.listenerName(), newAddress)
        );

        prepareLeaderToReceiveAddVoter(context, epoch, local, follower, newVoter);

        // Attempt to add new voter to the quorum
        context.deliverRequest(context.addVoterRequest(Integer.MAX_VALUE, newVoter, newListeners));

        completeApiVersionsForAddVoter(context, newVoter, newAddress);

        // Handle the API_VERSIONS response
        context.client.poll();
        // Append new VotersRecord to log
        context.client.poll();

        commitNewVoterSetForAddVoter(context, local, follower, newVoter, epoch);

        // Expect reply for AddVoter request
        context.pollUntilResponse();
        context.assertSentAddVoterResponse(Errors.NONE);
    }

    @Test
    void testAddVoterCompletesEarlyWithAckWhenCommittedFalse() throws Exception {
        ReplicaKey local = replicaKey(randomReplicaId(), true);
        ReplicaKey follower = replicaKey(local.id() + 1, true);

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, follower));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withRaftProtocol(RaftProtocol.KIP_1186_PROTOCOL)
            .withBootstrapSnapshot(Optional.of(voters))
            .withUnknownLeader(3)
            .build();

        context.unattachedToLeader();
        int epoch = context.currentEpoch();

        checkLeaderMetricValues(2, 0, 0, context);

        ReplicaKey newVoter = replicaKey(local.id() + 2, true);
        InetSocketAddress newAddress = InetSocketAddress.createUnresolved(
            "localhost",
            9990 + newVoter.id()
        );
        Endpoints newListeners = Endpoints.fromInetSocketAddresses(
            Map.of(context.channel.listenerName(), newAddress)
        );

        prepareLeaderToReceiveAddVoter(context, epoch, local, follower, newVoter);

        // Attempt to add new voter to the quorum
        context.deliverRequest(
            context.addVoterRequest(
                Integer.MAX_VALUE,
                newVoter,
                newListeners
            ).setAckWhenCommitted(false)
        );

        completeApiVersionsForAddVoter(context, newVoter, newAddress);

        // Handle the API_VERSIONS response
        context.client.poll();
        // Append new VotersRecord to log
        // Expect a response for AddVoter request before committing the new voter
        context.pollUntilResponse();
        context.assertSentAddVoterResponse(Errors.NONE);

        commitNewVoterSetForAddVoter(context, local, follower, newVoter, epoch);
    }

    // This method sets up the context so a test can send an AddVoter request after
    // exiting this method
    private void prepareLeaderToReceiveAddVoter(
        RaftClientTestContext context,
        int epoch,
        ReplicaKey leader,
        ReplicaKey follower,
        ReplicaKey observer
    ) throws Exception {
        // Show that the observer is not currently a voter
        assertFalse(context.client.quorum().isVoter(observer));

        // Establish a HWM and fence previous leaders
        context.deliverRequest(
            context.fetchRequest(epoch, follower, context.log.endOffset().offset(), epoch, 0)
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(leader.id()));

        // Catch up the new voter to the leader's LEO, the new voter is still an observer at this point
        context.deliverRequest(
            context.fetchRequest(epoch, observer, context.log.endOffset().offset(), epoch, 0)
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(leader.id()));
        checkLeaderMetricValues(2, 1, 0, context);
    }

    private void completeApiVersionsForAddVoter(
        RaftClientTestContext context,
        ReplicaKey newVoter,
        InetSocketAddress newAddress
    ) throws Exception {
        // Leader should send an API_VERSIONS request to the new voter's endpoint
        context.pollUntilRequest();
        RaftRequest.Outbound apiVersionRequest = context.assertSentApiVersionsRequest();
        assertEquals(
            new Node(newVoter.id(), newAddress.getHostString(), newAddress.getPort()),
            apiVersionRequest.destination()
        );

        // Reply with API_VERSIONS response with supported kraft.version
        context.deliverResponse(
            apiVersionRequest.correlationId(),
            apiVersionRequest.destination(),
            apiVersionsResponse(Errors.NONE)
        );
    }

    private void commitNewVoterSetForAddVoter(
        RaftClientTestContext context,
        ReplicaKey leader,
        ReplicaKey follower,
        ReplicaKey newVoter,
        int epoch
    ) throws Exception {
        // The new voter is now a voter after writing the VotersRecord to the log
        assertTrue(context.client.quorum().isVoter(newVoter));
        checkLeaderMetricValues(3, 0, 1, context);

        // Send a FETCH to increase the HWM and commit the new voter set
        context.deliverRequest(
            context.fetchRequest(
                epoch,
                follower,
                context.log.endOffset().offset(),
                epoch,
                0
            )
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(leader.id()));
        checkLeaderMetricValues(3, 0, 0, context);
    }

    @Test
    void testAddVoterInvalidClusterId() throws Exception {
        ReplicaKey local = replicaKey(randomReplicaId(), true);
        ReplicaKey follower = replicaKey(local.id() + 1, true);

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, follower));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .withUnknownLeader(3)
            .build();

        context.unattachedToLeader();

        ReplicaKey newVoter = replicaKey(local.id() + 2, true);
        InetSocketAddress newAddress = InetSocketAddress.createUnresolved(
            "localhost",
            9990 + newVoter.id()
        );
        Endpoints newListeners = Endpoints.fromInetSocketAddresses(
            Map.of(context.channel.listenerName(), newAddress)
        );

        // empty cluster id is rejected
        context.deliverRequest(context.addVoterRequest("", Integer.MAX_VALUE, newVoter, newListeners));
        context.pollUntilResponse();
        context.assertSentAddVoterResponse(Errors.INCONSISTENT_CLUSTER_ID);

        // invalid cluster id is rejected
        context.deliverRequest(context.addVoterRequest("invalid-uuid", Integer.MAX_VALUE, newVoter, newListeners));
        context.pollUntilResponse();
        context.assertSentAddVoterResponse(Errors.INCONSISTENT_CLUSTER_ID);

        assertFalse(context.client.quorum().isVoter(newVoter));
    }

    @Test
    void testAddVoterToNotLeader() throws Exception {
        ReplicaKey local = replicaKey(randomReplicaId(), true);
        ReplicaKey follower = replicaKey(local.id() + 1, true);

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, follower));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .withUnknownLeader(3)
            .build();

        ReplicaKey newVoter = replicaKey(local.id() + 2, true);
        InetSocketAddress newAddress = InetSocketAddress.createUnresolved(
            "localhost",
            9990 + newVoter.id()
        );
        Endpoints newListeners = Endpoints.fromInetSocketAddresses(
            Map.of(context.channel.listenerName(), newAddress)
        );

        // Attempt to add new voter to the quorum
        context.deliverRequest(context.addVoterRequest(Integer.MAX_VALUE, newVoter, newListeners));
        context.pollUntilResponse();
        context.assertSentAddVoterResponse(Errors.NOT_LEADER_OR_FOLLOWER);
    }

    @Test
    void testAddVoterWithMissingDefaultListener() throws Exception {
        ReplicaKey local = replicaKey(randomReplicaId(), true);
        ReplicaKey follower = replicaKey(local.id() + 1, true);

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, follower));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .withUnknownLeader(3)
            .build();

        context.unattachedToLeader();

        ReplicaKey newVoter = replicaKey(local.id() + 2, true);
        InetSocketAddress newAddress = InetSocketAddress.createUnresolved(
            "localhost",
            9990 + newVoter.id()
        );
        Endpoints newListeners = Endpoints.fromInetSocketAddresses(
            Map.of(ListenerName.normalised("not_the_default_listener"), newAddress)
        );

        // Attempt to add new voter to the quorum
        context.deliverRequest(context.addVoterRequest(Integer.MAX_VALUE, newVoter, newListeners));
        context.pollUntilResponse();
        context.assertSentAddVoterResponse(Errors.INVALID_REQUEST);
    }

    @Test
    void testAddVoterWithPendingAddVoter() throws Exception {
        ReplicaKey local = replicaKey(randomReplicaId(), true);
        ReplicaKey follower = replicaKey(local.id() + 1, true);

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, follower));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .withUnknownLeader(3)
            .build();

        context.unattachedToLeader();
        int epoch = context.currentEpoch();

        ReplicaKey newVoter = replicaKey(local.id() + 2, true);
        InetSocketAddress newAddress = InetSocketAddress.createUnresolved(
            "localhost",
            9990 + newVoter.id()
        );
        Endpoints newListeners = Endpoints.fromInetSocketAddresses(
            Map.of(context.channel.listenerName(), newAddress)
        );

        // Establish a HWM and fence previous leaders
        context.deliverRequest(
            context.fetchRequest(epoch, follower, context.log.endOffset().offset(), epoch, 0)
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(local.id()));

        // Catch up the new voter to the leader's LEO
        context.deliverRequest(
            context.fetchRequest(epoch, newVoter, context.log.endOffset().offset(), epoch, 0)
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(local.id()));

        // Attempt to add new voter to the quorum
        context.deliverRequest(context.addVoterRequest(Integer.MAX_VALUE, newVoter, newListeners));

        // Attempting to add another voter should be an error
        ReplicaKey anotherNewVoter = replicaKey(local.id() + 3, true);
        InetSocketAddress anotherNewAddress = InetSocketAddress.createUnresolved(
            "localhost",
            9990 + anotherNewVoter.id()
        );
        Endpoints anotherNewListeners = Endpoints.fromInetSocketAddresses(
            Map.of(context.channel.listenerName(), anotherNewAddress)
        );
        context.deliverRequest(context.addVoterRequest(Integer.MAX_VALUE, anotherNewVoter, anotherNewListeners));
        context.pollUntilResponse();
        context.assertSentAddVoterResponse(Errors.REQUEST_TIMED_OUT);
    }

    @Test
    void testAddVoterWithoutFencedPreviousLeaders() throws Exception {
        ReplicaKey local = replicaKey(randomReplicaId(), true);
        ReplicaKey follower = replicaKey(local.id() + 1, true);

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, follower));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .withUnknownLeader(3)
            .build();

        context.unattachedToLeader();
        int epoch = context.currentEpoch();

        ReplicaKey newVoter = replicaKey(local.id() + 2, true);
        InetSocketAddress newAddress = InetSocketAddress.createUnresolved(
            "localhost",
            9990 + newVoter.id()
        );
        Endpoints newListeners = Endpoints.fromInetSocketAddresses(
            Map.of(context.channel.listenerName(), newAddress)
        );

        // Catch up the new voter to the leader's LEO
        context.deliverRequest(
            context.fetchRequest(epoch, newVoter, context.log.endOffset().offset(), epoch, 0)
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(local.id()));

        // Attempt to add new voter to the quorum
        context.deliverRequest(context.addVoterRequest(Integer.MAX_VALUE, newVoter, newListeners));
        context.pollUntilResponse();
        context.assertSentAddVoterResponse(Errors.REQUEST_TIMED_OUT);
    }

    @Test
    void testAddVoterWithKraftVersion0() throws Exception {
        ReplicaKey local = replicaKey(randomReplicaId(), true);
        ReplicaKey follower = replicaKey(local.id() + 1, true);

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, follower));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withStaticVoters(voters)
            .withUnknownLeader(3)
            .build();

        context.unattachedToLeader();
        int epoch = context.currentEpoch();

        ReplicaKey newVoter = replicaKey(local.id() + 2, true);
        InetSocketAddress newAddress = InetSocketAddress.createUnresolved(
            "localhost",
            9990 + newVoter.id()
        );
        Endpoints newListeners = Endpoints.fromInetSocketAddresses(
            Map.of(context.channel.listenerName(), newAddress)
        );

        // Establish a HWM and fence previous leaders
        context.deliverRequest(
            context.fetchRequest(epoch, follower, context.log.endOffset().offset(), epoch, 0)
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(local.id()));

        // Catch up the new voter to the leader's LEO
        context.deliverRequest(
            context.fetchRequest(epoch, newVoter, context.log.endOffset().offset(), epoch, 0)
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(local.id()));

        // Attempt to add new voter to the quorum
        context.deliverRequest(context.addVoterRequest(Integer.MAX_VALUE, newVoter, newListeners));
        context.pollUntilResponse();
        context.assertSentAddVoterResponse(Errors.UNSUPPORTED_VERSION);
    }

    @Test
    void testAddVoterWithExistingVoter() throws Exception {
        ReplicaKey local = replicaKey(randomReplicaId(), true);
        ReplicaKey follower = replicaKey(local.id() + 1, true);

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, follower));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .withUnknownLeader(3)
            .build();

        context.unattachedToLeader();
        int epoch = context.currentEpoch();

        ReplicaKey newVoter = replicaKey(follower.id(), true);
        InetSocketAddress newAddress = InetSocketAddress.createUnresolved(
            "localhost",
            9990 + newVoter.id()
        );
        Endpoints newListeners = Endpoints.fromInetSocketAddresses(
            Map.of(context.channel.listenerName(), newAddress)
        );

        // Establish a HWM and fence previous leaders
        context.deliverRequest(
            context.fetchRequest(epoch, follower, context.log.endOffset().offset(), epoch, 0)
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(local.id()));

        // Catch up the new voter to the leader's LEO
        context.deliverRequest(
            context.fetchRequest(epoch, newVoter, context.log.endOffset().offset(), epoch, 0)
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(local.id()));

        // Attempt to add new voter with the same id as an existing voter
        context.deliverRequest(context.addVoterRequest(Integer.MAX_VALUE, newVoter, newListeners));
        context.pollUntilResponse();
        context.assertSentAddVoterResponse(Errors.DUPLICATE_VOTER);
    }

    @Test
    void testAddVoterTimeout() throws Exception {
        ReplicaKey local = replicaKey(randomReplicaId(), true);
        ReplicaKey follower = replicaKey(local.id() + 1, true);

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, follower));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .withUnknownLeader(3)
            .build();

        context.unattachedToLeader();
        int epoch = context.currentEpoch();

        ReplicaKey newVoter = replicaKey(local.id() + 2, true);
        InetSocketAddress newAddress = InetSocketAddress.createUnresolved(
            "localhost",
            9990 + newVoter.id()
        );
        Endpoints newListeners = Endpoints.fromInetSocketAddresses(
            Map.of(context.channel.listenerName(), newAddress)
        );

        // Establish a HWM and fence previous leaders
        context.deliverRequest(
            context.fetchRequest(epoch, follower, context.log.endOffset().offset(), epoch, 0)
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(local.id()));

        // Catch up the new voter to the leader's LEO
        context.deliverRequest(
            context.fetchRequest(epoch, newVoter, context.log.endOffset().offset(), epoch, 0)
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(local.id()));

        // Attempt to add new voter to the quorum
        context.deliverRequest(context.addVoterRequest(Integer.MAX_VALUE, newVoter, newListeners));

        // Leader should send an API_VERSIONS request to the new voter's endpoint
        context.pollUntilRequest();
        RaftRequest.Outbound apiVersionRequest = context.assertSentApiVersionsRequest();
        assertEquals(
            new Node(newVoter.id(), newAddress.getHostString(), newAddress.getPort()),
            apiVersionRequest.destination()
        );

        // Reply with API_VERSIONS response with supported kraft.version
        context.deliverResponse(
            apiVersionRequest.correlationId(),
            apiVersionRequest.destination(),
            apiVersionsResponse(Errors.NONE)
        );

        // Handle the API_VERSIONS response
        context.client.poll();

        // Wait for request timeout without sending a FETCH request to timeout the add voter RPC
        context.time.sleep(context.requestTimeoutMs());

        // Expect the AddVoter RPC to timeout
        context.pollUntilResponse();
        context.assertSentAddVoterResponse(Errors.REQUEST_TIMED_OUT);

        // Event though the voters record never committed and the RPC timeout show that the new
        // voter is part of the voter set
        assertTrue(context.client.quorum().isVoter(newVoter));
    }

    @Test
    void testAddVoterWithApiVersionsFromIncorrectNode() throws Exception {
        ReplicaKey local = replicaKey(randomReplicaId(), true);
        ReplicaKey follower = replicaKey(local.id() + 1, true);

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, follower));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .withUnknownLeader(3)
            .build();

        context.unattachedToLeader();
        int epoch = context.currentEpoch();

        ReplicaKey newVoter = replicaKey(local.id() + 2, true);
        InetSocketAddress newAddress = InetSocketAddress.createUnresolved(
            "localhost",
            9990 + newVoter.id()
        );
        Endpoints newListeners = Endpoints.fromInetSocketAddresses(
            Map.of(context.channel.listenerName(), newAddress)
        );

        // Establish a HWM and fence previous leaders
        context.deliverRequest(
            context.fetchRequest(epoch, follower, context.log.endOffset().offset(), epoch, 0)
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(local.id()));

        // Catch up the new voter to the leader's LEO
        context.deliverRequest(
            context.fetchRequest(epoch, newVoter, context.log.endOffset().offset(), epoch, 0)
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(local.id()));

        // Attempt to add new voter to the quorum
        context.deliverRequest(context.addVoterRequest(Integer.MAX_VALUE, newVoter, newListeners));

        // Leader should send an API_VERSIONS request to the new voter's endpoint
        context.pollUntilRequest();
        RaftRequest.Outbound apiVersionRequest = context.assertSentApiVersionsRequest();
        assertEquals(
            new Node(newVoter.id(), newAddress.getHostString(), newAddress.getPort()),
            apiVersionRequest.destination()
        );

        // Reply with API_VERSIONS response with supported kraft.version
        context.deliverResponse(
            apiVersionRequest.correlationId(),
            apiVersionRequest.destination(),
            apiVersionsResponse(Errors.INVALID_REQUEST)
        );
        context.pollUntilResponse();
        context.assertSentAddVoterResponse(Errors.REQUEST_TIMED_OUT);
    }

    @Test
    void testAddVoterInvalidFeatureVersion() throws Exception {
        ReplicaKey local = replicaKey(randomReplicaId(), true);
        ReplicaKey follower = replicaKey(local.id() + 1, true);

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, follower));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .withUnknownLeader(3)
            .build();

        context.unattachedToLeader();
        int epoch = context.currentEpoch();

        ReplicaKey newVoter = replicaKey(local.id() + 2, true);
        InetSocketAddress newAddress = InetSocketAddress.createUnresolved(
            "localhost",
            9990 + newVoter.id()
        );
        Endpoints newListeners = Endpoints.fromInetSocketAddresses(
            Map.of(context.channel.listenerName(), newAddress)
        );

        // Establish a HWM and fence previous leaders
        context.deliverRequest(
            context.fetchRequest(epoch, follower, context.log.endOffset().offset(), epoch, 0)
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(local.id()));

        // Catch up the new voter to the leader's LEO
        context.deliverRequest(
            context.fetchRequest(epoch, newVoter, context.log.endOffset().offset(), epoch, 0)
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(local.id()));

        // Attempt to add new voter to the quorum
        context.deliverRequest(context.addVoterRequest(Integer.MAX_VALUE, newVoter, newListeners));

        // Leader should send an API_VERSIONS request to the new voter's endpoint
        context.pollUntilRequest();
        RaftRequest.Outbound apiVersionRequest = context.assertSentApiVersionsRequest();
        assertEquals(
            new Node(newVoter.id(), newAddress.getHostString(), newAddress.getPort()),
            apiVersionRequest.destination()
        );

        // Reply with API_VERSIONS response that doesn't support kraft.version 1
        context.deliverResponse(
            apiVersionRequest.correlationId(),
            apiVersionRequest.destination(),
            apiVersionsResponse(Errors.NONE, new SupportedVersionRange((short) 0))
        );
        context.pollUntilResponse();
        context.assertSentAddVoterResponse(Errors.INVALID_REQUEST);
    }

    @Test
    void testAddVoterWithLaggingNewVoter() throws Exception {
        ReplicaKey local = replicaKey(randomReplicaId(), true);
        ReplicaKey follower = replicaKey(local.id() + 1, true);

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, follower));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .withUnknownLeader(3)
            .build();

        context.unattachedToLeader();
        int epoch = context.currentEpoch();

        ReplicaKey newVoter = replicaKey(local.id() + 2, true);
        InetSocketAddress newAddress = InetSocketAddress.createUnresolved(
            "localhost",
            9990 + newVoter.id()
        );
        Endpoints newListeners = Endpoints.fromInetSocketAddresses(
            Map.of(context.channel.listenerName(), newAddress)
        );

        // Establish a HWM and fence previous leaders
        context.deliverRequest(
            context.fetchRequest(epoch, follower, context.log.endOffset().offset(), epoch, 0)
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(local.id()));

        // Attempt to add new voter to the quorum
        context.deliverRequest(context.addVoterRequest(Integer.MAX_VALUE, newVoter, newListeners));

        // Leader should send an API_VERSIONS request to the new voter's endpoint
        context.pollUntilRequest();
        RaftRequest.Outbound apiVersionRequest = context.assertSentApiVersionsRequest();
        assertEquals(
            new Node(newVoter.id(), newAddress.getHostString(), newAddress.getPort()),
            apiVersionRequest.destination()
        );

        // Reply with API_VERSIONS response with supported kraft.version
        context.deliverResponse(
            apiVersionRequest.correlationId(),
            apiVersionRequest.destination(),
            apiVersionsResponse(Errors.NONE)
        );
        context.pollUntilResponse();
        context.assertSentAddVoterResponse(Errors.REQUEST_TIMED_OUT);
    }

    @Test
    void testAddVoterFailsWhenLosingLeadership() throws Exception {
        ReplicaKey local = replicaKey(randomReplicaId(), true);
        ReplicaKey follower = replicaKey(local.id() + 1, true);

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, follower));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .withUnknownLeader(3)
            .build();

        context.unattachedToLeader();
        int epoch = context.currentEpoch();

        checkLeaderMetricValues(2, 0, 0, context);

        ReplicaKey newVoter = replicaKey(local.id() + 2, true);
        InetSocketAddress newAddress = InetSocketAddress.createUnresolved(
            "localhost",
            9990 + newVoter.id()
        );
        Endpoints newListeners = Endpoints.fromInetSocketAddresses(
            Map.of(context.channel.listenerName(), newAddress)
        );

        // Establish a HWM and fence previous leaders
        context.deliverRequest(
            context.fetchRequest(epoch, follower, context.log.endOffset().offset(), epoch, 0)
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(local.id()));

        // Attempt to add new voter to the quorum
        context.deliverRequest(context.addVoterRequest(Integer.MAX_VALUE, newVoter, newListeners));

        // Leader should send an API_VERSIONS request to the new voter's endpoint
        context.pollUntilRequest();
        RaftRequest.Outbound apiVersionRequest = context.assertSentApiVersionsRequest();
        assertEquals(
            new Node(newVoter.id(), newAddress.getHostString(), newAddress.getPort()),
            apiVersionRequest.destination()
        );

        // Leader completes the AddVoter RPC when resigning
        context.client.resign(epoch);
        context.pollUntilResponse();
        context.assertSentAddVoterResponse(Errors.NOT_LEADER_OR_FOLLOWER);

        assertEquals(2, getMetric(context.metrics, "number-of-voters").metricValue());
        assertNull(getMetric(context.metrics, "number-of-observers"));
        assertNull(getMetric(context.metrics, "uncommitted-voter-change"));
    }

    @Test
    void testAddVoterWithMissingDirectoryId() throws Exception {
        ReplicaKey local = replicaKey(randomReplicaId(), true);
        ReplicaKey follower = replicaKey(local.id() + 1, true);

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, follower));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .withUnknownLeader(3)
            .build();

        context.unattachedToLeader();
        int epoch = context.currentEpoch();

        ReplicaKey newVoter = replicaKey(local.id() + 2, false);
        InetSocketAddress newAddress = InetSocketAddress.createUnresolved(
            "localhost",
            9990 + newVoter.id()
        );
        Endpoints newListeners = Endpoints.fromInetSocketAddresses(
            Map.of(context.channel.listenerName(), newAddress)
        );

        // Establish a HWM and fence previous leaders
        context.deliverRequest(
            context.fetchRequest(epoch, follower, context.log.endOffset().offset(), epoch, 0)
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(local.id()));

        // Attempt to add new voter to the quorum
        context.deliverRequest(context.addVoterRequest(Integer.MAX_VALUE, newVoter, newListeners));
        context.pollUntilResponse();
        context.assertSentAddVoterResponse(Errors.INVALID_REQUEST);
    }

    @Test
    public void testRemoveVoter() throws Exception {
        ReplicaKey local = replicaKey(randomReplicaId(), true);
        ReplicaKey follower1 = replicaKey(local.id() + 1, true);
        ReplicaKey follower2 = replicaKey(local.id() + 2, true);

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, follower1, follower2));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .withUnknownLeader(3)
            .build();

        context.unattachedToLeader();
        int epoch = context.currentEpoch();

        assertTrue(context.client.quorum().isVoter(follower2));

        checkLeaderMetricValues(3, 0, 0, context);

        // Establish a HWM and fence previous leaders
        context.deliverRequest(
            context.fetchRequest(epoch, follower1, context.log.endOffset().offset(), epoch, 0)
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(local.id()));

        // Attempt to remove follower2
        context.deliverRequest(context.removeVoterRequest(follower2));

        // Handle the remove voter request
        context.client.poll();
        // Append the VotersRecord to the log
        context.client.poll();

        // follower2 should not be a voter in the latest voter set
        assertFalse(context.client.quorum().isVoter(follower2));
        checkLeaderMetricValues(2, 1, 1, context);

        // Send a FETCH to increase the HWM and commit the new voter set
        context.deliverRequest(
            context.fetchRequest(epoch, follower1, context.log.endOffset().offset(), epoch, 0)
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(local.id()));
        checkLeaderMetricValues(2, 1, 0, context);

        // Expect reply for RemoveVoter request
        context.pollUntilResponse();
        context.assertSentRemoveVoterResponse(Errors.NONE);
    }

    @Test
    public void testRemoveVoterIsLeader() throws Exception {
        ReplicaKey local = replicaKey(randomReplicaId(), true);
        ReplicaKey follower1 = replicaKey(local.id() + 1, true);
        ReplicaKey follower2 = replicaKey(local.id() + 2, true);

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, follower1, follower2));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .withUnknownLeader(3)
            .build();

        context.unattachedToLeader();
        int epoch = context.currentEpoch();

        checkLeaderMetricValues(3, 0, 0, context);

        // Establish a HWM and fence previous leaders
        context.deliverRequest(
            context.fetchRequest(epoch, follower1, context.log.endOffset().offset(), epoch, 0)
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(local.id()));

        // Attempt to remove local leader
        context.deliverRequest(context.removeVoterRequest(local));

        // Handle the remove voter request
        context.client.poll();
        // Append the VotersRecord to the log
        context.client.poll();

        // local should not be a voter in the latest voter set
        assertFalse(context.client.quorum().isVoter(local));
        checkLeaderMetricValues(2, 1, 1, context);

        // Send a FETCH request for follower1
        context.deliverRequest(
            context.fetchRequest(epoch, follower1, context.log.endOffset().offset(), epoch, 0)
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(local.id()));

        // Send a FETCH request for follower2 and increase the HWM
        context.deliverRequest(
            context.fetchRequest(epoch, follower2, context.log.endOffset().offset(), epoch, 0)
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(local.id()));
        checkLeaderMetricValues(2, 1, 0, context);

        // Expect reply for RemoveVoter request
        context.pollUntilResponse();
        context.assertSentRemoveVoterResponse(Errors.NONE);

        // Expect END_QUORUM_EPOCH requests
        context.pollUntilRequest();
        context.collectEndQuorumRequests(
            epoch,
            Set.of(follower1.id(), follower2.id()),
            Optional.empty()
        );

        // Calls to resign should be allowed and not throw an exception
        context.client.resign(epoch);
        assertNull(getMetric(context.metrics, "number-of-observers"));
        assertNull(getMetric(context.metrics, "uncommitted-voter-change"));

        // Election timeout is random number in [electionTimeoutMs, 2 * electionTimeoutMs)
        context.time.sleep(2L * context.electionTimeoutMs());
        context.client.poll();

        assertTrue(context.client.quorum().isObserver());
        assertTrue(context.client.quorum().isUnattached());
    }

    @Test
    public void testRemoveVoterInvalidClusterId() throws Exception {
        ReplicaKey local = replicaKey(randomReplicaId(), true);
        ReplicaKey follower1 = replicaKey(local.id() + 1, true);
        ReplicaKey follower2 = replicaKey(local.id() + 2, true);

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, follower1, follower2));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .withUnknownLeader(3)
            .build();

        context.unattachedToLeader();

        // empty cluster id is rejected
        context.deliverRequest(context.removeVoterRequest("", follower1));
        context.pollUntilResponse();
        context.assertSentRemoveVoterResponse(Errors.INCONSISTENT_CLUSTER_ID);

        // invalid cluster id is rejected
        context.deliverRequest(context.removeVoterRequest("invalid-uuid", follower1));
        context.pollUntilResponse();
        context.assertSentRemoveVoterResponse(Errors.INCONSISTENT_CLUSTER_ID);

        assertTrue(context.client.quorum().isVoter(follower1));
    }

    @Test
    void testRemoveVoterToNotLeader() throws Exception {
        ReplicaKey local = replicaKey(randomReplicaId(), true);
        ReplicaKey follower1 = replicaKey(local.id() + 1, true);
        ReplicaKey follower2 = replicaKey(local.id() + 2, true);

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, follower1, follower2));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .withUnknownLeader(3)
            .build();

        // Attempt to remove voter to the quorum
        context.deliverRequest(context.removeVoterRequest(follower1));
        context.pollUntilResponse();
        context.assertSentRemoveVoterResponse(Errors.NOT_LEADER_OR_FOLLOWER);
    }

    @Test
    void testRemoveVoterWithPendingRemoveVoter() throws Exception {
        ReplicaKey local = replicaKey(randomReplicaId(), true);
        ReplicaKey follower1 = replicaKey(local.id() + 1, true);
        ReplicaKey follower2 = replicaKey(local.id() + 2, true);

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, follower1, follower2));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .withUnknownLeader(3)
            .build();

        context.unattachedToLeader();
        int epoch = context.currentEpoch();

        // Establish a HWM and fence previous leaders
        context.deliverRequest(
            context.fetchRequest(epoch, follower1, context.log.endOffset().offset(), epoch, 0)
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(local.id()));

        // Attempt to remove follower2
        context.deliverRequest(context.removeVoterRequest(follower2));

        // Handle the remove voter request
        context.client.poll();
        // Append the VotersRecord to the log
        context.client.poll();

        // Attempt to remove follower1
        context.deliverRequest(context.removeVoterRequest(follower1));
        context.pollUntilResponse();
        context.assertSentRemoveVoterResponse(Errors.REQUEST_TIMED_OUT);
    }

    @Test
    void testRemoveVoterWithoutFencedPreviousLeaders() throws Exception {
        ReplicaKey local = replicaKey(randomReplicaId(), true);
        ReplicaKey follower1 = replicaKey(local.id() + 1, true);
        ReplicaKey follower2 = replicaKey(local.id() + 2, true);

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, follower1, follower2));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .withUnknownLeader(3)
            .build();

        context.unattachedToLeader();

        // Attempt to remove follower2
        context.deliverRequest(context.removeVoterRequest(follower2));
        context.pollUntilResponse();
        context.assertSentRemoveVoterResponse(Errors.REQUEST_TIMED_OUT);
    }

    @Test
    void testRemoveVoterWithKraftVersion0() throws Exception {
        ReplicaKey local = replicaKey(randomReplicaId(), true);
        ReplicaKey follower1 = replicaKey(local.id() + 1, true);
        ReplicaKey follower2 = replicaKey(local.id() + 2, true);

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, follower1, follower2));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withStaticVoters(voters)
            .withUnknownLeader(3)
            .build();

        context.unattachedToLeader();
        int epoch = context.currentEpoch();

        // Establish a HWM and fence previous leaders
        context.deliverRequest(
            context.fetchRequest(epoch, follower1, context.log.endOffset().offset(), epoch, 0)
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(local.id()));

        // Attempt to remove follower2
        context.deliverRequest(context.removeVoterRequest(follower2));
        context.pollUntilResponse();
        context.assertSentRemoveVoterResponse(Errors.UNSUPPORTED_VERSION);
    }

    @Test
    void testRemoveVoterWithNoneVoter() throws Exception {
        ReplicaKey local = replicaKey(randomReplicaId(), true);
        ReplicaKey follower1 = replicaKey(local.id() + 1, true);
        ReplicaKey follower2 = replicaKey(local.id() + 2, true);

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, follower1, follower2));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .withUnknownLeader(3)
            .build();

        context.unattachedToLeader();
        int epoch = context.currentEpoch();

        // Establish a HWM and fence previous leaders
        context.deliverRequest(
            context.fetchRequest(epoch, follower1, context.log.endOffset().offset(), epoch, 0)
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(local.id()));

        // Attempt to remove replica with same id as follower2
        context.deliverRequest(context.removeVoterRequest(replicaKey(follower2.id(), true)));
        context.pollUntilResponse();
        context.assertSentRemoveVoterResponse(Errors.VOTER_NOT_FOUND);
    }

    @Test
    void testRemoveVoterWithNoneVoterId() throws Exception {
        ReplicaKey local = replicaKey(randomReplicaId(), true);
        ReplicaKey follower1 = replicaKey(local.id() + 1, true);
        ReplicaKey follower2 = replicaKey(local.id() + 2, true);

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, follower1, follower2));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .withUnknownLeader(3)
            .build();

        context.unattachedToLeader();
        int epoch = context.currentEpoch();

        // Establish a HWM and fence previous leaders
        context.deliverRequest(
            context.fetchRequest(epoch, follower1, context.log.endOffset().offset(), epoch, 0)
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(local.id()));

        // Attempt to remove follower2
        context.deliverRequest(
            context.removeVoterRequest(
                ReplicaKey.of(follower2.id() + 1, follower2.directoryId().get())
            )
        );
        context.pollUntilResponse();
        context.assertSentRemoveVoterResponse(Errors.VOTER_NOT_FOUND);
    }

    @Test
    void testRemoveVoterToEmptyVoterSet() throws Exception {
        ReplicaKey local = replicaKey(randomReplicaId(), true);

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .build();
        assertEquals(OptionalInt.of(local.id()), context.currentLeader());

        // Attempt to remove local leader to empty voter set
        context.deliverRequest(context.removeVoterRequest(local));
        context.pollUntilResponse();
        context.assertSentRemoveVoterResponse(Errors.VOTER_NOT_FOUND);
    }

    @Test
    void testRemoveVoterTimedOut() throws Exception {
        ReplicaKey local = replicaKey(randomReplicaId(), true);
        ReplicaKey follower1 = replicaKey(local.id() + 1, true);
        ReplicaKey follower2 = replicaKey(local.id() + 2, true);

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, follower1, follower2));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .withUnknownLeader(3)
            .build();

        context.unattachedToLeader();
        int epoch = context.currentEpoch();

        // Establish a HWM and fence previous leaders
        context.deliverRequest(
            context.fetchRequest(epoch, follower1, context.log.endOffset().offset(), epoch, 0)
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(local.id()));

        // Attempt to remove follower2
        context.deliverRequest(context.removeVoterRequest(follower2));

        // Handle the remove voter request
        context.client.poll();
        // Append the VotersRecord to the log
        context.client.poll();

        // Wait for request timeout without sending a FETCH request to timeout the remove voter RPC
        context.time.sleep(context.requestTimeoutMs());

        // Expect a timeout error
        context.pollUntilResponse();
        context.assertSentRemoveVoterResponse(Errors.REQUEST_TIMED_OUT);

        // Event though the voters record never committed and the RPC timeout show that the old
        // voter is not part of the voter set
        assertFalse(context.client.quorum().isVoter(follower2));
    }

    @Test
    void testRemoveVoterFailsWhenLosingLeadership() throws Exception {
        ReplicaKey local = replicaKey(randomReplicaId(), true);
        ReplicaKey follower1 = replicaKey(local.id() + 1, true);
        ReplicaKey follower2 = replicaKey(local.id() + 2, true);

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, follower1, follower2));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .withUnknownLeader(3)
            .build();

        context.unattachedToLeader();
        int epoch = context.currentEpoch();

        // Establish a HWM and fence previous leaders
        context.deliverRequest(
            context.fetchRequest(epoch, follower1, context.log.endOffset().offset(), epoch, 0)
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(local.id()));

        // Attempt to remove follower2
        context.deliverRequest(context.removeVoterRequest(follower2));

        // Handle the remove voter request
        context.client.poll();
        // Append the VotersRecord to the log
        context.client.poll();

        // Leader completes the RemoveVoter RPC when resigning
        context.client.resign(epoch);
        context.pollUntilResponse();
        context.assertSentRemoveVoterResponse(Errors.NOT_LEADER_OR_FOLLOWER);

        // Event though the voters record never committed, the old voter is not part of the voter
        // set
        assertFalse(context.client.quorum().isVoter(follower2));
    }

    @Test
    void testAddVoterWithPendingRemoveVoter() throws Exception {
        ReplicaKey local = replicaKey(randomReplicaId(), true);
        ReplicaKey follower1 = replicaKey(local.id() + 1, true);
        ReplicaKey follower2 = replicaKey(local.id() + 2, true);

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, follower1, follower2));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .withUnknownLeader(3)
            .build();

        context.unattachedToLeader();
        int epoch = context.currentEpoch();

        // Establish a HWM and fence previous leaders
        context.deliverRequest(
            context.fetchRequest(epoch, follower1, context.log.endOffset().offset(), epoch, 0)
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(local.id()));

        // Attempt to remove follower2
        context.deliverRequest(context.removeVoterRequest(follower2));

        // Handle the remove voter request
        context.client.poll();
        // Append the VotersRecord to the log
        context.client.poll();

        // Attempt to add a new voter while the RemoveVoter RPC is pending
        ReplicaKey newVoter = replicaKey(local.id() + 2, true);
        InetSocketAddress newAddress = InetSocketAddress.createUnresolved(
            "localhost",
            9990 + newVoter.id()
        );
        Endpoints newListeners = Endpoints.fromInetSocketAddresses(
            Map.of(context.channel.listenerName(), newAddress)
        );
        context.deliverRequest(context.addVoterRequest(Integer.MAX_VALUE, newVoter, newListeners));
        context.pollUntilResponse();
        context.assertSentAddVoterResponse(Errors.REQUEST_TIMED_OUT);
    }

    @Test
    void testRemoveVoterWithPendingAddVoter() throws Exception {
        ReplicaKey local = replicaKey(randomReplicaId(), true);
        ReplicaKey follower = replicaKey(local.id() + 1, true);

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, follower));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .withUnknownLeader(3)
            .build();

        context.unattachedToLeader();
        int epoch = context.currentEpoch();

        ReplicaKey newVoter = replicaKey(local.id() + 2, true);
        InetSocketAddress newAddress = InetSocketAddress.createUnresolved(
            "localhost",
            9990 + newVoter.id()
        );
        Endpoints newListeners = Endpoints.fromInetSocketAddresses(
            Map.of(context.channel.listenerName(), newAddress)
        );

        // Establish a HWM and fence previous leaders
        context.deliverRequest(
            context.fetchRequest(epoch, follower, context.log.endOffset().offset(), epoch, 0)
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(local.id()));

        // Catch up the new voter to the leader's LEO
        context.deliverRequest(
            context.fetchRequest(epoch, newVoter, context.log.endOffset().offset(), epoch, 0)
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(local.id()));

        // Attempt to add new voter to the quorum
        context.deliverRequest(context.addVoterRequest(Integer.MAX_VALUE, newVoter, newListeners));

        // Attempt to remove follower while AddVoter is pending
        context.deliverRequest(context.removeVoterRequest(follower));
        context.pollUntilResponse();
        context.assertSentRemoveVoterResponse(Errors.REQUEST_TIMED_OUT);
    }

    @Test
    void testUpdateVoter() throws Exception {
        ReplicaKey local = replicaKey(randomReplicaId(), true);
        ReplicaKey follower = replicaKey(local.id() + 1, true);

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, follower));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .withUnknownLeader(3)
            .build();

        context.unattachedToLeader();
        int epoch = context.currentEpoch();

        assertTrue(context.client.quorum().isVoter(follower));

        // Establish a HWM and fence previous leaders
        context.deliverRequest(
            context.fetchRequest(epoch, follower, context.log.endOffset().offset(), epoch, 0)
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(local.id()));

        // Attempt to update the follower
        InetSocketAddress defaultAddress = InetSocketAddress.createUnresolved(
            "localhost",
            9990 + follower.id()
        );
        InetSocketAddress newAddress = InetSocketAddress.createUnresolved(
            "localhost",
            8990 + follower.id()
        );
        HashMap<ListenerName, InetSocketAddress> listenersMap = new HashMap<>(2);
        listenersMap.put(context.channel.listenerName(), defaultAddress);
        listenersMap.put(ListenerName.normalised("ANOTHER_LISTENER"), newAddress);
        Endpoints newListeners = Endpoints.fromInetSocketAddresses(listenersMap);
        context.deliverRequest(
            context.updateVoterRequest(
                follower,
                Feature.KRAFT_VERSION.supportedVersionRange(),
                newListeners
            )
        );

        // Expect reply for UpdateVoter request without committing the record
        context.pollUntilResponse();
        context.assertSentUpdateVoterResponse(
            Errors.NONE,
            OptionalInt.of(local.id()),
            epoch
        );

        // follower should still be a voter in the latest voter set
        assertTrue(context.client.quorum().isVoter(follower));
    }

    @Test
    void testLeaderUpdatesVoter() throws Exception {
        ReplicaKey local = replicaKey(randomReplicaId(), true);
        ReplicaKey follower = replicaKey(local.id() + 1, true);

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, follower));

        HashMap<ListenerName, InetSocketAddress> listenersMap = new HashMap<>(2);
        listenersMap.put(
            VoterSetTest.DEFAULT_LISTENER_NAME,
            InetSocketAddress.createUnresolved("localhost", 9990 + local.id())
        );
        listenersMap.put(
            ListenerName.normalised("ANOTHER_LISTENER"),
            InetSocketAddress.createUnresolved("localhost", 8990 + local.id())
        );
        Endpoints localListeners = Endpoints.fromInetSocketAddresses(listenersMap);

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .withUnknownLeader(3)
            .withLocalListeners(localListeners)
            .build();

        context.unattachedToLeader();
        int epoch = context.currentEpoch();

        assertTrue(context.client.quorum().isVoter(follower));

        // Establish a HWM and commit the latest voter set
        context.deliverRequest(
            context.fetchRequest(epoch, follower, context.log.endOffset().offset(), epoch, 0)
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(local.id()));

        Optional<VoterSet> updatedVoterSet = voters.updateVoter(
            VoterSet.VoterNode.of(
                local,
                localListeners,
                Feature.KRAFT_VERSION.supportedVersionRange()
            )
        );
        assertEquals(updatedVoterSet, context.listener.lastCommittedVoterSet());
    }

    @Test
    public void testUpdateVoterInvalidClusterId() throws Exception {
        ReplicaKey local = replicaKey(randomReplicaId(), true);
        ReplicaKey follower = replicaKey(local.id() + 1, true);

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, follower));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .withUnknownLeader(3)
            .build();

        context.unattachedToLeader();
        int epoch = context.currentEpoch();

        // empty cluster id is rejected
        context.deliverRequest(
            context.updateVoterRequest(
                "",
                follower,
                epoch,
                Feature.KRAFT_VERSION.supportedVersionRange(),
                Endpoints.empty()
            )
        );
        context.pollUntilResponse();
        context.assertSentUpdateVoterResponse(
            Errors.INCONSISTENT_CLUSTER_ID,
            OptionalInt.of(local.id()),
            epoch
        );

        // invalid cluster id is rejected
        context.deliverRequest(
            context.updateVoterRequest(
                "invalid-uuid",
                follower,
                epoch,
                Feature.KRAFT_VERSION.supportedVersionRange(),
                Endpoints.empty()
            )
        );
        context.pollUntilResponse();
        context.assertSentUpdateVoterResponse(
            Errors.INCONSISTENT_CLUSTER_ID,
            OptionalInt.of(local.id()),
            epoch
        );
    }

    @Test
    public void testInvalidUpdateVoter() throws Exception {
        ReplicaKey local = replicaKey(randomReplicaId(), true);
        ReplicaKey follower = replicaKey(local.id() + 1, true);

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, follower));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .withUnknownLeader(3)
            .build();

        context.unattachedToLeader();
        int epoch = context.currentEpoch();

        // missing directory id
        context.deliverRequest(
            context.updateVoterRequest(
                ReplicaKey.of(follower.id(), Uuid.ZERO_UUID),
                Feature.KRAFT_VERSION.supportedVersionRange(),
                Endpoints.empty()
            )
        );
        context.pollUntilResponse();
        context.assertSentUpdateVoterResponse(
            Errors.INVALID_REQUEST,
            OptionalInt.of(local.id()),
            epoch
        );

        // missing endpoints
        context.deliverRequest(
            context.updateVoterRequest(
                follower,
                Feature.KRAFT_VERSION.supportedVersionRange(),
                Endpoints.empty()
            )
        );
        context.pollUntilResponse();
        context.assertSentUpdateVoterResponse(
            Errors.INVALID_REQUEST,
            OptionalInt.of(local.id()),
            epoch
        );
    }

    @Test
    void testUpdateVoterOldEpoch() throws Exception {
        ReplicaKey local = replicaKey(randomReplicaId(), true);
        ReplicaKey follower = replicaKey(local.id() + 1, true);

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, follower));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .withUnknownLeader(3)
            .build();

        context.unattachedToLeader();
        int epoch = context.currentEpoch();

        context.deliverRequest(
            context.updateVoterRequest(
                context.clusterId,
                follower,
                epoch - 1,
                Feature.KRAFT_VERSION.supportedVersionRange(),
                Endpoints.empty()
            )
        );
        context.pollUntilResponse();
        context.assertSentUpdateVoterResponse(
            Errors.FENCED_LEADER_EPOCH,
            OptionalInt.of(local.id()),
            epoch
        );
    }

    @Test
    void testUpdateVoterNewEpoch() throws Exception {
        ReplicaKey local = replicaKey(randomReplicaId(), true);
        ReplicaKey follower = replicaKey(local.id() + 1, true);

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, follower));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .withUnknownLeader(3)
            .build();

        context.unattachedToLeader();
        int epoch = context.currentEpoch();

        context.deliverRequest(
            context.updateVoterRequest(
                context.clusterId,
                follower,
                epoch + 1,
                Feature.KRAFT_VERSION.supportedVersionRange(),
                Endpoints.empty()
            )
        );
        context.pollUntilResponse();
        context.assertSentUpdateVoterResponse(
            Errors.UNKNOWN_LEADER_EPOCH,
            OptionalInt.of(local.id()),
            epoch
        );
    }

    @Test
    void testUpdateVoterToNotLeader() throws Exception {
        ReplicaKey local = replicaKey(randomReplicaId(), true);
        ReplicaKey follower = replicaKey(local.id() + 1, true);

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, follower));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .withUnknownLeader(3)
            .build();

        // Attempt to update voter in the quorum
        context.deliverRequest(
            context.updateVoterRequest(
                follower,
                Feature.KRAFT_VERSION.supportedVersionRange(),
                Endpoints.empty()
            )
        );
        context.pollUntilResponse();
        context.assertSentUpdateVoterResponse(
            Errors.NOT_LEADER_OR_FOLLOWER,
            OptionalInt.empty(),
            context.currentEpoch()
        );
    }

    @Test
    void testUpdateVoterWithoutFencedPreviousLeaders() throws Exception {
        ReplicaKey local = replicaKey(randomReplicaId(), true);
        ReplicaKey follower = replicaKey(local.id() + 1, true);

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, follower));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .withUnknownLeader(3)
            .build();

        context.unattachedToLeader();
        int epoch = context.currentEpoch();

        // Attempt to update the follower
        InetSocketAddress defaultAddress = InetSocketAddress.createUnresolved(
            "localhost",
            9990 + follower.id()
        );
        InetSocketAddress newAddress = InetSocketAddress.createUnresolved(
            "localhost",
            8990 + follower.id()
        );
        HashMap<ListenerName, InetSocketAddress> listenersMap = new HashMap<>(2);
        listenersMap.put(context.channel.listenerName(), defaultAddress);
        listenersMap.put(ListenerName.normalised("ANOTHER_LISTENER"), newAddress);
        Endpoints newListeners = Endpoints.fromInetSocketAddresses(listenersMap);
        context.deliverRequest(
            context.updateVoterRequest(
                follower,
                Feature.KRAFT_VERSION.supportedVersionRange(),
                newListeners
            )
        );
        context.pollUntilResponse();
        context.assertSentUpdateVoterResponse(
            Errors.REQUEST_TIMED_OUT,
            OptionalInt.of(local.id()),
            epoch
        );
    }

    @Test
    void testUpdateVoterWithKraftVersion0() throws Exception {
        ReplicaKey local = replicaKey(randomReplicaId(), true);
        ReplicaKey follower = replicaKey(local.id() + 1, true);

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, follower));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withStaticVoters(voters)
            .withUnknownLeader(3)
            .build();

        context.unattachedToLeader();
        int epoch = context.currentEpoch();

        // Establish a HWM and fence previous leaders
        context.deliverRequest(
            context.fetchRequest(epoch, follower, context.log.endOffset().offset(), epoch, 0)
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(local.id()));

        // Attempt to update the follower
        InetSocketAddress defaultAddress = InetSocketAddress.createUnresolved(
            "localhost",
            9990 + follower.id()
        );
        InetSocketAddress newAddress = InetSocketAddress.createUnresolved(
            "localhost",
            8990 + follower.id()
        );
        HashMap<ListenerName, InetSocketAddress> listenersMap = new HashMap<>(2);
        listenersMap.put(context.channel.listenerName(), defaultAddress);
        listenersMap.put(ListenerName.normalised("ANOTHER_LISTENER"), newAddress);
        Endpoints newListeners = Endpoints.fromInetSocketAddresses(listenersMap);
        context.deliverRequest(
            context.updateVoterRequest(
                follower,
                Feature.KRAFT_VERSION.supportedVersionRange(),
                newListeners
            )
        );
        context.pollUntilResponse();
        context.assertSentUpdateVoterResponse(
            Errors.NONE,
            OptionalInt.of(local.id()),
            epoch
        );
    }

    @Test
    void testUpdateVoterWithNoneVoter() throws Exception {
        ReplicaKey local = replicaKey(randomReplicaId(), true);
        ReplicaKey follower = replicaKey(local.id() + 1, true);

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, follower));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .withUnknownLeader(3)
            .build();

        context.unattachedToLeader();
        int epoch = context.currentEpoch();

        // Establish a HWM and fence previous leaders
        context.deliverRequest(
            context.fetchRequest(epoch, follower, context.log.endOffset().offset(), epoch, 0)
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(local.id()));

        // Attempt to update a replica with the same id as follower
        InetSocketAddress defaultAddress = InetSocketAddress.createUnresolved(
            "localhost",
            9990 + follower.id()
        );
        InetSocketAddress newAddress = InetSocketAddress.createUnresolved(
            "localhost",
            8990 + follower.id()
        );
        HashMap<ListenerName, InetSocketAddress> listenersMap = new HashMap<>(2);
        listenersMap.put(context.channel.listenerName(), defaultAddress);
        listenersMap.put(ListenerName.normalised("ANOTHER_LISTENER"), newAddress);
        Endpoints newListeners = Endpoints.fromInetSocketAddresses(listenersMap);
        context.deliverRequest(
            context.updateVoterRequest(
                replicaKey(follower.id(), true),
                Feature.KRAFT_VERSION.supportedVersionRange(),
                newListeners
            )
        );
        context.pollUntilResponse();
        context.assertSentUpdateVoterResponse(
            Errors.VOTER_NOT_FOUND,
            OptionalInt.of(local.id()),
            epoch
        );
    }

    @Test
    void testUpdateVoterWithNoneVoterId() throws Exception {
        ReplicaKey local = replicaKey(randomReplicaId(), true);
        ReplicaKey follower = replicaKey(local.id() + 1, true);

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, follower));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .withUnknownLeader(3)
            .build();

        context.unattachedToLeader();
        int epoch = context.currentEpoch();

        // Establish a HWM and fence previous leaders
        context.deliverRequest(
            context.fetchRequest(epoch, follower, context.log.endOffset().offset(), epoch, 0)
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(local.id()));

        // Attempt to update a replica with the same id as follower
        InetSocketAddress defaultAddress = InetSocketAddress.createUnresolved(
            "localhost",
            9990 + follower.id() + 1
        );
        InetSocketAddress newAddress = InetSocketAddress.createUnresolved(
            "localhost",
            8990 + follower.id() + 1
        );
        HashMap<ListenerName, InetSocketAddress> listenersMap = new HashMap<>(2);
        listenersMap.put(context.channel.listenerName(), defaultAddress);
        listenersMap.put(ListenerName.normalised("ANOTHER_LISTENER"), newAddress);
        Endpoints newListeners = Endpoints.fromInetSocketAddresses(listenersMap);
        context.deliverRequest(
            context.updateVoterRequest(
                ReplicaKey.of(follower.id() + 1, follower.directoryId().get()),
                Feature.KRAFT_VERSION.supportedVersionRange(),
                newListeners
            )
        );
        context.pollUntilResponse();
        context.assertSentUpdateVoterResponse(
            Errors.VOTER_NOT_FOUND,
            OptionalInt.of(local.id()),
            epoch
        );
    }

    @Test
    void testUpdateVoterWithPendingAddVoter() throws Exception {
        ReplicaKey local = replicaKey(randomReplicaId(), true);
        ReplicaKey follower = replicaKey(local.id() + 1, true);

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, follower));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .withUnknownLeader(3)
            .build();

        context.unattachedToLeader();
        int epoch = context.currentEpoch();

        ReplicaKey newVoter = replicaKey(local.id() + 2, true);
        InetSocketAddress newVoterAddress = InetSocketAddress.createUnresolved(
            "localhost",
            9990 + newVoter.id()
        );
        Endpoints newVoterListeners = Endpoints.fromInetSocketAddresses(
            Map.of(context.channel.listenerName(), newVoterAddress)
        );

        // Establish a HWM and fence previous leaders
        context.deliverRequest(
            context.fetchRequest(epoch, follower, context.log.endOffset().offset(), epoch, 0)
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(local.id()));

        // Catch up the new voter to the leader's LEO
        context.deliverRequest(
            context.fetchRequest(epoch, newVoter, context.log.endOffset().offset(), epoch, 0)
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(local.id()));

        // Attempt to add new voter to the quorum
        context.deliverRequest(context.addVoterRequest(Integer.MAX_VALUE, newVoter, newVoterListeners));

        // Attempt to update the follower
        InetSocketAddress defaultAddress = InetSocketAddress.createUnresolved(
            "localhost",
            9990 + follower.id()
        );
        InetSocketAddress newAddress = InetSocketAddress.createUnresolved(
            "localhost",
            8990 + follower.id()
        );
        HashMap<ListenerName, InetSocketAddress> listenersMap = new HashMap<>(2);
        listenersMap.put(context.channel.listenerName(), defaultAddress);
        listenersMap.put(ListenerName.normalised("ANOTHER_LISTENER"), newAddress);
        Endpoints newListeners = Endpoints.fromInetSocketAddresses(listenersMap);
        context.deliverRequest(
            context.updateVoterRequest(
                follower,
                Feature.KRAFT_VERSION.supportedVersionRange(),
                newListeners
            )
        );
        context.pollUntilResponse();
        context.assertSentUpdateVoterResponse(
            Errors.REQUEST_TIMED_OUT,
            OptionalInt.of(local.id()),
            epoch
        );
    }

    @Test
    void testFollowerSendsUpdateVoter() throws Exception {
        ReplicaKey local = replicaKey(randomReplicaId(), true);
        ReplicaKey voter1 = replicaKey(local.id() + 1, true);
        ReplicaKey voter2 = replicaKey(local.id() + 2, true);

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, voter1, voter2));
        int epoch = 4;

        HashMap<ListenerName, InetSocketAddress> listenersMap = new HashMap<>(2);
        listenersMap.put(
            VoterSetTest.DEFAULT_LISTENER_NAME,
            InetSocketAddress.createUnresolved("localhost", 9990 + local.id())
        );
        listenersMap.put(
            ListenerName.normalised("ANOTHER_LISTENER"),
            InetSocketAddress.createUnresolved("localhost", 8990 + local.id())
        );
        Endpoints localListeners = Endpoints.fromInetSocketAddresses(listenersMap);

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .withElectedLeader(epoch, voter1.id())
            .withLocalListeners(localListeners)
            .build();

        // waiting for FETCH requests until the UpdateRaftVoter request is sent
        context.advanceTimeAndCompleteFetch(epoch, voter1.id(), true);

        context.pollUntilRequest();
        RaftRequest.Outbound updateRequest = context.assertSentUpdateVoterRequest(
            local,
            epoch,
            Feature.KRAFT_VERSION.supportedVersionRange(),
            localListeners
        );
        context.deliverResponse(
            updateRequest.correlationId(),
            updateRequest.destination(),
            context.updateVoterResponse(
                Errors.NONE,
                new LeaderAndEpoch(OptionalInt.of(voter1.id()), epoch)
            )
        );

        // after sending an update voter the next request should be a fetch
        context.pollUntilRequest();
        RaftRequest.Outbound fetchRequest = context.assertSentFetchRequest();
        context.assertFetchRequestData(fetchRequest, epoch, 0L, 0, context.client.highWatermark());
    }

    @ParameterizedTest
    @EnumSource(value = Errors.class, names = {"NONE", "UNSUPPORTED_VERSION"})
    void testFollowerSendsUpdateVoterWithKraftVersion0(Errors updateVoterError) throws Exception {
        ReplicaKey local = replicaKey(randomReplicaId(), true);
        ReplicaKey voter1 = replicaKey(local.id() + 1, true);
        ReplicaKey voter2 = replicaKey(local.id() + 2, true);

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, voter1, voter2));
        int epoch = 4;

        HashMap<ListenerName, InetSocketAddress> listenersMap = new HashMap<>(2);
        listenersMap.put(
            VoterSetTest.DEFAULT_LISTENER_NAME,
            InetSocketAddress.createUnresolved("localhost", 9990 + local.id())
        );
        listenersMap.put(
            ListenerName.normalised("ANOTHER_LISTENER"),
            InetSocketAddress.createUnresolved("localhost", 8990 + local.id())
        );
        Endpoints localListeners = Endpoints.fromInetSocketAddresses(listenersMap);

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withStaticVoters(voters)
            .withElectedLeader(epoch, voter1.id())
            .withLocalListeners(localListeners)
            .build();

        // waiting for FETCH request until the UpdateRaftVoter request is set
        context.advanceTimeAndCompleteFetch(epoch, voter1.id(), true);

        context.pollUntilRequest();
        RaftRequest.Outbound updateRequest = context.assertSentUpdateVoterRequest(
            local,
            epoch,
            Feature.KRAFT_VERSION.supportedVersionRange(),
            localListeners
        );
        context.deliverResponse(
            updateRequest.correlationId(),
            updateRequest.destination(),
            context.updateVoterResponse(
                updateVoterError,
                new LeaderAndEpoch(OptionalInt.of(voter1.id()), epoch)
            )
        );
        context.client.poll();

        // after sending an update voter the next requests should be fetch and no update voter
        for (int i = 0; i < 10; i++) {
            context.time.sleep(context.fetchTimeoutMs - 1);
            context.pollUntilRequest();
            RaftRequest.Outbound fetchRequest = context.assertSentFetchRequest();
            context.assertFetchRequestData(fetchRequest, epoch, 0L, 0, context.client.highWatermark());

            context.deliverResponse(
                fetchRequest.correlationId(),
                fetchRequest.destination(),
                context.fetchResponse(
                    epoch,
                    voter1.id(),
                    MemoryRecords.EMPTY,
                    0L,
                    Errors.NONE
                )
            );
            // poll kraft to handle the fetch response
            context.client.poll();
        }
    }

    @ParameterizedTest
    @EnumSource(value = Errors.class, names = {"NONE", "UNSUPPORTED_VERSION"})
    void testFollowerSendsUpdateVoterAfterElectionWithKraftVersion0(Errors updateVoterError) throws Exception {
        ReplicaKey local = replicaKey(randomReplicaId(), true);
        ReplicaKey voter1 = replicaKey(local.id() + 1, true);
        ReplicaKey voter2 = replicaKey(local.id() + 2, true);

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, voter1, voter2));
        int epoch = 4;

        HashMap<ListenerName, InetSocketAddress> listenersMap = new HashMap<>(2);
        listenersMap.put(
            VoterSetTest.DEFAULT_LISTENER_NAME,
            InetSocketAddress.createUnresolved("localhost", 9990 + local.id())
        );
        listenersMap.put(
            ListenerName.normalised("ANOTHER_LISTENER"),
            InetSocketAddress.createUnresolved("localhost", 8990 + local.id())
        );
        Endpoints localListeners = Endpoints.fromInetSocketAddresses(listenersMap);

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withStaticVoters(voters)
            .withElectedLeader(epoch, voter1.id())
            .withLocalListeners(localListeners)
            .build();

        // waiting for FETCH request until the UpdateRaftVoter request is set
        context.advanceTimeAndCompleteFetch(epoch, voter1.id(), true);

        context.pollUntilRequest();
        RaftRequest.Outbound updateRequest = context.assertSentUpdateVoterRequest(
            local,
            epoch,
            Feature.KRAFT_VERSION.supportedVersionRange(),
            localListeners
        );
        context.deliverResponse(
            updateRequest.correlationId(),
            updateRequest.destination(),
            context.updateVoterResponse(
                updateVoterError,
                new LeaderAndEpoch(OptionalInt.of(voter1.id()), epoch)
            )
        );

        context.pollUntilRequest();
        RaftRequest.Outbound fetchRequest = context.assertSentFetchRequest();
        context.assertFetchRequestData(fetchRequest, epoch, 0L, 0, context.client.highWatermark());

        // Election a new leader causes the replica to resend update voter request
        int newEpoch = epoch + 1;
        context.deliverRequest(context.beginEpochRequest(newEpoch, voter1.id()));
        context.pollUntilResponse();

        // waiting for FETCH request until the UpdateRaftVoter request is set
        context.advanceTimeAndCompleteFetch(newEpoch, voter1.id(), true);

        context.pollUntilRequest();
        updateRequest = context.assertSentUpdateVoterRequest(
            local,
            newEpoch,
            Feature.KRAFT_VERSION.supportedVersionRange(),
            localListeners
        );
        context.deliverResponse(
            updateRequest.correlationId(),
            updateRequest.destination(),
            context.updateVoterResponse(
                Errors.NONE,
                new LeaderAndEpoch(OptionalInt.of(voter1.id()), newEpoch)
            )
        );

        context.pollUntilRequest();
        fetchRequest = context.assertSentFetchRequest();
        context.assertFetchRequestData(fetchRequest, newEpoch, 0L, 0, context.client.highWatermark());
    }

    @Test
    void testKRaftUpgradeVersion() throws Exception {
        var local = replicaKey(randomReplicaId(), true);
        var voter1 = replicaKey(local.id() + 1, true);
        var voter2  = replicaKey(local.id() + 2, true);

        VoterSet startingVoters = VoterSetTest.voterSet(
            VoterSetTest.voterMap(IntStream.of(local.id(), voter1.id(), voter2.id()), false)
        );

        var context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withRaftProtocol(RaftProtocol.KIP_853_PROTOCOL)
            .withStartingVoters(startingVoters, KRaftVersion.KRAFT_VERSION_0)
            .build();

        context.unattachedToLeader();
        var epoch = context.currentEpoch();

        // Establish a HWM and fence previous leaders
        for (var voter : List.of(voter1, voter2)) {
            context.deliverRequest(
                context.fetchRequest(epoch, voter, context.log.endOffset().offset(), epoch, 0)
            );
            context.pollUntilResponse();
            context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(local.id()));
        }

        // Update voters so that they supports kraft version 1
        for (var voter : List.of(voter1, voter2)) {
            context.deliverRequest(
                context.updateVoterRequest(
                    voter,
                    Feature.KRAFT_VERSION.supportedVersionRange(),
                    startingVoters.listeners(voter.id())
                )
            );
            context.pollUntilResponse();
            context.assertSentUpdateVoterResponse(
                Errors.NONE,
                OptionalInt.of(local.id()),
                epoch
            );
        }

        context.client.upgradeKRaftVersion(epoch, KRaftVersion.KRAFT_VERSION_1, false);
        assertEquals(KRaftVersion.KRAFT_VERSION_1, context.client.kraftVersion());

        var localLogEndOffset = context.log.endOffset().offset();
        context.client.poll();

        // check if leader writes 2 control records to the log;
        // one for the kraft version and one for the voter set
        var updatedVoters = VoterSetTest.voterSet(Stream.of(local, voter1, voter2));
        var records = context.log.read(localLogEndOffset, Isolation.UNCOMMITTED).records;
        var batch = records.batches().iterator().next();
        assertTrue(batch.isControlBatch());
        var recordsIterator = batch.iterator();
        var controlRecord = recordsIterator.next();
        verifyKRaftVersionRecord(
            KRaftVersion.KRAFT_VERSION_1.featureLevel(),
            controlRecord.key(),
            controlRecord.value()
        );
        controlRecord = recordsIterator.next();
        verifyVotersRecord(updatedVoters, controlRecord.key(), controlRecord.value());
    }

    @Test
    void testUpdateVoterAfterKRaftVersionUpgrade() throws Exception {
        var local = replicaKey(randomReplicaId(), true);
        var voter1 = replicaKey(local.id() + 1, true);
        var voter2  = replicaKey(local.id() + 2, true);

        VoterSet startingVoters = VoterSetTest.voterSet(
            VoterSetTest.voterMap(IntStream.of(local.id(), voter1.id(), voter2.id()), false)
        );

        var context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withRaftProtocol(RaftProtocol.KIP_853_PROTOCOL)
            .withStartingVoters(startingVoters, KRaftVersion.KRAFT_VERSION_0)
            .build();

        context.unattachedToLeader();
        var epoch = context.currentEpoch();

        // Establish a HWM and fence previous leaders
        for (var voter : List.of(voter1, voter2)) {
            context.deliverRequest(
                context.fetchRequest(epoch, voter, context.log.endOffset().offset(), epoch, 0)
            );
            context.pollUntilResponse();
            context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(local.id()));
        }

        // Update voters so that they supports kraft version 1
        for (var voter : List.of(voter1, voter2)) {
            context.deliverRequest(
                context.updateVoterRequest(
                    voter,
                    Feature.KRAFT_VERSION.supportedVersionRange(),
                    startingVoters.listeners(voter.id())
                )
            );
            context.pollUntilResponse();
            context.assertSentUpdateVoterResponse(
                Errors.NONE,
                OptionalInt.of(local.id()),
                epoch
            );
        }

        context.client.upgradeKRaftVersion(epoch, KRaftVersion.KRAFT_VERSION_1, false);
        assertEquals(KRaftVersion.KRAFT_VERSION_1, context.client.kraftVersion());

        // Push the control records to the log
        context.client.poll();
        // Advance the HWM to the LEO
        for (var voter : List.of(voter1, voter2)) {
            context.deliverRequest(
                context.fetchRequest(epoch, voter, context.log.endOffset().offset(), epoch, 0)
            );
            context.pollUntilResponse();
            context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(local.id()));
        }

        // Check that it can still handle update voter request after upgrade
        Endpoints newVoter1Listeners = Endpoints.fromInetSocketAddresses(
            Map.of(
                // first entry
                context.channel.listenerName(),
                InetSocketAddress.createUnresolved(
                    "localhost",
                    9990 + voter1.id()
                ),
                // second entry
                ListenerName.normalised("ANOTHER_LISTENER"),
                InetSocketAddress.createUnresolved(
                    "localhost",
                    8990 + voter1.id()
                )
            )
        );
        context.deliverRequest(
            context.updateVoterRequest(
                voter1,
                Feature.KRAFT_VERSION.supportedVersionRange(),
                newVoter1Listeners
            )
        );
        context.pollUntilResponse();
        context.assertSentUpdateVoterResponse(
            Errors.NONE,
            OptionalInt.of(local.id()),
            epoch
        );

        // Push the control records to the log
        var localLogEndOffset = context.log.endOffset().offset();
        context.client.poll();

        // check that the leader wrote voters control record to the log;
        var records = context.log.read(localLogEndOffset, Isolation.UNCOMMITTED).records;
        var batch = records.batches().iterator().next();
        assertTrue(batch.isControlBatch());
        var recordsIterator = batch.iterator();
        var controlRecord = recordsIterator.next();
        assertEquals(ControlRecordType.KRAFT_VOTERS, ControlRecordType.parse(controlRecord.key()));
        ControlRecordUtils.deserializeVotersRecord(controlRecord.value());
    }

    @Test
    void testInvalidKRaftUpgradeVersion() throws Exception {
        var local = replicaKey(randomReplicaId(), true);
        var voter1 = replicaKey(local.id() + 1, true);
        var voter2  = replicaKey(local.id() + 2, true);

        VoterSet startingVoters = VoterSetTest.voterSet(
            VoterSetTest.voterMap(IntStream.of(local.id(), voter1.id(), voter2.id()), false)
        );

        var context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withRaftProtocol(RaftProtocol.KIP_853_PROTOCOL)
            .withStartingVoters(startingVoters, KRaftVersion.KRAFT_VERSION_0)
            .build();

        context.unattachedToLeader();
        var epoch = context.currentEpoch();

        // Upgrade not allowed since none of the remote voters support the new version
        assertEquals(KRaftVersion.KRAFT_VERSION_0, context.client.kraftVersion());
        assertThrows(
            InvalidUpdateVersionException.class,
            () -> context.client.upgradeKRaftVersion(epoch, KRaftVersion.KRAFT_VERSION_1, false)
        );

        // Establish a HWM and fence previous leaders
        for (var voter : List.of(voter1, voter2)) {
            context.deliverRequest(
                context.fetchRequest(epoch, voter, context.log.endOffset().offset(), epoch, 0)
            );
            context.pollUntilResponse();
            context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(local.id()));
        }

        // Update only one of the voters so that they supports kraft version 1
        context.deliverRequest(
            context.updateVoterRequest(
                voter1,
                Feature.KRAFT_VERSION.supportedVersionRange(),
                startingVoters.listeners(voter1.id())
            )
        );
        context.pollUntilResponse();
        context.assertSentUpdateVoterResponse(
            Errors.NONE,
            OptionalInt.of(local.id()),
            epoch
        );

        // Upgrade not allowed since one of the voters doesn't support the new version
        assertEquals(KRaftVersion.KRAFT_VERSION_0, context.client.kraftVersion());
        assertThrows(
            InvalidUpdateVersionException.class,
            () -> context.client.upgradeKRaftVersion(epoch, KRaftVersion.KRAFT_VERSION_1, false)
        );
    }

    @Test
    void testFollowerSendsUpdateVoterWhenDifferent() throws Exception {
        ReplicaKey local = replicaKey(randomReplicaId(), true);
        ReplicaKey voter1 = replicaKey(local.id() + 1, true);
        ReplicaKey voter2 = replicaKey(local.id() + 2, true);

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, voter1, voter2));
        int epoch = 4;

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .withElectedLeader(epoch, voter1.id())
            .build();

        // waiting for FETCH request until the UpdateRaftVoter request is set
        context.advanceTimeAndCompleteFetch(epoch, voter1.id(), true);

        // update voter should not be sent because the local listener is not different from the voter set
        context.pollUntilRequest();
        RaftRequest.Outbound fetchRequest = context.assertSentFetchRequest();
        context.assertFetchRequestData(fetchRequest, epoch, 0L, 0, context.client.highWatermark());

        // after more than 3 fetch timeouts the update voter period timer should have expired.
        // check that the update voter period timer doesn't remain at zero (0) and cause the message queue to get
        // called with a zero (0) timeout and result in a busy-loop.
        assertNotEquals(OptionalLong.of(0L), context.messageQueue.lastPollTimeoutMs());
    }

    @Test
    void testFollowerSendsUpdateVoterIfPendingFetchDuringTimeout() throws Exception {
        ReplicaKey local = replicaKey(randomReplicaId(), true);
        ReplicaKey voter1 = replicaKey(local.id() + 1, true);
        ReplicaKey voter2 = replicaKey(local.id() + 2, true);

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, voter1, voter2));
        int epoch = 4;

        HashMap<ListenerName, InetSocketAddress> listenersMap = new HashMap<>(2);
        listenersMap.put(
            VoterSetTest.DEFAULT_LISTENER_NAME,
            InetSocketAddress.createUnresolved("localhost", 9990 + local.id())
        );
        listenersMap.put(
            ListenerName.normalised("ANOTHER_LISTENER"),
            InetSocketAddress.createUnresolved("localhost", 8990 + local.id())
        );
        Endpoints localListeners = Endpoints.fromInetSocketAddresses(listenersMap);

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .withElectedLeader(epoch, voter1.id())
            .withLocalListeners(localListeners)
            .build();

        // waiting up to the last FETCH request before the UpdateRaftVoter request is set
        context.advanceTimeAndCompleteFetch(epoch, voter1.id(), false);

        // expect one last FETCH request
        context.pollUntilRequest();
        RaftRequest.Outbound fetchRequest = context.assertSentFetchRequest();
        context.assertFetchRequestData(fetchRequest, epoch, 0L, 0, context.client.highWatermark());

        // don't send a response but increase the time
        context.time.sleep(context.requestTimeoutMs() - 1);
        context.client.poll();
        assertFalse(context.channel.hasSentRequests());

        // expect an update voter request after the FETCH rpc completes
        context.deliverResponse(
            fetchRequest.correlationId(),
            fetchRequest.destination(),
            context.fetchResponse(
                epoch,
                voter1.id(),
                MemoryRecords.EMPTY,
                0L,
                Errors.NONE
            )
        );
        context.pollUntilRequest();
        context.assertSentUpdateVoterRequest(
            local,
            epoch,
            Feature.KRAFT_VERSION.supportedVersionRange(),
            localListeners
        );
    }

    @Test
    void testUpdateVoterResponseCausesEpochChange() throws Exception {
        ReplicaKey local = replicaKey(randomReplicaId(), true);
        ReplicaKey voter1 = replicaKey(local.id() + 1, true);
        ReplicaKey voter2 = replicaKey(local.id() + 2, true);

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, voter1, voter2));
        int epoch = 4;

        HashMap<ListenerName, InetSocketAddress> listenersMap = new HashMap<>(2);
        listenersMap.put(
            VoterSetTest.DEFAULT_LISTENER_NAME,
            InetSocketAddress.createUnresolved("localhost", 9990 + local.id())
        );
        listenersMap.put(
            ListenerName.normalised("ANOTHER_LISTENER"),
            InetSocketAddress.createUnresolved("localhost", 8990 + local.id())
        );
        Endpoints localListeners = Endpoints.fromInetSocketAddresses(listenersMap);

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .withElectedLeader(epoch, voter1.id())
            .withLocalListeners(localListeners)
            .build();

        // waiting for FETCH request until the UpdateRaftVoter request is set
        context.advanceTimeAndCompleteFetch(epoch, voter1.id(), true);

        context.pollUntilRequest();
        RaftRequest.Outbound updateRequest = context.assertSentUpdateVoterRequest(
            local,
            epoch,
            Feature.KRAFT_VERSION.supportedVersionRange(),
            localListeners
        );
        context.deliverResponse(
            updateRequest.correlationId(),
            updateRequest.destination(),
            context.updateVoterResponse(
                Errors.NONE,
                new LeaderAndEpoch(OptionalInt.of(voter2.id()), epoch + 1)
            )
        );

        // check that there is a fetch to the new leader
        context.pollUntilRequest();
        RaftRequest.Outbound fetchRequest = context.assertSentFetchRequest();
        context.assertFetchRequestData(fetchRequest, epoch + 1, 0L, 0, context.client.highWatermark());
        assertEquals(voter2.id(), fetchRequest.destination().id());
    }

    @Test
    void testObserverDiscoversLeaderWithUnknownVoters() throws Exception {
        ReplicaKey local = replicaKey(randomReplicaId(), true);
        InetSocketAddress bootstrapAddress = InetSocketAddress.createUnresolved("localhost", 1234);
        int epoch = 3;

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.empty())
            .withUnknownLeader(epoch)
            .withBootstrapServers(Optional.of(List.of(bootstrapAddress)))
            .build();

        context.pollUntilRequest();
        RaftRequest.Outbound fetchRequest = context.assertSentFetchRequest();
        context.assertFetchRequestData(fetchRequest, epoch, 0L, 0, context.client.highWatermark());
        assertEquals(-2, fetchRequest.destination().id());
    }

    @Test
    public void testHandleBeginQuorumRequestMoreEndpoints() throws Exception {
        ReplicaKey local = replicaKey(randomReplicaId(), true);
        ReplicaKey leader = replicaKey(local.id() + 1, true);
        int leaderEpoch = 3;

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, leader));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withBootstrapSnapshot(Optional.of(voters))
            .withElectedLeader(leaderEpoch, leader.id())
            .withKip853Rpc(true)
            .build();

        context.client.poll();

        HashMap<ListenerName, InetSocketAddress> leaderListenersMap = new HashMap<>(2);
        leaderListenersMap.put(
            VoterSetTest.DEFAULT_LISTENER_NAME,
            InetSocketAddress.createUnresolved("localhost", 9990 + leader.id())
        );
        leaderListenersMap.put(
            ListenerName.normalised("ANOTHER_LISTENER"),
            InetSocketAddress.createUnresolved("localhost", 8990 + leader.id())
        );
        Endpoints leaderEndpoints = Endpoints.fromInetSocketAddresses(leaderListenersMap);

        context.deliverRequest(context.beginEpochRequest(leaderEpoch, leader.id(), leaderEndpoints));
        context.pollUntilResponse();

        context.assertElectedLeader(leaderEpoch, leader.id());

        context.assertSentBeginQuorumEpochResponse(
            Errors.NONE,
            leaderEpoch,
            OptionalInt.of(leader.id())
        );
    }

    @Test
    public void testLeaderMetricsAreReset() throws Exception {
        ReplicaKey local = replicaKey(randomReplicaId(), true);
        ReplicaKey follower = replicaKey(local.id() + 1, true);

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, follower));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .withUnknownLeader(3)
            .build();

        context.unattachedToLeader();
        int epoch = context.currentEpoch();

        checkLeaderMetricValues(2, 0, 0, context);

        ReplicaKey newVoter = replicaKey(local.id() + 2, true);
        InetSocketAddress newAddress = InetSocketAddress.createUnresolved(
            "localhost",
            9990 + newVoter.id()
        );
        Endpoints newListeners = Endpoints.fromInetSocketAddresses(
            Map.of(context.channel.listenerName(), newAddress)
        );

        // Establish a HWM and fence previous leaders
        context.deliverRequest(
            context.fetchRequest(epoch, follower, context.log.endOffset().offset(), epoch, 0)
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(local.id()));

        // Catch up the new voter to the leader's LEO, the new voter is still an observer at this point
        context.deliverRequest(
            context.fetchRequest(epoch, newVoter, context.log.endOffset().offset(), epoch, 0)
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(local.id()));

        // Attempt to add new voter to the quorum
        context.deliverRequest(context.addVoterRequest(Integer.MAX_VALUE, newVoter, newListeners));

        // Leader should send an API_VERSIONS request to the new voter's endpoint
        context.pollUntilRequest();
        checkLeaderMetricValues(2, 1, 1, context);
        RaftRequest.Outbound apiVersionRequest = context.assertSentApiVersionsRequest();
        assertEquals(
            new Node(newVoter.id(), newAddress.getHostString(), newAddress.getPort()),
            apiVersionRequest.destination()
        );

        // Leader completes the AddVoter RPC when resigning
        context.client.resign(epoch);
        context.pollUntilResponse();
        context.assertSentAddVoterResponse(Errors.NOT_LEADER_OR_FOLLOWER);

        assertEquals(2, getMetric(context.metrics, "number-of-voters").metricValue());
        assertNull(getMetric(context.metrics, "number-of-observers"));
        assertNull(getMetric(context.metrics, "uncommitted-voter-change"));

        context.pollUntilRequest();
        RaftRequest.Outbound request = context.assertSentEndQuorumEpochRequest(epoch, follower.id());

        EndQuorumEpochResponseData response = context.endEpochResponse(
            epoch,
            OptionalInt.of(local.id())
        );

        context.deliverResponse(request.correlationId(), request.destination(), response);
        context.client.poll();

        // Bump the context epoch after resignation completes
        context.time.sleep(context.electionTimeoutMs() * 2L);
        context.client.poll();
        assertEquals(epoch + 1, context.currentEpoch());

        // Become leader again and verify metrics values have been reset
        context.unattachedToLeader();
        checkLeaderMetricValues(2, 0, 0, context);
    }

    private static void verifyVotersRecord(
        VoterSet expectedVoterSet,
        ByteBuffer recordKey,
        ByteBuffer recordValue
    ) {
        assertEquals(ControlRecordType.KRAFT_VOTERS, ControlRecordType.parse(recordKey));
        VotersRecord votersRecord = ControlRecordUtils.deserializeVotersRecord(recordValue);
        assertEquals(
            expectedVoterSet,
            VoterSet.fromVotersRecord(votersRecord)
        );
    }

    private static void verifyKRaftVersionRecord(
        short expectedKRaftVersion,
        ByteBuffer recordKey,
        ByteBuffer recordValue
    ) {
        assertEquals(ControlRecordType.KRAFT_VERSION, ControlRecordType.parse(recordKey));
        KRaftVersionRecord kRaftVersionRecord = ControlRecordUtils.deserializeKRaftVersionRecord(recordValue);
        assertEquals(expectedKRaftVersion, kRaftVersionRecord.kRaftVersion());
    }

    private int randomReplicaId() {
        return ThreadLocalRandom.current().nextInt(1025);
    }

    private static ApiVersionsResponseData apiVersionsResponse(Errors error) {
        return apiVersionsResponse(error, Feature.KRAFT_VERSION.supportedVersionRange());
    }

    private static ApiVersionsResponseData apiVersionsResponse(Errors error, SupportedVersionRange supportedVersions) {
        ApiVersionsResponseData.SupportedFeatureKeyCollection supportedFeatures =
            new ApiVersionsResponseData.SupportedFeatureKeyCollection(1);

        if (supportedVersions.max() > 0) {
            supportedFeatures.add(
                new ApiVersionsResponseData.SupportedFeatureKey()
                    .setName(KRaftVersion.FEATURE_NAME)
                    .setMinVersion(supportedVersions.min())
                    .setMaxVersion(supportedVersions.max())
            );
        }

        return new ApiVersionsResponseData()
            .setErrorCode(error.code())
            .setSupportedFeatures(supportedFeatures);
    }

    private static KafkaMetric getMetric(final Metrics metrics, final String name) {
        return metrics.metrics().get(metrics.metricName(name, "raft-metrics"));
    }

    private static void checkLeaderMetricValues(
        final int expectedVoters,
        final int expectedObservers,
        final int expectedUncommittedVoterChange,
        final RaftClientTestContext context
    ) {
        assertEquals(expectedVoters, getMetric(context.metrics, "number-of-voters").metricValue());
        assertEquals(expectedObservers, getMetric(context.metrics, "number-of-observers").metricValue());
        assertEquals(expectedUncommittedVoterChange, getMetric(context.metrics, "uncommitted-voter-change").metricValue());

        // This metric should not be updated in these tests because the context is set up with
        // bootstrap records that initialize the voter set
        Mockito.verifyNoInteractions(context.externalKRaftMetrics);
    }
}
