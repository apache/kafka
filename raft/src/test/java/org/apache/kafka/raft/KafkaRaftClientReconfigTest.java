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
import org.apache.kafka.common.message.KRaftVersionRecord;
import org.apache.kafka.common.message.SnapshotFooterRecord;
import org.apache.kafka.common.message.SnapshotHeaderRecord;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.ControlRecordUtils;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.raft.internals.ReplicaKey;
import org.apache.kafka.raft.internals.VoterSet;
import org.apache.kafka.raft.internals.VoterSetTest;
import org.apache.kafka.snapshot.RawSnapshotReader;
import org.apache.kafka.snapshot.SnapshotWriterReaderTest;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KafkaRaftClientReconfigTest {
    public static final OffsetAndEpoch BOOTSTRAP_SNAPSHOT_ID = new OffsetAndEpoch(0, 0);

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testLeaderWritesBootstrapRecords(boolean withKip853Rpc) throws Exception {
        int localId = 0;
        int follower = 1;
        int observer = 2;
        Uuid localDirectoryId = Uuid.randomUuid();
        Uuid followerDirectoryId = Uuid.randomUuid();
        Uuid observerDirectoryId = Uuid.randomUuid();
        Set<Integer> voterIds = new HashSet<>(Arrays.asList(localId, follower));
        Set<ReplicaKey> voters = new HashSet<>(Arrays.asList(
            ReplicaKey.of(localId, localDirectoryId),
            ReplicaKey.of(follower, followerDirectoryId)));
        VoterSet voterSet = VoterSetTest.voterSet(voters.stream());
        short kRaftVersion = 1;
        int epoch = 0;

        RaftClientTestContext context = new RaftClientTestContext.Builder(OptionalInt.of(localId), voterIds, localDirectoryId, kRaftVersion)
            .withKip853Rpc(withKip853Rpc)
            .withBootstrapSnapshot(Optional.of(voterSet), kRaftVersion)
            .appendToLog(epoch, Collections.singletonList("first_record"))
            .withUnknownLeader(epoch)
            .build();

        // check the bootstrap snapshot exists and contains the expected records
        assertEquals(BOOTSTRAP_SNAPSHOT_ID, context.log.latestSnapshotId().get());
        RawSnapshotReader snapshot = context.log.latestSnapshot().get();
        List<String> expectedBootstrapRecords = new ArrayList<>();
        expectedBootstrapRecords.add(new SnapshotHeaderRecord()
            .setVersion((short) 0)
            .setLastContainedLogTimestamp(0).toString());
        expectedBootstrapRecords.add(new KRaftVersionRecord()
            .setVersion(ControlRecordUtils.KRAFT_VERSION_CURRENT_VERSION)
            .setKRaftVersion((short) 1).toString());
        expectedBootstrapRecords.add(voterSet.toVotersRecord(ControlRecordUtils.KRAFT_VOTERS_CURRENT_VERSION).toString());
        expectedBootstrapRecords.add(new SnapshotFooterRecord().setVersion((short) 0).toString());
        SnapshotWriterReaderTest.assertControlSnapshot(Collections.singletonList(expectedBootstrapRecords), snapshot);

        // check if leader writes 3 bootstrap records to log after first record
        context.becomeLeader();
        Records records = context.log.read(1, Isolation.UNCOMMITTED).records;
        RecordBatch batch = records.batches().iterator().next();
        assertTrue(batch.isControlBatch());
        Iterator<Record> recordIterator = batch.iterator();
        Record record = recordIterator.next();
        RaftClientTestContext.verifyLeaderChangeMessage(localId, Arrays.asList(localId, follower),
            Arrays.asList(localId, follower), record.key(), record.value());
        record = recordIterator.next();
        RaftClientTestContext.verifyKRaftVersionRecord(kRaftVersion, record.key(), record.value());
        record = recordIterator.next();
        RaftClientTestContext.verifyVotersRecord(voterIds, record.key(), record.value());

        // check that leader sends records to follower
        epoch = context.currentEpoch();
        long localLogEndOffset = context.log.endOffset().offset;
        context.deliverRequest(
            context.fetchRequest(
                epoch,
                ReplicaKey.of(follower, followerDirectoryId),
                localLogEndOffset,
                epoch,
                0
            )
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(localId));

        // check that leader does not respond with bootstrap snapshot id when follower fetches offset 0
        context.deliverRequest(
            context.fetchRequest(
                epoch,
                ReplicaKey.of(observer, observerDirectoryId),
                0,
                0,
                0
            )
        );
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(localId));

        // check no error if follower requests bootstrap snapshot from the leader
        // follower will fail to apply snapshot over its existing snapshot (handled in testCreateExistingSnapshot)
        context.deliverRequest(
            KafkaRaftClientSnapshotTest.fetchSnapshotRequest(
                context.metadataPartition,
                epoch,
                BOOTSTRAP_SNAPSHOT_ID,
                Integer.MAX_VALUE,
                0
            )
        );
        context.pollUntilResponse();
        context.assertSentFetchSnapshotResponse(Errors.NONE);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testLeaderDoesNotBootstrapRecordsWithKraftVersion0(boolean withKip853Rpc) throws Exception {
        int localId = 0;
        int follower = 1;
        Uuid localDirectoryId = Uuid.randomUuid();
        Uuid followerDirectoryId = Uuid.randomUuid();
        Set<Integer> voterIds = new HashSet<>(Arrays.asList(localId, follower));
        Set<ReplicaKey> voters = new HashSet<>(Arrays.asList(
            ReplicaKey.of(localId, localDirectoryId),
            ReplicaKey.of(follower, followerDirectoryId)));
        VoterSet voterSet = VoterSetTest.voterSet(voters.stream());
        short kRaftVersion = 0;
        int epoch = 0;

        RaftClientTestContext context = new RaftClientTestContext.Builder(OptionalInt.of(localId), voterIds, localDirectoryId, kRaftVersion)
            .withKip853Rpc(withKip853Rpc)
            .withBootstrapSnapshot(Optional.of(voterSet), kRaftVersion)
            .withUnknownLeader(epoch)
            .build();

        // check the bootstrap snapshot exists but is empty
        assertEquals(BOOTSTRAP_SNAPSHOT_ID, context.log.latestSnapshotId().get());
        RawSnapshotReader snapshot = context.log.latestSnapshot().get();
        List<String> expectedBootstrapRecords = Collections.emptyList();
        SnapshotWriterReaderTest.assertControlSnapshot(Collections.singletonList(expectedBootstrapRecords), snapshot);

        // check leader does not write bootstrap records to log
        context.becomeLeader();
        Records records = context.log.read(0, Isolation.UNCOMMITTED).records;
        RecordBatch batch = records.batches().iterator().next();
        assertTrue(batch.isControlBatch());
        Iterator<Record> recordIterator = batch.iterator();
        Record record = recordIterator.next();
        RaftClientTestContext.verifyLeaderChangeMessage(localId, Arrays.asList(localId, follower),
            Arrays.asList(localId, follower), record.key(), record.value());
        assertFalse(recordIterator.hasNext());
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testFollowerDoesNotRequestLeaderBootstrapSnapshot(boolean withKip853Rpc) throws Exception {
        int localId = 0;
        int leader = 1;
        Uuid localDirectoryId = Uuid.randomUuid();
        Uuid leaderDirectoryId = Uuid.randomUuid();
        Set<Integer> voterIds = new HashSet<>(Arrays.asList(localId, leader));
        Set<ReplicaKey> voters = new HashSet<>(Arrays.asList(
            ReplicaKey.of(localId, localDirectoryId),
            ReplicaKey.of(leader, leaderDirectoryId)));
        VoterSet voterSet = VoterSetTest.voterSet(voters.stream());
        short kRaftVersion = 1;
        int epoch = 0;

        RaftClientTestContext context = new RaftClientTestContext.Builder(OptionalInt.of(localId), voterIds, localDirectoryId, kRaftVersion)
            .withKip853Rpc(withKip853Rpc)
            .withElectedLeader(epoch, leader)
            .withBootstrapSnapshot(Optional.of(voterSet), kRaftVersion)
            .build();

        // check that follower will send fetch request to leader
        context.pollUntilRequest();
        RaftRequest.Outbound fetchRequest = context.assertSentFetchRequest();
        context.assertFetchRequestData(fetchRequest, epoch, 0L, 0);

        // check if leader response were to contain bootstrap snapshot id, follower would not send fetch snapshot request
        context.deliverResponse(
            fetchRequest.correlationId(),
            fetchRequest.destination(),
            context.snapshotFetchResponse(epoch, leader, BOOTSTRAP_SNAPSHOT_ID, 0)
        );
        context.pollUntilRequest();
        fetchRequest = context.assertSentFetchRequest();
        context.assertFetchRequestData(fetchRequest, epoch, 0L, 0);
    }

    // this test is maybe out of scope - we are checking that follower is able to apply votersRecord essentially
    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testFollowerReadsKRaftBootstrapRecords(boolean withKip853Rpc) throws Exception {
        int localId = 0;
        int leader = 1;
        int follower2 = 2;
        Uuid localDirectoryId = Uuid.randomUuid();
        Uuid leaderDirectoryId = Uuid.randomUuid();
        Uuid followerDirectoryId2 = Uuid.randomUuid();
        Set<Integer> voterIds = new HashSet<>(Arrays.asList(localId, leader));
        Set<ReplicaKey> voters = new HashSet<>(Arrays.asList(
            ReplicaKey.of(localId, localDirectoryId),
            ReplicaKey.of(leader, leaderDirectoryId)));
        VoterSet voterSet = VoterSetTest.voterSet(voters.stream());
        short kRaftVersion = 1;
        int epoch = 5;

        RaftClientTestContext context = new RaftClientTestContext.Builder(OptionalInt.of(localId), voterIds, localDirectoryId, kRaftVersion)
            .withKip853Rpc(withKip853Rpc)
            .withElectedLeader(epoch, leader)
            .withBootstrapSnapshot(Optional.of(voterSet), kRaftVersion)
            .appendToLog(epoch, Arrays.asList("a", "b", "c"))
            .build();

        // check that follower will send fetch request to leader
        context.pollUntilRequest();
        RaftRequest.Outbound fetchRequest = context.assertSentFetchRequest();
        context.assertFetchRequestData(fetchRequest, epoch, 3L, 5);

        // leader sends batch with bootstrap records
        Set<ReplicaKey> leadersVoters = new HashSet<>(voters);
        leadersVoters.add(ReplicaKey.of(follower2, followerDirectoryId2));
        VoterSet leadersVoterSet = VoterSetTest.voterSet(leadersVoters.stream());
        List<String> leaderRecords = Arrays.asList(leadersVoterSet.toVotersRecord(ControlRecordUtils.KRAFT_VOTERS_CURRENT_VERSION).toString());
        MemoryRecords batch = context.buildControlBatch(3L, epoch, leaderRecords);
        context.deliverResponse(
            fetchRequest.correlationId(),
            fetchRequest.destination(),
            context.fetchResponse(epoch, leader, batch, 3, Errors.NONE));

        // this is broken because RaftClientTestContext.buildBatch does not send records in viable format
        // follower applies the bootstrap records, registering follower2 as a new voter
        context.client.poll();
        assertTrue(context.client.quorum().isVoter(ReplicaKey.of(follower2, followerDirectoryId2)));
    }
}
