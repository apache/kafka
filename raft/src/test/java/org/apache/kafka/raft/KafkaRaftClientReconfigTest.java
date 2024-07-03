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
import org.apache.kafka.snapshot.SnapshotWriter;
import org.apache.kafka.snapshot.SnapshotWriterReaderTest;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

import static org.apache.kafka.raft.KafkaRaftClientSnapshotTest.fetchSnapshotResponse;
import static org.apache.kafka.raft.KafkaRaftClientSnapshotTest.snapshotWriter;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KafkaRaftClientReconfigTest {
    public static final OffsetAndEpoch BOOTSTRAP_SNAPSHOT_ID = new OffsetAndEpoch(0, 0);

    @Test
    public void testLeaderWritesKRaftBootstrapRecords() throws Exception {
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
            .withBootstrapSnapshot(Optional.of(voterSet), kRaftVersion)
            .appendToLog(epoch, Collections.singletonList("first_record"))
            .withUnknownLeader(epoch)
            .build();

        // check the bootstrap snapshot/checkpoint exist
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

        // check that leader sends bootstrap snapshot to observer
        // leader responds with bootstrap snapshotId when fetching offset 0
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
        context.assertSentFetchPartitionResponseWithSnapshotId(Errors.NONE, epoch, OptionalInt.of(localId), 0, 0);
        // leader responds with snapshot
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

    // follower that is voter, or observer
    // follower either has an empty or full 0-0.checkpoint
    @Test
    public void testFollowerReadsKRaftBootstrapSnapshot() throws Exception {
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
        int epoch = 0;

        RaftClientTestContext context = new RaftClientTestContext.Builder(OptionalInt.of(localId), voterIds, localDirectoryId, kRaftVersion)
            .withElectedLeader(epoch, leader)
            .withBootstrapSnapshot(Optional.of(voterSet), kRaftVersion)
            .build();

        // check that follower will send fetch request to leader
        context.pollUntilRequest();
        RaftRequest.Outbound fetchRequest = context.assertSentFetchRequest();
        context.assertFetchRequestData(fetchRequest, epoch, 0L, 0);

        // leader responds with its bootstrap snapshotId
        context.deliverResponse(
            fetchRequest.correlationId(),
            fetchRequest.destination(),
            context.snapshotFetchResponse(epoch, leader, BOOTSTRAP_SNAPSHOT_ID, 0)
        );

        // check follower will send fetch snapshot request to leader, even though follower has 0-0.checkpoint locally
        context.pollUntilRequest();
        RaftRequest.Outbound fetchSnapshotRequest = context.assertSentFetchSnapshotRequest();
        KafkaRaftClientSnapshotTest.assertFetchSnapshotRequest(
            fetchSnapshotRequest,
            context.metadataPartition,
            localId,
            Integer.MAX_VALUE
        );

        // leader would respond with its bootstrap snapshot, differing from local's snapshot by an additional follower
        Set<ReplicaKey> leadersVoters = new HashSet<>(voters);
        leadersVoters.add(ReplicaKey.of(follower2, followerDirectoryId2));
        VoterSet leadersVoterSet = VoterSetTest.voterSet(leadersVoters.stream());
        List<String> records = bootstrapSnapshotRecords(leadersVoterSet);
        KafkaRaftClientSnapshotTest.MemorySnapshotWriter memorySnapshot = new KafkaRaftClientSnapshotTest.MemorySnapshotWriter(BOOTSTRAP_SNAPSHOT_ID);
        try (SnapshotWriter<String> snapshotWriter = snapshotWriter(context, memorySnapshot)) {
            snapshotWriter.append(records);
            snapshotWriter.freeze();
        }

        context.deliverResponse(
            fetchSnapshotRequest.correlationId(),
            fetchSnapshotRequest.destination(),
            fetchSnapshotResponse(
                context,
                epoch,
                leader,
                BOOTSTRAP_SNAPSHOT_ID,
                memorySnapshot.buffer().remaining(),
                0L,
                memorySnapshot.buffer().slice()
            )
        );

        // check follower applies the snapshot, registering follower2 as a new voter
        context.client.poll();
        assertTrue(context.client.quorum().isVoter(ReplicaKey.of(follower2, followerDirectoryId2)));
    }

    @Test
    public void testFollowerReadsKRaftBootstrapRecords() throws Exception {
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
        MemoryRecords batch = context.buildBatch(3L, epoch, leaderRecords);
        context.deliverResponse(
            fetchRequest.correlationId(),
            fetchRequest.destination(),
            context.fetchResponse(epoch, leader, batch, 3, Errors.NONE));

        // follower applies the bootstrap records, registering follower2 as a new voter
        context.client.poll();
        assertTrue(context.client.quorum().isVoter(ReplicaKey.of(follower2, followerDirectoryId2)));
    }

    private static List<String> bootstrapSnapshotRecords(VoterSet voterSet) {
        List<String> expectedBootstrapRecords = new ArrayList<>();
        expectedBootstrapRecords.add(new SnapshotHeaderRecord()
            .setVersion((short) 0)
            .setLastContainedLogTimestamp(0).toString());
        expectedBootstrapRecords.add(new KRaftVersionRecord()
            .setVersion(ControlRecordUtils.KRAFT_VERSION_CURRENT_VERSION)
            .setKRaftVersion((short) 1).toString());
        expectedBootstrapRecords.add(voterSet.toVotersRecord(ControlRecordUtils.KRAFT_VOTERS_CURRENT_VERSION).toString());
        expectedBootstrapRecords.add(new SnapshotFooterRecord().setVersion((short) 0).toString());
        return expectedBootstrapRecords;
    }

}
