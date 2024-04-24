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
package org.apache.kafka.raft.internals;

import java.util.Arrays;
import java.util.Optional;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.KRaftVersionRecord;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.raft.MockLog;
import org.apache.kafka.raft.OffsetAndEpoch;
import org.apache.kafka.server.common.serialization.RecordSerde;
import org.apache.kafka.snapshot.RecordsSnapshotWriter;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

final class PartitionListenerTest {
    private static final RecordSerde<String> STRING_SERDE = new StringSerde();

    private static MockLog buildLog() {
        return new MockLog(new TopicPartition("partition", 0), Uuid.randomUuid(), new LogContext());
    }

    private static PartitionListener buildPartitionListener(MockLog log, Optional<VoterSet> staticVoterSet) {
        return new PartitionListener(
            staticVoterSet,
            log,
            STRING_SERDE,
            BufferSupplier.NO_CACHING,
            1024,
            new LogContext()
        );
    }

    @Test
    void testEmptyParition() {
        MockLog log = buildLog();
        VoterSet voterSet = VoterSetTest.voterSet(VoterSetTest.voterMap(Arrays.asList(1, 2, 3), true));

        PartitionListener partitionListener = buildPartitionListener(log, Optional.of(voterSet));

        // This should be a no-op operation
        partitionListener.updateListener();

        assertEquals(voterSet, partitionListener.lastVoterSet());
    }

    @Test
    void testUpdateWithoutSnapshot() {
        MockLog log = buildLog();
        VoterSet staticVoterSet = VoterSetTest.voterSet(VoterSetTest.voterMap(Arrays.asList(1, 2, 3), true));
        BufferSupplier bufferSupplier = BufferSupplier.NO_CACHING;
        int epoch = 1;

        PartitionListener partitionListener = buildPartitionListener(log, Optional.of(staticVoterSet));

        // Append the kraft.version control record
        short kraftVersion = 1;
        log.appendAsLeader(
            MemoryRecords.withKRaftVersionRecord(
                log.endOffset().offset,
                0,
                epoch,
                bufferSupplier.get(300),
                new KRaftVersionRecord().setKRaftVersion(kraftVersion)
            ),
            epoch
        );

        // Append the voter set control record
        VoterSet voterSet = VoterSetTest.voterSet(VoterSetTest.voterMap(Arrays.asList(4, 5, 6), true));
        log.appendAsLeader(
            MemoryRecords.withVotersRecord(
                log.endOffset().offset,
                0,
                epoch,
                bufferSupplier.get(300),
                voterSet.toVotersRecord((short) 0)
            ),
            epoch
        );

        // Read the entire partition
        partitionListener.updateListener();

        assertEquals(voterSet, partitionListener.lastVoterSet());
        assertEquals(Optional.of(voterSet), partitionListener.voterSetAtOffset(log.endOffset().offset - 1));
        assertEquals(kraftVersion, partitionListener.kraftVersionAtOffset(log.endOffset().offset - 1));
    }

    @Test
    void testUpdateWithEmptySnapshot() {
        MockLog log = buildLog();
        VoterSet staticVoterSet = VoterSetTest.voterSet(VoterSetTest.voterMap(Arrays.asList(1, 2, 3), true));
        BufferSupplier bufferSupplier = BufferSupplier.NO_CACHING;
        int epoch = 1;

        PartitionListener partitionListener = buildPartitionListener(log, Optional.of(staticVoterSet));

        // Create a snapshot that doesn't have any kraft.version or voter set control records
        RecordsSnapshotWriter.Builder builder = new RecordsSnapshotWriter.Builder()
            .setRawSnapshotWriter(log.createNewSnapshotUnchecked(new OffsetAndEpoch(10, epoch)).get());
        try (RecordsSnapshotWriter<?> writer = builder.build(STRING_SERDE)) {
            writer.freeze();
        }
        log.truncateToLatestSnapshot();

        // Append the kraft.version control record
        short kraftVersion = 1;
        log.appendAsLeader(
            MemoryRecords.withKRaftVersionRecord(
                log.endOffset().offset,
                0,
                epoch,
                bufferSupplier.get(300),
                new KRaftVersionRecord().setKRaftVersion(kraftVersion)
            ),
            epoch
        );

        // Append the voter set control record
        VoterSet voterSet = VoterSetTest.voterSet(VoterSetTest.voterMap(Arrays.asList(4, 5, 6), true));
        log.appendAsLeader(
            MemoryRecords.withVotersRecord(
                log.endOffset().offset,
                0,
                epoch,
                bufferSupplier.get(300),
                voterSet.toVotersRecord((short) 0)
            ),
            epoch
        );

        // Read the entire partition
        partitionListener.updateListener();

        assertEquals(voterSet, partitionListener.lastVoterSet());
        assertEquals(Optional.of(voterSet), partitionListener.voterSetAtOffset(log.endOffset().offset - 1));
        assertEquals(kraftVersion, partitionListener.kraftVersionAtOffset(log.endOffset().offset - 1));
    }

    @Test
    void testUpdateWithSnapshot() {
        MockLog log = buildLog();
        VoterSet staticVoterSet = VoterSetTest.voterSet(VoterSetTest.voterMap(Arrays.asList(1, 2, 3), true));
        int epoch = 1;

        PartitionListener partitionListener = buildPartitionListener(log, Optional.of(staticVoterSet));

        // Create a snapshot that has kraft.version and voter set control records
        short kraftVersion = 1;
        VoterSet voterSet = VoterSetTest.voterSet(VoterSetTest.voterMap(Arrays.asList(4, 5, 6), true));

        RecordsSnapshotWriter.Builder builder = new RecordsSnapshotWriter.Builder()
            .setRawSnapshotWriter(log.createNewSnapshotUnchecked(new OffsetAndEpoch(10, epoch)).get())
            .setKraftVersion(kraftVersion)
            .setVoterSet(Optional.of(voterSet));
        try (RecordsSnapshotWriter<?> writer = builder.build(STRING_SERDE)) {
            writer.freeze();
        }
        log.truncateToLatestSnapshot();

        // Read the entire partition
        partitionListener.updateListener();

        assertEquals(voterSet, partitionListener.lastVoterSet());
        assertEquals(Optional.of(voterSet), partitionListener.voterSetAtOffset(log.endOffset().offset - 1));
        assertEquals(kraftVersion, partitionListener.kraftVersionAtOffset(log.endOffset().offset - 1));
    }

    @Test
    void testUpdateWithSnapshotAndLogOverride() {
        MockLog log = buildLog();
        VoterSet staticVoterSet = VoterSetTest.voterSet(VoterSetTest.voterMap(Arrays.asList(1, 2, 3), true));
        BufferSupplier bufferSupplier = BufferSupplier.NO_CACHING;
        int epoch = 1;

        PartitionListener partitionListener = buildPartitionListener(log, Optional.of(staticVoterSet));

        // Create a snapshot that has kraft.version and voter set control records
        short kraftVersion = 1;
        VoterSet snapshotVoterSet = VoterSetTest.voterSet(VoterSetTest.voterMap(Arrays.asList(4, 5, 6), true));

        OffsetAndEpoch snapshotId = new OffsetAndEpoch(10, epoch);
        RecordsSnapshotWriter.Builder builder = new RecordsSnapshotWriter.Builder()
            .setRawSnapshotWriter(log.createNewSnapshotUnchecked(snapshotId).get())
            .setKraftVersion(kraftVersion)
            .setVoterSet(Optional.of(snapshotVoterSet));
        try (RecordsSnapshotWriter<?> writer = builder.build(STRING_SERDE)) {
            writer.freeze();
        }
        log.truncateToLatestSnapshot();

        // Append the voter set control record
        VoterSet voterSet = snapshotVoterSet.addVoter(VoterSetTest.voterNode(7, true)).get();
        log.appendAsLeader(
            MemoryRecords.withVotersRecord(
                log.endOffset().offset,
                0,
                epoch,
                bufferSupplier.get(300),
                voterSet.toVotersRecord((short) 0)
            ),
            epoch
        );

        // Read the entire partition
        partitionListener.updateListener();

        assertEquals(voterSet, partitionListener.lastVoterSet());
        assertEquals(Optional.of(voterSet), partitionListener.voterSetAtOffset(log.endOffset().offset - 1));
        assertEquals(kraftVersion, partitionListener.kraftVersionAtOffset(log.endOffset().offset - 1));

        // Check the voter set at the snapshot
        assertEquals(Optional.of(snapshotVoterSet), partitionListener.voterSetAtOffset(snapshotId.offset() - 1));
    }

    @Test
    void testTruncateTo() {
        MockLog log = buildLog();
        VoterSet staticVoterSet = VoterSetTest.voterSet(VoterSetTest.voterMap(Arrays.asList(1, 2, 3), true));
        BufferSupplier bufferSupplier = BufferSupplier.NO_CACHING;
        int epoch = 1;

        PartitionListener partitionListener = buildPartitionListener(log, Optional.of(staticVoterSet));

        // Append the kraft.version control record
        short kraftVersion = 1;
        log.appendAsLeader(
            MemoryRecords.withKRaftVersionRecord(
                log.endOffset().offset,
                0,
                epoch,
                bufferSupplier.get(300),
                new KRaftVersionRecord().setKRaftVersion(kraftVersion)
            ),
            epoch
        );

        // Append the voter set control record
        long firstVoterSetOffset = log.endOffset().offset;
        VoterSet firstVoterSet = VoterSetTest.voterSet(VoterSetTest.voterMap(Arrays.asList(4, 5, 6), true));
        log.appendAsLeader(
            MemoryRecords.withVotersRecord(
                firstVoterSetOffset,
                0,
                epoch,
                bufferSupplier.get(300),
                firstVoterSet.toVotersRecord((short) 0)
            ),
            epoch
        );

        // Append another voter set control record
        long voterSetOffset = log.endOffset().offset;
        VoterSet voterSet = firstVoterSet.addVoter(VoterSetTest.voterNode(7, true)).get();
        log.appendAsLeader(
            MemoryRecords.withVotersRecord(
                voterSetOffset,
                0,
                epoch,
                bufferSupplier.get(300),
                voterSet.toVotersRecord((short) 0)
            ),
            epoch
        );

        // Read the entire partition
        partitionListener.updateListener();

        assertEquals(voterSet, partitionListener.lastVoterSet());

        // Truncate log and listener
        log.truncateTo(voterSetOffset);
        partitionListener.truncateTo(voterSetOffset);

        assertEquals(firstVoterSet, partitionListener.lastVoterSet());

        // Truncate the entire log
        log.truncateTo(0);
        partitionListener.truncateTo(0);

        assertEquals(staticVoterSet, partitionListener.lastVoterSet());
    }

    @Test
    void testTrimPrefixTo() {
        MockLog log = buildLog();
        VoterSet staticVoterSet = VoterSetTest.voterSet(VoterSetTest.voterMap(Arrays.asList(1, 2, 3), true));
        BufferSupplier bufferSupplier = BufferSupplier.NO_CACHING;
        int epoch = 1;

        PartitionListener partitionListener = buildPartitionListener(log, Optional.of(staticVoterSet));

        // Append the kraft.version control record
        long kraftVersionOffset = log.endOffset().offset;
        short kraftVersion = 1;
        log.appendAsLeader(
            MemoryRecords.withKRaftVersionRecord(
                kraftVersionOffset,
                0,
                epoch,
                bufferSupplier.get(300),
                new KRaftVersionRecord().setKRaftVersion(kraftVersion)
            ),
            epoch
        );

        // Append the voter set control record
        long firstVoterSetOffset = log.endOffset().offset;
        VoterSet firstVoterSet = VoterSetTest.voterSet(VoterSetTest.voterMap(Arrays.asList(4, 5, 6), true));
        log.appendAsLeader(
            MemoryRecords.withVotersRecord(
                firstVoterSetOffset,
                0,
                epoch,
                bufferSupplier.get(300),
                firstVoterSet.toVotersRecord((short) 0)
            ),
            epoch
        );

        // Append another voter set control record
        long voterSetOffset = log.endOffset().offset;
        VoterSet voterSet = firstVoterSet.addVoter(VoterSetTest.voterNode(7, true)).get();
        log.appendAsLeader(
            MemoryRecords.withVotersRecord(
                voterSetOffset,
                0,
                epoch,
                bufferSupplier.get(300),
                voterSet.toVotersRecord((short) 0)
            ),
            epoch
        );

        // Read the entire partition
        partitionListener.updateListener();

        assertEquals(voterSet, partitionListener.lastVoterSet());
        assertEquals(kraftVersion, partitionListener.kraftVersionAtOffset(kraftVersionOffset));

        // Trim the prefix for the partition listener up to the kraft.version
        partitionListener.trimPrefixTo(kraftVersionOffset);
        assertEquals(kraftVersion, partitionListener.kraftVersionAtOffset(kraftVersionOffset));

        // Trim the prefix for the partition listener up to the first voter set
        partitionListener.trimPrefixTo(firstVoterSetOffset);
        assertEquals(kraftVersion, partitionListener.kraftVersionAtOffset(kraftVersionOffset));
        assertEquals(Optional.of(firstVoterSet), partitionListener.voterSetAtOffset(firstVoterSetOffset));

        // Trim the prefix for the partition listener up to the second voter set
        partitionListener.trimPrefixTo(voterSetOffset);
        assertEquals(kraftVersion, partitionListener.kraftVersionAtOffset(kraftVersionOffset));
        assertEquals(Optional.empty(), partitionListener.voterSetAtOffset(firstVoterSetOffset));
        assertEquals(Optional.of(voterSet), partitionListener.voterSetAtOffset(voterSetOffset));
    }
}
