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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.KRaftVersionRecord;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.raft.MockExternalKRaftMetrics;
import org.apache.kafka.raft.MockLog;
import org.apache.kafka.raft.OffsetAndEpoch;
import org.apache.kafka.raft.VoterSet;
import org.apache.kafka.raft.VoterSetTest;
import org.apache.kafka.server.common.KRaftVersion;
import org.apache.kafka.server.common.serialization.RecordSerde;
import org.apache.kafka.snapshot.RecordsSnapshotWriter;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;

import java.util.Optional;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class KRaftControlRecordStateMachineTest {
    private static Metrics metrics = new Metrics();
    private static KafkaRaftMetrics kafkaRaftMetrics = new KafkaRaftMetrics(metrics, "raft");
    private static MockExternalKRaftMetrics externalMetrics = new MockExternalKRaftMetrics();
    private static final RecordSerde<String> STRING_SERDE = new StringSerde();

    private static MockLog buildLog() {
        return new MockLog(new TopicPartition("partition", 0), Uuid.randomUuid(), new LogContext());
    }

    private static KafkaMetric getNumberOfVoters(final Metrics metrics) {
        return metrics.metrics().get(metrics.metricName("number-of-voters", "raft-metrics"));
    }

    private static KRaftControlRecordStateMachine buildPartitionListener(MockLog log, VoterSet staticVoterSet) {
        return new KRaftControlRecordStateMachine(
            staticVoterSet,
            log,
            STRING_SERDE,
            BufferSupplier.NO_CACHING,
            1024,
            new LogContext(),
            kafkaRaftMetrics,
            externalMetrics
        );
    }

    @BeforeEach
    public void resetMetrics() {
        metrics = new Metrics();
        kafkaRaftMetrics = new KafkaRaftMetrics(metrics, "raft");
        externalMetrics = new MockExternalKRaftMetrics();
    }

    @Test
    void testEmptyPartition() {
        MockLog log = buildLog();
        VoterSet voterSet = VoterSetTest.voterSet(VoterSetTest.voterMap(IntStream.of(1, 2, 3), true));

        KRaftControlRecordStateMachine partitionState = buildPartitionListener(log, voterSet);

        // This should be a no-op operation
        partitionState.updateState();

        assertEquals(voterSet, partitionState.lastVoterSet());
        assertFalse(externalMetrics.getIgnoredStaticVoters());
        assertEquals(3, getNumberOfVoters(metrics).metricValue());
    }

    @Test
    void testEmptyPartitionWithNoStaticVoters() {
        MockLog log = buildLog();

        KRaftControlRecordStateMachine partitionState = buildPartitionListener(log, VoterSet.empty());

        // This should be a no-op operation
        partitionState.updateState();

        assertEquals(VoterSet.empty(), partitionState.lastVoterSet());
        assertFalse(externalMetrics.getIgnoredStaticVoters());
        assertEquals(0, getNumberOfVoters(metrics).metricValue());
    }

    @Test
    void testUpdateWithoutSnapshot() {
        MockLog log = buildLog();
        VoterSet staticVoterSet = VoterSetTest.voterSet(VoterSetTest.voterMap(IntStream.of(1, 2, 3), true));
        BufferSupplier bufferSupplier = BufferSupplier.NO_CACHING;
        int epoch = 1;

        KRaftControlRecordStateMachine partitionState = buildPartitionListener(log, staticVoterSet);

        assertFalse(externalMetrics.getIgnoredStaticVoters());
        assertEquals(3, getNumberOfVoters(metrics).metricValue());

        // Append the kraft.version control record
        KRaftVersion kraftVersion = KRaftVersion.KRAFT_VERSION_1;
        log.appendAsLeader(
            MemoryRecords.withKRaftVersionRecord(
                log.endOffset().offset(),
                0,
                epoch,
                bufferSupplier.get(300),
                new KRaftVersionRecord().setKRaftVersion(kraftVersion.featureLevel())
            ),
            epoch
        );

        // Append the voter set control record
        VoterSet voterSet = VoterSetTest.voterSet(VoterSetTest.voterMap(IntStream.of(4, 5, 6), true));
        log.appendAsLeader(
            MemoryRecords.withVotersRecord(
                log.endOffset().offset(),
                0,
                epoch,
                bufferSupplier.get(300),
                voterSet.toVotersRecord((short) 0)
            ),
            epoch
        );

        // Read the entire partition
        partitionState.updateState();

        assertEquals(voterSet, partitionState.lastVoterSet());
        assertEquals(Optional.of(voterSet), partitionState.voterSetAtOffset(log.endOffset().offset() - 1));
        assertEquals(kraftVersion, partitionState.kraftVersionAtOffset(log.endOffset().offset() - 1));
        assertTrue(externalMetrics.getIgnoredStaticVoters());
        assertEquals(3, getNumberOfVoters(metrics).metricValue());
    }

    @Test
    void testUpdateWithEmptySnapshot() {
        MockLog log = buildLog();
        VoterSet staticVoterSet = VoterSetTest.voterSet(VoterSetTest.voterMap(IntStream.of(1, 2, 3), true));
        BufferSupplier bufferSupplier = BufferSupplier.NO_CACHING;
        int epoch = 1;

        KRaftControlRecordStateMachine partitionState = buildPartitionListener(log, staticVoterSet);

        assertFalse(externalMetrics.getIgnoredStaticVoters());
        assertEquals(3, getNumberOfVoters(metrics).metricValue());

        // Create a snapshot that doesn't have any kraft.version or voter set control records
        RecordsSnapshotWriter.Builder builder = new RecordsSnapshotWriter.Builder()
            .setRawSnapshotWriter(log.createNewSnapshotUnchecked(new OffsetAndEpoch(10, epoch)).get());
        try (RecordsSnapshotWriter<?> writer = builder.build(STRING_SERDE)) {
            writer.freeze();
        }
        log.truncateToLatestSnapshot();

        // Append the kraft.version control record
        KRaftVersion kraftVersion = KRaftVersion.KRAFT_VERSION_1;
        log.appendAsLeader(
            MemoryRecords.withKRaftVersionRecord(
                log.endOffset().offset(),
                0,
                epoch,
                bufferSupplier.get(300),
                new KRaftVersionRecord().setKRaftVersion(kraftVersion.featureLevel())
            ),
            epoch
        );

        // Append the voter set control record
        VoterSet voterSet = VoterSetTest.voterSet(VoterSetTest.voterMap(IntStream.of(4, 5, 6), true));
        log.appendAsLeader(
            MemoryRecords.withVotersRecord(
                log.endOffset().offset(),
                0,
                epoch,
                bufferSupplier.get(300),
                voterSet.toVotersRecord((short) 0)
            ),
            epoch
        );

        // Read the entire partition
        partitionState.updateState();

        assertEquals(voterSet, partitionState.lastVoterSet());
        assertEquals(Optional.of(voterSet), partitionState.voterSetAtOffset(log.endOffset().offset() - 1));
        assertEquals(kraftVersion, partitionState.kraftVersionAtOffset(log.endOffset().offset() - 1));
        assertTrue(externalMetrics.getIgnoredStaticVoters());
        assertEquals(3, getNumberOfVoters(metrics).metricValue());
    }

    @Test
    void testUpdateWithSnapshot() {
        MockLog log = buildLog();
        VoterSet staticVoterSet = VoterSetTest.voterSet(VoterSetTest.voterMap(IntStream.of(1, 2, 3), true));
        int epoch = 1;

        KRaftControlRecordStateMachine partitionState = buildPartitionListener(log, staticVoterSet);

        assertFalse(externalMetrics.getIgnoredStaticVoters());
        assertEquals(3, getNumberOfVoters(metrics).metricValue());

        // Create a snapshot that has kraft.version and voter set control records
        KRaftVersion kraftVersion = KRaftVersion.KRAFT_VERSION_1;
        VoterSet voterSet = VoterSetTest.voterSet(VoterSetTest.voterMap(IntStream.of(4, 5, 6), true));

        RecordsSnapshotWriter.Builder builder = new RecordsSnapshotWriter.Builder()
            .setRawSnapshotWriter(log.createNewSnapshotUnchecked(new OffsetAndEpoch(10, epoch)).get())
            .setKraftVersion(kraftVersion)
            .setVoterSet(Optional.of(voterSet));
        try (RecordsSnapshotWriter<?> writer = builder.build(STRING_SERDE)) {
            writer.freeze();
        }
        log.truncateToLatestSnapshot();

        // Read the entire partition
        partitionState.updateState();

        assertEquals(voterSet, partitionState.lastVoterSet());
        assertEquals(Optional.of(voterSet), partitionState.voterSetAtOffset(log.endOffset().offset() - 1));
        assertEquals(kraftVersion, partitionState.kraftVersionAtOffset(log.endOffset().offset() - 1));
        assertTrue(externalMetrics.getIgnoredStaticVoters());
        assertEquals(3, getNumberOfVoters(metrics).metricValue());
    }

    @Test
    void testUpdateWithSnapshotAndLogOverride() {
        MockLog log = buildLog();
        VoterSet staticVoterSet = VoterSetTest.voterSet(VoterSetTest.voterMap(IntStream.of(1, 2, 3), true));
        BufferSupplier bufferSupplier = BufferSupplier.NO_CACHING;
        int epoch = 1;

        KRaftControlRecordStateMachine partitionState = buildPartitionListener(log, staticVoterSet);

        assertFalse(externalMetrics.getIgnoredStaticVoters());
        assertEquals(3, getNumberOfVoters(metrics).metricValue());

        // Create a snapshot that has kraft.version and voter set control records
        KRaftVersion kraftVersion = KRaftVersion.KRAFT_VERSION_1;
        VoterSet snapshotVoterSet = VoterSetTest.voterSet(VoterSetTest.voterMap(IntStream.of(4, 5, 6), true));

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
                log.endOffset().offset(),
                0,
                epoch,
                bufferSupplier.get(300),
                voterSet.toVotersRecord((short) 0)
            ),
            epoch
        );

        // Read the entire partition
        partitionState.updateState();

        assertEquals(voterSet, partitionState.lastVoterSet());
        assertEquals(Optional.of(voterSet), partitionState.voterSetAtOffset(log.endOffset().offset() - 1));
        assertEquals(kraftVersion, partitionState.kraftVersionAtOffset(log.endOffset().offset() - 1));
        assertTrue(externalMetrics.getIgnoredStaticVoters());
        assertEquals(4, getNumberOfVoters(metrics).metricValue());

        // Check the voter set at the snapshot
        assertEquals(Optional.of(snapshotVoterSet), partitionState.voterSetAtOffset(snapshotId.offset() - 1));
    }

    @Test
    void testTruncateTo() {
        MockLog log = buildLog();
        VoterSet staticVoterSet = VoterSetTest.voterSet(VoterSetTest.voterMap(IntStream.of(1, 2, 3), true));
        BufferSupplier bufferSupplier = BufferSupplier.NO_CACHING;
        int epoch = 1;

        KRaftControlRecordStateMachine partitionState = buildPartitionListener(log, staticVoterSet);

        assertFalse(externalMetrics.getIgnoredStaticVoters());
        assertEquals(3, getNumberOfVoters(metrics).metricValue());

        // Append the kraft.version control record
        KRaftVersion kraftVersion = KRaftVersion.KRAFT_VERSION_1;
        log.appendAsLeader(
            MemoryRecords.withKRaftVersionRecord(
                log.endOffset().offset(),
                0,
                epoch,
                bufferSupplier.get(300),
                new KRaftVersionRecord().setKRaftVersion(kraftVersion.featureLevel())
            ),
            epoch
        );

        // Append the voter set control record
        long firstVoterSetOffset = log.endOffset().offset();
        VoterSet firstVoterSet = VoterSetTest.voterSet(VoterSetTest.voterMap(IntStream.of(4, 5, 6), true));
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
        long voterSetOffset = log.endOffset().offset();
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
        partitionState.updateState();

        assertEquals(voterSet, partitionState.lastVoterSet());
        assertTrue(externalMetrics.getIgnoredStaticVoters());
        assertEquals(4, getNumberOfVoters(metrics).metricValue());

        // Truncate log and listener
        log.truncateTo(voterSetOffset);
        partitionState.truncateNewEntries(voterSetOffset);

        assertEquals(firstVoterSet, partitionState.lastVoterSet());
        assertTrue(externalMetrics.getIgnoredStaticVoters());
        assertEquals(3, getNumberOfVoters(metrics).metricValue());

        // Truncate the entire log
        log.truncateTo(0);
        partitionState.truncateNewEntries(0);

        assertEquals(staticVoterSet, partitionState.lastVoterSet());
        assertFalse(externalMetrics.getIgnoredStaticVoters());
        assertEquals(3, getNumberOfVoters(metrics).metricValue());
    }

    @Test
    void testTrimPrefixTo() {
        MockLog log = buildLog();
        VoterSet staticVoterSet = VoterSetTest.voterSet(VoterSetTest.voterMap(IntStream.of(1, 2, 3), true));
        BufferSupplier bufferSupplier = BufferSupplier.NO_CACHING;
        int epoch = 1;

        KRaftControlRecordStateMachine partitionState = buildPartitionListener(log, staticVoterSet);

        assertFalse(externalMetrics.getIgnoredStaticVoters());
        assertEquals(3, getNumberOfVoters(metrics).metricValue());

        // Append the kraft.version control record
        long kraftVersionOffset = log.endOffset().offset();
        KRaftVersion kraftVersion = KRaftVersion.KRAFT_VERSION_1;
        log.appendAsLeader(
            MemoryRecords.withKRaftVersionRecord(
                kraftVersionOffset,
                0,
                epoch,
                bufferSupplier.get(300),
                new KRaftVersionRecord().setKRaftVersion(kraftVersion.featureLevel())
            ),
            epoch
        );

        // Append the voter set control record
        long firstVoterSetOffset = log.endOffset().offset();
        VoterSet firstVoterSet = VoterSetTest.voterSet(VoterSetTest.voterMap(IntStream.of(4, 5, 6), true));
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
        long voterSetOffset = log.endOffset().offset();
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
        partitionState.updateState();

        assertEquals(voterSet, partitionState.lastVoterSet());
        assertEquals(kraftVersion, partitionState.kraftVersionAtOffset(kraftVersionOffset));
        assertTrue(externalMetrics.getIgnoredStaticVoters());
        assertEquals(4, getNumberOfVoters(metrics).metricValue());

        // Trim the prefix for the partition listener up to the kraft.version
        partitionState.truncateOldEntries(kraftVersionOffset);
        assertEquals(kraftVersion, partitionState.kraftVersionAtOffset(kraftVersionOffset));

        // Trim the prefix for the partition listener up to the first voter set
        partitionState.truncateOldEntries(firstVoterSetOffset);
        assertEquals(kraftVersion, partitionState.kraftVersionAtOffset(kraftVersionOffset));
        assertEquals(Optional.of(firstVoterSet), partitionState.voterSetAtOffset(firstVoterSetOffset));

        // Trim the prefix for the partition listener up to the second voter set
        partitionState.truncateOldEntries(voterSetOffset);
        assertEquals(kraftVersion, partitionState.kraftVersionAtOffset(kraftVersionOffset));
        assertEquals(Optional.empty(), partitionState.voterSetAtOffset(firstVoterSetOffset));
        assertEquals(Optional.of(voterSet), partitionState.voterSetAtOffset(voterSetOffset));
        assertTrue(externalMetrics.getIgnoredStaticVoters());
        assertEquals(4, getNumberOfVoters(metrics).metricValue());
    }
}
