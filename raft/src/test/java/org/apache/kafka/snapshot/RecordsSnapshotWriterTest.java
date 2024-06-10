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

package org.apache.kafka.snapshot;


import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.common.message.KRaftVersionRecord;
import org.apache.kafka.common.message.SnapshotFooterRecord;
import org.apache.kafka.common.message.SnapshotHeaderRecord;
import org.apache.kafka.common.record.ControlRecordType;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.raft.Batch;
import org.apache.kafka.raft.OffsetAndEpoch;
import org.apache.kafka.raft.internals.StringSerde;
import org.apache.kafka.raft.internals.VoterSet;
import org.apache.kafka.raft.internals.VoterSetTest;
import org.apache.kafka.server.common.serialization.RecordSerde;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class RecordsSnapshotWriterTest {
    private static final RecordSerde<String> STRING_SERDE = new StringSerde();

    @Test
    void testBuilderKRaftVersion0() {
        OffsetAndEpoch snapshotId = new OffsetAndEpoch(100, 10);
        int maxBatchSize = 1024;
        AtomicReference<ByteBuffer> buffer = new AtomicReference<>(null);
        RecordsSnapshotWriter.Builder builder = new RecordsSnapshotWriter.Builder()
            .setKraftVersion((short) 0)
            .setVoterSet(Optional.empty())
            .setTime(new MockTime())
            .setMaxBatchSize(maxBatchSize)
            .setRawSnapshotWriter(
                new MockRawSnapshotWriter(snapshotId, snapshotBuf -> buffer.set(snapshotBuf))
            );
        try (RecordsSnapshotWriter<String> snapshot = builder.build(STRING_SERDE)) {
            snapshot.freeze();
        }

        try (RecordsSnapshotReader<String> reader = RecordsSnapshotReader.of(
                new MockRawSnapshotReader(snapshotId, buffer.get()),
                STRING_SERDE,
                BufferSupplier.NO_CACHING,
                maxBatchSize,
                true
            )
        ) {
            // Consume the control record batch
            Batch<String> batch = reader.next();
            assertEquals(1, batch.controlRecords().size());

            // Check snapshot header control record
            assertEquals(ControlRecordType.SNAPSHOT_HEADER, batch.controlRecords().get(0).type());
            assertEquals(new SnapshotHeaderRecord(), batch.controlRecords().get(0).message());

            // Consume the reader until we find a control record
            do {
                batch = reader.next();
            }
            while (batch.controlRecords().isEmpty());

            // Check snapshot footer control record
            assertEquals(1, batch.controlRecords().size());
            assertEquals(ControlRecordType.SNAPSHOT_FOOTER, batch.controlRecords().get(0).type());
            assertEquals(new SnapshotFooterRecord(), batch.controlRecords().get(0).message());

            // Snapshot footer must be last record
            assertFalse(reader.hasNext());
        }
    }

    @Test
    void testBuilderKRaftVersion0WithVoterSet() {
        OffsetAndEpoch snapshotId = new OffsetAndEpoch(100, 10);
        int maxBatchSize = 1024;
        VoterSet voterSet = VoterSetTest.voterSet(
            new HashMap<>(VoterSetTest.voterMap(Arrays.asList(1, 2, 3), true))
        );
        AtomicReference<ByteBuffer> buffer = new AtomicReference<>(null);
        RecordsSnapshotWriter.Builder builder = new RecordsSnapshotWriter.Builder()
            .setKraftVersion((short) 0)
            .setVoterSet(Optional.of(voterSet))
            .setTime(new MockTime())
            .setMaxBatchSize(maxBatchSize)
            .setRawSnapshotWriter(
                new MockRawSnapshotWriter(snapshotId, snapshotBuf -> buffer.set(snapshotBuf))
            );

        assertThrows(IllegalStateException.class, () -> builder.build(STRING_SERDE));
    }

    @Test
    void testKBuilderRaftVersion1WithVoterSet() {
        OffsetAndEpoch snapshotId = new OffsetAndEpoch(100, 10);
        int maxBatchSize = 1024;
        VoterSet voterSet = VoterSetTest.voterSet(
            new HashMap<>(VoterSetTest.voterMap(Arrays.asList(1, 2, 3), true))
        );
        AtomicReference<ByteBuffer> buffer = new AtomicReference<>(null);
        RecordsSnapshotWriter.Builder builder = new RecordsSnapshotWriter.Builder()
            .setKraftVersion((short) 1)
            .setVoterSet(Optional.of(voterSet))
            .setTime(new MockTime())
            .setMaxBatchSize(maxBatchSize)
            .setRawSnapshotWriter(
                new MockRawSnapshotWriter(snapshotId, snapshotBuf -> buffer.set(snapshotBuf))
            );
        try (RecordsSnapshotWriter<String> snapshot = builder.build(STRING_SERDE)) {
            snapshot.freeze();
        }

        try (RecordsSnapshotReader<String> reader = RecordsSnapshotReader.of(
                new MockRawSnapshotReader(snapshotId, buffer.get()),
                STRING_SERDE,
                BufferSupplier.NO_CACHING,
                maxBatchSize,
                true
            )
        ) {
            // Consume the control record batch
            Batch<String> batch = reader.next();
            assertEquals(3, batch.controlRecords().size());

            // Check snapshot header control record
            assertEquals(ControlRecordType.SNAPSHOT_HEADER, batch.controlRecords().get(0).type());
            assertEquals(new SnapshotHeaderRecord(), batch.controlRecords().get(0).message());

            // Check kraft version control record
            assertEquals(ControlRecordType.KRAFT_VERSION, batch.controlRecords().get(1).type());
            assertEquals(new KRaftVersionRecord().setKRaftVersion((short) 1), batch.controlRecords().get(1).message());

            // Check the voters control record
            assertEquals(ControlRecordType.KRAFT_VOTERS, batch.controlRecords().get(2).type());
            assertEquals(voterSet.toVotersRecord((short) 0), batch.controlRecords().get(2).message());

            // Consume the reader until we find a control record
            do {
                batch = reader.next();
            }
            while (batch.controlRecords().isEmpty());

            // Check snapshot footer control record
            assertEquals(1, batch.controlRecords().size());
            assertEquals(ControlRecordType.SNAPSHOT_FOOTER, batch.controlRecords().get(0).type());
            assertEquals(new SnapshotFooterRecord(), batch.controlRecords().get(0).message());

            // Snapshot footer must be last record
            assertFalse(reader.hasNext());
        }
    }

    @Test
    void testBuilderKRaftVersion1WithoutVoterSet() {
        OffsetAndEpoch snapshotId = new OffsetAndEpoch(100, 10);
        int maxBatchSize = 1024;
        AtomicReference<ByteBuffer> buffer = new AtomicReference<>(null);
        RecordsSnapshotWriter.Builder builder = new RecordsSnapshotWriter.Builder()
            .setKraftVersion((short) 1)
            .setVoterSet(Optional.empty())
            .setTime(new MockTime())
            .setMaxBatchSize(maxBatchSize)
            .setRawSnapshotWriter(
                new MockRawSnapshotWriter(snapshotId, snapshotBuf -> buffer.set(snapshotBuf))
            );
        try (RecordsSnapshotWriter<String> snapshot = builder.build(STRING_SERDE)) {
            snapshot.freeze();
        }

        try (RecordsSnapshotReader<String> reader = RecordsSnapshotReader.of(
                new MockRawSnapshotReader(snapshotId, buffer.get()),
                STRING_SERDE,
                BufferSupplier.NO_CACHING,
                maxBatchSize,
                true
            )
        ) {
            // Consume the control record batch
            Batch<String> batch = reader.next();
            assertEquals(2, batch.controlRecords().size());

            // Check snapshot header control record
            assertEquals(ControlRecordType.SNAPSHOT_HEADER, batch.controlRecords().get(0).type());
            assertEquals(new SnapshotHeaderRecord(), batch.controlRecords().get(0).message());

            // Check kraft version control record
            assertEquals(ControlRecordType.KRAFT_VERSION, batch.controlRecords().get(1).type());
            assertEquals(new KRaftVersionRecord().setKRaftVersion((short) 1), batch.controlRecords().get(1).message());

            // Consume the reader until we find a control record
            do {
                batch = reader.next();
            }
            while (batch.controlRecords().isEmpty());

            // Check snapshot footer control record
            assertEquals(1, batch.controlRecords().size());
            assertEquals(ControlRecordType.SNAPSHOT_FOOTER, batch.controlRecords().get(0).type());
            assertEquals(new SnapshotFooterRecord(), batch.controlRecords().get(0).message());

            // Snapshot footer must be last record
            assertFalse(reader.hasNext());
        }
    }
}
