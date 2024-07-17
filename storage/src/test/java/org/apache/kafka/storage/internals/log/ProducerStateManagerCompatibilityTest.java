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
package org.apache.kafka.storage.internals.log;

import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.SchemaException;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.protocol.types.Type;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.utils.ByteUtils;
import org.apache.kafka.common.utils.Crc32C;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;

// TODO: this is a temporary test class for verifying KAFKA-17056, DO NOT MERGE THIS FILE !
public class ProducerStateManagerCompatibilityTest {
    private static final short PRODUCER_SNAPSHOT_VERSION = 1;
    private static final String VERSION_FIELD = "version";
    private static final String CRC_FIELD = "crc";
    private static final String PRODUCER_ID_FIELD = "producer_id";
    private static final String LAST_SEQUENCE_FIELD = "last_sequence";
    private static final String PRODUCER_EPOCH_FIELD = "epoch";
    private static final String LAST_OFFSET_FIELD = "last_offset";
    private static final String OFFSET_DELTA_FIELD = "offset_delta";
    private static final String TIMESTAMP_FIELD = "timestamp";
    private static final String PRODUCER_ENTRIES_FIELD = "producer_entries";
    private static final String COORDINATOR_EPOCH_FIELD = "coordinator_epoch";
    private static final String CURRENT_TXN_FIRST_OFFSET_FIELD = "current_txn_first_offset";

    private static final int VERSION_OFFSET = 0;
    private static final int CRC_OFFSET = VERSION_OFFSET + 2;
    private static final int PRODUCER_ENTRIES_OFFSET = CRC_OFFSET + 4;

    private static final Schema PRODUCER_SNAPSHOT_ENTRY_SCHEMA =
            new Schema(new Field(PRODUCER_ID_FIELD, Type.INT64, "The producer ID"),
                    new Field(PRODUCER_EPOCH_FIELD, Type.INT16, "Current epoch of the producer"),
                    new Field(LAST_SEQUENCE_FIELD, Type.INT32, "Last written sequence of the producer"),
                    new Field(LAST_OFFSET_FIELD, Type.INT64, "Last written offset of the producer"),
                    new Field(OFFSET_DELTA_FIELD, Type.INT32, "The difference of the last sequence and first sequence in the last written batch"),
                    new Field(TIMESTAMP_FIELD, Type.INT64, "Max timestamp from the last written entry"),
                    new Field(COORDINATOR_EPOCH_FIELD, Type.INT32, "The epoch of the last transaction coordinator to send an end transaction marker"),
                    new Field(CURRENT_TXN_FIRST_OFFSET_FIELD, Type.INT64, "The first offset of the on-going transaction (-1 if there is none)"));
    private static final Schema PID_SNAPSHOT_MAP_SCHEMA =
            new Schema(new Field(VERSION_FIELD, Type.INT16, "Version of the snapshot file"),
                    new Field(CRC_FIELD, Type.UNSIGNED_INT32, "CRC of the snapshot data"),
                    new Field(PRODUCER_ENTRIES_FIELD, new ArrayOf(PRODUCER_SNAPSHOT_ENTRY_SCHEMA), "The entries in the producer table"));

    private static List<ProducerStateEntry> readSnapshot(File file) throws IOException {
        try {
            byte[] buffer = Files.readAllBytes(file.toPath());
            Struct struct = PID_SNAPSHOT_MAP_SCHEMA.read(ByteBuffer.wrap(buffer));

            Short version = struct.getShort(VERSION_FIELD);
            if (version != PRODUCER_SNAPSHOT_VERSION)
                throw new CorruptSnapshotException("Snapshot contained an unknown file version " + version);

            long crc = struct.getUnsignedInt(CRC_FIELD);
            long computedCrc = Crc32C.compute(buffer, PRODUCER_ENTRIES_OFFSET, buffer.length - PRODUCER_ENTRIES_OFFSET);
            if (crc != computedCrc)
                throw new CorruptSnapshotException("Snapshot is corrupt (CRC is no longer valid). Stored crc: " + crc
                        + ". Computed crc: " + computedCrc);

            Object[] producerEntryFields = struct.getArray(PRODUCER_ENTRIES_FIELD);
            List<ProducerStateEntry> entries = new ArrayList<>(producerEntryFields.length);
            for (Object producerEntryObj : producerEntryFields) {
                Struct producerEntryStruct = (Struct) producerEntryObj;
                long producerId = producerEntryStruct.getLong(PRODUCER_ID_FIELD);
                short producerEpoch = producerEntryStruct.getShort(PRODUCER_EPOCH_FIELD);
                int seq = producerEntryStruct.getInt(LAST_SEQUENCE_FIELD);
                long offset = producerEntryStruct.getLong(LAST_OFFSET_FIELD);
                long timestamp = producerEntryStruct.getLong(TIMESTAMP_FIELD);
                int offsetDelta = producerEntryStruct.getInt(OFFSET_DELTA_FIELD);
                int coordinatorEpoch = producerEntryStruct.getInt(COORDINATOR_EPOCH_FIELD);
                long currentTxnFirstOffset = producerEntryStruct.getLong(CURRENT_TXN_FIRST_OFFSET_FIELD);

                OptionalLong currentTxnFirstOffsetVal = currentTxnFirstOffset >= 0 ? OptionalLong.of(currentTxnFirstOffset) : OptionalLong.empty();
                Optional<BatchMetadata> batchMetadata =
                        (offset >= 0) ? Optional.of(new BatchMetadata(seq, offset, offsetDelta, timestamp)) : Optional.empty();
                entries.add(new ProducerStateEntry(producerId, producerEpoch, coordinatorEpoch, timestamp, currentTxnFirstOffsetVal, batchMetadata));
            }

            return entries;
        } catch (SchemaException e) {
            throw new CorruptSnapshotException("Snapshot failed schema validation: " + e.getMessage());
        }
    }

    private static void writeSnapshot(File file, Map<Long, ProducerStateEntry> entries, boolean sync) throws IOException {
        Struct struct = new Struct(PID_SNAPSHOT_MAP_SCHEMA);
        struct.set(VERSION_FIELD, PRODUCER_SNAPSHOT_VERSION);
        struct.set(CRC_FIELD, 0L); // we'll fill this after writing the entries
        Struct[] structEntries = new Struct[entries.size()];
        int i = 0;
        for (Map.Entry<Long, ProducerStateEntry> producerIdEntry : entries.entrySet()) {
            Long producerId = producerIdEntry.getKey();
            ProducerStateEntry entry = producerIdEntry.getValue();
            Struct producerEntryStruct = struct.instance(PRODUCER_ENTRIES_FIELD);
            producerEntryStruct.set(PRODUCER_ID_FIELD, producerId)
                    .set(PRODUCER_EPOCH_FIELD, entry.producerEpoch())
                    .set(LAST_SEQUENCE_FIELD, entry.lastSeq())
                    .set(LAST_OFFSET_FIELD, entry.lastDataOffset())
                    .set(OFFSET_DELTA_FIELD, entry.lastOffsetDelta())
                    .set(TIMESTAMP_FIELD, entry.lastTimestamp())
                    .set(COORDINATOR_EPOCH_FIELD, entry.coordinatorEpoch())
                    .set(CURRENT_TXN_FIRST_OFFSET_FIELD, entry.currentTxnFirstOffset().orElse(-1L));
            structEntries[i++] = producerEntryStruct;
        }
        struct.set(PRODUCER_ENTRIES_FIELD, structEntries);

        ByteBuffer buffer = ByteBuffer.allocate(struct.sizeOf());
        struct.writeTo(buffer);
        buffer.flip();

        // now fill in the CRC
        long crc = Crc32C.compute(buffer, PRODUCER_ENTRIES_OFFSET, buffer.limit() - PRODUCER_ENTRIES_OFFSET);
        ByteUtils.writeUnsignedInt(buffer, CRC_OFFSET, crc);

        try (FileChannel fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            fileChannel.write(buffer);
            if (sync) {
                fileChannel.force(true);
            }
        }
    }

    @Test
    public void testProducerSnapshotCompatibility() throws IOException {
        Map<Long, ProducerStateEntry> expectedEntryMap = new HashMap<>();
        expectedEntryMap.put(1L, new ProducerStateEntry(1L, (short) 2, 3, RecordBatch.NO_TIMESTAMP,
                OptionalLong.of(100L), Optional.of(new BatchMetadata(1, 2L, 3, RecordBatch.NO_TIMESTAMP))));
        expectedEntryMap.put(11L, new ProducerStateEntry(11L, (short) 12, 13, 123456L,
                OptionalLong.empty(), Optional.empty()));

        BiConsumer<ProducerStateEntry, ProducerStateEntry> assertProducerStateEntry = (expected, actual) -> {
            assertEquals(expected.producerId(), actual.producerId());
            assertEquals(expected.producerEpoch(), actual.producerEpoch());
            assertEquals(expected.coordinatorEpoch(), actual.coordinatorEpoch());
            assertEquals(expected.lastTimestamp(), actual.lastTimestamp());
            assertEquals(expected.currentTxnFirstOffset(), actual.currentTxnFirstOffset());
            assertIterableEquals(expected.batchMetadata(), actual.batchMetadata());
        };

        Consumer<List<ProducerStateEntry>> assertEntries = actual -> {
            Map<Long, ProducerStateEntry> actualEntryMap =
                    actual.stream().collect(Collectors.toMap(ProducerStateEntry::producerId, Function.identity()));
            expectedEntryMap.forEach((key, value) -> assertProducerStateEntry.accept(value, actualEntryMap.get(key)));
        };

        File tmpDir = TestUtils.tempDirectory();
        try {
            File oldImpl = new File(tmpDir, "old_impl");
            writeSnapshot(oldImpl, expectedEntryMap, true);
            File newImpl = new File(tmpDir, "new_impl");
            ProducerStateManager.writeSnapshot(newImpl, expectedEntryMap, true);
            // check bytes in files in old and new implementation are the same
            assertArrayEquals(Files.readAllBytes(oldImpl.toPath()), Files.readAllBytes(newImpl.toPath()));

            // check the output ProducerStateEntry from readSnapshot in old and new implementation are the same
            assertEntries.accept(readSnapshot(oldImpl));
            assertEntries.accept(readSnapshot(newImpl));
            assertEntries.accept(ProducerStateManager.readSnapshot(oldImpl));
            assertEntries.accept(ProducerStateManager.readSnapshot(newImpl));
        } finally {
            Utils.delete(tmpDir);
        }
    }
}
