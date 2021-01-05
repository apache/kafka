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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Iterator;
import java.util.stream.IntStream;
import org.apache.kafka.common.record.BufferSupplier.GrowableBufferSupplier;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.raft.OffsetAndEpoch;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public final class FileRawSnapshotTest {
    @Test
    public void testWritingSnapshot() throws IOException {
        Path tempDir = TestUtils.tempDirectory().toPath();
        OffsetAndEpoch offsetAndEpoch = new OffsetAndEpoch(10L, 3);
        int bufferSize = 256;
        int batches = 10;
        int expectedSize = 0;

        try (FileRawSnapshotWriter snapshot = FileRawSnapshotWriter.create(tempDir, offsetAndEpoch)) {
            assertEquals(0, snapshot.sizeInBytes());

            MemoryRecords records = buildRecords(ByteBuffer.wrap(randomBytes(bufferSize)));
            for (int i = 0; i < batches; i++) {
                snapshot.append(records.buffer());
                expectedSize += records.sizeInBytes();
            }

            assertEquals(expectedSize, snapshot.sizeInBytes());

            snapshot.freeze();
        }

        // File should exist and the size should be the sum of all the buffers
        assertTrue(Files.exists(Snapshots.snapshotPath(tempDir, offsetAndEpoch)));
        assertEquals(expectedSize, Files.size(Snapshots.snapshotPath(tempDir, offsetAndEpoch)));
    }

    @Test
    public void testWriteReadSnapshot() throws IOException {
        Path tempDir = TestUtils.tempDirectory().toPath();
        OffsetAndEpoch offsetAndEpoch = new OffsetAndEpoch(10L, 3);
        int bufferSize = 256;
        int batches = 10;

        ByteBuffer expectedBuffer = ByteBuffer.wrap(randomBytes(bufferSize));

        try (FileRawSnapshotWriter snapshot = FileRawSnapshotWriter.create(tempDir, offsetAndEpoch)) {
            MemoryRecords records = buildRecords(expectedBuffer);
            for (int i = 0; i < batches; i++) {
                snapshot.append(records.buffer());
            }

            snapshot.freeze();
        }

        try (FileRawSnapshotReader snapshot = FileRawSnapshotReader.open(tempDir, offsetAndEpoch)) {
            int countBatches = 0;
            int countRecords = 0;
            for (RecordBatch batch : snapshot) {
                countBatches += 1;

                Iterator<Record> records = batch.streamingIterator(new GrowableBufferSupplier());
                while (records.hasNext()) {
                    Record record = records.next();

                    countRecords += 1;

                    assertFalse(record.hasKey());
                    assertTrue(record.hasValue());
                    assertEquals(bufferSize, record.value().remaining());
                    assertEquals(expectedBuffer, record.value());
                }
            }

            assertEquals(batches, countBatches);
            assertEquals(batches, countRecords);
        }
    }

    @Test
    public void testBatchWriteReadSnapshot() throws IOException {
        Path tempDir = TestUtils.tempDirectory().toPath();
        OffsetAndEpoch offsetAndEpoch = new OffsetAndEpoch(10L, 3);
        int bufferSize = 256;
        int batchSize = 3;
        int batches = 10;

        try (FileRawSnapshotWriter snapshot = FileRawSnapshotWriter.create(tempDir, offsetAndEpoch)) {
            for (int i = 0; i < batches; i++) {
                ByteBuffer[] buffers = IntStream
                    .range(0, batchSize)
                    .mapToObj(ignore -> ByteBuffer.wrap(randomBytes(bufferSize))).toArray(ByteBuffer[]::new);

                snapshot.append(buildRecords(buffers).buffer());
            }

            snapshot.freeze();
        }

        try (FileRawSnapshotReader snapshot = FileRawSnapshotReader.open(tempDir, offsetAndEpoch)) {
            int countBatches = 0;
            int countRecords = 0;
            for (RecordBatch batch : snapshot) {
                countBatches += 1;

                Iterator<Record> records = batch.streamingIterator(new GrowableBufferSupplier());
                while (records.hasNext()) {
                    Record record = records.next();

                    countRecords += 1;

                    assertFalse(record.hasKey());
                    assertTrue(record.hasValue());
                    assertEquals(bufferSize, record.value().remaining());
                }
            }

            assertEquals(batches, countBatches);
            assertEquals(batches * batchSize, countRecords);
        }
    }

    @Test
    public void testBufferWriteReadSnapshot() throws IOException {
        Path tempDir = TestUtils.tempDirectory().toPath();
        OffsetAndEpoch offsetAndEpoch = new OffsetAndEpoch(10L, 3);
        int bufferSize = 256;
        int batchSize = 3;
        int batches = 10;
        int expectedSize = 0;

        try (FileRawSnapshotWriter snapshot = FileRawSnapshotWriter.create(tempDir, offsetAndEpoch)) {
            for (int i = 0; i < batches; i++) {
                ByteBuffer[] buffers = IntStream
                    .range(0, batchSize)
                    .mapToObj(ignore -> ByteBuffer.wrap(randomBytes(bufferSize))).toArray(ByteBuffer[]::new);

                MemoryRecords records = buildRecords(buffers);
                snapshot.append(records.buffer());
                expectedSize += records.sizeInBytes();
            }

            assertEquals(expectedSize, snapshot.sizeInBytes());

            snapshot.freeze();
        }

        // File should exist and the size should be the sum of all the buffers
        assertTrue(Files.exists(Snapshots.snapshotPath(tempDir, offsetAndEpoch)));
        assertEquals(expectedSize, Files.size(Snapshots.snapshotPath(tempDir, offsetAndEpoch)));

        try (FileRawSnapshotReader snapshot = FileRawSnapshotReader.open(tempDir, offsetAndEpoch)) {
            int countBatches = 0;
            int countRecords = 0;

            for (RecordBatch batch : snapshot) {
                countBatches += 1;

                Iterator<Record> records = batch.streamingIterator(new GrowableBufferSupplier());
                while (records.hasNext()) {
                    Record record = records.next();

                    countRecords += 1;

                    assertFalse(record.hasKey());
                    assertTrue(record.hasValue());
                    assertEquals(bufferSize, record.value().remaining());
                }
            }

            assertEquals(batches, countBatches);
            assertEquals(batches * batchSize, countRecords);
        }
    }

    @Test
    public void testAbortedSnapshot() throws IOException {
        Path tempDir = TestUtils.tempDirectory().toPath();
        OffsetAndEpoch offsetAndEpoch = new OffsetAndEpoch(20L, 2);
        int bufferSize = 256;
        int batches = 10;

        try (FileRawSnapshotWriter snapshot = FileRawSnapshotWriter.create(tempDir, offsetAndEpoch)) {
            MemoryRecords records = buildRecords(ByteBuffer.wrap(randomBytes(bufferSize)));
            for (int i = 0; i < batches; i++) {
                snapshot.append(records.buffer());
            }
        }

        // File should not exist since freeze was not called before
        assertFalse(Files.exists(Snapshots.snapshotPath(tempDir, offsetAndEpoch)));
        assertEquals(0, Files.list(Snapshots.snapshotDir(tempDir)).count());
    }

    @Test
    public void testAppendToFrozenSnapshot() throws IOException {
        Path tempDir = TestUtils.tempDirectory().toPath();
        OffsetAndEpoch offsetAndEpoch = new OffsetAndEpoch(10L, 3);
        int bufferSize = 256;
        int batches = 10;

        try (FileRawSnapshotWriter snapshot = FileRawSnapshotWriter.create(tempDir, offsetAndEpoch)) {
            MemoryRecords records = buildRecords(ByteBuffer.wrap(randomBytes(bufferSize)));
            for (int i = 0; i < batches; i++) {
                snapshot.append(records.buffer());
            }

            snapshot.freeze();

            assertThrows(RuntimeException.class, () -> snapshot.append(records.buffer()));
        }

        // File should exist and the size should be greater than the sum of all the buffers
        assertTrue(Files.exists(Snapshots.snapshotPath(tempDir, offsetAndEpoch)));
        assertTrue(Files.size(Snapshots.snapshotPath(tempDir, offsetAndEpoch)) > bufferSize * batches);
    }

    @Test
    public void testCreateSnapshotWithSameId() throws IOException {
        Path tempDir = TestUtils.tempDirectory().toPath();
        OffsetAndEpoch offsetAndEpoch = new OffsetAndEpoch(20L, 2);
        int bufferSize = 256;
        int batches = 1;

        try (FileRawSnapshotWriter snapshot = FileRawSnapshotWriter.create(tempDir, offsetAndEpoch)) {
            MemoryRecords records = buildRecords(ByteBuffer.wrap(randomBytes(bufferSize)));
            for (int i = 0; i < batches; i++) {
                snapshot.append(records.buffer());
            }

            snapshot.freeze();
        }

        // Create another snapshot with the same id
        try (FileRawSnapshotWriter snapshot = FileRawSnapshotWriter.create(tempDir, offsetAndEpoch)) {
            MemoryRecords records = buildRecords(ByteBuffer.wrap(randomBytes(bufferSize)));
            for (int i = 0; i < batches; i++) {
                snapshot.append(records.buffer());
            }

            snapshot.freeze();
        }
    }

    private static byte[] randomBytes(int size) {
        byte[] array = new byte[size];

        TestUtils.SEEDED_RANDOM.nextBytes(array);

        return array;
    }

    private static MemoryRecords buildRecords(ByteBuffer... buffers) {
        return MemoryRecords.withRecords(
            CompressionType.NONE,
            Arrays.stream(buffers).map(buffer -> new SimpleRecord(buffer)).toArray(SimpleRecord[]::new)
        );
    }
}
