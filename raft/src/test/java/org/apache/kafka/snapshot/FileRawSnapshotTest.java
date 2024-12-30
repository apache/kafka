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

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.UnalignedFileRecords;
import org.apache.kafka.common.record.UnalignedMemoryRecords;
import org.apache.kafka.common.utils.BufferSupplier.GrowableBufferSupplier;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.raft.OffsetAndEpoch;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Iterator;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public final class FileRawSnapshotTest {
    private Path tempDir = null;

    @BeforeEach
    public void setUp() {
        tempDir = TestUtils.tempDirectory().toPath();
    }

    @AfterEach
    public void tearDown() throws IOException {
        Utils.delete(tempDir.toFile());
    }

    @Test
    public void testWritingSnapshot() throws IOException {
        OffsetAndEpoch offsetAndEpoch = new OffsetAndEpoch(10L, 3);
        int bufferSize = 256;
        int numberOfBatches = 10;
        int expectedSize = 0;

        try (FileRawSnapshotWriter snapshot = createSnapshotWriter(tempDir, offsetAndEpoch)) {
            assertEquals(0, snapshot.sizeInBytes());

            UnalignedMemoryRecords records = buildRecords(ByteBuffer.wrap(randomBytes(bufferSize)));
            for (int i = 0; i < numberOfBatches; i++) {
                snapshot.append(records);
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
    public void testWriteReadSnapshot() {
        OffsetAndEpoch offsetAndEpoch = new OffsetAndEpoch(10L, 3);
        int bufferSize = 256;
        int numberOfBatches = 10;

        ByteBuffer expectedBuffer = ByteBuffer.wrap(randomBytes(bufferSize));

        try (FileRawSnapshotWriter snapshot = createSnapshotWriter(tempDir, offsetAndEpoch)) {
            UnalignedMemoryRecords records = buildRecords(expectedBuffer);
            for (int i = 0; i < numberOfBatches; i++) {
                snapshot.append(records);
            }

            snapshot.freeze();
        }

        try (FileRawSnapshotReader snapshot = FileRawSnapshotReader.open(tempDir, offsetAndEpoch)) {
            int countBatches = 0;
            int countRecords = 0;

            var batches = snapshot.records().batchIterator();
            while (batches.hasNext()) {
                RecordBatch batch = batches.next();
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

            assertEquals(numberOfBatches, countBatches);
            assertEquals(numberOfBatches, countRecords);
        }
    }

    @Test
    public void testPartialWriteReadSnapshot() {
        Path tempDir = TestUtils.tempDirectory().toPath();
        OffsetAndEpoch offsetAndEpoch = new OffsetAndEpoch(10L, 3);

        ByteBuffer records = buildRecords(ByteBuffer.wrap(Utils.utf8("foo"))).buffer();

        ByteBuffer expectedBuffer = ByteBuffer.wrap(records.array());

        ByteBuffer buffer1 = expectedBuffer.duplicate();
        buffer1.position(0);
        buffer1.limit(expectedBuffer.limit() / 2);
        ByteBuffer buffer2 = expectedBuffer.duplicate();
        buffer2.position(expectedBuffer.limit() / 2);
        buffer2.limit(expectedBuffer.limit());

        try (FileRawSnapshotWriter snapshot = createSnapshotWriter(tempDir, offsetAndEpoch)) {
            snapshot.append(new UnalignedMemoryRecords(buffer1));
            snapshot.append(new UnalignedMemoryRecords(buffer2));
            snapshot.freeze();
        }

        try (FileRawSnapshotReader snapshot = FileRawSnapshotReader.open(tempDir, offsetAndEpoch)) {
            int totalSize = Math.toIntExact(snapshot.sizeInBytes());
            assertEquals(expectedBuffer.remaining(), totalSize);

            UnalignedFileRecords record1 = (UnalignedFileRecords) snapshot.slice(0, totalSize / 2);
            UnalignedFileRecords record2 = (UnalignedFileRecords) snapshot.slice(totalSize / 2, totalSize - totalSize / 2);

            assertEquals(buffer1, TestUtils.toBuffer(record1));
            assertEquals(buffer2, TestUtils.toBuffer(record2));

            ByteBuffer readBuffer = ByteBuffer.allocate(record1.sizeInBytes() + record2.sizeInBytes());
            readBuffer.put(TestUtils.toBuffer(record1));
            readBuffer.put(TestUtils.toBuffer(record2));
            readBuffer.flip();
            assertEquals(expectedBuffer, readBuffer);
        }
    }

    @Test
    public void testBatchWriteReadSnapshot() {
        OffsetAndEpoch offsetAndEpoch = new OffsetAndEpoch(10L, 3);
        int bufferSize = 256;
        int batchSize = 3;
        int numberOfBatches = 10;

        try (FileRawSnapshotWriter snapshot = createSnapshotWriter(tempDir, offsetAndEpoch)) {
            for (int i = 0; i < numberOfBatches; i++) {
                ByteBuffer[] buffers = IntStream
                    .range(0, batchSize)
                    .mapToObj(ignore -> ByteBuffer.wrap(randomBytes(bufferSize))).toArray(ByteBuffer[]::new);

                snapshot.append(buildRecords(buffers));
            }

            snapshot.freeze();
        }

        try (FileRawSnapshotReader snapshot = FileRawSnapshotReader.open(tempDir, offsetAndEpoch)) {
            int countBatches = 0;
            int countRecords = 0;

            var batches = snapshot.records().batchIterator();
            while (batches.hasNext()) {
                RecordBatch batch = batches.next();
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

            assertEquals(numberOfBatches, countBatches);
            assertEquals(numberOfBatches * batchSize, countRecords);
        }
    }

    @Test
    public void testBufferWriteReadSnapshot() throws IOException {
        OffsetAndEpoch offsetAndEpoch = new OffsetAndEpoch(10L, 3);
        int bufferSize = 256;
        int batchSize = 3;
        int numberOfBatches = 10;
        int expectedSize = 0;

        try (FileRawSnapshotWriter snapshot = createSnapshotWriter(tempDir, offsetAndEpoch)) {
            for (int i = 0; i < numberOfBatches; i++) {
                ByteBuffer[] buffers = IntStream
                    .range(0, batchSize)
                    .mapToObj(ignore -> ByteBuffer.wrap(randomBytes(bufferSize))).toArray(ByteBuffer[]::new);

                UnalignedMemoryRecords records = buildRecords(buffers);
                snapshot.append(records);
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

            var batches = snapshot.records().batchIterator();
            while (batches.hasNext()) {
                RecordBatch batch = batches.next();
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

            assertEquals(numberOfBatches, countBatches);
            assertEquals(numberOfBatches * batchSize, countRecords);
        }
    }

    @Test
    public void testAbortedSnapshot() throws IOException {
        OffsetAndEpoch offsetAndEpoch = new OffsetAndEpoch(20L, 2);
        int bufferSize = 256;
        int numberOfBatches = 10;

        try (FileRawSnapshotWriter snapshot = createSnapshotWriter(tempDir, offsetAndEpoch)) {
            UnalignedMemoryRecords records = buildRecords(ByteBuffer.wrap(randomBytes(bufferSize)));
            for (int i = 0; i < numberOfBatches; i++) {
                snapshot.append(records);
            }
        }

        // File should not exist since freeze was not called before
        assertFalse(Files.exists(Snapshots.snapshotPath(tempDir, offsetAndEpoch)));
        assertEquals(0, Files.list(Snapshots.snapshotDir(tempDir)).count());
    }

    @Test
    public void testAppendToFrozenSnapshot() throws IOException {
        OffsetAndEpoch offsetAndEpoch = new OffsetAndEpoch(10L, 3);
        int bufferSize = 256;
        int numberOfBatches = 10;

        try (FileRawSnapshotWriter snapshot = createSnapshotWriter(tempDir, offsetAndEpoch)) {
            UnalignedMemoryRecords records = buildRecords(ByteBuffer.wrap(randomBytes(bufferSize)));
            for (int i = 0; i < numberOfBatches; i++) {
                snapshot.append(records);
            }

            snapshot.freeze();

            assertThrows(RuntimeException.class, () -> snapshot.append(records));
        }

        // File should exist and the size should be greater than the sum of all the buffers
        assertTrue(Files.exists(Snapshots.snapshotPath(tempDir, offsetAndEpoch)));
        assertTrue(Files.size(Snapshots.snapshotPath(tempDir, offsetAndEpoch)) > bufferSize * numberOfBatches);
    }

    @Test
    public void testCreateSnapshotWithSameId() {
        OffsetAndEpoch offsetAndEpoch = new OffsetAndEpoch(20L, 2);
        int bufferSize = 256;
        int numberOfBatches = 1;

        try (FileRawSnapshotWriter snapshot = createSnapshotWriter(tempDir, offsetAndEpoch)) {
            UnalignedMemoryRecords records = buildRecords(ByteBuffer.wrap(randomBytes(bufferSize)));
            for (int i = 0; i < numberOfBatches; i++) {
                snapshot.append(records);
            }

            snapshot.freeze();
        }

        // Create another snapshot with the same id
        try (FileRawSnapshotWriter snapshot = createSnapshotWriter(tempDir, offsetAndEpoch)) {
            UnalignedMemoryRecords records = buildRecords(ByteBuffer.wrap(randomBytes(bufferSize)));
            for (int i = 0; i < numberOfBatches; i++) {
                snapshot.append(records);
            }

            snapshot.freeze();
        }
    }

    private static byte[] randomBytes(int size) {
        byte[] array = new byte[size];

        TestUtils.SEEDED_RANDOM.nextBytes(array);

        return array;
    }

    private static UnalignedMemoryRecords buildRecords(ByteBuffer... buffers) {
        MemoryRecords records =  MemoryRecords.withRecords(
            Compression.NONE,
            Arrays.stream(buffers).map(SimpleRecord::new).toArray(SimpleRecord[]::new)
        );
        return new UnalignedMemoryRecords(records.buffer());
    }

    private static FileRawSnapshotWriter createSnapshotWriter(
        Path dir,
        OffsetAndEpoch snapshotId
    ) {
        return FileRawSnapshotWriter.create(dir, snapshotId);
    }
}
