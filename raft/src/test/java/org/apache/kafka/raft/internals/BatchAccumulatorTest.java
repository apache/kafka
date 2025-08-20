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

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.common.message.KRaftVersionRecord;
import org.apache.kafka.common.message.LeaderChangeMessage;
import org.apache.kafka.common.message.SnapshotHeaderRecord;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.protocol.Writable;
import org.apache.kafka.common.record.ControlRecordUtils;
import org.apache.kafka.common.record.DefaultRecord;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.ByteUtils;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Utils;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BatchAccumulatorTest {
    private final MemoryPool memoryPool = Mockito.mock(MemoryPool.class);
    private final MockTime time = new MockTime();
    private final StringSerde serde = new StringSerde();
    private final int maxNumberOfBatches = 10;

    private BatchAccumulator<String> buildAccumulator(
        int leaderEpoch,
        long baseOffset,
        int lingerMs,
        int maxBatchSize
    ) {
        return new BatchAccumulator<>(
            leaderEpoch,
            baseOffset,
            lingerMs,
            maxBatchSize,
            maxNumberOfBatches,
            memoryPool,
            time,
            Compression.NONE,
            serde
        );
    }

    @Test
    public void testLeaderChangeMessageWritten() {
        int leaderEpoch = 17;
        long baseOffset = 0;
        int lingerMs = 50;
        int maxBatchSize = 512;

        ByteBuffer buffer = ByteBuffer.allocate(maxBatchSize);
        Mockito.when(memoryPool.tryAllocate(maxBatchSize))
            .thenReturn(buffer);

        BatchAccumulator<String> acc = buildAccumulator(
            leaderEpoch,
            baseOffset,
            lingerMs,
            maxBatchSize
        );

        acc.appendLeaderChangeMessage(new LeaderChangeMessage(), time.milliseconds());
        assertTrue(acc.needsDrain(time.milliseconds()));

        List<BatchAccumulator.CompletedBatch<String>> batches = acc.drain();
        assertEquals(1, batches.size());

        BatchAccumulator.CompletedBatch<String> batch = batches.get(0);
        batch.release();
        Mockito.verify(memoryPool).release(buffer);
    }

    @Test
    public void testForceDrain() {
        int leaderEpoch = 17;
        long baseOffset = 157;
        int lingerMs = 50;
        int maxBatchSize = 512;

        Mockito.when(memoryPool.tryAllocate(maxBatchSize))
            .thenReturn(ByteBuffer.allocate(maxBatchSize));

        BatchAccumulator<String> acc = buildAccumulator(
            leaderEpoch,
            baseOffset,
            lingerMs,
            maxBatchSize
        );

        List<String> records = List.of("a", "b", "c", "d", "e", "f", "g", "h", "i");

        // Append records
        assertEquals(baseOffset, acc.append(leaderEpoch, records.subList(0, 1), false));
        assertEquals(baseOffset + 2, acc.append(leaderEpoch, records.subList(1, 3), false));
        assertEquals(baseOffset + 5, acc.append(leaderEpoch, records.subList(3, 6), false));
        assertEquals(baseOffset + 7, acc.append(leaderEpoch, records.subList(6, 8), false));
        assertEquals(baseOffset + 8, acc.append(leaderEpoch, records.subList(8, 9), false));

        assertFalse(acc.needsDrain(time.milliseconds()));
        acc.forceDrain();
        assertTrue(acc.needsDrain(time.milliseconds()));
        assertEquals(0, acc.timeUntilDrain(time.milliseconds()));

        // Drain completed batches
        List<BatchAccumulator.CompletedBatch<String>> batches = acc.drain();

        assertEquals(1, batches.size());
        assertFalse(acc.needsDrain(time.milliseconds()));
        assertEquals(Long.MAX_VALUE - time.milliseconds(), acc.timeUntilDrain(time.milliseconds()));

        BatchAccumulator.CompletedBatch<String> batch = batches.get(0);
        assertEquals(records, batch.records.get());
        assertEquals(baseOffset, batch.baseOffset);
        assertEquals(time.milliseconds(), batch.appendTimestamp());
    }

    @Test
    public void testForceDrainBeforeAppendLeaderChangeMessage() {
        int leaderEpoch = 17;
        long baseOffset = 157;
        int lingerMs = 50;
        int maxBatchSize = 512;

        Mockito.when(memoryPool.tryAllocate(maxBatchSize))
            .thenReturn(ByteBuffer.allocate(maxBatchSize));
        Mockito.when(memoryPool.tryAllocate(256))
            .thenReturn(ByteBuffer.allocate(256));

        BatchAccumulator<String> acc = buildAccumulator(
            leaderEpoch,
            baseOffset,
            lingerMs,
            maxBatchSize
        );

        List<String> records = List.of("a", "b", "c", "d", "e", "f", "g", "h", "i");

        // Append records
        assertEquals(baseOffset, acc.append(leaderEpoch, records.subList(0, 1), false));
        assertEquals(baseOffset + 2, acc.append(leaderEpoch, records.subList(1, 3), false));
        assertEquals(baseOffset + 5, acc.append(leaderEpoch, records.subList(3, 6), false));
        assertEquals(baseOffset + 7, acc.append(leaderEpoch, records.subList(6, 8), false));
        assertEquals(baseOffset + 8, acc.append(leaderEpoch, records.subList(8, 9), false));

        assertFalse(acc.needsDrain(time.milliseconds()));

        // Append a leader change message
        acc.appendLeaderChangeMessage(new LeaderChangeMessage(), time.milliseconds());

        assertTrue(acc.needsDrain(time.milliseconds()));

        // Test that drain status is FINISHED
        assertEquals(0, acc.timeUntilDrain(time.milliseconds()));

        // Drain completed batches
        List<BatchAccumulator.CompletedBatch<String>> batches = acc.drain();

        // Should have 2 batches, one consisting of `records` and one `leaderChangeMessage`
        assertEquals(2, batches.size());
        assertFalse(acc.needsDrain(time.milliseconds()));
        assertEquals(Long.MAX_VALUE - time.milliseconds(), acc.timeUntilDrain(time.milliseconds()));

        BatchAccumulator.CompletedBatch<String> batch = batches.get(0);
        assertEquals(records, batch.records.get());
        assertEquals(baseOffset, batch.baseOffset);
        assertEquals(time.milliseconds(), batch.appendTimestamp());
    }

    @Test
    public void testLingerIgnoredIfAccumulatorEmpty() {
        int leaderEpoch = 17;
        long baseOffset = 157;
        int lingerMs = 50;
        int maxBatchSize = 512;

        BatchAccumulator<String> acc = buildAccumulator(
            leaderEpoch,
            baseOffset,
            lingerMs,
            maxBatchSize
        );

        assertTrue(acc.isEmpty());
        assertFalse(acc.needsDrain(time.milliseconds()));
        assertEquals(Long.MAX_VALUE - time.milliseconds(), acc.timeUntilDrain(time.milliseconds()));
    }

    @Test
    public void testLingerBeginsOnFirstWrite() {
        int leaderEpoch = 17;
        long baseOffset = 157;
        int lingerMs = 50;
        int maxBatchSize = 512;

        Mockito.when(memoryPool.tryAllocate(maxBatchSize))
            .thenReturn(ByteBuffer.allocate(maxBatchSize));

        BatchAccumulator<String> acc = buildAccumulator(
            leaderEpoch,
            baseOffset,
            lingerMs,
            maxBatchSize
        );

        time.sleep(15);
        assertEquals(baseOffset, acc.append(leaderEpoch, List.of("a"), false));
        assertEquals(lingerMs, acc.timeUntilDrain(time.milliseconds()));
        assertFalse(acc.isEmpty());

        time.sleep(lingerMs / 2);
        assertEquals(lingerMs / 2, acc.timeUntilDrain(time.milliseconds()));
        assertFalse(acc.isEmpty());

        time.sleep(lingerMs / 2);
        assertEquals(0, acc.timeUntilDrain(time.milliseconds()));
        assertTrue(acc.needsDrain(time.milliseconds()));
        assertFalse(acc.isEmpty());
    }

    @Test
    public void testCompletedBatchReleaseBuffer() {
        int leaderEpoch = 17;
        long baseOffset = 157;
        int lingerMs = 50;
        int maxBatchSize = 512;

        ByteBuffer buffer = ByteBuffer.allocate(maxBatchSize);
        Mockito.when(memoryPool.tryAllocate(maxBatchSize))
            .thenReturn(buffer);

        BatchAccumulator<String> acc = buildAccumulator(
            leaderEpoch,
            baseOffset,
            lingerMs,
            maxBatchSize
        );

        assertEquals(baseOffset, acc.append(leaderEpoch, List.of("a"), false));
        time.sleep(lingerMs);

        List<BatchAccumulator.CompletedBatch<String>> batches = acc.drain();
        assertEquals(1, batches.size());

        BatchAccumulator.CompletedBatch<String> batch = batches.get(0);
        batch.release();
        Mockito.verify(memoryPool).release(buffer);
    }

    @Test
    public void testUnflushedBuffersReleasedByClose() {
        int leaderEpoch = 17;
        long baseOffset = 157;
        int lingerMs = 50;
        int maxBatchSize = 512;

        ByteBuffer buffer = ByteBuffer.allocate(maxBatchSize);
        Mockito.when(memoryPool.tryAllocate(maxBatchSize))
            .thenReturn(buffer);

        BatchAccumulator<String> acc = buildAccumulator(
            leaderEpoch,
            baseOffset,
            lingerMs,
            maxBatchSize
        );

        assertEquals(baseOffset, acc.append(leaderEpoch, List.of("a"), false));
        acc.close();
        Mockito.verify(memoryPool).release(buffer);
    }

    @Test
    public void testSingleBatchAccumulation() {
        int leaderEpoch = 17;
        long baseOffset = 157;
        int lingerMs = 50;
        int maxBatchSize = 512;

        Mockito.when(memoryPool.tryAllocate(maxBatchSize))
            .thenReturn(ByteBuffer.allocate(maxBatchSize));

        BatchAccumulator<String> acc = buildAccumulator(
            leaderEpoch,
            baseOffset,
            lingerMs,
            maxBatchSize
        );

        List<String> records = List.of("a", "b", "c", "d", "e", "f", "g", "h", "i");
        assertEquals(baseOffset, acc.append(leaderEpoch, records.subList(0, 1), false));
        assertEquals(baseOffset + 2, acc.append(leaderEpoch, records.subList(1, 3), false));
        assertEquals(baseOffset + 5, acc.append(leaderEpoch, records.subList(3, 6), false));
        assertEquals(baseOffset + 7, acc.append(leaderEpoch, records.subList(6, 8), false));
        assertEquals(baseOffset + 8, acc.append(leaderEpoch, records.subList(8, 9), false));

        long expectedAppendTimestamp = time.milliseconds();
        time.sleep(lingerMs);
        assertTrue(acc.needsDrain(time.milliseconds()));

        List<BatchAccumulator.CompletedBatch<String>> batches = acc.drain();
        assertEquals(1, batches.size());
        assertFalse(acc.needsDrain(time.milliseconds()));
        assertEquals(Long.MAX_VALUE - time.milliseconds(), acc.timeUntilDrain(time.milliseconds()));

        BatchAccumulator.CompletedBatch<String> batch = batches.get(0);
        assertEquals(records, batch.records.get());
        assertEquals(baseOffset, batch.baseOffset);
        assertEquals(expectedAppendTimestamp, batch.appendTimestamp());
    }

    @Test
    public void testMultipleBatchAccumulation() {
        int leaderEpoch = 17;
        long baseOffset = 157;
        int lingerMs = 50;
        int maxBatchSize = 256;
        int numberOfRecords = 100;

        Mockito.when(memoryPool.tryAllocate(maxBatchSize))
            .thenReturn(ByteBuffer.allocate(maxBatchSize));

        BatchAccumulator<String> acc = buildAccumulator(
            leaderEpoch,
            baseOffset,
            lingerMs,
            maxBatchSize
        );

        // Append enough records so that multiple batches get created
        for (int records = 0; records < numberOfRecords; records++) {
            acc.append(leaderEpoch, List.of("foo"), false);
        }

        List<BatchAccumulator.CompletedBatch<String>> batches = acc.drain();
        assertTrue(batches.size() > 1, () -> String.format("number of batches is %s", batches.size()));
        assertTrue(batches.stream().allMatch(batch -> batch.data.sizeInBytes() <= maxBatchSize));
    }

    @Test
    public void testCloseWhenEmpty() {
        int leaderEpoch = 17;
        long baseOffset = 157;
        int lingerMs = 50;
        int maxBatchSize = 256;

        BatchAccumulator<String> acc = buildAccumulator(
            leaderEpoch,
            baseOffset,
            lingerMs,
            maxBatchSize
        );

        acc.close();
        Mockito.verifyNoInteractions(memoryPool);
    }

    @Test
    public void testDrainDoesNotBlockWithConcurrentAppend() throws Exception {
        int leaderEpoch = 17;
        long baseOffset = 157;
        int lingerMs = 50;
        int maxBatchSize = 256;
        int maxNumberOfBatches = 10;

        StringSerde serde = Mockito.spy(new StringSerde());
        BatchAccumulator<String> acc = new BatchAccumulator<>(
            leaderEpoch,
            baseOffset,
            lingerMs,
            maxBatchSize,
            maxNumberOfBatches,
            memoryPool,
            time,
            Compression.NONE,
            serde
        );

        CountDownLatch acquireLockLatch = new CountDownLatch(1);
        CountDownLatch releaseLockLatch = new CountDownLatch(1);

        // Do the first append outside the thread to start the linger timer
        Mockito.when(memoryPool.tryAllocate(maxBatchSize))
            .thenReturn(ByteBuffer.allocate(maxBatchSize));
        acc.append(leaderEpoch, List.of("a"), false);

        // Let the serde block to simulate a slow append
        Mockito.doAnswer(invocation -> {
            Writable writable = invocation.getArgument(2);
            acquireLockLatch.countDown();
            releaseLockLatch.await();
            writable.writeByteArray(Utils.utf8("b"));
            return null;
        }).when(serde).write(
            Mockito.eq("b"),
            Mockito.any(ObjectSerializationCache.class),
            Mockito.any(Writable.class)
        );

        Thread appendThread = new Thread(() -> acc.append(leaderEpoch, List.of("b"), false));
        appendThread.start();

        // Attempt to drain while the append thread is holding the lock
        acquireLockLatch.await();
        time.sleep(lingerMs);
        assertTrue(acc.needsDrain(time.milliseconds()));
        assertEquals(List.of(), acc.drain());
        assertTrue(acc.needsDrain(time.milliseconds()));

        // Now let the append thread complete and verify that we can finish the drain
        releaseLockLatch.countDown();
        appendThread.join();
        List<BatchAccumulator.CompletedBatch<String>> drained = acc.drain();
        assertEquals(1, drained.size());
        assertEquals(Long.MAX_VALUE - time.milliseconds(), acc.timeUntilDrain(time.milliseconds()));
        drained.forEach(completedBatch ->
            completedBatch.data.batches().forEach(recordBatch ->
                assertEquals(leaderEpoch, recordBatch.partitionLeaderEpoch()))
        );

        acc.close();
    }

    int recordSizeInBytes(String record, int numberOfRecords) {
        int serdeSize = serde.recordSize(record, new ObjectSerializationCache());

        int recordSizeInBytes = DefaultRecord.sizeOfBodyInBytes(
            numberOfRecords,
            0,
            -1,
            serdeSize,
            DefaultRecord.EMPTY_HEADERS
        );

        return ByteUtils.sizeOfVarint(recordSizeInBytes) + recordSizeInBytes;
    }

    @Test
    public void testMultipleControlRecords() {
        int leaderEpoch = 17;
        long baseOffset = 157;
        int lingerMs = 50;
        int maxBatchSize = 512;

        ByteBuffer buffer = ByteBuffer.allocate(maxBatchSize);
        Mockito.when(memoryPool.tryAllocate(maxBatchSize))
                .thenReturn(buffer);

        try (BatchAccumulator<String> acc = buildAccumulator(
                leaderEpoch,
                baseOffset,
                lingerMs,
                maxBatchSize
            )
        ) {
            acc.appendControlMessages((offset, epoch, compression, buf) -> {
                long now = 1234;
                try (MemoryRecordsBuilder builder = controlRecordsBuilder(
                        offset,
                        epoch,
                        compression,
                        now,
                        buf
                    )
                ) {
                    builder.appendSnapshotHeaderMessage(
                        now,
                        new SnapshotHeaderRecord()
                            .setVersion(ControlRecordUtils.SNAPSHOT_HEADER_CURRENT_VERSION)
                            .setLastContainedLogTimestamp(now)
                    );

                    builder.appendKRaftVersionMessage(
                        now,
                        new KRaftVersionRecord()
                            .setVersion(ControlRecordUtils.KRAFT_VERSION_CURRENT_VERSION)
                            .setKRaftVersion((short) 0)
                    );

                    return builder.build();
                }
            });

            List<BatchAccumulator.CompletedBatch<String>> batches = acc.drain();
            assertEquals(1, batches.size());

            BatchAccumulator.CompletedBatch<String> batch = batches.get(0);
            assertEquals(baseOffset, batch.baseOffset);
            assertEquals(2, batch.numRecords);
            assertEquals(buffer.duplicate().flip(), batch.data.buffer());

            batch.release();
        }
    }

    @Test
    public void testInvalidControlRecordOffset() {
        int leaderEpoch = 17;
        long baseOffset = 157;
        int lingerMs = 50;
        int maxBatchSize = 512;

        ByteBuffer buffer = ByteBuffer.allocate(maxBatchSize);
        Mockito.when(memoryPool.tryAllocate(maxBatchSize))
                .thenReturn(buffer);

        BatchAccumulator.MemoryRecordsCreator creator = (offset, epoch, compression, buf) -> {
            long now = 1234;
            try (MemoryRecordsBuilder builder = controlRecordsBuilder(
                    offset + 1,
                    epoch,
                    compression,
                    now,
                    buf
                )
            ) {
                builder.appendSnapshotHeaderMessage(
                    now,
                    new SnapshotHeaderRecord()
                        .setVersion(ControlRecordUtils.SNAPSHOT_HEADER_CURRENT_VERSION)
                        .setLastContainedLogTimestamp(now)
                );

                return builder.build();
            }
        };

        try (BatchAccumulator<String> acc = buildAccumulator(
                leaderEpoch,
                baseOffset,
                lingerMs,
                maxBatchSize
            )
        ) {
            assertThrows(IllegalArgumentException.class, () -> acc.appendControlMessages(creator));
        }
    }

    @Test
    public void testInvalidControlRecordEpoch() {
        int leaderEpoch = 17;
        long baseOffset = 157;
        int lingerMs = 50;
        int maxBatchSize = 512;

        ByteBuffer buffer = ByteBuffer.allocate(maxBatchSize);
        Mockito.when(memoryPool.tryAllocate(maxBatchSize))
                .thenReturn(buffer);

        BatchAccumulator.MemoryRecordsCreator creator = (offset, epoch, compression, buf) -> {
            long now = 1234;
            try (MemoryRecordsBuilder builder = controlRecordsBuilder(
                    offset,
                    epoch + 1,
                    compression,
                    now,
                    buf
                )
            ) {
                builder.appendSnapshotHeaderMessage(
                    now,
                    new SnapshotHeaderRecord()
                        .setVersion(ControlRecordUtils.SNAPSHOT_HEADER_CURRENT_VERSION)
                        .setLastContainedLogTimestamp(now)
                );

                return builder.build();
            }
        };

        try (BatchAccumulator<String> acc = buildAccumulator(
                leaderEpoch,
                baseOffset,
                lingerMs,
                maxBatchSize
            )
        ) {
            assertThrows(IllegalArgumentException.class, () -> acc.appendControlMessages(creator));
        }
    }

    @Test
    public void testEmptyControlBatch() {
        int leaderEpoch = 17;
        long baseOffset = 157;
        int lingerMs = 50;
        int maxBatchSize = 512;

        ByteBuffer buffer = ByteBuffer.allocate(maxBatchSize);
        Mockito.when(memoryPool.tryAllocate(maxBatchSize))
                .thenReturn(buffer);

        BatchAccumulator.MemoryRecordsCreator creator = (offset, epoch, compression, buf) -> {
            long now = 1234;
            try (MemoryRecordsBuilder builder = controlRecordsBuilder(
                    offset,
                    epoch,
                    compression,
                    now,
                    buf
                )
            ) {
                // Create a control batch without any records
                return builder.build();
            }
        };

        try (BatchAccumulator<String> acc = buildAccumulator(
                leaderEpoch,
                baseOffset,
                lingerMs,
                maxBatchSize
            )
        ) {
            assertThrows(IllegalArgumentException.class, () -> acc.appendControlMessages(creator));
        }
    }

    @Test
    void testDelayedDrain() {
        int leaderEpoch = 17;
        long baseOffset = 157;
        int lingerMs = 50;
        int maxBatchSize = 256;

        Mockito.when(memoryPool.tryAllocate(maxBatchSize))
            .thenReturn(ByteBuffer.allocate(maxBatchSize));

        BatchAccumulator<String> acc = buildAccumulator(
            leaderEpoch,
            baseOffset,
            lingerMs,
            maxBatchSize
        );

        List<String> records = List.of("a", "b", "c", "d", "e", "f", "g");
        assertEquals(baseOffset, acc.append(leaderEpoch, records.subList(0, 1), true));
        acc.forceDrain();
        assertEquals(baseOffset + 2, acc.append(leaderEpoch, records.subList(1, 3), true));
        acc.forceDrain();
        assertEquals(baseOffset + 5, acc.append(leaderEpoch, records.subList(3, 6), true));
        acc.forceDrain();

        assertFalse(acc.needsDrain(time.milliseconds()));
        assertNotEquals(0, acc.timeUntilDrain(time.milliseconds()));
        assertEquals(0, acc.drain().size());

        acc.allowDrain();
        assertTrue(acc.needsDrain(time.milliseconds()));
        assertEquals(0, acc.timeUntilDrain(time.milliseconds()));
        assertEquals(3, acc.drain().size());
    }

    @Test
    void testDelayedDrainAreReleased() {
        int leaderEpoch = 17;
        long baseOffset = 157;
        int lingerMs = 50;
        int maxBatchSize = 256;

        ByteBuffer buffer = ByteBuffer.allocate(maxBatchSize);
        Mockito.when(memoryPool.tryAllocate(maxBatchSize))
            .thenReturn(buffer);

        BatchAccumulator<String> acc = buildAccumulator(
            leaderEpoch,
            baseOffset,
            lingerMs,
            maxBatchSize
        );

        List<String> records = List.of("a", "b", "c", "d", "e", "f", "g");
        assertEquals(baseOffset, acc.append(leaderEpoch, records.subList(0, 1), true));
        acc.forceDrain();

        acc.close();
        Mockito.verify(memoryPool).release(buffer);
    }

    @Test
    public void testMaxNumberOfBatches() {
        int leaderEpoch = 17;
        long baseOffset = 157;
        int lingerMs = 50;
        int maxBatchSize = 256;
        int maxNumberOfBatches = 2;

        Mockito.when(memoryPool.tryAllocate(maxBatchSize))
            .thenReturn(ByteBuffer.allocate(maxBatchSize));

        StringSerde serde = Mockito.spy(new StringSerde());
        BatchAccumulator<String> acc = new BatchAccumulator<>(
            leaderEpoch,
            baseOffset,
            lingerMs,
            maxBatchSize,
            maxNumberOfBatches,
            memoryPool,
            time,
            Compression.NONE,
            serde
        );

        List<String> records = List.of("a", "b", "c", "d", "e", "f", "g");
        assertEquals(baseOffset, acc.append(leaderEpoch, records.subList(0, 1), true));
        acc.forceDrain();
        assertEquals(baseOffset + 2, acc.append(leaderEpoch, records.subList(1, 3), true));
        acc.forceDrain();

        assertThrows(IllegalStateException.class, () -> acc.append(leaderEpoch, records.subList(3, 6), true));
    }

    private static MemoryRecordsBuilder controlRecordsBuilder(
        long baseOffset,
        int epoch,
        Compression compression,
        long now,
        ByteBuffer buffer
    ) {
        return new MemoryRecordsBuilder(
            buffer,
            RecordBatch.CURRENT_MAGIC_VALUE,
            compression,
            TimestampType.CREATE_TIME,
            baseOffset,
            now,
            RecordBatch.NO_PRODUCER_ID,
            RecordBatch.NO_PRODUCER_EPOCH,
            RecordBatch.NO_SEQUENCE,
            false, // isTransactional
            true,  // isControlBatch
            epoch,
            buffer.capacity()
        );
    }
}
