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

import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.common.message.LeaderChangeMessage;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.protocol.Writable;
import org.apache.kafka.common.record.AbstractRecords;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.DefaultRecord;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.utils.ByteUtils;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Utils;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BatchAccumulatorTest {
    private final MemoryPool memoryPool = Mockito.mock(MemoryPool.class);
    private final MockTime time = new MockTime();
    private final StringSerde serde = new StringSerde();

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
            memoryPool,
            time,
            CompressionType.NONE,
            serde
        );
    }

    @Test
    public void testLeaderChangeMessageWritten() {
        int leaderEpoch = 17;
        long baseOffset = 0;
        int lingerMs = 50;
        int maxBatchSize = 512;

        ByteBuffer buffer = ByteBuffer.allocate(256);
        Mockito.when(memoryPool.tryAllocate(256))
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
        asList(APPEND, APPEND_ATOMIC).forEach(appender -> {
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

            List<String> records = asList("a", "b", "c", "d", "e", "f", "g", "h", "i");

            // Append records 
            assertEquals(baseOffset, appender.call(acc, leaderEpoch, records.subList(0, 1)));
            assertEquals(baseOffset + 2, appender.call(acc, leaderEpoch, records.subList(1, 3)));
            assertEquals(baseOffset + 5, appender.call(acc, leaderEpoch, records.subList(3, 6)));
            assertEquals(baseOffset + 7, appender.call(acc, leaderEpoch, records.subList(6, 8)));
            assertEquals(baseOffset + 8, appender.call(acc, leaderEpoch, records.subList(8, 9)));

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
        });
    }

    @Test
    public void testForceDrainBeforeAppendLeaderChangeMessage() {
        asList(APPEND, APPEND_ATOMIC).forEach(appender -> {
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

            List<String> records = asList("a", "b", "c", "d", "e", "f", "g", "h", "i");

            // Append records 
            assertEquals(baseOffset, appender.call(acc, leaderEpoch, records.subList(0, 1)));
            assertEquals(baseOffset + 2, appender.call(acc, leaderEpoch, records.subList(1, 3)));
            assertEquals(baseOffset + 5, appender.call(acc, leaderEpoch, records.subList(3, 6)));
            assertEquals(baseOffset + 7, appender.call(acc, leaderEpoch, records.subList(6, 8)));
            assertEquals(baseOffset + 8, appender.call(acc, leaderEpoch, records.subList(8, 9)));

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
        });
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
        assertEquals(baseOffset, acc.append(leaderEpoch, singletonList("a")));
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

        assertEquals(baseOffset, acc.append(leaderEpoch, singletonList("a")));
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

        assertEquals(baseOffset, acc.append(leaderEpoch, singletonList("a")));
        acc.close();
        Mockito.verify(memoryPool).release(buffer);
    }

    @Test
    public void testSingleBatchAccumulation() {
        asList(APPEND, APPEND_ATOMIC).forEach(appender -> {
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

            List<String> records = asList("a", "b", "c", "d", "e", "f", "g", "h", "i");
            assertEquals(baseOffset, appender.call(acc, leaderEpoch, records.subList(0, 1)));
            assertEquals(baseOffset + 2, appender.call(acc, leaderEpoch, records.subList(1, 3)));
            assertEquals(baseOffset + 5, appender.call(acc, leaderEpoch, records.subList(3, 6)));
            assertEquals(baseOffset + 7, appender.call(acc, leaderEpoch, records.subList(6, 8)));
            assertEquals(baseOffset + 8, appender.call(acc, leaderEpoch, records.subList(8, 9)));

            time.sleep(lingerMs);
            assertTrue(acc.needsDrain(time.milliseconds()));

            List<BatchAccumulator.CompletedBatch<String>> batches = acc.drain();
            assertEquals(1, batches.size());
            assertFalse(acc.needsDrain(time.milliseconds()));
            assertEquals(Long.MAX_VALUE - time.milliseconds(), acc.timeUntilDrain(time.milliseconds()));

            BatchAccumulator.CompletedBatch<String> batch = batches.get(0);
            assertEquals(records, batch.records.get());
            assertEquals(baseOffset, batch.baseOffset);
        });
    }

    @Test
    public void testMultipleBatchAccumulation() {
        asList(APPEND, APPEND_ATOMIC).forEach(appender -> {
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

            // Append entries until we have 4 batches to drain (3 completed, 1 building)
            while (acc.numCompletedBatches() < 3) {
                appender.call(acc, leaderEpoch, singletonList("foo"));
            }

            List<BatchAccumulator.CompletedBatch<String>> batches = acc.drain();
            assertEquals(4, batches.size());
            assertTrue(batches.stream().allMatch(batch -> batch.data.sizeInBytes() <= maxBatchSize));
        });
    }

    @Test
    public void testRecordsAreSplit() {
        int leaderEpoch = 17;
        long baseOffset = 157;
        int lingerMs = 50;
        String record = "a";
        int numberOfRecords = 9;
        int recordsPerBatch = 2;
        int batchHeaderSize = AbstractRecords.recordBatchHeaderSizeInBytes(
            RecordBatch.MAGIC_VALUE_V2,
            CompressionType.NONE
        );
        int maxBatchSize = batchHeaderSize + recordsPerBatch * recordSizeInBytes(record, recordsPerBatch);

        Mockito.when(memoryPool.tryAllocate(maxBatchSize))
            .thenReturn(ByteBuffer.allocate(maxBatchSize));

        BatchAccumulator<String> acc = buildAccumulator(
            leaderEpoch,
            baseOffset,
            lingerMs,
            maxBatchSize
        );

        List<String> records = Stream
            .generate(() -> record)
            .limit(numberOfRecords)
            .collect(Collectors.toList());
        assertEquals(baseOffset + numberOfRecords - 1, acc.append(leaderEpoch, records));

        time.sleep(lingerMs);
        assertTrue(acc.needsDrain(time.milliseconds()));

        List<BatchAccumulator.CompletedBatch<String>> batches = acc.drain();
        // ceilingDiv(records.size(), recordsPerBatch)
        int expectedBatches = (records.size() + recordsPerBatch - 1) / recordsPerBatch;
        assertEquals(expectedBatches, batches.size());
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

        StringSerde serde = Mockito.spy(new StringSerde());
        BatchAccumulator<String> acc = new BatchAccumulator<>(
            leaderEpoch,
            baseOffset,
            lingerMs,
            maxBatchSize,
            memoryPool,
            time,
            CompressionType.NONE,
            serde
        );

        CountDownLatch acquireLockLatch = new CountDownLatch(1);
        CountDownLatch releaseLockLatch = new CountDownLatch(1);

        // Do the first append outside the thread to start the linger timer
        Mockito.when(memoryPool.tryAllocate(maxBatchSize))
            .thenReturn(ByteBuffer.allocate(maxBatchSize));
        acc.append(leaderEpoch, singletonList("a"));

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

        Thread appendThread = new Thread(() -> acc.append(leaderEpoch, singletonList("b")));
        appendThread.start();

        // Attempt to drain while the append thread is holding the lock
        acquireLockLatch.await();
        time.sleep(lingerMs);
        assertTrue(acc.needsDrain(time.milliseconds()));
        assertEquals(Collections.emptyList(), acc.drain());
        assertTrue(acc.needsDrain(time.milliseconds()));

        // Now let the append thread complete and verify that we can finish the drain
        releaseLockLatch.countDown();
        appendThread.join();
        List<BatchAccumulator.CompletedBatch<String>> drained = acc.drain();
        assertEquals(1, drained.size());
        assertEquals(Long.MAX_VALUE - time.milliseconds(), acc.timeUntilDrain(time.milliseconds()));
        drained.stream().forEach(completedBatch -> {
            completedBatch.data.batches().forEach(recordBatch -> {
                assertEquals(leaderEpoch, recordBatch.partitionLeaderEpoch()); });
        });
    }

    int recordSizeInBytes(String record, int numberOfRecords) {
        int serdeSize = serde.recordSize("a", new ObjectSerializationCache());

        int recordSizeInBytes = DefaultRecord.sizeOfBodyInBytes(
            numberOfRecords,
            0,
            -1,
            serdeSize,
            DefaultRecord.EMPTY_HEADERS
        );

        return ByteUtils.sizeOfVarint(recordSizeInBytes) + recordSizeInBytes;
    }

    static interface Appender {
        Long call(BatchAccumulator<String> acc, int epoch, List<String> records);
    }

    static final Appender APPEND_ATOMIC = new Appender() {
        @Override
        public Long call(BatchAccumulator<String> acc, int epoch, List<String> records) {
            return acc.appendAtomic(epoch, records);
        }
    };

    static final Appender APPEND = new Appender() {
        @Override
        public Long call(BatchAccumulator<String> acc, int epoch, List<String> records) {
            return acc.append(epoch, records);
        }
    };
}
