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

import org.apache.kafka.common.record.BufferSupplier;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.raft.BatchReader;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import static java.util.Arrays.asList;
import static org.apache.kafka.test.TestUtils.tempFile;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RecordsBatchReaderTest {
    private static final int MAX_BATCH_BYTES = 128;

    private final MockTime time = new MockTime();
    private final StringSerde serde = new StringSerde();

    @ParameterizedTest
    @EnumSource(CompressionType.class)
    public void testReadFromMemoryRecords(CompressionType compressionType) {
        long baseOffset = 57;

        List<BatchReader.Batch<String>> batches = asList(
            new BatchReader.Batch<>(baseOffset, 1, asList("a", "b", "c")),
            new BatchReader.Batch<>(baseOffset + 3, 2, asList("d", "e")),
            new BatchReader.Batch<>(baseOffset + 5, 2, asList("f"))
        );

        MemoryRecords memRecords = buildRecords(compressionType, batches);
        testBatchReader(baseOffset, memRecords, batches);
    }

    @ParameterizedTest
    @EnumSource(CompressionType.class)
    public void testReadFromFileRecords(CompressionType compressionType) throws Exception {
        long baseOffset = 57;

        List<BatchReader.Batch<String>> batches = asList(
            new BatchReader.Batch<>(baseOffset, 1, asList("a", "b", "c")),
            new BatchReader.Batch<>(baseOffset + 3, 2, asList("d", "e")),
            new BatchReader.Batch<>(baseOffset + 5, 2, asList("f"))
        );

        MemoryRecords memRecords = buildRecords(compressionType, batches);

        FileRecords fileRecords = FileRecords.open(tempFile());
        fileRecords.append(memRecords);

        testBatchReader(baseOffset, fileRecords, batches);
    }

    private MemoryRecords buildRecords(
        CompressionType compressionType,
        List<BatchReader.Batch<String>> batches
    ) {
        ByteBuffer buffer = ByteBuffer.allocate(1024);

        for (BatchReader.Batch<String> batch : batches) {
            BatchBuilder<String> builder = new BatchBuilder<>(
                buffer,
                serde,
                compressionType,
                batch.baseOffset(),
                time.milliseconds(),
                false,
                batch.epoch(),
                MAX_BATCH_BYTES
            );

            for (String record : batch.records()) {
                builder.appendRecord(record, null);
            }

            builder.build();
        }

        buffer.flip();
        return MemoryRecords.readableRecords(buffer);
    }

    private void testBatchReader(
        long baseOffset,
        Records records,
        List<BatchReader.Batch<String>> expectedBatches
    ) {
        BufferSupplier bufferSupplier = Mockito.mock(BufferSupplier.class);
        Set<ByteBuffer> allocatedBuffers = Collections.newSetFromMap(new IdentityHashMap<>());

        Mockito.when(bufferSupplier.get(Mockito.anyInt())).thenAnswer(invocation -> {
            int size = invocation.getArgument(0);
            ByteBuffer buffer = ByteBuffer.allocate(size);
            allocatedBuffers.add(buffer);
            return buffer;
        });

        Mockito.doAnswer(invocation -> {
            ByteBuffer released = invocation.getArgument(0);
            allocatedBuffers.remove(released);
            return null;
        }).when(bufferSupplier).release(Mockito.any(ByteBuffer.class));

        @SuppressWarnings("unchecked")
        CloseListener<BatchReader<String>> closeListener = Mockito.mock(CloseListener.class);

        RecordsBatchReader<String> reader = new RecordsBatchReader<>(
            baseOffset,
            records,
            serde,
            bufferSupplier,
            closeListener
        );

        for (BatchReader.Batch<String> batch : expectedBatches) {
            assertTrue(reader.hasNext());
            assertEquals(batch, reader.next());
        }

        assertFalse(reader.hasNext());
        assertThrows(NoSuchElementException.class, reader::next);

        reader.close();
        Mockito.verify(closeListener).onClose(reader);
        assertEquals(Collections.emptySet(), allocatedBuffers);
    }

}
