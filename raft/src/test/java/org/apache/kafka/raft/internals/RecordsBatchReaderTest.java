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

import org.apache.kafka.common.record.CompressionConfig;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.raft.BatchReader;
import org.apache.kafka.raft.internals.RecordsIteratorTest.TestBatch;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import static org.apache.kafka.test.TestUtils.tempFile;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RecordsBatchReaderTest {
    private static final int MAX_BATCH_BYTES = 128;

    private final StringSerde serde = new StringSerde();

    @ParameterizedTest
    @EnumSource(CompressionType.class)
    public void testReadFromMemoryRecords(CompressionType compressionType) {
        long baseOffset = 57;

        List<TestBatch<String>> batches = RecordsIteratorTest.createBatches(baseOffset);
        MemoryRecords memRecords = RecordsIteratorTest.buildRecords(CompressionConfig.of(compressionType).build(), batches);

        testBatchReader(baseOffset, memRecords, batches);
    }

    @ParameterizedTest
    @EnumSource(CompressionType.class)
    public void testReadFromFileRecords(CompressionType compressionType) throws Exception {
        long baseOffset = 57;

        List<TestBatch<String>> batches = RecordsIteratorTest.createBatches(baseOffset);
        MemoryRecords memRecords = RecordsIteratorTest.buildRecords(CompressionConfig.of(compressionType).build(), batches);

        FileRecords fileRecords = FileRecords.open(tempFile());
        fileRecords.append(memRecords);

        testBatchReader(baseOffset, fileRecords, batches);
    }

    private void testBatchReader(
        long baseOffset,
        Records records,
        List<TestBatch<String>> expectedBatches
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

        RecordsBatchReader<String> reader = RecordsBatchReader.of(
            baseOffset,
            records,
            serde,
            bufferSupplier,
            MAX_BATCH_BYTES,
            closeListener
        );

        for (TestBatch<String> batch : expectedBatches) {
            assertTrue(reader.hasNext());
            assertEquals(batch, TestBatch.from(reader.next()));
        }

        assertFalse(reader.hasNext());
        assertThrows(NoSuchElementException.class, reader::next);

        reader.close();
        Mockito.verify(closeListener).onClose(reader);
        assertEquals(Collections.emptySet(), allocatedBuffers);
    }

}
