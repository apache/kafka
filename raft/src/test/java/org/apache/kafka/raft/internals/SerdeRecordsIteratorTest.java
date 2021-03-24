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



import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.raft.BatchReader;
import org.apache.kafka.raft.RecordSerde;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public final class SerdeRecordsIteratorTest {
    private static final int MAX_BATCH_BYTES = 128;
    private static final RecordSerde<String> STRING_SERDE = new StringSerde();

    private static Stream<Arguments> emptyRecords() throws IOException {
        return Stream.of(
            FileRecords.open(TestUtils.tempFile()),
            MemoryRecords.EMPTY
        ).map(Arguments::of);
    }

    @ParameterizedTest
    @MethodSource("emptyRecords")
    void testEmptyRecords(Records records) throws IOException {
        testIterator(Collections.emptyList(), records);
    }

    @ParameterizedTest
    @EnumSource(CompressionType.class)
    public void testMemoryRecords(CompressionType compressionType) {
        List<BatchReader.Batch<String>> batches = createBatches(57);

        MemoryRecords memRecords = buildRecords(compressionType, batches);
        testIterator(batches, memRecords);
    }

    @ParameterizedTest
    @EnumSource(CompressionType.class)
    public void testFileRecords(CompressionType compressionType) throws IOException {
        List<BatchReader.Batch<String>> batches = createBatches(57);

        MemoryRecords memRecords = buildRecords(compressionType, batches);
        FileRecords fileRecords = FileRecords.open(TestUtils.tempFile());
        fileRecords.append(memRecords);

        testIterator(batches, fileRecords);
    }

    @Test
    public void testMaxBatchTooSmall() throws IOException {
        List<BatchReader.Batch<String>> batches = createBatches(57);

        MemoryRecords memRecords = buildRecords(CompressionType.NONE, batches);
        FileRecords fileRecords = FileRecords.open(TestUtils.tempFile());
        fileRecords.append(memRecords);

        SerdeRecordsIterator<String> iterator = createIterator(fileRecords, BufferSupplier.create(), 10);
        assertThrows(IllegalStateException.class, iterator::hasNext);
        assertThrows(IllegalStateException.class, iterator::next);
    }

    private void testIterator(
        List<BatchReader.Batch<String>> expectedBatches,
        Records records
    ) {
        Set<ByteBuffer> allocatedBuffers = Collections.newSetFromMap(new IdentityHashMap<>());

        SerdeRecordsIterator<String> iterator = createIterator(
            records,
            mockBufferSupplier(allocatedBuffers),
            MAX_BATCH_BYTES
        );

        for (BatchReader.Batch<String> batch : expectedBatches) {
            assertTrue(iterator.hasNext());
            assertEquals(batch, iterator.next());
        }

        assertFalse(iterator.hasNext());
        assertThrows(NoSuchElementException.class, iterator::next);

        iterator.close();
        assertEquals(Collections.emptySet(), allocatedBuffers);
    }

    static SerdeRecordsIterator<String> createIterator(Records records, BufferSupplier bufferSupplier, int maxBatchSize) {
        return new SerdeRecordsIterator<>(records, STRING_SERDE, bufferSupplier, maxBatchSize);
    }

    static BufferSupplier mockBufferSupplier(Set<ByteBuffer> buffers) {
        BufferSupplier bufferSupplier = Mockito.mock(BufferSupplier.class);

        Mockito.when(bufferSupplier.get(Mockito.anyInt())).thenAnswer(invocation -> {
            int size = invocation.getArgument(0);
            ByteBuffer buffer = ByteBuffer.allocate(size);
            buffers.add(buffer);
            return buffer;
        });

        Mockito.doAnswer(invocation -> {
            ByteBuffer released = invocation.getArgument(0);
            buffers.remove(released);
            return null;
        }).when(bufferSupplier).release(Mockito.any(ByteBuffer.class));

        return bufferSupplier;
    }

    public static List<BatchReader.Batch<String>> createBatches(long baseOffset) {
        return asList(
            BatchReader.Batch.of(baseOffset, 1, asList("a", "b", "c")),
            BatchReader.Batch.of(baseOffset + 3, 2, asList("d", "e")),
            BatchReader.Batch.of(baseOffset + 5, 2, asList("f"))
        );
    }

    public static MemoryRecords buildRecords(
        CompressionType compressionType,
        List<BatchReader.Batch<String>> batches
    ) {
        ByteBuffer buffer = ByteBuffer.allocate(1024);

        for (BatchReader.Batch<String> batch : batches) {
            BatchBuilder<String> builder = new BatchBuilder<>(
                buffer,
                STRING_SERDE,
                compressionType,
                batch.baseOffset(),
                12345L,
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
}
