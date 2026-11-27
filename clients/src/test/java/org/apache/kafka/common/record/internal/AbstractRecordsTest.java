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
package org.apache.kafka.common.record.internal;

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.CloseableIterator;
import org.apache.kafka.common.utils.Utils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InOrder;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AbstractRecordsTest {
    @ParameterizedTest
    @ValueSource(bytes = {RecordBatch.MAGIC_VALUE_V0, RecordBatch.MAGIC_VALUE_V1, RecordBatch.MAGIC_VALUE_V2})
    void testEstimateSizeInBytesConsumesIterator(byte magic) {
        assertEquals(0, AbstractRecords.estimateSizeInBytes(magic, 0L,
            CompressionType.NONE, Collections.emptyIterator()));

        MemoryRecords records = MemoryRecords.withRecords(magic, Compression.NONE,
            new SimpleRecord(1L, "key".getBytes(), "value".getBytes()),
            new SimpleRecord(2L, null, "another value".getBytes()));
        try (CloseableIterator<Record> iterator = records.records()) {
            assertEquals(records.sizeInBytes(), AbstractRecords.estimateSizeInBytes(magic, 0L,
                CompressionType.NONE, iterator));
            assertFalse(iterator.hasNext());
        }
    }

    @ParameterizedTest
    @EnumSource(CompressionType.class)
    void testRecordsAcrossBatches(CompressionType compressionType) {
        Compression compression = Compression.of(compressionType).build();
        MemoryRecords first = MemoryRecords.withRecords(compression,
            new SimpleRecord("a".getBytes()), new SimpleRecord("b".getBytes()));
        MemoryRecords second = MemoryRecords.withRecords(compression, new SimpleRecord("c".getBytes()));
        ByteBuffer buffer = ByteBuffer.allocate(first.sizeInBytes() + second.sizeInBytes());
        buffer.put(first.buffer()).put(second.buffer()).flip();

        try (CloseableIterator<Record> iterator = MemoryRecords.readableRecords(buffer).records()) {
            List<Record> records = Utils.toList(iterator);
            assertEquals(List.of("a", "b", "c"), records.stream()
                .map(record -> Utils.utf8(record.value())).collect(Collectors.toList()));
        }
    }

    @Test
    void testEarlyCloseDoesNotReadRemainingRecordsOrOpenNextBatch() {
        Record firstRecord = mock(Record.class);
        CloseableIterator<Record> first = spy(CloseableIterator.wrap(
            List.of(firstRecord, mock(Record.class)).iterator()));
        RecordBatch firstBatch = batch(first);
        RecordBatch nextBatch = mock(RecordBatch.class);

        try (CloseableIterator<Record> iterator = records(firstBatch, nextBatch).records()) {
            verify(firstBatch, never()).streamingIterator(any());
            assertTrue(iterator.hasNext());
            assertSame(firstRecord, iterator.next());
            verify(first).next();
        }

        verify(first).close();
        verify(nextBatch, never()).streamingIterator(any());
    }

    @Test
    void testClosesExhaustedBatchesAndSkipsEmptyBatches() {
        Record firstRecord = mock(Record.class);
        Record lastRecord = mock(Record.class);
        CloseableIterator<Record> first = spy(CloseableIterator.wrap(List.of(firstRecord).iterator()));
        CloseableIterator<Record> empty = spy(CloseableIterator.wrap(Collections.<Record>emptyIterator()));
        CloseableIterator<Record> last = spy(CloseableIterator.wrap(List.of(lastRecord).iterator()));
        RecordBatch emptyBatch = batch(empty);
        RecordBatch lastBatch = batch(last);

        try (CloseableIterator<Record> iterator = records(batch(first), emptyBatch, lastBatch).records()) {
            assertEquals(List.of(firstRecord, lastRecord), Utils.toList(iterator));
            verify(last).close();
        }

        InOrder order = inOrder(first, emptyBatch, empty, lastBatch);
        order.verify(first).close();
        order.verify(emptyBatch).streamingIterator(BufferSupplier.NO_CACHING);
        order.verify(empty).close();
        order.verify(lastBatch).streamingIterator(BufferSupplier.NO_CACHING);
        verify(last).close();
    }

    @Test
    void testCloseDiscardsPrefetchedRecordAndIsIdempotent() {
        CloseableIterator<Record> batchIterator = spy(CloseableIterator.wrap(List.of(mock(Record.class)).iterator()));
        try (CloseableIterator<Record> iterator = records(batch(batchIterator)).records()) {
            assertTrue(iterator.hasNext());
            iterator.close();
            assertFalse(iterator.hasNext());
            assertThrows(NoSuchElementException.class, iterator::next);
        }
        verify(batchIterator).close();
    }

    @Test
    void testCloseBeforeReadingDoesNotOpenBatch() {
        RecordBatch batch = mock(RecordBatch.class);
        try (CloseableIterator<Record> iterator = records(batch).records()) {
            iterator.close();
            assertFalse(iterator.hasNext());
        }
        verify(batch, never()).streamingIterator(any());
    }

    @Test
    void testReadFailureClosesCurrentBatch() {
        CloseableIterator<Record> batchIterator = spy(CloseableIterator.wrap(List.of(mock(Record.class)).iterator()));
        RuntimeException failure = new RuntimeException("Failed to decode record");
        doThrow(failure).when(batchIterator).next();

        RuntimeException thrown = assertThrows(RuntimeException.class, () -> {
            try (CloseableIterator<Record> iterator = records(batch(batchIterator)).records()) {
                iterator.next();
            }
        });

        assertSame(failure, thrown);
        verify(batchIterator).close();
    }

    private static RecordBatch batch(CloseableIterator<Record> iterator) {
        RecordBatch batch = mock(RecordBatch.class);
        when(batch.streamingIterator(BufferSupplier.NO_CACHING)).thenReturn(iterator);
        return batch;
    }

    private static AbstractRecords records(RecordBatch... batches) {
        AbstractRecords records = mock(AbstractRecords.class, CALLS_REAL_METHODS);
        doReturn(List.of(batches)).when(records).batches();
        return records;
    }
}
