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

import org.apache.kafka.common.record.internal.CompressionType;
import org.apache.kafka.common.record.internal.ControlRecordType;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.utils.internals.BufferSupplier;
import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.raft.Batch;
import org.apache.kafka.raft.internals.RecordsIteratorTest.TestBatch;
import org.apache.kafka.server.common.serialization.RecordSerde;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class DecodingStrategyTest {
    private static final RecordSerde<String> STRING_SERDE = new StringSerde();

    private static RecordsIterator<String> iterator(MemoryRecords records, DecodingStrategy<String> strategy) {
        return new RecordsIterator<>(
            records,
            strategy,
            BufferSupplier.NO_CACHING,
            Integer.MAX_VALUE,
            true,
            new LogContext()
        );
    }

    @Test
    void testControlAndDataDecodesBoth() {
        TestBatch<String> dataBatch = new TestBatch<>(0L, 1, 100L, List.of("a", "b", "c"));
        MemoryRecords records = RecordsIteratorTest.buildRecords(CompressionType.NONE, List.of(dataBatch));

        try (RecordsIterator<String> iterator = iterator(records, new ControlAndDataDecodingStrategy<>(STRING_SERDE))) {
            assertTrue(iterator.hasNext());
            Batch<String> batch = iterator.next();
            assertEquals(List.of("a", "b", "c"), batch.records());
            assertEquals(0L, batch.baseOffset());
            assertEquals(2L, batch.lastOffset());
            assertFalse(iterator.hasNext());
        }
    }

    @Test
    void testControlAndDataDecodesControlBatch() {
        MemoryRecords records = RecordsIteratorTest.buildControlRecords(ControlRecordType.KRAFT_VERSION);

        try (RecordsIterator<String> iterator = iterator(records, new ControlAndDataDecodingStrategy<>(STRING_SERDE))) {
            assertTrue(iterator.hasNext());
            Batch<String> batch = iterator.next();
            assertFalse(batch.controlRecords().isEmpty());
            assertTrue(batch.records().isEmpty());
        }
    }

    @Test
    void testControlOnlySkipsDataRecords() {
        TestBatch<String> dataBatch = new TestBatch<>(5L, 1, 100L, List.of("a", "b", "c"));
        MemoryRecords records = RecordsIteratorTest.buildRecords(CompressionType.NONE, List.of(dataBatch));

        try (RecordsIterator<String> iterator = iterator(records, new ControlOnlyDecodingStrategy<>())) {
            assertTrue(iterator.hasNext());
            Batch<String> batch = iterator.next();
            assertTrue(batch.records().isEmpty());
            assertEquals(5L, batch.baseOffset());
            assertEquals(7L, batch.lastOffset());
            assertFalse(iterator.hasNext());
        }
    }

    @Test
    void testControlOnlyDecodesControlRecords() {
        MemoryRecords records = RecordsIteratorTest.buildControlRecords(ControlRecordType.KRAFT_VERSION);

        try (RecordsIterator<String> iterator = iterator(records, new ControlOnlyDecodingStrategy<>())) {
            assertTrue(iterator.hasNext());
            Batch<String> batch = iterator.next();
            assertFalse(batch.controlRecords().isEmpty());
        }
    }

    @Test
    void testDataOnlyDecodesDataRecords() {
        TestBatch<String> dataBatch = new TestBatch<>(0L, 1, 100L, List.of("a", "b", "c"));
        MemoryRecords records = RecordsIteratorTest.buildRecords(CompressionType.NONE, List.of(dataBatch));

        try (RecordsIterator<String> iterator = iterator(records, new DataOnlyDecodingStrategy<>(STRING_SERDE))) {
            assertTrue(iterator.hasNext());
            Batch<String> batch = iterator.next();
            assertEquals(List.of("a", "b", "c"), batch.records());
            assertFalse(iterator.hasNext());
        }
    }

    @Test
    void testDataOnlySkipsControlRecords() {
        MemoryRecords records = RecordsIteratorTest.buildControlRecords(ControlRecordType.KRAFT_VERSION);

        try (RecordsIterator<String> iterator = iterator(records, new DataOnlyDecodingStrategy<>(STRING_SERDE))) {
            assertTrue(iterator.hasNext());
            Batch<String> batch = iterator.next();
            assertTrue(batch.controlRecords().isEmpty());
            assertTrue(batch.records().isEmpty());
        }
    }
}
