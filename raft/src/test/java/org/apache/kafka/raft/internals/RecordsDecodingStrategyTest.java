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

import org.apache.kafka.common.message.KRaftVersionRecord;
import org.apache.kafka.common.record.internal.CompressionType;
import org.apache.kafka.common.record.internal.ControlRecordType;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.utils.internals.BufferSupplier;
import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.raft.Batch;
import org.apache.kafka.raft.ControlRecord;
import org.apache.kafka.raft.internals.RecordsIteratorTest.TestBatch;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class RecordsDecodingStrategyTest {
    private static final StringSerde STRING_SERDE = new StringSerde();

    // The records under test: a control batch at offset 0 followed by a data batch at offsets 1,2,3.
    private static final long CONTROL_BASE_OFFSET = 0L;
    private static final long DATA_BASE_OFFSET = 1L;
    private static final List<String> DATA_RECORDS = List.of("a", "b", "c");
    private static final List<ControlRecord> CONTROL_RECORDS = List.of(ControlRecord.of(new KRaftVersionRecord()));
    private static final MemoryRecords RECORDS = controlBatchThenDataBatch();

    private static RecordsIterator<String> iterator(RecordsDecodingStrategy<String> strategy) {
        return new RecordsIterator<>(
            RECORDS,
            strategy,
            BufferSupplier.NO_CACHING,
            Integer.MAX_VALUE,
            true,
            new LogContext()
        );
    }

    @Test
    void testDataAndControl() {
        try (RecordsIterator<String> iterator = iterator(RecordsDecodingStrategy.dataAndControl(STRING_SERDE))) {
            // The control batch is decoded.
            Batch<String> controlBatch = iterator.next();
            assertEquals(CONTROL_RECORDS, controlBatch.controlRecords());
            assertEquals(CONTROL_BASE_OFFSET, controlBatch.baseOffset());
            assertEquals(CONTROL_BASE_OFFSET, controlBatch.lastOffset());

            // The data batch is decoded.
            Batch<String> dataBatch = iterator.next();
            assertEquals(DATA_RECORDS, dataBatch.records());
            assertEquals(DATA_BASE_OFFSET, dataBatch.baseOffset());
            assertEquals(DATA_BASE_OFFSET + DATA_RECORDS.size() - 1, dataBatch.lastOffset());
        }
    }

    @Test
    void testControlOnly() {
        try (RecordsIterator<String> iterator = iterator(RecordsDecodingStrategy.controlOnly())) {
            // The control batch is decoded.
            Batch<String> controlBatch = iterator.next();
            assertEquals(CONTROL_RECORDS, controlBatch.controlRecords());
            assertEquals(CONTROL_BASE_OFFSET, controlBatch.baseOffset());
            assertEquals(CONTROL_BASE_OFFSET, controlBatch.lastOffset());

            // The data records are skipped, but the offset information is preserved.
            Batch<String> dataBatch = iterator.next();
            assertTrue(dataBatch.records().isEmpty());
            assertEquals(DATA_BASE_OFFSET, dataBatch.baseOffset());
            assertEquals(DATA_BASE_OFFSET + DATA_RECORDS.size() - 1, dataBatch.lastOffset());
        }
    }

    @Test
    void testDataOnly() {
        try (RecordsIterator<String> iterator = iterator(RecordsDecodingStrategy.dataOnly(STRING_SERDE))) {
            // The control records are skipped, but the offset information is preserved.
            Batch<String> controlBatch = iterator.next();
            assertTrue(controlBatch.controlRecords().isEmpty());
            assertEquals(CONTROL_BASE_OFFSET, controlBatch.baseOffset());
            assertEquals(CONTROL_BASE_OFFSET, controlBatch.lastOffset());

            // The data batch is decoded.
            Batch<String> dataBatch = iterator.next();
            assertEquals(DATA_RECORDS, dataBatch.records());
            assertEquals(DATA_BASE_OFFSET, dataBatch.baseOffset());
            assertEquals(DATA_BASE_OFFSET + DATA_RECORDS.size() - 1, dataBatch.lastOffset());
        }
    }

    @Test
    void testNone() {
        try (RecordsIterator<String> iterator = iterator(RecordsDecodingStrategy.none())) {
            // Both the control and data records are skipped, but the offset information is preserved.
            Batch<String> controlBatch = iterator.next();
            assertTrue(controlBatch.controlRecords().isEmpty());
            assertEquals(CONTROL_BASE_OFFSET, controlBatch.baseOffset());
            assertEquals(CONTROL_BASE_OFFSET, controlBatch.lastOffset());

            Batch<String> dataBatch = iterator.next();
            assertTrue(dataBatch.records().isEmpty());
            assertEquals(DATA_BASE_OFFSET, dataBatch.baseOffset());
            assertEquals(DATA_BASE_OFFSET + DATA_RECORDS.size() - 1, dataBatch.lastOffset());
        }
    }

    // Builds records containing a control batch at CONTROL_BASE_OFFSET followed by a data batch at DATA_BASE_OFFSET.
    private static MemoryRecords controlBatchThenDataBatch() {
        MemoryRecords controlRecords = RecordsIteratorTest.buildControlRecords(ControlRecordType.KRAFT_VERSION);
        TestBatch<String> dataBatch = new TestBatch<>(DATA_BASE_OFFSET, 1, 100L, DATA_RECORDS);
        MemoryRecords dataRecords = RecordsIteratorTest.buildRecords(CompressionType.NONE, List.of(dataBatch));

        ByteBuffer combined = ByteBuffer.allocate(controlRecords.sizeInBytes() + dataRecords.sizeInBytes());
        combined.put(controlRecords.buffer());
        combined.put(dataRecords.buffer());
        combined.flip();
        return MemoryRecords.readableRecords(combined);
    }
}
