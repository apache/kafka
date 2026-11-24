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
import org.apache.kafka.common.record.internal.DefaultRecordBatch;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.utils.internals.BufferSupplier;
import org.apache.kafka.raft.Batch;
import org.apache.kafka.raft.ControlRecord;
import org.apache.kafka.raft.internals.RecordsIteratorTest.TestBatch;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class RecordsDecodingStrategyTest {
    private static final StringSerde STRING_SERDE = new StringSerde();

    // The records under test: a control batch at offset 0 followed by a data batch at offsets 1,2,3.
    private static final long CONTROL_BASE_OFFSET = 0L;
    private static final long DATA_BASE_OFFSET = 1L;
    private static final List<String> DATA_RECORDS = List.of("a", "b", "c");
    private static final long DATA_LAST_OFFSET = DATA_BASE_OFFSET + DATA_RECORDS.size() - 1;
    private static final List<ControlRecord> CONTROL_RECORDS = List.of(ControlRecord.of(new KRaftVersionRecord()));

    @Test
    void testDataAndControl() {
        RecordsDecodingStrategy<String> strategy = RecordsDecodingStrategy.dataAndControl(STRING_SERDE);
        assertControlBatchDecoded(strategy.readBatch(controlBatch(), BufferSupplier.NO_CACHING));
        assertDataBatchDecoded(strategy.readBatch(dataBatch(), BufferSupplier.NO_CACHING));
    }

    @Test
    void testControlOnly() {
        RecordsDecodingStrategy<String> strategy = RecordsDecodingStrategy.controlOnly();
        assertControlBatchDecoded(strategy.readBatch(controlBatch(), BufferSupplier.NO_CACHING));
        assertDataBatchSkipped(strategy.readBatch(dataBatch(), BufferSupplier.NO_CACHING));
    }

    @Test
    void testDataOnly() {
        RecordsDecodingStrategy<String> strategy = RecordsDecodingStrategy.dataOnly(STRING_SERDE);
        assertControlBatchSkipped(strategy.readBatch(controlBatch(), BufferSupplier.NO_CACHING));
        assertDataBatchDecoded(strategy.readBatch(dataBatch(), BufferSupplier.NO_CACHING));
    }

    @Test
    void testNone() {
        RecordsDecodingStrategy<String> strategy = RecordsDecodingStrategy.none();
        assertControlBatchSkipped(strategy.readBatch(controlBatch(), BufferSupplier.NO_CACHING));
        assertDataBatchSkipped(strategy.readBatch(dataBatch(), BufferSupplier.NO_CACHING));
    }

    private static void assertControlBatchDecoded(Batch<String> batch) {
        assertEquals(CONTROL_RECORDS, batch.controlRecords());
        assertEquals(CONTROL_BASE_OFFSET, batch.baseOffset());
        assertEquals(CONTROL_BASE_OFFSET, batch.lastOffset());
    }

    private static void assertControlBatchSkipped(Batch<String> batch) {
        assertTrue(batch.controlRecords().isEmpty());
        assertEquals(CONTROL_BASE_OFFSET, batch.baseOffset());
        assertEquals(CONTROL_BASE_OFFSET, batch.lastOffset());
    }

    private static void assertDataBatchDecoded(Batch<String> batch) {
        assertEquals(DATA_RECORDS, batch.records());
        assertEquals(DATA_BASE_OFFSET, batch.baseOffset());
        assertEquals(DATA_LAST_OFFSET, batch.lastOffset());
    }

    private static void assertDataBatchSkipped(Batch<String> batch) {
        assertTrue(batch.records().isEmpty());
        assertEquals(DATA_BASE_OFFSET, batch.baseOffset());
        assertEquals(DATA_LAST_OFFSET, batch.lastOffset());
    }

    private static DefaultRecordBatch controlBatch() {
        MemoryRecords records = RecordsIteratorTest.buildControlRecords(ControlRecordType.KRAFT_VERSION);
        return (DefaultRecordBatch) records.batches().iterator().next();
    }

    private static DefaultRecordBatch dataBatch() {
        TestBatch<String> batch = new TestBatch<>(DATA_BASE_OFFSET, 1, 100L, DATA_RECORDS);
        MemoryRecords records = RecordsIteratorTest.buildRecords(CompressionType.NONE, List.of(batch));
        return (DefaultRecordBatch) records.batches().iterator().next();
    }
}
