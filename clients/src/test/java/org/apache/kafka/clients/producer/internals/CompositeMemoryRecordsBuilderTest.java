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
package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.MemoryRecordsBuilder;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.record.internal.RecordBatch;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Verifies that {@link CompositeMemoryRecordsBuilder} — which writes the v2 batch header into the
 * first chunk and computes the batch CRC across the chunks — produces output byte-for-byte identical
 * to the single-buffer {@link MemoryRecordsBuilder} for the same records. Byte identity is the
 * strongest possible check here: it covers the header fields, the cross-chunk CRC, and the record
 * bytes at once, and proves a broker would accept the batch. Chunk sizes are varied so records
 * straddle chunk boundaries.
 */
public class CompositeMemoryRecordsBuilderTest {

    private static final long BASE_OFFSET = 0L;

    private static final class Rec {
        final long timestamp;
        final byte[] key;
        final byte[] value;

        Rec(long timestamp, byte[] key, byte[] value) {
            this.timestamp = timestamp;
            this.key = key;
            this.value = value;
        }

        long timestamp() {
            return timestamp;
        }

        byte[] key() {
            return key;
        }

        byte[] value() {
            return value;
        }
    }

    private static final List<Rec> RECORDS = List.of(
            new Rec(100L, "key1".getBytes(), "value-one".getBytes()),
            new Rec(101L, "key2".getBytes(), "the second, noticeably longer value".getBytes()),
            new Rec(102L, null, "third".getBytes()),
            new Rec(103L, "k4".getBytes(), new byte[80]));

    /** Reference batch built the normal (single contiguous buffer) way. */
    private MemoryRecords reference() {
        MemoryRecordsBuilder builder = MemoryRecords.builder(ByteBuffer.allocate(1024),
                RecordBatch.CURRENT_MAGIC_VALUE, Compression.NONE, TimestampType.CREATE_TIME, BASE_OFFSET,
                RecordBatch.NO_TIMESTAMP);
        for (Rec r : RECORDS) {
            builder.append(r.timestamp(), r.key(), r.value());
        }
        return builder.build();
    }

    private CompositeMemoryRecordsBuilder chunkedBuilder(int chunkSize, int chunkCount) {
        List<ByteBuffer> chunks = new ArrayList<>(chunkCount);
        for (int i = 0; i < chunkCount; i++) {
            chunks.add(ByteBuffer.allocate(chunkSize));
        }
        // pool == null: nothing is returned to a pool in this unit test.
        ChunkedByteBufferOutputStream stream = new ChunkedByteBufferOutputStream(chunks, chunkSize, null);
        return new CompositeMemoryRecordsBuilder(stream, RecordBatch.CURRENT_MAGIC_VALUE, Compression.NONE,
                TimestampType.CREATE_TIME, BASE_OFFSET, RecordBatch.NO_TIMESTAMP, RecordBatch.NO_PRODUCER_ID,
                RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE, false, false,
                RecordBatch.NO_PARTITION_LEADER_EPOCH, /* writeLimit */ 8192);
    }

    @ParameterizedTest
    @ValueSource(ints = {64, 96, 128, 4096})
    public void testCompositeBatchIsByteIdenticalToSingleBuffer(int chunkSize) {
        MemoryRecords expected = reference();

        // Enough chunks to hold the whole batch (the stream does not grow on its own).
        int chunkCount = expected.sizeInBytes() / chunkSize + 2;
        CompositeMemoryRecordsBuilder builder = chunkedBuilder(chunkSize, chunkCount);
        for (Rec r : RECORDS) {
            builder.append(r.timestamp(), r.key(), r.value(), Record.EMPTY_HEADERS);
        }
        CompositeMemoryRecords composite = (CompositeMemoryRecords) builder.build();

        // Byte identity => identical header and identical cross-chunk CRC.
        assertEquals(expected, composite.flatten());
        composite.flatten().batches().iterator().next().ensureValid();
    }

    @Test
    public void testEmptyBatchProducesEmptyRecords() {
        CompositeMemoryRecordsBuilder builder = chunkedBuilder(64, 4);
        CompositeMemoryRecords records = builder.build();
        assertSame(CompositeMemoryRecords.EMPTY, records);
        assertEquals(0, records.sizeInBytes());
    }

    @Test
    public void testRejectsCompression() {
        List<ByteBuffer> chunks = new ArrayList<>();
        chunks.add(ByteBuffer.allocate(256));
        ChunkedByteBufferOutputStream stream = new ChunkedByteBufferOutputStream(chunks, 256, null);
        assertThrows(IllegalArgumentException.class, () ->
                new CompositeMemoryRecordsBuilder(stream, RecordBatch.CURRENT_MAGIC_VALUE, Compression.gzip().build(),
                        TimestampType.CREATE_TIME, BASE_OFFSET, RecordBatch.NO_TIMESTAMP, RecordBatch.NO_PRODUCER_ID,
                        RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE, false, false,
                        RecordBatch.NO_PARTITION_LEADER_EPOCH, 8192));
        // Rejected before the base constructor reserved the header or wrapped the stream.
        assertEquals(0, stream.position());
    }

    @Test
    public void testRejectsStreamNotAtPositionZero() {
        List<ByteBuffer> chunks = new ArrayList<>();
        chunks.add(ByteBuffer.allocate(256));
        ChunkedByteBufferOutputStream stream = new ChunkedByteBufferOutputStream(chunks, 256, null);
        stream.position(10);
        assertThrows(IllegalArgumentException.class, () ->
                new CompositeMemoryRecordsBuilder(stream, RecordBatch.CURRENT_MAGIC_VALUE, Compression.NONE,
                        TimestampType.CREATE_TIME, BASE_OFFSET, RecordBatch.NO_TIMESTAMP, RecordBatch.NO_PRODUCER_ID,
                        RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE, false, false,
                        RecordBatch.NO_PARTITION_LEADER_EPOCH, 8192));
    }
}
