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
package org.apache.kafka.common.record;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.utils.CloseableIterator;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestUtils;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import static org.apache.kafka.common.record.DefaultRecordBatch.RECORDS_COUNT_OFFSET;
import static org.apache.kafka.common.record.RecordBatch.MAGIC_VALUE_V2;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DefaultRecordBatchTest {

    private SimpleRecord[] simpleRecordSet = {
        new SimpleRecord(1L, "a".getBytes(), "1".getBytes()),
        new SimpleRecord(2L, "b".getBytes(), "2".getBytes()),
        new SimpleRecord(3L, "c".getBytes(), "3".getBytes())
    };

    @Test
    public void testWriteEmptyHeader() {
        long producerId = 23423L;
        short producerEpoch = 145;
        int baseSequence = 983;
        long baseOffset = 15L;
        long lastOffset = 37;
        int partitionLeaderEpoch = 15;
        long timestamp = System.currentTimeMillis();

        for (TimestampType timestampType : Arrays.asList(TimestampType.CREATE_TIME, TimestampType.LOG_APPEND_TIME)) {
            for (boolean isTransactional : Arrays.asList(true, false)) {
                for (boolean isControlBatch : Arrays.asList(true, false)) {
                    ByteBuffer buffer = ByteBuffer.allocate(2048);
                    DefaultRecordBatch.writeEmptyHeader(buffer, RecordBatch.CURRENT_MAGIC_VALUE, producerId,
                            producerEpoch, baseSequence, baseOffset, lastOffset, partitionLeaderEpoch, timestampType,
                            timestamp, isTransactional, isControlBatch);
                    buffer.flip();
                    DefaultRecordBatch batch = new DefaultRecordBatch(buffer);
                    assertEquals(producerId, batch.producerId());
                    assertEquals(producerEpoch, batch.producerEpoch());
                    assertEquals(baseSequence, batch.baseSequence());
                    assertEquals(baseSequence + ((int) (lastOffset - baseOffset)), batch.lastSequence());
                    assertEquals(baseOffset, batch.baseOffset());
                    assertEquals(lastOffset, batch.lastOffset());
                    assertEquals(partitionLeaderEpoch, batch.partitionLeaderEpoch());
                    assertEquals(isTransactional, batch.isTransactional());
                    assertEquals(timestampType, batch.timestampType());
                    assertEquals(timestamp, batch.maxTimestamp());
                    assertEquals(RecordBatch.NO_TIMESTAMP, batch.firstTimestamp());
                    assertEquals(isControlBatch, batch.isControlBatch());
                }
            }
        }
    }

    @Test
    public void buildDefaultRecordBatch() {
        RecordsBuilder builder = new RecordsBuilder(1234567L).withMagic(RecordBatch.MAGIC_VALUE_V2);
        RecordsBuilder.BatchBuilder batchBuilder = builder.newBatch();
        batchBuilder.append(new SimpleRecord(1L, "a".getBytes(), "v".getBytes()));
        batchBuilder.append(new SimpleRecord(2L, "b".getBytes(), "v".getBytes()));
        batchBuilder.closeBatch();

        MemoryRecords records = builder.build();
        for (MutableRecordBatch batch : records.batches()) {
            assertTrue(batch.isValid());
            assertEquals(1234567, batch.baseOffset());
            assertEquals(1234568, batch.lastOffset());
            assertEquals(2L, batch.maxTimestamp());
            assertEquals(RecordBatch.NO_PRODUCER_ID, batch.producerId());
            assertEquals(RecordBatch.NO_PRODUCER_EPOCH, batch.producerEpoch());
            assertEquals(RecordBatch.NO_SEQUENCE, batch.baseSequence());
            assertEquals(RecordBatch.NO_SEQUENCE, batch.lastSequence());

            for (Record record : batch) {
                assertTrue(record.isValid());
            }
        }
    }

    @Test
    public void buildDefaultRecordBatchWithProducerId() {
        long pid = 23423L;
        short epoch = 145;
        int baseSequence = 983;

        RecordsBuilder builder = new RecordsBuilder(1234567L).withMagic(RecordBatch.MAGIC_VALUE_V2);
        RecordsBuilder.BatchBuilder batchBuilder = builder.newBatch()
                .withProducerMetadata(pid, epoch, baseSequence);
        batchBuilder.append(new SimpleRecord(1L, "a".getBytes(), "v".getBytes()));
        batchBuilder.append(new SimpleRecord(2L, "b".getBytes(), "v".getBytes()));
        batchBuilder.closeBatch();

        MemoryRecords records = builder.build();
        for (MutableRecordBatch batch : records.batches()) {
            assertTrue(batch.isValid());
            assertEquals(1234567, batch.baseOffset());
            assertEquals(1234568, batch.lastOffset());
            assertEquals(2L, batch.maxTimestamp());
            assertEquals(pid, batch.producerId());
            assertEquals(epoch, batch.producerEpoch());
            assertEquals(baseSequence, batch.baseSequence());
            assertEquals(baseSequence + 1, batch.lastSequence());

            for (Record record : batch) {
                assertTrue(record.isValid());
            }
        }
    }

    @Test
    public void buildDefaultRecordBatchWithSequenceWrapAround() {
        long pid = 23423L;
        short epoch = 145;
        int baseSequence = Integer.MAX_VALUE - 1;

        RecordsBuilder builder = new RecordsBuilder(1234567L).withMagic(RecordBatch.MAGIC_VALUE_V2);
        RecordsBuilder.BatchBuilder batchBuilder = builder.newBatch()
                .withProducerMetadata(pid, epoch, baseSequence);
        batchBuilder.append(new SimpleRecord(1L, "a".getBytes(), "v".getBytes()));
        batchBuilder.append(new SimpleRecord(2L, "b".getBytes(), "v".getBytes()));
        batchBuilder.append(new SimpleRecord(3L, "c".getBytes(), "v".getBytes()));
        batchBuilder.closeBatch();

        MemoryRecords records = builder.build();
        List<MutableRecordBatch> batches = TestUtils.toList(records.batches());
        assertEquals(1, batches.size());
        RecordBatch batch = batches.get(0);

        assertEquals(pid, batch.producerId());
        assertEquals(epoch, batch.producerEpoch());
        assertEquals(baseSequence, batch.baseSequence());
        assertEquals(0, batch.lastSequence());
        List<Record> allRecords = TestUtils.toList(batch);
        assertEquals(3, allRecords.size());
        assertEquals(Integer.MAX_VALUE - 1, allRecords.get(0).sequence());
        assertEquals(Integer.MAX_VALUE, allRecords.get(1).sequence());
        assertEquals(0, allRecords.get(2).sequence());
    }

    @Test
    public void testSizeInBytes() {
        Header[] headers = new Header[] {
            new RecordHeader("foo", "value".getBytes()),
            new RecordHeader("bar", (byte[]) null)
        };

        long timestamp = System.currentTimeMillis();
        SimpleRecord[] records = new SimpleRecord[] {
            new SimpleRecord(timestamp, "key".getBytes(), "value".getBytes()),
            new SimpleRecord(timestamp + 30000, null, "value".getBytes()),
            new SimpleRecord(timestamp + 60000, "key".getBytes(), null),
            new SimpleRecord(timestamp + 60000, "key".getBytes(), "value".getBytes(), headers)
        };
        int actualSize = new RecordsBuilder().addBatch(records).build().sizeInBytes();
        assertEquals(actualSize, DefaultRecordBatch.sizeInBytes(Arrays.asList(records)));
    }

    @Test(expected = InvalidRecordException.class)
    public void testInvalidRecordSize() {
        MemoryRecords records = new RecordsBuilder()
                .withMagic(RecordBatch.MAGIC_VALUE_V2)
                .addBatch(simpleRecordSet)
                .build();

        ByteBuffer buffer = records.buffer();
        buffer.putInt(DefaultRecordBatch.LENGTH_OFFSET, 10);

        DefaultRecordBatch batch = new DefaultRecordBatch(buffer);
        assertFalse(batch.isValid());
        batch.ensureValid();
    }

    @Test(expected = InvalidRecordException.class)
    public void testInvalidRecordCountTooManyNonCompressedV2() {
        long now = System.currentTimeMillis();
        DefaultRecordBatch batch = recordsWithInvalidRecordCount(RecordBatch.MAGIC_VALUE_V2, now, CompressionType.NONE, 5);
        // force iteration through the batch to execute validation
        // batch validation is a part of normal workflow for LogValidator.validateMessagesAndAssignOffsets
        for (Record record: batch) {
            record.isValid();
        }
    }

    @Test(expected = InvalidRecordException.class)
    public void testInvalidRecordCountTooLittleNonCompressedV2() {
        long now = System.currentTimeMillis();
        DefaultRecordBatch batch = recordsWithInvalidRecordCount(RecordBatch.MAGIC_VALUE_V2, now, CompressionType.NONE, 2);
        // force iteration through the batch to execute validation
        // batch validation is a part of normal workflow for LogValidator.validateMessagesAndAssignOffsets
        for (Record record: batch) {
            record.isValid();
        }
    }

    @Test(expected = InvalidRecordException.class)
    public void testInvalidRecordCountTooManyCompressedV2() {
        long now = System.currentTimeMillis();
        DefaultRecordBatch batch = recordsWithInvalidRecordCount(RecordBatch.MAGIC_VALUE_V2, now, CompressionType.GZIP, 5);
        // force iteration through the batch to execute validation
        // batch validation is a part of normal workflow for LogValidator.validateMessagesAndAssignOffsets
        for (Record record: batch) {
            record.isValid();
        }
    }

    @Test(expected = InvalidRecordException.class)
    public void testInvalidRecordCountTooLittleCompressedV2() {
        long now = System.currentTimeMillis();
        DefaultRecordBatch batch = recordsWithInvalidRecordCount(RecordBatch.MAGIC_VALUE_V2, now, CompressionType.GZIP, 2);
        // force iteration through the batch to execute validation
        // batch validation is a part of normal workflow for LogValidator.validateMessagesAndAssignOffsets
        for (Record record: batch) {
            record.isValid();
        }
    }

    @Test(expected = InvalidRecordException.class)
    public void testInvalidCrc() {
        MemoryRecords records = new RecordsBuilder()
                .withMagic(MAGIC_VALUE_V2)
                .addBatch(simpleRecordSet)
                .build();
        ByteBuffer buffer = records.buffer();
        buffer.putInt(DefaultRecordBatch.LAST_OFFSET_DELTA_OFFSET, 23);

        DefaultRecordBatch batch = new DefaultRecordBatch(buffer);
        assertFalse(batch.isValid());
        batch.ensureValid();
    }

    @Test
    public void testSetLastOffset() {
        MemoryRecords records = new RecordsBuilder()
                .withMagic(MAGIC_VALUE_V2)
                .addBatch(simpleRecordSet)
                .build();

        long lastOffset = 500L;
        long firstOffset = lastOffset - simpleRecordSet.length + 1;

        DefaultRecordBatch batch = new DefaultRecordBatch(records.buffer());
        batch.setLastOffset(lastOffset);
        assertEquals(lastOffset, batch.lastOffset());
        assertEquals(firstOffset, batch.baseOffset());
        assertTrue(batch.isValid());

        List<MutableRecordBatch> recordBatches = Utils.toList(records.batches().iterator());
        assertEquals(1, recordBatches.size());
        assertEquals(lastOffset, recordBatches.get(0).lastOffset());

        long offset = firstOffset;
        for (Record record : records.records())
            assertEquals(offset++, record.offset());
    }

    @Test
    public void testSetPartitionLeaderEpoch() {
        MemoryRecords records = new RecordsBuilder()
                .withMagic(MAGIC_VALUE_V2)
                .addBatch(simpleRecordSet)
                .build();
        int leaderEpoch = 500;

        DefaultRecordBatch batch = new DefaultRecordBatch(records.buffer());
        batch.setPartitionLeaderEpoch(leaderEpoch);
        assertEquals(leaderEpoch, batch.partitionLeaderEpoch());
        assertTrue(batch.isValid());

        List<MutableRecordBatch> recordBatches = Utils.toList(records.batches().iterator());
        assertEquals(1, recordBatches.size());
        assertEquals(leaderEpoch, recordBatches.get(0).partitionLeaderEpoch());
    }

    @Test
    public void testSetLogAppendTime() {
        MemoryRecords records = new RecordsBuilder()
                .withMagic(MAGIC_VALUE_V2)
                .addBatch(simpleRecordSet)
                .build();

        long logAppendTime = 15L;

        DefaultRecordBatch batch = new DefaultRecordBatch(records.buffer());
        batch.setMaxTimestamp(TimestampType.LOG_APPEND_TIME, logAppendTime);
        assertEquals(TimestampType.LOG_APPEND_TIME, batch.timestampType());
        assertEquals(logAppendTime, batch.maxTimestamp());
        assertTrue(batch.isValid());

        List<MutableRecordBatch> recordBatches = Utils.toList(records.batches().iterator());
        assertEquals(1, recordBatches.size());
        assertEquals(logAppendTime, recordBatches.get(0).maxTimestamp());
        assertEquals(TimestampType.LOG_APPEND_TIME, recordBatches.get(0).timestampType());

        for (Record record : records.records())
            assertEquals(logAppendTime, record.timestamp());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetNoTimestampTypeNotAllowed() {
        MemoryRecords records = new RecordsBuilder()
                .withMagic(MAGIC_VALUE_V2)
                .addBatch(simpleRecordSet)
                .build();
        DefaultRecordBatch batch = new DefaultRecordBatch(records.buffer());
        batch.setMaxTimestamp(TimestampType.NO_TIMESTAMP_TYPE, RecordBatch.NO_TIMESTAMP);
    }

    @Test
    public void testReadAndWriteControlBatch() {
        long producerId = 1L;
        short producerEpoch = 0;
        int coordinatorEpoch = 15;

        EndTransactionMarker marker = new EndTransactionMarker(ControlRecordType.COMMIT, coordinatorEpoch);
        RecordsBuilder builder = new RecordsBuilder()
                .withProducerMetadata(producerId, producerEpoch)
                .setTransactional(true)
                .addControlBatch(System.currentTimeMillis(), marker);
        MemoryRecords records = builder.build();

        List<MutableRecordBatch> batches = TestUtils.toList(records.batches());
        assertEquals(1, batches.size());

        MutableRecordBatch batch = batches.get(0);
        assertTrue(batch.isControlBatch());

        List<Record> logRecords = TestUtils.toList(records.records());
        assertEquals(1, logRecords.size());

        Record commitRecord = logRecords.get(0);
        assertEquals(marker, EndTransactionMarker.deserialize(commitRecord));
    }

    @Test
    public void testStreamingIteratorConsistency() {
        MemoryRecords records = new RecordsBuilder()
                .withMagic(MAGIC_VALUE_V2)
                .withCompression(CompressionType.GZIP)
                .addBatch(simpleRecordSet)
                .build();
        DefaultRecordBatch batch = new DefaultRecordBatch(records.buffer());
        try (CloseableIterator<Record> streamingIterator = batch.streamingIterator(BufferSupplier.create())) {
            TestUtils.checkEquals(streamingIterator, batch.iterator());
        }
    }

    @Test
    public void testIncrementSequence() {
        assertEquals(10, DefaultRecordBatch.incrementSequence(5, 5));
        assertEquals(0, DefaultRecordBatch.incrementSequence(Integer.MAX_VALUE, 1));
        assertEquals(4, DefaultRecordBatch.incrementSequence(Integer.MAX_VALUE - 5, 10));
    }

    private static DefaultRecordBatch recordsWithInvalidRecordCount(Byte magicValue, long timestamp,
                                                                    CompressionType codec, int invalidCount) {
        RecordsBuilder builder = new RecordsBuilder().withMagic(magicValue).withCompression(codec);
        RecordsBuilder.BatchBuilder batchBuilder = builder.newBatch();
        batchBuilder.append(new SimpleRecord(timestamp, null, "hello".getBytes()));
        batchBuilder.append(new SimpleRecord(timestamp, null, "there".getBytes()));
        batchBuilder.append(new SimpleRecord(timestamp, null, "beautiful".getBytes()));
        batchBuilder.closeBatch();
        MemoryRecords records = builder.build();
        ByteBuffer buffer = records.buffer();
        buffer.position(0);
        buffer.putInt(RECORDS_COUNT_OFFSET, invalidCount);
        buffer.position(0);
        return new DefaultRecordBatch(buffer);
    }
}
