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

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(value = Parameterized.class)
public class RecordBatchWriterTest {

    private final CompressionType compressionType;
    private final int bufferOffset;
    private final Time time;

    public RecordBatchWriterTest(int bufferOffset, CompressionType compressionType) {
        this.bufferOffset = bufferOffset;
        this.compressionType = compressionType;
        this.time = Time.SYSTEM;
    }

    @Test
    public void testWriteEmptyRecordSet() {
        ByteBufferOutputStream output = initOutputStream(128);
        RecordBatchWriter writer = new RecordBatchWriter(output, RecordBatch.MAGIC_VALUE_V0, compressionType,
                TimestampType.CREATE_TIME, 0L, 0L, false);
        writer.close();
        assertEquals(bufferOffset, output.position());
    }

    @Test
    public void testWriteTransactionalRecordSet() {
        ByteBufferOutputStream out = initOutputStream(128);

        long pid = 9809;
        short epoch = 15;
        int sequence = 2342;

        RecordBatchWriter writer = new RecordBatchWriter(out, RecordBatch.CURRENT_MAGIC_VALUE, compressionType,
                TimestampType.CREATE_TIME, 0L, 0L, false);
        writer.setTransactional(true);
        writer.setProducerIdAndEpoch(pid, epoch);
        writer.setProducerBaseSequence(sequence);
        writer.append(System.currentTimeMillis(), "foo".getBytes(), "bar".getBytes());
        writer.close();

        MemoryRecords records = writer.toRecords();
        List<MutableRecordBatch> batches = Utils.toList(records.batches().iterator());
        assertEquals(1, batches.size());
        assertTrue(batches.get(0).isTransactional());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWriteTransactionalNotAllowedMagicV0() {
        ByteBufferOutputStream out = initOutputStream(128);
        RecordBatchWriter writer = new RecordBatchWriter(out, RecordBatch.MAGIC_VALUE_V0, compressionType,
                TimestampType.CREATE_TIME, 0L, 0L, false);
        writer.setTransactional(true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWriteTransactionalNotAllowedMagicV1() {
        ByteBufferOutputStream out = initOutputStream(128);
        RecordBatchWriter writer = new RecordBatchWriter(out, RecordBatch.MAGIC_VALUE_V1, compressionType,
                TimestampType.CREATE_TIME, 0L, 0L, false);
        writer.setTransactional(true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWriteControlBatchNotAllowedMagicV0() {
        ByteBufferOutputStream out = initOutputStream(128);
        new RecordBatchWriter(out, RecordBatch.MAGIC_VALUE_V0, compressionType,
                TimestampType.CREATE_TIME, 0L, 0L, true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWriteControlBatchNotAllowedMagicV1() {
        ByteBufferOutputStream out = initOutputStream(128);
        new RecordBatchWriter(out, RecordBatch.MAGIC_VALUE_V1, compressionType,
                TimestampType.CREATE_TIME, 0L, 0L, true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWriteTransactionalWithInvalidProducerId() {
        ByteBufferOutputStream out = initOutputStream(128);

        RecordBatchWriter writer = new RecordBatchWriter(out, RecordBatch.CURRENT_MAGIC_VALUE, compressionType,
                TimestampType.CREATE_TIME, 0L, 0L, false);
        writer.setProducerIdAndEpoch(RecordBatch.NO_PRODUCER_ID, (short) 15);
        writer.setProducerBaseSequence(25);
        writer.setTransactional(true);
        writer.close();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWriteTransactionalWithInvalidEpoch() {
        ByteBufferOutputStream out = initOutputStream(128);

        RecordBatchWriter writer = new RecordBatchWriter(out, RecordBatch.CURRENT_MAGIC_VALUE, compressionType,
                TimestampType.CREATE_TIME, 0L, 0L, false);
        writer.setProducerIdAndEpoch(9809, RecordBatch.NO_PRODUCER_EPOCH);
        writer.setProducerBaseSequence(25);
        writer.setTransactional(true);
        writer.close();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWriteTransactionalWithInvalidBaseSequence() {
        ByteBufferOutputStream out = initOutputStream(128);

        RecordBatchWriter writer = new RecordBatchWriter(out, RecordBatch.CURRENT_MAGIC_VALUE, compressionType,
                TimestampType.CREATE_TIME, 0L, 0L, false);
        writer.setProducerIdAndEpoch(9809, (short) 15);
        writer.setProducerBaseSequence(RecordBatch.NO_SEQUENCE);
        writer.setTransactional(true);
        writer.close();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWriteEndTxnMarkerNonTransactionalBatch() {
        ByteBufferOutputStream out = initOutputStream(128);

        RecordBatchWriter writer = new RecordBatchWriter(out, RecordBatch.CURRENT_MAGIC_VALUE, compressionType,
                TimestampType.CREATE_TIME, 0L, 0L, true);
        writer.setProducerIdAndEpoch(9809, (short) 15);
        writer.setProducerBaseSequence(RecordBatch.NO_SEQUENCE);
        writer.setTransactional(false);
        writer.appendEndTxnMarker(RecordBatch.NO_TIMESTAMP, new EndTransactionMarker(ControlRecordType.ABORT, 0));
        writer.close();

        ByteBuffer buffer = ByteBuffer.allocate(128);
        buffer.position(bufferOffset);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWriteEndTxnMarkerNonControlBatch() {
        ByteBufferOutputStream out = initOutputStream(128);

        RecordBatchWriter writer = new RecordBatchWriter(out, RecordBatch.CURRENT_MAGIC_VALUE, compressionType,
                TimestampType.CREATE_TIME, 0L, 0L, false);
        writer.setProducerIdAndEpoch(9809, (short) 15);
        writer.setProducerBaseSequence(RecordBatch.NO_SEQUENCE);
        writer.setTransactional(false);
        writer.appendEndTxnMarker(RecordBatch.NO_TIMESTAMP, new EndTransactionMarker(ControlRecordType.ABORT, 0));
    }

    @Test
    public void testCompressionRateV0() {
        ByteBufferOutputStream out = initOutputStream(1024);

        LegacyRecord[] records = new LegacyRecord[] {
                LegacyRecord.create(RecordBatch.MAGIC_VALUE_V0, 0L, "a".getBytes(), "1".getBytes()),
                LegacyRecord.create(RecordBatch.MAGIC_VALUE_V0, 1L, "b".getBytes(), "2".getBytes()),
                LegacyRecord.create(RecordBatch.MAGIC_VALUE_V0, 2L, "c".getBytes(), "3".getBytes()),
        };

        RecordBatchWriter writer = new RecordBatchWriter(out, RecordBatch.MAGIC_VALUE_V0, compressionType,
                TimestampType.CREATE_TIME, 0L, 0L, false);

        int uncompressedSize = 0;
        for (LegacyRecord record : records) {
            uncompressedSize += record.sizeInBytes() + Records.LOG_OVERHEAD;
            writer.append(record);
        }

        writer.close();

        MemoryRecords built = writer.toRecords();
        if (compressionType == CompressionType.NONE) {
            assertEquals(1.0, writer.compressionRatio(), 0.00001);
        } else {
            int compressedSize = built.sizeInBytes() - Records.LOG_OVERHEAD - LegacyRecord.RECORD_OVERHEAD_V0;
            double computedCompressionRate = (double) compressedSize / uncompressedSize;
            assertEquals(computedCompressionRate, writer.compressionRatio(), 0.00001);
        }
    }

    @Test
    public void testEstimatedSizeInBytes() {
        ByteBufferOutputStream out = initOutputStream(1024);

        RecordBatchWriter writer = new RecordBatchWriter(out, RecordBatch.CURRENT_MAGIC_VALUE, compressionType,
                TimestampType.CREATE_TIME, 0L, 0L, false);

        int previousEstimate = 0;
        for (int i = 0; i < 10; i++) {
            writer.append(new SimpleRecord(i, ("" + i).getBytes()));
            int currentEstimate = writer.estimatedSizeInBytes();
            assertTrue(currentEstimate > previousEstimate);
            previousEstimate = currentEstimate;
        }

        int bytesWrittenBeforeClose = writer.estimatedSizeInBytes();
        writer.close();
        MemoryRecords records = writer.toRecords();
        assertEquals(records.sizeInBytes(), writer.estimatedSizeInBytes());
        if (compressionType == CompressionType.NONE)
            assertEquals(records.sizeInBytes(), bytesWrittenBeforeClose);
    }

    @Test
    public void testCompressionRateV1() {
        ByteBufferOutputStream out = initOutputStream(1024);

        LegacyRecord[] records = new LegacyRecord[] {
                LegacyRecord.create(RecordBatch.MAGIC_VALUE_V1, 0L, "a".getBytes(), "1".getBytes()),
                LegacyRecord.create(RecordBatch.MAGIC_VALUE_V1, 1L, "b".getBytes(), "2".getBytes()),
                LegacyRecord.create(RecordBatch.MAGIC_VALUE_V1, 2L, "c".getBytes(), "3".getBytes()),
        };

        RecordBatchWriter writer = new RecordBatchWriter(out, RecordBatch.MAGIC_VALUE_V1, compressionType,
                TimestampType.CREATE_TIME, 0L, 0L, false);

        int uncompressedSize = 0;
        for (LegacyRecord record : records) {
            uncompressedSize += record.sizeInBytes() + Records.LOG_OVERHEAD;
            writer.append(record);
        }

        writer.close();
        MemoryRecords built = writer.toRecords();
        if (compressionType == CompressionType.NONE) {
            assertEquals(1.0, writer.compressionRatio(), 0.00001);
        } else {
            int compressedSize = built.sizeInBytes() - Records.LOG_OVERHEAD - LegacyRecord.RECORD_OVERHEAD_V1;
            double computedCompressionRate = (double) compressedSize / uncompressedSize;
            assertEquals(computedCompressionRate, writer.compressionRatio(), 0.00001);
        }
    }

    @Test
    public void buildUsingLogAppendTime() {
        ByteBufferOutputStream out = initOutputStream(1024);

        long logAppendTime = System.currentTimeMillis();
        RecordBatchWriter writer = new RecordBatchWriter(out, RecordBatch.MAGIC_VALUE_V1, compressionType,
                TimestampType.LOG_APPEND_TIME, 0L, logAppendTime, false);
        writer.append(0L, "a".getBytes(), "1".getBytes());
        writer.append(0L, "b".getBytes(), "2".getBytes());
        writer.append(0L, "c".getBytes(), "3".getBytes());

        writer.close();
        MemoryRecords records = writer.toRecords();

        RecordBatchWriter.RecordsInfo info = writer.info();
        assertEquals(logAppendTime, info.maxTimestamp);

        if (compressionType != CompressionType.NONE)
            assertEquals(2L, info.shallowOffsetOfMaxTimestamp);
        else
            assertEquals(0L, info.shallowOffsetOfMaxTimestamp);

        for (RecordBatch batch : records.batches()) {
            assertEquals(TimestampType.LOG_APPEND_TIME, batch.timestampType());
            for (Record record : batch)
                assertEquals(logAppendTime, record.timestamp());
        }
    }

    @Test
    public void buildUsingCreateTime() {
        ByteBufferOutputStream out = initOutputStream(1024);

        RecordBatchWriter writer = new RecordBatchWriter(out, RecordBatch.MAGIC_VALUE_V1, compressionType,
                TimestampType.CREATE_TIME, 0L, RecordBatch.NO_TIMESTAMP, false);
        writer.append(0L, "a".getBytes(), "1".getBytes());
        writer.append(2L, "b".getBytes(), "2".getBytes());
        writer.append(1L, "c".getBytes(), "3".getBytes());
        writer.close();
        MemoryRecords records = writer.toRecords();

        RecordBatchWriter.RecordsInfo info = writer.info();
        assertEquals(2L, info.maxTimestamp);

        if (compressionType == CompressionType.NONE)
            assertEquals(1L, info.shallowOffsetOfMaxTimestamp);
        else
            assertEquals(2L, info.shallowOffsetOfMaxTimestamp);

        int i = 0;
        long[] expectedTimestamps = new long[] {0L, 2L, 1L};
        for (RecordBatch batch : records.batches()) {
            assertEquals(TimestampType.CREATE_TIME, batch.timestampType());
            for (Record record : batch)
                assertEquals(expectedTimestamps[i++], record.timestamp());
        }
    }

    @Test
    public void testAppendedChecksumConsistency() {
        for (byte magic : Arrays.asList(RecordBatch.MAGIC_VALUE_V0, RecordBatch.MAGIC_VALUE_V1, RecordBatch.MAGIC_VALUE_V2)) {
            ByteBufferOutputStream out = initOutputStream(512);
            RecordBatchWriter writer = new RecordBatchWriter(out, magic, compressionType,
                    TimestampType.CREATE_TIME, 0L, LegacyRecord.NO_TIMESTAMP, false);
            Long checksumOrNull = writer.append(1L, "key".getBytes(), "value".getBytes());
            writer.close();
            MemoryRecords memoryRecords = writer.toRecords();
            List<Record> records = TestUtils.toList(memoryRecords.records());
            assertEquals(1, records.size());
            assertEquals(checksumOrNull, records.get(0).checksumOrNull());
        }
    }

    @Test
    public void testSmallWriteLimit() {
        // with a small write limit, we always allow at least one record to be added

        byte[] key = "foo".getBytes();
        byte[] value = "bar".getBytes();
        ByteBufferOutputStream out = initOutputStream(4);
        RecordBatchWriter writer = new RecordBatchWriter(out, RecordBatch.CURRENT_MAGIC_VALUE, compressionType,
                TimestampType.CREATE_TIME, 0L, LegacyRecord.NO_TIMESTAMP, false);

        assertFalse(writer.isFull());
        assertTrue(writer.hasRoomFor(0L, key, value, Record.EMPTY_HEADERS));
        writer.append(0L, key, value);

        assertTrue(writer.isFull());
        assertFalse(writer.hasRoomFor(0L, key, value, Record.EMPTY_HEADERS));

        writer.close();
        MemoryRecords memRecords = writer.toRecords();
        List<Record> records = TestUtils.toList(memRecords.records());
        assertEquals(1, records.size());

        Record record = records.get(0);
        assertEquals(ByteBuffer.wrap(key), record.key());
        assertEquals(ByteBuffer.wrap(value), record.value());
    }

    @Test
    public void writePastLimit() {
        ByteBufferOutputStream out = initOutputStream(64);

        long logAppendTime = System.currentTimeMillis();
        RecordBatchWriter writer = new RecordBatchWriter(out, RecordBatch.MAGIC_VALUE_V1, compressionType,
                TimestampType.CREATE_TIME, 0L, logAppendTime, false);
        writer.setEstimatedCompressionRatio(0.5f);
        writer.append(0L, "a".getBytes(), "1".getBytes());
        writer.append(1L, "b".getBytes(), "2".getBytes());

        assertFalse(writer.hasRoomFor(2L, "c".getBytes(), "3".getBytes(), Record.EMPTY_HEADERS));
        writer.append(2L, "c".getBytes(), "3".getBytes());
        writer.close();
        MemoryRecords records = writer.toRecords();

        RecordBatchWriter.RecordsInfo info = writer.info();
        assertEquals(2L, info.maxTimestamp);
        assertEquals(2L, info.shallowOffsetOfMaxTimestamp);

        long i = 0L;
        for (RecordBatch batch : records.batches()) {
            assertEquals(TimestampType.CREATE_TIME, batch.timestampType());
            for (Record record : batch)
                assertEquals(i++, record.timestamp());
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAppendAtInvalidOffset() {
        ByteBufferOutputStream out = initOutputStream(128);

        RecordBatchWriter writer = new RecordBatchWriter(out, RecordBatch.MAGIC_VALUE_V1, compressionType,
                TimestampType.CREATE_TIME, 0L, RecordBatch.NO_TIMESTAMP, false);

        writer.appendWithOffset(0L, System.currentTimeMillis(), "a".getBytes(), null);

        // offsets must increase monotonically
        writer.appendWithOffset(0L, System.currentTimeMillis(), "b".getBytes(), null);
    }

    @Test
    public void convertV2ToV1UsingMixedCreateAndLogAppendTime() {
        RecordsBuilder builder = new RecordsBuilder()
                .withMagic(RecordBatch.MAGIC_VALUE_V2)
                .withCompression(compressionType);

        builder.newBatch()
                .withTimestampType(TimestampType.LOG_APPEND_TIME)
                .append(new SimpleRecord(10L, "1".getBytes(), "a".getBytes()))
                .closeBatch();

        int sizeExcludingTxnMarkers = builder.bufferPosition();

        builder.newControlBatch()
                .withProducerMetadata(15L, (short) 0)
                .setTxnMarker(System.currentTimeMillis(), new EndTransactionMarker(ControlRecordType.ABORT, 0))
                .closeBatch();

        int position = builder.bufferPosition();

        builder.newBatch()
                .withTimestampType(TimestampType.CREATE_TIME)
                .append(new SimpleRecord(12L, "2".getBytes(), "b".getBytes()))
                .append(new SimpleRecord(13L, "3".getBytes(), "c".getBytes()))
                .closeBatch();

        sizeExcludingTxnMarkers += builder.bufferPosition() - position;

        builder.newControlBatchFromOffset(14L)
                .withProducerMetadata(1L, (short) 0)
                .setTxnMarker(System.currentTimeMillis(), new EndTransactionMarker(ControlRecordType.COMMIT, 0))
                .closeBatch();

        ConvertedRecords<MemoryRecords> convertedRecords = builder.build()
                .downConvert(RecordBatch.MAGIC_VALUE_V1, 0, time);
        MemoryRecords records = convertedRecords.records();

        // Transactional markers are skipped when down converting to V1, so exclude them from size
        verifyRecordsProcessingStats(convertedRecords.recordsProcessingStats(),
                3, 3, records.sizeInBytes(), sizeExcludingTxnMarkers);

        List<? extends RecordBatch> batches = Utils.toList(records.batches().iterator());
        if (compressionType != CompressionType.NONE) {
            assertEquals(2, batches.size());
            assertEquals(TimestampType.LOG_APPEND_TIME, batches.get(0).timestampType());
            assertEquals(TimestampType.CREATE_TIME, batches.get(1).timestampType());
        } else {
            assertEquals(3, batches.size());
            assertEquals(TimestampType.LOG_APPEND_TIME, batches.get(0).timestampType());
            assertEquals(TimestampType.CREATE_TIME, batches.get(1).timestampType());
            assertEquals(TimestampType.CREATE_TIME, batches.get(2).timestampType());
        }

        List<Record> logRecords = Utils.toList(records.records().iterator());
        assertEquals(3, logRecords.size());
        assertEquals(ByteBuffer.wrap("1".getBytes()), logRecords.get(0).key());
        assertEquals(ByteBuffer.wrap("2".getBytes()), logRecords.get(1).key());
        assertEquals(ByteBuffer.wrap("3".getBytes()), logRecords.get(2).key());
    }

    @Test
    public void convertToV1WithMixedV0AndV2Data() {
        RecordsBuilder builder = new RecordsBuilder().withCompression(compressionType);

        builder.newBatch()
                .withMagic(RecordBatch.MAGIC_VALUE_V0)
                .append(new SimpleRecord(RecordBatch.NO_TIMESTAMP, "1".getBytes(), "a".getBytes()))
                .closeBatch();

        builder.newBatch().withMagic(RecordBatch.MAGIC_VALUE_V2)
                .append(new SimpleRecord(11L, "2".getBytes(), "b".getBytes()))
                .append(new SimpleRecord(12L, "3".getBytes(), "c".getBytes()))
                .closeBatch();

        MemoryRecords initialRecords = builder.build();
        ConvertedRecords<MemoryRecords> convertedRecords = initialRecords
                .downConvert(RecordBatch.MAGIC_VALUE_V1, 0, time);
        MemoryRecords records = convertedRecords.records();
        verifyRecordsProcessingStats(convertedRecords.recordsProcessingStats(), 3, 2,
                records.sizeInBytes(), initialRecords.sizeInBytes());

        List<? extends RecordBatch> batches = Utils.toList(records.batches().iterator());
        if (compressionType != CompressionType.NONE) {
            assertEquals(2, batches.size());
            assertEquals(RecordBatch.MAGIC_VALUE_V0, batches.get(0).magic());
            assertEquals(0, batches.get(0).baseOffset());
            assertEquals(RecordBatch.MAGIC_VALUE_V1, batches.get(1).magic());
            assertEquals(1, batches.get(1).baseOffset());
        } else {
            assertEquals(3, batches.size());
            assertEquals(RecordBatch.MAGIC_VALUE_V0, batches.get(0).magic());
            assertEquals(0, batches.get(0).baseOffset());
            assertEquals(RecordBatch.MAGIC_VALUE_V1, batches.get(1).magic());
            assertEquals(1, batches.get(1).baseOffset());
            assertEquals(RecordBatch.MAGIC_VALUE_V1, batches.get(2).magic());
            assertEquals(2, batches.get(2).baseOffset());
        }

        List<Record> logRecords = Utils.toList(records.records().iterator());
        assertEquals("1", utf8(logRecords.get(0).key()));
        assertEquals("2", utf8(logRecords.get(1).key()));
        assertEquals("3", utf8(logRecords.get(2).key()));

        convertedRecords = initialRecords.downConvert(RecordBatch.MAGIC_VALUE_V1, 2L, time);
        records = convertedRecords.records();

        batches = Utils.toList(records.batches().iterator());
        logRecords = Utils.toList(records.records().iterator());

        if (compressionType != CompressionType.NONE) {
            assertEquals(2, batches.size());
            assertEquals(RecordBatch.MAGIC_VALUE_V0, batches.get(0).magic());
            assertEquals(0, batches.get(0).baseOffset());
            assertEquals(RecordBatch.MAGIC_VALUE_V1, batches.get(1).magic());
            assertEquals(1, batches.get(1).baseOffset());
            assertEquals("1", utf8(logRecords.get(0).key()));
            assertEquals("2", utf8(logRecords.get(1).key()));
            assertEquals("3", utf8(logRecords.get(2).key()));
            verifyRecordsProcessingStats(convertedRecords.recordsProcessingStats(), 3, 2,
                    records.sizeInBytes(), initialRecords.sizeInBytes());
        } else {
            assertEquals(2, batches.size());
            assertEquals(RecordBatch.MAGIC_VALUE_V0, batches.get(0).magic());
            assertEquals(0, batches.get(0).baseOffset());
            assertEquals(RecordBatch.MAGIC_VALUE_V1, batches.get(1).magic());
            assertEquals(2, batches.get(1).baseOffset());
            assertEquals("1", utf8(logRecords.get(0).key()));
            assertEquals("3", utf8(logRecords.get(1).key()));
            verifyRecordsProcessingStats(convertedRecords.recordsProcessingStats(), 3, 1,
                    records.sizeInBytes(), initialRecords.sizeInBytes());
        }
    }

    @Test
    public void testHasRoomFor() {
        testHasRoomFor(RecordBatch.MAGIC_VALUE_V0);
        testHasRoomFor(RecordBatch.MAGIC_VALUE_V1);
        testHasRoomFor(RecordBatch.MAGIC_VALUE_V2);
    }

    public void testHasRoomFor(byte magic) {
        ByteBufferOutputStream out = initOutputStream(128);
        RecordBatchWriter writer = new RecordBatchWriter(out, magic, compressionType,
                TimestampType.CREATE_TIME, 0L, RecordBatch.NO_TIMESTAMP, false);
        writer.append(0L, "a".getBytes(), "1".getBytes());
        assertTrue(writer.hasRoomFor(1L, "b".getBytes(), "2".getBytes(), Record.EMPTY_HEADERS));
        writer.close();
        assertFalse(writer.hasRoomFor(1L, "b".getBytes(), "2".getBytes(), Record.EMPTY_HEADERS));
    }

    @Test
    public void testHasRoomForMethodWithHeaders() {
        ByteBufferOutputStream out = initOutputStream(128);
        RecordBatchWriter writer = new RecordBatchWriter(out, RecordBatch.CURRENT_MAGIC_VALUE, compressionType,
                TimestampType.CREATE_TIME, 0L, RecordBatch.NO_TIMESTAMP, false);
        long logAppendTime = System.currentTimeMillis();
        RecordHeaders headers = new RecordHeaders();
        headers.add("hello", "world.world".getBytes());
        headers.add("hello", "world.world".getBytes());
        headers.add("hello", "world.world".getBytes());
        headers.add("hello", "world.world".getBytes());
        headers.add("hello", "world.world".getBytes());
        writer.append(logAppendTime, "key".getBytes(), "value".getBytes());
        // Make sure that hasRoomFor accounts for header sizes by letting a record without headers pass, but stopping
        // a record with a large number of headers.
        assertTrue(writer.hasRoomFor(logAppendTime, "key".getBytes(), "value".getBytes(), Record.EMPTY_HEADERS));
        assertFalse(writer.hasRoomFor(logAppendTime, "key".getBytes(), "value".getBytes(), headers.toArray()));
    }

    private String utf8(ByteBuffer buffer) {
        return Utils.utf8(buffer, buffer.remaining());
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowIllegalStateExceptionOnCloseWhenAborted() throws Exception {
        ByteBufferOutputStream out = initOutputStream(128);
        RecordBatchWriter writer = new RecordBatchWriter(out, RecordBatch.MAGIC_VALUE_V0, compressionType,
                TimestampType.CREATE_TIME, 0L, 0L, false);
        writer.abort();
        writer.close();
    }

    @Test
    public void shouldResetBufferToInitialPositionOnAbort() throws Exception {
        ByteBufferOutputStream out = initOutputStream(128);
        RecordBatchWriter writer = new RecordBatchWriter(out, RecordBatch.MAGIC_VALUE_V0, compressionType,
                TimestampType.CREATE_TIME, 0L, 0L, false);
        writer.append(0L, "a".getBytes(), "1".getBytes());
        writer.abort();
        assertEquals(bufferOffset, out.buffer().position());
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowIllegalStateExceptionOnAppendWhenAborted() throws Exception {
        ByteBufferOutputStream out = initOutputStream(128);
        RecordBatchWriter writer = new RecordBatchWriter(out, RecordBatch.MAGIC_VALUE_V0, compressionType,
                TimestampType.CREATE_TIME, 0L, 0L, false);
        writer.abort();
        writer.append(0L, "a".getBytes(), "1".getBytes());
    }

    @Parameterized.Parameters(name = "bufferOffset={0}, compression={1}")
    public static Collection<Object[]> data() {
        List<Object[]> values = new ArrayList<>();
        for (int bufferOffset : Arrays.asList(0, 15))
            for (CompressionType compressionType : CompressionType.values())
                values.add(new Object[] {bufferOffset, compressionType});
        return values;
    }

    private void verifyRecordsProcessingStats(RecordsProcessingStats processingStats, int numRecords,
            int numRecordsConverted, long finalBytes, long preConvertedBytes) {
        assertNotNull("Records processing info is null", processingStats);
        assertEquals(numRecordsConverted, processingStats.numRecordsConverted());
        // Since nanoTime accuracy on build machines may not be sufficient to measure small conversion times,
        // only check if the value >= 0. Default is -1, so this checks if time has been recorded.
        assertTrue("Processing time not recorded: " + processingStats, processingStats.conversionTimeNanos() >= 0);
        long tempBytes = processingStats.temporaryMemoryBytes();
        if (compressionType == CompressionType.NONE) {
            if (numRecordsConverted == 0)
                assertEquals(finalBytes, tempBytes);
            else if (numRecordsConverted == numRecords)
                assertEquals(preConvertedBytes + finalBytes, tempBytes);
            else {
                assertTrue(String.format("Unexpected temp bytes %d final %d pre %d", tempBytes, finalBytes, preConvertedBytes),
                        tempBytes > finalBytes && tempBytes < finalBytes + preConvertedBytes);
            }
        } else {
            long compressedBytes = finalBytes - Records.LOG_OVERHEAD - LegacyRecord.RECORD_OVERHEAD_V0;
            assertTrue(String.format("Uncompressed size expected temp=%d, compressed=%d", tempBytes, compressedBytes),
                    tempBytes > compressedBytes);
        }
    }

    private ByteBufferOutputStream initOutputStream(int size) {
        ByteBufferOutputStream out = new ByteBufferOutputStream(size);
        out.position(bufferOffset);
        return out;
    }

}
