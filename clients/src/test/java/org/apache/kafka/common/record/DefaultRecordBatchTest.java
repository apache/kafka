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

import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.compress.ZstdFactory;
import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.ChunkedBytesStream;
import org.apache.kafka.common.utils.CloseableIterator;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

import static org.apache.kafka.common.record.DefaultRecordBatch.RECORDS_COUNT_OFFSET;
import static org.apache.kafka.common.record.DefaultRecordBatch.RECORDS_OFFSET;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyByte;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DefaultRecordBatchTest {
    // We avoid SecureRandom.getInstanceStrong() here because it reads from /dev/random and blocks on Linux. Since these
    // tests don't require cryptographically strong random data, we avoid a CSPRNG (SecureRandom) altogether.
    private static final Random RANDOM = new Random(20231025);

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
                    assertEquals(RecordBatch.NO_TIMESTAMP, batch.baseTimestamp());
                    assertEquals(isControlBatch, batch.isControlBatch());
                }
            }
        }
    }

    @Test
    public void buildDefaultRecordBatch() {
        ByteBuffer buffer = ByteBuffer.allocate(2048);

        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, RecordBatch.MAGIC_VALUE_V2, CompressionType.NONE,
                TimestampType.CREATE_TIME, 1234567L);
        builder.appendWithOffset(1234567, 1L, "a".getBytes(), "v".getBytes());
        builder.appendWithOffset(1234568, 2L, "b".getBytes(), "v".getBytes());

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

            for (Record record : batch) record.ensureValid();
        }
    }

    @Test
    public void buildDefaultRecordBatchWithProducerId() {
        long pid = 23423L;
        short epoch = 145;
        int baseSequence = 983;

        ByteBuffer buffer = ByteBuffer.allocate(2048);

        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, RecordBatch.MAGIC_VALUE_V2, CompressionType.NONE,
                TimestampType.CREATE_TIME, 1234567L, RecordBatch.NO_TIMESTAMP, pid, epoch, baseSequence);
        builder.appendWithOffset(1234567, 1L, "a".getBytes(), "v".getBytes());
        builder.appendWithOffset(1234568, 2L, "b".getBytes(), "v".getBytes());

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

            for (Record record : batch) record.ensureValid();
        }
    }

    @Test
    public void buildDefaultRecordBatchWithSequenceWrapAround() {
        long pid = 23423L;
        short epoch = 145;
        int baseSequence = Integer.MAX_VALUE - 1;
        ByteBuffer buffer = ByteBuffer.allocate(2048);

        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, RecordBatch.MAGIC_VALUE_V2, CompressionType.NONE,
                TimestampType.CREATE_TIME, 1234567L, RecordBatch.NO_TIMESTAMP, pid, epoch, baseSequence);
        builder.appendWithOffset(1234567, 1L, "a".getBytes(), "v".getBytes());
        builder.appendWithOffset(1234568, 2L, "b".getBytes(), "v".getBytes());
        builder.appendWithOffset(1234569, 3L, "c".getBytes(), "v".getBytes());

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
            new RecordHeader("bar", null)
        };

        long timestamp = System.currentTimeMillis();
        SimpleRecord[] records = new SimpleRecord[] {
            new SimpleRecord(timestamp, "key".getBytes(), "value".getBytes()),
            new SimpleRecord(timestamp + 30000, null, "value".getBytes()),
            new SimpleRecord(timestamp + 60000, "key".getBytes(), null),
            new SimpleRecord(timestamp + 60000, "key".getBytes(), "value".getBytes(), headers)
        };
        int actualSize = MemoryRecords.withRecords(CompressionType.NONE, records).sizeInBytes();
        assertEquals(actualSize, DefaultRecordBatch.sizeInBytes(Arrays.asList(records)));
    }

    @Test
    public void testInvalidRecordSize() {
        MemoryRecords records = MemoryRecords.withRecords(RecordBatch.MAGIC_VALUE_V2, 0L,
                CompressionType.NONE, TimestampType.CREATE_TIME,
                new SimpleRecord(1L, "a".getBytes(), "1".getBytes()),
                new SimpleRecord(2L, "b".getBytes(), "2".getBytes()),
                new SimpleRecord(3L, "c".getBytes(), "3".getBytes()));

        ByteBuffer buffer = records.buffer();
        buffer.putInt(DefaultRecordBatch.LENGTH_OFFSET, 10);

        DefaultRecordBatch batch = new DefaultRecordBatch(buffer);
        assertFalse(batch.isValid());
        assertThrows(CorruptRecordException.class, batch::ensureValid);
    }

    @Test
    public void testInvalidRecordCountTooManyNonCompressedV2() {
        long now = System.currentTimeMillis();
        DefaultRecordBatch batch = recordsWithInvalidRecordCount(RecordBatch.MAGIC_VALUE_V2, now, CompressionType.NONE, 5);
        // force iteration through the batch to execute validation
        // batch validation is a part of normal workflow for LogValidator.validateMessagesAndAssignOffsets
        assertThrows(InvalidRecordException.class, () -> batch.forEach(Record::ensureValid));
    }

    @Test
    public void testInvalidRecordCountTooLittleNonCompressedV2() {
        long now = System.currentTimeMillis();
        DefaultRecordBatch batch = recordsWithInvalidRecordCount(RecordBatch.MAGIC_VALUE_V2, now, CompressionType.NONE, 2);
        // force iteration through the batch to execute validation
        // batch validation is a part of normal workflow for LogValidator.validateMessagesAndAssignOffsets
        assertThrows(InvalidRecordException.class, () -> batch.forEach(Record::ensureValid));
    }

    @Test
    public void testInvalidRecordCountTooManyCompressedV2() {
        long now = System.currentTimeMillis();
        DefaultRecordBatch batch = recordsWithInvalidRecordCount(RecordBatch.MAGIC_VALUE_V2, now, CompressionType.GZIP, 5);
        // force iteration through the batch to execute validation
        // batch validation is a part of normal workflow for LogValidator.validateMessagesAndAssignOffsets
        assertThrows(InvalidRecordException.class, () -> batch.forEach(Record::ensureValid));
    }

    @Test
    public void testInvalidRecordCountTooLittleCompressedV2() {
        long now = System.currentTimeMillis();
        DefaultRecordBatch batch = recordsWithInvalidRecordCount(RecordBatch.MAGIC_VALUE_V2, now, CompressionType.GZIP, 2);
        // force iteration through the batch to execute validation
        // batch validation is a part of normal workflow for LogValidator.validateMessagesAndAssignOffsets
        assertThrows(InvalidRecordException.class, () -> batch.forEach(Record::ensureValid));
    }

    @Test
    public void testInvalidCrc() {
        MemoryRecords records = MemoryRecords.withRecords(RecordBatch.MAGIC_VALUE_V2, 0L,
                CompressionType.NONE, TimestampType.CREATE_TIME,
                new SimpleRecord(1L, "a".getBytes(), "1".getBytes()),
                new SimpleRecord(2L, "b".getBytes(), "2".getBytes()),
                new SimpleRecord(3L, "c".getBytes(), "3".getBytes()));

        ByteBuffer buffer = records.buffer();
        buffer.putInt(DefaultRecordBatch.LAST_OFFSET_DELTA_OFFSET, 23);

        DefaultRecordBatch batch = new DefaultRecordBatch(buffer);
        assertFalse(batch.isValid());
        assertThrows(CorruptRecordException.class, batch::ensureValid);
    }

    @Test
    public void testSetLastOffset() {
        SimpleRecord[] simpleRecords = new SimpleRecord[] {
            new SimpleRecord(1L, "a".getBytes(), "1".getBytes()),
            new SimpleRecord(2L, "b".getBytes(), "2".getBytes()),
            new SimpleRecord(3L, "c".getBytes(), "3".getBytes())
        };
        MemoryRecords records = MemoryRecords.withRecords(RecordBatch.MAGIC_VALUE_V2, 0L,
                CompressionType.NONE, TimestampType.CREATE_TIME, simpleRecords);

        long lastOffset = 500L;
        long firstOffset = lastOffset - simpleRecords.length + 1;

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
        MemoryRecords records = MemoryRecords.withRecords(RecordBatch.MAGIC_VALUE_V2, 0L,
                CompressionType.NONE, TimestampType.CREATE_TIME,
                new SimpleRecord(1L, "a".getBytes(), "1".getBytes()),
                new SimpleRecord(2L, "b".getBytes(), "2".getBytes()),
                new SimpleRecord(3L, "c".getBytes(), "3".getBytes()));

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
        MemoryRecords records = MemoryRecords.withRecords(RecordBatch.MAGIC_VALUE_V2, 0L,
                CompressionType.NONE, TimestampType.CREATE_TIME,
                new SimpleRecord(1L, "a".getBytes(), "1".getBytes()),
                new SimpleRecord(2L, "b".getBytes(), "2".getBytes()),
                new SimpleRecord(3L, "c".getBytes(), "3".getBytes()));

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

    @Test
    public void testSetNoTimestampTypeNotAllowed() {
        MemoryRecords records = MemoryRecords.withRecords(RecordBatch.MAGIC_VALUE_V2, 0L,
                CompressionType.NONE, TimestampType.CREATE_TIME,
                new SimpleRecord(1L, "a".getBytes(), "1".getBytes()),
                new SimpleRecord(2L, "b".getBytes(), "2".getBytes()),
                new SimpleRecord(3L, "c".getBytes(), "3".getBytes()));
        DefaultRecordBatch batch = new DefaultRecordBatch(records.buffer());
        assertThrows(IllegalArgumentException.class, () -> batch.setMaxTimestamp(TimestampType.NO_TIMESTAMP_TYPE, RecordBatch.NO_TIMESTAMP));
    }

    @Test
    public void testReadAndWriteControlBatch() {
        long producerId = 1L;
        short producerEpoch = 0;
        int coordinatorEpoch = 15;

        ByteBuffer buffer = ByteBuffer.allocate(128);
        MemoryRecordsBuilder builder = new MemoryRecordsBuilder(buffer, RecordBatch.CURRENT_MAGIC_VALUE,
                CompressionType.NONE, TimestampType.CREATE_TIME, 0L, RecordBatch.NO_TIMESTAMP, producerId,
                producerEpoch, RecordBatch.NO_SEQUENCE, true, true, RecordBatch.NO_PARTITION_LEADER_EPOCH,
                buffer.remaining());

        EndTransactionMarker marker = new EndTransactionMarker(ControlRecordType.COMMIT, coordinatorEpoch);
        builder.appendEndTxnMarker(System.currentTimeMillis(), marker);
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

    @ParameterizedTest
    @EnumSource(value = CompressionType.class)
    public void testStreamingIteratorConsistency(CompressionType compressionType) {
        MemoryRecords records = MemoryRecords.withRecords(RecordBatch.MAGIC_VALUE_V2, 0L,
                compressionType, TimestampType.CREATE_TIME,
                new SimpleRecord(1L, "a".getBytes(), "1".getBytes()),
                new SimpleRecord(2L, "b".getBytes(), "2".getBytes()),
                new SimpleRecord(3L, "c".getBytes(), "3".getBytes()));
        DefaultRecordBatch batch = new DefaultRecordBatch(records.buffer());
        try (CloseableIterator<Record> streamingIterator = batch.streamingIterator(BufferSupplier.create())) {
            TestUtils.checkEquals(streamingIterator, batch.iterator());
        }
    }

    @ParameterizedTest
    @EnumSource(value = CompressionType.class)
    public void testSkipKeyValueIteratorCorrectness(CompressionType compressionType) {
        Header[] headers = {new RecordHeader("k1", "v1".getBytes()), new RecordHeader("k2", null)};
        byte[] largeRecordValue = new byte[200 * 1024]; // 200KB
        RANDOM.nextBytes(largeRecordValue);

        MemoryRecords records = MemoryRecords.withRecords(RecordBatch.MAGIC_VALUE_V2, 0L,
            compressionType, TimestampType.CREATE_TIME,
            // one sample with small value size
            new SimpleRecord(1L, "a".getBytes(), "1".getBytes()),
            // one sample with null value
            new SimpleRecord(2L, "b".getBytes(), null),
            // one sample with null key
            new SimpleRecord(3L, null, "3".getBytes()),
            // one sample with null key and null value
            new SimpleRecord(4L, null, (byte[]) null),
            // one sample with large value size
            new SimpleRecord(1000L, "abc".getBytes(), largeRecordValue),
            // one sample with headers, one of the header has null value
            new SimpleRecord(9999L, "abc".getBytes(), "0".getBytes(), headers)
            );

        DefaultRecordBatch batch = new DefaultRecordBatch(records.buffer());

        try (BufferSupplier bufferSupplier = BufferSupplier.create();
             CloseableIterator<Record> skipKeyValueIterator = batch.skipKeyValueIterator(bufferSupplier)) {

            if (CompressionType.NONE == compressionType) {
                // assert that for uncompressed data stream record iterator is not used
                assertInstanceOf(DefaultRecordBatch.RecordIterator.class, skipKeyValueIterator);
                // superficial validation for correctness. Deep validation is already performed in other tests
                assertEquals(Utils.toList(records.records()).size(), Utils.toList(skipKeyValueIterator).size());
            } else {
                // assert that a streaming iterator is used for compressed records
                assertInstanceOf(DefaultRecordBatch.StreamRecordIterator.class, skipKeyValueIterator);
                // assert correctness for compressed records
                assertIterableEquals(Arrays.asList(
                        new PartialDefaultRecord(9, (byte) 0, 0L, 1L, -1, 1, 1),
                        new PartialDefaultRecord(8, (byte) 0, 1L, 2L, -1, 1, -1),
                        new PartialDefaultRecord(8, (byte) 0, 2L, 3L, -1, -1, 1),
                        new PartialDefaultRecord(7, (byte) 0, 3L, 4L, -1, -1, -1),
                        new PartialDefaultRecord(15 + largeRecordValue.length, (byte) 0, 4L, 1000L, -1, 3, largeRecordValue.length),
                        new PartialDefaultRecord(23, (byte) 0, 5L, 9999L, -1, 3, 1)
                    ), Utils.toList(skipKeyValueIterator));
            }
        }
    }

    @ParameterizedTest
    @MethodSource
    public void testBufferReuseInSkipKeyValueIterator(CompressionType compressionType, int expectedNumBufferAllocations, byte[] recordValue) {
        MemoryRecords records = MemoryRecords.withRecords(RecordBatch.MAGIC_VALUE_V2, 0L,
            compressionType, TimestampType.CREATE_TIME,
            new SimpleRecord(1000L, "a".getBytes(), "0".getBytes()),
            new SimpleRecord(9999L, "b".getBytes(), recordValue)
        );

        DefaultRecordBatch batch = new DefaultRecordBatch(records.buffer());

        try (BufferSupplier bufferSupplier = spy(BufferSupplier.create());
             CloseableIterator<Record> streamingIterator = batch.skipKeyValueIterator(bufferSupplier)) {

            // Consume through the iterator
            Utils.toList(streamingIterator);

            // Close the iterator to release any buffers
            streamingIterator.close();

            // assert number of buffer allocations
            verify(bufferSupplier, times(expectedNumBufferAllocations)).get(anyInt());
            verify(bufferSupplier, times(expectedNumBufferAllocations)).release(any(ByteBuffer.class));
        }
    }
    private static Stream<Arguments> testBufferReuseInSkipKeyValueIterator() {
        byte[] smallRecordValue = "1".getBytes();
        byte[] largeRecordValue = new byte[512 * 1024]; // 512KB
        RANDOM.nextBytes(largeRecordValue);

        return Stream.of(
            /*
             * 1 allocation per batch (i.e. per iterator instance) for buffer holding uncompressed data
             * = 1 buffer allocations
             */
            Arguments.of(CompressionType.GZIP, 1, smallRecordValue),
            Arguments.of(CompressionType.GZIP, 1, largeRecordValue),
            Arguments.of(CompressionType.SNAPPY, 1, smallRecordValue),
            Arguments.of(CompressionType.SNAPPY, 1, largeRecordValue),
            /*
             * 1 allocation per batch (i.e. per iterator instance) for buffer holding compressed data
             * 1 allocation per batch (i.e. per iterator instance) for buffer holding uncompressed data
             * = 2 buffer allocations
             */
            Arguments.of(CompressionType.LZ4, 2, smallRecordValue),
            Arguments.of(CompressionType.LZ4, 2, largeRecordValue),
            Arguments.of(CompressionType.ZSTD, 2, smallRecordValue),
            Arguments.of(CompressionType.ZSTD, 2, largeRecordValue)
        );
    }

    @ParameterizedTest
    @MethodSource
    public void testZstdJniForSkipKeyValueIterator(int expectedJniCalls, byte[] recordValue) throws IOException {
        MemoryRecords records = MemoryRecords.withRecords(RecordBatch.MAGIC_VALUE_V2, 0L,
            CompressionType.ZSTD, TimestampType.CREATE_TIME,
            new SimpleRecord(9L, "hakuna-matata".getBytes(), recordValue)
        );

        // Buffer containing compressed data
        final ByteBuffer compressedBuf = records.buffer();
        // Create a RecordBatch object
        final DefaultRecordBatch batch = spy(new DefaultRecordBatch(compressedBuf.duplicate()));
        final CompressionType mockCompression = mock(CompressionType.ZSTD.getClass());
        doReturn(mockCompression).when(batch).compressionType();

        // Buffer containing compressed records to be used for creating zstd-jni stream
        ByteBuffer recordsBuffer = compressedBuf.duplicate();
        recordsBuffer.position(RECORDS_OFFSET);

        try (final BufferSupplier bufferSupplier = BufferSupplier.create();
             final InputStream zstdStream = spy(ZstdFactory.wrapForInput(recordsBuffer, batch.magic(), bufferSupplier));
             final InputStream chunkedStream = new ChunkedBytesStream(zstdStream, bufferSupplier, 16 * 1024, false)) {

            when(mockCompression.wrapForInput(any(ByteBuffer.class), anyByte(), any(BufferSupplier.class))).thenReturn(chunkedStream);

            try (CloseableIterator<Record> streamingIterator = batch.skipKeyValueIterator(bufferSupplier)) {
                assertNotNull(streamingIterator);
                Utils.toList(streamingIterator);
                // verify the number of read() calls to zstd JNI stream. Each read() call is a JNI call.
                verify(zstdStream, times(expectedJniCalls)).read(any(byte[].class), anyInt(), anyInt());
                // verify that we don't use the underlying skip() functionality. The underlying skip() allocates
                // 1 buffer per skip call from he buffer pool whereas our implementation does not perform any allocation
                // during skip.
                verify(zstdStream, never()).skip(anyLong());
            }
        }
    }

    private static Stream<Arguments> testZstdJniForSkipKeyValueIterator() {
        byte[] smallRecordValue = "1".getBytes();
        byte[] largeRecordValue = new byte[40 * 1024]; // 40KB
        RANDOM.nextBytes(largeRecordValue);

        return Stream.of(
            /*
             * We expect exactly 2 read call to the JNI:
             * 1 for fetching the full data (size < 16KB)
             * 1 for detecting end of stream by trying to read more data
             */
            Arguments.of(2, smallRecordValue),
            /*
             * We expect exactly 4 read call to the JNI:
             * 3 for fetching the full data (Math.ceil(40/16))
             * 1 for detecting end of stream by trying to read more data
             */
            Arguments.of(4, largeRecordValue)
        );
    }

    @Test
    public void testIncrementSequence() {
        assertEquals(10, DefaultRecordBatch.incrementSequence(5, 5));
        assertEquals(0, DefaultRecordBatch.incrementSequence(Integer.MAX_VALUE, 1));
        assertEquals(4, DefaultRecordBatch.incrementSequence(Integer.MAX_VALUE - 5, 10));
    }

    @Test
    public void testDecrementSequence() {
        assertEquals(0, DefaultRecordBatch.decrementSequence(5, 5));
        assertEquals(Integer.MAX_VALUE, DefaultRecordBatch.decrementSequence(0, 1));
    }

    private static DefaultRecordBatch recordsWithInvalidRecordCount(Byte magicValue, long timestamp,
                                              CompressionType codec, int invalidCount) {
        ByteBuffer buf = ByteBuffer.allocate(512);
        MemoryRecordsBuilder builder = MemoryRecords.builder(buf, magicValue, codec, TimestampType.CREATE_TIME, 0L);
        builder.appendWithOffset(0, timestamp, null, "hello".getBytes());
        builder.appendWithOffset(1, timestamp, null, "there".getBytes());
        builder.appendWithOffset(2, timestamp, null, "beautiful".getBytes());
        MemoryRecords records = builder.build();
        ByteBuffer buffer = records.buffer();
        buffer.position(0);
        buffer.putInt(RECORDS_COUNT_OFFSET, invalidCount);
        buffer.position(0);
        return new DefaultRecordBatch(buffer);
    }
}
