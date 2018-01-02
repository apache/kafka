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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.MemoryRecords.RecordFilter.BatchRetention;
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

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(value = Parameterized.class)
public class MemoryRecordsTest {

    private CompressionType compression;
    private byte magic;
    private long firstOffset;
    private long pid;
    private short epoch;
    private int firstSequence;
    private long logAppendTime = System.currentTimeMillis();
    private int partitionLeaderEpoch = 998;

    public MemoryRecordsTest(byte magic, long firstOffset, CompressionType compression) {
        this.magic = magic;
        this.compression = compression;
        this.firstOffset = firstOffset;
        if (magic >= RecordBatch.MAGIC_VALUE_V2) {
            pid = 134234L;
            epoch = 28;
            firstSequence = 777;
        } else {
            pid = RecordBatch.NO_PRODUCER_ID;
            epoch = RecordBatch.NO_PRODUCER_EPOCH;
            firstSequence = RecordBatch.NO_SEQUENCE;
        }
    }

    @Test
    public void testIterator() {
        RecordsBuilder builder = new RecordsBuilder(firstOffset)
                .withMagic(magic)
                .withCompression(compression)
                .withPartitionLeaderEpoch(partitionLeaderEpoch)
                .withProducerMetadata(pid, epoch, firstSequence);

        SimpleRecord[] records = new SimpleRecord[] {
            new SimpleRecord(1L, "a".getBytes(), "1".getBytes()),
            new SimpleRecord(2L, "b".getBytes(), "2".getBytes()),
            new SimpleRecord(3L, "c".getBytes(), "3".getBytes()),
            new SimpleRecord(4L, null, "4".getBytes()),
            new SimpleRecord(5L, "d".getBytes(), null),
            new SimpleRecord(6L, (byte[]) null, null)
        };

        builder.addBatch(records);

        MemoryRecords memoryRecords = builder.build();
        for (int iteration = 0; iteration < 2; iteration++) {
            int total = 0;
            for (RecordBatch batch : memoryRecords.batches()) {
                assertTrue(batch.isValid());
                assertEquals(compression, batch.compressionType());
                assertEquals(firstOffset + total, batch.baseOffset());

                if (magic >= RecordBatch.MAGIC_VALUE_V2) {
                    assertEquals(pid, batch.producerId());
                    assertEquals(epoch, batch.producerEpoch());
                    assertEquals(firstSequence + total, batch.baseSequence());
                    assertEquals(partitionLeaderEpoch, batch.partitionLeaderEpoch());
                    assertEquals(records.length, batch.countOrNull().intValue());
                    assertEquals(TimestampType.CREATE_TIME, batch.timestampType());
                    assertEquals(records[records.length - 1].timestamp(), batch.maxTimestamp());
                } else {
                    assertEquals(RecordBatch.NO_PRODUCER_ID, batch.producerId());
                    assertEquals(RecordBatch.NO_PRODUCER_EPOCH, batch.producerEpoch());
                    assertEquals(RecordBatch.NO_SEQUENCE, batch.baseSequence());
                    assertEquals(RecordBatch.NO_PARTITION_LEADER_EPOCH, batch.partitionLeaderEpoch());
                    assertNull(batch.countOrNull());
                    if (magic == RecordBatch.MAGIC_VALUE_V0)
                        assertEquals(TimestampType.NO_TIMESTAMP_TYPE, batch.timestampType());
                    else
                        assertEquals(TimestampType.CREATE_TIME, batch.timestampType());
                }

                int recordCount = 0;
                for (Record record : batch) {
                    assertTrue(record.isValid());
                    assertTrue(record.hasMagic(batch.magic()));
                    assertFalse(record.isCompressed());
                    assertEquals(firstOffset + total, record.offset());
                    assertEquals(records[total].key(), record.key());
                    assertEquals(records[total].value(), record.value());

                    if (magic >= RecordBatch.MAGIC_VALUE_V2)
                        assertEquals(firstSequence + total, record.sequence());

                    assertFalse(record.hasTimestampType(TimestampType.LOG_APPEND_TIME));
                    if (magic == RecordBatch.MAGIC_VALUE_V0) {
                        assertEquals(RecordBatch.NO_TIMESTAMP, record.timestamp());
                        assertFalse(record.hasTimestampType(TimestampType.CREATE_TIME));
                        assertTrue(record.hasTimestampType(TimestampType.NO_TIMESTAMP_TYPE));
                    } else {
                        assertEquals(records[total].timestamp(), record.timestamp());
                        assertFalse(record.hasTimestampType(TimestampType.NO_TIMESTAMP_TYPE));
                        if (magic < RecordBatch.MAGIC_VALUE_V2)
                            assertTrue(record.hasTimestampType(TimestampType.CREATE_TIME));
                        else
                            assertFalse(record.hasTimestampType(TimestampType.CREATE_TIME));
                    }

                    total++;
                    recordCount++;
                }

                assertEquals(batch.baseOffset() + recordCount - 1, batch.lastOffset());
            }
        }
    }

    /**
     * This test verifies that the checksum returned for various versions matches hardcoded values to catch unintentional
     * changes to how the checksum is computed.
     */
    @Test
    public void testChecksum() {
        // we get reasonable coverage with uncompressed and one compression type
        if (compression != CompressionType.NONE && compression != CompressionType.LZ4)
            return;

        SimpleRecord[] records = {
            new SimpleRecord(283843L, "key1".getBytes(), "value1".getBytes()),
            new SimpleRecord(1234L, "key2".getBytes(), "value2".getBytes())
        };
        MemoryRecords builtRecords = new RecordsBuilder()
                .withMagic(magic)
                .withCompression(compression)
                .addBatch(records)
                .build();

        RecordBatch batch = builtRecords.batches().iterator().next();
        long expectedChecksum;
        if (magic == RecordBatch.MAGIC_VALUE_V0) {
            if (compression == CompressionType.NONE)
                expectedChecksum = 1978725405L;
            else
                expectedChecksum = 66944826L;
        } else if (magic == RecordBatch.MAGIC_VALUE_V1) {
            if (compression == CompressionType.NONE)
                expectedChecksum = 109425508L;
            else
                expectedChecksum = 1407303399L;
        } else {
            if (compression == CompressionType.NONE)
                expectedChecksum = 3851219455L;
            else
                expectedChecksum = 2745969314L;
        }
        assertEquals("Unexpected checksum for magic " + magic + " and compression type " + compression,
                expectedChecksum, batch.checksum());
    }

    @Test
    public void testFilterToPreservesPartitionLeaderEpoch() {
        if (magic >= RecordBatch.MAGIC_VALUE_V2) {
            int partitionLeaderEpoch = 67;

            RecordsBuilder builder = new RecordsBuilder()
                    .withMagic(magic)
                    .withCompression(compression)
                    .withPartitionLeaderEpoch(partitionLeaderEpoch);

            RecordsBuilder.BatchBuilder batchBuilder = builder.newBatch();
            batchBuilder.append(new SimpleRecord(10L, null, "a".getBytes()));
            batchBuilder.append(new SimpleRecord(11L, "1".getBytes(), "b".getBytes()));
            batchBuilder.append(new SimpleRecord(12L, null, "c".getBytes()));
            batchBuilder.closeBatch();

            ByteBuffer filtered = ByteBuffer.allocate(2048);
            builder.build().filterTo(new TopicPartition("foo", 0), new RetainNonNullKeysFilter(), filtered,
                    Integer.MAX_VALUE, BufferSupplier.NO_CACHING);

            filtered.flip();
            MemoryRecords filteredRecords = new MemoryRecords(filtered);

            List<MutableRecordBatch> batches = TestUtils.toList(filteredRecords.batches());
            assertEquals(1, batches.size());

            MutableRecordBatch firstBatch = batches.get(0);
            assertEquals(partitionLeaderEpoch, firstBatch.partitionLeaderEpoch());
        }
    }

    @Test
    public void testFilterToEmptyBatchRetention() {
        if (magic >= RecordBatch.MAGIC_VALUE_V2) {
            for (boolean isTransactional : Arrays.asList(true, false)) {
                long producerId = 23L;
                short producerEpoch = 5;
                long baseOffset = 3L;
                int baseSequence = 10;
                int partitionLeaderEpoch = 293;

                RecordsBuilder builder = new RecordsBuilder(baseOffset)
                        .withMagic(magic)
                        .withCompression(compression)
                        .withPartitionLeaderEpoch(partitionLeaderEpoch)
                        .setTransactional(isTransactional)
                        .withProducerMetadata(producerId, producerEpoch, baseSequence);

                RecordsBuilder.BatchBuilder batchBuilder = builder.newBatch();
                batchBuilder.append(new SimpleRecord(11L, "2".getBytes(), "b".getBytes()));
                batchBuilder.append(new SimpleRecord(12L, "3".getBytes(), "c".getBytes()));
                batchBuilder.closeBatch();

                ByteBuffer filtered = ByteBuffer.allocate(2048);
                builder.build().filterTo(new TopicPartition("foo", 0), new MemoryRecords.RecordFilter() {
                    @Override
                    protected BatchRetention checkBatchRetention(RecordBatch batch) {
                        // retain all batches
                        return BatchRetention.RETAIN_EMPTY;
                    }

                    @Override
                    protected boolean shouldRetainRecord(RecordBatch recordBatch, Record record) {
                        // delete the records
                        return false;
                    }
                }, filtered, Integer.MAX_VALUE, BufferSupplier.NO_CACHING);

                filtered.flip();
                MemoryRecords filteredRecords = new MemoryRecords(filtered);

                List<MutableRecordBatch> batches = TestUtils.toList(filteredRecords.batches());
                assertEquals(1, batches.size());

                MutableRecordBatch batch = batches.get(0);
                assertEquals(0, batch.countOrNull().intValue());
                assertEquals(12L, batch.maxTimestamp());
                assertEquals(TimestampType.CREATE_TIME, batch.timestampType());
                assertEquals(baseOffset, batch.baseOffset());
                assertEquals(baseOffset + 1, batch.lastOffset());
                assertEquals(baseSequence, batch.baseSequence());
                assertEquals(baseSequence + 1, batch.lastSequence());
                assertEquals(isTransactional, batch.isTransactional());
            }
        }
    }

    @Test
    public void testEmptyBatchDeletion() {
        if (magic >= RecordBatch.MAGIC_VALUE_V2) {
            for (final BatchRetention deleteRetention : Arrays.asList(BatchRetention.DELETE, BatchRetention.DELETE_EMPTY)) {
                ByteBuffer buffer = ByteBuffer.allocate(DefaultRecordBatch.RECORD_BATCH_OVERHEAD);
                long producerId = 23L;
                short producerEpoch = 5;
                long baseOffset = 3L;
                int baseSequence = 10;
                int partitionLeaderEpoch = 293;

                DefaultRecordBatch.writeEmptyHeader(buffer, RecordBatch.MAGIC_VALUE_V2, producerId, producerEpoch,
                        baseSequence, baseOffset, baseOffset, partitionLeaderEpoch, TimestampType.CREATE_TIME,
                        System.currentTimeMillis(), false, false);
                buffer.flip();

                ByteBuffer filtered = ByteBuffer.allocate(2048);
                new MemoryRecords(buffer).filterTo(new TopicPartition("foo", 0), new MemoryRecords.RecordFilter() {
                    @Override
                    protected BatchRetention checkBatchRetention(RecordBatch batch) {
                        return deleteRetention;
                    }

                    @Override
                    protected boolean shouldRetainRecord(RecordBatch recordBatch, Record record) {
                        return false;
                    }
                }, filtered, Integer.MAX_VALUE, BufferSupplier.NO_CACHING);

                filtered.flip();
                MemoryRecords filteredRecords = new MemoryRecords(filtered);
                assertEquals(0, filteredRecords.sizeInBytes());
            }
        }
    }

    @Test
    public void testBuildEndTxnMarker() {
        if (magic >= RecordBatch.MAGIC_VALUE_V2) {
            long producerId = 73;
            short producerEpoch = 13;
            long initialOffset = 983L;
            int coordinatorEpoch = 347;
            int partitionLeaderEpoch = 29;

            EndTransactionMarker marker = new EndTransactionMarker(ControlRecordType.COMMIT, coordinatorEpoch);
            MemoryRecords records = new RecordsBuilder(initialOffset)
                    .withProducerMetadata(producerId, producerEpoch)
                    .withPartitionLeaderEpoch(partitionLeaderEpoch)
                    .addControlBatch(System.currentTimeMillis(), marker)
                    .build();
            // verify that buffer allocation was precise
            assertEquals(records.buffer().remaining(), records.buffer().capacity());

            List<MutableRecordBatch> batches = TestUtils.toList(records.batches());
            assertEquals(1, batches.size());

            RecordBatch batch = batches.get(0);
            assertTrue(batch.isControlBatch());
            assertEquals(producerId, batch.producerId());
            assertEquals(producerEpoch, batch.producerEpoch());
            assertEquals(initialOffset, batch.baseOffset());
            assertEquals(partitionLeaderEpoch, batch.partitionLeaderEpoch());
            assertTrue(batch.isValid());

            List<Record> createdRecords = TestUtils.toList(batch);
            assertEquals(1, createdRecords.size());

            Record record = createdRecords.get(0);
            assertTrue(record.isValid());
            EndTransactionMarker deserializedMarker = EndTransactionMarker.deserialize(record);
            assertEquals(ControlRecordType.COMMIT, deserializedMarker.controlType());
            assertEquals(coordinatorEpoch, deserializedMarker.coordinatorEpoch());
        }
    }

    @Test
    public void testFilterToBatchDiscard() {
        if (compression != CompressionType.NONE || magic >= RecordBatch.MAGIC_VALUE_V2) {
            RecordsBuilder builder = new RecordsBuilder().withMagic(magic).withCompression(compression);

            RecordsBuilder.BatchBuilder firstBatch = builder.newBatch();
            firstBatch.append(new SimpleRecord(10L, "1".getBytes(), "a".getBytes()));
            firstBatch.closeBatch();

            RecordsBuilder.BatchBuilder secondBatch = builder.newBatch();
            secondBatch.append(new SimpleRecord(11L, "2".getBytes(), "b".getBytes()));
            secondBatch.append(new SimpleRecord(12L, "3".getBytes(), "c".getBytes()));
            secondBatch.closeBatch();

            RecordsBuilder.BatchBuilder thirdBatch = builder.newBatch();
            thirdBatch.append(new SimpleRecord(13L, "4".getBytes(), "d".getBytes()));
            thirdBatch.append(new SimpleRecord(20L, "5".getBytes(), "e".getBytes()));
            thirdBatch.append(new SimpleRecord(15L, "6".getBytes(), "f".getBytes()));
            thirdBatch.closeBatch();

            RecordsBuilder.BatchBuilder fourthBatch = builder.newBatch();
            fourthBatch.append(new SimpleRecord(16L, "7".getBytes(), "g".getBytes()));
            fourthBatch.closeBatch();

            ByteBuffer filtered = ByteBuffer.allocate(2048);
            builder.build().filterTo(new TopicPartition("foo", 0), new MemoryRecords.RecordFilter() {
                @Override
                protected BatchRetention checkBatchRetention(RecordBatch batch) {
                    // discard the second and fourth batches
                    if (batch.lastOffset() == 2L || batch.lastOffset() == 6L)
                        return BatchRetention.DELETE;
                    return BatchRetention.DELETE_EMPTY;
                }

                @Override
                protected boolean shouldRetainRecord(RecordBatch recordBatch, Record record) {
                    return true;
                }
            }, filtered, Integer.MAX_VALUE, BufferSupplier.NO_CACHING);

            filtered.flip();
            MemoryRecords filteredRecords = new MemoryRecords(filtered);

            List<MutableRecordBatch> batches = TestUtils.toList(filteredRecords.batches());
            assertEquals(2, batches.size());
            assertEquals(0L, batches.get(0).lastOffset());
            assertEquals(5L, batches.get(1).lastOffset());
        }
    }

    @Test
    public void testFilterToAlreadyCompactedLog() {
        // create a batch with some offset gaps to simulate a compacted batch
        RecordsBuilder builder = new RecordsBuilder().withMagic(magic).withCompression(compression);

        RecordsBuilder.BatchBuilder batchBuilder = builder.newBatch();
        batchBuilder.appendWithOffset(5L, new SimpleRecord(10L, null, "a".getBytes()));
        batchBuilder.appendWithOffset(8L, new SimpleRecord(11L, "1".getBytes(), "b".getBytes()));
        batchBuilder.appendWithOffset(10L, new SimpleRecord(12L, null, "c".getBytes()));
        batchBuilder.closeBatch();

        ByteBuffer filtered = ByteBuffer.allocate(2048);
        builder.build().filterTo(new TopicPartition("foo", 0), new RetainNonNullKeysFilter(),
                filtered, Integer.MAX_VALUE, BufferSupplier.NO_CACHING);
        filtered.flip();
        MemoryRecords filteredRecords = new MemoryRecords(filtered);

        List<MutableRecordBatch> batches = TestUtils.toList(filteredRecords.batches());
        assertEquals(1, batches.size());

        MutableRecordBatch batch = batches.get(0);
        List<Record> records = TestUtils.toList(batch);
        assertEquals(1, records.size());
        assertEquals(8L, records.get(0).offset());


        if (magic >= RecordBatch.MAGIC_VALUE_V1)
            assertEquals(new SimpleRecord(11L, "1".getBytes(), "b".getBytes()), new SimpleRecord(records.get(0)));
        else
            assertEquals(new SimpleRecord(RecordBatch.NO_TIMESTAMP, "1".getBytes(), "b".getBytes()),
                    new SimpleRecord(records.get(0)));

        if (magic >= RecordBatch.MAGIC_VALUE_V2) {
            // the new format preserves first and last offsets from the original batch
            assertEquals(0L, batch.baseOffset());
            assertEquals(10L, batch.lastOffset());
        } else {
            assertEquals(8L, batch.baseOffset());
            assertEquals(8L, batch.lastOffset());
        }
    }

    @Test
    public void testFilterToPreservesProducerInfo() {
        if (magic >= RecordBatch.MAGIC_VALUE_V2) {
            RecordsBuilder builder = new RecordsBuilder().withMagic(magic).withCompression(compression);

            // non-idempotent, non-transactional
            RecordsBuilder.BatchBuilder batch = builder.newBatch();
            batch.append(new SimpleRecord(10L, null, "a".getBytes()));
            batch.append(new SimpleRecord(11L, "1".getBytes(), "b".getBytes()));
            batch.append(new SimpleRecord(12L, null, "c".getBytes()));
            batch.closeBatch();

            // idempotent
            long pid1 = 23L;
            short epoch1 = 5;
            int baseSequence1 = 10;
            RecordsBuilder.BatchBuilder idempotentBatch = builder.newBatch()
                    .withProducerMetadata(pid1, epoch1, baseSequence1);
            idempotentBatch.append(new SimpleRecord(13L, null, "d".getBytes()));
            idempotentBatch.append(new SimpleRecord(14L, "4".getBytes(), "e".getBytes()));
            idempotentBatch.append(new SimpleRecord(15L, "5".getBytes(), "f".getBytes()));
            idempotentBatch.closeBatch();

            // transactional
            long pid2 = 99384L;
            short epoch2 = 234;
            int baseSequence2 = 15;
            RecordsBuilder.BatchBuilder transactionalBatch = builder.newBatch()
                    .setTransactional(true)
                    .withProducerMetadata(pid2, epoch2, baseSequence2);
            transactionalBatch.append(new SimpleRecord(16L, "6".getBytes(), "g".getBytes()));
            transactionalBatch.append(new SimpleRecord(17L, "7".getBytes(), "h".getBytes()));
            transactionalBatch.append(new SimpleRecord(18L, null, "i".getBytes()));
            transactionalBatch.closeBatch();

            MemoryRecords unfilteredRecords = builder.build();
            ByteBuffer filtered = ByteBuffer.allocate(2048);
            unfilteredRecords.filterTo(new TopicPartition("foo", 0), new RetainNonNullKeysFilter(),
                    filtered, Integer.MAX_VALUE, BufferSupplier.NO_CACHING);

            filtered.flip();
            MemoryRecords filteredRecords = new MemoryRecords(filtered);

            List<MutableRecordBatch> batches = TestUtils.toList(filteredRecords.batches());
            assertEquals(3, batches.size());

            MutableRecordBatch filteredBatch = batches.get(0);
            assertEquals(1, filteredBatch.countOrNull().intValue());
            assertEquals(0L, filteredBatch.baseOffset());
            assertEquals(2L, filteredBatch.lastOffset());
            assertEquals(RecordBatch.NO_PRODUCER_ID, filteredBatch.producerId());
            assertEquals(RecordBatch.NO_PRODUCER_EPOCH, filteredBatch.producerEpoch());
            assertEquals(RecordBatch.NO_SEQUENCE, filteredBatch.baseSequence());
            assertEquals(RecordBatch.NO_SEQUENCE, filteredBatch.lastSequence());
            assertFalse(filteredBatch.isTransactional());
            List<Record> firstBatchRecords = TestUtils.toList(filteredBatch);
            assertEquals(1, firstBatchRecords.size());
            assertEquals(RecordBatch.NO_SEQUENCE, firstBatchRecords.get(0).sequence());
            assertEquals(new SimpleRecord(11L, "1".getBytes(), "b".getBytes()), new SimpleRecord(firstBatchRecords.get(0)));

            MutableRecordBatch filteredIdempotentBatch = batches.get(1);
            assertEquals(2, filteredIdempotentBatch.countOrNull().intValue());
            assertEquals(3L, filteredIdempotentBatch.baseOffset());
            assertEquals(5L, filteredIdempotentBatch.lastOffset());
            assertEquals(pid1, filteredIdempotentBatch.producerId());
            assertEquals(epoch1, filteredIdempotentBatch.producerEpoch());
            assertEquals(baseSequence1, filteredIdempotentBatch.baseSequence());
            assertEquals(baseSequence1 + 2, filteredIdempotentBatch.lastSequence());
            assertFalse(filteredIdempotentBatch.isTransactional());
            List<Record> secondBatchRecords = TestUtils.toList(filteredIdempotentBatch);
            assertEquals(2, secondBatchRecords.size());
            assertEquals(baseSequence1 + 1, secondBatchRecords.get(0).sequence());
            assertEquals(new SimpleRecord(14L, "4".getBytes(), "e".getBytes()), new SimpleRecord(secondBatchRecords.get(0)));
            assertEquals(baseSequence1 + 2, secondBatchRecords.get(1).sequence());
            assertEquals(new SimpleRecord(15L, "5".getBytes(), "f".getBytes()), new SimpleRecord(secondBatchRecords.get(1)));

            MutableRecordBatch filteredTransactionalBatch = batches.get(2);
            assertEquals(2, filteredTransactionalBatch.countOrNull().intValue());
            assertEquals(6L, filteredTransactionalBatch.baseOffset());
            assertEquals(8L, filteredTransactionalBatch.lastOffset());
            assertEquals(pid2, filteredTransactionalBatch.producerId());
            assertEquals(epoch2, filteredTransactionalBatch.producerEpoch());
            assertEquals(baseSequence2, filteredTransactionalBatch.baseSequence());
            assertEquals(baseSequence2 + 2, filteredTransactionalBatch.lastSequence());
            assertTrue(filteredTransactionalBatch.isTransactional());
            List<Record> thirdBatchRecords = TestUtils.toList(filteredTransactionalBatch);
            assertEquals(2, thirdBatchRecords.size());
            assertEquals(baseSequence2, thirdBatchRecords.get(0).sequence());
            assertEquals(new SimpleRecord(16L, "6".getBytes(), "g".getBytes()), new SimpleRecord(thirdBatchRecords.get(0)));
            assertEquals(baseSequence2 + 1, thirdBatchRecords.get(1).sequence());
            assertEquals(new SimpleRecord(17L, "7".getBytes(), "h".getBytes()), new SimpleRecord(thirdBatchRecords.get(1)));
        }
    }

    @Test
    public void testFilterToWithUndersizedBuffer() {
        RecordsBuilder builder = new RecordsBuilder().withMagic(magic).withCompression(compression);
        builder.addBatch(
                new SimpleRecord(10L, null, "a".getBytes()));
        builder.addBatch(
                new SimpleRecord(11L, "1".getBytes(), new byte[128]),
                new SimpleRecord(12L, "2".getBytes(), "c".getBytes()),
                new SimpleRecord(13L, null, "d".getBytes()));
        builder.addBatch(
                new SimpleRecord(14L, null, "e".getBytes()),
                new SimpleRecord(15L, "5".getBytes(), "f".getBytes()),
                new SimpleRecord(16L, "6".getBytes(), "g".getBytes()));
        builder.addBatch(
                new SimpleRecord(17L, "7".getBytes(), new byte[128]));

        ByteBuffer buffer = builder.build().buffer();
        ByteBuffer output = ByteBuffer.allocate(64);

        List<Record> records = new ArrayList<>();
        while (buffer.hasRemaining()) {
            output.rewind();

            MemoryRecords.FilterResult result = new MemoryRecords(buffer)
                    .filterTo(new TopicPartition("foo", 0), new RetainNonNullKeysFilter(), output, Integer.MAX_VALUE,
                              BufferSupplier.NO_CACHING);

            buffer.position(buffer.position() + result.bytesRead);
            result.output.flip();

            if (output != result.output)
                assertEquals(0, output.position());

            MemoryRecords filtered = new MemoryRecords(result.output);
            records.addAll(TestUtils.toList(filtered.records()));
        }

        assertEquals(5, records.size());
        for (Record record : records)
            assertNotNull(record.key());
    }

    @Test
    public void testToString() {
        long timestamp = 1000000;
        MemoryRecords memoryRecords = new RecordsBuilder().withMagic(magic).withCompression(compression)
                .newBatch()
                .append(new SimpleRecord(timestamp, "key1".getBytes(), "value1".getBytes()))
                .append(new SimpleRecord(timestamp + 1, "key2".getBytes(), "value2".getBytes()))
                .closeBatch()
                .build();
        switch (magic) {
            case RecordBatch.MAGIC_VALUE_V0:
                assertEquals("[(record=LegacyRecordBatch(offset=0, Record(magic=0, attributes=0, compression=NONE, " +
                                "crc=1978725405, key=4 bytes, value=6 bytes))), (record=LegacyRecordBatch(offset=1, Record(magic=0, " +
                                "attributes=0, compression=NONE, crc=1964753830, key=4 bytes, value=6 bytes)))]",
                        memoryRecords.toString());
                break;
            case RecordBatch.MAGIC_VALUE_V1:
                assertEquals("[(record=LegacyRecordBatch(offset=0, Record(magic=1, attributes=0, compression=NONE, " +
                        "crc=97210616, CreateTime=1000000, key=4 bytes, value=6 bytes))), (record=LegacyRecordBatch(offset=1, " +
                        "Record(magic=1, attributes=0, compression=NONE, crc=3535988507, CreateTime=1000001, key=4 bytes, " +
                        "value=6 bytes)))]",
                        memoryRecords.toString());
                break;
            case RecordBatch.MAGIC_VALUE_V2:
                assertEquals("[(record=DefaultRecord(offset=0, timestamp=1000000, key=4 bytes, value=6 bytes)), " +
                                "(record=DefaultRecord(offset=1, timestamp=1000001, key=4 bytes, value=6 bytes))]",
                        memoryRecords.toString());
                break;
            default:
                fail("Unexpected magic " + magic);
        }
    }

    @Test
    public void testFilterTo() {
        RecordsBuilder builder = new RecordsBuilder().withMagic(magic).withCompression(compression);
        builder.addBatch(
                new SimpleRecord(10L, null, "a".getBytes()));
        builder.addBatch(
                new SimpleRecord(11L, "1".getBytes(), "b".getBytes()),
                new SimpleRecord(12L, null, "c".getBytes()));
        builder.addBatch(
                new SimpleRecord(13L, null, "d".getBytes()),
                new SimpleRecord(20L, "4".getBytes(), "e".getBytes()),
                new SimpleRecord(15L, "5".getBytes(), "f".getBytes()));
        builder.addBatch(
                new SimpleRecord(16L, "6".getBytes(), "g".getBytes()));

        MemoryRecords unfiltered = builder.build();
        ByteBuffer filtered = ByteBuffer.allocate(2048);
        MemoryRecords.FilterResult result = unfiltered.filterTo(
                new TopicPartition("foo", 0), new RetainNonNullKeysFilter(), filtered, Integer.MAX_VALUE,
                BufferSupplier.NO_CACHING);

        filtered.flip();

        assertEquals(7, result.messagesRead);
        assertEquals(4, result.messagesRetained);
        assertEquals(unfiltered.sizeInBytes(), result.bytesRead);
        assertEquals(filtered.limit(), result.bytesRetained);
        if (magic > RecordBatch.MAGIC_VALUE_V0) {
            assertEquals(20L, result.maxTimestamp);
            if (compression == CompressionType.NONE && magic < RecordBatch.MAGIC_VALUE_V2)
                assertEquals(4L, result.shallowOffsetOfMaxTimestamp);
            else
                assertEquals(5L, result.shallowOffsetOfMaxTimestamp);
        }

        MemoryRecords filteredRecords = new MemoryRecords(filtered);

        List<MutableRecordBatch> batches = TestUtils.toList(filteredRecords.batches());
        final List<Long> expectedEndOffsets;
        final List<Long> expectedStartOffsets;
        final List<Long> expectedMaxTimestamps;

        if (magic < RecordBatch.MAGIC_VALUE_V2 && compression == CompressionType.NONE) {
            expectedEndOffsets = asList(1L, 4L, 5L, 6L);
            expectedStartOffsets = asList(1L, 4L, 5L, 6L);
            expectedMaxTimestamps = asList(11L, 20L, 15L, 16L);
        } else if (magic < RecordBatch.MAGIC_VALUE_V2) {
            expectedEndOffsets = asList(1L, 5L, 6L);
            expectedStartOffsets = asList(1L, 4L, 6L);
            expectedMaxTimestamps = asList(11L, 20L, 16L);
        } else {
            expectedEndOffsets = asList(2L, 5L, 6L);
            expectedStartOffsets = asList(1L, 3L, 6L);
            expectedMaxTimestamps = asList(11L, 20L, 16L);
        }

        assertEquals(expectedEndOffsets.size(), batches.size());

        for (int i = 0; i < expectedEndOffsets.size(); i++) {
            RecordBatch batch = batches.get(i);
            assertEquals(expectedStartOffsets.get(i).longValue(), batch.baseOffset());
            assertEquals(expectedEndOffsets.get(i).longValue(), batch.lastOffset());
            assertEquals(magic, batch.magic());
            assertEquals(compression, batch.compressionType());
            if (magic >= RecordBatch.MAGIC_VALUE_V1) {
                assertEquals(expectedMaxTimestamps.get(i).longValue(), batch.maxTimestamp());
                assertEquals(TimestampType.CREATE_TIME, batch.timestampType());
            } else {
                assertEquals(RecordBatch.NO_TIMESTAMP, batch.maxTimestamp());
                assertEquals(TimestampType.NO_TIMESTAMP_TYPE, batch.timestampType());
            }
        }

        List<Record> records = TestUtils.toList(filteredRecords.records());
        assertEquals(4, records.size());

        Record first = records.get(0);
        assertEquals(1L, first.offset());
        if (magic > RecordBatch.MAGIC_VALUE_V0)
            assertEquals(11L, first.timestamp());
        assertEquals("1", Utils.utf8(first.key(), first.keySize()));
        assertEquals("b", Utils.utf8(first.value(), first.valueSize()));

        Record second = records.get(1);
        assertEquals(4L, second.offset());
        if (magic > RecordBatch.MAGIC_VALUE_V0)
            assertEquals(20L, second.timestamp());
        assertEquals("4", Utils.utf8(second.key(), second.keySize()));
        assertEquals("e", Utils.utf8(second.value(), second.valueSize()));

        Record third = records.get(2);
        assertEquals(5L, third.offset());
        if (magic > RecordBatch.MAGIC_VALUE_V0)
            assertEquals(15L, third.timestamp());
        assertEquals("5", Utils.utf8(third.key(), third.keySize()));
        assertEquals("f", Utils.utf8(third.value(), third.valueSize()));

        Record fourth = records.get(3);
        assertEquals(6L, fourth.offset());
        if (magic > RecordBatch.MAGIC_VALUE_V0)
            assertEquals(16L, fourth.timestamp());
        assertEquals("6", Utils.utf8(fourth.key(), fourth.keySize()));
        assertEquals("g", Utils.utf8(fourth.value(), fourth.valueSize()));
    }

    @Test
    public void testFilterToPreservesLogAppendTime() {
        long logAppendTime = System.currentTimeMillis();

        RecordsBuilder builder = new RecordsBuilder()
                .withMagic(magic)
                .withCompression(compression)
                .withTimestampType(TimestampType.LOG_APPEND_TIME);

        builder.newBatch()
                .withLogAppendTime(logAppendTime)
                .withProducerMetadata(pid, epoch, firstSequence)
                .append(new SimpleRecord(10L, null, "a".getBytes()))
                .closeBatch();

        builder.newBatch()
                .withLogAppendTime(logAppendTime)
                .withProducerMetadata(pid, epoch, firstSequence)
                .append(new SimpleRecord(11L, "1".getBytes(), "b".getBytes()))
                .append(new SimpleRecord(12L, null, "c".getBytes()))
                .closeBatch();

        builder.newBatch()
                .withLogAppendTime(logAppendTime)
                .withProducerMetadata(pid, epoch, firstSequence)
                .append(new SimpleRecord(13L, null, "d".getBytes()))
                .append(new SimpleRecord(14L, "4".getBytes(), "e".getBytes()))
                .append(new SimpleRecord(15L, "5".getBytes(), "f".getBytes()))
                .closeBatch();

        MemoryRecords unfilteredRecords = builder.build();
        ByteBuffer filtered = ByteBuffer.allocate(2048);
        unfilteredRecords.filterTo(new TopicPartition("foo", 0), new RetainNonNullKeysFilter(),
                filtered, Integer.MAX_VALUE, BufferSupplier.NO_CACHING);

        filtered.flip();
        MemoryRecords filteredRecords = new MemoryRecords(filtered);

        List<MutableRecordBatch> batches = TestUtils.toList(filteredRecords.batches());
        assertEquals(magic < RecordBatch.MAGIC_VALUE_V2 && compression == CompressionType.NONE ? 3 : 2, batches.size());

        for (RecordBatch batch : batches) {
            assertEquals(compression, batch.compressionType());
            if (magic > RecordBatch.MAGIC_VALUE_V0) {
                assertEquals(TimestampType.LOG_APPEND_TIME, batch.timestampType());
                assertEquals(logAppendTime, batch.maxTimestamp());
            }
        }
    }



    @Parameterized.Parameters(name = "{index} magic={0}, firstOffset={1}, compressionType={2}")
    public static Collection<Object[]> data() {
        List<Object[]> values = new ArrayList<>();
        for (long firstOffset : asList(0L, 57L))
            for (byte magic : asList(RecordBatch.MAGIC_VALUE_V0, RecordBatch.MAGIC_VALUE_V1, RecordBatch.MAGIC_VALUE_V2))
                for (CompressionType type: CompressionType.values())
                    values.add(new Object[] {magic, firstOffset, type});
        return values;
    }

    private static class RetainNonNullKeysFilter extends MemoryRecords.RecordFilter {
        @Override
        protected BatchRetention checkBatchRetention(RecordBatch batch) {
            return BatchRetention.DELETE_EMPTY;
        }

        @Override
        public boolean shouldRetainRecord(RecordBatch batch, Record record) {
            return record.hasKey();
        }
    }
}
