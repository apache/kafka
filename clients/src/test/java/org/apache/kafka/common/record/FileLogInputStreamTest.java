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

import org.apache.kafka.common.record.FileLogInputStream.FileChannelRecordBatch;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static java.util.Arrays.asList;
import static org.apache.kafka.common.record.RecordBatch.MAGIC_VALUE_V0;
import static org.apache.kafka.common.record.RecordBatch.MAGIC_VALUE_V1;
import static org.apache.kafka.common.record.RecordBatch.MAGIC_VALUE_V2;
import static org.apache.kafka.common.record.RecordBatch.NO_TIMESTAMP;
import static org.apache.kafka.common.record.TimestampType.CREATE_TIME;
import static org.apache.kafka.common.record.TimestampType.NO_TIMESTAMP_TYPE;
import static org.apache.kafka.test.TestUtils.tempFile;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(value = Parameterized.class)
public class FileLogInputStreamTest {

    private final byte magic;
    private final CompressionType compression;

    public FileLogInputStreamTest(byte magic, CompressionType compression) {
        this.magic = magic;
        this.compression = compression;
    }

    @Test
    public void testWriteTo() throws IOException {
        try (FileRecords fileRecords = FileRecords.open(tempFile())) {
            fileRecords.append(MemoryRecords.withRecords(magic, compression, new SimpleRecord("foo".getBytes())));
            fileRecords.flush();

            FileLogInputStream logInputStream = new FileLogInputStream(fileRecords, 0, fileRecords.sizeInBytes());

            FileChannelRecordBatch batch = logInputStream.nextBatch();
            assertNotNull(batch);
            assertEquals(magic, batch.magic());

            ByteBuffer buffer = ByteBuffer.allocate(128);
            batch.writeTo(buffer);
            buffer.flip();

            MemoryRecords memRecords = MemoryRecords.readableRecords(buffer);
            List<Record> records = Utils.toList(memRecords.records().iterator());
            assertEquals(1, records.size());
            Record record0 = records.get(0);
            assertTrue(record0.hasMagic(magic));
            assertEquals("foo", Utils.utf8(record0.value(), record0.valueSize()));
        }
    }

    @Test
    public void testSimpleBatchIteration() throws IOException {
        try (FileRecords fileRecords = FileRecords.open(tempFile())) {
            SimpleRecord firstBatchRecord = new SimpleRecord(3241324L, "a".getBytes(), "foo".getBytes());
            SimpleRecord secondBatchRecord = new SimpleRecord(234280L, "b".getBytes(), "bar".getBytes());

            fileRecords.append(MemoryRecords.withRecords(magic, 0L, compression, CREATE_TIME, firstBatchRecord));
            fileRecords.append(MemoryRecords.withRecords(magic, 1L, compression, CREATE_TIME, secondBatchRecord));
            fileRecords.flush();

            FileLogInputStream logInputStream = new FileLogInputStream(fileRecords, 0, fileRecords.sizeInBytes());

            FileChannelRecordBatch firstBatch = logInputStream.nextBatch();
            assertGenericRecordBatchData(firstBatch, 0L, 3241324L, firstBatchRecord);
            assertNoProducerData(firstBatch);

            FileChannelRecordBatch secondBatch = logInputStream.nextBatch();
            assertGenericRecordBatchData(secondBatch, 1L, 234280L, secondBatchRecord);
            assertNoProducerData(secondBatch);

            assertNull(logInputStream.nextBatch());
        }
    }

    @Test
    public void testBatchIterationWithMultipleRecordsPerBatch() throws IOException {
        if (magic < MAGIC_VALUE_V2 && compression == CompressionType.NONE)
            return;

        try (FileRecords fileRecords = FileRecords.open(tempFile())) {
            SimpleRecord[] firstBatchRecords = new SimpleRecord[]{
                new SimpleRecord(3241324L, "a".getBytes(), "1".getBytes()),
                new SimpleRecord(234280L, "b".getBytes(), "2".getBytes())
            };

            SimpleRecord[] secondBatchRecords = new SimpleRecord[]{
                new SimpleRecord(238423489L, "c".getBytes(), "3".getBytes()),
                new SimpleRecord(897839L, null, "4".getBytes()),
                new SimpleRecord(8234020L, "e".getBytes(), null)
            };

            fileRecords.append(MemoryRecords.withRecords(magic, 0L, compression, CREATE_TIME, firstBatchRecords));
            fileRecords.append(MemoryRecords.withRecords(magic, 1L, compression, CREATE_TIME, secondBatchRecords));
            fileRecords.flush();

            FileLogInputStream logInputStream = new FileLogInputStream(fileRecords, 0, fileRecords.sizeInBytes());

            FileChannelRecordBatch firstBatch = logInputStream.nextBatch();
            assertNoProducerData(firstBatch);
            assertGenericRecordBatchData(firstBatch, 0L, 3241324L, firstBatchRecords);

            FileChannelRecordBatch secondBatch = logInputStream.nextBatch();
            assertNoProducerData(secondBatch);
            assertGenericRecordBatchData(secondBatch, 1L, 238423489L, secondBatchRecords);

            assertNull(logInputStream.nextBatch());
        }
    }

    @Test
    public void testBatchIterationV2() throws IOException {
        if (magic != MAGIC_VALUE_V2)
            return;

        try (FileRecords fileRecords = FileRecords.open(tempFile())) {
            long producerId = 83843L;
            short producerEpoch = 15;
            int baseSequence = 234;
            int partitionLeaderEpoch = 9832;

            SimpleRecord[] firstBatchRecords = new SimpleRecord[]{
                new SimpleRecord(3241324L, "a".getBytes(), "1".getBytes()),
                new SimpleRecord(234280L, "b".getBytes(), "2".getBytes())
            };

            SimpleRecord[] secondBatchRecords = new SimpleRecord[]{
                new SimpleRecord(238423489L, "c".getBytes(), "3".getBytes()),
                new SimpleRecord(897839L, null, "4".getBytes()),
                new SimpleRecord(8234020L, "e".getBytes(), null)
            };

            fileRecords.append(MemoryRecords.withIdempotentRecords(magic, 15L, compression, producerId,
                    producerEpoch, baseSequence, partitionLeaderEpoch, firstBatchRecords));
            fileRecords.append(MemoryRecords.withTransactionalRecords(magic, 27L, compression, producerId,
                    producerEpoch, baseSequence + firstBatchRecords.length, partitionLeaderEpoch, secondBatchRecords));
            fileRecords.flush();

            FileLogInputStream logInputStream = new FileLogInputStream(fileRecords, 0, fileRecords.sizeInBytes());

            FileChannelRecordBatch firstBatch = logInputStream.nextBatch();
            assertProducerData(firstBatch, producerId, producerEpoch, baseSequence, false, firstBatchRecords);
            assertGenericRecordBatchData(firstBatch, 15L, 3241324L, firstBatchRecords);
            assertEquals(partitionLeaderEpoch, firstBatch.partitionLeaderEpoch());

            FileChannelRecordBatch secondBatch = logInputStream.nextBatch();
            assertProducerData(secondBatch, producerId, producerEpoch, baseSequence + firstBatchRecords.length,
                    true, secondBatchRecords);
            assertGenericRecordBatchData(secondBatch, 27L, 238423489L, secondBatchRecords);
            assertEquals(partitionLeaderEpoch, secondBatch.partitionLeaderEpoch());

            assertNull(logInputStream.nextBatch());
        }
    }

    @Test
    public void testBatchIterationIncompleteBatch() throws IOException {
        try (FileRecords fileRecords = FileRecords.open(tempFile())) {
            SimpleRecord firstBatchRecord = new SimpleRecord(100L, "foo".getBytes());
            SimpleRecord secondBatchRecord = new SimpleRecord(200L, "bar".getBytes());

            fileRecords.append(MemoryRecords.withRecords(magic, 0L, compression, CREATE_TIME, firstBatchRecord));
            fileRecords.append(MemoryRecords.withRecords(magic, 1L, compression, CREATE_TIME, secondBatchRecord));
            fileRecords.flush();
            fileRecords.truncateTo(fileRecords.sizeInBytes() - 13);

            FileLogInputStream logInputStream = new FileLogInputStream(fileRecords, 0, fileRecords.sizeInBytes());

            FileChannelRecordBatch firstBatch = logInputStream.nextBatch();
            assertNoProducerData(firstBatch);
            assertGenericRecordBatchData(firstBatch, 0L, 100L, firstBatchRecord);

            assertNull(logInputStream.nextBatch());
        }
    }

    @Test
    public void testNextBatchSelectionWithMaxedParams() throws IOException {
        try (FileRecords fileRecords = FileRecords.open(tempFile())) {
            FileLogInputStream logInputStream = new FileLogInputStream(fileRecords, Integer.MAX_VALUE, Integer.MAX_VALUE);
            assertNull(logInputStream.nextBatch());
        }
    }

    @Test
    public void testNextBatchSelectionWithZeroedParams() throws IOException {
        try (FileRecords fileRecords = FileRecords.open(tempFile())) {
            FileLogInputStream logInputStream = new FileLogInputStream(fileRecords, 0, 0);
            assertNull(logInputStream.nextBatch());
        }
    }

    private void assertProducerData(RecordBatch batch, long producerId, short producerEpoch, int baseSequence,
                                    boolean isTransactional, SimpleRecord... records) {
        assertEquals(producerId, batch.producerId());
        assertEquals(producerEpoch, batch.producerEpoch());
        assertEquals(baseSequence, batch.baseSequence());
        assertEquals(baseSequence + records.length - 1, batch.lastSequence());
        assertEquals(isTransactional, batch.isTransactional());
    }

    private void assertNoProducerData(RecordBatch batch) {
        assertEquals(RecordBatch.NO_PRODUCER_ID, batch.producerId());
        assertEquals(RecordBatch.NO_PRODUCER_EPOCH, batch.producerEpoch());
        assertEquals(RecordBatch.NO_SEQUENCE, batch.baseSequence());
        assertEquals(RecordBatch.NO_SEQUENCE, batch.lastSequence());
        assertFalse(batch.isTransactional());
    }

    private void assertGenericRecordBatchData(RecordBatch batch, long baseOffset, long maxTimestamp, SimpleRecord... records) {
        assertEquals(magic, batch.magic());
        assertEquals(compression, batch.compressionType());

        if (magic == MAGIC_VALUE_V0) {
            assertEquals(NO_TIMESTAMP_TYPE, batch.timestampType());
        } else {
            assertEquals(CREATE_TIME, batch.timestampType());
            assertEquals(maxTimestamp, batch.maxTimestamp());
        }

        assertEquals(baseOffset + records.length - 1, batch.lastOffset());
        if (magic >= MAGIC_VALUE_V2)
            assertEquals(Integer.valueOf(records.length), batch.countOrNull());

        assertEquals(baseOffset, batch.baseOffset());
        assertTrue(batch.isValid());

        List<Record> batchRecords = TestUtils.toList(batch);
        for (int i = 0; i < records.length; i++) {
            assertEquals(baseOffset + i, batchRecords.get(i).offset());
            assertEquals(records[i].key(), batchRecords.get(i).key());
            assertEquals(records[i].value(), batchRecords.get(i).value());
            if (magic == MAGIC_VALUE_V0)
                assertEquals(NO_TIMESTAMP, batchRecords.get(i).timestamp());
            else
                assertEquals(records[i].timestamp(), batchRecords.get(i).timestamp());
        }
    }

    @Parameterized.Parameters(name = "magic={0}, compression={1}")
    public static Collection<Object[]> data() {
        List<Object[]> values = new ArrayList<>();
        for (byte magic : asList(MAGIC_VALUE_V0, MAGIC_VALUE_V1, MAGIC_VALUE_V2))
            for (CompressionType type: CompressionType.values())
                values.add(new Object[] {magic, type});
        return values;
    }
}
