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
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.utils.Time;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static java.util.Arrays.asList;
import static org.apache.kafka.common.utils.Utils.utf8;
import static org.apache.kafka.test.TestUtils.tempFile;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LazyDownConversionRecordsTest {
    /**
     * Test the lazy down-conversion path in the presence of commit markers. When converting to V0 or V1, these batches
     * are dropped. If there happen to be no more batches left to convert, we must get an overflow message batch after
     * conversion.
     */
    @Test
    public void testConversionOfCommitMarker() throws IOException {
        MemoryRecords recordsToConvert = MemoryRecords.withEndTransactionMarker(0, Time.SYSTEM.milliseconds(), RecordBatch.NO_PARTITION_LEADER_EPOCH,
                1, (short) 1, new EndTransactionMarker(ControlRecordType.COMMIT, 0));
        MemoryRecords convertedRecords = convertRecords(recordsToConvert, (byte) 1, recordsToConvert.sizeInBytes());
        ByteBuffer buffer = convertedRecords.buffer();

        // read the offset and the batch length
        buffer.getLong();
        int sizeOfConvertedRecords = buffer.getInt();

        // assert we got an overflow message batch
        assertTrue(sizeOfConvertedRecords > buffer.limit());
        assertFalse(convertedRecords.batchIterator().hasNext());
    }

    @RunWith(value = Parameterized.class)
    public static class ParameterizedConversionTest {
        private final CompressionType compressionType;
        private final byte toMagic;

        public ParameterizedConversionTest(CompressionType compressionType, byte toMagic) {
            this.compressionType = compressionType;
            this.toMagic = toMagic;
        }

        @Parameterized.Parameters(name = "compressionType={0}, toMagic={1}")
        public static Collection<Object[]> data() {
            List<Object[]> values = new ArrayList<>();
            for (byte toMagic = RecordBatch.MAGIC_VALUE_V0; toMagic <= RecordBatch.CURRENT_MAGIC_VALUE; toMagic++) {
                values.add(new Object[]{CompressionType.NONE, toMagic});
                values.add(new Object[]{CompressionType.GZIP, toMagic});
            }
            return values;
        }

        /**
         * Test the lazy down-conversion path.
         */
        @Test
        public void testConversion() throws IOException {
            doTestConversion(false);
        }

        /**
         * Test the lazy down-conversion path where the number of bytes we want to convert is much larger than the
         * number of bytes we get after conversion. This causes overflow message batch(es) to be appended towards the
         * end of the converted output.
         */
        @Test
        public void testConversionWithOverflow() throws IOException {
            doTestConversion(true);
        }

        private void doTestConversion(boolean testConversionOverflow) throws IOException {
            List<Long> offsets = asList(0L, 2L, 3L, 9L, 11L, 15L, 16L, 17L, 22L, 24L);

            Header[] headers = {new RecordHeader("headerKey1", "headerValue1".getBytes()),
                                new RecordHeader("headerKey2", "headerValue2".getBytes()),
                                new RecordHeader("headerKey3", "headerValue3".getBytes())};

            List<SimpleRecord> records = asList(
                    new SimpleRecord(1L, "k1".getBytes(), "hello".getBytes()),
                    new SimpleRecord(2L, "k2".getBytes(), "goodbye".getBytes()),
                    new SimpleRecord(3L, "k3".getBytes(), "hello again".getBytes()),
                    new SimpleRecord(4L, "k4".getBytes(), "goodbye for now".getBytes()),
                    new SimpleRecord(5L, "k5".getBytes(), "hello again".getBytes()),
                    new SimpleRecord(6L, "k6".getBytes(), "I sense indecision".getBytes()),
                    new SimpleRecord(7L, "k7".getBytes(), "what now".getBytes()),
                    new SimpleRecord(8L, "k8".getBytes(), "running out".getBytes(), headers),
                    new SimpleRecord(9L, "k9".getBytes(), "ok, almost done".getBytes()),
                    new SimpleRecord(10L, "k10".getBytes(), "finally".getBytes(), headers));
            assertEquals("incorrect test setup", offsets.size(), records.size());

            ByteBuffer buffer = ByteBuffer.allocate(1024);
            MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, RecordBatch.CURRENT_MAGIC_VALUE, compressionType,
                    TimestampType.CREATE_TIME, 0L);
            for (int i = 0; i < 3; i++)
                builder.appendWithOffset(offsets.get(i), records.get(i));
            builder.close();

            builder = MemoryRecords.builder(buffer, RecordBatch.CURRENT_MAGIC_VALUE, compressionType, TimestampType.CREATE_TIME,
                    0L);
            for (int i = 3; i < 6; i++)
                builder.appendWithOffset(offsets.get(i), records.get(i));
            builder.close();

            builder = MemoryRecords.builder(buffer, RecordBatch.CURRENT_MAGIC_VALUE, compressionType, TimestampType.CREATE_TIME,
                    0L);
            for (int i = 6; i < 10; i++)
                builder.appendWithOffset(offsets.get(i), records.get(i));
            builder.close();
            buffer.flip();

            MemoryRecords recordsToConvert = MemoryRecords.readableRecords(buffer);
            int numBytesToConvert = recordsToConvert.sizeInBytes();
            if (testConversionOverflow)
                numBytesToConvert *= 2;

            MemoryRecords convertedRecords = convertRecords(recordsToConvert, toMagic, numBytesToConvert);
            verifyDownConvertedRecords(records, offsets, convertedRecords, compressionType, toMagic);
        }
    }

    private static MemoryRecords convertRecords(MemoryRecords recordsToConvert, byte toMagic, int bytesToConvert) throws IOException {
        try (FileRecords inputRecords = FileRecords.open(tempFile())) {
            inputRecords.append(recordsToConvert);
            inputRecords.flush();

            LazyDownConversionRecords lazyRecords = new LazyDownConversionRecords(new TopicPartition("test", 1),
                    inputRecords, toMagic, 0L, Time.SYSTEM);
            LazyDownConversionRecordsSend lazySend = lazyRecords.toSend("foo");
            File outputFile = tempFile();
            FileChannel channel = FileChannel.open(outputFile.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE);

            int written = 0;
            while (written < bytesToConvert)
                written += lazySend.writeTo(channel, written, bytesToConvert - written);

            FileRecords convertedRecords = FileRecords.open(outputFile, true, (int) channel.size(), false);
            ByteBuffer convertedRecordsBuffer = ByteBuffer.allocate(convertedRecords.sizeInBytes());
            convertedRecords.readInto(convertedRecordsBuffer, 0);

            // cleanup
            convertedRecords.close();
            channel.close();

            return MemoryRecords.readableRecords(convertedRecordsBuffer);
        }
    }

    private static void verifyDownConvertedRecords(List<SimpleRecord> initialRecords,
                                                   List<Long> initialOffsets,
                                                   MemoryRecords downConvertedRecords,
                                                   CompressionType compressionType,
                                                   byte toMagic) {
        int i = 0;
        for (RecordBatch batch : downConvertedRecords.batches()) {
            assertTrue("Magic byte should be lower than or equal to " + toMagic, batch.magic() <= toMagic);
            if (batch.magic() == RecordBatch.MAGIC_VALUE_V0)
                assertEquals(TimestampType.NO_TIMESTAMP_TYPE, batch.timestampType());
            else
                assertEquals(TimestampType.CREATE_TIME, batch.timestampType());
            assertEquals("Compression type should not be affected by conversion", compressionType, batch.compressionType());
            for (Record record : batch) {
                assertTrue("Inner record should have magic " + toMagic, record.hasMagic(batch.magic()));
                assertEquals("Offset should not change", initialOffsets.get(i).longValue(), record.offset());
                assertEquals("Key should not change", utf8(initialRecords.get(i).key()), utf8(record.key()));
                assertEquals("Value should not change", utf8(initialRecords.get(i).value()), utf8(record.value()));
                assertFalse(record.hasTimestampType(TimestampType.LOG_APPEND_TIME));
                if (batch.magic() == RecordBatch.MAGIC_VALUE_V0) {
                    assertEquals(RecordBatch.NO_TIMESTAMP, record.timestamp());
                    assertFalse(record.hasTimestampType(TimestampType.CREATE_TIME));
                    assertTrue(record.hasTimestampType(TimestampType.NO_TIMESTAMP_TYPE));
                } else if (batch.magic() == RecordBatch.MAGIC_VALUE_V1) {
                    assertEquals("Timestamp should not change", initialRecords.get(i).timestamp(), record.timestamp());
                    assertTrue(record.hasTimestampType(TimestampType.CREATE_TIME));
                    assertFalse(record.hasTimestampType(TimestampType.NO_TIMESTAMP_TYPE));
                } else {
                    assertEquals("Timestamp should not change", initialRecords.get(i).timestamp(), record.timestamp());
                    assertFalse(record.hasTimestampType(TimestampType.CREATE_TIME));
                    assertFalse(record.hasTimestampType(TimestampType.NO_TIMESTAMP_TYPE));
                    assertArrayEquals("Headers should not change", initialRecords.get(i).headers(), record.headers());
                }
                i += 1;
            }
        }
        assertEquals(initialOffsets.size(), i);
    }
}
