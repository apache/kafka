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
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.network.TransferableChannel;
import org.apache.kafka.common.utils.Time;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

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
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

    private static Collection<Arguments> parameters() {
        List<Arguments> arguments = new ArrayList<>();
        for (byte toMagic = RecordBatch.MAGIC_VALUE_V0; toMagic <= RecordBatch.CURRENT_MAGIC_VALUE; toMagic++) {
            for (boolean overflow : asList(true, false)) {
                arguments.add(Arguments.of(CompressionType.NONE, toMagic, overflow));
                arguments.add(Arguments.of(CompressionType.GZIP, toMagic, overflow));
            }
        }
        return arguments;
    }

    /**
     * Test the lazy down-conversion path.
     *
     * If `overflow` is true, the number of bytes we want to convert is much larger
     * than the number of bytes we get after conversion. This causes overflow message batch(es) to be appended towards the
     * end of the converted output.
     */
    @ParameterizedTest
    @MethodSource("parameters")
    public void testConversion(CompressionType compressionType, byte toMagic, boolean overflow) throws IOException {
        doTestConversion(compressionType, toMagic, overflow);
    }

    private void doTestConversion(CompressionType compressionType, byte toMagic, boolean testConversionOverflow) throws IOException {
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
        assertEquals(offsets.size(), records.size(), "incorrect test setup");

        ByteBuffer buffer = ByteBuffer.allocate(1024);
        Compression compression = Compression.of(compressionType).build();
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, RecordBatch.CURRENT_MAGIC_VALUE, compression,
                TimestampType.CREATE_TIME, 0L);
        for (int i = 0; i < 3; i++)
            builder.appendWithOffset(offsets.get(i), records.get(i));
        builder.close();

        builder = MemoryRecords.builder(buffer, RecordBatch.CURRENT_MAGIC_VALUE, compression, TimestampType.CREATE_TIME,
                0L);
        for (int i = 3; i < 6; i++)
            builder.appendWithOffset(offsets.get(i), records.get(i));
        builder.close();

        builder = MemoryRecords.builder(buffer, RecordBatch.CURRENT_MAGIC_VALUE, compression, TimestampType.CREATE_TIME,
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

    private static MemoryRecords convertRecords(MemoryRecords recordsToConvert, byte toMagic, int bytesToConvert) throws IOException {
        try (FileRecords inputRecords = FileRecords.open(tempFile())) {
            inputRecords.append(recordsToConvert);
            inputRecords.flush();

            LazyDownConversionRecords lazyRecords = new LazyDownConversionRecords(new TopicPartition("test", 1),
                    inputRecords, toMagic, 0L, Time.SYSTEM);
            LazyDownConversionRecordsSend lazySend = lazyRecords.toSend();
            File outputFile = tempFile();
            ByteBuffer convertedRecordsBuffer;
            try (TransferableChannel channel = toTransferableChannel(FileChannel.open(outputFile.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE))) {
                int written = 0;
                while (written < bytesToConvert) written += lazySend.writeTo(channel, written, bytesToConvert - written);
                try (FileRecords convertedRecords = FileRecords.open(outputFile, true, written, false)) {
                    convertedRecordsBuffer = ByteBuffer.allocate(convertedRecords.sizeInBytes());
                    convertedRecords.readInto(convertedRecordsBuffer, 0);
                }
            }
            return MemoryRecords.readableRecords(convertedRecordsBuffer);
        }
    }

    private static TransferableChannel toTransferableChannel(FileChannel channel) {
        return new TransferableChannel() {

            @Override
            public boolean hasPendingWrites() {
                return false;
            }

            @Override
            public long transferFrom(FileChannel fileChannel, long position, long count) throws IOException {
                return fileChannel.transferTo(position, count, channel);
            }

            @Override
            public boolean isOpen() {
                return channel.isOpen();
            }

            @Override
            public void close() throws IOException {
                channel.close();
            }

            @Override
            public int write(ByteBuffer src) throws IOException {
                return channel.write(src);
            }

            @Override
            public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
                return channel.write(srcs, offset, length);
            }

            @Override
            public long write(ByteBuffer[] srcs) throws IOException {
                return channel.write(srcs);
            }
        };
    }

    private static void verifyDownConvertedRecords(List<SimpleRecord> initialRecords,
                                                   List<Long> initialOffsets,
                                                   MemoryRecords downConvertedRecords,
                                                   CompressionType compressionType,
                                                   byte toMagic) {
        int i = 0;
        for (RecordBatch batch : downConvertedRecords.batches()) {
            assertTrue(batch.magic() <= toMagic, "Magic byte should be lower than or equal to " + toMagic);
            if (batch.magic() == RecordBatch.MAGIC_VALUE_V0)
                assertEquals(TimestampType.NO_TIMESTAMP_TYPE, batch.timestampType());
            else
                assertEquals(TimestampType.CREATE_TIME, batch.timestampType());
            assertEquals(compressionType, batch.compressionType(), "Compression type should not be affected by conversion");
            for (Record record : batch) {
                assertTrue(record.hasMagic(batch.magic()), "Inner record should have magic " + toMagic);
                assertEquals(initialOffsets.get(i).longValue(), record.offset(), "Offset should not change");
                assertEquals(utf8(initialRecords.get(i).key()), utf8(record.key()), "Key should not change");
                assertEquals(utf8(initialRecords.get(i).value()), utf8(record.value()), "Value should not change");
                assertFalse(record.hasTimestampType(TimestampType.LOG_APPEND_TIME));
                if (batch.magic() == RecordBatch.MAGIC_VALUE_V0) {
                    assertEquals(RecordBatch.NO_TIMESTAMP, record.timestamp());
                    assertFalse(record.hasTimestampType(TimestampType.CREATE_TIME));
                    assertTrue(record.hasTimestampType(TimestampType.NO_TIMESTAMP_TYPE));
                } else if (batch.magic() == RecordBatch.MAGIC_VALUE_V1) {
                    assertEquals(initialRecords.get(i).timestamp(), record.timestamp(), "Timestamp should not change");
                    assertTrue(record.hasTimestampType(TimestampType.CREATE_TIME));
                    assertFalse(record.hasTimestampType(TimestampType.NO_TIMESTAMP_TYPE));
                } else {
                    assertEquals(initialRecords.get(i).timestamp(), record.timestamp(), "Timestamp should not change");
                    assertFalse(record.hasTimestampType(TimestampType.CREATE_TIME));
                    assertFalse(record.hasTimestampType(TimestampType.NO_TIMESTAMP_TYPE));
                    assertArrayEquals(initialRecords.get(i).headers(), record.headers(), "Headers should not change");
                }
                i += 1;
            }
        }
        assertEquals(initialOffsets.size(), i);
    }
}
