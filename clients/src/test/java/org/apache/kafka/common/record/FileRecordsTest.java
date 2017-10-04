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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestUtils;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static java.util.Arrays.asList;
import static org.apache.kafka.test.TestUtils.tempFile;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class FileRecordsTest {

    private byte[][] values = new byte[][] {
            "abcd".getBytes(),
            "efgh".getBytes(),
            "ijkl".getBytes()
    };
    private FileRecords fileRecords;
    private Time time;

    @Before
    public void setup() throws IOException {
        this.fileRecords = createFileRecords(values);
        this.time = new MockTime();
    }

    /**
     * Test that the cached size variable matches the actual file size as we append messages
     */
    @Test
    public void testFileSize() throws IOException {
        assertEquals(fileRecords.channel().size(), fileRecords.sizeInBytes());
        for (int i = 0; i < 20; i++) {
            fileRecords.append(MemoryRecords.withRecords(CompressionType.NONE, new SimpleRecord("abcd".getBytes())));
            assertEquals(fileRecords.channel().size(), fileRecords.sizeInBytes());
        }
    }

    /**
     * Test that adding invalid bytes to the end of the log doesn't break iteration
     */
    @Test
    public void testIterationOverPartialAndTruncation() throws IOException {
        testPartialWrite(0, fileRecords);
        testPartialWrite(2, fileRecords);
        testPartialWrite(4, fileRecords);
        testPartialWrite(5, fileRecords);
        testPartialWrite(6, fileRecords);
    }

    private void testPartialWrite(int size, FileRecords fileRecords) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(size);
        for (int i = 0; i < size; i++)
            buffer.put((byte) 0);

        buffer.rewind();

        fileRecords.channel().write(buffer);

        // appending those bytes should not change the contents
        Iterator<Record> records = fileRecords.records().iterator();
        for (byte[] value : values) {
            assertTrue(records.hasNext());
            assertEquals(records.next().value(), ByteBuffer.wrap(value));
        }
    }

    /**
     * Iterating over the file does file reads but shouldn't change the position of the underlying FileChannel.
     */
    @Test
    public void testIterationDoesntChangePosition() throws IOException {
        long position = fileRecords.channel().position();
        Iterator<Record> records = fileRecords.records().iterator();
        for (byte[] value : values) {
            assertTrue(records.hasNext());
            assertEquals(records.next().value(), ByteBuffer.wrap(value));
        }
        assertEquals(position, fileRecords.channel().position());
    }

    /**
     * Test a simple append and read.
     */
    @Test
    public void testRead() throws IOException {
        FileRecords read = fileRecords.read(0, fileRecords.sizeInBytes());
        TestUtils.checkEquals(fileRecords.batches(), read.batches());

        List<RecordBatch> items = batches(read);
        RecordBatch second = items.get(1);

        read = fileRecords.read(second.sizeInBytes(), fileRecords.sizeInBytes());
        assertEquals("Try a read starting from the second message",
                items.subList(1, 3), batches(read));

        read = fileRecords.read(second.sizeInBytes(), second.sizeInBytes());
        assertEquals("Try a read of a single message starting from the second message",
                Collections.singletonList(second), batches(read));
    }

    /**
     * Test the MessageSet.searchFor API.
     */
    @Test
    public void testSearch() throws IOException {
        // append a new message with a high offset
        SimpleRecord lastMessage = new SimpleRecord("test".getBytes());
        fileRecords.append(MemoryRecords.withRecords(50L, CompressionType.NONE, lastMessage));

        List<RecordBatch> batches = batches(fileRecords);
        int position = 0;

        int message1Size = batches.get(0).sizeInBytes();
        assertEquals("Should be able to find the first message by its offset",
                new FileRecords.LogOffsetPosition(0L, position, message1Size),
                fileRecords.searchForOffsetWithSize(0, 0));
        position += message1Size;

        int message2Size = batches.get(1).sizeInBytes();
        assertEquals("Should be able to find second message when starting from 0",
                new FileRecords.LogOffsetPosition(1L, position, message2Size),
                fileRecords.searchForOffsetWithSize(1, 0));
        assertEquals("Should be able to find second message starting from its offset",
                new FileRecords.LogOffsetPosition(1L, position, message2Size),
                fileRecords.searchForOffsetWithSize(1, position));
        position += message2Size + batches.get(2).sizeInBytes();

        int message4Size = batches.get(3).sizeInBytes();
        assertEquals("Should be able to find fourth message from a non-existant offset",
                new FileRecords.LogOffsetPosition(50L, position, message4Size),
                fileRecords.searchForOffsetWithSize(3, position));
        assertEquals("Should be able to find fourth message by correct offset",
                new FileRecords.LogOffsetPosition(50L, position, message4Size),
                fileRecords.searchForOffsetWithSize(50,  position));
    }

    /**
     * Test that the message set iterator obeys start and end slicing
     */
    @Test
    public void testIteratorWithLimits() throws IOException {
        RecordBatch batch = batches(fileRecords).get(1);
        int start = fileRecords.searchForOffsetWithSize(1, 0).position;
        int size = batch.sizeInBytes();
        FileRecords slice = fileRecords.read(start, size);
        assertEquals(Collections.singletonList(batch), batches(slice));
        FileRecords slice2 = fileRecords.read(start, size - 1);
        assertEquals(Collections.emptyList(), batches(slice2));
    }

    /**
     * Test the truncateTo method lops off messages and appropriately updates the size
     */
    @Test
    public void testTruncate() throws IOException {
        RecordBatch batch = batches(fileRecords).get(0);
        int end = fileRecords.searchForOffsetWithSize(1, 0).position;
        fileRecords.truncateTo(end);
        assertEquals(Collections.singletonList(batch), batches(fileRecords));
        assertEquals(batch.sizeInBytes(), fileRecords.sizeInBytes());
    }

    /**
     * Test that truncateTo only calls truncate on the FileChannel if the size of the
     * FileChannel is bigger than the target size. This is important because some JVMs
     * change the mtime of the file, even if truncate should do nothing.
     */
    @Test
    public void testTruncateNotCalledIfSizeIsSameAsTargetSize() throws IOException {
        FileChannel channelMock = EasyMock.createMock(FileChannel.class);

        EasyMock.expect(channelMock.size()).andReturn(42L).atLeastOnce();
        EasyMock.expect(channelMock.position(42L)).andReturn(null);
        EasyMock.replay(channelMock);

        FileRecords fileRecords = new FileRecords(tempFile(), channelMock, 0, Integer.MAX_VALUE, false);
        fileRecords.truncateTo(42);

        EasyMock.verify(channelMock);
    }

    /**
     * Expect a KafkaException if targetSize is bigger than the size of
     * the FileRecords.
     */
    @Test
    public void testTruncateNotCalledIfSizeIsBiggerThanTargetSize() throws IOException {
        FileChannel channelMock = EasyMock.createMock(FileChannel.class);

        EasyMock.expect(channelMock.size()).andReturn(42L).atLeastOnce();
        EasyMock.expect(channelMock.position(42L)).andReturn(null);
        EasyMock.replay(channelMock);

        FileRecords fileRecords = new FileRecords(tempFile(), channelMock, 0, Integer.MAX_VALUE, false);

        try {
            fileRecords.truncateTo(43);
            fail("Should throw KafkaException");
        } catch (KafkaException e) {
            // expected
        }

        EasyMock.verify(channelMock);
    }

    /**
     * see #testTruncateNotCalledIfSizeIsSameAsTargetSize
     */
    @Test
    public void testTruncateIfSizeIsDifferentToTargetSize() throws IOException {
        FileChannel channelMock = EasyMock.createMock(FileChannel.class);

        EasyMock.expect(channelMock.size()).andReturn(42L).atLeastOnce();
        EasyMock.expect(channelMock.position(42L)).andReturn(null).once();
        EasyMock.expect(channelMock.truncate(23L)).andReturn(null).once();
        EasyMock.replay(channelMock);

        FileRecords fileRecords = new FileRecords(tempFile(), channelMock, 0, Integer.MAX_VALUE, false);
        fileRecords.truncateTo(23);

        EasyMock.verify(channelMock);
    }

    /**
     * Test the new FileRecords with pre allocate as true
     */
    @Test
    public void testPreallocateTrue() throws IOException {
        File temp = tempFile();
        FileRecords fileRecords = FileRecords.open(temp, false, 512 * 1024 * 1024, true);
        long position = fileRecords.channel().position();
        int size = fileRecords.sizeInBytes();
        assertEquals(0, position);
        assertEquals(0, size);
        assertEquals(512 * 1024 * 1024, temp.length());
    }

    /**
     * Test the new FileRecords with pre allocate as false
     */
    @Test
    public void testPreallocateFalse() throws IOException {
        File temp = tempFile();
        FileRecords set = FileRecords.open(temp, false, 512 * 1024 * 1024, false);
        long position = set.channel().position();
        int size = set.sizeInBytes();
        assertEquals(0, position);
        assertEquals(0, size);
        assertEquals(0, temp.length());
    }

    /**
     * Test the new FileRecords with pre allocate as true and file has been clearly shut down, the file will be truncate to end of valid data.
     */
    @Test
    public void testPreallocateClearShutdown() throws IOException {
        File temp = tempFile();
        FileRecords fileRecords = FileRecords.open(temp, false, 512 * 1024 * 1024, true);
        append(fileRecords, values);

        int oldPosition = (int) fileRecords.channel().position();
        int oldSize = fileRecords.sizeInBytes();
        assertEquals(this.fileRecords.sizeInBytes(), oldPosition);
        assertEquals(this.fileRecords.sizeInBytes(), oldSize);
        fileRecords.close();

        File tempReopen = new File(temp.getAbsolutePath());
        FileRecords setReopen = FileRecords.open(tempReopen, true, 512 * 1024 * 1024, true);
        int position = (int) setReopen.channel().position();
        int size = setReopen.sizeInBytes();

        assertEquals(oldPosition, position);
        assertEquals(oldPosition, size);
        assertEquals(oldPosition, tempReopen.length());
    }

    @Test
    public void testFormatConversionWithPartialMessage() throws IOException {
        RecordBatch batch = batches(fileRecords).get(1);
        int start = fileRecords.searchForOffsetWithSize(1, 0).position;
        int size = batch.sizeInBytes();
        FileRecords slice = fileRecords.read(start, size - 1);
        Records messageV0 = slice.downConvert(RecordBatch.MAGIC_VALUE_V0, 0, time).records();
        assertTrue("No message should be there", batches(messageV0).isEmpty());
        assertEquals("There should be " + (size - 1) + " bytes", size - 1, messageV0.sizeInBytes());
    }

    @Test
    public void testConversion() throws IOException {
        doTestConversion(CompressionType.NONE, RecordBatch.MAGIC_VALUE_V0);
        doTestConversion(CompressionType.GZIP, RecordBatch.MAGIC_VALUE_V0);
        doTestConversion(CompressionType.NONE, RecordBatch.MAGIC_VALUE_V1);
        doTestConversion(CompressionType.GZIP, RecordBatch.MAGIC_VALUE_V1);
        doTestConversion(CompressionType.NONE, RecordBatch.MAGIC_VALUE_V2);
        doTestConversion(CompressionType.GZIP, RecordBatch.MAGIC_VALUE_V2);
    }

    private void doTestConversion(CompressionType compressionType, byte toMagic) throws IOException {
        List<Long> offsets = asList(0L, 2L, 3L, 9L, 11L, 15L, 16L, 17L, 22L, 24L);
        List<SimpleRecord> records = asList(
                new SimpleRecord(1L, "k1".getBytes(), "hello".getBytes()),
                new SimpleRecord(2L, "k2".getBytes(), "goodbye".getBytes()),
                new SimpleRecord(3L, "k3".getBytes(), "hello again".getBytes()),
                new SimpleRecord(4L, "k4".getBytes(), "goodbye for now".getBytes()),
                new SimpleRecord(5L, "k5".getBytes(), "hello again".getBytes()),
                new SimpleRecord(6L, "k6".getBytes(), "I sense indecision".getBytes()),
                new SimpleRecord(7L, "k7".getBytes(), "what now".getBytes()),
                new SimpleRecord(8L, "k8".getBytes(), "running out".getBytes()),
                new SimpleRecord(9L, "k9".getBytes(), "ok, almost done".getBytes()),
                new SimpleRecord(10L, "k10".getBytes(), "finally".getBytes()));

        ByteBuffer buffer = ByteBuffer.allocate(1024);
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, RecordBatch.MAGIC_VALUE_V0, compressionType,
                TimestampType.CREATE_TIME, 0L);
        for (int i = 0; i < 3; i++)
            builder.appendWithOffset(offsets.get(i), records.get(i));
        builder.close();

        builder = MemoryRecords.builder(buffer, RecordBatch.MAGIC_VALUE_V1, compressionType, TimestampType.CREATE_TIME,
                0L);
        for (int i = 3; i < 6; i++)
            builder.appendWithOffset(offsets.get(i), records.get(i));
        builder.close();

        builder = MemoryRecords.builder(buffer, RecordBatch.MAGIC_VALUE_V2, compressionType, TimestampType.CREATE_TIME, 0L);
        for (int i = 6; i < 10; i++)
            builder.appendWithOffset(offsets.get(i), records.get(i));
        builder.close();

        buffer.flip();

        try (FileRecords fileRecords = FileRecords.open(tempFile())) {
            fileRecords.append(MemoryRecords.readableRecords(buffer));
            fileRecords.flush();
            Records convertedRecords = fileRecords.downConvert(toMagic, 0L, time).records();
            verifyConvertedRecords(records, offsets, convertedRecords, compressionType, toMagic);

            if (toMagic <= RecordBatch.MAGIC_VALUE_V1 && compressionType == CompressionType.NONE) {
                long firstOffset;
                if (toMagic == RecordBatch.MAGIC_VALUE_V0)
                    firstOffset = 11L; // v1 record
                else
                    firstOffset = 17; // v2 record
                Records convertedRecords2 = fileRecords.downConvert(toMagic, firstOffset, time).records();
                List<Long> filteredOffsets = new ArrayList<>(offsets);
                List<SimpleRecord> filteredRecords = new ArrayList<>(records);
                int index = filteredOffsets.indexOf(firstOffset) - 1;
                filteredRecords.remove(index);
                filteredOffsets.remove(index);
                verifyConvertedRecords(filteredRecords, filteredOffsets, convertedRecords2, compressionType, toMagic);
            } else {
                // firstOffset doesn't have any effect in this case
                Records convertedRecords2 = fileRecords.downConvert(toMagic, 10L, time).records();
                verifyConvertedRecords(records, offsets, convertedRecords2, compressionType, toMagic);
            }
        }
    }

    private String utf8(ByteBuffer buffer) {
        return Utils.utf8(buffer, buffer.remaining());
    }

    private void verifyConvertedRecords(List<SimpleRecord> initialRecords,
                                        List<Long> initialOffsets,
                                        Records convertedRecords,
                                        CompressionType compressionType,
                                        byte magicByte) {
        int i = 0;
        for (RecordBatch batch : convertedRecords.batches()) {
            assertTrue("Magic byte should be lower than or equal to " + magicByte, batch.magic() <= magicByte);
            if (batch.magic() == RecordBatch.MAGIC_VALUE_V0)
                assertEquals(TimestampType.NO_TIMESTAMP_TYPE, batch.timestampType());
            else
                assertEquals(TimestampType.CREATE_TIME, batch.timestampType());
            assertEquals("Compression type should not be affected by conversion", compressionType, batch.compressionType());
            for (Record record : batch) {
                assertTrue("Inner record should have magic " + magicByte, record.hasMagic(batch.magic()));
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
                }
                i += 1;
            }
        }
        assertEquals(initialOffsets.size(), i);
    }

    private static List<RecordBatch> batches(Records buffer) {
        return TestUtils.toList(buffer.batches());
    }

    private FileRecords createFileRecords(byte[][] values) throws IOException {
        FileRecords fileRecords = FileRecords.open(tempFile());
        append(fileRecords, values);
        return fileRecords;
    }

    private void append(FileRecords fileRecords, byte[][] values) throws IOException {
        long offset = 0L;
        for (byte[] value : values) {
            ByteBuffer buffer = ByteBuffer.allocate(128);
            MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, RecordBatch.CURRENT_MAGIC_VALUE,
                    CompressionType.NONE, TimestampType.CREATE_TIME, offset);
            builder.appendWithOffset(offset++, System.currentTimeMillis(), null, value);
            fileRecords.append(builder.build());
        }
        fileRecords.flush();
    }

}
