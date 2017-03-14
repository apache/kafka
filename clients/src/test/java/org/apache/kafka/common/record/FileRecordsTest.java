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
import org.apache.kafka.test.TestUtils;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.apache.kafka.test.TestUtils.tempFile;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class FileRecordsTest {

    private byte[][] values = new byte[][] {
            "abcd".getBytes(),
            "efgh".getBytes(),
            "ijkl".getBytes()
    };
    private FileRecords fileRecords;

    @Before
    public void setup() throws IOException {
        this.fileRecords = createFileRecords(values);
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
        Iterator<Record> logRecords = fileRecords.records().iterator();
        for (byte[] value : values) {
            assertTrue(logRecords.hasNext());
            assertEquals(logRecords.next().value(), ByteBuffer.wrap(value));
        }
    }

    /**
     * Iterating over the file does file reads but shouldn't change the position of the underlying FileChannel.
     */
    @Test
    public void testIterationDoesntChangePosition() throws IOException {
        long position = fileRecords.channel().position();
        Iterator<Record> logRecords = fileRecords.records().iterator();
        for (byte[] value : values) {
            assertTrue(logRecords.hasNext());
            assertEquals(logRecords.next().value(), ByteBuffer.wrap(value));
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

        List<RecordBatch> items = shallowBatches(read);
        RecordBatch second = items.get(1);

        read = fileRecords.read(second.sizeInBytes(), fileRecords.sizeInBytes());
        assertEquals("Try a read starting from the second message",
                items.subList(1, 3), shallowBatches(read));

        read = fileRecords.read(second.sizeInBytes(), second.sizeInBytes());
        assertEquals("Try a read of a single message starting from the second message",
                Collections.singletonList(second), shallowBatches(read));
    }

    /**
     * Test the MessageSet.searchFor API.
     */
    @Test
    public void testSearch() throws IOException {
        // append a new message with a high offset
        SimpleRecord lastMessage = new SimpleRecord("test".getBytes());
        fileRecords.append(MemoryRecords.withRecords(50L, CompressionType.NONE, lastMessage));

        List<RecordBatch> entries = shallowBatches(fileRecords);
        int position = 0;

        int message1Size = entries.get(0).sizeInBytes();
        assertEquals("Should be able to find the first message by its offset",
                new FileRecords.LogEntryPosition(0L, position, message1Size),
                fileRecords.searchForOffsetWithSize(0, 0));
        position += message1Size;

        int message2Size = entries.get(1).sizeInBytes();
        assertEquals("Should be able to find second message when starting from 0",
                new FileRecords.LogEntryPosition(1L, position, message2Size),
                fileRecords.searchForOffsetWithSize(1, 0));
        assertEquals("Should be able to find second message starting from its offset",
                new FileRecords.LogEntryPosition(1L, position, message2Size),
                fileRecords.searchForOffsetWithSize(1, position));
        position += message2Size + entries.get(2).sizeInBytes();

        int message4Size = entries.get(3).sizeInBytes();
        assertEquals("Should be able to find fourth message from a non-existant offset",
                new FileRecords.LogEntryPosition(50L, position, message4Size),
                fileRecords.searchForOffsetWithSize(3, position));
        assertEquals("Should be able to find fourth message by correct offset",
                new FileRecords.LogEntryPosition(50L, position, message4Size),
                fileRecords.searchForOffsetWithSize(50,  position));
    }

    /**
     * Test that the message set iterator obeys start and end slicing
     */
    @Test
    public void testIteratorWithLimits() throws IOException {
        RecordBatch entry = shallowBatches(fileRecords).get(1);
        int start = fileRecords.searchForOffsetWithSize(1, 0).position;
        int size = entry.sizeInBytes();
        FileRecords slice = fileRecords.read(start, size);
        assertEquals(Collections.singletonList(entry), shallowBatches(slice));
        FileRecords slice2 = fileRecords.read(start, size - 1);
        assertEquals(Collections.emptyList(), shallowBatches(slice2));
    }

    /**
     * Test the truncateTo method lops off messages and appropriately updates the size
     */
    @Test
    public void testTruncate() throws IOException {
        RecordBatch entry = shallowBatches(fileRecords).get(0);
        int end = fileRecords.searchForOffsetWithSize(1, 0).position;
        fileRecords.truncateTo(end);
        assertEquals(Collections.singletonList(entry), shallowBatches(fileRecords));
        assertEquals(entry.sizeInBytes(), fileRecords.sizeInBytes());
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
        EasyMock.expect(channelMock.position(23L)).andReturn(null).once();
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
        RecordBatch entry = shallowBatches(fileRecords).get(1);
        int start = fileRecords.searchForOffsetWithSize(1, 0).position;
        int size = entry.sizeInBytes();
        FileRecords slice = fileRecords.read(start, size - 1);
        Records messageV0 = slice.downConvert(RecordBatch.MAGIC_VALUE_V0);
        assertTrue("No message should be there", shallowBatches(messageV0).isEmpty());
        assertEquals("There should be " + (size - 1) + " bytes", size - 1, messageV0.sizeInBytes());
    }

    @Test
    public void testConvertNonCompressedToMagic0() throws IOException {
        List<Long> offsets = Arrays.asList(0L, 2L);
        List<SimpleRecord> records = Arrays.asList(
                new SimpleRecord(1L, "k1".getBytes(), "hello".getBytes()),
                new SimpleRecord(2L, "k2".getBytes(), "goodbye".getBytes()));

        ByteBuffer buffer = ByteBuffer.allocate(512);
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, RecordBatch.MAGIC_VALUE_V1, CompressionType.NONE,
                TimestampType.CREATE_TIME, 0L);
        for (int i = 0; i < offsets.size(); i++)
            builder.appendWithOffset(offsets.get(i), records.get(i));

        // down conversion for non-compressed messages
        try (FileRecords fileRecords = FileRecords.open(tempFile())) {
            fileRecords.append(builder.build());
            fileRecords.flush();
            Records convertedRecords = fileRecords.downConvert(RecordBatch.MAGIC_VALUE_V0);
            verifyConvertedMessageSet(records, offsets, convertedRecords, RecordBatch.MAGIC_VALUE_V0);
        }
    }

    @Test
    public void testConvertCompressedToMagic0() throws IOException {
        List<Long> offsets = Arrays.asList(0L, 2L);
        List<SimpleRecord> records = Arrays.asList(
                new SimpleRecord(1L, "k1".getBytes(), "hello".getBytes()),
                new SimpleRecord(2L, "k2".getBytes(), "goodbye".getBytes()));

        ByteBuffer buffer = ByteBuffer.allocate(512);
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, RecordBatch.MAGIC_VALUE_V1, CompressionType.GZIP,
                TimestampType.CREATE_TIME, 0L);
        for (int i = 0; i < offsets.size(); i++)
            builder.appendWithOffset(offsets.get(i), records.get(i));

        // down conversion for compressed messages
        try (FileRecords fileRecords = FileRecords.open(tempFile())) {
            fileRecords.append(builder.build());
            fileRecords.flush();
            Records convertedRecords = fileRecords.downConvert(RecordBatch.MAGIC_VALUE_V0);
            verifyConvertedMessageSet(records, offsets, convertedRecords, RecordBatch.MAGIC_VALUE_V0);
        }
    }

    private void verifyConvertedMessageSet(List<SimpleRecord> initialRecords,
                                           List<Long> initialOffsets,
                                           Records convertedRecords,
                                           byte magicByte) {
        int i = 0;
        for (RecordBatch batch : convertedRecords.batches()) {
            assertEquals("magic byte should be " + magicByte, magicByte, batch.magic());
            for (Record record : batch) {
                assertTrue("Inner record should have magic " + magicByte, record.hasMagic(magicByte));
                assertEquals("offset should not change", initialOffsets.get(i).longValue(), record.offset());
                assertEquals("key should not change", initialRecords.get(i).key(), record.key());
                assertEquals("value should not change", initialRecords.get(i).value(), record.value());
                i += 1;
            }
        }
    }

    private static List<RecordBatch> shallowBatches(Records buffer) {
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
