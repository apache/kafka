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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.network.TransferableChannel;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static java.util.Arrays.asList;
import static org.apache.kafka.common.utils.Utils.utf8;
import static org.apache.kafka.test.TestUtils.tempFile;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class FileRecordsTest {

    private byte[][] values = new byte[][] {
            "abcd".getBytes(),
            "efgh".getBytes(),
            "ijkl".getBytes()
    };
    private FileRecords fileRecords;
    private Time time;

    @BeforeEach
    public void setup() throws IOException {
        this.fileRecords = createFileRecords(values);
        this.time = new MockTime();
    }

    @AfterEach
    public void cleanup() throws IOException {
        this.fileRecords.close();
    }

    @Test
    public void testAppendProtectsFromOverflow() throws Exception {
        File fileMock = mock(File.class);
        FileChannel fileChannelMock = mock(FileChannel.class);
        when(fileChannelMock.size()).thenReturn((long) Integer.MAX_VALUE);

        FileRecords records = new FileRecords(fileMock, fileChannelMock, 0, Integer.MAX_VALUE, false);
        assertThrows(IllegalArgumentException.class, () -> append(records, values));
    }

    @Test
    public void testOpenOversizeFile() throws Exception {
        File fileMock = mock(File.class);
        FileChannel fileChannelMock = mock(FileChannel.class);
        when(fileChannelMock.size()).thenReturn(Integer.MAX_VALUE + 5L);

        assertThrows(KafkaException.class, () -> new FileRecords(fileMock, fileChannelMock, 0, Integer.MAX_VALUE, false));
    }

    @Test
    public void testOutOfRangeSlice() {
        assertThrows(IllegalArgumentException.class,
            () -> this.fileRecords.slice(fileRecords.sizeInBytes() + 1, 15).sizeInBytes());
    }

    /**
     * Test that the cached size variable matches the actual file size as we append messages
     */
    @Test
    public void testFileSize() throws IOException {
        assertEquals(fileRecords.channel().size(), fileRecords.sizeInBytes());
        for (int i = 0; i < 20; i++) {
            fileRecords.append(MemoryRecords.withRecords(CompressionConfig.NONE, new SimpleRecord("abcd".getBytes())));
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

    @Test
    public void testSliceSizeLimitWithConcurrentWrite() throws Exception {
        FileRecords log = FileRecords.open(tempFile());
        ExecutorService executor = Executors.newFixedThreadPool(2);
        int maxSizeInBytes = 16384;

        try {
            Future<Object> readerCompletion = executor.submit(() -> {
                while (log.sizeInBytes() < maxSizeInBytes) {
                    int currentSize = log.sizeInBytes();
                    FileRecords slice = log.slice(0, currentSize);
                    assertEquals(currentSize, slice.sizeInBytes());
                }
                return null;
            });

            Future<Object> writerCompletion = executor.submit(() -> {
                while (log.sizeInBytes() < maxSizeInBytes) {
                    append(log, values);
                }
                return null;
            });

            writerCompletion.get();
            readerCompletion.get();
        } finally {
            executor.shutdownNow();
        }
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
        FileRecords read = fileRecords.slice(0, fileRecords.sizeInBytes());
        assertEquals(fileRecords.sizeInBytes(), read.sizeInBytes());
        TestUtils.checkEquals(fileRecords.batches(), read.batches());

        List<RecordBatch> items = batches(read);
        RecordBatch first = items.get(0);

        // read from second message until the end
        read = fileRecords.slice(first.sizeInBytes(), fileRecords.sizeInBytes() - first.sizeInBytes());
        assertEquals(fileRecords.sizeInBytes() - first.sizeInBytes(), read.sizeInBytes());
        assertEquals(items.subList(1, items.size()), batches(read), "Read starting from the second message");

        // read from second message and size is past the end of the file
        read = fileRecords.slice(first.sizeInBytes(), fileRecords.sizeInBytes());
        assertEquals(fileRecords.sizeInBytes() - first.sizeInBytes(), read.sizeInBytes());
        assertEquals(items.subList(1, items.size()), batches(read), "Read starting from the second message");

        // read from second message and position + size overflows
        read = fileRecords.slice(first.sizeInBytes(), Integer.MAX_VALUE);
        assertEquals(fileRecords.sizeInBytes() - first.sizeInBytes(), read.sizeInBytes());
        assertEquals(items.subList(1, items.size()), batches(read), "Read starting from the second message");

        // read from second message and size is past the end of the file on a view/slice
        read = fileRecords.slice(1, fileRecords.sizeInBytes() - 1)
                .slice(first.sizeInBytes() - 1, fileRecords.sizeInBytes());
        assertEquals(fileRecords.sizeInBytes() - first.sizeInBytes(), read.sizeInBytes());
        assertEquals(items.subList(1, items.size()), batches(read), "Read starting from the second message");

        // read from second message and position + size overflows on a view/slice
        read = fileRecords.slice(1, fileRecords.sizeInBytes() - 1)
                .slice(first.sizeInBytes() - 1, Integer.MAX_VALUE);
        assertEquals(fileRecords.sizeInBytes() - first.sizeInBytes(), read.sizeInBytes());
        assertEquals(items.subList(1, items.size()), batches(read), "Read starting from the second message");

        // read a single message starting from second message
        RecordBatch second = items.get(1);
        read = fileRecords.slice(first.sizeInBytes(), second.sizeInBytes());
        assertEquals(second.sizeInBytes(), read.sizeInBytes());
        assertEquals(Collections.singletonList(second), batches(read), "Read a single message starting from the second message");
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
        assertEquals(new FileRecords.LogOffsetPosition(0L, position, message1Size),
            fileRecords.searchForOffsetWithSize(0, 0),
            "Should be able to find the first message by its offset");
        position += message1Size;

        int message2Size = batches.get(1).sizeInBytes();
        assertEquals(new FileRecords.LogOffsetPosition(1L, position, message2Size),
            fileRecords.searchForOffsetWithSize(1, 0),
            "Should be able to find second message when starting from 0");
        assertEquals(new FileRecords.LogOffsetPosition(1L, position, message2Size),
            fileRecords.searchForOffsetWithSize(1, position),
            "Should be able to find second message starting from its offset");
        position += message2Size + batches.get(2).sizeInBytes();

        int message4Size = batches.get(3).sizeInBytes();
        assertEquals(new FileRecords.LogOffsetPosition(50L, position, message4Size),
            fileRecords.searchForOffsetWithSize(3, position),
            "Should be able to find fourth message from a non-existent offset");
        assertEquals(new FileRecords.LogOffsetPosition(50L, position, message4Size),
            fileRecords.searchForOffsetWithSize(50,  position),
            "Should be able to find fourth message by correct offset");
    }

    /**
     * Test that the message set iterator obeys start and end slicing
     */
    @Test
    public void testIteratorWithLimits() throws IOException {
        RecordBatch batch = batches(fileRecords).get(1);
        int start = fileRecords.searchForOffsetWithSize(1, 0).position;
        int size = batch.sizeInBytes();
        FileRecords slice = fileRecords.slice(start, size);
        assertEquals(Collections.singletonList(batch), batches(slice));
        FileRecords slice2 = fileRecords.slice(start, size - 1);
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
        FileChannel channelMock = mock(FileChannel.class);

        when(channelMock.size()).thenReturn(42L);
        when(channelMock.position(42L)).thenReturn(null);

        FileRecords fileRecords = new FileRecords(tempFile(), channelMock, 0, Integer.MAX_VALUE, false);
        fileRecords.truncateTo(42);

        verify(channelMock, atLeastOnce()).size();
        verify(channelMock, times(0)).truncate(anyLong());
    }

    /**
     * Expect a KafkaException if targetSize is bigger than the size of
     * the FileRecords.
     */
    @Test
    public void testTruncateNotCalledIfSizeIsBiggerThanTargetSize() throws IOException {
        FileChannel channelMock = mock(FileChannel.class);

        when(channelMock.size()).thenReturn(42L);

        FileRecords fileRecords = new FileRecords(tempFile(), channelMock, 0, Integer.MAX_VALUE, false);

        try {
            fileRecords.truncateTo(43);
            fail("Should throw KafkaException");
        } catch (KafkaException e) {
            // expected
        }

        verify(channelMock, atLeastOnce()).size();
    }

    /**
     * see #testTruncateNotCalledIfSizeIsSameAsTargetSize
     */
    @Test
    public void testTruncateIfSizeIsDifferentToTargetSize() throws IOException {
        FileChannel channelMock = mock(FileChannel.class);

        when(channelMock.size()).thenReturn(42L);
        when(channelMock.truncate(anyLong())).thenReturn(channelMock);

        FileRecords fileRecords = new FileRecords(tempFile(), channelMock, 0, Integer.MAX_VALUE, false);
        fileRecords.truncateTo(23);

        verify(channelMock, atLeastOnce()).size();
        verify(channelMock).truncate(23);
    }

    /**
     * Test the new FileRecords with pre allocate as true
     */
    @Test
    public void testPreallocateTrue() throws IOException {
        File temp = tempFile();
        FileRecords fileRecords = FileRecords.open(temp, false, 1024 * 1024, true);
        long position = fileRecords.channel().position();
        int size = fileRecords.sizeInBytes();
        assertEquals(0, position);
        assertEquals(0, size);
        assertEquals(1024 * 1024, temp.length());
    }

    /**
     * Test the new FileRecords with pre allocate as false
     */
    @Test
    public void testPreallocateFalse() throws IOException {
        File temp = tempFile();
        FileRecords set = FileRecords.open(temp, false, 1024 * 1024, false);
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
        FileRecords fileRecords = FileRecords.open(temp, false, 1024 * 1024, true);
        append(fileRecords, values);

        int oldPosition = (int) fileRecords.channel().position();
        int oldSize = fileRecords.sizeInBytes();
        assertEquals(this.fileRecords.sizeInBytes(), oldPosition);
        assertEquals(this.fileRecords.sizeInBytes(), oldSize);
        fileRecords.close();

        File tempReopen = new File(temp.getAbsolutePath());
        FileRecords setReopen = FileRecords.open(tempReopen, true, 1024 * 1024, true);
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
        FileRecords slice = fileRecords.slice(start, size - 1);
        Records messageV0 = slice.downConvert(RecordBatch.MAGIC_VALUE_V0, 0, time).records();
        assertTrue(batches(messageV0).isEmpty(), "No message should be there");
        assertEquals(size - 1, messageV0.sizeInBytes(), "There should be " + (size - 1) + " bytes");

        // Lazy down-conversion will not return any messages for a partial input batch
        TopicPartition tp = new TopicPartition("topic-1", 0);
        LazyDownConversionRecords lazyRecords = new LazyDownConversionRecords(tp, slice, RecordBatch.MAGIC_VALUE_V0, 0, Time.SYSTEM);
        Iterator<ConvertedRecords<?>> it = lazyRecords.iterator(16 * 1024L);
        assertFalse(it.hasNext(), "No messages should be returned");
    }

    @Test
    public void testFormatConversionWithNoMessages() throws IOException {
        TopicPartition tp = new TopicPartition("topic-1", 0);
        LazyDownConversionRecords lazyRecords = new LazyDownConversionRecords(tp, MemoryRecords.EMPTY, RecordBatch.MAGIC_VALUE_V0,
            0, Time.SYSTEM);
        assertEquals(0, lazyRecords.sizeInBytes());
        Iterator<ConvertedRecords<?>> it = lazyRecords.iterator(16 * 1024L);
        assertFalse(it.hasNext(), "No messages should be returned");
    }

    @Test
    public void testSearchForTimestamp() throws IOException {
        for (RecordVersion version : RecordVersion.values()) {
            testSearchForTimestamp(version);
        }
    }

    private void testSearchForTimestamp(RecordVersion version) throws IOException {
        File temp = tempFile();
        FileRecords fileRecords = FileRecords.open(temp, false, 1024 * 1024, true);
        appendWithOffsetAndTimestamp(fileRecords, version, 10L, 5, 0);
        appendWithOffsetAndTimestamp(fileRecords, version, 11L, 6, 1);

        assertFoundTimestamp(new FileRecords.TimestampAndOffset(10L, 5, Optional.of(0)),
                fileRecords.searchForTimestamp(9L, 0, 0L), version);
        assertFoundTimestamp(new FileRecords.TimestampAndOffset(10L, 5, Optional.of(0)),
                fileRecords.searchForTimestamp(10L, 0, 0L), version);
        assertFoundTimestamp(new FileRecords.TimestampAndOffset(11L, 6, Optional.of(1)),
                fileRecords.searchForTimestamp(11L, 0, 0L), version);
        assertNull(fileRecords.searchForTimestamp(12L, 0, 0L));
    }

    private void assertFoundTimestamp(FileRecords.TimestampAndOffset expected,
                                      FileRecords.TimestampAndOffset actual,
                                      RecordVersion version) {
        if (version == RecordVersion.V0) {
            assertNull(actual, "Expected no match for message format v0");
        } else {
            assertNotNull(actual, "Expected to find timestamp for message format " + version);
            assertEquals(expected.timestamp, actual.timestamp, "Expected matching timestamps for message format" + version);
            assertEquals(expected.offset, actual.offset, "Expected matching offsets for message format " + version);
            Optional<Integer> expectedLeaderEpoch = version.value >= RecordVersion.V2.value ?
                    expected.leaderEpoch : Optional.empty();
            assertEquals(expectedLeaderEpoch, actual.leaderEpoch, "Non-matching leader epoch for version " + version);
        }
    }

    private void appendWithOffsetAndTimestamp(FileRecords fileRecords,
                                              RecordVersion recordVersion,
                                              long timestamp,
                                              long offset,
                                              int leaderEpoch) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(128);
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, recordVersion.value,
                CompressionConfig.NONE, TimestampType.CREATE_TIME, offset, timestamp, leaderEpoch);
        builder.append(new SimpleRecord(timestamp, new byte[0], new byte[0]));
        fileRecords.append(builder.build());
    }

    @Test
    public void testDownconversionAfterMessageFormatDowngrade() throws IOException {
        // random bytes
        Random random = new Random();
        byte[] bytes = new byte[3000];
        random.nextBytes(bytes);

        // records
        CompressionConfig compressionConfig = CompressionConfig.gzip().build();
        List<Long> offsets = asList(0L, 1L);
        List<Byte> magic = asList(RecordBatch.MAGIC_VALUE_V2, RecordBatch.MAGIC_VALUE_V1);  // downgrade message format from v2 to v1
        List<SimpleRecord> records = asList(
                new SimpleRecord(1L, "k1".getBytes(), bytes),
                new SimpleRecord(2L, "k2".getBytes(), bytes));
        byte toMagic = 1;

        // create MemoryRecords
        ByteBuffer buffer = ByteBuffer.allocate(8000);
        for (int i = 0; i < records.size(); i++) {
            MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, magic.get(i), compressionConfig, TimestampType.CREATE_TIME, 0L);
            builder.appendWithOffset(offsets.get(i), records.get(i));
            builder.close();
        }
        buffer.flip();

        // create FileRecords, down-convert and verify
        try (FileRecords fileRecords = FileRecords.open(tempFile())) {
            fileRecords.append(MemoryRecords.readableRecords(buffer));
            fileRecords.flush();
            downConvertAndVerifyRecords(records, offsets, fileRecords, compressionConfig.getType(), toMagic, 0L, time);
        }
    }

    @Test
    public void testConversion() throws IOException {
        doTestConversion(CompressionConfig.NONE, RecordBatch.MAGIC_VALUE_V0);
        doTestConversion(CompressionConfig.gzip().build(), RecordBatch.MAGIC_VALUE_V0);
        doTestConversion(CompressionConfig.NONE, RecordBatch.MAGIC_VALUE_V1);
        doTestConversion(CompressionConfig.gzip().build(), RecordBatch.MAGIC_VALUE_V1);
        doTestConversion(CompressionConfig.NONE, RecordBatch.MAGIC_VALUE_V2);
        doTestConversion(CompressionConfig.gzip().build(), RecordBatch.MAGIC_VALUE_V2);
    }

    @Test
    public void testBytesLengthOfWriteTo() throws IOException {

        int size = fileRecords.sizeInBytes();
        long firstWritten = size / 3;

        TransferableChannel channel = Mockito.mock(TransferableChannel.class);

        // Firstly we wrote some of the data
        fileRecords.writeTo(channel, 0, (int) firstWritten);
        verify(channel).transferFrom(any(), anyLong(), eq(firstWritten));

        // Ensure (length > size - firstWritten)
        int secondWrittenLength = size - (int) firstWritten + 1;
        fileRecords.writeTo(channel, firstWritten, secondWrittenLength);
        // But we still only write (size - firstWritten), which is not fulfilled in the old version
        verify(channel).transferFrom(any(), anyLong(), eq(size - firstWritten));
    }

    private void doTestConversion(CompressionConfig compressionConfig, byte toMagic) throws IOException {
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
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, RecordBatch.MAGIC_VALUE_V0, compressionConfig,
                TimestampType.CREATE_TIME, 0L);
        for (int i = 0; i < 3; i++)
            builder.appendWithOffset(offsets.get(i), records.get(i));
        builder.close();

        builder = MemoryRecords.builder(buffer, RecordBatch.MAGIC_VALUE_V1, compressionConfig, TimestampType.CREATE_TIME,
                0L);
        for (int i = 3; i < 6; i++)
            builder.appendWithOffset(offsets.get(i), records.get(i));
        builder.close();

        builder = MemoryRecords.builder(buffer, RecordBatch.MAGIC_VALUE_V2, compressionConfig, TimestampType.CREATE_TIME, 0L);
        for (int i = 6; i < 10; i++)
            builder.appendWithOffset(offsets.get(i), records.get(i));
        builder.close();

        buffer.flip();

        try (FileRecords fileRecords = FileRecords.open(tempFile())) {
            fileRecords.append(MemoryRecords.readableRecords(buffer));
            fileRecords.flush();
            downConvertAndVerifyRecords(records, offsets, fileRecords, compressionConfig.getType(), toMagic, 0L, time);

            if (toMagic <= RecordBatch.MAGIC_VALUE_V1 && compressionConfig.getType() == CompressionType.NONE) {
                long firstOffset;
                if (toMagic == RecordBatch.MAGIC_VALUE_V0)
                    firstOffset = 11L; // v1 record
                else
                    firstOffset = 17; // v2 record
                List<Long> filteredOffsets = new ArrayList<>(offsets);
                List<SimpleRecord> filteredRecords = new ArrayList<>(records);
                int index = filteredOffsets.indexOf(firstOffset) - 1;
                filteredRecords.remove(index);
                filteredOffsets.remove(index);
                downConvertAndVerifyRecords(filteredRecords, filteredOffsets, fileRecords, compressionConfig.getType(), toMagic, firstOffset, time);
            } else {
                // firstOffset doesn't have any effect in this case
                downConvertAndVerifyRecords(records, offsets, fileRecords, compressionConfig.getType(), toMagic, 10L, time);
            }
        }
    }

    private void downConvertAndVerifyRecords(List<SimpleRecord> initialRecords,
                                             List<Long> initialOffsets,
                                             FileRecords fileRecords,
                                             CompressionType compressionType,
                                             byte toMagic,
                                             long firstOffset,
                                             Time time) {
        long minBatchSize = Long.MAX_VALUE;
        long maxBatchSize = Long.MIN_VALUE;
        for (RecordBatch batch : fileRecords.batches()) {
            minBatchSize = Math.min(minBatchSize, batch.sizeInBytes());
            maxBatchSize = Math.max(maxBatchSize, batch.sizeInBytes());
        }

        // Test the normal down-conversion path
        List<Records> convertedRecords = new ArrayList<>();
        convertedRecords.add(fileRecords.downConvert(toMagic, firstOffset, time).records());
        verifyConvertedRecords(initialRecords, initialOffsets, convertedRecords, compressionType, toMagic);
        convertedRecords.clear();

        // Test the lazy down-conversion path
        List<Long> maximumReadSize = asList(16L * 1024L,
                (long) fileRecords.sizeInBytes(),
                (long) fileRecords.sizeInBytes() - 1,
                (long) fileRecords.sizeInBytes() / 4,
                maxBatchSize + 1,
                1L);
        for (long readSize : maximumReadSize) {
            TopicPartition tp = new TopicPartition("topic-1", 0);
            LazyDownConversionRecords lazyRecords = new LazyDownConversionRecords(tp, fileRecords, toMagic, firstOffset, Time.SYSTEM);
            Iterator<ConvertedRecords<?>> it = lazyRecords.iterator(readSize);
            while (it.hasNext())
                convertedRecords.add(it.next().records());
            verifyConvertedRecords(initialRecords, initialOffsets, convertedRecords, compressionType, toMagic);
            convertedRecords.clear();
        }
    }

    private void verifyConvertedRecords(List<SimpleRecord> initialRecords,
                                        List<Long> initialOffsets,
                                        List<Records> convertedRecordsList,
                                        CompressionType compressionType,
                                        byte magicByte) {
        int i = 0;

        for (Records convertedRecords : convertedRecordsList) {
            for (RecordBatch batch : convertedRecords.batches()) {
                assertTrue(batch.magic() <= magicByte, "Magic byte should be lower than or equal to " + magicByte);
                if (batch.magic() == RecordBatch.MAGIC_VALUE_V0)
                    assertEquals(TimestampType.NO_TIMESTAMP_TYPE, batch.timestampType());
                else
                    assertEquals(TimestampType.CREATE_TIME, batch.timestampType());
                assertEquals(compressionType, batch.compressionType(), "Compression type should not be affected by conversion");
                for (Record record : batch) {
                    assertTrue(record.hasMagic(batch.magic()), "Inner record should have magic " + magicByte);
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
                CompressionConfig.NONE, TimestampType.CREATE_TIME, offset);
            builder.appendWithOffset(offset++, System.currentTimeMillis(), null, value);
            fileRecords.append(builder.build());
        }
        fileRecords.flush();
    }
}
