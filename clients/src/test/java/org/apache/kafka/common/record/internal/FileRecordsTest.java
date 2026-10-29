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
package org.apache.kafka.common.record.internal;

import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.network.FileReadException;
import org.apache.kafka.common.network.PlaintextTransportLayer;
import org.apache.kafka.common.network.SslTransportLayer;
import org.apache.kafka.common.network.TransferableChannel;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Arrays.asList;
import static org.apache.kafka.test.TestUtils.tempFile;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class FileRecordsTest {

    private final byte[][] values = new byte[][] {
            "abcd".getBytes(),
            "efgh".getBytes(),
            "ijkl".getBytes()
    };
    private static final String LOG_FILE_PATH = "/logs/topic-0/00000000000000000000.log";

    private FileRecords fileRecords;

    @BeforeEach
    public void setup() throws IOException {
        this.fileRecords = createFileRecords(values);
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

        FileRecords records = new FileRecords(fileMock, fileChannelMock, Integer.MAX_VALUE);
        assertThrows(IllegalArgumentException.class, () -> append(records, values));
    }

    @Test
    public void testOpenOversizeFile() throws Exception {
        File fileMock = mock(File.class);
        FileChannel fileChannelMock = mock(FileChannel.class);
        when(fileChannelMock.size()).thenReturn(Integer.MAX_VALUE + 5L);

        assertThrows(KafkaException.class, () -> new FileRecords(fileMock, fileChannelMock, Integer.MAX_VALUE));
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
            fileRecords.append(MemoryRecords.withRecords(Compression.NONE, new SimpleRecord("abcd".getBytes())));
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
                    Records slice = log.slice(0, currentSize);
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
    public void testRead() {
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

    @Test
    public void testReadIntoStopsAtEndOfSlice() throws IOException {
        List<RecordBatch> items = batches(fileRecords);
        RecordBatch second = items.get(1);
        // The slice holds only the second batch. The third batch should not be read into the buffer.
        FileRecords slice = fileRecords.slice(items.get(0).sizeInBytes(), second.sizeInBytes());
        ByteBuffer buffer = ByteBuffer.allocate(fileRecords.sizeInBytes());

        slice.readInto(buffer, 0);

        assertEquals(second.sizeInBytes(), buffer.remaining());
        assertEquals(
            List.of(second.baseOffset()),
            batches(MemoryRecords.readableRecords(buffer)).stream()
                .map(RecordBatch::baseOffset).collect(Collectors.toList())
        );
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testReadIntoStopsAtEndOfRecords(boolean preallocate) throws IOException {
        try (FileRecords fileRecords = createFileRecords(values, preallocate)) {
            // The buffer has room past the records, where a preallocated file has zeros instead of ending
            ByteBuffer buffer = ByteBuffer.allocate(2 * fileRecords.sizeInBytes());

            fileRecords.readInto(buffer, 0);

            assertEquals(fileRecords.sizeInBytes(), buffer.remaining());
            assertEquals(
                List.of(0L, 1L, 2L),
                batches(MemoryRecords.readableRecords(buffer)).stream()
                    .map(RecordBatch::baseOffset).collect(Collectors.toList())
            );
        }
    }

    @Test
    public void testReadIntoAtEndOfRecordsReadsNothing() throws IOException {
        List<RecordBatch> items = batches(fileRecords);
        FileRecords slice = fileRecords.slice(items.get(0).sizeInBytes(), items.get(1).sizeInBytes());
        ByteBuffer buffer = ByteBuffer.allocate(fileRecords.sizeInBytes());

        slice.readInto(buffer, slice.sizeInBytes());

        assertEquals(0, buffer.remaining());
    }

    @Test
    public void testReadIntoWithPositionOutsideRecords() {
        List<RecordBatch> items = batches(fileRecords);
        FileRecords slice = fileRecords.slice(items.get(0).sizeInBytes(), items.get(1).sizeInBytes());
        ByteBuffer buffer = ByteBuffer.allocate(fileRecords.sizeInBytes());

        // Both positions are inside the file but outside the slice
        assertThrows(IllegalArgumentException.class, () -> slice.readInto(buffer, -1));
        assertThrows(IllegalArgumentException.class, () -> slice.readInto(buffer, slice.sizeInBytes() + 1));
    }

    /**
     * Test the MessageSet.searchFor API.
     */
    @Test
    public void testSearch() throws IOException {
        // append a new message with a high offset
        SimpleRecord lastMessage = new SimpleRecord("test".getBytes());
        fileRecords.append(MemoryRecords.withRecords(50L, Compression.NONE, lastMessage));

        List<RecordBatch> batches = batches(fileRecords);
        int position = 0;

        int message1Size = batches.get(0).sizeInBytes();
        assertEquals(new FileRecords.LogOffsetPosition(0L, position, message1Size),
            fileRecords.searchForOffsetFromPosition(0, 0),
            "Should be able to find the first message by its offset");
        position += message1Size;

        int message2Size = batches.get(1).sizeInBytes();
        assertEquals(new FileRecords.LogOffsetPosition(1L, position, message2Size),
            fileRecords.searchForOffsetFromPosition(1, 0),
            "Should be able to find second message when starting from 0");
        assertEquals(new FileRecords.LogOffsetPosition(1L, position, message2Size),
            fileRecords.searchForOffsetFromPosition(1, position),
            "Should be able to find second message starting from its offset");
        position += message2Size + batches.get(2).sizeInBytes();

        int message4Size = batches.get(3).sizeInBytes();
        assertEquals(new FileRecords.LogOffsetPosition(50L, position, message4Size),
            fileRecords.searchForOffsetFromPosition(3, position),
            "Should be able to find fourth message from a non-existent offset");
        assertEquals(new FileRecords.LogOffsetPosition(50L, position, message4Size),
            fileRecords.searchForOffsetFromPosition(50,  position),
            "Should be able to find fourth message by correct offset");
    }

    /**
     * Test that the message set iterator obeys start and end slicing
     */
    @Test
    public void testIteratorWithLimits() {
        RecordBatch batch = batches(fileRecords).get(1);
        int start = fileRecords.searchForOffsetFromPosition(1, 0).position;
        int size = batch.sizeInBytes();
        Records slice = fileRecords.slice(start, size);
        assertEquals(Collections.singletonList(batch), batches(slice));
        Records slice2 = fileRecords.slice(start, size - 1);
        assertEquals(Collections.emptyList(), batches(slice2));
    }

    /**
     * Test the truncateTo method lops off messages and appropriately updates the size
     */
    @Test
    public void testTruncate() throws IOException {
        RecordBatch batch = batches(fileRecords).get(0);
        int end = fileRecords.searchForOffsetFromPosition(1, 0).position;
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

        FileRecords fileRecords = new FileRecords(tempFile(), channelMock, Integer.MAX_VALUE);
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

        FileRecords fileRecords = new FileRecords(tempFile(), channelMock, Integer.MAX_VALUE);

        assertThrows(KafkaException.class, () -> fileRecords.truncateTo(43));

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

        FileRecords fileRecords = new FileRecords(tempFile(), channelMock, Integer.MAX_VALUE);
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
        fileRecords.trim();
        fileRecords.flush();
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
    public void testSearchForTimestamp() throws IOException {
        for (RecordVersion version : RecordVersion.values()) {
            testSearchForTimestamp(version);
        }
    }

    /**
     * Test slice when already sliced file records have start position greater than available bytes
     * in the file records.
     */
    @Test
    public void testSliceForAlreadySlicedFileRecords() throws IOException {
        byte[][] values = new byte[][] {
            "abcd".getBytes(),
            "efgh".getBytes(),
            "ijkl".getBytes(),
            "mnopqr".getBytes(),
            "stuv".getBytes()
        };
        try (FileRecords fileRecords = createFileRecords(values)) {
            List<RecordBatch> items = batches(fileRecords.slice(0, fileRecords.sizeInBytes()));

            // Slice from fourth message until the end.
            int position = IntStream.range(0, 3).map(i -> items.get(i).sizeInBytes()).sum();
            Records sliced  = fileRecords.slice(position, fileRecords.sizeInBytes() - position);
            assertEquals(fileRecords.sizeInBytes() - position, sliced.sizeInBytes());
            assertEquals(items.subList(3, items.size()), batches(sliced), "Read starting from the fourth message");

            // Further slice the already sliced file records, from fifth message until the end. Now the
            // bytes available in the sliced records are less than the moved position from original records.
            position = items.get(3).sizeInBytes();
            Records finalSliced = sliced.slice(position, sliced.sizeInBytes() - position);
            assertEquals(sliced.sizeInBytes() - position, finalSliced.sizeInBytes());
            assertEquals(items.subList(4, items.size()), batches(finalSliced), "Read starting from the fifth message");
        }
    }

    private void testSearchForTimestamp(RecordVersion version) throws IOException {
        File temp = tempFile();
        FileRecords fileRecords = FileRecords.open(temp, false, 1024 * 1024, true);
        appendWithOffsetAndTimestamp(fileRecords, version, 10L, 5, 0);
        appendWithOffsetAndTimestamp(fileRecords, version, 11L, 6, 1);

        assertFoundTimestamp(new FileRecords.TimestampAndOffset(10L, 5, Optional.of(0)),
                fileRecords.searchForTimestamp(9L, 0, 0L, Records.SOFT_MAX_ARRAY_LENGTH), version);
        assertFoundTimestamp(new FileRecords.TimestampAndOffset(10L, 5, Optional.of(0)),
                fileRecords.searchForTimestamp(10L, 0, 0L, Records.SOFT_MAX_ARRAY_LENGTH), version);
        assertFoundTimestamp(new FileRecords.TimestampAndOffset(11L, 6, Optional.of(1)),
                fileRecords.searchForTimestamp(11L, 0, 0L, Records.SOFT_MAX_ARRAY_LENGTH), version);
        assertNull(fileRecords.searchForTimestamp(12L, 0, 0L, Records.SOFT_MAX_ARRAY_LENGTH));
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
                Compression.NONE, TimestampType.CREATE_TIME, offset, timestamp, leaderEpoch);
        builder.append(new SimpleRecord(timestamp, new byte[0], new byte[0]));
        fileRecords.append(builder.build());
    }

    @Test
    public void testConversion() throws IOException {
        doTestConversion(Compression.NONE, RecordBatch.MAGIC_VALUE_V0);
        doTestConversion(Compression.gzip().build(), RecordBatch.MAGIC_VALUE_V0);
        doTestConversion(Compression.NONE, RecordBatch.MAGIC_VALUE_V1);
        doTestConversion(Compression.gzip().build(), RecordBatch.MAGIC_VALUE_V1);
        doTestConversion(Compression.NONE, RecordBatch.MAGIC_VALUE_V2);
        doTestConversion(Compression.gzip().build(), RecordBatch.MAGIC_VALUE_V2);
    }

    @Test
    public void testBytesLengthOfWriteTo() throws IOException {

        int size = fileRecords.sizeInBytes();
        int firstWritten = size / 3;

        TransferableChannel channel = Mockito.mock(TransferableChannel.class);

        // Firstly we wrote some of the data
        fileRecords.writeTo(channel, 0, firstWritten);
        verify(channel).transferFrom(any(), anyLong(), eq((long) firstWritten));

        // Ensure (length > size - firstWritten)
        int secondWrittenLength = size - firstWritten + 1;
        fileRecords.writeTo(channel, firstWritten, secondWrittenLength);
        // But we still only write (size - firstWritten), which is not fulfilled in the old version
        verify(channel).transferFrom(any(), anyLong(), eq((long) size - firstWritten));
    }

    /**
     * A failed write to a network channel that cannot tell a failed read of the file from a failed write is reported
     * to the handler with the region being written, and the exception is rethrown as it arrived: the handler finds out
     * later whether the file could be read.
     */
    @Test
    public void testWriteToNotifiesHandlerAndRethrows() throws IOException {
        List<List<Object>> notifications = new ArrayList<>();
        FileRecords records = fileRecords.slice(1, fileRecords.sizeInBytes() - 1)
            .withTransferFailureHandler(recordingHandler(notifications));
        IOException socketFailure = new IOException("Broken pipe");

        IOException thrown = assertThrows(IOException.class,
            () -> records.writeTo(failingChannel(socketFailure), 2, records.sizeInBytes() - 2));
        assertSame(socketFailure, thrown);
        assertEquals(List.of(List.of(records, 3L, records.sizeInBytes() - 2, socketFailure)), notifications);
    }

    /**
     * A network channel that tells its own failed reads of the file apart fails with a FileReadException for them,
     * which is reported to the handler, and with any other exception only when it could not be written to.
     */
    @Test
    public void testWriteToNotifiesHandlerOnlyOfFailedReadsWhenChannelDistinguishesThem() throws IOException {
        List<List<Object>> notifications = new ArrayList<>();
        FileRecords records = fileRecords.slice(1, fileRecords.sizeInBytes() - 1)
            .withTransferFailureHandler(recordingHandler(notifications));
        IOException socketFailure = new IOException("Broken pipe");
        FileReadException readFailure = new FileReadException(5, new IOException("Input/output error"));
        int length = records.sizeInBytes() - 2;

        assertSame(socketFailure, assertThrows(IOException.class,
            () -> records.writeTo(failingChannelDistinguishingFileReadFailures(socketFailure), 2, length)));
        assertTrue(notifications.isEmpty());
        assertSame(readFailure, assertThrows(IOException.class,
            () -> records.writeTo(failingChannelDistinguishingFileReadFailures(readFailure), 2, length)));
        assertEquals(List.of(List.of(records, 3L, length, readFailure)), notifications);
    }

    /**
     * A socket failure reaches the handler, to be told apart from a failed read by re-reading the region, only on the
     * transport layer that cannot tell them apart itself: PLAINTEXT, where one system call reads the file and writes
     * the socket, and not TLS.
     */
    @Test
    public void testWriteToNotifiesHandlerOfSocketFailureOnlyOverPlaintext() throws IOException {
        List<List<Object>> notifications = new ArrayList<>();
        FileRecords records = fileRecords.slice(0, fileRecords.sizeInBytes())
            .withTransferFailureHandler(recordingHandler(notifications));
        IOException socketFailure = new IOException("Broken pipe");
        TransferableChannel tls = transportFailingWith(SslTransportLayer.class, socketFailure);
        TransferableChannel plaintext = transportFailingWith(PlaintextTransportLayer.class, socketFailure);

        assertSame(socketFailure, assertThrows(IOException.class, () -> records.writeTo(tls, 0, records.sizeInBytes())));
        assertTrue(notifications.isEmpty());
        assertSame(socketFailure, assertThrows(IOException.class,
            () -> records.writeTo(plaintext, 0, records.sizeInBytes())));
        assertEquals(List.of(List.of(records, 0L, records.sizeInBytes(), socketFailure)), notifications);
    }

    /**
     * A channel closed concurrently, for instance by a deletion or a shutdown, says nothing about the medium, so the
     * handler is not notified.
     */
    @Test
    public void testWriteToDoesNotNotifyHandlerWhenChannelClosed() throws IOException {
        List<List<Object>> notifications = new ArrayList<>();
        FileRecords records = fileRecords.slice(0, fileRecords.sizeInBytes())
            .withTransferFailureHandler(recordingHandler(notifications));
        IOException closed = new ClosedChannelException();

        IOException thrown = assertThrows(IOException.class,
            () -> records.writeTo(failingChannel(closed), 0, records.sizeInBytes()));
        assertSame(closed, thrown);
        assertTrue(notifications.isEmpty());
    }

    /**
     * Getting the size of the file before the transfer is part of reading it, so its failure is a failed read at the
     * position of the region, reported even when the network channel tells its own failed reads apart.
     */
    @Test
    public void testWriteToReportsFailedSizeAsFailedRead() throws IOException {
        FileChannel fileChannel = fileChannelOfSize(1024);
        List<List<Object>> notifications = new ArrayList<>();
        FileRecords records = new FileRecords(mock(File.class), fileChannel, 1024).slice(100, 900)
            .withTransferFailureHandler(recordingHandler(notifications));
        IOException sizeFailure = new IOException("Input/output error");
        when(fileChannel.size()).thenThrow(sizeFailure);
        TransferableChannel channel = mock(TransferableChannel.class);
        when(channel.distinguishesFileReadFailures()).thenReturn(true);

        FileReadException thrown = assertThrows(FileReadException.class, () -> records.writeTo(channel, 10, 800));
        assertEquals(110, thrown.position());
        assertSame(sizeFailure, thrown.getCause());
        assertEquals(List.of(List.of(records, 110L, 800, thrown)), notifications);
        verify(channel, never()).transferFrom(any(), anyLong(), anyLong());
    }

    /**
     * A file closed before its size could be read, for instance by a deletion, is not a failed read either, so the
     * handler is not notified.
     */
    @Test
    public void testWriteToDoesNotNotifyHandlerWhenFileClosed() throws IOException {
        FileChannel fileChannel = fileChannelOfSize(1024);
        List<List<Object>> notifications = new ArrayList<>();
        FileRecords records = new FileRecords(mock(File.class), fileChannel, 1024).slice(0, 1024)
            .withTransferFailureHandler(recordingHandler(notifications));
        ClosedChannelException closed = new ClosedChannelException();
        when(fileChannel.size()).thenThrow(closed);

        assertSame(closed, assertThrows(IOException.class,
            () -> records.writeTo(mock(TransferableChannel.class), 0, 1024)));
        assertTrue(notifications.isEmpty());
    }

    @Test
    public void testTransferFailureHandlerIsKeptBySlices() {
        FileRecords.TransferFailureHandler handler = (records, position, count, e) -> { };
        FileRecords slice = fileRecords.slice(0, fileRecords.sizeInBytes());
        FileRecords records = slice.withTransferFailureHandler(handler);

        assertSame(handler, records.transferFailureHandler());
        assertSame(handler, records.slice(1, records.sizeInBytes() - 1).transferFailureHandler());
        assertEquals(slice.sizeInBytes(), records.sizeInBytes());
        assertNull(slice.transferFailureHandler());
    }

    /**
     * Only slices are written to a network channel; the records of a whole segment keep growing.
     */
    @Test
    public void testTransferFailureHandlerCannotBeSetOnNonSlice() {
        assertThrows(IllegalStateException.class,
            () -> fileRecords.withTransferFailureHandler((records, position, count, e) -> { }));
    }

    @Test
    public void testReadFailureIsEmptyWhenRegionIsReadable() {
        assertEquals(Optional.empty(), fileRecords.readFailure(0, fileRecords.sizeInBytes()));
    }

    @Test
    public void testReadFailureAtStartOfRegion() throws IOException {
        FileChannel fileChannel = fileChannelOfSize(1024);
        IOException readFailure = new IOException("Input/output error");
        when(fileChannel.read(any(ByteBuffer.class), anyLong())).thenThrow(readFailure);

        IOException failure = recordsOn(fileChannel, 1024).readFailure(0, 1024).orElseThrow();
        assertSame(readFailure, failure.getCause());
        assertTrue(failure.getMessage().contains(LOG_FILE_PATH + " at position 0"));
    }

    /**
     * Depending on the platform, the transfer may fail anywhere within the region without reporting progress, so a
     * failure past the first bytes of the region must be found as well.
     */
    @Test
    public void testReadFailureInsideRegion() throws IOException {
        int size = 2 * FileRecords.REREAD_BUFFER_SIZE;
        FileChannel fileChannel = fileChannelOfSize(size);
        IOException readFailure = new IOException("Input/output error");
        when(fileChannel.read(any(ByteBuffer.class), anyLong()))
            .thenAnswer(FileRecordsTest::fillBuffer)
            .thenThrow(readFailure);

        IOException failure = recordsOn(fileChannel, size).readFailure(0, size).orElseThrow();
        assertSame(readFailure, failure.getCause());
        assertTrue(failure.getMessage().contains("at position " + FileRecords.REREAD_BUFFER_SIZE));
        verify(fileChannel).read(any(ByteBuffer.class), eq(0L));
        verify(fileChannel).read(any(ByteBuffer.class), eq((long) FileRecords.REREAD_BUFFER_SIZE));
    }

    /**
     * The log can be truncated while the region is re-read, for instance when the replica becomes a follower while
     * a fetch is being served. The end of the file says nothing about the medium.
     */
    @Test
    public void testReadFailureIsEmptyWhenTruncatedDuringRead() throws IOException {
        int size = 2 * FileRecords.REREAD_BUFFER_SIZE;
        FileChannel fileChannel = fileChannelOfSize(size);
        when(fileChannel.read(any(ByteBuffer.class), anyLong()))
            .thenAnswer(FileRecordsTest::fillBuffer)
            .thenReturn(-1);

        assertEquals(Optional.empty(), recordsOn(fileChannel, size).readFailure(0, size));
    }

    /**
     * A channel closed while the region is re-read, by a deletion of the segment or a shutdown, says nothing about
     * the medium.
     */
    @Test
    public void testReadFailureIsEmptyWhenChannelClosedDuringRead() throws IOException {
        FileChannel fileChannel = fileChannelOfSize(1024);
        when(fileChannel.read(any(ByteBuffer.class), anyLong())).thenThrow(new ClosedChannelException());

        assertEquals(Optional.empty(), recordsOn(fileChannel, 1024).readFailure(0, 1024));
    }

    /**
     * A read that makes no progress cannot conclude anything, and must not spin.
     */
    @Test
    public void testReadFailureIsEmptyWhenReadMakesNoProgress() throws IOException {
        FileChannel fileChannel = fileChannelOfSize(1024);
        when(fileChannel.read(any(ByteBuffer.class), anyLong())).thenReturn(0);

        assertEquals(Optional.empty(), recordsOn(fileChannel, 1024).readFailure(0, 1024));
    }

    // Records each notification as a list of the handler's arguments
    private static FileRecords.TransferFailureHandler recordingHandler(List<List<Object>> notifications) {
        return (records, position, count, e) -> notifications.add(List.of(records, position, count, e));
    }

    private static FileChannel fileChannelOfSize(long size) throws IOException {
        FileChannel fileChannel = mock(FileChannel.class);
        when(fileChannel.size()).thenReturn(size);
        return fileChannel;
    }

    private static FileRecords recordsOn(FileChannel fileChannel, int size) throws IOException {
        File file = mock(File.class);
        when(file.getAbsolutePath()).thenReturn(LOG_FILE_PATH);
        return new FileRecords(file, fileChannel, size);
    }

    private static TransferableChannel failingChannel(IOException failure) throws IOException {
        TransferableChannel channel = mock(TransferableChannel.class);
        when(channel.transferFrom(any(), anyLong(), anyLong())).thenThrow(failure);
        return channel;
    }

    // A network channel that fails with the given exception and tells its own failed reads of the file apart
    private static TransferableChannel failingChannelDistinguishingFileReadFailures(IOException failure)
            throws IOException {
        TransferableChannel channel = failingChannel(failure);
        when(channel.distinguishesFileReadFailures()).thenReturn(true);
        return channel;
    }

    // A transport layer of the given type whose transfers fail with the given exception, and which says whether it
    // tells its own failed reads of the file apart as its type does
    private static <T extends TransferableChannel> T transportFailingWith(Class<T> type, IOException failure)
            throws IOException {
        T transportLayer = mock(type);
        when(transportLayer.transferFrom(any(), anyLong(), anyLong())).thenThrow(failure);
        when(transportLayer.distinguishesFileReadFailures()).thenCallRealMethod();
        return transportLayer;
    }

    // An answer to FileChannel.read(ByteBuffer, long) that fills the buffer
    private static int fillBuffer(InvocationOnMock invocation) {
        ByteBuffer buffer = invocation.getArgument(0);
        int bytesRead = buffer.remaining();
        buffer.position(buffer.limit());
        return bytesRead;
    }

    /**
     * Test two conditions:
     * 1. If the target offset equals the base offset of the first batch
     * 2. If the target offset is less than the base offset of the first batch
     * <p>
     * If the base offset of the first batch is equal to or greater than the target offset, it should return the
     * position of the first batch and the lastOffset method should not be called.
     */
    @ParameterizedTest
    @ValueSource(longs = {5, 10})
    public void testSearchForOffsetFromPosition1(long baseOffset) throws IOException {
        File mockFile = mock(File.class);
        FileChannel mockChannel = mock(FileChannel.class);
        FileLogInputStream.FileChannelRecordBatch batch = mock(FileLogInputStream.FileChannelRecordBatch.class);
        when(batch.baseOffset()).thenReturn(baseOffset);

        FileRecords fileRecords = Mockito.spy(new FileRecords(mockFile, mockChannel, 100));
        mockFileRecordBatches(fileRecords, batch);

        FileRecords.LogOffsetPosition result = fileRecords.searchForOffsetFromPosition(5L, 0);

        assertEquals(FileRecords.LogOffsetPosition.fromBatch(batch), result);
        verify(batch, never()).lastOffset();
    }

    /**
     * Test the case when the target offset equals the last offset of the first batch.
     */
    @Test
    public void testSearchForOffsetFromPosition2() throws IOException {
        File mockFile = mock(File.class);
        FileChannel mockChannel = mock(FileChannel.class);
        FileLogInputStream.FileChannelRecordBatch batch = mock(FileLogInputStream.FileChannelRecordBatch.class);
        when(batch.baseOffset()).thenReturn(3L);
        when(batch.lastOffset()).thenReturn(5L);

        FileRecords fileRecords = Mockito.spy(new FileRecords(mockFile, mockChannel, 100));
        mockFileRecordBatches(fileRecords, batch);

        FileRecords.LogOffsetPosition result = fileRecords.searchForOffsetFromPosition(5L, 0);

        assertEquals(FileRecords.LogOffsetPosition.fromBatch(batch), result);
        // target is equal to the last offset of the batch, we should call lastOffset
        verify(batch, times(1)).lastOffset();
    }

    /**
     * Test the case when the target offset equals the last offset of the last batch.
     */
    @Test
    public void testSearchForOffsetFromPosition3() throws IOException {
        File mockFile = mock(File.class);
        FileChannel mockChannel = mock(FileChannel.class);
        FileLogInputStream.FileChannelRecordBatch prevBatch = mock(FileLogInputStream.FileChannelRecordBatch.class);
        when(prevBatch.baseOffset()).thenReturn(5L);
        when(prevBatch.lastOffset()).thenReturn(12L);
        FileLogInputStream.FileChannelRecordBatch currentBatch = mock(FileLogInputStream.FileChannelRecordBatch.class);
        when(currentBatch.baseOffset()).thenReturn(15L);
        when(currentBatch.lastOffset()).thenReturn(20L);

        FileRecords fileRecords = Mockito.spy(new FileRecords(mockFile, mockChannel, 100));
        mockFileRecordBatches(fileRecords, prevBatch, currentBatch);

        FileRecords.LogOffsetPosition result = fileRecords.searchForOffsetFromPosition(20L, 0);

        assertEquals(FileRecords.LogOffsetPosition.fromBatch(currentBatch), result);
        // Because the target offset is in the current batch, we should not call lastOffset in the previous batch
        verify(prevBatch, never()).lastOffset();
        verify(currentBatch, times(1)).lastOffset();
    }

    /**
     * Test the case when the target offset is within the range of the previous batch.
     */
    @Test
    public void testSearchForOffsetFromPosition4() throws IOException {
        File mockFile = mock(File.class);
        FileChannel mockChannel = mock(FileChannel.class);
        FileLogInputStream.FileChannelRecordBatch prevBatch = mock(FileLogInputStream.FileChannelRecordBatch.class);
        when(prevBatch.baseOffset()).thenReturn(5L);
        when(prevBatch.lastOffset()).thenReturn(12L); // > targetOffset
        FileLogInputStream.FileChannelRecordBatch currentBatch = mock(FileLogInputStream.FileChannelRecordBatch.class);
        when(currentBatch.baseOffset()).thenReturn(15L); // >= targetOffset

        FileRecords fileRecords = Mockito.spy(new FileRecords(mockFile, mockChannel, 100));
        mockFileRecordBatches(fileRecords, prevBatch, currentBatch);

        FileRecords.LogOffsetPosition result = fileRecords.searchForOffsetFromPosition(10L, 0);

        assertEquals(FileRecords.LogOffsetPosition.fromBatch(prevBatch), result);
        // Because the target offset is in the current batch, we should call lastOffset
        // on the previous batch
        verify(prevBatch, times(1)).lastOffset();
    }

    /**
     * Test the case when no batch matches the target offset.
     */
    @Test
    public void testSearchForOffsetFromPosition5() throws IOException {
        File mockFile = mock(File.class);
        FileChannel mockChannel = mock(FileChannel.class);
        FileLogInputStream.FileChannelRecordBatch batch1 = mock(FileLogInputStream.FileChannelRecordBatch.class);
        when(batch1.baseOffset()).thenReturn(5L);  // < targetOffset
        FileLogInputStream.FileChannelRecordBatch batch2 = mock(FileLogInputStream.FileChannelRecordBatch.class);
        when(batch2.baseOffset()).thenReturn(8L);  // < targetOffset
        when(batch2.lastOffset()).thenReturn(9L);  // < targetOffset

        FileRecords fileRecords = Mockito.spy(new FileRecords(mockFile, mockChannel, 100));
        mockFileRecordBatches(fileRecords, batch1, batch2);

        FileRecords.LogOffsetPosition result = fileRecords.searchForOffsetFromPosition(10L, 0);

        assertNull(result);
        // Because the target offset is exceeded by the last offset of the batch2,
        // we should call lastOffset on the batch2
        verify(batch1, never()).lastOffset();
        verify(batch2, times(1)).lastOffset();
    }

    /**
     * Test two conditions:
     * 1. If the target offset is less than the base offset of the last batch
     * 2. If the target offset equals the base offset of the last batch
     */
    @ParameterizedTest
    @ValueSource(longs = {8, 10})
    public void testSearchForOffsetFromPosition6(long baseOffset) throws IOException {
        File mockFile = mock(File.class);
        FileChannel mockChannel = mock(FileChannel.class);
        FileLogInputStream.FileChannelRecordBatch batch1 = mock(FileLogInputStream.FileChannelRecordBatch.class);
        when(batch1.baseOffset()).thenReturn(5L);  // < targetOffset
        FileLogInputStream.FileChannelRecordBatch batch2 = mock(FileLogInputStream.FileChannelRecordBatch.class);
        when(batch2.baseOffset()).thenReturn(baseOffset);  // < targetOffset or == targetOffset
        when(batch2.lastOffset()).thenReturn(12L); // >= targetOffset

        FileRecords fileRecords = Mockito.spy(new FileRecords(mockFile, mockChannel, 100));
        mockFileRecordBatches(fileRecords, batch1, batch2);

        long targetOffset = 10L;
        FileRecords.LogOffsetPosition result = fileRecords.searchForOffsetFromPosition(targetOffset, 0);

        assertEquals(FileRecords.LogOffsetPosition.fromBatch(batch2), result);
        if (targetOffset == baseOffset) {
            // Because the target offset is equal to the base offset of the batch2, we should not call
            // lastOffset on batch2 and batch1
            verify(batch1, never()).lastOffset();
            verify(batch2, never()).lastOffset();
        } else {
            // Because the target offset is in the batch2, we should not call
            // lastOffset on batch1
            verify(batch1, never()).lastOffset();
            verify(batch2, times(1)).lastOffset();
        }
    }

    /**
     * Test the case when the target offset is between two batches.
     */
    @Test
    public void testSearchForOffsetFromPosition7() throws IOException {
        File mockFile = mock(File.class);
        FileChannel mockChannel = mock(FileChannel.class);
        FileLogInputStream.FileChannelRecordBatch batch1 = mock(FileLogInputStream.FileChannelRecordBatch.class);
        when(batch1.baseOffset()).thenReturn(5L);
        when(batch1.lastOffset()).thenReturn(10L);
        FileLogInputStream.FileChannelRecordBatch batch2 = mock(FileLogInputStream.FileChannelRecordBatch.class);
        when(batch2.baseOffset()).thenReturn(15L);
        when(batch2.lastOffset()).thenReturn(20L);

        FileRecords fileRecords = Mockito.spy(new FileRecords(mockFile, mockChannel, 100));
        mockFileRecordBatches(fileRecords, batch1, batch2);

        FileRecords.LogOffsetPosition result = fileRecords.searchForOffsetFromPosition(13L, 0);

        assertEquals(FileRecords.LogOffsetPosition.fromBatch(batch2), result);
        // Because the target offset is between the two batches, we should call lastOffset on the batch1
        verify(batch1, times(1)).lastOffset();
        verify(batch2, never()).lastOffset();
    }

    private void mockFileRecordBatches(FileRecords fileRecords, FileLogInputStream.FileChannelRecordBatch... batch) {
        List<FileLogInputStream.FileChannelRecordBatch> batches = asList(batch);
        doReturn((Iterable<FileLogInputStream.FileChannelRecordBatch>) batches::iterator)
                .when(fileRecords)
                .batchesFrom(anyInt());
    }

    private void doTestConversion(Compression compression, byte toMagic) throws IOException {
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
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, RecordBatch.MAGIC_VALUE_V0, compression,
                TimestampType.CREATE_TIME, 0L);
        for (int i = 0; i < 3; i++)
            builder.appendWithOffset(offsets.get(i), records.get(i));
        builder.close();

        builder = MemoryRecords.builder(buffer, RecordBatch.MAGIC_VALUE_V1, compression, TimestampType.CREATE_TIME,
                0L);
        for (int i = 3; i < 6; i++)
            builder.appendWithOffset(offsets.get(i), records.get(i));
        builder.close();

        builder = MemoryRecords.builder(buffer, RecordBatch.MAGIC_VALUE_V2, compression, TimestampType.CREATE_TIME, 0L);
        for (int i = 6; i < 10; i++)
            builder.appendWithOffset(offsets.get(i), records.get(i));
        builder.close();

        buffer.flip();

        try (FileRecords fileRecords = FileRecords.open(tempFile())) {
            fileRecords.append(MemoryRecords.readableRecords(buffer));
            fileRecords.flush();

            if (toMagic <= RecordBatch.MAGIC_VALUE_V1 && compression.type() == CompressionType.NONE) {
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
            }
        }
    }

    private static List<RecordBatch> batches(Records buffer) {
        return TestUtils.toList(buffer.batches());
    }

    private FileRecords createFileRecords(byte[][] values) throws IOException {
        return createFileRecords(values, false);
    }

    private FileRecords createFileRecords(byte[][] values, boolean preallocate) throws IOException {
        FileRecords fileRecords = FileRecords.open(tempFile(), false, 1024 * 1024, preallocate);
        append(fileRecords, values);
        return fileRecords;
    }

    private void append(FileRecords fileRecords, byte[][] values) throws IOException {
        long offset = 0L;
        for (byte[] value : values) {
            ByteBuffer buffer = ByteBuffer.allocate(128);
            MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, RecordBatch.CURRENT_MAGIC_VALUE,
                    Compression.NONE, TimestampType.CREATE_TIME, offset);
            builder.appendWithOffset(offset++, System.currentTimeMillis(), null, value);
            fileRecords.append(builder.build());
        }
        fileRecords.flush();
    }


    @Test
    public void testSearchForTimestampRejectsCompressedRecordExceedingMaxRecordBodySize() throws IOException {
        FileRecords fileRecords = FileRecords.open(tempFile(), false, 1024 * 1024, true);
        long timestamp = 10L;
        fileRecords.append(MemoryRecords.withRecords(5L, Compression.gzip().build(), 0,
                new SimpleRecord(timestamp, "key".getBytes(), new byte[1000])));
        fileRecords.flush();

        // With no configured limit the record is found
        assertEquals(new FileRecords.TimestampAndOffset(timestamp, 5L, Optional.of(0)),
                fileRecords.searchForTimestamp(timestamp, 0, 0L, Records.SOFT_MAX_ARRAY_LENGTH));

        // With a limit below the record's decompressed body the lookup is rejected before the body is allocated
        InvalidRecordException e = assertThrows(InvalidRecordException.class,
                () -> fileRecords.searchForTimestamp(timestamp, 0, 0L, 100));
        assertTrue(e.getMessage().contains("exceeds the configured maximum record size of 100"), e.getMessage());
    }
}
