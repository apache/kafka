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
package org.apache.kafka.common.utils;

import org.apache.kafka.test.TestUtils;
import org.junit.Test;
import org.mockito.stubbing.OngoingStubbing;

import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Arrays.asList;
import static org.apache.kafka.common.utils.Utils.formatAddress;
import static org.apache.kafka.common.utils.Utils.formatBytes;
import static org.apache.kafka.common.utils.Utils.getHost;
import static org.apache.kafka.common.utils.Utils.getPort;
import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.apache.kafka.common.utils.Utils.validHostPattern;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class UtilsTest {

    @Test
    public void testGetHost() {
        assertEquals("127.0.0.1", getHost("127.0.0.1:8000"));
        assertEquals("mydomain.com", getHost("PLAINTEXT://mydomain.com:8080"));
        assertEquals("MyDomain.com", getHost("PLAINTEXT://MyDomain.com:8080"));
        assertEquals("My_Domain.com", getHost("PLAINTEXT://My_Domain.com:8080"));
        assertEquals("::1", getHost("[::1]:1234"));
        assertEquals("2001:db8:85a3:8d3:1319:8a2e:370:7348", getHost("PLAINTEXT://[2001:db8:85a3:8d3:1319:8a2e:370:7348]:5678"));
        assertEquals("2001:DB8:85A3:8D3:1319:8A2E:370:7348", getHost("PLAINTEXT://[2001:DB8:85A3:8D3:1319:8A2E:370:7348]:5678"));
        assertEquals("fe80::b1da:69ca:57f7:63d8%3", getHost("PLAINTEXT://[fe80::b1da:69ca:57f7:63d8%3]:5678"));
    }

    @Test
    public void testHostPattern() {
        assertTrue(validHostPattern("127.0.0.1"));
        assertTrue(validHostPattern("mydomain.com"));
        assertTrue(validHostPattern("MyDomain.com"));
        assertTrue(validHostPattern("My_Domain.com"));
        assertTrue(validHostPattern("::1"));
        assertTrue(validHostPattern("2001:db8:85a3:8d3:1319:8a2e:370"));
    }

    @Test
    public void testGetPort() {
        assertEquals(8000, getPort("127.0.0.1:8000").intValue());
        assertEquals(8080, getPort("mydomain.com:8080").intValue());
        assertEquals(8080, getPort("MyDomain.com:8080").intValue());
        assertEquals(1234, getPort("[::1]:1234").intValue());
        assertEquals(5678, getPort("[2001:db8:85a3:8d3:1319:8a2e:370:7348]:5678").intValue());
        assertEquals(5678, getPort("[2001:DB8:85A3:8D3:1319:8A2E:370:7348]:5678").intValue());
        assertEquals(5678, getPort("[fe80::b1da:69ca:57f7:63d8%3]:5678").intValue());
    }

    @Test
    public void testFormatAddress() {
        assertEquals("127.0.0.1:8000", formatAddress("127.0.0.1", 8000));
        assertEquals("mydomain.com:8080", formatAddress("mydomain.com", 8080));
        assertEquals("[::1]:1234", formatAddress("::1", 1234));
        assertEquals("[2001:db8:85a3:8d3:1319:8a2e:370:7348]:5678", formatAddress("2001:db8:85a3:8d3:1319:8a2e:370:7348", 5678));
    }

    @Test
    public void testFormatBytes() {
        assertEquals("-1", formatBytes(-1));
        assertEquals("1023 B", formatBytes(1023));
        assertEquals("1 KB", formatBytes(1024));
        assertEquals("1024 KB", formatBytes((1024 * 1024) - 1));
        assertEquals("1 MB", formatBytes(1024 * 1024));
        assertEquals("1.1 MB", formatBytes((long) (1.1 * 1024 * 1024)));
        assertEquals("10 MB", formatBytes(10 * 1024 * 1024));
    }

    @Test
    public void testJoin() {
        assertEquals("", Utils.join(Collections.emptyList(), ","));
        assertEquals("1", Utils.join(asList("1"), ","));
        assertEquals("1,2,3", Utils.join(asList(1, 2, 3), ","));
    }

    @Test
    public void testAbs() {
        assertEquals(0, Utils.abs(Integer.MIN_VALUE));
        assertEquals(10, Utils.abs(-10));
        assertEquals(10, Utils.abs(10));
        assertEquals(0, Utils.abs(0));
        assertEquals(1, Utils.abs(-1));
    }

    @Test
    public void writeToBuffer() throws IOException {
        byte[] input = {0, 1, 2, 3, 4, 5};
        ByteBuffer source = ByteBuffer.wrap(input);

        doTestWriteToByteBuffer(source, ByteBuffer.allocate(input.length));
        doTestWriteToByteBuffer(source, ByteBuffer.allocateDirect(input.length));
        assertEquals(0, source.position());

        source.position(2);
        doTestWriteToByteBuffer(source, ByteBuffer.allocate(input.length));
        doTestWriteToByteBuffer(source, ByteBuffer.allocateDirect(input.length));
    }

    private void doTestWriteToByteBuffer(ByteBuffer source, ByteBuffer dest) throws IOException {
        int numBytes = source.remaining();
        int position = source.position();
        DataOutputStream out = new DataOutputStream(new ByteBufferOutputStream(dest));
        Utils.writeTo(out, source, source.remaining());
        dest.flip();
        assertEquals(numBytes, dest.remaining());
        assertEquals(position, source.position());
        assertEquals(source, dest);
    }

    @Test
    public void toArray() {
        byte[] input = {0, 1, 2, 3, 4};
        ByteBuffer buffer = ByteBuffer.wrap(input);
        assertArrayEquals(input, Utils.toArray(buffer));
        assertEquals(0, buffer.position());

        assertArrayEquals(new byte[] {1, 2}, Utils.toArray(buffer, 1, 2));
        assertEquals(0, buffer.position());

        buffer.position(2);
        assertArrayEquals(new byte[] {2, 3, 4}, Utils.toArray(buffer));
        assertEquals(2, buffer.position());
    }

    @Test
    public void toArrayDirectByteBuffer() {
        byte[] input = {0, 1, 2, 3, 4};
        ByteBuffer buffer = ByteBuffer.allocateDirect(5);
        buffer.put(input);
        buffer.rewind();

        assertArrayEquals(input, Utils.toArray(buffer));
        assertEquals(0, buffer.position());

        assertArrayEquals(new byte[] {1, 2}, Utils.toArray(buffer, 1, 2));
        assertEquals(0, buffer.position());

        buffer.position(2);
        assertArrayEquals(new byte[] {2, 3, 4}, Utils.toArray(buffer));
        assertEquals(2, buffer.position());
    }

    @Test
    public void utf8ByteArraySerde() {
        String utf8String = "A\u00ea\u00f1\u00fcC";
        byte[] utf8Bytes = utf8String.getBytes(StandardCharsets.UTF_8);
        assertArrayEquals(utf8Bytes, Utils.utf8(utf8String));
        assertEquals(utf8Bytes.length, Utils.utf8Length(utf8String));
        assertEquals(utf8String, Utils.utf8(utf8Bytes));
    }

    @Test
    public void utf8ByteBufferSerde() {
        doTestUtf8ByteBuffer(ByteBuffer.allocate(20));
        doTestUtf8ByteBuffer(ByteBuffer.allocateDirect(20));
    }

    private void doTestUtf8ByteBuffer(ByteBuffer utf8Buffer) {
        String utf8String = "A\u00ea\u00f1\u00fcC";
        byte[] utf8Bytes = utf8String.getBytes(StandardCharsets.UTF_8);

        utf8Buffer.position(4);
        utf8Buffer.put(utf8Bytes);

        utf8Buffer.position(4);
        assertEquals(utf8String, Utils.utf8(utf8Buffer, utf8Bytes.length));
        assertEquals(4, utf8Buffer.position());

        utf8Buffer.position(0);
        assertEquals(utf8String, Utils.utf8(utf8Buffer, 4, utf8Bytes.length));
        assertEquals(0, utf8Buffer.position());
    }

    private void subTest(ByteBuffer buffer) {
        // The first byte should be 'A'
        assertEquals('A', (Utils.readBytes(buffer, 0, 1))[0]);

        // The offset is 2, so the first 2 bytes should be skipped.
        byte[] results = Utils.readBytes(buffer, 2, 3);
        assertEquals('y', results[0]);
        assertEquals(' ', results[1]);
        assertEquals('S', results[2]);
        assertEquals(3, results.length);

        // test readBytes without offset and length specified.
        results = Utils.readBytes(buffer);
        assertEquals('A', results[0]);
        assertEquals('t', results[buffer.limit() - 1]);
        assertEquals(buffer.limit(), results.length);
    }

    @Test
    public void testReadBytes() {
        byte[] myvar = "Any String you want".getBytes();
        ByteBuffer buffer = ByteBuffer.allocate(myvar.length);
        buffer.put(myvar);
        buffer.rewind();

        this.subTest(buffer);

        // test readonly buffer, different path
        buffer = ByteBuffer.wrap(myvar).asReadOnlyBuffer();
        this.subTest(buffer);
    }

    @Test
    public void testMin() {
        assertEquals(1, Utils.min(1));
        assertEquals(1, Utils.min(1, 2, 3));
        assertEquals(1, Utils.min(2, 1, 3));
        assertEquals(1, Utils.min(2, 3, 1));
    }

    @Test
    public void testCloseAll() {
        TestCloseable[] closeablesWithoutException = TestCloseable.createCloseables(false, false, false);
        try {
            Utils.closeAll(closeablesWithoutException);
            TestCloseable.checkClosed(closeablesWithoutException);
        } catch (IOException e) {
            fail("Unexpected exception: " + e);
        }

        TestCloseable[] closeablesWithException = TestCloseable.createCloseables(true, true, true);
        try {
            Utils.closeAll(closeablesWithException);
            fail("Expected exception not thrown");
        } catch (IOException e) {
            TestCloseable.checkClosed(closeablesWithException);
            TestCloseable.checkException(e, closeablesWithException);
        }

        TestCloseable[] singleExceptionCloseables = TestCloseable.createCloseables(false, true, false);
        try {
            Utils.closeAll(singleExceptionCloseables);
            fail("Expected exception not thrown");
        } catch (IOException e) {
            TestCloseable.checkClosed(singleExceptionCloseables);
            TestCloseable.checkException(e, singleExceptionCloseables[1]);
        }

        TestCloseable[] mixedCloseables = TestCloseable.createCloseables(false, true, false, true, true);
        try {
            Utils.closeAll(mixedCloseables);
            fail("Expected exception not thrown");
        } catch (IOException e) {
            TestCloseable.checkClosed(mixedCloseables);
            TestCloseable.checkException(e, mixedCloseables[1], mixedCloseables[3], mixedCloseables[4]);
        }
    }

    @Test
    public void testReadFullyOrFailWithRealFile() throws IOException {
        try (FileChannel channel = FileChannel.open(TestUtils.tempFile().toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            // prepare channel
            String msg = "hello, world";
            channel.write(ByteBuffer.wrap(msg.getBytes()), 0);
            channel.force(true);
            assertEquals("Message should be written to the file channel", channel.size(), msg.length());

            ByteBuffer perfectBuffer = ByteBuffer.allocate(msg.length());
            ByteBuffer smallBuffer = ByteBuffer.allocate(5);
            ByteBuffer largeBuffer = ByteBuffer.allocate(msg.length() + 1);
            // Scenario 1: test reading into a perfectly-sized buffer
            Utils.readFullyOrFail(channel, perfectBuffer, 0, "perfect");
            assertFalse("Buffer should be filled up", perfectBuffer.hasRemaining());
            assertEquals("Buffer should be populated correctly", msg, new String(perfectBuffer.array()));
            // Scenario 2: test reading into a smaller buffer
            Utils.readFullyOrFail(channel, smallBuffer, 0, "small");
            assertFalse("Buffer should be filled", smallBuffer.hasRemaining());
            assertEquals("Buffer should be populated correctly", "hello", new String(smallBuffer.array()));
            // Scenario 3: test reading starting from a non-zero position
            smallBuffer.clear();
            Utils.readFullyOrFail(channel, smallBuffer, 7, "small");
            assertFalse("Buffer should be filled", smallBuffer.hasRemaining());
            assertEquals("Buffer should be populated correctly", "world", new String(smallBuffer.array()));
            // Scenario 4: test end of stream is reached before buffer is filled up
            try {
                Utils.readFullyOrFail(channel, largeBuffer, 0, "large");
                fail("Expected EOFException to be raised");
            } catch (EOFException e) {
                // expected
            }
        }
    }

    /**
     * Tests that `readFullyOrFail` behaves correctly if multiple `FileChannel.read` operations are required to fill
     * the destination buffer.
     */
    @Test
    public void testReadFullyOrFailWithPartialFileChannelReads() throws IOException {
        FileChannel channelMock = mock(FileChannel.class);
        final int bufferSize = 100;
        ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
        String expectedBufferContent = fileChannelMockExpectReadWithRandomBytes(channelMock, bufferSize);
        Utils.readFullyOrFail(channelMock, buffer, 0L, "test");
        assertEquals("The buffer should be populated correctly", expectedBufferContent,
                new String(buffer.array()));
        assertFalse("The buffer should be filled", buffer.hasRemaining());
        verify(channelMock, atLeastOnce()).read(any(), anyLong());
    }

    /**
     * Tests that `readFullyOrFail` behaves correctly if multiple `FileChannel.read` operations are required to fill
     * the destination buffer.
     */
    @Test
    public void testReadFullyWithPartialFileChannelReads() throws IOException {
        FileChannel channelMock = mock(FileChannel.class);
        final int bufferSize = 100;
        String expectedBufferContent = fileChannelMockExpectReadWithRandomBytes(channelMock, bufferSize);
        ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
        Utils.readFully(channelMock, buffer, 0L);
        assertEquals("The buffer should be populated correctly.", expectedBufferContent,
                new String(buffer.array()));
        assertFalse("The buffer should be filled", buffer.hasRemaining());
        verify(channelMock, atLeastOnce()).read(any(), anyLong());
    }

    @Test
    public void testReadFullyIfEofIsReached() throws IOException {
        final FileChannel channelMock = mock(FileChannel.class);
        final int bufferSize = 100;
        final String fileChannelContent = "abcdefghkl";
        ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
        when(channelMock.read(any(), anyLong())).then(invocation -> {
            ByteBuffer bufferArg = invocation.getArgument(0);
            bufferArg.put(fileChannelContent.getBytes());
            return -1;
        });
        Utils.readFully(channelMock, buffer, 0L);
        assertEquals("abcdefghkl", new String(buffer.array(), 0, buffer.position()));
        assertEquals(fileChannelContent.length(), buffer.position());
        assertTrue(buffer.hasRemaining());
        verify(channelMock, atLeastOnce()).read(any(), anyLong());
    }

    /**
     * Expectation setter for multiple reads where each one reads random bytes to the buffer.
     *
     * @param channelMock           The mocked FileChannel object
     * @param bufferSize            The buffer size
     * @return                      Expected buffer string
     * @throws IOException          If an I/O error occurs
     */
    private String fileChannelMockExpectReadWithRandomBytes(final FileChannel channelMock,
                                                            final int bufferSize) throws IOException {
        final int step = 20;
        final Random random = new Random();
        int remainingBytes = bufferSize;
        OngoingStubbing<Integer> when = when(channelMock.read(any(), anyLong()));
        StringBuilder expectedBufferContent = new StringBuilder();
        while (remainingBytes > 0) {
            final int bytesRead = remainingBytes < step ? remainingBytes : random.nextInt(step);
            final String stringRead = IntStream.range(0, bytesRead).mapToObj(i -> "a").collect(Collectors.joining());
            expectedBufferContent.append(stringRead);
            when = when.then(invocation -> {
                ByteBuffer buffer = invocation.getArgument(0);
                buffer.put(stringRead.getBytes());
                return bytesRead;
            });
            remainingBytes -= bytesRead;
        }
        return expectedBufferContent.toString();
    }

    private static class TestCloseable implements Closeable {
        private final int id;
        private final IOException closeException;
        private boolean closed;

        TestCloseable(int id, boolean exceptionOnClose) {
            this.id = id;
            this.closeException = exceptionOnClose ? new IOException("Test close exception " + id) : null;
        }

        @Override
        public void close() throws IOException {
            closed = true;
            if (closeException != null)
                throw closeException;
        }

        static TestCloseable[] createCloseables(boolean... exceptionOnClose) {
            TestCloseable[] closeables = new TestCloseable[exceptionOnClose.length];
            for (int i = 0; i < closeables.length; i++)
                closeables[i] = new TestCloseable(i, exceptionOnClose[i]);
            return closeables;
        }

        static void checkClosed(TestCloseable... closeables) {
            for (TestCloseable closeable : closeables)
                assertTrue("Close not invoked for " + closeable.id, closeable.closed);
        }

        static void checkException(IOException e, TestCloseable... closeablesWithException) {
            assertEquals(closeablesWithException[0].closeException, e);
            Throwable[] suppressed = e.getSuppressed();
            assertEquals(closeablesWithException.length - 1, suppressed.length);
            for (int i = 1; i < closeablesWithException.length; i++)
                assertEquals(closeablesWithException[i].closeException, suppressed[i - 1]);
        }
    }

    @Test(timeout = 120000)
    public void testRecursiveDelete() throws IOException {
        Utils.delete(null); // delete of null does nothing.

        // Test that deleting a temporary file works.
        File tempFile = TestUtils.tempFile();
        Utils.delete(tempFile);
        assertFalse(Files.exists(tempFile.toPath()));

        // Test recursive deletes
        File tempDir = TestUtils.tempDirectory();
        File tempDir2 = TestUtils.tempDirectory(tempDir.toPath(), "a");
        TestUtils.tempDirectory(tempDir.toPath(), "b");
        TestUtils.tempDirectory(tempDir2.toPath(), "c");
        Utils.delete(tempDir);
        assertFalse(Files.exists(tempDir.toPath()));
        assertFalse(Files.exists(tempDir2.toPath()));

        // Test that deleting a non-existent directory hierarchy works.
        Utils.delete(tempDir);
        assertFalse(Files.exists(tempDir.toPath()));
    }

    @Test
    public void testConvertTo32BitField() {
        Set<Byte> bytes = mkSet((byte) 0, (byte) 1, (byte) 5, (byte) 10, (byte) 31);
        int bitField = Utils.to32BitField(bytes);
        assertEquals(bytes, Utils.from32BitField(bitField));

        bytes = new HashSet<>();
        bitField = Utils.to32BitField(bytes);
        assertEquals(bytes, Utils.from32BitField(bitField));

        bytes = mkSet((byte) 0, (byte) 11, (byte) 32);
        try {
            Utils.to32BitField(bytes);
            fail("Expected exception not thrown");
        } catch (IllegalArgumentException e) {
        }
    }
}
