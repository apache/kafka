/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import java.io.EOFException;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collections;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

import org.apache.kafka.test.TestUtils;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.Test;


import static org.apache.kafka.common.utils.Utils.formatAddress;
import static org.apache.kafka.common.utils.Utils.getHost;
import static org.apache.kafka.common.utils.Utils.getPort;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
    public void testJoin() {
        assertEquals("", Utils.join(Collections.emptyList(), ","));
        assertEquals("1", Utils.join(Arrays.asList("1"), ","));
        assertEquals("1,2,3", Utils.join(Arrays.asList(1, 2, 3), ","));
    }

    @Test
    public void testAbs() {
        assertEquals(0, Utils.abs(Integer.MIN_VALUE));
        assertEquals(10, Utils.abs(-10));
        assertEquals(10, Utils.abs(10));
        assertEquals(0, Utils.abs(0));
        assertEquals(1, Utils.abs(-1));
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
        FileChannel channelMock = EasyMock.createMock(FileChannel.class);
        final int bufferSize = 100;
        ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
        StringBuilder expectedBufferContent = new StringBuilder();
        fileChannelMockExpectReadWithRandomBytes(channelMock, expectedBufferContent, bufferSize);
        EasyMock.replay(channelMock);
        Utils.readFullyOrFail(channelMock, buffer, 0L, "test");
        assertEquals("The buffer should be populated correctly", expectedBufferContent.toString(),
                new String(buffer.array()));
        assertFalse("The buffer should be filled", buffer.hasRemaining());
        EasyMock.verify(channelMock);
    }

    /**
     * Tests that `readFullyOrFail` behaves correctly if multiple `FileChannel.read` operations are required to fill
     * the destination buffer.
     */
    @Test
    public void testReadFullyWithPartialFileChannelReads() throws IOException {
        FileChannel channelMock = EasyMock.createMock(FileChannel.class);
        final int bufferSize = 100;
        StringBuilder expectedBufferContent = new StringBuilder();
        fileChannelMockExpectReadWithRandomBytes(channelMock, expectedBufferContent, bufferSize);
        EasyMock.replay(channelMock);
        ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
        Utils.readFully(channelMock, buffer, 0L);
        assertEquals("The buffer should be populated correctly.", expectedBufferContent.toString(),
                new String(buffer.array()));
        assertFalse("The buffer should be filled", buffer.hasRemaining());
        EasyMock.verify(channelMock);
    }

    @Test
    public void testReadFullyIfEofIsReached() throws IOException {
        final FileChannel channelMock = EasyMock.createMock(FileChannel.class);
        final int bufferSize = 100;
        final String fileChannelContent = "abcdefghkl";
        ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
        EasyMock.expect(channelMock.size()).andReturn((long) fileChannelContent.length());
        EasyMock.expect(channelMock.read(EasyMock.anyObject(ByteBuffer.class), EasyMock.anyInt())).andAnswer(new IAnswer<Integer>() {
            @Override
            public Integer answer() throws Throwable {
                ByteBuffer buffer = (ByteBuffer) EasyMock.getCurrentArguments()[0];
                buffer.put(fileChannelContent.getBytes());
                return -1;
            }
        });
        EasyMock.replay(channelMock);
        Utils.readFully(channelMock, buffer, 0L);
        assertEquals("abcdefghkl", new String(buffer.array(), 0, buffer.position()));
        assertEquals(buffer.position(), channelMock.size());
        assertTrue(buffer.hasRemaining());
        EasyMock.verify(channelMock);
    }

    /**
     * Expectation setter for multiple reads where each one reads random bytes to the buffer.
     *
     * @param channelMock           The mocked FileChannel object
     * @param expectedBufferContent buffer that will be updated to contain the expected buffer content after each
     *                              `FileChannel.read` invocation
     * @param bufferSize            The buffer size
     * @throws IOException          If an I/O error occurs
     */
    private void fileChannelMockExpectReadWithRandomBytes(final FileChannel channelMock,
                                                          final StringBuilder expectedBufferContent,
                                                          final int bufferSize) throws IOException {
        final int step = 20;
        final Random random = new Random();
        int remainingBytes = bufferSize;
        while (remainingBytes > 0) {
            final int mockedBytesRead = remainingBytes < step ? remainingBytes : random.nextInt(step);
            final StringBuilder sb = new StringBuilder();
            EasyMock.expect(channelMock.read(EasyMock.anyObject(ByteBuffer.class), EasyMock.anyInt())).andAnswer(new IAnswer<Integer>() {
                @Override
                public Integer answer() throws Throwable {
                    ByteBuffer buffer = (ByteBuffer) EasyMock.getCurrentArguments()[0];
                    for (int i = 0; i < mockedBytesRead; i++)
                        sb.append("a");
                    buffer.put(sb.toString().getBytes());
                    expectedBufferContent.append(sb);
                    return mockedBytesRead;
                }
            });
            remainingBytes -= mockedBytesRead;
        }
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
}
