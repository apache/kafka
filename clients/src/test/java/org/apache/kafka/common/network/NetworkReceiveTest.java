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
package org.apache.kafka.common.network;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ScatteringByteChannel;

import org.apache.kafka.test.TestUtils;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class NetworkReceiveTest {

    @Test
    public void testBytesRead() throws IOException {
        NetworkReceive receive = new NetworkReceive(128, "0");
        assertEquals(0, receive.bytesRead());

        ScatteringByteChannel channel = Mockito.mock(ScatteringByteChannel.class);

        ByteBuffer testData = (ByteBuffer) ByteBuffer.allocate(4 + 128).putInt(128)
                .put(TestUtils.randomBytes(128)).rewind();

        ByteBuffer testSizeRead = (ByteBuffer) testData.duplicate().position(0).limit(4);
        ArgumentCaptor<ByteBuffer> bufferCaptor = ArgumentCaptor.forClass(ByteBuffer.class);
        Mockito.when(channel.read(bufferCaptor.capture())).thenAnswer(invocation -> {
            ByteBuffer inputBuffer = invocation.<ByteBuffer>getArgument(0);
            int remaining = Math.min(testSizeRead.remaining(), inputBuffer.remaining());

            ByteBuffer slice = (ByteBuffer) testSizeRead.slice().limit(remaining);

            // write the test data into to the test
            inputBuffer.put(slice);

            testSizeRead.position(testSizeRead.position() + remaining);

            return remaining;
        });

        assertEquals(4, receive.readFrom(channel));
        assertEquals(4, receive.bytesRead());
        assertFalse(receive.complete());

        ByteBuffer testPayloadOne = (ByteBuffer) testData.duplicate().position(4).limit(4 + 64);

        Mockito.reset(channel);
        Mockito.when(channel.read(bufferCaptor.capture())).thenAnswer(invocation -> {
            ByteBuffer inputBuffer = invocation.<ByteBuffer>getArgument(0);
            int remaining = Math.min(testPayloadOne.remaining(), inputBuffer.remaining());

            ByteBuffer slice = (ByteBuffer) testPayloadOne.slice().limit(remaining);

            // write the test data into to the test
            inputBuffer.put(slice);

            testPayloadOne.position(testPayloadOne.position() + remaining);

            return remaining;
        });

        assertEquals(64, receive.readFrom(channel));
        assertEquals(68, receive.bytesRead());
        assertFalse(receive.complete());


        ByteBuffer testPayloadTwo =
                (ByteBuffer) testData.duplicate().position(4 + 64).limit(4 + 64 + 64);

        Mockito.reset(channel);
        Mockito.when(channel.read(bufferCaptor.capture())).thenAnswer(invocation -> {
            ByteBuffer inputBuffer = invocation.<ByteBuffer>getArgument(0);
            int remaining = Math.min(testPayloadTwo.remaining(), inputBuffer.remaining());

            ByteBuffer slice = (ByteBuffer) testPayloadTwo.slice().limit(remaining);

            // write the test data into to the test
            inputBuffer.put(slice);

            testPayloadTwo.position(testPayloadTwo.position() + remaining);

            return remaining;
        });

        assertEquals(64, receive.readFrom(channel));
        assertEquals(132, receive.bytesRead());
        assertTrue(receive.complete());
    }

    /**
     * Emulate a plain-text client connecting to an SSL-enabled server.
     */
    @Test(expected = InvalidReceiveException.class)
    public void testAccidentalSSLRead() throws IOException {
        NetworkReceive receive = new NetworkReceive(128, "0");
        assertEquals(0, receive.bytesRead());

        ScatteringByteChannel channel = Mockito.mock(ScatteringByteChannel.class);

        // Simulate a SSL ALERT response
        // Occurs when submitting a plain-text message to a SSL server
        byte[] sslResponse = new byte[] {(byte) 0x15, (byte) 0x03, (byte) 0x03, (byte) 0x00, (byte) 0x02,
            (byte) 0x02, (byte) 0x50};

        ByteBuffer testData = (ByteBuffer) ByteBuffer.allocate(7).put(sslResponse).rewind();

        ByteBuffer testSizeRead = (ByteBuffer) testData.duplicate().position(0).limit(4);
        ArgumentCaptor<ByteBuffer> bufferCaptor = ArgumentCaptor.forClass(ByteBuffer.class);
        Mockito.when(channel.read(bufferCaptor.capture())).thenAnswer(invocation -> {
            ByteBuffer inputBuffer = invocation.<ByteBuffer>getArgument(0);
            int remaining = Math.min(testSizeRead.remaining(), inputBuffer.remaining());

            ByteBuffer slice = (ByteBuffer) testSizeRead.slice().limit(remaining);

            // write the test data into to the test
            inputBuffer.put(slice);

            testSizeRead.position(testSizeRead.position() + remaining);

            return remaining;
        });

        assertEquals(4, receive.readFrom(channel));
        assertEquals(4, receive.bytesRead());
        assertFalse(receive.complete());

        ByteBuffer testPayloadOne = (ByteBuffer) testData.duplicate().position(4).limit(7);

        Mockito.reset(channel);
        Mockito.when(channel.read(bufferCaptor.capture())).thenAnswer(invocation -> {
            ByteBuffer inputBuffer = invocation.<ByteBuffer>getArgument(0);
            int remaining = Math.min(testPayloadOne.remaining(), inputBuffer.remaining());

            ByteBuffer slice = (ByteBuffer) testPayloadOne.slice().limit(remaining);

            // write the test data into to the test
            inputBuffer.put(slice);

            testPayloadOne.position(testPayloadOne.position() + remaining);

            return remaining;
        });

        receive.readFrom(channel);
    }

}
