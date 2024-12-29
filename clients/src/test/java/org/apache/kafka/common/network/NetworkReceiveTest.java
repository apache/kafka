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

import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ScatteringByteChannel;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class NetworkReceiveTest {

    @Test
    public void testBytesRead() throws IOException {
        NetworkReceive receive = new NetworkReceive(128, "0");
        assertEquals(0, receive.bytesRead());

        ScatteringByteChannel channel = Mockito.mock(ScatteringByteChannel.class);

        ArgumentCaptor<ByteBuffer> bufferCaptor = ArgumentCaptor.forClass(ByteBuffer.class);
        Mockito.when(channel.read(bufferCaptor.capture())).thenAnswer(invocation -> {
            bufferCaptor.getValue().putInt(128);
            return 4;
        }).thenReturn(0);

        assertEquals(4, receive.readFrom(channel));
        assertEquals(4, receive.bytesRead());
        assertFalse(receive.complete());

        Mockito.reset(channel);
        Mockito.when(channel.read(bufferCaptor.capture())).thenAnswer(invocation -> {
            bufferCaptor.getValue().put(TestUtils.randomBytes(64));
            return 64;
        });

        assertEquals(64, receive.readFrom(channel));
        assertEquals(68, receive.bytesRead());
        assertFalse(receive.complete());

        Mockito.reset(channel);
        Mockito.when(channel.read(bufferCaptor.capture())).thenAnswer(invocation -> {
            bufferCaptor.getValue().put(TestUtils.randomBytes(64));
            return 64;
        });

        assertEquals(64, receive.readFrom(channel));
        assertEquals(132, receive.bytesRead());
        assertTrue(receive.complete());
    }

    @Test
    public void testRequiredMemoryAmountKnownWhenNotSet() {
        NetworkReceive receive = new NetworkReceive("0");
        assertFalse(receive.requiredMemoryAmountKnown(), "Memory amount should not be known before read.");
    }

    @Test
    public void testRequiredMemoryAmountKnownWhenSet() throws IOException {
        NetworkReceive receive = new NetworkReceive(128, "0");

        ScatteringByteChannel channel = Mockito.mock(ScatteringByteChannel.class);

        ArgumentCaptor<ByteBuffer> bufferCaptor = ArgumentCaptor.forClass(ByteBuffer.class);
        Mockito.when(channel.read(bufferCaptor.capture())).thenAnswer(invocation -> {
            bufferCaptor.getValue().putInt(64);
            return 4;
        });

        receive.readFrom(channel);
        assertTrue(receive.requiredMemoryAmountKnown(), "Memory amount should be known after read.");
    }

    @Test
    public void testSizeWithPredefineBuffer() {
        int payloadSize = 8;
        int expectedTotalSize = 4 + payloadSize; // 4 bytes for size buffer + payload size

        ByteBuffer payloadBuffer = ByteBuffer.allocate(payloadSize);
        IntStream.range(0, payloadSize).forEach(i -> payloadBuffer.put((byte) i));

        NetworkReceive networkReceive = new NetworkReceive("0", payloadBuffer);
        assertEquals(expectedTotalSize, networkReceive.size(), "The total size should be the sum of the size buffer and payload.");
    }

    @Test
    public void testSizeAfterRead() throws IOException {
        int payloadSize = 32;
        int expectedTotalSize = 4 + payloadSize; // 4 bytes for size buffer + payload size
        NetworkReceive receive = new NetworkReceive(128, "0");

        ScatteringByteChannel channel = Mockito.mock(ScatteringByteChannel.class);

        ArgumentCaptor<ByteBuffer> bufferCaptor = ArgumentCaptor.forClass(ByteBuffer.class);
        Mockito.when(channel.read(bufferCaptor.capture())).thenAnswer(invocation -> {
            bufferCaptor.getValue().putInt(payloadSize);
            return 4;
        });

        receive.readFrom(channel);
        assertEquals(expectedTotalSize, receive.size(), "The total size should be the sum of the size buffer and receive size.");
    }
}
