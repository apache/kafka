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

import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.test.TestUtils;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class KafkaChannelTest {

    @Test
    public void testSending() throws IOException {
        Authenticator authenticator = Mockito.mock(Authenticator.class);
        TransportLayer transport = Mockito.mock(TransportLayer.class);
        MemoryPool pool = Mockito.mock(MemoryPool.class);
        ChannelMetadataRegistry metadataRegistry = Mockito.mock(ChannelMetadataRegistry.class);

        KafkaChannel channel = new KafkaChannel("0", transport, () -> authenticator,
            1024, pool, metadataRegistry);
        NetworkSend send = new NetworkSend("0", ByteBuffer.wrap(TestUtils.randomBytes(128)));

        channel.setSend(send);
        assertTrue(channel.hasSend());
        assertThrows(IllegalStateException.class, () -> channel.setSend(send));

        Mockito.when(transport.write(Mockito.any(ByteBuffer[].class))).thenReturn(4L);
        assertEquals(4L, channel.write());
        assertEquals(128, send.remaining());
        assertNull(channel.maybeCompleteSend());

        Mockito.when(transport.write(Mockito.any(ByteBuffer[].class))).thenReturn(64L);
        assertEquals(64, channel.write());
        assertEquals(64, send.remaining());
        assertNull(channel.maybeCompleteSend());

        Mockito.when(transport.write(Mockito.any(ByteBuffer[].class))).thenReturn(64L);
        assertEquals(64, channel.write());
        assertEquals(0, send.remaining());
        assertEquals(send, channel.maybeCompleteSend());
    }

    @Test
    public void testReceiving() throws IOException {
        Authenticator authenticator = Mockito.mock(Authenticator.class);
        TransportLayer transport = Mockito.mock(TransportLayer.class);
        MemoryPool pool = Mockito.mock(MemoryPool.class);
        ChannelMetadataRegistry metadataRegistry = Mockito.mock(ChannelMetadataRegistry.class);

        ByteBuffer testData = (ByteBuffer) ByteBuffer.allocate(132).putInt(128)
                .put(TestUtils.randomBytes(128)).rewind();

        ArgumentCaptor<Integer> sizeCaptor = ArgumentCaptor.forClass(Integer.class);
        Mockito.when(pool.tryAllocate(sizeCaptor.capture())).thenAnswer(invocation -> {
            return ByteBuffer.allocate(sizeCaptor.getValue());
        });

        KafkaChannel channel = new KafkaChannel("0", transport, () -> authenticator,
            1024, pool, metadataRegistry);

        ArgumentCaptor<ByteBuffer> bufferCaptor = ArgumentCaptor.forClass(ByteBuffer.class);
        Mockito.when(transport.read(bufferCaptor.capture())).thenAnswer(invocation -> {
            int remaining = bufferCaptor.getValue().remaining();

            ByteBuffer slice = testData.slice();
            slice.limit(slice.position() + remaining);

            // write the test data into to the test
            bufferCaptor.getValue().put(slice);

            testData.position(testData.position() + remaining);

            return remaining;
        }).thenReturn(0);

        assertEquals(4, channel.read());
        assertEquals(4, channel.currentReceive().bytesRead());
        assertNull(channel.maybeCompleteReceive());

        Mockito.reset(transport);
        Mockito.when(transport.read(bufferCaptor.capture())).thenAnswer(invocation -> {
            int remaining = bufferCaptor.getValue().remaining();

            ByteBuffer slice = testData.slice();
            slice.limit(slice.position() + remaining);

            // write the test data into to the test
            bufferCaptor.getValue().put(slice);

            testData.position(testData.position() + remaining);

            return remaining;
        });

        // Read the remaining buffer
        assertEquals(128, channel.read());

        // Read the entire size (4) + payload (128)
        assertEquals(132, channel.currentReceive().bytesRead());

        assertNotNull(channel.maybeCompleteReceive());
        assertNull(channel.currentReceive());
    }

}
