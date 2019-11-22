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
import java.net.InetAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

public class KafkaChannelTest {

    @Test
    public void testSending() throws IOException {
        Authenticator authenticator = Mockito.mock(Authenticator.class);
        TransportLayer transport = Mockito.mock(TransportLayer.class);
        MemoryPool pool = Mockito.mock(MemoryPool.class);

        KafkaChannel channel = new KafkaChannel("0", transport, () -> authenticator, 1024, pool);
        NetworkSend send = new NetworkSend("0", ByteBuffer.wrap(TestUtils.randomBytes(128)));

        channel.setSend(send);
        assertTrue(channel.hasSend());
        assertThrows(IllegalStateException.class, () -> channel.setSend(send));

        when(transport.write(Mockito.any(ByteBuffer[].class))).thenReturn(4L);
        assertEquals(4L, channel.write());
        assertEquals(128, send.remaining());
        assertNull(channel.maybeCompleteSend());

        when(transport.write(Mockito.any(ByteBuffer[].class))).thenReturn(64L);
        assertEquals(64, channel.write());
        assertEquals(64, send.remaining());
        assertNull(channel.maybeCompleteSend());

        when(transport.write(Mockito.any(ByteBuffer[].class))).thenReturn(64L);
        assertEquals(64, channel.write());
        assertEquals(0, send.remaining());
        assertEquals(send, channel.maybeCompleteSend());
    }

    @Test
    public void testReceiving() throws IOException {
        Authenticator authenticator = Mockito.mock(Authenticator.class);
        TransportLayer transport = Mockito.mock(TransportLayer.class);
        MemoryPool pool = Mockito.mock(MemoryPool.class);

        ArgumentCaptor<Integer> sizeCaptor = ArgumentCaptor.forClass(Integer.class);
        when(pool.tryAllocate(sizeCaptor.capture())).thenAnswer(invocation -> {
            return ByteBuffer.allocate(sizeCaptor.getValue());
        });

        KafkaChannel channel = new KafkaChannel("0", transport, () -> authenticator, 1024, pool);

        ArgumentCaptor<ByteBuffer> bufferCaptor = ArgumentCaptor.forClass(ByteBuffer.class);
        when(transport.read(bufferCaptor.capture())).thenAnswer(invocation -> {
            bufferCaptor.getValue().putInt(128);
            return 4;
        }).thenReturn(0);
        assertEquals(4, channel.read());
        assertEquals(4, channel.currentReceive().bytesRead());
        assertNull(channel.maybeCompleteReceive());

        Mockito.reset(transport);
        when(transport.read(bufferCaptor.capture())).thenAnswer(invocation -> {
            bufferCaptor.getValue().put(TestUtils.randomBytes(64));
            return 64;
        });
        assertEquals(64, channel.read());
        assertEquals(68, channel.currentReceive().bytesRead());
        assertNull(channel.maybeCompleteReceive());

        Mockito.reset(transport);
        when(transport.read(bufferCaptor.capture())).thenAnswer(invocation -> {
            bufferCaptor.getValue().put(TestUtils.randomBytes(64));
            return 64;
        });
        assertEquals(64, channel.read());
        assertEquals(132, channel.currentReceive().bytesRead());
        assertNotNull(channel.maybeCompleteReceive());
        assertNull(channel.currentReceive());
    }

    @Test
    public void testSocketDescriptionWithNonConnectedSocket() {
        Authenticator authenticator = Mockito.mock(Authenticator.class);
        TransportLayer transport = Mockito.mock(TransportLayer.class);
        SocketChannel socketChannel = Mockito.mock(SocketChannel.class);
        InetAddress localAddress = Mockito.mock(InetAddress.class);
        Socket socket = Mockito.mock(Socket.class);

        when(socketChannel.socket()).thenReturn(socket);
        when(transport.socketChannel()).thenReturn(socketChannel);
        when(localAddress.toString()).thenReturn("localhost");
        when(socket.getInetAddress()).thenReturn(null);
        when(socket.getLocalAddress()).thenReturn(localAddress);
        MemoryPool pool = Mockito.mock(MemoryPool.class);

        KafkaChannel channel = new KafkaChannel("0", transport, () -> authenticator, 1024, pool);
        assertEquals("(Remote Address: [non-connected socket], Local Address: localhost)", channel.socketDescription());
    }

    @Test
    public void testSocketDescriptionWithSocket() {
        Authenticator authenticator = Mockito.mock(Authenticator.class);
        TransportLayer transport = Mockito.mock(TransportLayer.class);
        SocketChannel socketChannel = Mockito.mock(SocketChannel.class);
        InetAddress localAddress = Mockito.mock(InetAddress.class);
        InetAddress address = Mockito.mock(InetAddress.class);
        Socket socket = Mockito.mock(Socket.class);

        when(socketChannel.socket()).thenReturn(socket);
        when(transport.socketChannel()).thenReturn(socketChannel);
        when(localAddress.toString()).thenReturn("localhost");
        when(address.toString()).thenReturn("192.192.192.192");
        when(socket.getInetAddress()).thenReturn(address);
        when(socket.getLocalAddress()).thenReturn(localAddress);
        MemoryPool pool = Mockito.mock(MemoryPool.class);

        KafkaChannel channel = new KafkaChannel("0", transport, () -> authenticator, 1024, pool);
        assertEquals("(Remote Address: 192.192.192.192, Local Address: localhost)", channel.socketDescription());
    }

    @Test
    public void testSocketDescriptionWithNoSocket() {
        Authenticator authenticator = Mockito.mock(Authenticator.class);
        TransportLayer transport = Mockito.mock(TransportLayer.class);
        SocketChannel socketChannel = Mockito.mock(SocketChannel.class);

        when(socketChannel.socket()).thenReturn(null);
        when(transport.socketChannel()).thenReturn(socketChannel);
        MemoryPool pool = Mockito.mock(MemoryPool.class);

        KafkaChannel channel = new KafkaChannel("0", transport, () -> authenticator, 1024, pool);
        assertEquals("[non-existing socket]", channel.socketDescription());
    }
}
