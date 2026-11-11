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
import org.apache.kafka.common.utils.internals.OperatingSystem;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class KafkaChannelTest {

    @Test
    public void testSending() throws IOException {
        Authenticator authenticator = Mockito.mock(Authenticator.class);
        TransportLayer transport = Mockito.mock(TransportLayer.class);
        MemoryPool pool = Mockito.mock(MemoryPool.class);
        ChannelMetadataRegistry metadataRegistry = Mockito.mock(ChannelMetadataRegistry.class);

        KafkaChannel channel = new KafkaChannel("0", transport, () -> authenticator,
            1024, pool, metadataRegistry);
        ByteBufferSend send = ByteBufferSend.sizePrefixed(ByteBuffer.wrap(TestUtils.randomBytes(128)));
        NetworkSend networkSend = new NetworkSend("0", send);

        channel.setSend(networkSend);
        assertTrue(channel.hasSend());
        assertThrows(IllegalStateException.class, () -> channel.setSend(networkSend));

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
        assertEquals(networkSend, channel.maybeCompleteSend());
    }

    @Test
    public void testWriteTurnsTcpNoDelayOffDuringSendOnLinux() throws IOException {
        assumeTrue(OperatingSystem.IS_LINUX);
        Authenticator authenticator = Mockito.mock(Authenticator.class);
        TransportLayer transport = Mockito.mock(TransportLayer.class);
        SocketChannel socketChannel = Mockito.mock(SocketChannel.class);
        Mockito.when(transport.socketChannel()).thenReturn(socketChannel);
        MemoryPool pool = Mockito.mock(MemoryPool.class);
        ChannelMetadataRegistry metadataRegistry = Mockito.mock(ChannelMetadataRegistry.class);

        KafkaChannel channel = new KafkaChannel("0", transport, () -> authenticator,
            1024, pool, metadataRegistry);
        Send send = Mockito.mock(Send.class);
        channel.setSend(new NetworkSend("0", send));

        Mockito.when(send.writeTo(transport)).thenReturn(4L);
        assertEquals(4L, channel.write());
        InOrder inOrder = Mockito.inOrder(socketChannel, send);
        inOrder.verify(socketChannel).setOption(StandardSocketOptions.TCP_NODELAY, false);
        inOrder.verify(send).writeTo(transport);
        inOrder.verify(socketChannel).setOption(StandardSocketOptions.TCP_NODELAY, true);

        Mockito.when(send.writeTo(transport)).thenThrow(new IOException("Connection reset"));
        assertThrows(IOException.class, channel::write);
        Mockito.verify(socketChannel, Mockito.times(2)).setOption(StandardSocketOptions.TCP_NODELAY, true);
    }

    @Test
    public void testWriteKeepsTcpNoDelayForByteBufferSendWithoutSsl() throws IOException {
        assumeTrue(OperatingSystem.IS_LINUX);
        Authenticator authenticator = Mockito.mock(Authenticator.class);
        TransportLayer plaintext = Mockito.mock(TransportLayer.class);
        SslTransportLayer ssl = Mockito.mock(SslTransportLayer.class);
        SocketChannel socketChannel = Mockito.mock(SocketChannel.class);
        Mockito.when(plaintext.socketChannel()).thenReturn(socketChannel);
        Mockito.when(ssl.socketChannel()).thenReturn(socketChannel);
        MemoryPool pool = Mockito.mock(MemoryPool.class);
        ChannelMetadataRegistry metadataRegistry = Mockito.mock(ChannelMetadataRegistry.class);

        KafkaChannel plaintextChannel = new KafkaChannel("0", plaintext, () -> authenticator,
            1024, pool, metadataRegistry);
        plaintextChannel.setSend(new NetworkSend("0", ByteBufferSend.sizePrefixed(ByteBuffer.wrap(TestUtils.randomBytes(128)))));
        plaintextChannel.write();
        Mockito.verify(socketChannel, Mockito.never()).setOption(Mockito.any(), Mockito.any());

        KafkaChannel sslChannel = new KafkaChannel("1", ssl, () -> authenticator,
            1024, pool, metadataRegistry);
        sslChannel.setSend(new NetworkSend("1", ByteBufferSend.sizePrefixed(ByteBuffer.wrap(TestUtils.randomBytes(128)))));
        sslChannel.write();
        Mockito.verify(socketChannel).setOption(StandardSocketOptions.TCP_NODELAY, false);
        Mockito.verify(socketChannel).setOption(StandardSocketOptions.TCP_NODELAY, true);
    }

    @Test
    public void testReceiving() throws IOException {
        Authenticator authenticator = Mockito.mock(Authenticator.class);
        TransportLayer transport = Mockito.mock(TransportLayer.class);
        MemoryPool pool = Mockito.mock(MemoryPool.class);
        ChannelMetadataRegistry metadataRegistry = Mockito.mock(ChannelMetadataRegistry.class);

        ArgumentCaptor<Integer> sizeCaptor = ArgumentCaptor.forClass(Integer.class);
        Mockito.when(pool.tryAllocate(sizeCaptor.capture())).thenAnswer(invocation ->
            ByteBuffer.allocate(sizeCaptor.getValue())
        );

        KafkaChannel channel = new KafkaChannel("0", transport, () -> authenticator,
            1024, pool, metadataRegistry);

        ArgumentCaptor<ByteBuffer> bufferCaptor = ArgumentCaptor.forClass(ByteBuffer.class);
        Mockito.when(transport.read(bufferCaptor.capture())).thenAnswer(invocation -> {
            bufferCaptor.getValue().putInt(128);
            return 4;
        }).thenReturn(0);
        assertEquals(4, channel.read());
        assertEquals(4, channel.currentReceive().bytesRead());
        assertNull(channel.maybeCompleteReceive());

        Mockito.reset(transport);
        Mockito.when(transport.read(bufferCaptor.capture())).thenAnswer(invocation -> {
            bufferCaptor.getValue().put(TestUtils.randomBytes(64));
            return 64;
        });
        assertEquals(64, channel.read());
        assertEquals(68, channel.currentReceive().bytesRead());
        assertNull(channel.maybeCompleteReceive());

        Mockito.reset(transport);
        Mockito.when(transport.read(bufferCaptor.capture())).thenAnswer(invocation -> {
            bufferCaptor.getValue().put(TestUtils.randomBytes(64));
            return 64;
        });
        assertEquals(64, channel.read());
        assertEquals(132, channel.currentReceive().bytesRead());
        assertNotNull(channel.maybeCompleteReceive());
        assertNull(channel.currentReceive());
    }

}
