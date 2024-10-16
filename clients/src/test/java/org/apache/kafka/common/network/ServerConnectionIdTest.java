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

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ServerConnectionIdTest {

    @Test
    public void testFromString() {
        // Test valid connection id
        String connectionIdString = "localhost:9092-localhost:9093-1-2";
        Optional<ServerConnectionId> serverConnectionIdOptional = ServerConnectionId.fromString(connectionIdString);
        assertTrue(serverConnectionIdOptional.isPresent());
        ServerConnectionId serverConnectionId = serverConnectionIdOptional.get();

        assertEquals("localhost", serverConnectionId.localHost());
        assertEquals(9092, serverConnectionId.localPort());
        assertEquals("localhost", serverConnectionId.remoteHost());
        assertEquals(9093, serverConnectionId.remotePort());
        assertEquals(1, serverConnectionId.processorId());
        assertEquals(2, serverConnectionId.index());

        connectionIdString = "localhost:9092-127.0.1:9093-0-0";
        serverConnectionIdOptional = ServerConnectionId.fromString(connectionIdString);
        assertTrue(serverConnectionIdOptional.isPresent());
        serverConnectionId = serverConnectionIdOptional.get();

        assertEquals("localhost", serverConnectionId.localHost());
        assertEquals(9092, serverConnectionId.localPort());
        assertEquals("127.0.1", serverConnectionId.remoteHost());
        assertEquals(9093, serverConnectionId.remotePort());
        assertEquals(0, serverConnectionId.processorId());
        assertEquals(0, serverConnectionId.index());
    }

    @Test
    public void testFromStringInvalid() {
        // Test invalid connection id params length
        String connectionIdString = "localhost:9092-localhost:9093-1";
        assertFalse(ServerConnectionId.fromString(connectionIdString).isPresent());

        connectionIdString = "localhost:9092-localhost:9093-1-2-3";
        assertFalse(ServerConnectionId.fromString(connectionIdString).isPresent());

        // Invalid separator
        connectionIdString = "localhost-9092-localhost:9093-1-2";
        assertFalse(ServerConnectionId.fromString(connectionIdString).isPresent());

        connectionIdString = "localhost:9092:localhost-9093-1-2";
        assertFalse(ServerConnectionId.fromString(connectionIdString).isPresent());

        // No separator in port
        connectionIdString = "localhost9092-localhost:9093-1-2";
        assertFalse(ServerConnectionId.fromString(connectionIdString).isPresent());

        connectionIdString = "localhost:9092-localhost9093-1-2";
        assertFalse(ServerConnectionId.fromString(connectionIdString).isPresent());

        // Invalid port
        connectionIdString = "localhost:abcd-localhost:9093-1-2";
        assertFalse(ServerConnectionId.fromString(connectionIdString).isPresent());

        connectionIdString = "localhost:9092-localhost:abcd-1-2";
        assertFalse(ServerConnectionId.fromString(connectionIdString).isPresent());

        // Invalid processorId
        connectionIdString = "localhost:9092-localhost:9093-a-2";
        assertFalse(ServerConnectionId.fromString(connectionIdString).isPresent());

        // Invalid index
        connectionIdString = "localhost:9092-localhost:9093-1-b";
        assertFalse(ServerConnectionId.fromString(connectionIdString).isPresent());
    }

    @Test
    public void testGenerateConnectionId() throws IOException {
        Socket socket = mock(Socket.class);
        when(socket.getLocalAddress()).thenReturn(InetAddress.getByName("127.0.0.1"));
        when(socket.getLocalPort()).thenReturn(9092);
        when(socket.getInetAddress()).thenReturn(InetAddress.getByName("127.0.0.1"));
        when(socket.getPort()).thenReturn(9093);

        assertEquals("127.0.0.1:9092-127.0.0.1:9093-0-0", ServerConnectionId.generateConnectionId(socket, 0, 0));
        assertEquals("127.0.0.1:9092-127.0.0.1:9093-1-2", ServerConnectionId.generateConnectionId(socket, 1, 2));
    }
}
