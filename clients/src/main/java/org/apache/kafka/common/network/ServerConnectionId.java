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

import java.net.Socket;
import java.util.Optional;

/**
 * ServerConnectionId is used to uniquely identify a connection between a client and a server.
 */
public class ServerConnectionId {

    private final String localHost;
    private final int localPort;
    private final String remoteHost;
    private final int remotePort;
    private final int processorId;
    private final int index;

    public ServerConnectionId(
        BrokerEndPoint localEndpoint,
        BrokerEndPoint remoteEndpoint,
        int processorId,
        int index
    ) {
        this(localEndpoint.host(), localEndpoint.port(), remoteEndpoint.host(), remoteEndpoint.port(), processorId, index);
    }

    public ServerConnectionId(
        String localHost,
        int localPort,
        String remoteHost,
        int remotePort,
        int processorId,
        int index
    ) {
        this.localHost = localHost;
        this.localPort = localPort;
        this.remoteHost = remoteHost;
        this.remotePort = remotePort;
        this.processorId = processorId;
        this.index = index;
    }

    public String localHost() {
        return localHost;
    }

    public int localPort() {
        return localPort;
    }

    public String remoteHost() {
        return remoteHost;
    }

    public int remotePort() {
        return remotePort;
    }

    public int processorId() {
        return processorId;
    }

    public int index() {
        return index;
    }

    /**
     * Returns an optional ServerConnectionId object from the given connection ID string.
     *
     * @param connectionIdString The connection ID string to parse.
     * @return An optional ServerConnectionId object.
     */
    public static Optional<ServerConnectionId> fromString(String connectionIdString) {
        String[] split = connectionIdString.split("-");
        if (split.length != 4) {
            return Optional.empty();
        }

        try {
            return BrokerEndPoint.parseHostPort(split[0])
                .flatMap(localHost -> BrokerEndPoint.parseHostPort(split[1])
                    .map(remoteHost -> new ServerConnectionId(localHost, remoteHost, Integer.parseInt(split[2]), Integer.parseInt(split[3]))));
        } catch (NumberFormatException e) {
            return Optional.empty();
        }
    }

    /**
     * Generates a unique connection ID for the given socket.
     *
     * @param socket The socket for which the connection ID is to be generated.
     * @param processorId The ID of the server processor that will handle this connection.
     * @param connectionIndex The index to be used in the connection ID to ensure uniqueness.
     * @return A string representing the unique connection ID.
     */
    public static String generateConnectionId(Socket socket, int processorId, int connectionIndex) {
        String localHost = socket.getLocalAddress().getHostAddress();
        int localPort = socket.getLocalPort();
        String remoteHost = socket.getInetAddress().getHostAddress();
        int remotePort = socket.getPort();
        return localHost + ":" + localPort + "-" + remoteHost + ":" + remotePort + "-" + processorId + "-" + connectionIndex;
    }
}
