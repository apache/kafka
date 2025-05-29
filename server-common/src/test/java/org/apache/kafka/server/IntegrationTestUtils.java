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
package org.apache.kafka.server;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.ResponseHeader;
import org.apache.kafka.common.utils.Utils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;

public class IntegrationTestUtils {

    private static int correlationId = 0;

    public static void sendRequest(Socket socket, byte[] request) throws IOException {
        DataOutputStream outgoing = new DataOutputStream(socket.getOutputStream());
        outgoing.writeInt(request.length);
        outgoing.write(request);
        outgoing.flush();
    }

    private static void sendWithHeader(AbstractRequest request, RequestHeader header, Socket socket) throws IOException {
        byte[] serializedBytes = Utils.toArray(request.serializeWithHeader(header));
        sendRequest(socket, serializedBytes);
    }

    public static RequestHeader nextRequestHeader(ApiKeys apiKey, short apiVersion, String clientId, Integer correlationIdOpt) {
        int correlationId = (correlationIdOpt != null) ? correlationIdOpt : ++IntegrationTestUtils.correlationId;
        return new RequestHeader(apiKey, apiVersion, clientId, correlationId);
    }

    public static RequestHeader nextRequestHeader(ApiKeys apiKey, short apiVersion) {

        return new RequestHeader(apiKey, apiVersion, "client-id", 1);
    }

    public static void send(AbstractRequest request, Socket socket, String clientId, Integer correlationId) throws IOException {
        RequestHeader header = nextRequestHeader(request.apiKey(), request.version(), clientId, correlationId);
        sendWithHeader(request, header, socket);
    }

    public static void send(AbstractRequest request, Socket socket) throws IOException {
        RequestHeader header = nextRequestHeader(request.apiKey(), request.version(), "client-id", 0);
        sendWithHeader(request, header, socket);
    }

    public static <T extends AbstractResponse> T receive(Socket socket, ApiKeys apiKey, short version) throws IOException, ClassCastException {
        DataInputStream incoming = new DataInputStream(socket.getInputStream());
        int len = incoming.readInt();

        byte[] responseBytes = new byte[len];
        incoming.readFully(responseBytes);

        ByteBuffer responseBuffer = ByteBuffer.wrap(responseBytes);
        ResponseHeader.parse(responseBuffer, apiKey.responseHeaderVersion(version));

        AbstractResponse response = AbstractResponse.parseResponse(apiKey, new ByteBufferAccessor(responseBuffer), version);
        if (response.getClass().isAssignableFrom(response.getClass())) {
            return (T) response;
        } else {
            throw new ClassCastException("Expected response with type " + response.getClass() + ", but found " + response.getClass());
        }
    }

    public static <T extends AbstractResponse> T sendAndReceive(
            AbstractRequest request,
            Socket socket,
            String clientId,
            Integer correlationId
    ) throws IOException {
        send(request, socket, clientId, correlationId);
        return (T) receive(socket, request.apiKey(), request.version());
    }

    public static <T extends AbstractResponse> T sendAndReceive(
            AbstractRequest request,
            Socket socket
    ) throws IOException {
        return sendAndReceive(request, socket, "client-id", 0);
    }



    public static <T extends AbstractResponse> T connectAndReceive(
            AbstractRequest request,
            int port
    ) throws IOException {
        try (Socket socket = connect(port)){
            return sendAndReceive(request, socket);
        } finally {
            socket.close();
        }
    }

    public static Socket connect(int port) throws IOException {
        return new Socket("localhost", port);
    }
}