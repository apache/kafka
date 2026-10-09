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
import java.util.concurrent.atomic.AtomicInteger;

public class IntegrationTestUtils {

    private static final AtomicInteger CORRELATION_ID = new AtomicInteger(0);

    public static void sendRequest(Socket socket, byte[] request) throws IOException {
        DataOutputStream outgoing = new DataOutputStream(socket.getOutputStream());
        outgoing.writeInt(request.length);
        outgoing.write(request);
        outgoing.flush();
    }

    public static RequestHeader nextRequestHeader(ApiKeys apiKey, short apiVersion) {
        return new RequestHeader(apiKey, apiVersion, "client-id", CORRELATION_ID.getAndIncrement());
    }

    @SuppressWarnings("unchecked")
    public static <T extends AbstractResponse> T receive(Socket socket, ApiKeys apiKey, short version) throws IOException, ClassCastException {
        var incoming = new DataInputStream(socket.getInputStream());
        int len = incoming.readInt();

        var responseBytes = new byte[len];
        incoming.readFully(responseBytes);

        var responseBuffer = ByteBuffer.wrap(responseBytes);
        ResponseHeader.parse(responseBuffer, apiKey.responseHeaderVersion(version));

        return (T) AbstractResponse.parseResponse(apiKey, new ByteBufferAccessor(responseBuffer), version);
    }

    public static <T extends AbstractResponse> T sendAndReceive(
        AbstractRequest request,
        Socket socket
    ) throws IOException {
        var header = nextRequestHeader(request.apiKey(), request.version());
        sendRequest(socket, Utils.toArray(request.serializeWithHeader(header)));
        return receive(socket, request.apiKey(), request.version());
    }

    public static <T extends AbstractResponse> T connectAndReceive(
        AbstractRequest request,
        int port
    ) throws IOException {
        try (Socket socket = connect(port)) {
            return sendAndReceive(request, socket);
        }
    }

    public static Socket connect(int port) throws IOException {
        return new Socket("localhost", port);
    }
}