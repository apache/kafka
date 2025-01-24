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
package org.apache.kafka.common.requests;

import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersionCollection;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.SaslAuthenticateRequestData;
import org.apache.kafka.common.network.ClientInformation;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;

import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RequestContextTest {

    @Test
    public void testSerdeUnsupportedApiVersionRequest() throws Exception {
        int correlationId = 23423;

        RequestHeader header = new RequestHeader(ApiKeys.API_VERSIONS, Short.MAX_VALUE, "", correlationId);
        RequestContext context = new RequestContext(header, "0", InetAddress.getLocalHost(), KafkaPrincipal.ANONYMOUS,
                new ListenerName("ssl"), SecurityProtocol.SASL_SSL, ClientInformation.EMPTY, false);
        assertEquals(0, context.apiVersion());

        // Write some garbage to the request buffer. This should be ignored since we will treat
        // the unknown version type as v0 which has an empty request body.
        ByteBuffer requestBuffer = ByteBuffer.allocate(8);
        requestBuffer.putInt(3709234);
        requestBuffer.putInt(29034);
        requestBuffer.flip();

        RequestAndSize requestAndSize = context.parseRequest(requestBuffer);
        assertInstanceOf(ApiVersionsRequest.class, requestAndSize.request);
        ApiVersionsRequest request = (ApiVersionsRequest) requestAndSize.request;
        assertTrue(request.hasUnsupportedRequestVersion());

        Send send = context.buildResponseSend(new ApiVersionsResponse(new ApiVersionsResponseData()
            .setThrottleTimeMs(0)
            .setErrorCode(Errors.UNSUPPORTED_VERSION.code())
            .setApiKeys(new ApiVersionCollection())));
        ByteBufferChannel channel = new ByteBufferChannel(256);
        send.writeTo(channel);

        ByteBuffer responseBuffer = channel.buffer();
        responseBuffer.flip();
        responseBuffer.getInt(); // strip off the size

        ResponseHeader responseHeader = ResponseHeader.parse(responseBuffer,
            ApiKeys.API_VERSIONS.responseHeaderVersion(header.apiVersion()));
        assertEquals(correlationId, responseHeader.correlationId());

        ApiVersionsResponse response = (ApiVersionsResponse) AbstractResponse.parseResponse(ApiKeys.API_VERSIONS,
            responseBuffer, (short) 0);
        assertEquals(Errors.UNSUPPORTED_VERSION.code(), response.data().errorCode());
        assertTrue(response.data().apiKeys().isEmpty());
    }

    @Test
    public void testEnvelopeResponseSerde() throws Exception {
        CreateTopicsResponseData.CreatableTopicResultCollection collection =
            new CreateTopicsResponseData.CreatableTopicResultCollection();
        collection.add(new CreateTopicsResponseData.CreatableTopicResult()
            .setTopicConfigErrorCode(Errors.CLUSTER_AUTHORIZATION_FAILED.code())
            .setNumPartitions(5));
        CreateTopicsResponseData expectedResponse = new CreateTopicsResponseData()
            .setThrottleTimeMs(10)
            .setTopics(collection);

        int correlationId = 15;
        String clientId = "clientId";
        RequestHeader header = new RequestHeader(ApiKeys.CREATE_TOPICS, ApiKeys.CREATE_TOPICS.latestVersion(),
            clientId, correlationId);

        RequestContext context = new RequestContext(header, "0", InetAddress.getLocalHost(),
            KafkaPrincipal.ANONYMOUS, new ListenerName("ssl"), SecurityProtocol.SASL_SSL,
            ClientInformation.EMPTY, true);

        ByteBuffer buffer = context.buildResponseEnvelopePayload(new CreateTopicsResponse(expectedResponse));
        assertEquals(buffer.capacity(), buffer.limit(), "Buffer limit and capacity should be the same");
        CreateTopicsResponse parsedResponse = (CreateTopicsResponse) AbstractResponse.parseResponse(buffer, header);
        assertEquals(expectedResponse, parsedResponse.data());
    }

    @Test
    public void testInvalidRequestForImplicitHashCollection() throws UnknownHostException {
        short version = (short) 7; // choose a version with fixed length encoding, for simplicity
        ByteBuffer corruptBuffer = produceRequest(version);
        // corrupt the length of the topics array
        corruptBuffer.putInt(8, (Integer.MAX_VALUE - 1) / 2);

        RequestHeader header = new RequestHeader(ApiKeys.PRODUCE, version, "console-producer", 3);
        RequestContext context = new RequestContext(header, "0", InetAddress.getLocalHost(),
                KafkaPrincipal.ANONYMOUS, new ListenerName("ssl"), SecurityProtocol.SASL_SSL,
                ClientInformation.EMPTY, true);

        String msg = assertThrows(InvalidRequestException.class,
                () -> context.parseRequest(corruptBuffer)).getCause().getMessage();
        assertEquals("Tried to allocate a collection of size 1073741823, but there are only 17 bytes remaining.", msg);
    }

    @Test
    public void testInvalidRequestForArrayList() throws UnknownHostException {
        short version = (short) 7; // choose a version with fixed length encoding, for simplicity
        ByteBuffer corruptBuffer = produceRequest(version);
        // corrupt the length of the partitions array
        corruptBuffer.putInt(17, Integer.MAX_VALUE);

        RequestHeader header = new RequestHeader(ApiKeys.PRODUCE, version, "console-producer", 3);
        RequestContext context = new RequestContext(header, "0", InetAddress.getLocalHost(),
                KafkaPrincipal.ANONYMOUS, new ListenerName("ssl"), SecurityProtocol.SASL_SSL,
                ClientInformation.EMPTY, true);

        String msg = assertThrows(InvalidRequestException.class,
                () -> context.parseRequest(corruptBuffer)).getCause().getMessage();
        assertEquals(
                "Tried to allocate a collection of size 2147483647, but there are only 8 bytes remaining.", msg);
    }

    private ByteBuffer produceRequest(short version) {
        ProduceRequestData data = new ProduceRequestData()
                .setAcks((short) -1)
                .setTimeoutMs(1);
        data.topicData().add(
                new ProduceRequestData.TopicProduceData()
                        .setName("foo")
                        .setPartitionData(Collections.singletonList(new ProduceRequestData.PartitionProduceData()
                                .setIndex(42))));

        return serialize(version, data);
    }

    private ByteBuffer serialize(short version, ApiMessage data) {
        ObjectSerializationCache cache = new ObjectSerializationCache();
        data.size(cache, version);
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        data.write(new ByteBufferAccessor(buffer), cache, version);
        buffer.flip();
        return buffer;
    }

    @Test
    public void testInvalidRequestForByteArray() throws UnknownHostException {
        short version = (short) 1; // choose a version with fixed length encoding, for simplicity
        ByteBuffer corruptBuffer = serialize(version, new SaslAuthenticateRequestData().setAuthBytes(new byte[0]));
        // corrupt the length of the bytes array
        corruptBuffer.putInt(0, Integer.MAX_VALUE);

        RequestHeader header = new RequestHeader(ApiKeys.SASL_AUTHENTICATE, version, "console-producer", 1);
        RequestContext context = new RequestContext(header, "0", InetAddress.getLocalHost(),
                KafkaPrincipal.ANONYMOUS, new ListenerName("ssl"), SecurityProtocol.SASL_SSL,
                ClientInformation.EMPTY, true);

        String msg = assertThrows(InvalidRequestException.class,
                () -> context.parseRequest(corruptBuffer)).getCause().getMessage();
        assertEquals("Error reading byte array of 2147483647 byte(s): only 0 byte(s) available", msg);
    }

}
