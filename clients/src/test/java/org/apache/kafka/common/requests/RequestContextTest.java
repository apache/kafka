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

import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersionsResponseKeyCollection;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.network.ClientInformation;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.junit.Test;

import java.net.InetAddress;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
        assertTrue(requestAndSize.request instanceof ApiVersionsRequest);
        ApiVersionsRequest request = (ApiVersionsRequest) requestAndSize.request;
        assertTrue(request.hasUnsupportedRequestVersion());

        Send send = context.buildResponseSend(new ApiVersionsResponse(new ApiVersionsResponseData()
            .setThrottleTimeMs(0)
            .setErrorCode(Errors.UNSUPPORTED_VERSION.code())
            .setApiKeys(new ApiVersionsResponseKeyCollection())));
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
        assertEquals("Buffer limit and capacity should be the same", buffer.capacity(), buffer.limit());
        CreateTopicsResponse parsedResponse = (CreateTopicsResponse) AbstractResponse.parseResponse(buffer, header);
        assertEquals(expectedResponse, parsedResponse.data());
    }

}
