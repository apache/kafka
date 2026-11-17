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
package org.apache.kafka.network;

import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.common.message.AlterPartitionRequestData;
import org.apache.kafka.common.message.ApiMessageType;
import org.apache.kafka.common.message.DescribeAclsRequestData;
import org.apache.kafka.common.message.DescribeLogDirsResponseData;
import org.apache.kafka.common.message.RequestHeaderDataJsonConverter;
import org.apache.kafka.common.network.ClientInformation;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.network.NetworkSend;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.protocol.Readable;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.AlterPartitionRequest;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.network.metrics.RequestChannelMetrics;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;

import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;

public class RequestConvertToJsonTest {

    @Test
    public void testAllRequestTypesHandled() {
        List<String> unhandledKeys = new ArrayList<>();
        for (ApiKeys key : ApiKeys.values()) {
            if (key.hasValidVersion()) {
                short version = key.latestVersion();
                ApiMessage message;
                if (key == ApiKeys.DESCRIBE_ACLS) {
                    message = ApiMessageType.fromApiKey(key.id).newRequest();
                    DescribeAclsRequestData requestData = (DescribeAclsRequestData) message;
                    requestData.setPatternTypeFilter((byte) 1);
                    requestData.setResourceTypeFilter((byte) 1);
                    requestData.setPermissionType((byte) 1);
                    requestData.setOperation((byte) 1);
                } else {
                    message = ApiMessageType.fromApiKey(key.id).newRequest();
                }
                Readable bytes = MessageUtil.toByteBufferAccessor(message, version);
                AbstractRequest req = AbstractRequest.parseRequest(key, version, bytes).request;
                try {
                    RequestConvertToJson.request(req);
                } catch (IllegalStateException e) {
                    unhandledKeys.add(key.toString());
                }
            }
        }
        assertEquals(List.of(), unhandledKeys, "Unhandled request keys");
    }

    @Test
    public void testAllApiVersionsResponseHandled() {
        for (ApiKeys key : ApiKeys.values()) {
            List<Short> unhandledVersions = new ArrayList<>();
            for (short version : key.allVersions()) {
                ApiMessage message;
                // Specify top-level error handling for verifying compatibility across versions
                if (key == ApiKeys.DESCRIBE_LOG_DIRS) {
                    message = ApiMessageType.fromApiKey(key.id).newResponse();
                    DescribeLogDirsResponseData responseData = (DescribeLogDirsResponseData) message;
                    responseData.setErrorCode(Errors.CLUSTER_AUTHORIZATION_FAILED.code());
                } else {
                    message = ApiMessageType.fromApiKey(key.id).newResponse();
                }

                ByteBufferAccessor readable = MessageUtil.toByteBufferAccessor(message, version);
                AbstractResponse response = AbstractResponse.parseResponse(key, readable, version);
                try {
                    RequestConvertToJson.response(response, version);
                } catch (IllegalStateException e) {
                    unhandledVersions.add(version);
                }
            }
            assertEquals(new ArrayList<>(), unhandledVersions, "API: " + key + " - Unhandled request versions");
        }
    }

    @Test
    public void testAllResponseTypesHandled() {
        List<String> unhandledKeys = new ArrayList<>();
        for (ApiKeys key : ApiKeys.values()) {
            if (key.hasValidVersion()) {
                short version = key.latestVersion();
                ApiMessage message = ApiMessageType.fromApiKey(key.id).newResponse();
                ByteBufferAccessor readable = MessageUtil.toByteBufferAccessor(message, version);
                AbstractResponse res = AbstractResponse.parseResponse(key, readable, version);
                try {
                    RequestConvertToJson.response(res, version);
                } catch (IllegalStateException e) {
                    unhandledKeys.add(key.toString());
                }
            }
        }
        assertEquals(List.of(), unhandledKeys, "Unhandled response keys");
    }

    @Test
    public void testClientInfoNode() {
        ClientInformation clientInfo = new ClientInformation("name", "1");
        JsonNode actualNode = RequestConvertToJson.clientInfoNode(clientInfo);

        assertEquals("name", actualNode.get("softwareName").asText());
        assertEquals("1", actualNode.get("softwareVersion").asText());
    }

    @Test
    public void testRequestHeaderNode() {
        AlterPartitionRequest alterIsrRequest = new AlterPartitionRequest(new AlterPartitionRequestData(), ApiKeys.ALTER_PARTITION.latestVersion());
        Request req = request(alterIsrRequest);
        RequestHeader header = req.header();

        ObjectNode expectedNode = (ObjectNode) RequestHeaderDataJsonConverter.write(header.data(), header.headerVersion(), false);
        expectedNode.set("requestApiKeyName", new TextNode(header.apiKey().toString()));

        JsonNode actualNode = RequestConvertToJson.requestHeaderNode(header);

        assertEquals(expectedNode, actualNode);
    }

    @Test
    public void testRequestDesc() {
        AlterPartitionRequest alterIsrRequest = new AlterPartitionRequest(new AlterPartitionRequestData(), ApiKeys.ALTER_PARTITION.latestVersion());
        Request req = request(alterIsrRequest);

        JsonNode actualNode = RequestConvertToJson.requestDesc(req.header(), req.requestLog(), req.isForwarded());

        assertFalse(actualNode.get("isForwarded").asBoolean());
        assertEquals(RequestConvertToJson.requestHeaderNode(req.header()), actualNode.get("requestHeader"));
        assertEquals(req.requestLog().orElse(NullNode.getInstance()), actualNode.get("request"));
    }

    @Test
    public void testRequestDescMetrics() {
        AlterPartitionRequest alterIsrRequest = new AlterPartitionRequest(new AlterPartitionRequestData(), ApiKeys.ALTER_PARTITION.latestVersion());
        Request req = request(alterIsrRequest);
        NetworkSend send = new NetworkSend(req.context().connectionId, alterIsrRequest.toSend(req.header()));
        JsonNode headerLog = RequestConvertToJson.requestHeaderNode(req.header());
        SendResponse res = new SendResponse(req, send, Optional.of(headerLog));

        ObjectNode actualNode = (ObjectNode) RequestConvertToJson.requestDescMetrics(req.header(), req.requestLog(), res.responseLog(), req.context(), req.session(), req.isForwarded(),
                1, 2, 3, 4, 5, 6, 7, 8, 9);

        assertFalse(actualNode.get("isForwarded").asBoolean());
        assertEquals(req.requestLog().orElse(NullNode.getInstance()), actualNode.get("request"));
        assertEquals(res.responseLog().orElse(NullNode.getInstance()), actualNode.get("response"));
        assertEquals("connection-id", actualNode.get("connection").asText());
        assertEquals(1.0, actualNode.get("totalTimeMs").asDouble());
        assertEquals(2.0, actualNode.get("requestQueueTimeMs").asDouble());
        assertEquals(3.0, actualNode.get("localTimeMs").asDouble());
        assertEquals(4.0, actualNode.get("remoteTimeMs").asDouble());
        assertEquals(5, actualNode.get("throttleTimeMs").asLong());
        assertEquals(6.0, actualNode.get("responseQueueTimeMs").asDouble());
        assertEquals(7.0, actualNode.get("sendTimeMs").asDouble());
        assertEquals("PLAINTEXT", actualNode.get("securityProtocol").asText());
        assertEquals("User:user", actualNode.get("principal").asText());
        assertEquals("PLAINTEXT", actualNode.get("listener").asText());
        assertEquals("name", actualNode.get("clientInformation").get("softwareName").asText());
        assertEquals("version", actualNode.get("clientInformation").get("softwareVersion").asText());
        assertEquals(8, actualNode.get("temporaryMemoryBytes").asLong());
        assertEquals(9.0, actualNode.get("messageConversionsTime").asDouble());
    }

    @Test
    public void testRequestDescMetricsOmitsNonPositiveMetrics() {
        AlterPartitionRequest alterIsrRequest = new AlterPartitionRequest(new AlterPartitionRequestData(), ApiKeys.ALTER_PARTITION.latestVersion());
        Request req = request(alterIsrRequest);

        ObjectNode zeroMetricsNode = (ObjectNode) RequestConvertToJson.requestDescMetrics(req.header(), req.requestLog(), Optional.empty(),
                req.context(), req.session(), req.isForwarded(), 1, 2, 3, 4, 5, 6, 7, 0, 0);

        assertFalse(zeroMetricsNode.has("temporaryMemoryBytes"));
        assertFalse(zeroMetricsNode.has("messageConversionsTime"));

        ObjectNode negativeMetricsNode = (ObjectNode) RequestConvertToJson.requestDescMetrics(req.header(), req.requestLog(), Optional.empty(),
                req.context(), req.session(), req.isForwarded(), 1, 2, 3, 4, 5, 6, 7, -1, -1);

        assertFalse(negativeMetricsNode.has("temporaryMemoryBytes"));
        assertFalse(negativeMetricsNode.has("messageConversionsTime"));
    }

    private static Request request(AbstractRequest req) {
        ByteBuffer buffer = req.serializeWithHeader(new RequestHeader(req.apiKey(), req.version(), "client-id", 1));
        RequestContext requestContext = newRequestContext(buffer);
        return new Request(1, requestContext, 0, mock(MemoryPool.class), buffer, mock(RequestChannelMetrics.class));
    }

    private static RequestContext newRequestContext(ByteBuffer buffer) {
        return new RequestContext(
                RequestHeader.parse(buffer),
                "connection-id",
                InetAddress.getLoopbackAddress(),
                new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "user"),
                ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT),
                SecurityProtocol.PLAINTEXT,
                new ClientInformation("name", "version"),
                false);
    }
}
