///*
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements. See the NOTICE file distributed with
// * this work for additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License. You may obtain a copy of the License at
// *
// *    http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//package org.apache.kafka.network;
//
//import org.apache.kafka.common.message.AlterPartitionRequestData;
//import org.apache.kafka.common.message.ApiMessageType;
//import org.apache.kafka.common.message.DescribeAclsRequestData;
//import org.apache.kafka.common.message.DescribeLogDirsResponseData;
//import org.apache.kafka.common.network.ClientInformation;
//import org.apache.kafka.common.network.ListenerName;
//import org.apache.kafka.common.protocol.ApiKeys;
//import org.apache.kafka.common.protocol.Errors;
//import org.apache.kafka.common.protocol.Message;
//import org.apache.kafka.common.protocol.MessageUtil;
//import org.apache.kafka.common.requests.AbstractRequest;
//import org.apache.kafka.common.requests.AbstractResponse;
//import org.apache.kafka.common.requests.AlterPartitionRequest;
//import org.apache.kafka.common.requests.RequestContext;
//import org.apache.kafka.common.requests.RequestHeader;
//import org.apache.kafka.common.security.auth.KafkaPrincipal;
//import org.apache.kafka.common.security.auth.SecurityProtocol;
//import org.junit.jupiter.api.Test;
//
//import java.net.InetAddress;
//import java.nio.ByteBuffer;
//import java.util.ArrayList;
//import java.util.Collections;
//import java.util.List;
//
//import static org.junit.jupiter.api.Assertions.assertEquals;
//import static org.mockito.Mockito.mock;
//
//public class RequestConvertToJsonTest {
//
//    @Test
//    public void testAllRequestTypesHandled() {
//        List<String> unhandledKeys = new ArrayList<>();
//        for (ApiKeys key : ApiKeys.values()) {
//            short version = key.latestVersion();
//            Message message;
//            if (key == ApiKeys.DESCRIBE_ACLS) {
//                message = ApiMessageType.fromApiKey(key.id).newRequest();
//                DescribeAclsRequestData requestData = (DescribeAclsRequestData) message;
//                requestData.setPatternTypeFilter((byte) 1)
//                    .setResourceTypeFilter((byte) 1)
//                    .setPermissionType((byte) 1)
//                    .setOperation((byte) 1);
//            } else {
//                message = ApiMessageType.fromApiKey(key.id).newRequest();
//            }
//
//            ByteBuffer bytes = MessageUtil.toByteBuffer(message, version);
//            AbstractRequest req = AbstractRequest.parseRequest(key, version, bytes).request;
//            try {
//                RequestConvertToJson.request(req);
//            } catch (IllegalStateException e) {
//                unhandledKeys.add(key.toString());
//            }
//        }
//        assertEquals(Collections.emptyList(), unhandledKeys, "Unhandled request keys");
//    }
//
//    @Test
//    public void testAllApiVersionsResponseHandled() {
//        for (ApiKeys key : ApiKeys.values()) {
//            List<Short> unhandledVersions = new ArrayList<>();
//            for (short version : key.allVersions()) {
//                Message message;
//                if (key == ApiKeys.DESCRIBE_LOG_DIRS) {
//                    message = ApiMessageType.fromApiKey(key.id).newResponse();
//                    DescribeLogDirsResponseData responseData = (DescribeLogDirsResponseData) message;
//                    responseData.setErrorCode(Errors.CLUSTER_AUTHORIZATION_FAILED.code());
//                } else {
//                    message = ApiMessageType.fromApiKey(key.id).newResponse();
//                }
//
//                ByteBuffer bytes = MessageUtil.toByteBuffer(message, version);
//                AbstractResponse response = AbstractResponse.parseResponse(key, bytes, version);
//                try {
//                    RequestConvertToJson.response(response, version);
//                } catch (IllegalStateException e) {
//                    unhandledVersions.add(version);
//                }
//            }
//            assertEquals(Collections.emptyList(), unhandledVersions, "API: " + key + " - Unhandled request versions");
//        }
//    }
//
//    @Test
//    public void testAllResponseTypesHandled() {
//        List<String> unhandledKeys = new ArrayList<>();
//        for (ApiKeys key : ApiKeys.values()) {
//            short version = key.latestVersion();
//            Message message = ApiMessageType.fromApiKey(key.id).newResponse();
//            ByteBuffer bytes = MessageUtil.toByteBuffer(message, version);
//            AbstractResponse res = AbstractResponse.parseResponse(key, bytes, version);
//            try {
//                RequestConvertToJson.response(res, version);
//            } catch (IllegalStateException e) {
//                unhandledKeys.add(key.toString());
//            }
//        }
//        assertEquals(new ArrayList<>(), unhandledKeys, "Unhandled response keys");
//    }
//
//    @Test
//    public void testRequestHeaderNode() {
//        AlterPartitionRequest alterIsrRequest = new AlterPartitionRequest(new AlterPartitionRequestData(), 0);
//        Request req = RequestConvertToJson.request(alterIsrRequest);
//        RequestHeader header = req.header;
//
//        ObjectNode expectedNode = RequestHeaderDataJsonConverter.write(header.data, header.headerVersion, false);
//        expectedNode.set("requestApiKeyName", new TextNode(header.apiKey.toString()));
//
//        ObjectNode actualNode = RequestConvertToJson.requestHeaderNode(header);
//
//        assertEquals(expectedNode, actualNode);
//    }
//
//    @Test
//    public void testRequestHeaderNodeWithDeprecatedApiVersion() {
//        FetchRequest fetchRequest = FetchRequest.Builder.forConsumer(0, 0, 0, Collections.emptyMap()).build(0);
//        Request req = RequestConvertToJson.request(fetchRequest);
//        RequestHeader header = req.header;
//
//        ObjectNode expectedNode = RequestHeaderDataJsonConverter.write(header.data, header.headerVersion, false);
//        expectedNode.set("requestApiKeyName", new TextNode(header.apiKey.toString()));
//        expectedNode.set("requestApiVersionDeprecated", BooleanNode.TRUE);
//
//        ObjectNode actualNode = RequestConvertToJson.requestHeaderNode(header);
//
//        assertEquals(expectedNode, actualNode);
//    }
//
//    @Test
//    public void testClientInfoNode() {
//        ClientInformation clientInfo = new ClientInformation("name", "1");
//
//        ObjectNode expectedNode = new ObjectNode(JsonNodeFactory.instance);
//        expectedNode.set("softwareName", new TextNode(clientInfo.softwareName));
//        expectedNode.set("softwareVersion", new TextNode(clientInfo.softwareVersion));
//
//        ObjectNode actualNode = RequestConvertToJson.clientInfoNode(clientInfo);
//
//        assertEquals(expectedNode, actualNode);
//    }
//
//    @Test
//    public void testRequestDesc() {
//        AlterPartitionRequest alterIsrRequest = new AlterPartitionRequest(new AlterPartitionRequestData(), 0);
//        Request req = RequestConvertToJson.request(alterIsrRequest);
//
//        ObjectNode expectedNode = new ObjectNode(JsonNodeFactory.instance);
//        expectedNode.set("isForwarded", req.isForwarded() ? BooleanNode.TRUE : BooleanNode.FALSE);
//        expectedNode.set("requestHeader", RequestConvertToJson.requestHeaderNode(req.header));
//        expectedNode.set("request", req.requestLog().orElse(new TextNode("")));
//
//        ObjectNode actualNode = RequestConvertToJson.requestDesc(req.header, req.requestLog(), req.isForwarded());
//
//        assertEquals(expectedNode, actualNode);
//    }
//
//    @Test
//    public void testRequestDescMetrics() {
//        AlterPartitionRequest alterIsrRequest = new AlterPartitionRequest(new AlterPartitionRequestData(), 0);
//        Request req = request(alterIsrRequest);
//        NetworkSend send = new NetworkSend(req.context.connectionId, alterIsrRequest.toSend(req.header));
//        ObjectNode headerLog = RequestConvertToJson.requestHeaderNode(req.header);
//        RequestChannel.SendResponse res = new RequestChannel.SendResponse(req, send, Optional.of(headerLog), Optional.empty());
//
//        int totalTimeMs = 1;
//        int requestQueueTimeMs = 2;
//        int apiLocalTimeMs = 3;
//        int apiRemoteTimeMs = 4;
//        long apiThrottleTimeMs = 5;
//        int responseQueueTimeMs = 6;
//        int responseSendTimeMs = 7;
//        long temporaryMemoryBytes = 8;
//        int messageConversionsTimeMs = 9;
//
//        ObjectNode expectedNode = RequestConvertToJson.requestDesc(req.header, req.requestLog(), req.isForwarded());
//        expectedNode.set("response", res.responseLog().orElse(new TextNode("")));
//        expectedNode.set("connection", new TextNode(req.context.connectionId));
//        expectedNode.set("totalTimeMs", new DoubleNode(totalTimeMs));
//        expectedNode.set("requestQueueTimeMs", new DoubleNode(requestQueueTimeMs));
//        expectedNode.set("localTimeMs", new DoubleNode(apiLocalTimeMs));
//        expectedNode.set("remoteTimeMs", new DoubleNode(apiRemoteTimeMs));
//        expectedNode.set("throttleTimeMs", new LongNode(apiThrottleTimeMs));
//        expectedNode.set("responseQueueTimeMs", new DoubleNode(responseQueueTimeMs));
//        expectedNode.set("sendTimeMs", new DoubleNode(responseSendTimeMs));
//        expectedNode.set("securityProtocol", new TextNode(req.context.securityProtocol.toString()));
//        expectedNode.set("principal", new TextNode(req.session.principal.toString()));
//        expectedNode.set("listener", new TextNode(req.context.listenerName.value()));
//        expectedNode.set("clientInformation", RequestConvertToJson.clientInfoNode(req.context.clientInformation));
//        expectedNode.set("temporaryMemoryBytes", new LongNode(temporaryMemoryBytes));
//        expectedNode.set("messageConversionsTime", new DoubleNode(messageConversionsTimeMs));
//
//        ObjectNode actualNode = RequestConvertToJson.requestDescMetrics(
//            req.header,
//            req.requestLog(),
//            res.responseLog(),
//            req.context,
//            req.session,
//            req.isForwarded(),
//            totalTimeMs,
//            requestQueueTimeMs,
//            apiLocalTimeMs,
//            apiRemoteTimeMs,
//            apiThrottleTimeMs,
//            responseQueueTimeMs,
//            responseSendTimeMs,
//            temporaryMemoryBytes,
//            messageConversionsTimeMs
//        );
//
//        assertEquals(expectedNode, actualNode);
//    }
//
//    private RequestChannel.Request request(AbstractRequest req) {
//        ByteBuffer buffer = req.serializeWithHeader(new RequestHeader(req.apiKey(), req.version(), "client-id", 1));
//        RequestContext requestContext = newRequestContext(buffer);
//        return new RequestChannel.Request(
//            1,
//            requestContext,
//            0,
//            mock(MemoryPool.class),
//            buffer,
//            mock(RequestChannelMetrics.class)
//        );
//    }
//
//    private RequestContext newRequestContext(ByteBuffer buffer) {
//        return new RequestContext(
//            RequestHeader.parse(buffer),
//            "connection-id",
//            InetAddress.getLoopbackAddress(),
//            new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "user"),
//            ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT),
//            SecurityProtocol.PLAINTEXT,
//            new ClientInformation("name", "version"),
//            false
//        );
//    }
//
//}
