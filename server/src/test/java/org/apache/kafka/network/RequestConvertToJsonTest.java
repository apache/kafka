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

import org.apache.kafka.common.message.ApiMessageType;
import org.apache.kafka.common.message.DescribeAclsRequestData;
import org.apache.kafka.common.message.DescribeLogDirsResponseData;
import org.apache.kafka.common.network.ClientInformation;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RequestConvertToJsonTest {

    @Test
    public void testAllRequestTypesHandled() {
        List<String> unhandledKeys = new ArrayList<>();
        for (ApiKeys key : ApiKeys.values()) {
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
            ByteBuffer bytes = MessageUtil.toByteBuffer(message, version);
            AbstractRequest req = AbstractRequest.parseRequest(key, version, bytes).request;
            try {
                RequestConvertToJson.request(req);
            } catch (IllegalStateException e) {
                unhandledKeys.add(key.toString());
            }
        }
        assertEquals(Collections.emptyList(), unhandledKeys, "Unhandled request keys");
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

                ByteBuffer bytes = MessageUtil.toByteBuffer(message, version);
                AbstractResponse response = AbstractResponse.parseResponse(key, bytes, version);
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
            short version = key.latestVersion();
            ApiMessage message = ApiMessageType.fromApiKey(key.id).newResponse();
            ByteBuffer bytes = MessageUtil.toByteBuffer(message, version);
            AbstractResponse res = AbstractResponse.parseResponse(key, bytes, version);
            try {
                RequestConvertToJson.response(res, version);
            } catch (IllegalStateException e) {
                unhandledKeys.add(key.toString());
            }
        }
        assertEquals(Collections.emptyList(), unhandledKeys, "Unhandled response keys");
    }

    @Test
    public void testClientInfoNode() {
        ClientInformation clientInfo = new ClientInformation("name", "1");
        ObjectNode expectedNode = JsonNodeFactory.instance.objectNode();
        expectedNode.set("softwareName", new TextNode(clientInfo.softwareName()));
        expectedNode.set("softwareVersion", new TextNode(clientInfo.softwareVersion()));
        JsonNode actualNode = RequestConvertToJson.clientInfoNode(clientInfo);
        assertEquals(expectedNode, actualNode);
    }
}
