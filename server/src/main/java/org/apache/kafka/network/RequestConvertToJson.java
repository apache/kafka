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

import org.apache.kafka.common.message.RequestHeaderDataJsonConverter;
import org.apache.kafka.common.network.ClientInformation;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.RequestHeader;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;

import java.util.Optional;

public class RequestConvertToJson {

    public static JsonNode request(AbstractRequest request) {
        return GeneratedRequestConvertToJson.request(request);
    }

    public static JsonNode response(AbstractResponse response, short version) {
        return GeneratedRequestConvertToJson.response(response, version);
    }

    public static JsonNode requestHeaderNode(RequestHeader header) {
        ObjectNode node = (ObjectNode) RequestHeaderDataJsonConverter.write(
            header.data(), header.headerVersion(), false
        );
        node.set("requestApiKeyName", new TextNode(header.apiKey().toString()));
        if (header.isApiVersionDeprecated()) {
            node.set("requestApiVersionDeprecated", BooleanNode.TRUE);
        }
        return node;
    }

    public static JsonNode requestDesc(RequestHeader header, Optional<JsonNode> requestNode, boolean isForwarded) {
        ObjectNode node = JsonNodeFactory.instance.objectNode();
        node.set("isForwarded", isForwarded ? BooleanNode.TRUE : BooleanNode.FALSE);
        node.set("requestHeader", requestHeaderNode(header));
        node.set("request", requestNode.orElse(NullNode.getInstance()));
        return node;
    }

    public static JsonNode clientInfoNode(ClientInformation clientInfo) {
        ObjectNode node = JsonNodeFactory.instance.objectNode();
        node.set("softwareName", new TextNode(clientInfo.softwareName()));
        node.set("softwareVersion", new TextNode(clientInfo.softwareVersion()));
        return node;
    }

    public static JsonNode requestDescMetrics(RequestHeader header, Optional<JsonNode> requestNode, Optional<JsonNode> responseNode,
                                              RequestContext context, Session session, boolean isForwarded,
                                              double totalTimeMs, double requestQueueTimeMs, double apiLocalTimeMs,
                                              double apiRemoteTimeMs, long apiThrottleTimeMs, double responseQueueTimeMs,
                                              double responseSendTimeMs, long temporaryMemoryBytes,
                                              double messageConversionsTimeMs) {
        ObjectNode node = (ObjectNode) requestDesc(header, requestNode, isForwarded);
        node.set("response", responseNode.orElse(NullNode.getInstance()));
        node.set("connection", new TextNode(context.connectionId));
        node.set("totalTimeMs", new DoubleNode(totalTimeMs));
        node.set("requestQueueTimeMs", new DoubleNode(requestQueueTimeMs));
        node.set("localTimeMs", new DoubleNode(apiLocalTimeMs));
        node.set("remoteTimeMs", new DoubleNode(apiRemoteTimeMs));
        node.set("throttleTimeMs", new LongNode(apiThrottleTimeMs));
        node.set("responseQueueTimeMs", new DoubleNode(responseQueueTimeMs));
        node.set("sendTimeMs", new DoubleNode(responseSendTimeMs));
        node.set("securityProtocol", new TextNode(context.securityProtocol.toString()));
        node.set("principal", new TextNode(session.principal.toString()));
        node.set("listener", new TextNode(context.listenerName.value()));
        node.set("clientInformation", clientInfoNode(context.clientInformation));
        if (temporaryMemoryBytes > 0) {
            node.set("temporaryMemoryBytes", new LongNode(temporaryMemoryBytes));
        }
        if (messageConversionsTimeMs > 0) {
            node.set("messageConversionsTime", new DoubleNode(messageConversionsTimeMs));
        }
        return node;
    }
}
