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
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Protocol;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;

public class BrokerRequestUtils {

    public static InboundRequest parseInboundRequest(ByteBuffer buffer, BrokerRequestContext context) {
        try {
            RequestHeader header = RequestHeader.parse(buffer);
            return parseInboundRequest(header, buffer, context);
        } catch (Throwable  ex) {
            throw new InvalidRequestException("Error parsing request header. Our best guess of the apiKey is: " +
                    buffer.getShort(0), ex);
        }
    }

    public static InboundRequest parseInboundRequest(RequestHeader header, ByteBuffer buffer, BrokerRequestContext context) {
        if (header.apiKey() == ApiKeys.API_VERSIONS.id &&
                !Protocol.apiVersionSupported(ApiKeys.API_VERSIONS.id, header.apiVersion())) {
            // Unsupported ApiVersion requests are treated as v0 requests and are not parsed
            ApiVersionsRequest apiVersionsRequest = new ApiVersionsRequest((short) 0, header.apiVersion());
            RequestHeader updatedHeader = new RequestHeader(header.apiKey(), (short) 0, header.clientId(),
                    header.correlationId());
            return new InboundRequest(updatedHeader, apiVersionsRequest, 0);
        } else {
            try {
                ApiKeys apiKey = ApiKeys.forId(header.apiKey());
                short apiVersion = header.apiVersion();
                Struct struct = apiKey.parseRequest(apiVersion, buffer);
                AbstractRequest body = AbstractRequest.parseRequest(apiKey, apiVersion, struct);
                return new InboundRequest(header, body, struct.sizeOf());
            } catch (Throwable ex) {
                throw new InvalidRequestException("Error getting request for apiKey: " + header.apiKey() +
                        " and apiVersion: " + header.apiVersion() + " connectionId: " + context.connectionId, ex);
            }
        }
    }

    public static Send buildOutboundSend(RequestHeader header, AbstractResponse body, BrokerRequestContext context) {
        return body.toSend(context.connectionId, header);
    }

}
