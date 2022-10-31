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
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.ObjectSerializationCache;

import java.nio.ByteBuffer;

/**
 * The header for a request in the Kafka protocol
 */
public class RequestHeader implements AbstractRequestResponse {
    private final RequestHeaderData data;
    private final short headerVersion;

    public RequestHeader(ApiKeys requestApiKey, short requestVersion, String clientId, int correlationId) {
        this(new RequestHeaderData().
                setRequestApiKey(requestApiKey.id).
                setRequestApiVersion(requestVersion).
                setClientId(clientId).
                setCorrelationId(correlationId),
            requestApiKey.requestHeaderVersion(requestVersion));
    }

    public RequestHeader(RequestHeaderData data, short headerVersion) {
        this.data = data;
        this.headerVersion = headerVersion;
    }

    public ApiKeys apiKey() {
        return ApiKeys.forId(data.requestApiKey());
    }

    public short apiVersion() {
        return data.requestApiVersion();
    }

    public short headerVersion() {
        return headerVersion;
    }

    public String clientId() {
        return data.clientId();
    }

    public int correlationId() {
        return data.correlationId();
    }

    public RequestHeaderData data() {
        return data;
    }

    public void write(ByteBuffer buffer, ObjectSerializationCache serializationCache) {
        data.write(new ByteBufferAccessor(buffer), serializationCache, headerVersion);
    }

    public int size(ObjectSerializationCache serializationCache) {
        return data.size(serializationCache, headerVersion);
    }

    public ResponseHeader toResponseHeader() {
        return new ResponseHeader(data.correlationId(), apiKey().responseHeaderVersion(apiVersion()));
    }

    public static RequestHeader parse(ByteBuffer buffer) {
        short apiKey = -1;
        try {
            // We derive the header version from the request api version, so we read that first.
            // The request api version is part of `RequestHeaderData`, so we reset the buffer position after the read.
            int position = buffer.position();
            apiKey = buffer.getShort();
            short apiVersion = buffer.getShort();
            short headerVersion = ApiKeys.forId(apiKey).requestHeaderVersion(apiVersion);
            buffer.position(position);
            RequestHeaderData headerData = new RequestHeaderData(
                new ByteBufferAccessor(buffer), headerVersion);
            // Due to a quirk in the protocol, client ID is marked as nullable.
            // However, we treat a null client ID as equivalent to an empty client ID.
            if (headerData.clientId() == null) {
                headerData.setClientId("");
            }
            return new RequestHeader(headerData, headerVersion);
        } catch (UnsupportedVersionException e) {
            throw new InvalidRequestException("Unknown API key " + apiKey, e);
        } catch (Throwable ex) {
            throw new InvalidRequestException("Error parsing request header. Our best guess of the apiKey is: " +
                    apiKey, ex);
        }
    }

    @Override
    public String toString() {
        return "RequestHeader(apiKey=" + apiKey() +
                ", apiVersion=" + apiVersion() +
                ", clientId=" + clientId() +
                ", correlationId=" + correlationId() +
                ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RequestHeader that = (RequestHeader) o;
        return this.data.equals(that.data);
    }

    @Override
    public int hashCode() {
        return this.data.hashCode();
    }
}
