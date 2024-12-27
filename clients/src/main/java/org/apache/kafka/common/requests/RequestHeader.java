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
import java.util.Objects;

/**
 * The header for a request in the Kafka protocol
 */
public class RequestHeader implements AbstractRequestResponse {
    private static final int SIZE_NOT_INITIALIZED = -1;
    private final RequestHeaderData data;
    private final short headerVersion;
    private int size = SIZE_NOT_INITIALIZED;

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

    // Visible for testing.
    void write(ByteBuffer buffer, ObjectSerializationCache serializationCache) {
        data.write(new ByteBufferAccessor(buffer), serializationCache, headerVersion);
    }

    /**
     * Calculates the size of {@link RequestHeader} in bytes.
     *
     * This method to calculate size should be only when it is immediately followed by
     * {@link #write(ByteBuffer, ObjectSerializationCache)} method call. In such cases, ObjectSerializationCache
     * helps to avoid the serialization twice. In all other cases, {@link #size()} should be preferred instead.
     *
     * Calls to this method leads to calculation of size every time it is invoked. {@link #size()} should be preferred
     * instead.
     *
     * Visible for testing.
     */
    int size(ObjectSerializationCache serializationCache) {
        this.size = data.size(serializationCache, headerVersion);
        return size;
    }

    /**
     * Returns the size of {@link RequestHeader} in bytes.
     *
     * Calls to this method are idempotent and inexpensive since it returns the cached value of size after the first
     * invocation.
     */
    public int size() {
        if (this.size == SIZE_NOT_INITIALIZED) {
            this.size = size(new ObjectSerializationCache());
        }
        return size;
    }

    public boolean isApiVersionDeprecated() {
        return apiKey().isVersionDeprecated(apiVersion());
    }

    public ResponseHeader toResponseHeader() {
        return new ResponseHeader(data.correlationId(), apiKey().responseHeaderVersion(apiVersion()));
    }

    public static RequestHeader parse(ByteBuffer buffer) {
        short apiKey = -1;
        try {
            // We derive the header version from the request api version, so we read that first.
            // The request api version is part of `RequestHeaderData`, so we reset the buffer position after the read.
            int bufferStartPositionForHeader = buffer.position();
            apiKey = buffer.getShort();
            short apiVersion = buffer.getShort();
            short headerVersion = ApiKeys.forId(apiKey).requestHeaderVersion(apiVersion);
            buffer.position(bufferStartPositionForHeader);
            final RequestHeaderData headerData = new RequestHeaderData(new ByteBufferAccessor(buffer), headerVersion);
            // Due to a quirk in the protocol, client ID is marked as nullable.
            // However, we treat a null client ID as equivalent to an empty client ID.
            if (headerData.clientId() == null) {
                headerData.setClientId("");
            }
            final RequestHeader header = new RequestHeader(headerData, headerVersion);
            // Size of header is calculated by the shift in the position of buffer's start position during parsing.
            // Prior to parsing, the buffer's start position points to header data and after the parsing operation
            // the buffer's start position points to api message. For more information on how the buffer is
            // constructed, see RequestUtils#serialize()
            header.size = Math.max(buffer.position() - bufferStartPositionForHeader, 0);
            return header;
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
                ", headerVersion=" + headerVersion +
                ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RequestHeader that = (RequestHeader) o;
        return headerVersion == that.headerVersion &&
            Objects.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(data, headerVersion);
    }
}
