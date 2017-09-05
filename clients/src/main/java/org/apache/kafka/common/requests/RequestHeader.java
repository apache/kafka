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
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Protocol;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;

import static java.util.Objects.requireNonNull;

/**
 * The header for a request in the Kafka protocol
 */
public class RequestHeader extends AbstractRequestResponse {
    private static final String API_KEY_FIELD_NAME = "api_key";
    private static final String API_VERSION_FIELD_NAME = "api_version";
    private static final String CLIENT_ID_FIELD_NAME = "client_id";
    private static final String CORRELATION_ID_FIELD_NAME = "correlation_id";

    private final ApiKeys apiKey;
    private final short apiVersion;
    private final String clientId;
    private final int correlationId;

    public RequestHeader(Struct struct) {
        short apiKey = struct.getShort(API_KEY_FIELD_NAME);
        if (!ApiKeys.hasId(apiKey))
            throw new InvalidRequestException("Unknown API key " + apiKey);

        this.apiKey = ApiKeys.forId(apiKey);
        apiVersion = struct.getShort(API_VERSION_FIELD_NAME);

        // only v0 of the controlled shutdown request is missing the clientId
        if (struct.hasField(CLIENT_ID_FIELD_NAME))
            clientId = struct.getString(CLIENT_ID_FIELD_NAME);
        else
            clientId = "";
        correlationId = struct.getInt(CORRELATION_ID_FIELD_NAME);
    }

    public RequestHeader(ApiKeys apiKey, short version, String clientId, int correlation) {
        this.apiKey = requireNonNull(apiKey);
        this.apiVersion = version;
        this.clientId = clientId;
        this.correlationId = correlation;
    }

    public Struct toStruct() {
        Schema schema = Protocol.requestHeaderSchema(apiKey.id, apiVersion);
        Struct struct = new Struct(schema);
        struct.set(API_KEY_FIELD_NAME, apiKey.id);
        struct.set(API_VERSION_FIELD_NAME, apiVersion);

        // only v0 of the controlled shutdown request is missing the clientId
        if (struct.hasField(CLIENT_ID_FIELD_NAME))
            struct.set(CLIENT_ID_FIELD_NAME, clientId);
        struct.set(CORRELATION_ID_FIELD_NAME, correlationId);
        return struct;
    }

    public ApiKeys apiKey() {
        return apiKey;
    }

    public short apiVersion() {
        return apiVersion;
    }

    public String clientId() {
        return clientId;
    }

    public int correlationId() {
        return correlationId;
    }

    public ResponseHeader toResponseHeader() {
        return new ResponseHeader(correlationId);
    }

    public static RequestHeader parse(ByteBuffer buffer) {
        try {
            short apiKey = buffer.getShort();
            short apiVersion = buffer.getShort();
            Schema schema = Protocol.requestHeaderSchema(apiKey, apiVersion);
            buffer.rewind();
            return new RequestHeader(schema.read(buffer));
        } catch (InvalidRequestException e) {
            throw e;
        } catch (Throwable  ex) {
            throw new InvalidRequestException("Error parsing request header. Our best guess of the apiKey is: " +
                    buffer.getShort(0), ex);
        }
    }

    @Override
    public String toString() {
        return "RequestHeader(apiKey=" + apiKey +
                ", apiVersion=" + apiVersion +
                ", clientId=" + clientId +
                ", correlationId=" + correlationId +
                ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RequestHeader that = (RequestHeader) o;
        return apiKey == that.apiKey &&
                apiVersion == that.apiVersion &&
                correlationId == that.correlationId &&
                (clientId == null ? that.clientId == null : clientId.equals(that.clientId));
    }

    @Override
    public int hashCode() {
        int result = apiKey.hashCode();
        result = 31 * result + (int) apiVersion;
        result = 31 * result + (clientId != null ? clientId.hashCode() : 0);
        result = 31 * result + correlationId;
        return result;
    }
}
