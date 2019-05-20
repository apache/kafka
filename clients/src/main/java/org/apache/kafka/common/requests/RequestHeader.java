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
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.Objects;

import static java.util.Objects.requireNonNull;
import static org.apache.kafka.common.protocol.types.Type.INT16;
import static org.apache.kafka.common.protocol.types.Type.INT32;
import static org.apache.kafka.common.protocol.types.Type.NULLABLE_STRING;

/**
 * The header for a request in the Kafka protocol
 */
public class RequestHeader extends AbstractRequestResponse {
    private static final String API_KEY_FIELD_NAME = "api_key";
    private static final String API_VERSION_FIELD_NAME = "api_version";
    private static final String CLIENT_ID_FIELD_NAME = "client_id";
    private static final String CORRELATION_ID_FIELD_NAME = "correlation_id";

    public static final Schema SCHEMA = new Schema(
            new Field(API_KEY_FIELD_NAME, INT16, "The id of the request type."),
            new Field(API_VERSION_FIELD_NAME, INT16, "The version of the API."),
            new Field(CORRELATION_ID_FIELD_NAME, INT32, "A user-supplied integer value that will be passed back with the response"),
            new Field(CLIENT_ID_FIELD_NAME, NULLABLE_STRING, "A user specified identifier for the client making the request.", ""));

    // Version 0 of the controlled shutdown API used a non-standard request header (the clientId is missing).
    // This can be removed once we drop support for that version.
    private static final Schema CONTROLLED_SHUTDOWN_V0_SCHEMA = new Schema(
            new Field(API_KEY_FIELD_NAME, INT16, "The id of the request type."),
            new Field(API_VERSION_FIELD_NAME, INT16, "The version of the API."),
            new Field(CORRELATION_ID_FIELD_NAME, INT32, "A user-supplied integer value that will be passed back with the response"));

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
        Schema schema = schema(apiKey.id, apiVersion);
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
            Schema schema = schema(apiKey, apiVersion);
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
                Objects.equals(clientId, that.clientId);
    }

    @Override
    public int hashCode() {
        int result = apiKey.hashCode();
        result = 31 * result + (int) apiVersion;
        result = 31 * result + (clientId != null ? clientId.hashCode() : 0);
        result = 31 * result + correlationId;
        return result;
    }

    private static Schema schema(short apiKey, short version) {
        if (apiKey == ApiKeys.CONTROLLED_SHUTDOWN.id && version == 0)
            // This will be removed once we remove support for v0 of ControlledShutdownRequest, which
            // depends on a non-standard request header (it does not have a clientId)
            return CONTROLLED_SHUTDOWN_V0_SCHEMA;
        return SCHEMA;
    }
}
