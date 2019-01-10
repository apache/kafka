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

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;

import static org.apache.kafka.common.protocol.types.Type.BYTES;

/**
 * Request from SASL client containing client SASL authentication token as defined by the
 * SASL protocol for the configured SASL mechanism.
 * <p/>
 * For interoperability with versions prior to Kafka 1.0.0, this request is used only with broker
 * version 1.0.0 and higher that support SaslHandshake request v1. Clients connecting to older
 * brokers will send SaslHandshake request v0 followed by SASL tokens without the Kafka request headers.
 */
public class SaslAuthenticateRequest extends AbstractRequest {
    private static final String SASL_AUTH_BYTES_KEY_NAME = "sasl_auth_bytes";

    private static final Schema SASL_AUTHENTICATE_REQUEST_V0 = new Schema(
            new Field(SASL_AUTH_BYTES_KEY_NAME, BYTES, "SASL authentication bytes from client as defined by the SASL mechanism."));

    /* v1 request is the same as v0; session_lifetime_ms has been added to the response */
    private static final Schema SASL_AUTHENTICATE_REQUEST_V1 = SASL_AUTHENTICATE_REQUEST_V0;

    public static Schema[] schemaVersions() {
        return new Schema[]{SASL_AUTHENTICATE_REQUEST_V0, SASL_AUTHENTICATE_REQUEST_V1};
    }

    private final ByteBuffer saslAuthBytes;

    public static class Builder extends AbstractRequest.Builder<SaslAuthenticateRequest> {
        private final ByteBuffer saslAuthBytes;

        public Builder(ByteBuffer saslAuthBytes) {
            super(ApiKeys.SASL_AUTHENTICATE);
            this.saslAuthBytes = saslAuthBytes;
        }

        @Override
        public SaslAuthenticateRequest build(short version) {
            return new SaslAuthenticateRequest(saslAuthBytes, version);
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(type=SaslAuthenticateRequest)");
            return bld.toString();
        }
    }

    public SaslAuthenticateRequest(ByteBuffer saslAuthBytes) {
        this(saslAuthBytes, ApiKeys.SASL_AUTHENTICATE.latestVersion());
    }

    public SaslAuthenticateRequest(ByteBuffer saslAuthBytes, short version) {
        super(ApiKeys.SASL_AUTHENTICATE, version);
        this.saslAuthBytes = saslAuthBytes;
    }

    public SaslAuthenticateRequest(Struct struct, short version) {
        super(ApiKeys.SASL_AUTHENTICATE, version);
        saslAuthBytes = struct.getBytes(SASL_AUTH_BYTES_KEY_NAME);
    }

    public ByteBuffer saslAuthBytes() {
        return saslAuthBytes;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        short versionId = version();
        switch (versionId) {
            case 0:
            case 1:
                return new SaslAuthenticateResponse(Errors.forException(e), e.getMessage());
            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                        versionId, this.getClass().getSimpleName(), ApiKeys.SASL_AUTHENTICATE.latestVersion()));
        }
    }

    public static SaslAuthenticateRequest parse(ByteBuffer buffer, short version) {
        return new SaslAuthenticateRequest(ApiKeys.SASL_AUTHENTICATE.parseRequest(version, buffer), version);
    }

    @Override
    protected Struct toStruct() {
        Struct struct = new Struct(ApiKeys.SASL_AUTHENTICATE.requestSchema(version()));
        struct.set(SASL_AUTH_BYTES_KEY_NAME, saslAuthBytes);
        return struct;
    }
}

