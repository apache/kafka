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
import java.util.Collections;
import java.util.List;

import static org.apache.kafka.common.protocol.types.Type.STRING;

/**
 * Request from SASL client containing client SASL mechanism.
 * <p/>
 * For interoperability with Kafka 0.9.0.x, the mechanism flow may be omitted when using GSSAPI. Hence
 * this request should not conflict with the first GSSAPI client packet. For GSSAPI, the first context
 * establishment packet starts with byte 0x60 (APPLICATION-0 tag) followed by a variable-length encoded size.
 * This handshake request starts with a request header two-byte API key set to 17, followed by a mechanism name,
 * making it easy to distinguish from a GSSAPI packet.
 */
public class SaslHandshakeRequest extends AbstractRequest {
    private static final String MECHANISM_KEY_NAME = "mechanism";

    private static final Schema SASL_HANDSHAKE_REQUEST_V0 = new Schema(
            new Field("mechanism", STRING, "SASL Mechanism chosen by the client."));

    // SASL_HANDSHAKE_REQUEST_V1 added to support SASL_AUTHENTICATE request to improve diagnostics
    private static final Schema SASL_HANDSHAKE_REQUEST_V1 = SASL_HANDSHAKE_REQUEST_V0;

    public static Schema[] schemaVersions() {
        return new Schema[]{SASL_HANDSHAKE_REQUEST_V0, SASL_HANDSHAKE_REQUEST_V1};
    }

    private final String mechanism;

    public static class Builder extends AbstractRequest.Builder<SaslHandshakeRequest> {
        private final String mechanism;

        public Builder(String mechanism) {
            super(ApiKeys.SASL_HANDSHAKE);
            this.mechanism = mechanism;
        }

        @Override
        public SaslHandshakeRequest build(short version) {
            return new SaslHandshakeRequest(mechanism, version);
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(type=SaslHandshakeRequest").
                append(", mechanism=").append(mechanism).
                append(")");
            return bld.toString();
        }
    }

    public SaslHandshakeRequest(String mechanism) {
        this(mechanism, ApiKeys.SASL_HANDSHAKE.latestVersion());
    }

    public SaslHandshakeRequest(String mechanism, short version) {
        super(ApiKeys.SASL_HANDSHAKE, version);
        this.mechanism = mechanism;
    }

    public SaslHandshakeRequest(Struct struct, short version) {
        super(ApiKeys.SASL_HANDSHAKE, version);
        mechanism = struct.getString(MECHANISM_KEY_NAME);
    }

    public String mechanism() {
        return mechanism;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        short versionId = version();
        switch (versionId) {
            case 0:
            case 1:
                List<String> enabledMechanisms = Collections.emptyList();
                return new SaslHandshakeResponse(Errors.forException(e), enabledMechanisms);
            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                        versionId, this.getClass().getSimpleName(), ApiKeys.SASL_HANDSHAKE.latestVersion()));
        }
    }

    public static SaslHandshakeRequest parse(ByteBuffer buffer, short version) {
        return new SaslHandshakeRequest(ApiKeys.SASL_HANDSHAKE.parseRequest(version, buffer), version);
    }

    @Override
    protected Struct toStruct() {
        Struct struct = new Struct(ApiKeys.SASL_HANDSHAKE.requestSchema(version()));
        struct.set(MECHANISM_KEY_NAME, mechanism);
        return struct;
    }
}

