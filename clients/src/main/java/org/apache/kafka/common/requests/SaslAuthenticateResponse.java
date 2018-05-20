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
import java.util.Map;

import static org.apache.kafka.common.protocol.CommonFields.ERROR_CODE;
import static org.apache.kafka.common.protocol.CommonFields.ERROR_MESSAGE;
import static org.apache.kafka.common.protocol.types.Type.BYTES;


/**
 * Response from SASL server which for a SASL challenge as defined by the SASL protocol
 * for the mechanism configured for the client.
 */
public class SaslAuthenticateResponse extends AbstractResponse {
    private static final String SASL_AUTH_BYTES_KEY_NAME = "sasl_auth_bytes";

    private static final Schema SASL_AUTHENTICATE_RESPONSE_V0 = new Schema(
            ERROR_CODE,
            ERROR_MESSAGE,
            new Field(SASL_AUTH_BYTES_KEY_NAME, BYTES, "SASL authentication bytes from server as defined by the SASL mechanism."));

    public static Schema[] schemaVersions() {
        return new Schema[]{SASL_AUTHENTICATE_RESPONSE_V0};
    }

    private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);

    private final ByteBuffer saslAuthBytes;

    /**
     * Possible error codes:
     *   SASL_AUTHENTICATION_FAILED(57) : Authentication failed
     */
    private final Errors error;
    private final String errorMessage;

    public SaslAuthenticateResponse(Errors error, String errorMessage) {
        this(error, errorMessage, EMPTY_BUFFER);
    }

    public SaslAuthenticateResponse(Errors error, String errorMessage, ByteBuffer saslAuthBytes) {
        this.error = error;
        this.errorMessage = errorMessage;
        this.saslAuthBytes = saslAuthBytes;
    }

    public SaslAuthenticateResponse(Struct struct) {
        error = Errors.forCode(struct.get(ERROR_CODE));
        errorMessage = struct.get(ERROR_MESSAGE);
        saslAuthBytes = struct.getBytes(SASL_AUTH_BYTES_KEY_NAME);
    }

    public Errors error() {
        return error;
    }

    public String errorMessage() {
        return errorMessage;
    }

    public ByteBuffer saslAuthBytes() {
        return saslAuthBytes;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return errorCounts(error);
    }

    @Override
    public Struct toStruct(short version) {
        Struct struct = new Struct(ApiKeys.SASL_AUTHENTICATE.responseSchema(version));
        struct.set(ERROR_CODE, error.code());
        struct.set(ERROR_MESSAGE, errorMessage);
        struct.set(SASL_AUTH_BYTES_KEY_NAME, saslAuthBytes);
        return struct;
    }

    public static SaslAuthenticateResponse parse(ByteBuffer buffer, short version) {
        return new SaslAuthenticateResponse(ApiKeys.SASL_AUTHENTICATE.parseResponse(version, buffer));
    }
}