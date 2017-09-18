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

import java.nio.ByteBuffer;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;


/**
 * Response from SASL server which for a SASL challenge as defined by the SASL protocol
 * for the mechanism configured for the client.
 */
public class SaslAuthenticateResponse extends AbstractResponse {

    private static final String ERROR_CODE_KEY_NAME = "error_code";
    private static final String ERROR_MESSAGE_KEY_NAME = "error_message";
    private static final String SASL_AUTH_BYTES_KEY_NAME = "sasl_auth_bytes";

    private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);

    private final ByteBuffer saslAuthBytes;

    /**
     * Possible error codes:
     *   AUTHENTICATION_FAILED(57) : Authentication failed
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
        error = Errors.forCode(struct.getShort(ERROR_CODE_KEY_NAME));
        errorMessage = struct.getString(ERROR_MESSAGE_KEY_NAME);
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
    public Struct toStruct(short version) {
        Struct struct = new Struct(ApiKeys.SASL_AUTHENTICATE.responseSchema(version));
        struct.set(ERROR_CODE_KEY_NAME, error.code());
        struct.set(ERROR_MESSAGE_KEY_NAME, errorMessage);
        struct.set(SASL_AUTH_BYTES_KEY_NAME, saslAuthBytes);
        return struct;
    }

    public static SaslAuthenticateResponse parse(ByteBuffer buffer, short version) {
        return new SaslAuthenticateResponse(ApiKeys.SASL_AUTHENTICATE.parseResponse(version, buffer));
    }
}

