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
import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.protocol.types.Type;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.protocol.CommonFields.ERROR_CODE;


/**
 * Response from SASL server which indicates if the client-chosen mechanism is enabled in the server.
 * For error responses, the list of enabled mechanisms is included in the response.
 */
public class SaslHandshakeResponse extends AbstractResponse {
    private static final String ENABLED_MECHANISMS_KEY_NAME = "enabled_mechanisms";

    private static final Schema SASL_HANDSHAKE_RESPONSE_V0 = new Schema(
            ERROR_CODE,
            new Field(ENABLED_MECHANISMS_KEY_NAME, new ArrayOf(Type.STRING), "Array of mechanisms enabled in the server."));

    private static final Schema SASL_HANDSHAKE_RESPONSE_V1 = SASL_HANDSHAKE_RESPONSE_V0;

    public static Schema[] schemaVersions() {
        return new Schema[]{SASL_HANDSHAKE_RESPONSE_V0, SASL_HANDSHAKE_RESPONSE_V1};
    }

    /**
     * Possible error codes:
     *   UNSUPPORTED_SASL_MECHANISM(33): Client mechanism not enabled in server
     *   ILLEGAL_SASL_STATE(34) : Invalid request during SASL handshake
     */
    private final Errors error;
    private final List<String> enabledMechanisms;

    public SaslHandshakeResponse(Errors error, Collection<String> enabledMechanisms) {
        this.error = error;
        this.enabledMechanisms = new ArrayList<>(enabledMechanisms);
    }

    public SaslHandshakeResponse(Struct struct) {
        error = Errors.forCode(struct.get(ERROR_CODE));
        Object[] mechanisms = struct.getArray(ENABLED_MECHANISMS_KEY_NAME);
        ArrayList<String> enabledMechanisms = new ArrayList<>();
        for (Object mechanism : mechanisms)
            enabledMechanisms.add((String) mechanism);
        this.enabledMechanisms = enabledMechanisms;
    }

    public Errors error() {
        return error;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return errorCounts(error);
    }

    @Override
    public Struct toStruct(short version) {
        Struct struct = new Struct(ApiKeys.SASL_HANDSHAKE.responseSchema(version));
        struct.set(ERROR_CODE, error.code());
        struct.set(ENABLED_MECHANISMS_KEY_NAME, enabledMechanisms.toArray());
        return struct;
    }

    public List<String> enabledMechanisms() {
        return enabledMechanisms;
    }

    public static SaslHandshakeResponse parse(ByteBuffer buffer, short version) {
        return new SaslHandshakeResponse(ApiKeys.SASL_HANDSHAKE.parseResponse(version, buffer));
    }
}

