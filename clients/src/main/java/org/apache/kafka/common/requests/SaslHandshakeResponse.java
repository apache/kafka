/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.common.requests;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ProtoUtils;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;


/**
 * Response from SASL server which indicates if the client-chosen mechanism is enabled in the server.
 * For error responses, the list of enabled mechanisms is included in the response.
 */
public class SaslHandshakeResponse extends AbstractRequestResponse {

    private static final Schema CURRENT_SCHEMA = ProtoUtils.currentResponseSchema(ApiKeys.SASL_HANDSHAKE.id);

    private static final String ERROR_CODE_KEY_NAME = "error_code";
    private static final String ENABLED_MECHANISMS_KEY_NAME = "enabled_mechanisms";

    /**
     * Possible error codes:
     *   UNSUPPORTED_SASL_MECHANISM(33): Client mechanism not enabled in server
     *   ILLEGAL_SASL_STATE(34) : Invalid request during SASL handshake
     */
    private final short errorCode;
    private final List<String> enabledMechanisms;

    public SaslHandshakeResponse(short errorCode, Collection<String> enabledMechanisms) {
        super(new Struct(CURRENT_SCHEMA));
        struct.set(ERROR_CODE_KEY_NAME, errorCode);
        struct.set(ENABLED_MECHANISMS_KEY_NAME, enabledMechanisms.toArray());
        this.errorCode = errorCode;
        this.enabledMechanisms = new ArrayList<>(enabledMechanisms);
    }

    public SaslHandshakeResponse(Struct struct) {
        super(struct);
        errorCode = struct.getShort(ERROR_CODE_KEY_NAME);
        Object[] mechanisms = struct.getArray(ENABLED_MECHANISMS_KEY_NAME);
        ArrayList<String> enabledMechanisms = new ArrayList<>();
        for (Object mechanism : mechanisms)
            enabledMechanisms.add((String) mechanism);
        this.enabledMechanisms = enabledMechanisms;
    }

    public short errorCode() {
        return errorCode;
    }

    public List<String> enabledMechanisms() {
        return enabledMechanisms;
    }

    public static SaslHandshakeResponse parse(ByteBuffer buffer) {
        return new SaslHandshakeResponse(CURRENT_SCHEMA.read(buffer));
    }

    public static SaslHandshakeResponse parse(ByteBuffer buffer, int version) {
        return new SaslHandshakeResponse(ProtoUtils.parseResponse(ApiKeys.SASL_HANDSHAKE.id, version, buffer));
    }
}

