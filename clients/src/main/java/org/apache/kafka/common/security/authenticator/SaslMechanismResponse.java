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

package org.apache.kafka.common.security.authenticator;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.protocol.types.Type;


/**
 * Response from SASL server which indicates if the client-chosen mechanism is enabled in the server.
 * For error responses, the list of enabled mechanisms is included in the response.
 */
public class SaslMechanismResponse {
    public static final String ERROR_CODE_KEY_NAME = "error_code";
    public static final String ENABLED_MECHANISMS_KEY_NAME = "enabled_mechanisms";

    public static final Schema MECHANISM_RESPONSE =
            new Schema(new Field(ERROR_CODE_KEY_NAME, Type.INT16, "Error code."),
                       new Field(ENABLED_MECHANISMS_KEY_NAME, new ArrayOf(Type.STRING), "Array of mechanisms enabled in the server, included for non-zero error code."));

    public static final short UNSUPPORTED_SASL_MECHANISM = 1;

    /**
     * Possible error codes:
     *   UNSUPPORTED_SASL_MECHANISM(1): Client mechanism not enabled in server
     */
    private final short errorCode;
    private List<String> enabledMechanisms = new ArrayList<String>();

    public SaslMechanismResponse(short errorCode, Collection<String> mechanisms) {
        this.errorCode = errorCode;
        this.enabledMechanisms.addAll(mechanisms);
    }

    public SaslMechanismResponse(ByteBuffer buffer) {
        Struct struct = MECHANISM_RESPONSE.read(buffer);
        errorCode = struct.getShort(ERROR_CODE_KEY_NAME);
        Object[] mechArray = struct.getArray(ENABLED_MECHANISMS_KEY_NAME);
        for (Object mechanism : mechArray)
            enabledMechanisms.add((String) mechanism);
    }

    public short errorCode() {
        return errorCode;
    }

    public List<String> enabledMechanisms() {
        return enabledMechanisms;
    }

    public ByteBuffer toByteBuffer() {
        Struct struct = new Struct(MECHANISM_RESPONSE);
        struct.set(ERROR_CODE_KEY_NAME, errorCode);
        struct.set(ENABLED_MECHANISMS_KEY_NAME, enabledMechanisms.toArray());
        ByteBuffer buffer = ByteBuffer.allocate(struct.sizeOf());
        MECHANISM_RESPONSE.write(buffer, struct);
        buffer.flip();
        return buffer;
    }
}

