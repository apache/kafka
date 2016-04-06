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

import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.SchemaException;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.protocol.types.Type;


/**
 * Message from SASL client containing client SASL mechanism.
 * <p/>
 * For interoperability with Kafka 0.9.0.x, mechanism flow may be omitted when using GSSAPI. Hence
 * this request should not conflict with the first GSSAPI client packet. For GSSAPI, the first context
 * establishment packet starts with byte 0x60 (APPLICATION-0 tag) followed by a variable-length encoded size.
 * This mechanism request starts with a two-byte version currently set to zero, followed by a mechanism name,
 * making it easy to distinguish from a GSSAPI packet. To avoid conflicts and simplify debugging, version numbers
 * with 0x60 in the first byte should be avoided.
 */
public class SaslMechanismRequest {

    public static final String VERSION_KEY_NAME = "version";
    public static final String MECHANISM_KEY_NAME = "mechanism";
    public static final short CLIENT_MECHANISM_V0 = 0;

    public static final Schema CLIENT_MECHANISM =
            new Schema(new Field(VERSION_KEY_NAME, Type.INT16, "Protocol Version."),
                       new Field(MECHANISM_KEY_NAME, Type.STRING, "Mechanism chosen by the client."));

    private String mechanism;

    public SaslMechanismRequest(String mechanism) {
        this.mechanism = mechanism;
    }

    public SaslMechanismRequest(ByteBuffer buffer) throws SchemaException {
        try {
            Struct struct = CLIENT_MECHANISM.read(buffer);
            short version = (short) struct.getShort(VERSION_KEY_NAME);
            if (version != CLIENT_MECHANISM_V0)
                throw new SchemaException("Unsupported client mechanism message version: " + version);
            mechanism = (String) struct.getString(MECHANISM_KEY_NAME);
        } catch (Exception e) {
            throw new SchemaException("Buffer does not contain a valid SASL mechanism request: " + e);
        }
    }

    public String mechanism() {
        return mechanism;
    }

    public ByteBuffer toByteBuffer() {
        Struct struct = new Struct(CLIENT_MECHANISM);
        struct.set(VERSION_KEY_NAME, CLIENT_MECHANISM_V0);
        struct.set(MECHANISM_KEY_NAME, mechanism);
        ByteBuffer buffer = ByteBuffer.allocate(struct.sizeOf());
        CLIENT_MECHANISM.write(buffer, struct);
        buffer.flip();
        return buffer;
    }
}

