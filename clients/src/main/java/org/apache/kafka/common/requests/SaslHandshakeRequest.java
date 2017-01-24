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
import java.util.Collections;
import java.util.List;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ProtoUtils;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;


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

    private static final Schema CURRENT_SCHEMA = ProtoUtils.currentRequestSchema(ApiKeys.SASL_HANDSHAKE.id);
    public static final String MECHANISM_KEY_NAME = "mechanism";

    private final String mechanism;

    public SaslHandshakeRequest(String mechanism) {
        super(new Struct(CURRENT_SCHEMA), ProtoUtils.latestVersion(ApiKeys.SASL_HANDSHAKE.id));
        struct.set(MECHANISM_KEY_NAME, mechanism);
        this.mechanism = mechanism;
    }

    public SaslHandshakeRequest(Struct struct, short versionId) {
        super(struct, versionId);
        mechanism = struct.getString(MECHANISM_KEY_NAME);
    }

    public String mechanism() {
        return mechanism;
    }

    @Override
    public AbstractResponse getErrorResponse(Throwable e) {
        short versionId = version();
        switch (versionId) {
            case 0:
                List<String> enabledMechanisms = Collections.emptyList();
                return new SaslHandshakeResponse(Errors.forException(e).code(), enabledMechanisms);
            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                        versionId, this.getClass().getSimpleName(), ProtoUtils.latestVersion(ApiKeys.SASL_HANDSHAKE.id)));
        }
    }

    public static SaslHandshakeRequest parse(ByteBuffer buffer, int versionId) {
        return new SaslHandshakeRequest(ProtoUtils.parseRequest(ApiKeys.SASL_HANDSHAKE.id, versionId, buffer),
                (short) versionId);
    }

    public static SaslHandshakeRequest parse(ByteBuffer buffer) {
        return parse(buffer, ProtoUtils.latestVersion(ApiKeys.SASL_HANDSHAKE.id));
    }
}

