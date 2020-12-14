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

import org.apache.kafka.common.message.SaslHandshakeResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

/**
 * Response from SASL server which indicates if the client-chosen mechanism is enabled in the server.
 * For error responses, the list of enabled mechanisms is included in the response.
 */
public class SaslHandshakeResponse extends AbstractResponse {

    private final SaslHandshakeResponseData data;

    public SaslHandshakeResponse(SaslHandshakeResponseData data) {
        super(ApiKeys.SASL_HANDSHAKE);
        this.data = data;
    }

    /*
    * Possible error codes:
    *   UNSUPPORTED_SASL_MECHANISM(33): Client mechanism not enabled in server
    *   ILLEGAL_SASL_STATE(34) : Invalid request during SASL handshake
    */
    public Errors error() {
        return Errors.forCode(data.errorCode());
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return errorCounts(Errors.forCode(data.errorCode()));
    }

    @Override
    public int throttleTimeMs() {
        return DEFAULT_THROTTLE_TIME;
    }

    @Override
    public SaslHandshakeResponseData data() {
        return data;
    }

    public List<String> enabledMechanisms() {
        return data.mechanisms();
    }

    public static SaslHandshakeResponse parse(ByteBuffer buffer, short version) {
        return new SaslHandshakeResponse(new SaslHandshakeResponseData(new ByteBufferAccessor(buffer), version));
    }
}
