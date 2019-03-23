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

import org.apache.kafka.common.message.SaslAuthenticateResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;


/**
 * Response from SASL server which for a SASL challenge as defined by the SASL protocol
 * for the mechanism configured for the client.
 */
public class SaslAuthenticateResponse extends AbstractResponse {

    private final SaslAuthenticateResponseData data;

    public SaslAuthenticateResponse(SaslAuthenticateResponseData data) {
        this.data = data;
    }

    public SaslAuthenticateResponse(Struct struct, short version) {
        this.data = new SaslAuthenticateResponseData(struct, version);
    }

    /**
     * Possible error codes:
     *   SASL_AUTHENTICATION_FAILED(57) : Authentication failed
     */
    public Errors error() {
        return Errors.forCode(data.errorCode());
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return Collections.singletonMap(Errors.forCode(data.errorCode()), 1);
    }

    public String errorMessage() {
        return data.errorMessage();
    }

    public long sessionLifetimeMs() {
        return data.sessionLifetimeMs();
    }

    public byte[] saslAuthBytes() {
        return data.authBytes();
    }

    @Override
    public Struct toStruct(short version) {
        return data.toStruct(version);
    }

    public static SaslAuthenticateResponse parse(ByteBuffer buffer, short version) {
        return new SaslAuthenticateResponse(ApiKeys.SASL_AUTHENTICATE.parseResponse(version, buffer), version);
    }
}