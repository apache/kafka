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

import org.apache.kafka.common.message.SaslAuthenticateRequestData;
import org.apache.kafka.common.message.SaslAuthenticateResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;

import java.nio.ByteBuffer;


/**
 * Request from SASL client containing client SASL authentication token as defined by the
 * SASL protocol for the configured SASL mechanism.
 * <p/>
 * For interoperability with versions prior to Kafka 1.0.0, this request is used only with broker
 * version 1.0.0 and higher that support SaslHandshake request v1. Clients connecting to older
 * brokers will send SaslHandshake request v0 followed by SASL tokens without the Kafka request headers.
 */
public class SaslAuthenticateRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<SaslAuthenticateRequest> {
        private final SaslAuthenticateRequestData data;

        public Builder(SaslAuthenticateRequestData data) {
            super(ApiKeys.SASL_AUTHENTICATE);
            this.data = data;
        }

        @Override
        public SaslAuthenticateRequest build(short version) {
            return new SaslAuthenticateRequest(data, version);
        }

        @Override
        public String toString() {
            return "(type=SaslAuthenticateRequest)";
        }
    }

    private final SaslAuthenticateRequestData data;

    public SaslAuthenticateRequest(SaslAuthenticateRequestData data, short version) {
        super(ApiKeys.SASL_AUTHENTICATE, version);
        this.data = data;
    }

    @Override
    public SaslAuthenticateRequestData data() {
        return data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        ApiError apiError = ApiError.fromThrowable(e);
        SaslAuthenticateResponseData response = new SaslAuthenticateResponseData()
                .setErrorCode(apiError.error().code())
                .setErrorMessage(apiError.message());
        return new SaslAuthenticateResponse(response);
    }

    public static SaslAuthenticateRequest parse(ByteBuffer buffer, short version) {
        return new SaslAuthenticateRequest(new SaslAuthenticateRequestData(new ByteBufferAccessor(buffer), version),
            version);
    }
}

