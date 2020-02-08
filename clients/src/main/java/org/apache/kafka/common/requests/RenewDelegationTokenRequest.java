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

import org.apache.kafka.common.message.RenewDelegationTokenRequestData;
import org.apache.kafka.common.message.RenewDelegationTokenResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;

public class RenewDelegationTokenRequest extends AbstractRequest {

    private final RenewDelegationTokenRequestData data;

    public RenewDelegationTokenRequest(RenewDelegationTokenRequestData data, short version) {
        super(ApiKeys.RENEW_DELEGATION_TOKEN, version);
        this.data = data;
    }

    public RenewDelegationTokenRequest(Struct struct, short version) {
        super(ApiKeys.RENEW_DELEGATION_TOKEN, version);
        this.data = new RenewDelegationTokenRequestData(struct, version);
    }

    public static RenewDelegationTokenRequest parse(ByteBuffer buffer, short version) {
        return new RenewDelegationTokenRequest(ApiKeys.RENEW_DELEGATION_TOKEN.parseRequest(version, buffer), version);
    }

    @Override
    protected Struct toStruct() {
        return data.toStruct(version());
    }

    public RenewDelegationTokenRequestData data() {
        return data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        return new RenewDelegationTokenResponse(
                new RenewDelegationTokenResponseData()
                    .setThrottleTimeMs(throttleTimeMs)
                    .setErrorCode(Errors.forException(e).code()));
    }

    public static class Builder extends AbstractRequest.Builder<RenewDelegationTokenRequest> {
        private final RenewDelegationTokenRequestData data;

        public Builder(RenewDelegationTokenRequestData data) {
            super(ApiKeys.RENEW_DELEGATION_TOKEN);
            this.data = data;
        }

        @Override
        public RenewDelegationTokenRequest build(short version) {
            return new RenewDelegationTokenRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }
}
