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

import org.apache.kafka.common.message.ExpireDelegationTokenRequestData;
import org.apache.kafka.common.message.ExpireDelegationTokenResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

public class ExpireDelegationTokenRequest extends AbstractRequest {

    private final ExpireDelegationTokenRequestData data;

    private ExpireDelegationTokenRequest(ExpireDelegationTokenRequestData data, short version) {
        super(ApiKeys.EXPIRE_DELEGATION_TOKEN, version);
        this.data = data;
    }

    public static ExpireDelegationTokenRequest parse(ByteBuffer buffer, short version) {
        return new ExpireDelegationTokenRequest(
            new ExpireDelegationTokenRequestData(new ByteBufferAccessor(buffer), version), version);
    }

    @Override
    public ExpireDelegationTokenRequestData data() {
        return data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        return new ExpireDelegationTokenResponse(
                new ExpireDelegationTokenResponseData()
                    .setErrorCode(Errors.forException(e).code())
                    .setThrottleTimeMs(throttleTimeMs));
    }

    public ByteBuffer hmac() {
        return ByteBuffer.wrap(data.hmac());
    }

    public long expiryTimePeriod() {
        return data.expiryTimePeriodMs();
    }

    public static class Builder extends AbstractRequest.Builder<ExpireDelegationTokenRequest> {
        private final ExpireDelegationTokenRequestData data;

        public Builder(ExpireDelegationTokenRequestData data) {
            super(ApiKeys.EXPIRE_DELEGATION_TOKEN);
            this.data = data;
        }

        @Override
        public ExpireDelegationTokenRequest build(short version) {
            return new ExpireDelegationTokenRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }
}
