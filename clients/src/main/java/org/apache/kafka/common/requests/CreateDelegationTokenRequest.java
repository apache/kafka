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

import org.apache.kafka.common.message.CreateDelegationTokenRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

import java.nio.ByteBuffer;

public class CreateDelegationTokenRequest extends AbstractRequest {

    private final CreateDelegationTokenRequestData data;

    private CreateDelegationTokenRequest(CreateDelegationTokenRequestData data, short version) {
        super(ApiKeys.CREATE_DELEGATION_TOKEN, version);
        this.data = data;
    }

    public static CreateDelegationTokenRequest parse(ByteBuffer buffer, short version) {
        return new CreateDelegationTokenRequest(new CreateDelegationTokenRequestData(new ByteBufferAccessor(buffer), version),
            version);
    }

    @Override
    public CreateDelegationTokenRequestData data() {
        return data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        return CreateDelegationTokenResponse.prepareResponse(version(), throttleTimeMs, Errors.forException(e),
            KafkaPrincipal.ANONYMOUS, KafkaPrincipal.ANONYMOUS);
    }

    public static class Builder extends AbstractRequest.Builder<CreateDelegationTokenRequest> {
        private final CreateDelegationTokenRequestData data;

        public Builder(CreateDelegationTokenRequestData data) {
            super(ApiKeys.CREATE_DELEGATION_TOKEN);
            this.data = data;
        }

        @Override
        public CreateDelegationTokenRequest build(short version) {
            return new CreateDelegationTokenRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }
}
