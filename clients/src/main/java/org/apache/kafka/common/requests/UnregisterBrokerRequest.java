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

import org.apache.kafka.common.message.UnregisterBrokerRequestData;
import org.apache.kafka.common.message.UnregisterBrokerResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;

public class UnregisterBrokerRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<UnregisterBrokerRequest> {
        private final UnregisterBrokerRequestData data;

        public Builder(UnregisterBrokerRequestData data) {
            super(ApiKeys.UNREGISTER_BROKER);
            this.data = data;
        }

        @Override
        public UnregisterBrokerRequest build(short version) {
            return new UnregisterBrokerRequest(data, version);
        }
    }

    private final UnregisterBrokerRequestData data;

    public UnregisterBrokerRequest(UnregisterBrokerRequestData data, short version) {
        super(ApiKeys.UNREGISTER_BROKER, version);
        this.data = data;
    }

    @Override
    public UnregisterBrokerRequestData data() {
        return data;
    }

    @Override
    public UnregisterBrokerResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);
        return new UnregisterBrokerResponse(new UnregisterBrokerResponseData()
                .setThrottleTimeMs(throttleTimeMs)
                .setErrorCode(error.code()));
    }

    public static UnregisterBrokerRequest parse(ByteBuffer buffer, short version) {
        return new UnregisterBrokerRequest(new UnregisterBrokerRequestData(new ByteBufferAccessor(buffer), version),
                version);
    }
}
