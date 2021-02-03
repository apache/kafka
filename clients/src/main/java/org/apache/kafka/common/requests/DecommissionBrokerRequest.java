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

import org.apache.kafka.common.message.DecommissionBrokerRequestData;
import org.apache.kafka.common.message.DecommissionBrokerResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;

public class DecommissionBrokerRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<DecommissionBrokerRequest> {
        private final DecommissionBrokerRequestData data;

        public Builder(DecommissionBrokerRequestData data) {
            super(ApiKeys.DECOMMISSION_BROKER);
            this.data = data;
        }

        @Override
        public DecommissionBrokerRequest build(short version) {
            return new DecommissionBrokerRequest(data, version);
        }
    }

    private final DecommissionBrokerRequestData data;

    public DecommissionBrokerRequest(DecommissionBrokerRequestData data, short version) {
        super(ApiKeys.DECOMMISSION_BROKER, version);
        this.data = data;
    }

    @Override
    public DecommissionBrokerRequestData data() {
        return data;
    }

    @Override
    public DecommissionBrokerResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);
        return new DecommissionBrokerResponse(new DecommissionBrokerResponseData()
                .setThrottleTimeMs(throttleTimeMs)
                .setErrorCode(error.code()));
    }

    public static DecommissionBrokerRequest parse(ByteBuffer buffer, short version) {
        return new DecommissionBrokerRequest(new DecommissionBrokerRequestData(new ByteBufferAccessor(buffer), version),
                version);
    }
}
