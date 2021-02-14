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

import org.apache.kafka.common.message.BrokerHeartbeatRequestData;
import org.apache.kafka.common.message.BrokerHeartbeatResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;

public class BrokerHeartbeatRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<BrokerHeartbeatRequest> {
        private final BrokerHeartbeatRequestData data;

        public Builder(BrokerHeartbeatRequestData data) {
            super(ApiKeys.BROKER_HEARTBEAT);
            this.data = data;
        }

        @Override
        public BrokerHeartbeatRequest build(short version) {
            return new BrokerHeartbeatRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final BrokerHeartbeatRequestData data;

    public BrokerHeartbeatRequest(BrokerHeartbeatRequestData data, short version) {
        super(ApiKeys.BROKER_HEARTBEAT, version);
        this.data = data;
    }

    @Override
    public BrokerHeartbeatRequestData data() {
        return data;
    }

    @Override
    public BrokerHeartbeatResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);
        return new BrokerHeartbeatResponse(new BrokerHeartbeatResponseData()
                .setThrottleTimeMs(throttleTimeMs)
                .setErrorCode(error.code()));
    }

    public static BrokerHeartbeatRequest parse(ByteBuffer buffer, short version) {
        return new BrokerHeartbeatRequest(new BrokerHeartbeatRequestData(new ByteBufferAccessor(buffer), version),
                version);
    }
}
