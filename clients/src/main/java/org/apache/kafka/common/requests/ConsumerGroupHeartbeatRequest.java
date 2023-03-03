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

import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;

public class ConsumerGroupHeartbeatRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<ConsumerGroupHeartbeatRequest> {
        private final ConsumerGroupHeartbeatRequestData data;

        public Builder(ConsumerGroupHeartbeatRequestData data) {
            super(ApiKeys.CONSUMER_GROUP_HEARTBEAT);
            this.data = data;
        }

        @Override
        public ConsumerGroupHeartbeatRequest build(short version) {
            return new ConsumerGroupHeartbeatRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final ConsumerGroupHeartbeatRequestData data;

    public ConsumerGroupHeartbeatRequest(ConsumerGroupHeartbeatRequestData data, short version) {
        super(ApiKeys.CONSUMER_GROUP_HEARTBEAT, version);
        this.data = data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        return new ConsumerGroupHeartbeatResponse(
            new ConsumerGroupHeartbeatResponseData()
                .setThrottleTimeMs(throttleTimeMs)
                .setErrorCode(Errors.forException(e).code())
        );
    }

    @Override
    public ConsumerGroupHeartbeatRequestData data() {
        return data;
    }

    public static ConsumerGroupHeartbeatRequest parse(ByteBuffer buffer, short version) {
        return new ConsumerGroupHeartbeatRequest(new ConsumerGroupHeartbeatRequestData(
            new ByteBufferAccessor(buffer), version), version);
    }
}
