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

import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.HeartbeatRequestData;
import org.apache.kafka.common.message.HeartbeatResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;

public class HeartbeatRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<HeartbeatRequest> {
        private final HeartbeatRequestData data;

        public Builder(HeartbeatRequestData data) {
            super(ApiKeys.HEARTBEAT);
            this.data = data;
        }

        @Override
        public HeartbeatRequest build(short version) {
            if (data.groupInstanceId() != null && version < 3) {
                throw new UnsupportedVersionException("The broker heartbeat protocol version " +
                        version + " does not support usage of config group.instance.id.");
            }
            return new HeartbeatRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final HeartbeatRequestData data;

    private HeartbeatRequest(HeartbeatRequestData data, short version) {
        super(ApiKeys.HEARTBEAT, version);
        this.data = data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        HeartbeatResponseData responseData = new HeartbeatResponseData().
            setErrorCode(Errors.forException(e).code());
        if (version() >= 1) {
            responseData.setThrottleTimeMs(throttleTimeMs);
        }
        return new HeartbeatResponse(responseData);
    }

    public static HeartbeatRequest parse(ByteBuffer buffer, short version) {
        return new HeartbeatRequest(new HeartbeatRequestData(new ByteBufferAccessor(buffer), version), version);
    }

    @Override
    public HeartbeatRequestData data() {
        return data;
    }
}
