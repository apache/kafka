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

import org.apache.kafka.common.message.ControllerHeartbeatRequestData;
import org.apache.kafka.common.message.ControllerHeartbeatResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;

public class ControllerHeartbeatRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<ControllerHeartbeatRequest> {
        private final ControllerHeartbeatRequestData data;

        public Builder(ControllerHeartbeatRequestData data) {
            super(ApiKeys.CONTROLLER_HEARTBEAT);
            this.data = data;
        }

        @Override
        public ControllerHeartbeatRequest build(short version) {
            return new ControllerHeartbeatRequest(data, version);
        }
    }

    private final ControllerHeartbeatRequestData data;

    public ControllerHeartbeatRequest(ControllerHeartbeatRequestData data, short version) {
        super(ApiKeys.CONTROLLER_HEARTBEAT, version);
        this.data = data;
    }

    public ControllerHeartbeatRequest(Struct struct, short version) {
        super(ApiKeys.CONTROLLER_HEARTBEAT, version);
        this.data = new ControllerHeartbeatRequestData(struct, version);
    }

    public ControllerHeartbeatRequestData data() {
        return data;
    }

    @Override
    protected Struct toStruct() {
        return data.toStruct(version());
    }

    @Override
    public ControllerHeartbeatResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);
        return new ControllerHeartbeatResponse(new ControllerHeartbeatResponseData()
                .setThrottleTimeMs(throttleTimeMs)
                .setErrorCode(error.code()));
    }

    public static ControllerHeartbeatRequest parse(ByteBuffer buffer, short version) {
        return new ControllerHeartbeatRequest(
            ApiKeys.CONTROLLER_HEARTBEAT.parseRequest(version, buffer), version);
    }
}
