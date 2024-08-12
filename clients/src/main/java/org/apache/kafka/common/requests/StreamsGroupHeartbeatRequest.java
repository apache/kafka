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

import org.apache.kafka.common.message.StreamsGroupHeartbeatRequestData;
import org.apache.kafka.common.message.StreamsGroupHeartbeatResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;

public class StreamsGroupHeartbeatRequest extends AbstractRequest {

    /**
     * A member epoch of <code>-1</code> means that the member wants to leave the group.
     */
    public static final int LEAVE_GROUP_MEMBER_EPOCH = -1;
    public static final int LEAVE_GROUP_STATIC_MEMBER_EPOCH = -2;

    /**
     * A member epoch of <code>0</code> means that the member wants to join the group.
     */
    public static final int JOIN_GROUP_MEMBER_EPOCH = 0;

    public static class Builder extends AbstractRequest.Builder<StreamsGroupHeartbeatRequest> {
        private final StreamsGroupHeartbeatRequestData data;

        public Builder(StreamsGroupHeartbeatRequestData data) {
            this(data, false);
        }

        public Builder(StreamsGroupHeartbeatRequestData data, boolean enableUnstableLastVersion) {
            super(ApiKeys.STREAMS_GROUP_HEARTBEAT, enableUnstableLastVersion);
            this.data = data;
        }

        @Override
        public StreamsGroupHeartbeatRequest build(short version) {
            return new StreamsGroupHeartbeatRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final StreamsGroupHeartbeatRequestData data;

    public StreamsGroupHeartbeatRequest(StreamsGroupHeartbeatRequestData data, short version) {
        super(ApiKeys.STREAMS_GROUP_HEARTBEAT, version);
        this.data = data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        return new StreamsGroupHeartbeatResponse(
            new StreamsGroupHeartbeatResponseData()
                .setThrottleTimeMs(throttleTimeMs)
                .setErrorCode(Errors.forException(e).code())
        );
    }

    @Override
    public StreamsGroupHeartbeatRequestData data() {
        return data;
    }

    public static StreamsGroupHeartbeatRequest parse(ByteBuffer buffer, short version) {
        return new StreamsGroupHeartbeatRequest(new StreamsGroupHeartbeatRequestData(
            new ByteBufferAccessor(buffer), version), version);
    }
}
