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

import org.apache.kafka.common.message.ShareGroupHeartbeatRequestData;
import org.apache.kafka.common.message.ShareGroupHeartbeatResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Readable;

public class ShareGroupHeartbeatRequest extends AbstractRequest {
    /**
     * A member epoch of <code>-1</code> means that the member wants to leave the group.
     */
    public static final int LEAVE_GROUP_MEMBER_EPOCH = -1;

    /**
     * A member epoch of <code>0</code> means that the member wants to join the group.
     */
    public static final int JOIN_GROUP_MEMBER_EPOCH = 0;

    public static class Builder extends AbstractRequest.Builder<ShareGroupHeartbeatRequest> {
        private final ShareGroupHeartbeatRequestData data;

        public Builder(ShareGroupHeartbeatRequestData data) {
            super(ApiKeys.SHARE_GROUP_HEARTBEAT);
            this.data = data;
        }

        @Override
        public ShareGroupHeartbeatRequest build(short version) {
            return new ShareGroupHeartbeatRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final ShareGroupHeartbeatRequestData data;

    public ShareGroupHeartbeatRequest(ShareGroupHeartbeatRequestData data, short version) {
        super(ApiKeys.SHARE_GROUP_HEARTBEAT, version);
        this.data = data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        return new ShareGroupHeartbeatResponse(
                new ShareGroupHeartbeatResponseData()
                        .setThrottleTimeMs(throttleTimeMs)
                        .setErrorCode(Errors.forException(e).code())
        );
    }

    @Override
    public ShareGroupHeartbeatRequestData data() {
        return data;
    }

    public static ShareGroupHeartbeatRequest parse(Readable readable, short version) {
        return new ShareGroupHeartbeatRequest(new ShareGroupHeartbeatRequestData(
                readable, version), version);
    }
}
