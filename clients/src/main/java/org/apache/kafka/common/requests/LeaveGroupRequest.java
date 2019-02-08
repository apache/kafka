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

import org.apache.kafka.common.message.LeaveGroupRequestData;
import org.apache.kafka.common.message.LeaveGroupResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;

public class LeaveGroupRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<LeaveGroupRequest> {

        private final LeaveGroupRequestData data;

        public Builder(LeaveGroupRequestData data) {
            super(ApiKeys.LEAVE_GROUP);
            this.data = data;
        }

        @Override
        public LeaveGroupRequest build(short version) {
            return new LeaveGroupRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }
    private final LeaveGroupRequestData data;
    private final short version;

    private LeaveGroupRequest(LeaveGroupRequestData data, short version) {
        super(ApiKeys.LEAVE_GROUP, version);
        this.data = data;
        this.version = version;
    }

    public LeaveGroupRequest(Struct struct, short version) {
        super(ApiKeys.LEAVE_GROUP, version);
        this.data = new LeaveGroupRequestData(struct, version);
        this.version = version;
    }

    public LeaveGroupRequestData data() {
        return data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        LeaveGroupResponseData response = new LeaveGroupResponseData();
        if (version() >= 2) {
            response.setThrottleTimeMs(throttleTimeMs);
        }
        return new LeaveGroupResponse(response);
    }

    public static LeaveGroupRequest parse(ByteBuffer buffer, short version) {
        return new LeaveGroupRequest(ApiKeys.LEAVE_GROUP.parseRequest(version, buffer), version);
    }

    @Override
    protected Struct toStruct() {
        return data.toStruct(version);
    }
}
