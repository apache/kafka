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
import org.apache.kafka.common.message.SyncGroupRequestData;
import org.apache.kafka.common.message.SyncGroupResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class SyncGroupRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<SyncGroupRequest> {

        private final SyncGroupRequestData data;

        public Builder(SyncGroupRequestData data) {
            super(ApiKeys.SYNC_GROUP);
            this.data = data;
        }

        @Override
        public SyncGroupRequest build(short version) {
            if (data.groupInstanceId() != null && version < 3) {
                throw new UnsupportedVersionException("The broker sync group protocol version " +
                        version + " does not support usage of config group.instance.id.");
            }
            return new SyncGroupRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final SyncGroupRequestData data;

    public SyncGroupRequest(SyncGroupRequestData data, short version) {
        super(ApiKeys.SYNC_GROUP, version);
        this.data = data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        return new SyncGroupResponse(new SyncGroupResponseData()
                .setErrorCode(Errors.forException(e).code())
                .setAssignment(new byte[0])
                .setThrottleTimeMs(throttleTimeMs));
    }

    public Map<String, ByteBuffer> groupAssignments() {
        Map<String, ByteBuffer> groupAssignments = new HashMap<>();
        for (SyncGroupRequestData.SyncGroupRequestAssignment assignment : data.assignments()) {
            groupAssignments.put(assignment.memberId(), ByteBuffer.wrap(assignment.assignment()));
        }
        return groupAssignments;
    }

    /**
     * ProtocolType and ProtocolName are mandatory since version 5. This methods verifies that
     * they are defined for version 5 or higher, or returns true otherwise for older versions.
     */
    public boolean areMandatoryProtocolTypeAndNamePresent() {
        if (version() >= 5)
            return data.protocolType() != null && data.protocolName() != null;
        else
            return true;
    }

    public static SyncGroupRequest parse(ByteBuffer buffer, short version) {
        return new SyncGroupRequest(new SyncGroupRequestData(new ByteBufferAccessor(buffer), version), version);
    }

    @Override
    public SyncGroupRequestData data() {
        return data;
    }
}
