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
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;

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

    public final SyncGroupRequestData data;

    public SyncGroupRequest(SyncGroupRequestData data, short version) {
        super(ApiKeys.SYNC_GROUP, version);
        this.data = data;
    }

    public SyncGroupRequest(Struct struct, short version) {
        super(ApiKeys.SYNC_GROUP, version);
        this.data = new SyncGroupRequestData(struct, version);
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        short versionId = version();
        switch (versionId) {
            case 0:
                return new SyncGroupResponse(
                        new SyncGroupResponseData()
                            .setErrorCode(Errors.forException(e).code())
                            .setAssignment(new byte[0])
                       );
            case 1:
            case 2:
            case 3:
                return new SyncGroupResponse(
                        new SyncGroupResponseData()
                            .setErrorCode(Errors.forException(e).code())
                            .setAssignment(new byte[0])
                            .setThrottleTimeMs(throttleTimeMs)
                );
            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                        versionId, this.getClass().getSimpleName(), ApiKeys.SYNC_GROUP.latestVersion()));
        }
    }

    public Map<String, ByteBuffer> groupAssignments() {
        Map<String, ByteBuffer> groupAssignments = new HashMap<>();
        for (SyncGroupRequestData.SyncGroupRequestAssignment assignment : data.assignments()) {
            groupAssignments.put(assignment.memberId(), ByteBuffer.wrap(assignment.assignment()));
        }
        return groupAssignments;
    }

    public static SyncGroupRequest parse(ByteBuffer buffer, short version) {
        return new SyncGroupRequest(ApiKeys.SYNC_GROUP.parseRequest(version, buffer), version);
    }

    @Override
    protected Struct toStruct() {
        return data.toStruct(version());
    }
}
