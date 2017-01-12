/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.common.requests;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ProtoUtils;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;

public class GroupCoordinatorRequest extends AbstractRequest {
    private static final String GROUP_ID_KEY_NAME = "group_id";

    public static class Builder extends AbstractRequest.Builder<GroupCoordinatorRequest> {
        private final String groupId;

        public Builder(String groupId) {
            super(ApiKeys.GROUP_COORDINATOR);
            this.groupId = groupId;
        }

        @Override
        public GroupCoordinatorRequest build() {
            short version = version();
            return new GroupCoordinatorRequest(this.groupId, version);
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(type=GroupCoordinatorRequest, groupId=");
            bld.append(groupId).append(")");
            return bld.toString();
        }
    }

    private final String groupId;

    private GroupCoordinatorRequest(String groupId, short version) {
        super(new Struct(ProtoUtils.requestSchema(ApiKeys.GROUP_COORDINATOR.id, version)),
                version);
        struct.set(GROUP_ID_KEY_NAME, groupId);
        this.groupId = groupId;
    }

    public GroupCoordinatorRequest(Struct struct, short versionId) {
        super(struct, versionId);
        groupId = struct.getString(GROUP_ID_KEY_NAME);
    }

    @Override
    public AbstractResponse getErrorResponse(Throwable e) {
        short versionId = version();
        switch (versionId) {
            case 0:
                return new GroupCoordinatorResponse(Errors.GROUP_COORDINATOR_NOT_AVAILABLE.code(), Node.noNode());
            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                        versionId, this.getClass().getSimpleName(), ProtoUtils.latestVersion(ApiKeys.GROUP_COORDINATOR.id)));
        }
    }

    public String groupId() {
        return groupId;
    }

    public static GroupCoordinatorRequest parse(ByteBuffer buffer, int versionId) {
        return new GroupCoordinatorRequest(ProtoUtils.parseRequest(ApiKeys.GROUP_COORDINATOR.id, versionId, buffer),
                (short) versionId);
    }

    public static GroupCoordinatorRequest parse(ByteBuffer buffer) {
        return parse(buffer, ProtoUtils.latestVersion(ApiKeys.GROUP_COORDINATOR.id));
    }
}
