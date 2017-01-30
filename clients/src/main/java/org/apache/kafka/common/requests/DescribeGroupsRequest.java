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

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ProtoUtils;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.utils.Utils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class DescribeGroupsRequest extends AbstractRequest {
    private static final String GROUP_IDS_KEY_NAME = "group_ids";

    public static class Builder extends AbstractRequest.Builder<DescribeGroupsRequest> {
        private final List<String> groupIds;

        public Builder(List<String> groupIds) {
            super(ApiKeys.DESCRIBE_GROUPS);
            this.groupIds = groupIds;
        }

        @Override
        public DescribeGroupsRequest build() {
            short version = version();
            return new DescribeGroupsRequest(this.groupIds, version);
        }

        @Override
        public String toString() {
            return "(type=DescribeGroupsRequest, groupIds=(" + Utils.join(groupIds, ",") + "))";
        }
    }

    private final List<String> groupIds;

    private DescribeGroupsRequest(List<String> groupIds, short version) {
        super(new Struct(ProtoUtils.requestSchema(ApiKeys.DESCRIBE_GROUPS.id, version)),
                version);
        struct.set(GROUP_IDS_KEY_NAME, groupIds.toArray());
        this.groupIds = groupIds;
    }

    public DescribeGroupsRequest(Struct struct, short version) {
        super(struct, version);
        this.groupIds = new ArrayList<>();
        for (Object groupId : struct.getArray(GROUP_IDS_KEY_NAME))
            this.groupIds.add((String) groupId);
    }

    public List<String> groupIds() {
        return groupIds;
    }

    @Override
    public AbstractResponse getErrorResponse(Throwable e) {
        short versionId = version();
        switch (versionId) {
            case 0:
                return DescribeGroupsResponse.fromError(Errors.forException(e), groupIds);

            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                        versionId, this.getClass().getSimpleName(), ProtoUtils.latestVersion(ApiKeys.DESCRIBE_GROUPS.id)));
        }
    }

    public static DescribeGroupsRequest parse(ByteBuffer buffer, int versionId) {
        return new DescribeGroupsRequest(ProtoUtils.parseRequest(ApiKeys.DESCRIBE_GROUPS.id, versionId, buffer),
                (short) versionId);
    }

    public static DescribeGroupsRequest parse(ByteBuffer buffer) {
        return parse(buffer, ProtoUtils.latestVersion(ApiKeys.DESCRIBE_GROUPS.id));
    }
}
