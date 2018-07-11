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

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.utils.Utils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.apache.kafka.common.protocol.types.Type.STRING;

public class DescribeGroupsRequest extends AbstractRequest {
    private static final String GROUP_IDS_KEY_NAME = "group_ids";

    /* Describe group api */
    private static final Schema DESCRIBE_GROUPS_REQUEST_V0 = new Schema(
            new Field(GROUP_IDS_KEY_NAME, new ArrayOf(STRING), "List of groupIds to request metadata for (an " +
                    "empty groupId array will return empty group metadata)."));

    /* v1 request is the same as v0. Throttle time has been added to response */
    private static final Schema DESCRIBE_GROUPS_REQUEST_V1 = DESCRIBE_GROUPS_REQUEST_V0;

    /**
     * The version number is bumped to indicate that on quota violation brokers send out responses before throttling.
     */
    private static final Schema DESCRIBE_GROUPS_REQUEST_V2 = DESCRIBE_GROUPS_REQUEST_V1;

    public static Schema[] schemaVersions() {
        return new Schema[]{DESCRIBE_GROUPS_REQUEST_V0, DESCRIBE_GROUPS_REQUEST_V1, DESCRIBE_GROUPS_REQUEST_V2};
    }

    public static class Builder extends AbstractRequest.Builder<DescribeGroupsRequest> {
        private final List<String> groupIds;

        public Builder(List<String> groupIds) {
            super(ApiKeys.DESCRIBE_GROUPS);
            this.groupIds = groupIds;
        }

        @Override
        public DescribeGroupsRequest build(short version) {
            return new DescribeGroupsRequest(this.groupIds, version);
        }

        @Override
        public String toString() {
            return "(type=DescribeGroupsRequest, groupIds=(" + Utils.join(groupIds, ",") + "))";
        }
    }

    private final List<String> groupIds;

    private DescribeGroupsRequest(List<String> groupIds, short version) {
        super(version);
        this.groupIds = groupIds;
    }

    public DescribeGroupsRequest(Struct struct, short version) {
        super(version);
        this.groupIds = new ArrayList<>();
        for (Object groupId : struct.getArray(GROUP_IDS_KEY_NAME))
            this.groupIds.add((String) groupId);
    }

    public List<String> groupIds() {
        return groupIds;
    }

    @Override
    protected Struct toStruct() {
        Struct struct = new Struct(ApiKeys.DESCRIBE_GROUPS.requestSchema(version()));
        struct.set(GROUP_IDS_KEY_NAME, groupIds.toArray());
        return struct;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        short version = version();
        switch (version) {
            case 0:
                return DescribeGroupsResponse.fromError(Errors.forException(e), groupIds);
            case 1:
            case 2:
                return DescribeGroupsResponse.fromError(throttleTimeMs, Errors.forException(e), groupIds);

            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                        version, this.getClass().getSimpleName(), ApiKeys.DESCRIBE_GROUPS.latestVersion()));
        }
    }

    public static DescribeGroupsRequest parse(ByteBuffer buffer, short version) {
        return new DescribeGroupsRequest(ApiKeys.DESCRIBE_GROUPS.parseRequest(version, buffer), version);
    }
}
