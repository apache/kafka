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

import java.nio.ByteBuffer;
import java.util.Collections;

public class ListGroupsRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<ListGroupsRequest> {
        public Builder() {
            super(ApiKeys.LIST_GROUPS);
        }

        @Override
        public ListGroupsRequest build() {
            return new ListGroupsRequest(version());
        }

        @Override
        public String toString() {
            return "(type=ListGroupsRequest)";
        }
    }

    public ListGroupsRequest(short version) {
        super(new Struct(ProtoUtils.requestSchema(ApiKeys.LIST_GROUPS.id, version)),
                version);
    }

    public ListGroupsRequest(Struct struct, short versionId) {
        super(struct, versionId);
    }

    @Override
    public AbstractResponse getErrorResponse(Throwable e) {
        short versionId = version();
        switch (versionId) {
            case 0:
                short errorCode = Errors.forException(e).code();
                return new ListGroupsResponse(errorCode, Collections.<ListGroupsResponse.Group>emptyList());
            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                        versionId, this.getClass().getSimpleName(), ProtoUtils.latestVersion(ApiKeys.LIST_GROUPS.id)));
        }
    }

    public static ListGroupsRequest parse(ByteBuffer buffer, int versionId) {
        return new ListGroupsRequest(ProtoUtils.parseRequest(ApiKeys.LIST_GROUPS.id, versionId, buffer),
                (short) versionId);
    }

    public static ListGroupsRequest parse(ByteBuffer buffer) {
        return parse(buffer, ProtoUtils.latestVersion(ApiKeys.LIST_GROUPS.id));
    }
}
