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
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.Collections;

public class ListGroupsRequest extends AbstractRequest {

    /* List groups api */
    private static final Schema LIST_GROUPS_REQUEST_V0 = new Schema();

    /* v1 request is the same as v0. Throttle time has been added to response */
    private static final Schema LIST_GROUPS_REQUEST_V1 = LIST_GROUPS_REQUEST_V0;

    /**
     * The version number is bumped to indicate that on quota violation brokers send out responses before throttling.
     */
    private static final Schema LIST_GROUPS_REQUEST_V2 = LIST_GROUPS_REQUEST_V1;

    public static Schema[] schemaVersions() {
        return new Schema[] {LIST_GROUPS_REQUEST_V0, LIST_GROUPS_REQUEST_V1,
            LIST_GROUPS_REQUEST_V2};
    }

    public static class Builder extends AbstractRequest.Builder<ListGroupsRequest> {
        public Builder() {
            super(ApiKeys.LIST_GROUPS);
        }

        @Override
        public ListGroupsRequest build(short version) {
            return new ListGroupsRequest(version);
        }

        @Override
        public String toString() {
            return "(type=ListGroupsRequest)";
        }
    }

    public ListGroupsRequest(short version) {
        super(ApiKeys.LIST_GROUPS, version);
    }

    public ListGroupsRequest(Struct struct, short versionId) {
        super(ApiKeys.LIST_GROUPS, versionId);
    }

    @Override
    public ListGroupsResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        short versionId = version();
        switch (versionId) {
            case 0:
                return new ListGroupsResponse(Errors.forException(e), Collections.emptyList());
            case 1:
            case 2:
                return new ListGroupsResponse(throttleTimeMs, Errors.forException(e), Collections.emptyList());
            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                        versionId, this.getClass().getSimpleName(), ApiKeys.LIST_GROUPS.latestVersion()));
        }
    }

    public static ListGroupsRequest parse(ByteBuffer buffer, short version) {
        return new ListGroupsRequest(ApiKeys.LIST_GROUPS.parseRequest(version, buffer), version);
    }

    @Override
    protected Struct toStruct() {
        return new Struct(ApiKeys.LIST_GROUPS.requestSchema(version()));
    }
}
