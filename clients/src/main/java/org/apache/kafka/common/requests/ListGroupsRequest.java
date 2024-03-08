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
import org.apache.kafka.common.message.ListGroupsRequestData;
import org.apache.kafka.common.message.ListGroupsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;
import java.util.Collections;

/**
 * Possible error codes:
 *
 * COORDINATOR_LOAD_IN_PROGRESS (14)
 * COORDINATOR_NOT_AVAILABLE (15)
 * AUTHORIZATION_FAILED (29)
 */
public class ListGroupsRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<ListGroupsRequest> {

        private final ListGroupsRequestData data;

        public Builder(ListGroupsRequestData data) {
            super(ApiKeys.LIST_GROUPS);
            this.data = data;
        }

        @Override
        public ListGroupsRequest build(short version) {
            if (!data.statesFilter().isEmpty() && version < 4) {
                throw new UnsupportedVersionException("The broker only supports ListGroups " +
                        "v" + version + ", but we need v4 or newer to request groups by states.");
            }
            if (!data.typesFilter().isEmpty() && version < 5) {
                throw new UnsupportedVersionException("The broker only supports ListGroups " +
                    "v" + version + ", but we need v5 or newer to request groups by type.");
            }
            return new ListGroupsRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final ListGroupsRequestData data;

    public ListGroupsRequest(ListGroupsRequestData data, short version) {
        super(ApiKeys.LIST_GROUPS, version);
        this.data = data;
    }

    @Override
    public ListGroupsResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        ListGroupsResponseData listGroupsResponseData = new ListGroupsResponseData().
            setGroups(Collections.emptyList()).
            setErrorCode(Errors.forException(e).code());
        if (version() >= 1) {
            listGroupsResponseData.setThrottleTimeMs(throttleTimeMs);
        }
        return new ListGroupsResponse(listGroupsResponseData);
    }

    public static ListGroupsRequest parse(ByteBuffer buffer, short version) {
        return new ListGroupsRequest(new ListGroupsRequestData(new ByteBufferAccessor(buffer), version), version);
    }

    @Override
    public ListGroupsRequestData data() {
        return data;
    }
}
