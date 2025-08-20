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

import org.apache.kafka.common.GroupType;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.ListGroupsRequestData;
import org.apache.kafka.common.message.ListGroupsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Readable;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;

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
                // Types filter is supported by brokers with version 3.8.0 or later. Older brokers only support
                // classic groups, so listing consumer groups on an older broker does not need to use a types filter.
                // If the types filter is only for consumer and classic, or just classic groups, it can be safely omitted.
                // This allows a modern admin client to list consumer groups on older brokers in a straightforward way.
                HashSet<String> typesCopy = new HashSet<>(data.typesFilter());
                boolean containedClassic = typesCopy.remove(GroupType.CLASSIC.toString());
                boolean containedConsumer = typesCopy.remove(GroupType.CONSUMER.toString());
                if (!typesCopy.isEmpty() || (!containedClassic && containedConsumer)) {
                    throw new UnsupportedVersionException("The broker only supports ListGroups " +
                        "v" + version + ", but we need v5 or newer to request groups by type. " +
                        "Requested group types: [" + String.join(", ", data.typesFilter()) + "].");
                }
                return new ListGroupsRequest(data.duplicate().setTypesFilter(List.of()), version);
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

    public static ListGroupsRequest parse(Readable readable, short version) {
        return new ListGroupsRequest(new ListGroupsRequestData(readable, version), version);
    }

    @Override
    public ListGroupsRequestData data() {
        return data;
    }
}
