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

import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.ListConfigResourcesRequestData;
import org.apache.kafka.common.message.ListConfigResourcesResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Readable;

import java.util.HashSet;
import java.util.Set;

public class ListConfigResourcesRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<ListConfigResourcesRequest> {
        public final ListConfigResourcesRequestData data;

        public Builder(ListConfigResourcesRequestData data) {
            super(ApiKeys.LIST_CONFIG_RESOURCES);
            this.data = data;
        }

        @Override
        public ListConfigResourcesRequest build(short version) {
            if (version == 0) {
                // The v0 only supports CLIENT_METRICS resource type.
                Set<Byte> resourceTypes = new HashSet<>(data.resourceTypes());
                if (resourceTypes.size() != 1 || !resourceTypes.contains(ConfigResource.Type.CLIENT_METRICS.id())) {
                    throw new UnsupportedVersionException("The v0 ListConfigResources only supports CLIENT_METRICS");
                }
                // The v0 request does not have resource types field, so creating a new request data.
                return new ListConfigResourcesRequest(new ListConfigResourcesRequestData(), version);
            }
            return new ListConfigResourcesRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final ListConfigResourcesRequestData data;

    private ListConfigResourcesRequest(ListConfigResourcesRequestData data, short version) {
        super(ApiKeys.LIST_CONFIG_RESOURCES, version);
        this.data = data;
    }

    public ListConfigResourcesRequestData data() {
        return data;
    }

    @Override
    public ListConfigResourcesResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);
        ListConfigResourcesResponseData response = new ListConfigResourcesResponseData()
            .setErrorCode(error.code())
            .setThrottleTimeMs(throttleTimeMs);
        return new ListConfigResourcesResponse(response);
    }

    public static ListConfigResourcesRequest parse(Readable readable, short version) {
        return new ListConfigResourcesRequest(new ListConfigResourcesRequestData(
            readable, version), version);
    }

    @Override
    public String toString(boolean verbose) {
        return data.toString();
    }

    /**
     * Return the supported config resource types in different request version.
     * If there is a new config resource type, the ListConfigResourcesRequest should bump a new request version to include it.
     * For v0, the supported config resource types contain CLIENT_METRICS (16).
     * For v1, the supported config resource types contain TOPIC (2), BROKER (4), BROKER_LOGGER (8), CLIENT_METRICS (16), and GROUP (32).
     */
    public Set<Byte> supportedResourceTypes() {
        return version() == 0 ?
            Set.of(ConfigResource.Type.CLIENT_METRICS.id()) :
            Set.of(
                ConfigResource.Type.TOPIC.id(),
                ConfigResource.Type.BROKER.id(),
                ConfigResource.Type.BROKER_LOGGER.id(),
                ConfigResource.Type.CLIENT_METRICS.id(),
                ConfigResource.Type.GROUP.id()
            );
    }
}
