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
import org.apache.kafka.common.message.ListConfigResourcesResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Readable;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

public class ListConfigResourcesResponse extends AbstractResponse {
    private final ListConfigResourcesResponseData data;

    public ListConfigResourcesResponse(ListConfigResourcesResponseData data) {
        super(ApiKeys.LIST_CONFIG_RESOURCES);
        this.data = data;
    }

    public ListConfigResourcesResponseData data() {
        return data;
    }

    public ApiError error() {
        return new ApiError(Errors.forCode(data.errorCode()));
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return errorCounts(Errors.forCode(data.errorCode()));
    }

    public static ListConfigResourcesResponse parse(Readable readable, short version) {
        return new ListConfigResourcesResponse(new ListConfigResourcesResponseData(
            readable, version));
    }

    @Override
    public String toString() {
        return data.toString();
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    @Override
    public void maybeSetThrottleTimeMs(int throttleTimeMs) {
        data.setThrottleTimeMs(throttleTimeMs);
    }

    public Collection<ConfigResource> configResources() {
        return data.configResources()
            .stream()
            .map(entry ->
                new ConfigResource(
                    ConfigResource.Type.forId(entry.resourceType()),
                    entry.resourceName()
                )
            ).collect(Collectors.toList());
    }
}
