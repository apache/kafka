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
import org.apache.kafka.common.message.AlterConfigsResponseData;
import org.apache.kafka.common.message.AlterConfigsResponseData.AlterConfigsResourceResponse;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class AlterConfigsResponse extends AbstractResponse {

    private final AlterConfigsResponseData data;

    public AlterConfigsResponse(AlterConfigsResponseData data) {
        this.data = data;
    }

    public AlterConfigsResponse(Struct struct, short version) {
        this.data = new AlterConfigsResponseData(struct, version);
    }

    public AlterConfigsResponseData data() {
        return data;
    }

    public AlterConfigsResponse(Map<ConfigResource, ApiError> results, int requestThrottleMs) {
        this.data = new AlterConfigsResponseData()
                       .setThrottleTimeMs(requestThrottleMs);
        addResults(results);
    }

    public AlterConfigsResponse addResults(final Map<ConfigResource, ApiError> results) {
        List<AlterConfigsResourceResponse> originalResults = data.responses();
        final List<AlterConfigsResourceResponse> newResults = new ArrayList<>(originalResults);

        results.forEach(
            (resource, error) -> newResults.add(
                new AlterConfigsResourceResponse()
                    .setErrorCode(error.error().code())
                    .setErrorMessage(error.message())
                    .setResourceName(resource.name())
                    .setResourceType(resource.type().id()))
        );
        this.data.setResponses(newResults);

        return this;
    }

    public Map<ConfigResource, ApiError> errors() {
        return data.responses().stream().collect(Collectors.toMap(
            response -> new ConfigResource(
                    ConfigResource.Type.forId(response.resourceType()),
                    response.resourceName()),
            response -> new ApiError(Errors.forCode(response.errorCode()), response.errorMessage())
        ));
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return apiErrorCounts(errors());
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    @Override
    protected Struct toStruct(short version) {
        return data.toStruct(version);
    }

    public static AlterConfigsResponse parse(ByteBuffer buffer, short version) {
        return new AlterConfigsResponse(ApiKeys.ALTER_CONFIGS.parseResponse(version, buffer), version);
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 1;
    }
}
