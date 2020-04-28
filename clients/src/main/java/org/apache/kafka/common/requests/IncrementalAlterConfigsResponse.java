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
import org.apache.kafka.common.message.IncrementalAlterConfigsResponseData;
import org.apache.kafka.common.message.IncrementalAlterConfigsResponseData.AlterConfigsResourceResponse;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class IncrementalAlterConfigsResponse extends AbstractResponse {

    public static IncrementalAlterConfigsResponseData toResponseData(final int requestThrottleMs,
                                                                     final Map<ConfigResource, ApiError> results) {
        IncrementalAlterConfigsResponseData responseData = new IncrementalAlterConfigsResponseData();
        responseData.setThrottleTimeMs(requestThrottleMs);
        for (Map.Entry<ConfigResource, ApiError> entry : results.entrySet()) {
            responseData.responses().add(new AlterConfigsResourceResponse().
                    setResourceName(entry.getKey().name()).
                    setResourceType(entry.getKey().type().id()).
                    setErrorCode(entry.getValue().error().code()).
                    setErrorMessage(entry.getValue().message()));
        }
        return responseData;
    }

    public static Map<ConfigResource, ApiError> fromResponseData(final IncrementalAlterConfigsResponseData data) {
        Map<ConfigResource, ApiError> map = new HashMap<>();
        for (AlterConfigsResourceResponse response : data.responses()) {
            map.put(new ConfigResource(ConfigResource.Type.forId(response.resourceType()), response.resourceName()),
                    new ApiError(Errors.forCode(response.errorCode()), response.errorMessage()));
        }
        return map;
    }

    private final IncrementalAlterConfigsResponseData data;

    public IncrementalAlterConfigsResponse(IncrementalAlterConfigsResponseData data) {
        this.data = data;
    }

    public IncrementalAlterConfigsResponseData data() {
        return data;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        HashMap<Errors, Integer> counts = new HashMap<>();
        for (AlterConfigsResourceResponse response : data.responses()) {
            Errors error = Errors.forCode(response.errorCode());
            counts.put(error, counts.getOrDefault(error, 0) + 1);
        }
        return counts;
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 0;
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    public static IncrementalAlterConfigsResponse parse(ByteBuffer buffer, short version) {
        return new IncrementalAlterConfigsResponse(new IncrementalAlterConfigsResponseData(
            new ByteBufferAccessor(buffer), version));
    }
}
