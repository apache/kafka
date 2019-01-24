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
import org.apache.kafka.common.message.IncrementalAlterConfigsResponseData.AlterConfigsResourceResult;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class IncrementalAlterConfigsResponse extends AbstractResponse {

    public static IncrementalAlterConfigsResponseData toResponseData(final int requestThrottleMs,
                                                                     final Map<ConfigResource, ApiError> results) {
        IncrementalAlterConfigsResponseData responseData = new IncrementalAlterConfigsResponseData();
        responseData.setThrottleTimeMs(requestThrottleMs);
        for (Map.Entry<ConfigResource, ApiError> entry : results.entrySet()) {
            responseData.responses().add(new AlterConfigsResourceResult().
                    setResourceName(entry.getKey().name()).
                    setResourceType(entry.getKey().type().id()).
                    setErrorCode(entry.getValue().error().code()).
                    setErrorMessage(entry.getValue().message()));
        }
        return responseData;
    }

    public static Map<ConfigResource, ApiError> fromResponseData(final IncrementalAlterConfigsResponseData data) {
        Map<ConfigResource, ApiError> map = new HashMap<>();
        for (AlterConfigsResourceResult result : data.responses()) {
            map.put(new ConfigResource(ConfigResource.Type.forId(result.resourceType()), result.resourceName()),
                    new ApiError(Errors.forCode(result.errorCode()), result.errorMessage()));
        }
        return map;
    }

    private final IncrementalAlterConfigsResponseData data;

    public IncrementalAlterConfigsResponse(IncrementalAlterConfigsResponseData data) {
        this.data = data;
    }

    public IncrementalAlterConfigsResponse(final Struct struct, final short version) {
        this.data = new IncrementalAlterConfigsResponseData(struct, version);
    }

    public IncrementalAlterConfigsResponseData data() {
        return data;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        HashMap<Errors, Integer> counts = new HashMap<>();
        for (AlterConfigsResourceResult result : data.responses()) {
            Errors error = Errors.forCode(result.errorCode());
            counts.put(error, counts.getOrDefault(error, 0) + 1);
        }
        return counts;
    }

    @Override
    protected Struct toStruct(final short version) {
        return data.toStruct(version);
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
        return new IncrementalAlterConfigsResponse(
                ApiKeys.INCREMENTAL_ALTER_CONFIGS.responseSchema(version).read(buffer), version);
    }
}
