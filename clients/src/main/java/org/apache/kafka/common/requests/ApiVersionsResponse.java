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

import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersionsResponseKey;
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersionsResponseKeyCollection;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.record.RecordBatch;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ApiVersionsResponse extends AbstractResponse {

    public final ApiVersionsResponseData data;

    // initialized lazily to avoid circular initialization dependence with ApiKeys
    private static volatile ApiVersionsResponse defaultApiVersionsResponse;

    public static final class ApiVersion {
        public final short apiKey;
        public final short minVersion;
        public final short maxVersion;

        public ApiVersion(ApiKeys apiKey) {
            this(apiKey.id, apiKey.oldestVersion(), apiKey.latestVersion());
        }

        public ApiVersion(ApiKeys apiKey, short minVersion, short maxVersion) {
            this(apiKey.id, minVersion, maxVersion);
        }

        public ApiVersion(short apiKey, short minVersion, short maxVersion) {
            this.apiKey = apiKey;
            this.minVersion = minVersion;
            this.maxVersion = maxVersion;
        }

        @Override
        public String toString() {
            return "ApiVersion(" +
                       "apiKey=" + apiKey +
                       ", minVersion=" + minVersion +
                       ", maxVersion= " + maxVersion +
                       ")";
        }

        static ApiVersionsResponseKey toResponseKey(ApiVersion apiVersion) {
            return new ApiVersionsResponseKey()
                .setIndex(apiVersion.apiKey)
                .setMaxVersion(apiVersion.maxVersion)
                .setMinVersion(apiVersion.minVersion);
        }

        static ApiVersion fromResponseKey(ApiVersionsResponseKey responseKey) {
            return new ApiVersion(responseKey.index(),
                                  responseKey.maxVersion(),
                                  responseKey.minVersion());

        }
    }

    /**
     * Possible error codes:
     *
     * UNSUPPORTED_VERSION (33)
     */
    public ApiVersionsResponse(Errors error, List<ApiVersion> apiVersions) {
        this(DEFAULT_THROTTLE_TIME, error, apiVersions);
    }

    public ApiVersionsResponse(Struct struct, short version) {
        this.data = new ApiVersionsResponseData(struct, version);
    }

    public ApiVersionsResponse(int throttleTimeMs, Errors error, List<ApiVersion> apiVersions) {
        this.data = new ApiVersionsResponseData()
            .setApiKeys(new ApiVersionsResponseKeyCollection(
                apiVersions.stream().map(ApiVersion::toResponseKey).iterator()))
            .setErrorCode(error.code())
            .setThrottleTimeMs(throttleTimeMs);
    }

    @Override
    protected Struct toStruct(short version) {
        return data.toStruct(version);
    }

    public static ApiVersionsResponse apiVersionsResponse(int throttleTimeMs, byte maxMagic) {
        if (maxMagic == RecordBatch.CURRENT_MAGIC_VALUE && throttleTimeMs == DEFAULT_THROTTLE_TIME) {
            return defaultApiVersionsResponse();
        }
        return createApiVersionsResponse(throttleTimeMs, maxMagic);
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    public Collection<ApiVersion> apiVersions() {
        return data.apiKeys().stream()
                   .map(ApiVersion::fromResponseKey).collect(Collectors.toList());
    }

    public ApiVersion apiVersion(short apiKey) {
        return ApiVersion.fromResponseKey(data.apiKeys().find(apiKey));
    }

    public Errors error() {
        return Errors.forCode(data.errorCode());
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return errorCounts(error());
    }

    public static ApiVersionsResponse parse(ByteBuffer buffer, short version) {
        return new ApiVersionsResponse(ApiKeys.API_VERSIONS.parseResponse(version, buffer), version);
    }

    public static ApiVersionsResponse createApiVersionsResponse(int throttleTimeMs, final byte minMagic) {
        List<ApiVersion> apiVersionsResponseKeys = new ArrayList<>();
        for (ApiKeys apiKey : ApiKeys.values()) {
            if (apiKey.minRequiredInterBrokerMagic <= minMagic) {
                apiVersionsResponseKeys.add(new ApiVersion(apiKey));
            }
        }
        return new ApiVersionsResponse(throttleTimeMs, Errors.NONE, apiVersionsResponseKeys);
    }

    public static ApiVersionsResponse defaultApiVersionsResponse() {
        if (defaultApiVersionsResponse == null)
            defaultApiVersionsResponse = createApiVersionsResponse(DEFAULT_THROTTLE_TIME, RecordBatch.CURRENT_MAGIC_VALUE);
        return defaultApiVersionsResponse;
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 2;
    }
}
