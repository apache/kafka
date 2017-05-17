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

import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ApiVersionsResponse extends AbstractResponse {

    public static final ApiVersionsResponse API_VERSIONS_RESPONSE = createApiVersionsResponse(DEFAULT_THROTTLE_TIME);
    private static final String THROTTLE_TIME_KEY_NAME = "throttle_time_ms";
    public static final String ERROR_CODE_KEY_NAME = "error_code";
    public static final String API_VERSIONS_KEY_NAME = "api_versions";
    public static final String API_KEY_NAME = "api_key";
    public static final String MIN_VERSION_KEY_NAME = "min_version";
    public static final String MAX_VERSION_KEY_NAME = "max_version";

    /**
     * Possible error codes:
     *
     * UNSUPPORTED_VERSION (33)
     */
    private final Errors error;
    private final int throttleTimeMs;
    private final Map<Short, ApiVersion> apiKeyToApiVersion;

    public static final class ApiVersion {
        public final short apiKey;
        public final short minVersion;
        public final short maxVersion;

        public ApiVersion(ApiKeys apiKey) {
            this(apiKey.id, apiKey.oldestVersion(), apiKey.latestVersion());
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
    }

    public ApiVersionsResponse(Errors error, List<ApiVersion> apiVersions) {
        this(DEFAULT_THROTTLE_TIME, error, apiVersions);
    }

    public ApiVersionsResponse(int throttleTimeMs, Errors error, List<ApiVersion> apiVersions) {
        this.throttleTimeMs = throttleTimeMs;
        this.error = error;
        this.apiKeyToApiVersion = buildApiKeyToApiVersion(apiVersions);
    }

    public ApiVersionsResponse(Struct struct) {
        this.throttleTimeMs = struct.hasField(THROTTLE_TIME_KEY_NAME) ? struct.getInt(THROTTLE_TIME_KEY_NAME) : DEFAULT_THROTTLE_TIME;
        this.error = Errors.forCode(struct.getShort(ERROR_CODE_KEY_NAME));
        List<ApiVersion> tempApiVersions = new ArrayList<>();
        for (Object apiVersionsObj : struct.getArray(API_VERSIONS_KEY_NAME)) {
            Struct apiVersionStruct = (Struct) apiVersionsObj;
            short apiKey = apiVersionStruct.getShort(API_KEY_NAME);
            short minVersion = apiVersionStruct.getShort(MIN_VERSION_KEY_NAME);
            short maxVersion = apiVersionStruct.getShort(MAX_VERSION_KEY_NAME);
            tempApiVersions.add(new ApiVersion(apiKey, minVersion, maxVersion));
        }
        this.apiKeyToApiVersion = buildApiKeyToApiVersion(tempApiVersions);
    }

    @Override
    protected Struct toStruct(short version) {
        Struct struct = new Struct(ApiKeys.API_VERSIONS.responseSchema(version));
        if (struct.hasField(THROTTLE_TIME_KEY_NAME))
            struct.set(THROTTLE_TIME_KEY_NAME, throttleTimeMs);
        struct.set(ERROR_CODE_KEY_NAME, error.code());
        List<Struct> apiVersionList = new ArrayList<>();
        for (ApiVersion apiVersion : apiKeyToApiVersion.values()) {
            Struct apiVersionStruct = struct.instance(API_VERSIONS_KEY_NAME);
            apiVersionStruct.set(API_KEY_NAME, apiVersion.apiKey);
            apiVersionStruct.set(MIN_VERSION_KEY_NAME, apiVersion.minVersion);
            apiVersionStruct.set(MAX_VERSION_KEY_NAME, apiVersion.maxVersion);
            apiVersionList.add(apiVersionStruct);
        }
        struct.set(API_VERSIONS_KEY_NAME, apiVersionList.toArray());
        return struct;
    }

    public static ApiVersionsResponse apiVersionsResponse(short version, int throttleTimeMs) {
        if (throttleTimeMs == 0 || version == 0)
            return API_VERSIONS_RESPONSE;
        else
            return createApiVersionsResponse(throttleTimeMs);
    }

    /**
     * Returns Errors.UNSUPPORTED_VERSION response with version 0 since we don't support the requested version.
     */
    public static Send unsupportedVersionSend(String destination, RequestHeader requestHeader) {
        ApiVersionsResponse response = new ApiVersionsResponse(DEFAULT_THROTTLE_TIME, Errors.UNSUPPORTED_VERSION,
                Collections.<ApiVersion>emptyList());
        return response.toSend(destination, (short) 0, requestHeader.toResponseHeader());
    }

    public int throttleTimeMs() {
        return throttleTimeMs;
    }

    public Collection<ApiVersion> apiVersions() {
        return apiKeyToApiVersion.values();
    }

    public ApiVersion apiVersion(short apiKey) {
        return apiKeyToApiVersion.get(apiKey);
    }

    public Errors error() {
        return error;
    }

    public static ApiVersionsResponse parse(ByteBuffer buffer, short version) {
        return new ApiVersionsResponse(ApiKeys.API_VERSIONS.parseResponse(version, buffer));
    }

    public static ApiVersionsResponse createApiVersionsResponse(int throttleTimeMs) {
        List<ApiVersion> versionList = new ArrayList<>();
        for (ApiKeys apiKey : ApiKeys.values()) {
            versionList.add(new ApiVersion(apiKey));
        }
        return new ApiVersionsResponse(throttleTimeMs, Errors.NONE, versionList);
    }

    private Map<Short, ApiVersion> buildApiKeyToApiVersion(List<ApiVersion> apiVersions) {
        Map<Short, ApiVersion> tempApiIdToApiVersion = new HashMap<>();
        for (ApiVersion apiVersion: apiVersions) {
            tempApiIdToApiVersion.put(apiVersion.apiKey, apiVersion);
        }
        return tempApiIdToApiVersion;
    }
}
