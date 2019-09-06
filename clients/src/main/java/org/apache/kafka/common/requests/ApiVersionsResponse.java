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
import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.record.RecordBatch;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.protocol.CommonFields.ERROR_CODE;
import static org.apache.kafka.common.protocol.CommonFields.THROTTLE_TIME_MS;
import static org.apache.kafka.common.protocol.types.Type.INT16;

public class ApiVersionsResponse extends AbstractResponse {
    private static final String API_VERSIONS_KEY_NAME = "api_versions";
    private static final String API_KEY_NAME = "api_key";
    private static final String MIN_VERSION_KEY_NAME = "min_version";
    private static final String MAX_VERSION_KEY_NAME = "max_version";

    private static final Schema API_VERSIONS_V0 = new Schema(
            new Field(API_KEY_NAME, INT16, "API key."),
            new Field(MIN_VERSION_KEY_NAME, INT16, "Minimum supported version."),
            new Field(MAX_VERSION_KEY_NAME, INT16, "Maximum supported version."));

    private static final Schema API_VERSIONS_RESPONSE_V0 = new Schema(
            ERROR_CODE,
            new Field(API_VERSIONS_KEY_NAME, new ArrayOf(API_VERSIONS_V0), "API versions supported by the broker."));
    private static final Schema API_VERSIONS_RESPONSE_V1 = new Schema(
            ERROR_CODE,
            new Field(API_VERSIONS_KEY_NAME, new ArrayOf(API_VERSIONS_V0), "API versions supported by the broker."),
            THROTTLE_TIME_MS);

    /**
     * The version number is bumped to indicate that on quota violation brokers send out responses before throttling.
     */
    private static final Schema API_VERSIONS_RESPONSE_V2 = API_VERSIONS_RESPONSE_V1;

    // initialized lazily to avoid circular initialization dependence with ApiKeys
    private static volatile ApiVersionsResponse defaultApiVersionsResponse;

    public static Schema[] schemaVersions() {
        return new Schema[]{API_VERSIONS_RESPONSE_V0, API_VERSIONS_RESPONSE_V1, API_VERSIONS_RESPONSE_V2};
    }

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
        this.throttleTimeMs = struct.getOrElse(THROTTLE_TIME_MS, DEFAULT_THROTTLE_TIME);
        this.error = Errors.forCode(struct.get(ERROR_CODE));
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
        struct.setIfExists(THROTTLE_TIME_MS, throttleTimeMs);
        struct.set(ERROR_CODE, error.code());
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

    public static ApiVersionsResponse apiVersionsResponse(int throttleTimeMs, byte maxMagic) {
        if (maxMagic == RecordBatch.CURRENT_MAGIC_VALUE && throttleTimeMs == DEFAULT_THROTTLE_TIME) {
            return defaultApiVersionsResponse();
        }
        return createApiVersionsResponse(throttleTimeMs, maxMagic);
    }

    @Override
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

    @Override
    public Map<Errors, Integer> errorCounts() {
        return errorCounts(error);
    }

    public static ApiVersionsResponse parse(ByteBuffer buffer, short version) {
        return new ApiVersionsResponse(ApiKeys.API_VERSIONS.parseResponse(version, buffer));
    }

    private Map<Short, ApiVersion> buildApiKeyToApiVersion(List<ApiVersion> apiVersions) {
        Map<Short, ApiVersion> tempApiIdToApiVersion = new HashMap<>();
        for (ApiVersion apiVersion : apiVersions) {
            tempApiIdToApiVersion.put(apiVersion.apiKey, apiVersion);
        }
        return tempApiIdToApiVersion;
    }

    public static ApiVersionsResponse createApiVersionsResponse(int throttleTimeMs, final byte minMagic) {
        List<ApiVersionsResponse.ApiVersion> versionList = new ArrayList<>();
        for (ApiKeys apiKey : ApiKeys.values()) {
            if (apiKey.minRequiredInterBrokerMagic <= minMagic) {
                versionList.add(new ApiVersionsResponse.ApiVersion(apiKey));
            }
        }
        return new ApiVersionsResponse(throttleTimeMs, Errors.NONE, versionList);
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
