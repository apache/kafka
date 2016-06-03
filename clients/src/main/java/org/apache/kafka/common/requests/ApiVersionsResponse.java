/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.common.requests;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ProtoUtils;
import org.apache.kafka.common.protocol.Protocol;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ApiVersionsResponse extends AbstractRequestResponse {

    private static final Schema CURRENT_SCHEMA = ProtoUtils.currentResponseSchema(ApiKeys.API_VERSIONS.id);
    private static final ApiVersionsResponse API_VERSIONS_RESPONSE = createApiVersionsResponse();

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
    private final short errorCode;
    private final Map<Short, ApiVersion> apiKeyToApiVersion;

    public static final class ApiVersion {
        public final short apiKey;
        public final short minVersion;
        public final short maxVersion;

        public ApiVersion(short apiKey, short minVersion, short maxVersion) {
            this.apiKey = apiKey;
            this.minVersion = minVersion;
            this.maxVersion = maxVersion;
        }
    }

    public ApiVersionsResponse(short errorCode, List<ApiVersion> apiVersions) {
        super(new Struct(CURRENT_SCHEMA));
        struct.set(ERROR_CODE_KEY_NAME, errorCode);
        List<Struct> apiVersionList = new ArrayList<>();
        for (ApiVersion apiVersion : apiVersions) {
            Struct apiVersionStruct = struct.instance(API_VERSIONS_KEY_NAME);
            apiVersionStruct.set(API_KEY_NAME, apiVersion.apiKey);
            apiVersionStruct.set(MIN_VERSION_KEY_NAME, apiVersion.minVersion);
            apiVersionStruct.set(MAX_VERSION_KEY_NAME, apiVersion.maxVersion);
            apiVersionList.add(apiVersionStruct);
        }
        struct.set(API_VERSIONS_KEY_NAME, apiVersionList.toArray());
        this.errorCode = errorCode;
        this.apiKeyToApiVersion = buildApiKeyToApiVersion(apiVersions);
    }

    public ApiVersionsResponse(Struct struct) {
        super(struct);
        this.errorCode = struct.getShort(ERROR_CODE_KEY_NAME);
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

    public Collection<ApiVersion> apiVersions() {
        return apiKeyToApiVersion.values();
    }

    public ApiVersion apiVersion(short apiKey) {
        return apiKeyToApiVersion.get(apiKey);
    }

    public short errorCode() {
        return errorCode;
    }

    public static ApiVersionsResponse parse(ByteBuffer buffer) {
        return new ApiVersionsResponse(CURRENT_SCHEMA.read(buffer));
    }

    public static ApiVersionsResponse fromError(Errors error) {
        return new ApiVersionsResponse(error.code(), Collections.<ApiVersion>emptyList());
    }

    public static ApiVersionsResponse apiVersionsResponse() {
        return API_VERSIONS_RESPONSE;
    }

    private static ApiVersionsResponse createApiVersionsResponse() {
        List<ApiVersion> versionList = new ArrayList<>();
        for (ApiKeys apiKey : ApiKeys.values()) {
            versionList.add(new ApiVersion(apiKey.id, Protocol.MIN_VERSIONS[apiKey.id], Protocol.CURR_VERSION[apiKey.id]));
        }
        return new ApiVersionsResponse(Errors.NONE.code(), versionList);
    }

    private Map<Short, ApiVersion> buildApiKeyToApiVersion(List<ApiVersion> apiVersions) {
        Map<Short, ApiVersion> tempApiIdToApiVersion = new HashMap<>();
        for (ApiVersion apiVersion: apiVersions) {
            tempApiIdToApiVersion.put(apiVersion.apiKey, apiVersion);
        }
        return tempApiIdToApiVersion;
    }
}