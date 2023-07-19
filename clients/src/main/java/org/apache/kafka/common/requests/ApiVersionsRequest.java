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

import java.util.regex.Pattern;
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersionCollection;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.AppInfoParser;

import java.nio.ByteBuffer;

public class ApiVersionsRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<ApiVersionsRequest> {
        private static final String DEFAULT_CLIENT_SOFTWARE_NAME = "apache-kafka-java";

        private static final ApiVersionsRequestData DATA = new ApiVersionsRequestData()
            .setClientSoftwareName(DEFAULT_CLIENT_SOFTWARE_NAME)
            .setClientSoftwareVersion(AppInfoParser.getVersion());

        public Builder() {
            super(ApiKeys.API_VERSIONS);
        }

        public Builder(short version) {
            super(ApiKeys.API_VERSIONS, version);
        }

        @Override
        public ApiVersionsRequest build(short version) {
            return new ApiVersionsRequest(DATA, version);
        }

        @Override
        public String toString() {
            return DATA.toString();
        }
    }

    private static final Pattern SOFTWARE_NAME_VERSION_PATTERN = Pattern.compile("[a-zA-Z0-9](?:[a-zA-Z0-9\\-.]*[a-zA-Z0-9])?");

    private final Short unsupportedRequestVersion;

    private final ApiVersionsRequestData data;

    public ApiVersionsRequest(ApiVersionsRequestData data, short version) {
        this(data, version, null);
    }

    public ApiVersionsRequest(ApiVersionsRequestData data, short version, Short unsupportedRequestVersion) {
        super(ApiKeys.API_VERSIONS, version);
        this.data = data;

        // Unlike other request types, the broker handles ApiVersion requests with higher versions than
        // supported. It does so by treating the request as if it were v0 and returns a response using
        // the v0 response schema. The reason for this is that the client does not yet know what versions
        // a broker supports when this request is sent, so instead of assuming the lowest supported version,
        // it can use the most recent version and only fallback to the old version when necessary.
        this.unsupportedRequestVersion = unsupportedRequestVersion;
    }

    public boolean hasUnsupportedRequestVersion() {
        return unsupportedRequestVersion != null;
    }

    public boolean isValid() {
        if (version() >= 3) {
            return SOFTWARE_NAME_VERSION_PATTERN.matcher(data.clientSoftwareName()).matches() &&
                SOFTWARE_NAME_VERSION_PATTERN.matcher(data.clientSoftwareVersion()).matches();
        } else {
            return true;
        }
    }

    @Override
    public ApiVersionsRequestData data() {
        return data;
    }

    @Override
    public ApiVersionsResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        ApiVersionsResponseData data = new ApiVersionsResponseData()
            .setErrorCode(Errors.forException(e).code());

        if (version() >= 1) {
            data.setThrottleTimeMs(throttleTimeMs);
        }

        // Starting from Apache Kafka 2.4 (KIP-511), ApiKeys field is populated with the supported
        // versions of the ApiVersionsRequest when an UNSUPPORTED_VERSION error is returned.
        if (Errors.forException(e) == Errors.UNSUPPORTED_VERSION) {
            ApiVersionCollection apiKeys = new ApiVersionCollection();
            apiKeys.add(ApiVersionsResponse.toApiVersion(ApiKeys.API_VERSIONS));
            data.setApiKeys(apiKeys);
        }

        return new ApiVersionsResponse(data);
    }

    public static ApiVersionsRequest parse(ByteBuffer buffer, short version) {
        return new ApiVersionsRequest(new ApiVersionsRequestData(new ByteBufferAccessor(buffer), version), version);
    }

}
