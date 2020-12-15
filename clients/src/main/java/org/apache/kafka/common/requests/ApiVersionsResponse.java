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

import org.apache.kafka.common.feature.Features;
import org.apache.kafka.common.feature.FinalizedVersionRange;
import org.apache.kafka.common.feature.SupportedVersionRange;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersionsResponseKey;
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersionsResponseKeyCollection;
import org.apache.kafka.common.message.ApiVersionsResponseData.FinalizedFeatureKey;
import org.apache.kafka.common.message.ApiVersionsResponseData.FinalizedFeatureKeyCollection;
import org.apache.kafka.common.message.ApiVersionsResponseData.SupportedFeatureKey;
import org.apache.kafka.common.message.ApiVersionsResponseData.SupportedFeatureKeyCollection;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.RecordBatch;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Possible error codes:
 * - {@link Errors#UNSUPPORTED_VERSION}
 * - {@link Errors#INVALID_REQUEST}
 */
public class ApiVersionsResponse extends AbstractResponse {

    public static final long UNKNOWN_FINALIZED_FEATURES_EPOCH = -1L;

    public static final ApiVersionsResponse DEFAULT_API_VERSIONS_RESPONSE = createApiVersionsResponse(
            DEFAULT_THROTTLE_TIME, RecordBatch.CURRENT_MAGIC_VALUE);

    private final ApiVersionsResponseData data;

    public ApiVersionsResponse(ApiVersionsResponseData data) {
        super(ApiKeys.API_VERSIONS);
        this.data = data;
    }

    @Override
    public ApiVersionsResponseData data() {
        return data;
    }

    public ApiVersionsResponseKey apiVersion(short apiKey) {
        return data.apiKeys().find(apiKey);
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return errorCounts(Errors.forCode(this.data.errorCode()));
    }

    @Override
    public int throttleTimeMs() {
        return this.data.throttleTimeMs();
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 2;
    }

    public static ApiVersionsResponse parse(ByteBuffer buffer, short version) {
        // Fallback to version 0 for ApiVersions response. If a client sends an ApiVersionsRequest
        // using a version higher than that supported by the broker, a version 0 response is sent
        // to the client indicating UNSUPPORTED_VERSION. When the client receives the response, it
        // falls back while parsing it which means that the version received by this
        // method is not necessarily the real one. It may be version 0 as well.
        int prev = buffer.position();
        try {
            return new ApiVersionsResponse(new ApiVersionsResponseData(new ByteBufferAccessor(buffer), version));
        } catch (RuntimeException e) {
            buffer.position(prev);
            if (version != 0)
                return new ApiVersionsResponse(new ApiVersionsResponseData(new ByteBufferAccessor(buffer), (short) 0));
            else
                throw e;
        }
    }

    public static ApiVersionsResponse createApiVersionsResponse(final int throttleTimeMs, final byte minMagic) {
        return createApiVersionsResponse(throttleTimeMs, minMagic, Features.emptySupportedFeatures(),
            Features.emptyFinalizedFeatures(), UNKNOWN_FINALIZED_FEATURES_EPOCH);
    }

    private static ApiVersionsResponse createApiVersionsResponse(
        final int throttleTimeMs,
        final byte minMagic,
        final Features<SupportedVersionRange> latestSupportedFeatures,
        final Features<FinalizedVersionRange> finalizedFeatures,
        final long finalizedFeaturesEpoch) {
        return new ApiVersionsResponse(
            createApiVersionsResponseData(
                throttleTimeMs,
                Errors.NONE,
                defaultApiKeys(minMagic),
                latestSupportedFeatures,
                finalizedFeatures,
                finalizedFeaturesEpoch));
    }

    public static ApiVersionsResponseKeyCollection defaultApiKeys(final byte minMagic) {
        ApiVersionsResponseKeyCollection apiKeys = new ApiVersionsResponseKeyCollection();
        for (ApiKeys apiKey : ApiKeys.enabledApis()) {
            if (apiKey.minRequiredInterBrokerMagic <= minMagic) {
                apiKeys.add(new ApiVersionsResponseKey()
                                .setApiKey(apiKey.id)
                                .setMinVersion(apiKey.oldestVersion())
                                .setMaxVersion(apiKey.latestVersion()));
            }
        }
        return apiKeys;
    }

    public static ApiVersionsResponseData createApiVersionsResponseData(
        final int throttleTimeMs,
        final Errors error,
        final ApiVersionsResponseKeyCollection apiKeys,
        final Features<SupportedVersionRange> latestSupportedFeatures,
        final Features<FinalizedVersionRange> finalizedFeatures,
        final long finalizedFeaturesEpoch
    ) {
        final ApiVersionsResponseData data = new ApiVersionsResponseData();
        data.setThrottleTimeMs(throttleTimeMs);
        data.setErrorCode(error.code());
        data.setApiKeys(apiKeys);
        data.setSupportedFeatures(createSupportedFeatureKeys(latestSupportedFeatures));
        data.setFinalizedFeatures(createFinalizedFeatureKeys(finalizedFeatures));
        data.setFinalizedFeaturesEpoch(finalizedFeaturesEpoch);

        return data;
    }

    private static SupportedFeatureKeyCollection createSupportedFeatureKeys(
        Features<SupportedVersionRange> latestSupportedFeatures) {
        SupportedFeatureKeyCollection converted = new SupportedFeatureKeyCollection();
        for (Map.Entry<String, SupportedVersionRange> feature : latestSupportedFeatures.features().entrySet()) {
            final SupportedFeatureKey key = new SupportedFeatureKey();
            final SupportedVersionRange versionRange = feature.getValue();
            key.setName(feature.getKey());
            key.setMinVersion(versionRange.min());
            key.setMaxVersion(versionRange.max());
            converted.add(key);
        }

        return converted;
    }

    private static FinalizedFeatureKeyCollection createFinalizedFeatureKeys(
        Features<FinalizedVersionRange> finalizedFeatures) {
        FinalizedFeatureKeyCollection converted = new FinalizedFeatureKeyCollection();
        for (Map.Entry<String, FinalizedVersionRange> feature : finalizedFeatures.features().entrySet()) {
            final FinalizedFeatureKey key = new FinalizedFeatureKey();
            final FinalizedVersionRange versionLevelRange = feature.getValue();
            key.setName(feature.getKey());
            key.setMinVersionLevel(versionLevelRange.min());
            key.setMaxVersionLevel(versionLevelRange.max());
            converted.add(key);
        }

        return converted;
    }
}
