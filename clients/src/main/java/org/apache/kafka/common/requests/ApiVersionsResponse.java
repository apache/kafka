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
import org.apache.kafka.common.protocol.types.SchemaException;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.record.RecordBatch;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Possible error codes:
 * - {@link Errors#UNSUPPORTED_VERSION}
 * - {@link Errors#INVALID_REQUEST}
 */
public class ApiVersionsResponse extends AbstractResponse {

    public static final int UNKNOWN_FINALIZED_FEATURES_EPOCH = -1;

    public static final ApiVersionsResponse DEFAULT_API_VERSIONS_RESPONSE =
        createApiVersionsResponse(
            DEFAULT_THROTTLE_TIME,
            RecordBatch.CURRENT_MAGIC_VALUE,
            Features.emptySupportedFeatures(),
            Features.emptyFinalizedFeatures(),
            UNKNOWN_FINALIZED_FEATURES_EPOCH);

    public final ApiVersionsResponseData data;

    public ApiVersionsResponse(ApiVersionsResponseData data) {
        this.data = data;
    }

    public ApiVersionsResponse(Struct struct) {
        this(new ApiVersionsResponseData(struct, (short) (ApiVersionsResponseData.SCHEMAS.length - 1)));
    }

    public ApiVersionsResponse(Struct struct, short version) {
        this(new ApiVersionsResponseData(struct, version));
    }

    @Override
    protected Struct toStruct(short version) {
        return this.data.toStruct(version);
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
        // falls back while parsing it into a Struct which means that the version received by this
        // method is not necessary the real one. It may be version 0 as well.
        int prev = buffer.position();
        try {
            return new ApiVersionsResponse(
                new ApiVersionsResponseData(new ByteBufferAccessor(buffer), version));
        } catch (RuntimeException e) {
            buffer.position(prev);
            if (version != 0)
                return new ApiVersionsResponse(
                    new ApiVersionsResponseData(new ByteBufferAccessor(buffer), (short) 0));
            else
                throw e;
        }
    }

    public static ApiVersionsResponse fromStruct(Struct struct, short version) {
        // Fallback to version 0 for ApiVersions response. If a client sends an ApiVersionsRequest
        // using a version higher than that supported by the broker, a version 0 response is sent
        // to the client indicating UNSUPPORTED_VERSION. When the client receives the response, it
        // falls back while parsing it into a Struct which means that the version received by this
        // method is not necessary the real one. It may be version 0 as well.
        try {
            return new ApiVersionsResponse(struct, version);
        } catch (SchemaException e) {
            if (version != 0)
                return new ApiVersionsResponse(struct, (short) 0);
            else
                throw e;
        }
    }

    public static ApiVersionsResponse apiVersionsResponse(
        int throttleTimeMs,
        byte maxMagic,
        Features<SupportedVersionRange> latestSupportedFeatures) {
        return apiVersionsResponse(
            throttleTimeMs, maxMagic, latestSupportedFeatures, Features.emptyFinalizedFeatures(), UNKNOWN_FINALIZED_FEATURES_EPOCH);
    }

    public static ApiVersionsResponse apiVersionsResponse(
        int throttleTimeMs,
        byte maxMagic,
        Features<SupportedVersionRange> latestSupportedFeatures,
        Features<FinalizedVersionRange> finalizedFeatures,
        int finalizedFeaturesEpoch) {
        if (maxMagic == RecordBatch.CURRENT_MAGIC_VALUE && throttleTimeMs == DEFAULT_THROTTLE_TIME) {
            return DEFAULT_API_VERSIONS_RESPONSE;
        }
        return createApiVersionsResponse(
            throttleTimeMs, maxMagic, latestSupportedFeatures, finalizedFeatures, finalizedFeaturesEpoch);
    }

    public static ApiVersionsResponse createApiVersionsResponse(
        final int throttleTimeMs,
        final byte minMagic) {
        return createApiVersionsResponse(
            throttleTimeMs,
            minMagic,
            Features.emptySupportedFeatures(),
            Features.emptyFinalizedFeatures(),
            UNKNOWN_FINALIZED_FEATURES_EPOCH);
    }

    public static ApiVersionsResponse createApiVersionsResponse(
        final int throttleTimeMs,
        final byte minMagic,
        final Features<SupportedVersionRange> latestSupportedFeatures,
        final Features<FinalizedVersionRange> finalizedFeatures,
        final int finalizedFeaturesEpoch
    ) {
        ApiVersionsResponseKeyCollection apiKeys = new ApiVersionsResponseKeyCollection();
        for (ApiKeys apiKey : ApiKeys.values()) {
            if (apiKey.minRequiredInterBrokerMagic <= minMagic) {
                apiKeys.add(new ApiVersionsResponseKey()
                    .setApiKey(apiKey.id)
                    .setMinVersion(apiKey.oldestVersion())
                    .setMaxVersion(apiKey.latestVersion()));
            }
        }

        ApiVersionsResponseData data = new ApiVersionsResponseData();
        data.setThrottleTimeMs(throttleTimeMs);
        data.setErrorCode(Errors.NONE.code());
        data.setApiKeys(apiKeys);
        data.setSupportedFeatures(createSupportedFeatureKeys(latestSupportedFeatures));
        data.setFinalizedFeatures(createFinalizedFeatureKeys(finalizedFeatures));
        data.setFinalizedFeaturesEpoch(finalizedFeaturesEpoch);

        return new ApiVersionsResponse(data);
    }

    private static SupportedFeatureKeyCollection createSupportedFeatureKeys(
        Features<SupportedVersionRange> latestSupportedFeatures) {
        SupportedFeatureKeyCollection converted = new SupportedFeatureKeyCollection();
        for (Map.Entry<String, SupportedVersionRange> feature : latestSupportedFeatures.features().entrySet()) {
            SupportedFeatureKey key = new SupportedFeatureKey();
            key.setName(feature.getKey());
            key.setMinVersion(feature.getValue().min());
            key.setMaxVersion(feature.getValue().max());
            converted.add(key);
        }

        return converted;
    }

    private static FinalizedFeatureKeyCollection createFinalizedFeatureKeys(
        Features<FinalizedVersionRange> finalizedFeatures) {
        FinalizedFeatureKeyCollection converted = new FinalizedFeatureKeyCollection();
        for (Map.Entry<String, FinalizedVersionRange> feature : finalizedFeatures.features().entrySet()) {
            FinalizedFeatureKey key = new FinalizedFeatureKey();
            key.setName(feature.getKey());
            key.setMinVersionLevel(feature.getValue().min());
            key.setMaxVersionLevel(feature.getValue().max());
            converted.add(key);
        }

        return converted;
    }
}
