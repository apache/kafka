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

import org.apache.kafka.common.message.UpdateFeaturesResponseData;
import org.apache.kafka.common.message.UpdateFeaturesResponseData.UpdatableFeatureResult;
import org.apache.kafka.common.message.UpdateFeaturesResponseData.UpdatableFeatureResultCollection;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Possible error codes:
 *
 *   - {@link Errors#CLUSTER_AUTHORIZATION_FAILED}
 *   - {@link Errors#NOT_CONTROLLER}
 *   - {@link Errors#INVALID_REQUEST}
 *   - {@link Errors#FEATURE_UPDATE_FAILED}
 */
public class UpdateFeaturesResponse extends AbstractResponse {

    private final UpdateFeaturesResponseData data;

    public UpdateFeaturesResponse(UpdateFeaturesResponseData data) {
        super(ApiKeys.UPDATE_FEATURES);
        this.data = data;
    }

    public ApiError topLevelError() {
        return new ApiError(Errors.forCode(data.errorCode()), data.errorMessage());
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        Map<Errors, Integer> errorCounts = new HashMap<>();
        updateErrorCounts(errorCounts, Errors.forCode(data.errorCode()));
        for (UpdatableFeatureResult result : data.results()) {
            updateErrorCounts(errorCounts, Errors.forCode(result.errorCode()));
        }
        return errorCounts;
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    @Override
    public void maybeSetThrottleTimeMs(int throttleTimeMs) {
        data.setThrottleTimeMs(throttleTimeMs);
    }

    @Override
    public String toString() {
        return data.toString();
    }

    @Override
    public UpdateFeaturesResponseData data() {
        return data;
    }

    public static UpdateFeaturesResponse parse(ByteBuffer buffer, short version) {
        return new UpdateFeaturesResponse(new UpdateFeaturesResponseData(new ByteBufferAccessor(buffer), version));
    }

    public static UpdateFeaturesResponse createWithErrors(ApiError topLevelError, Set<String> updates, int throttleTimeMs) {
        final UpdatableFeatureResultCollection results = new UpdatableFeatureResultCollection();
        if (topLevelError == ApiError.NONE) {
            for (final String feature : updates) {
                final UpdatableFeatureResult result = new UpdatableFeatureResult();
                result.setFeature(feature)
                    .setErrorCode(topLevelError.error().code())
                    .setErrorMessage(topLevelError.message());
                results.add(result);
            }
        }
        final UpdateFeaturesResponseData responseData = new UpdateFeaturesResponseData()
            .setThrottleTimeMs(throttleTimeMs)
            .setErrorCode(topLevelError.error().code())
            .setErrorMessage(topLevelError.message())
            .setResults(results)
            .setThrottleTimeMs(throttleTimeMs);
        return new UpdateFeaturesResponse(responseData);
    }
}
