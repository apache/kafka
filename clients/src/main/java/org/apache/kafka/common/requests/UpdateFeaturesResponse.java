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
import java.util.Optional;
import java.util.stream.Stream;

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

    public static UpdateFeaturesResponse createWithErrors(short version, ApiError topLevelError, Map<String, ApiError> updateErrors, int throttleTimeMs) {
        final UpdatableFeatureResultCollection results = new UpdatableFeatureResultCollection();
        Optional<Map.Entry<String, ApiError>> errorEntry = Optional.empty();
        if (version > 1) {
            Stream<Map.Entry<String, ApiError>> errorEntries = updateErrors.entrySet().stream().filter(entry ->
                    !entry.getValue().error().equals(Errors.NONE));
            errorEntry = errorEntries.findFirst();
        }

        if (errorEntry.isPresent()) {
            String errorFeatureName = errorEntry.get().getKey();
            ApiError topError = errorEntry.get().getValue();
            String errorString = errorFeatureName + ":" + topError.error().exceptionName() + " (" + topError.message() + ")";
            topLevelError = new ApiError(topError.error(),
                    "The update failed for all features since the following feature had an error: " + errorString);
        } else {
            updateErrors.forEach((featureName, error) ->
                    results.add(
                            new UpdatableFeatureResult()
                                    .setFeature(featureName)
                                    .setErrorCode(error.error().code())
                                    .setErrorMessage(error.message()))
            );
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
