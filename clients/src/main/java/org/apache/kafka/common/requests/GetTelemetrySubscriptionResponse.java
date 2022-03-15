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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.GetTelemetrySubscriptionsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.CompressionType;

import java.nio.ByteBuffer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.List;

public class GetTelemetrySubscriptionResponse  extends AbstractResponse {
    private final GetTelemetrySubscriptionsResponseData data;

    public GetTelemetrySubscriptionResponse(GetTelemetrySubscriptionsResponseData data) {
        super(ApiKeys.GET_TELEMETRY_SUBSCRIPTIONS);
        this.data = data;
    }

    public GetTelemetrySubscriptionResponse(int throttleTime,
                                            short errorCode,
                                            Uuid clientInstanceId,
                                            int subscriptionId,
                                            int pushInterval,
                                            boolean deltaValues,
                                            List<String> metrics) {
        super(ApiKeys.GET_TELEMETRY_SUBSCRIPTIONS);
        this.data =  new GetTelemetrySubscriptionsResponseData()
                .setThrottleTimeMs(throttleTime)
                .setErrorCode(errorCode)
                .setClientInstanceId(clientInstanceId)
                .setAcceptedCompressionTypes(getSupportedCompressionTypes())
                .setSubscriptionId(subscriptionId)
                .setPushIntervalMs(pushInterval)
                .setDeltaTemporality(deltaValues)
                .setRequestedMetrics(metrics);
    }

    @Override
    public GetTelemetrySubscriptionsResponseData data() {
        return data;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        HashMap<Errors, Integer> counts = new HashMap<>();
        updateErrorCounts(counts, Errors.forCode(data.errorCode()));
        return counts;
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    public boolean hasError() {
        return error() != Errors.NONE;
    }

    public Errors error() {
        return Errors.forCode(data.errorCode());
    }

    public static GetTelemetrySubscriptionResponse parse(ByteBuffer buffer, short version) {
        return new GetTelemetrySubscriptionResponse(new GetTelemetrySubscriptionsResponseData(
                new ByteBufferAccessor(buffer), version));
    }

    private List<Byte> getSupportedCompressionTypes() {
        List<Byte> compressionTypes = new ArrayList<>();
        for (CompressionType c : CompressionType.values()) {
            compressionTypes.add((byte) c.id);
        }
        return compressionTypes;
    }
}
