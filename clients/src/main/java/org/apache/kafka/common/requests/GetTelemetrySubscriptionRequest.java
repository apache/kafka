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
import org.apache.kafka.common.message.GetTelemetrySubscriptionsRequestData;
import org.apache.kafka.common.message.GetTelemetrySubscriptionsResponseData;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;

public class GetTelemetrySubscriptionRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<GetTelemetrySubscriptionRequest> {
        private Uuid clientInstanceId;

        public Builder(Uuid clientInstanceId) {
            super(ApiKeys.GET_TELEMETRY_SUBSCRIPTIONS);
            this.clientInstanceId = clientInstanceId;
        }

        @Override
        public GetTelemetrySubscriptionRequest build(short version) {
            GetTelemetrySubscriptionsRequestData data = new GetTelemetrySubscriptionsRequestData();
            data.setClientInstanceId(clientInstanceId);
            return new GetTelemetrySubscriptionRequest(data, version);
        }
    }

    private final GetTelemetrySubscriptionsRequestData data;

    public GetTelemetrySubscriptionRequest(GetTelemetrySubscriptionsRequestData data, short version) {
        super(ApiKeys.GET_TELEMETRY_SUBSCRIPTIONS, version);
        this.data = data;
    }

    @Override
    public GetTelemetrySubscriptionResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        GetTelemetrySubscriptionsResponseData responseData = new GetTelemetrySubscriptionsResponseData()
                .setErrorCode(Errors.forException(e).code())
                .setThrottleTimeMs(throttleTimeMs);
        return new GetTelemetrySubscriptionResponse(responseData);
    }

    @Override
    public GetTelemetrySubscriptionsRequestData data() {
        return data;
    }

    public Uuid getClientInstanceId() {
        return this.data.clientInstanceId();
    }

    public static GetTelemetrySubscriptionRequest parse(ByteBuffer buffer, short version) {
        return new GetTelemetrySubscriptionRequest(new GetTelemetrySubscriptionsRequestData(
                new ByteBufferAccessor(buffer), version), version);
    }
}