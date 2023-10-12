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

import org.apache.kafka.common.message.GetTelemetrySubscriptionsRequestData;
import org.apache.kafka.common.message.GetTelemetrySubscriptionsResponseData;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;

public class GetTelemetrySubscriptionsRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<GetTelemetrySubscriptionsRequest> {

        private final GetTelemetrySubscriptionsRequestData data;

        public Builder(GetTelemetrySubscriptionsRequestData data) {
            this(data, false);
        }

        public Builder(GetTelemetrySubscriptionsRequestData data, boolean enableUnstableLastVersion) {
            super(ApiKeys.GET_TELEMETRY_SUBSCRIPTIONS, enableUnstableLastVersion);
            this.data = data;
        }

        @Override
        public GetTelemetrySubscriptionsRequest build(short version) {
            return new GetTelemetrySubscriptionsRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final GetTelemetrySubscriptionsRequestData data;

    public GetTelemetrySubscriptionsRequest(GetTelemetrySubscriptionsRequestData data, short version) {
        super(ApiKeys.GET_TELEMETRY_SUBSCRIPTIONS, version);
        this.data = data;
    }

    @Override
    public GetTelemetrySubscriptionsResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        GetTelemetrySubscriptionsResponseData responseData = new GetTelemetrySubscriptionsResponseData()
                .setErrorCode(Errors.forException(e).code())
                .setThrottleTimeMs(throttleTimeMs);
        return new GetTelemetrySubscriptionsResponse(responseData);
    }

    @Override
    public GetTelemetrySubscriptionsRequestData data() {
        return data;
    }

    public static GetTelemetrySubscriptionsRequest parse(ByteBuffer buffer, short version) {
        return new GetTelemetrySubscriptionsRequest(new GetTelemetrySubscriptionsRequestData(
                new ByteBufferAccessor(buffer), version), version);
    }
}