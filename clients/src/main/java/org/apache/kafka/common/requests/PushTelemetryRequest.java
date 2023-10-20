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

import org.apache.kafka.common.message.PushTelemetryRequestData;
import org.apache.kafka.common.message.PushTelemetryResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;

public class PushTelemetryRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<PushTelemetryRequest> {

        private final PushTelemetryRequestData data;

        public Builder(PushTelemetryRequestData data) {
            this(data, false);
        }

        public Builder(PushTelemetryRequestData data, boolean enableUnstableLastVersion) {
            super(ApiKeys.PUSH_TELEMETRY, enableUnstableLastVersion);
            this.data = data;
        }

        @Override
        public PushTelemetryRequest build(short version) {
            return new PushTelemetryRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final PushTelemetryRequestData data;

    public PushTelemetryRequest(PushTelemetryRequestData data, short version) {
        super(ApiKeys.PUSH_TELEMETRY, version);
        this.data = data;
    }

    @Override
    public PushTelemetryResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        PushTelemetryResponseData responseData = new PushTelemetryResponseData()
                .setErrorCode(Errors.forException(e).code())
                .setThrottleTimeMs(throttleTimeMs);
        return new PushTelemetryResponse(responseData);
    }

    @Override
    public PushTelemetryRequestData data() {
        return data;
    }

    public static PushTelemetryRequest parse(ByteBuffer buffer, short version) {
        return new PushTelemetryRequest(new PushTelemetryRequestData(
                new ByteBufferAccessor(buffer), version), version);
    }
}
