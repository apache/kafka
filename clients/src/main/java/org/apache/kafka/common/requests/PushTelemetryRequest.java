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
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.telemetry.internals.ClientTelemetryUtils;

import java.nio.ByteBuffer;

public class PushTelemetryRequest extends AbstractRequest {

    private static final String OTLP_CONTENT_TYPE = "OTLP";

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
        return errorResponse(throttleTimeMs, Errors.forException(e));
    }

    @Override
    public PushTelemetryRequestData data() {
        return data;
    }

    public PushTelemetryResponse errorResponse(int throttleTimeMs, Errors errors) {
        PushTelemetryResponseData responseData = new PushTelemetryResponseData();
        responseData.setErrorCode(errors.code());
        responseData.setThrottleTimeMs(throttleTimeMs);
        return new PushTelemetryResponse(responseData);
    }

    public String metricsContentType() {
        // Future versions of PushTelemetryRequest and GetTelemetrySubscriptionsRequest may include a content-type
        // field to allow for updated OTLP format versions (or additional formats), but this field is currently not
        // included since only one format is specified in the current proposal of the kip-714
        return OTLP_CONTENT_TYPE;
    }

    public ByteBuffer metricsData() {
        CompressionType cType = CompressionType.forId(this.data.compressionType());
        return (cType == CompressionType.NONE) ?
            ByteBuffer.wrap(this.data.metrics()) : ClientTelemetryUtils.decompress(this.data.metrics(), cType);
    }

    public static PushTelemetryRequest parse(ByteBuffer buffer, short version) {
        return new PushTelemetryRequest(new PushTelemetryRequestData(
                new ByteBufferAccessor(buffer), version), version);
    }
}
