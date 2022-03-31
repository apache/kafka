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

import org.apache.kafka.common.message.PushTelemetryRequestData;
import org.apache.kafka.common.message.PushTelemetryResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Utils;

import java.io.InputStream;
import java.nio.ByteBuffer;

public class PushTelemetryRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<PushTelemetryRequest> {
        Uuid clientInstanceId;
        int subscriptionId;
        boolean terminating;
        CompressionType compressionType;
        Bytes metricsData;

        public Builder(Uuid clientInstanceId, int subscriptionId, boolean terminating, CompressionType compressionType, Bytes metricsData) {
            super(ApiKeys.PUSH_TELEMETRY);
            this.clientInstanceId = clientInstanceId;
            this.subscriptionId = subscriptionId;
            this.terminating = terminating;
            this.compressionType = compressionType;
            this.metricsData = metricsData;
        }

        @Override
        public PushTelemetryRequest build(short version) {
            PushTelemetryRequestData data = new PushTelemetryRequestData();
            data.setClientInstanceId(clientInstanceId);
            data.setSubscriptionId(subscriptionId);
            data.setTerminating(terminating);
            data.setCompressionType((byte) compressionType.id);
            data.setMetrics(metricsData.get());
            return new PushTelemetryRequest(data, version);
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
                .setErrorCode(Errors.forException(e).code());
        responseData.setThrottleTimeMs(throttleTimeMs);
        return new PushTelemetryResponse(responseData);
    }

    public PushTelemetryResponse createResponse(int throttleTimeMs, Errors errors) {
        PushTelemetryResponseData responseData = new PushTelemetryResponseData();
        responseData.setErrorCode(errors.code());
        responseData.setThrottleTimeMs(throttleTimeMs);
        return new PushTelemetryResponse(responseData);
    }

    @Override
    public PushTelemetryRequestData data() {
        return data;
    }

    public Uuid getClientInstanceId() {
        return this.data.clientInstanceId();
    }

    public int getSubscriptionId() {
        return this.data.subscriptionId();
    }

    public boolean isClientTerminating() {
        return this.data.terminating();
    }

    public String getMetricsContentType() {
        // Future versions of PushTelemetryRequest and GetTelemetrySubscriptionsRequest may include a content-type
        // field to allow for updated OTLP format versions (or additional formats), but this field is currently not
        // included since only one format is specified in the current proposal of the kip-714
        return "OTLP";
    }

    public ByteBuffer getMetricsData() throws Exception {
        CompressionType cType = CompressionType.forId(this.data.compressionType());
        return cType == CompressionType.NONE ?
                ByteBuffer.wrap(this.data.metrics()) : decompressMetricsData(cType, this.data.metrics());
    }

    public static ByteBuffer decompressMetricsData(CompressionType compressionType, byte[] metrics) throws Exception {
        ByteBuffer data = ByteBuffer.wrap(metrics);
        ByteBuffer decompressedData = ByteBuffer.allocate(data.capacity() * 4);
        try (InputStream in = compressionType.wrapForInput(data, RecordBatch.CURRENT_MAGIC_VALUE, BufferSupplier.create())) {
            Utils.readFully(in, decompressedData);
        }
        return decompressedData;
    }

    public static PushTelemetryRequest parse(ByteBuffer buffer, short version) {
        return new PushTelemetryRequest(new PushTelemetryRequestData(
                new ByteBufferAccessor(buffer), version), version);
    }
}
