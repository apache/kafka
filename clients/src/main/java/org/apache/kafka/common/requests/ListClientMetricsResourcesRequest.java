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

import org.apache.kafka.common.message.ListClientMetricsResourcesRequestData;
import org.apache.kafka.common.message.ListClientMetricsResourcesResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;

public class ListClientMetricsResourcesRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<ListClientMetricsResourcesRequest> {
        public final ListClientMetricsResourcesRequestData data;

        public Builder(ListClientMetricsResourcesRequestData data) {
            super(ApiKeys.LIST_CLIENT_METRICS_RESOURCES);
            this.data = data;
        }

        @Override
        public ListClientMetricsResourcesRequest build(short version) {
            return new ListClientMetricsResourcesRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final ListClientMetricsResourcesRequestData data;

    private ListClientMetricsResourcesRequest(ListClientMetricsResourcesRequestData data, short version) {
        super(ApiKeys.LIST_CLIENT_METRICS_RESOURCES, version);
        this.data = data;
    }

    public ListClientMetricsResourcesRequestData data() {
        return data;
    }

    @Override
    public ListClientMetricsResourcesResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);
        ListClientMetricsResourcesResponseData response = new ListClientMetricsResourcesResponseData()
            .setErrorCode(error.code())
            .setThrottleTimeMs(throttleTimeMs);
        return new ListClientMetricsResourcesResponse(response);
    }

    public static ListClientMetricsResourcesRequest parse(ByteBuffer buffer, short version) {
        return new ListClientMetricsResourcesRequest(new ListClientMetricsResourcesRequestData(
            new ByteBufferAccessor(buffer), version), version);
    }

    @Override
    public String toString(boolean verbose) {
        return data.toString();
    }

}
