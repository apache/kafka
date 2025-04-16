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

import org.apache.kafka.common.message.ListConfigResourcesRequestData;
import org.apache.kafka.common.message.ListConfigResourcesResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Readable;

public class ListConfigResourcesRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<ListConfigResourcesRequest> {
        public final ListConfigResourcesRequestData data;

        public Builder(ListConfigResourcesRequestData data) {
            super(ApiKeys.LIST_CONFIG_RESOURCES);
            this.data = data;
        }

        @Override
        public ListConfigResourcesRequest build(short version) {
            return new ListConfigResourcesRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final ListConfigResourcesRequestData data;

    private ListConfigResourcesRequest(ListConfigResourcesRequestData data, short version) {
        super(ApiKeys.LIST_CONFIG_RESOURCES, version);
        this.data = data;
    }

    public ListConfigResourcesRequestData data() {
        return data;
    }

    @Override
    public ListConfigResourcesResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);
        ListConfigResourcesResponseData response = new ListConfigResourcesResponseData()
            .setErrorCode(error.code())
            .setThrottleTimeMs(throttleTimeMs);
        return new ListConfigResourcesResponse(response);
    }

    public static ListConfigResourcesRequest parse(Readable readable, short version) {
        return new ListConfigResourcesRequest(new ListConfigResourcesRequestData(
            readable, version), version);
    }

    @Override
    public String toString(boolean verbose) {
        return data.toString();
    }

}
