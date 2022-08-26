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

import org.apache.kafka.common.message.DescribeConfigsRequestData;
import org.apache.kafka.common.message.DescribeConfigsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;
import java.util.stream.Collectors;

public class DescribeConfigsRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<DescribeConfigsRequest> {
        private final DescribeConfigsRequestData data;

        public Builder(DescribeConfigsRequestData data) {
            super(ApiKeys.DESCRIBE_CONFIGS);
            this.data = data;
        }

        @Override
        public DescribeConfigsRequest build(short version) {
            return new DescribeConfigsRequest(data, version);
        }
    }

    private final DescribeConfigsRequestData data;

    public DescribeConfigsRequest(DescribeConfigsRequestData data, short version) {
        super(ApiKeys.DESCRIBE_CONFIGS, version);
        this.data = data;
    }

    @Override
    public DescribeConfigsRequestData data() {
        return data;
    }

    @Override
    public DescribeConfigsResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);
        return new DescribeConfigsResponse(new DescribeConfigsResponseData()
                .setThrottleTimeMs(throttleTimeMs)
                .setResults(data.resources().stream().map(result -> {
                    return new DescribeConfigsResponseData.DescribeConfigsResult().setErrorCode(error.code())
                            .setErrorMessage(error.message())
                            .setResourceName(result.resourceName())
                            .setResourceType(result.resourceType());
                }).collect(Collectors.toList())
        ));
    }

    public static DescribeConfigsRequest parse(ByteBuffer buffer, short version) {
        return new DescribeConfigsRequest(new DescribeConfigsRequestData(new ByteBufferAccessor(buffer), version), version);
    }
}
