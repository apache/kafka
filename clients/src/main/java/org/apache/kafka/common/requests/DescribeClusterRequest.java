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

import java.nio.ByteBuffer;
import org.apache.kafka.common.message.DescribeClusterRequestData;
import org.apache.kafka.common.message.DescribeClusterResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;

public class DescribeClusterRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<DescribeClusterRequest> {

        private final DescribeClusterRequestData data;

        public Builder(DescribeClusterRequestData data) {
            super(ApiKeys.DESCRIBE_CLUSTER);
            this.data = data;
        }

        @Override
        public DescribeClusterRequest build(final short version) {
            return new DescribeClusterRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final DescribeClusterRequestData data;

    public DescribeClusterRequest(DescribeClusterRequestData data, short version) {
        super(ApiKeys.DESCRIBE_CLUSTER, version);
        this.data = data;
    }

    @Override
    public DescribeClusterRequestData data() {
        return data;
    }

    @Override
    public AbstractResponse getErrorResponse(final int throttleTimeMs, final Throwable e) {
        ApiError apiError = ApiError.fromThrowable(e);
        return new DescribeClusterResponse(new DescribeClusterResponseData()
            .setErrorCode(apiError.error().code())
            .setErrorMessage(apiError.message()));
    }

    @Override
    public String toString(final boolean verbose) {
        return data.toString();
    }

    public static DescribeClusterRequest parse(ByteBuffer buffer, short version) {
        return new DescribeClusterRequest(new DescribeClusterRequestData(new ByteBufferAccessor(buffer), version), version);
    }
}
