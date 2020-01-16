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

import org.apache.kafka.common.message.DescribeGroupsRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;

import static org.apache.kafka.common.requests.AbstractResponse.DEFAULT_THROTTLE_TIME;

public class DescribeGroupsRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<DescribeGroupsRequest> {
        private final DescribeGroupsRequestData data;

        public Builder(DescribeGroupsRequestData data) {
            super(ApiKeys.DESCRIBE_GROUPS);
            this.data = data;
        }

        @Override
        public DescribeGroupsRequest build(short version) {
            return new DescribeGroupsRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final DescribeGroupsRequestData data;
    private final short version;

    private DescribeGroupsRequest(DescribeGroupsRequestData data, short version) {
        super(ApiKeys.DESCRIBE_GROUPS, version);
        this.data = data;
        this.version = version;
    }

    public DescribeGroupsRequest(Struct struct, short version) {
        super(ApiKeys.DESCRIBE_GROUPS, version);
        this.data = new DescribeGroupsRequestData(struct, version);
        this.version = version;
    }

    public DescribeGroupsRequestData data() {
        return data;
    }

    @Override
    protected Struct toStruct() {
        return data.toStruct(version);
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        if (version == 0) {
            return DescribeGroupsResponse.fromError(DEFAULT_THROTTLE_TIME, Errors.forException(e), data.groups());
        } else {
            return DescribeGroupsResponse.fromError(throttleTimeMs, Errors.forException(e), data.groups());
        }
    }

    public static DescribeGroupsRequest parse(ByteBuffer buffer, short version) {
        return new DescribeGroupsRequest(ApiKeys.DESCRIBE_GROUPS.parseRequest(version, buffer), version);
    }
}
