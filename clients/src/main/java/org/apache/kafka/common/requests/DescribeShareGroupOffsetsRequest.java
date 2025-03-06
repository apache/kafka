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

import org.apache.kafka.common.message.DescribeShareGroupOffsetsRequestData;
import org.apache.kafka.common.message.DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestGroup;
import org.apache.kafka.common.message.DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;

public class DescribeShareGroupOffsetsRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<DescribeShareGroupOffsetsRequest> {

        private final DescribeShareGroupOffsetsRequestData data;

        public Builder(DescribeShareGroupOffsetsRequestData data) {
            this(data, false);
        }

        public Builder(DescribeShareGroupOffsetsRequestData data, boolean enableUnstableLastVersion) {
            super(ApiKeys.DESCRIBE_SHARE_GROUP_OFFSETS, enableUnstableLastVersion);
            this.data = data;
        }

        @Override
        public DescribeShareGroupOffsetsRequest build(short version) {
            return new DescribeShareGroupOffsetsRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final DescribeShareGroupOffsetsRequestData data;

    public DescribeShareGroupOffsetsRequest(DescribeShareGroupOffsetsRequestData data, short version) {
        super(ApiKeys.DESCRIBE_SHARE_GROUP_OFFSETS, version);
        this.data = data;
    }

    public List<String> groupIds() {
        return data.groups()
            .stream()
            .map(DescribeShareGroupOffsetsRequestGroup::groupId)
            .collect(Collectors.toList());
    }

    public List<DescribeShareGroupOffsetsRequestGroup> groups() {
        return data.groups();
    }

    @Override
    public DescribeShareGroupOffsetsResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        // Sets the exception as the group-level error code for all groups, with empty partitions data for all groups
        return new DescribeShareGroupOffsetsResponse(throttleTimeMs, groupIds(), e);
    }

    @Override
    public DescribeShareGroupOffsetsRequestData data() {
        return data;
    }

    public static DescribeShareGroupOffsetsRequest parse(ByteBuffer buffer, short version) {
        return new DescribeShareGroupOffsetsRequest(
                new DescribeShareGroupOffsetsRequestData(new ByteBufferAccessor(buffer), version),
                version
        );
    }

    public static DescribeShareGroupOffsetsResponseGroup getErrorDescribedGroup(String groupId, Errors error) {
        return new DescribeShareGroupOffsetsResponseGroup()
            .setGroupId(groupId)
            .setErrorCode(error.code())
            .setErrorMessage(error.message());
    }
}
